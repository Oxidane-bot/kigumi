"""150 张合成客服工单的零真实请求抽取试点。"""

from __future__ import annotations

import json
import random
import re
import shutil
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import Any

from pipeline import build_dag

from kigumi import LLMCaller
from kigumi.store import gc_artifacts
from kigumi.testing import ScriptedTransport

TICKET_COUNT = 150
BAD_JSON_IDS = {"ticket-009", "ticket-038", "ticket-067", "ticket-096", "ticket-125"}
WRONG_FIELD_IDS = {"ticket-017", "ticket-104"}
CHANGED_IDS = ("ticket-021", "ticket-072", "ticket-143")
FIELD_NAMES = ("customer", "product", "severity", "request_category")

CUSTOMERS = ("安澜科技", "北辰商贸", "春望医疗", "东岭教育", "飞桥物流")
PRODUCTS = ("云盘专业版", "客服工作台", "库存管家", "订单中心", "数据看板")
SEVERITIES = ("低", "中", "高", "紧急")
REQUESTS = {
    "登录故障": "无法登录账户",
    "账单咨询": "需要核对本月账单",
    "配送查询": "想查询订单配送进度",
    "功能咨询": "想了解批量导出功能",
    "退款申请": "申请退回重复扣费",
}


def _ticket_text(
    customer: str, product: str, severity: str, request_category: str, variant: int
) -> str:
    request = REQUESTS[request_category]
    templates = (
        "客户 {customer} 反馈其 {product} 出现{severity}级问题，主要诉求是{request}。",
        "来电方：{customer}；所用产品：{product}；严重级别：{severity}。希望处理：{request}。",
        "{customer} 使用 {product}。这是一张{severity}优先级工单，客户诉求：{request}。",
    )
    return templates[variant % len(templates)].format(
        customer=customer, product=product, severity=severity, request=request
    )


def _write_fixtures(root: Path) -> dict[str, dict[str, str]]:
    """以固定 seed 生成工单正文与同时落盘的真值标签。"""
    rng = random.Random(20260712)
    ticket_dir = root / "fixtures" / "tickets"
    ticket_dir.mkdir(parents=True)
    manifest: list[dict[str, Any]] = []
    truth: dict[str, dict[str, str]] = {}
    categories = tuple(REQUESTS)
    for index in range(1, TICKET_COUNT + 1):
        ticket_id = f"ticket-{index:03d}"
        labels = {
            "customer": rng.choice(CUSTOMERS),
            "product": rng.choice(PRODUCTS),
            "severity": rng.choice(SEVERITIES),
            "request_category": rng.choice(categories),
        }
        ticket_text = _ticket_text(**labels, variant=index)
        source = f"fixtures/tickets/{ticket_id}.txt"
        (root / source).write_text(ticket_text, encoding="utf-8")
        manifest.append({"id": ticket_id, "source": source, "truth": labels})
        truth[ticket_id] = labels
    (root / "fixtures" / "manifest.json").write_text(
        json.dumps({"tickets": manifest}, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return truth


def _fixture_root(temporary: Path) -> tuple[Path, dict[str, dict[str, str]]]:
    root = temporary / "ticket-extract"
    shutil.copytree(Path(__file__).parent / "prompts", root / "prompts")
    return root, _write_fixtures(root)


def _parse_ticket_request(text: str) -> tuple[str, str]:
    match = re.search(r"```json\n(.*?)\n```", text, flags=re.DOTALL)
    if match is None:
        raise AssertionError("脚本化 responder 未找到工单材料")
    payload = json.loads(match.group(1))
    return str(payload["id"]), str(payload["text"])


def _answer_from_text(ticket_text: str) -> dict[str, str]:
    """从请求中的工单正文解析字段，不读取 fixture 真值。"""
    customer = next(name for name in CUSTOMERS if name in ticket_text)
    product = next(name for name in PRODUCTS if name in ticket_text)
    severity_match = re.search(
        r"(紧急|低|中|高)(?:级问题|优先级工单)|严重级别：(紧急|低|中|高)", ticket_text
    )
    if severity_match is None:
        raise AssertionError("脚本化 responder 未找到严重级别")
    severity = next(level for level in severity_match.groups() if level is not None)
    request_category = next(
        category for category, request in REQUESTS.items() if request in ticket_text
    )
    return {
        "customer": customer,
        "product": product,
        "severity": severity,
        "request_category": request_category,
    }


def _caller(root: Path, transport: ScriptedTransport) -> LLMCaller:
    return LLMCaller(transport, cache_dir=root / "artifacts" / "_llm", seed=20260712)


def _count_files(path: Path) -> int:
    return sum(1 for candidate in path.rglob("*") if candidate.is_file())


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="kigumi-ticket-extract-") as directory:
        root, truth = _fixture_root(Path(directory))
        attempts: Counter[str] = Counter()

        def extract_response(text: str, model: str) -> str:
            del model
            ticket_id, ticket_text = _parse_ticket_request(text)
            attempts[ticket_id] += 1
            if ticket_id in BAD_JSON_IDS and attempts[ticket_id] == 1:
                return "{坏 JSON"
            answer = _answer_from_text(ticket_text)
            if ticket_id == "ticket-017":
                answer["product"] = next(item for item in PRODUCTS if item != answer["product"])
            if ticket_id == "ticket-104":
                answer["severity"] = next(item for item in SEVERITIES if item != answer["severity"])
            return json.dumps(answer, ensure_ascii=False)

        transport = ScriptedTransport(
            {
                "STAGE: ticket_extract": extract_response,
                "STAGE: ticket_report": (
                    "确定性校验发现两张工单各有一个字段错误，错误已按产品和严重级别汇总。"
                ),
            },
            aliases={"default": "fixture-default"},
        )
        dag = build_dag(root, _caller(root, transport))

        started = time.perf_counter()
        first = dag.run(workers=8)
        first_seconds = time.perf_counter() - started
        expected_correct = TICKET_COUNT * len(FIELD_NAMES) - len(WRONG_FIELD_IDS)
        validation = first.artifacts["validate"]
        assert first.artifacts["extract"]["count"] == TICKET_COUNT
        assert validation["correct_fields"] == expected_correct
        assert validation["total_fields"] == TICKET_COUNT * len(FIELD_NAMES)
        assert validation["accuracy"] == expected_correct / (TICKET_COUNT * len(FIELD_NAMES))
        assert first.artifacts["stats"]["error_tickets"] == len(WRONG_FIELD_IDS)
        assert all(attempts[ticket_id] == 2 for ticket_id in BAD_JSON_IDS)
        assert len(transport.requests) == TICKET_COUNT + len(BAD_JSON_IDS) + 1
        print("[1] 150 项首跑、确定性 accuracy 与五项修复入库：通过")

        for ticket_id in CHANGED_IDS:
            ticket_path = root / "fixtures" / "tickets" / f"{ticket_id}.txt"
            ticket_path.write_text(
                ticket_path.read_text(encoding="utf-8") + "\n补充：客户要求尽快反馈。",
                encoding="utf-8",
            )

        changed = build_dag(root, _caller(root, transport))
        started = time.perf_counter()
        preview = changed.plan(targets=("report",))
        preview_seconds = time.perf_counter() - started
        changed_items = {f"extract@{ticket_id}" for ticket_id in CHANGED_IDS}
        assert changed_items <= set(preview.certain)
        assert set(preview.certain) - changed_items == {"extract"}
        assert set(preview.at_risk) == {"validate", "stats", "report"}
        second = changed.run(workers=8)
        assert second.map_items["extract"] == {
            f"ticket-{index:03d}": "miss" if f"ticket-{index:03d}" in CHANGED_IDS else "hit"
            for index in range(1, TICKET_COUNT + 1)
        }
        assert second.artifacts["validate"]["accuracy"] == validation["accuracy"]
        print("[2] 三张文本变更的预告、3 miss / 147 hit 与下游风险：通过")

        started = time.perf_counter()
        all_hit = build_dag(root, _caller(root, transport)).run(workers=8)
        all_hit_seconds = time.perf_counter() - started
        assert all(status == "hit" for status in all_hit.map_items["extract"].values())
        assert all_hit_seconds < first_seconds
        artifacts_files = _count_files(root / "artifacts")
        print("[3] 全命中重跑快于首跑：通过")

        removed = gc_artifacts(root / "artifacts", keep_last=1)
        assert removed > 0
        after_gc = build_dag(root, _caller(root, transport)).run(workers=8)
        assert all(status == "hit" for status in after_gc.map_items["extract"].values())
        assert after_gc.artifacts["validate"]["accuracy"] == validation["accuracy"]
        print("[4] 保留最近 run 的 gc 未误删活缓存：通过")

        print(
            "规模测量: "
            f"首跑={first_seconds:.4f}s, 预告={preview_seconds:.4f}s, "
            f"全命中重跑={all_hit_seconds:.4f}s, artifacts 文件数={artifacts_files}, "
            f"gc 删除={removed}"
        )
        print("演示通过:150 项抽取、确定性真值校验、局部失效、修复、全命中与 gc 全部走通。")


if __name__ == "__main__":
    main()
