"""客服工单抽取试点的 DAG 组装。"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from kigumi import Dag, KigumiConfig, LLMCaller, inject

Severity = Literal["低", "中", "高", "紧急"]
RequestCategory = Literal["登录故障", "账单咨询", "配送查询", "功能咨询", "退款申请"]


class TicketExtraction(BaseModel):
    """抽取结果的严格结构化契约。"""

    model_config = ConfigDict(extra="forbid")

    customer: str = Field(description="提交工单的客户名称")
    product: str = Field(description="工单涉及的产品或服务名称")
    severity: Severity = Field(description="工单问题的紧急程度")
    request_category: RequestCategory = Field(description="客户请求所属的业务类别")


def build_dag(root: Path, caller: LLMCaller) -> Dag:
    """创建本地优先、逐工单缓存的抽取流水线。"""
    dag = Dag(
        KigumiConfig(
            project_root=root,
            prompts_dir="prompts",
            artifacts_dir="artifacts",
            source_dirs=[],
        ),
        caller,
    )

    @dag.node("ingest", files=("fixtures/manifest.json",))
    def ingest(inputs: dict[str, dict[str, Any]], ctx: Any) -> dict[str, Any]:
        """读取并归一工单目录；正文由逐项 files_fn 进入抽取缓存键。"""
        del inputs
        raw_manifest = json.loads(ctx.read_text("fixtures/manifest.json"))
        tickets: list[dict[str, Any]] = []
        truth: dict[str, dict[str, str]] = {}
        for record in raw_manifest["tickets"]:
            ticket_id = str(record["id"])
            source = str(record["source"])
            labels = {name: str(value) for name, value in record["truth"].items()}
            # 真值不进 item:抽取的缓存键不该与它不消费的答案耦合,
            # 真值修订也不该无谓失效全部抽取缓存。
            tickets.append({"id": ticket_id, "source": source})
            truth[ticket_id] = labels
        return {"tickets": tickets, "truth": truth}

    @dag.map(
        "extract",
        items_from=("ingest", "tickets"),
        key_fn=lambda ticket: str(ticket["id"]),
        prompts=("extract",),
        files_fn=lambda ticket: (str(ticket["source"]),),
    )
    def extract(
        ticket: dict[str, Any], inputs: dict[str, dict[str, Any]], ctx: Any
    ) -> dict[str, Any]:
        """从单张工单抽取字段；原文不进入下游 artifact。"""
        del inputs
        raw_text = ctx.read_text(str(ticket["source"]))
        prompt = ctx.render("extract", ticket=inject({"id": ticket["id"], "text": raw_text}))
        extraction = ctx.call_validated(prompt, TicketExtraction, max_repairs=1)
        return extraction.model_dump()

    @dag.node("validate", deps=("ingest", "extract"))
    def validate(inputs: dict[str, dict[str, Any]], ctx: Any) -> dict[str, Any]:
        """完全以真值逐字段比对，拒绝使用任何 LLM 评委。"""
        del ctx
        truth = inputs["ingest"]["truth"]
        extracted = inputs["extract"]["items"]
        field_names = ("customer", "product", "severity", "request_category")
        details: list[dict[str, Any]] = []
        correct_fields = 0
        for ticket_id in inputs["extract"]["order"]:
            expected = truth[ticket_id]
            actual = extracted[ticket_id]
            matches = {field: actual[field] == expected[field] for field in field_names}
            correct_fields += sum(matches.values())
            details.append(
                {
                    "id": ticket_id,
                    "matches": matches,
                    "expected": expected,
                    "actual": actual,
                }
            )
        total_fields = len(details) * len(field_names)
        return {
            "details": details,
            "correct_fields": correct_fields,
            "total_fields": total_fields,
            "accuracy": correct_fields / total_fields,
        }

    @dag.node("stats", deps=("validate",))
    def stats(inputs: dict[str, dict[str, Any]], ctx: Any) -> dict[str, Any]:
        """按真值产品和严重级别聚合错误，供集合级报告使用。"""
        del ctx
        product_errors: Counter[str] = Counter()
        severity_errors: Counter[str] = Counter()
        error_tickets = 0
        for detail in inputs["validate"]["details"]:
            errors = [field for field, matched in detail["matches"].items() if not matched]
            if errors:
                error_tickets += 1
                product_errors[detail["expected"]["product"]] += len(errors)
                severity_errors[detail["expected"]["severity"]] += len(errors)
        return {
            "accuracy": inputs["validate"]["accuracy"],
            "error_tickets": error_tickets,
            "errors_by_product": dict(sorted(product_errors.items())),
            "errors_by_severity": dict(sorted(severity_errors.items())),
        }

    @dag.node("report", deps=("stats",), prompts=("report",))
    def report(inputs: dict[str, dict[str, Any]], ctx: Any) -> dict[str, Any]:
        """唯一集合级 LLM 调用，只将统计摘要交给模型。"""
        prompt = ctx.render("report", stats=inject(inputs["stats"]))
        return {"text": ctx.call(prompt)}

    return dag
