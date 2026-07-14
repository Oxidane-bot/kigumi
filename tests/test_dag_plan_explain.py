from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from kigumi.dag import Dag
from tests._dag_helpers import _build_scan_dag, _make_dag


def test_explain_records_key_components_and_reports_one_changed_input(tmp_path: Path) -> None:
    """教训 opaque_key: 重算理由必须能从同源成分摘要直接看见。"""

    def build(value: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("work", params={"value": value})
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": ctx.params["value"]}

        return dag

    first = build(1)
    run = first.run()
    assert first.explain("work", run.run_id).status == "hit"

    changed = build(2).explain("work", run.run_id)
    assert changed.status == "miss"
    assert changed.changed == ["params"]
    assert set(changed.details["params"]) == {"old", "new"}


def test_explain_map_item_uses_its_own_sidecar(tmp_path: Path) -> None:
    """教训 map_explain: 单项缓存键不能被聚合节点摘要掩盖。"""

    def build(tone: str) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
            return {"items": [{"id": "a"}, {"id": "b"}]}

        @dag.map(
            "work",
            items_from=("source", "items"),
            key_fn=lambda item: item["id"],
            params={"tone": tone},
        )
        def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"id": item["id"], "tone": ctx.params["tone"]}

        return dag

    first = build("brief")
    run = first.run()
    assert first.explain("work@a", run.run_id).status == "hit"

    changed = build("full").explain("work@a", run.run_id)
    assert changed.status == "miss"
    assert changed.changed == ["params"]


def test_explain_reports_unknown_no_entry_and_legacy_without_guessing(tmp_path: Path) -> None:
    """教训 explain_honesty: 无法取得上游或旧记录时不能伪造变化原因。"""

    def build(value: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source", params={"value": value})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": ctx.params["value"]}

        @dag.node("leaf", deps=("source",))
        def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return dict(inputs["source"])

        return dag

    first = build(1)
    assert first.explain("source").status == "no_entry"
    run = first.run()

    unknown = build(2).explain("leaf", run.run_id)
    assert unknown.status == "unknown"
    assert unknown.pending_on == ("source",)

    sidecar = tmp_path / "artifacts" / "runs" / run.run_id / "source.json.meta.json"
    metadata = json.loads(sidecar.read_text(encoding="utf-8"))
    metadata.pop("key_components")
    sidecar.write_text(json.dumps(metadata), encoding="utf-8")
    assert first.explain("source", run.run_id).status == "legacy"


def test_explain_without_run_id_uses_numeric_latest_run(tmp_path: Path) -> None:
    """教训 explain_run_sort: 最新 sidecar 的选择必须按 run 序号而非字典序。"""
    dag = _make_dag(tmp_path)
    root = tmp_path / "artifacts" / "runs"
    for run_id, status in [("run-9999", "old"), ("run-10000", "latest")]:
        run = root / run_id
        run.mkdir(parents=True)
        (run / "node.json.meta.json").write_text('{"status": "' + status + '"}', encoding="utf-8")

    assert dag._read_explain_sidecar("node", None) == {"status": "latest"}


def test_plan_forecasts_cache_without_calling_nodes_and_matches_run(tmp_path: Path) -> None:
    """教训 plan_truthfulness: 预览只能复用真实键路径，不能猜上游 miss 的产物。"""
    calls: list[str] = []

    def build(value: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source", params={"value": value})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            calls.append("source")
            return {"value": ctx.params["value"]}

        @dag.node("leaf", deps=("source",))
        def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            calls.append("leaf")
            return {"value": inputs["source"]["value"] + 1}

        return dag

    fresh = build(1).plan()
    assert fresh.nodes == {"source": "miss", "leaf": "unknown"}
    assert fresh.misses == ["source", "leaf"]
    assert calls == []
    assert not (tmp_path / "artifacts" / "runs").exists()
    build(1).run()
    assert build(1).plan().nodes == {"source": "hit", "leaf": "hit"}

    changed = build(2)
    forecast = changed.plan()
    result = changed.run()
    actual_misses = [name for name in ("source", "leaf") if name not in result.cache_hits]

    assert forecast.misses == actual_misses == ["source", "leaf"]


def test_plan_reports_unknown_reason_chains_and_cost_bounds(tmp_path: Path) -> None:
    """教训 plan_intervals: unknown 必须说明直接等待谁，而非被误判为确定重算。"""

    def build(value: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("a", params={"value": value})
        def a(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            del inputs
            return {"value": ctx.params["value"]}

        @dag.node("b", deps=("a",))
        def b(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": inputs["a"]["value"] + 1}

        @dag.node("c", deps=("b",))
        def c(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": inputs["b"]["value"] + 1}

        return dag

    build(1).run()
    forecast = build(2).plan()

    assert forecast.nodes == {"a": "miss", "b": "unknown", "c": "unknown"}
    assert forecast.pending_on == {"b": ("a",), "c": ("b",)}
    assert forecast.certain == ["a"]
    assert forecast.at_risk == ["b", "c"]


def test_plan_map_aggregate_miss_is_the_downstream_unknown_reason(tmp_path: Path) -> None:
    """教训 plan_map_reason: map 聚合未命中时下游只直接等待 map 节点。"""

    def build(tag: str) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, int | str]]]:
            del inputs, ctx
            return {"items": [{"id": "a", "value": 1}]}

        @dag.map(
            "mapped",
            items_from=("source", "items"),
            key_fn=lambda item: item["id"],
            params={"tag": tag},
        )
        def mapped(item: dict[str, int | str], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            del inputs, ctx
            return {"value": item["value"]}

        @dag.node("after", deps=("mapped",))
        def after(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": int(inputs["mapped"]["items"]["a"]["value"])}

        return dag

    build("v1").run()
    forecast = build("v2").plan()

    assert forecast.nodes == {
        "source": "hit",
        "mapped@a": "miss",
        "mapped": "miss",
        "after": "unknown",
    }
    assert forecast.pending_on == {"after": ("mapped",)}


def test_plan_map_items_expose_their_direct_unknown_dependency(tmp_path: Path) -> None:
    """教训 plan_map_item_reason: 已知清单的展开项也必须带直接等待边。"""

    def build(style: str) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
            del inputs, ctx
            return {"items": [{"id": "a"}, {"id": "b"}]}

        @dag.node("style", params={"style": style})
        def style_node(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            del inputs
            return {"style": ctx.params["style"]}

        @dag.map(
            "mapped",
            items_from=("source", "items"),
            key_fn=lambda item: item["id"],
            deps=("style",),
        )
        def mapped(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            del inputs, ctx
            return item

        return dag

    build("v1").run()
    forecast = build("v2").plan()

    assert forecast.nodes == {
        "source": "hit",
        "style": "miss",
        "mapped@a": "unknown",
        "mapped@b": "unknown",
        "mapped": "unknown",
    }
    assert forecast.pending_on == {
        "mapped@a": ("style",),
        "mapped@b": ("style",),
        "mapped": ("style",),
    }


def test_plan_expands_map_items_and_propagates_unknown(tmp_path: Path) -> None:
    """教训 plan_map_cost: map 只有在上游 artifact 已知时才能诚实展开 item 成本。"""

    def build(value: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"value": value})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"id": "a", "value": ctx.params["value"]}, {"id": "b", "value": 2}]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"value": item["value"]}

        @dag.node("after", deps=("m",))
        def after(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"count": inputs["m"]["count"]}

        return dag

    assert build(1).plan().nodes == {"scan": "miss", "m": "unknown", "after": "unknown"}
    build(1).run()
    forecast = build(2).plan()

    assert forecast.nodes == {
        "scan": "miss",
        "m": "unknown",
        "after": "unknown",
    }
    forced_item = build(1).plan(force=("m@a",))
    assert forced_item.nodes == {
        "scan": "hit",
        "m@a": "miss",
        "m@b": "hit",
        "m": "miss",
        "after": "unknown",
    }
    assert build(1).plan().nodes == {
        "scan": "hit",
        "m@a": "hit",
        "m@b": "hit",
        "m": "hit",
        "after": "hit",
    }


def test_plan_rejects_unknown_forced_map_items_like_run(tmp_path: Path) -> None:
    """教训 plan_force: force 项名打错时预告必须与 run 同样报错,
    静默忽略会让成本闸门看似通过、实跑才失败。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    dag, _ = _build_scan_dag(tmp_path, items)
    dag.run()

    with pytest.raises(ValueError, match="Unknown forced map items: chain@nope"):
        dag.plan(force=["chain@nope"])


def test_plan_wraps_carry_fn_failure_with_scan_item_context_like_run(tmp_path: Path) -> None:
    """教训 plan_carry_error: 同一个 carry_fn 故障在 plan 与 run 必须
    给出同形态的节点+项上下文,不许裸抛原始异常。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    dag, _ = _build_scan_dag(tmp_path, items)
    dag.run()

    def broken_carry(artifact: dict[str, Any]) -> Any:
        raise ValueError(f"bad ledger {artifact['id']}")

    rebuilt, _ = _build_scan_dag(tmp_path, items, carry_fn=broken_carry)
    with pytest.raises(RuntimeError, match="Scan node 'chain' failed item 'a'"):
        rebuilt.plan()
    with pytest.raises(RuntimeError, match="Scan node 'chain' failed item 'a'"):
        rebuilt.run()
