from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

import pytest

from tests._dag_helpers import _build_scan_dag, _make_dag


def test_scan_reuses_prefix_and_invalidates_suffix(tmp_path: Path) -> None:
    """教训 scan_prefix: 改第 K 项时，先前 carry 链必须继续命中。"""
    original = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    original_dag, _ = _build_scan_dag(tmp_path, original)
    original_dag.run()
    changed, executed = _build_scan_dag(
        tmp_path, [{"id": "a", "value": 1}, {"id": "b", "value": 20}, {"id": "c", "value": 3}]
    )

    result = changed.run()

    assert result.map_items["chain"] == {"a": "hit", "b": "miss", "c": "miss"}
    assert executed == ["b", "c"]


def test_scan_carry_fn_code_is_irrelevant_when_extracted_content_is_equal(tmp_path: Path) -> None:
    """教训 scan_carry_content: carry_fn 的实现不是输入，实际 carry 内容才是。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    original_dag, _ = _build_scan_dag(tmp_path, items, carry_fn=lambda artifact: artifact["carry"])
    original_dag.run()
    equivalent, _ = _build_scan_dag(
        tmp_path, items, carry_fn=lambda artifact: dict(artifact["carry"])
    )

    equal_result = equivalent.run()
    changed, _ = _build_scan_dag(
        tmp_path,
        items,
        carry_fn=lambda artifact: {
            "total": artifact["carry"]["total"] + 1,
            "attempt": artifact["carry"]["attempt"],
        },
    )
    changed_result = changed.run()

    assert equal_result.map_items["chain"] == {"a": "hit", "b": "hit", "c": "hit"}
    assert changed_result.map_items["chain"] == {"a": "hit", "b": "miss", "c": "miss"}


def test_scan_carry_from_content_invalidates_the_whole_chain(tmp_path: Path) -> None:
    """教训 scan_initial_carry: 初始 carry 内容变化必须从第一项自然传导。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    original_dag, _ = _build_scan_dag(tmp_path, items, initial=0)
    original_dag.run()
    changed, _ = _build_scan_dag(tmp_path, items, initial=10)

    result = changed.run()

    assert result.map_items["chain"] == {"a": "miss", "b": "miss", "c": "miss"}


def test_scan_items_stay_serial_while_a_ready_side_branch_runs(tmp_path: Path) -> None:
    """教训 scan_serial: carry 链串行不应占掉无关旁支的并行调度机会。"""
    dag = _make_dag(tmp_path)
    scan_started = threading.Event()
    side_finished = threading.Event()
    completed: list[str] = []

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        del inputs, ctx
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.scan("chain", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def chain(item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del carry, inputs, ctx
        if item["id"] == "a":
            scan_started.set()
            assert side_finished.wait(timeout=1)
        else:
            assert completed == ["a"]
        completed.append(item["id"])
        return item

    @dag.node("side", deps=("source",))
    def side(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        del inputs, ctx
        assert scan_started.wait(timeout=1)
        side_finished.set()
        return {"ran": True}

    result = dag.run(workers=2)

    assert completed == ["a", "b"]
    assert result.artifacts["side"] == {"ran": True}


def test_scan_force_item_propagates_changed_carry_to_suffix(tmp_path: Path) -> None:
    """教训 scan_force_carry: 强算项产物变化后，后缀不能错误复用旧 carry。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    attempts: dict[str, int] = {}
    dag, _ = _build_scan_dag(tmp_path, items, attempts=attempts)
    dag.run()

    result = dag.run(force=("chain@b",))

    assert result.map_items["chain"] == {"a": "hit", "b": "miss", "c": "miss"}


def test_scan_preserves_completed_prefix_after_item_failure(tmp_path: Path) -> None:
    """教训 scan_resume: 第 K 项失败不能丢弃已落盘的前缀缓存。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    failed_once = {"active": True}
    dag, _ = _build_scan_dag(
        tmp_path, items, fail=lambda item_id: failed_once["active"] and item_id == "b"
    )

    with pytest.raises(RuntimeError, match="failed item 'b'"):
        dag.run()
    failed_once["active"] = False
    result = dag.run()

    assert result.map_items["chain"] == {"a": "hit", "b": "miss", "c": "miss"}


def test_scan_plan_forecasts_prefix_and_matches_execution(tmp_path: Path) -> None:
    """教训 scan_plan: 已知前缀可精确预告，首个 miss 后只能诚实标 unknown。"""
    original = [
        {"id": "a", "value": 1, "file": "a.txt"},
        {"id": "b", "value": 2, "file": "b.txt"},
        {"id": "c", "value": 3, "file": "c.txt"},
    ]
    for item in original:
        (tmp_path / item["file"]).write_text(item["id"], encoding="utf-8")
    original_dag, _ = _build_scan_dag(tmp_path, original, item_files=True)
    original_dag.run()
    (tmp_path / "b.txt").write_text("changed", encoding="utf-8")
    changed, _ = _build_scan_dag(tmp_path, original, item_files=True)

    forecast = changed.plan()
    result = changed.run()

    assert forecast.nodes == {
        "source": "hit",
        "initial": "hit",
        "chain@a": "hit",
        "chain@b": "miss",
        "chain@c": "unknown",
        "chain": "miss",
        "after": "unknown",
    }
    assert forecast.pending_on == {"chain@c": ("chain@b",), "after": ("chain",)}
    assert result.map_items["chain"] == {"a": "hit", "b": "miss", "c": "hit"}


def test_scan_rebuilds_aggregate_records_items_and_preserves_gc_references(tmp_path: Path) -> None:
    """教训 scan_sidecar: 聚合不缓存，但逐项键必须留在 sidecar 防止 gc 误删。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    dag, _ = _build_scan_dag(tmp_path, items)

    result = dag.run()
    metadata = json.loads(
        (tmp_path / "artifacts" / "runs" / result.run_id / "chain.json.meta.json").read_text(
            encoding="utf-8"
        )
    )

    assert result.artifacts["chain"]["order"] == ["a", "b", "c"]
    assert result.map_items["chain"] == {"a": "miss", "b": "miss", "c": "miss"}
    assert len(metadata["cache_key"]) == 3
    assert dag.gc(keep_last=1) == 0


def test_scan_registration_keeps_the_loop_guard_active(tmp_path: Path) -> None:
    """教训 scan_guard: 新原语不能成为节点内裸循环调用的守卫绕行。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        del inputs, ctx
        return {"items": [{"id": "a"}]}

    with pytest.raises(ValueError, match="Raw LLM calls inside loops"):

        @dag.scan("chain", items_from=("source", "items"), key_fn=lambda item: item["id"])
        def chain(
            item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any
        ) -> dict[str, Any]:
            del item, carry, inputs
            for _ in range(1):
                ctx.call("not allowed")
            return {}


def test_scan_resolves_a_dotted_runtime_list_path(tmp_path: Path) -> None:
    """教训 scan_dot_path: scan 与 map 必须共用点路径清单解析。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"nested": {"items": [{"id": "a", "value": 1}, {"id": "b", "value": 2}]}}

    @dag.scan("chain", items_from=("source", "nested.items"), key_fn=lambda item: item["id"])
    def chain(item: dict[str, Any], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del carry, inputs, ctx
        return {"value": item["value"]}

    result = dag.run()

    assert result.map_items["chain"] == {"a": "miss", "b": "miss"}
