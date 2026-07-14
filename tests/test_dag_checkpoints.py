from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from kigumi.dag import CheckpointPending, Dag
from tests._dag_helpers import _make_dag


def test_checkpoint_names_must_be_safe_single_path_components(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("review")
    def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"approval": ctx.checkpoint("../escape", {"ready": True})}

    with pytest.raises(
        ValueError,
        match="Checkpoint name.*single non-empty relative path component",
    ):
        dag.run(run_id="safe-run")

    assert not (tmp_path / "artifacts" / "runs" / "safe-run" / "escape.pending.json").exists()


def test_checkpoint_pending_approval_and_resume(tmp_path: Path) -> None:
    """教训 interrupt_resume: 待审批分支停止，批准后以同一 run 续过。"""
    dag = _make_dag(tmp_path)

    @dag.node("independent")
    def independent(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        return {"completed": True}

    @dag.node("review")
    def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"approval": ctx.checkpoint("editor", {"question": "approve?"})}

    @dag.node("publish", deps=("review",))
    def publish(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"published": True}

    pending = dag.run(run_id="review-run")
    pending_path = (
        tmp_path / "artifacts" / "runs" / "review-run" / "approvals" / "editor.pending.json"
    )
    assert pending.artifacts == {"independent": {"completed": True}}
    assert pending.pending_checkpoints == ["editor"]
    # 教训 visible_skip: 挂起的下游不执行可以,静默消失不可以。
    assert pending.skipped == ["publish"]
    assert pending_path.exists()

    dag.approve("review-run", "editor", {"accepted": True})
    resumed = dag.run(run_id="review-run")

    assert resumed.pending_checkpoints == []
    assert resumed.skipped == []
    assert resumed.artifacts == {
        "independent": {"completed": True},
        "review": {"approval": {"accepted": True}},
        "publish": {"published": True},
    }

    fresh = dag.run(run_id="fresh-review")
    assert fresh.pending_checkpoints == ["editor"]
    assert "review" not in fresh.cache_hits
    assert fresh.skipped == ["publish"]


def test_checkpoint_exception_exposes_name_and_payload() -> None:
    """教训 checkpoint_contract: runner 只能通过结构化 pending 信息落盘。"""
    pending = CheckpointPending("editor", {"question": "approve?"})

    assert pending.name == "editor"
    assert pending.payload == {"question": "approve?"}


def test_approval_binds_to_payload_content(tmp_path: Path) -> None:
    """教训 checkpoint_binding: 审批绑定 payload 内容哈希,内容变更必须重批。"""

    def make(value: str) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("review", params={"value": value})
        def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"approval": ctx.checkpoint("editor", {"content": ctx.params["value"]})}

        return dag

    first = make("v1")
    with pytest.raises(ValueError, match="No pending checkpoint"):
        first.approve("bind-run", "editor", {"ok": True})

    assert first.run(run_id="bind-run").pending_checkpoints == ["editor"]
    first.approve("bind-run", "editor", {"ok": True})
    assert first.run(run_id="bind-run").artifacts["review"] == {"approval": {"ok": True}}

    changed = make("v2")
    assert changed.run(run_id="bind-run").pending_checkpoints == ["editor"]
    assert (
        tmp_path / "artifacts" / "runs" / "bind-run" / "approvals" / "editor.pending.json"
    ).exists()
    changed.approve("bind-run", "editor", {"ok": "second"})
    resumed = changed.run(run_id="bind-run")
    assert resumed.artifacts["review"] == {"approval": {"ok": "second"}}


def test_map_checkpoint_is_namespaced_and_resumes_one_item(tmp_path: Path) -> None:
    """教训 map_checkpoint: item 审批要隔离命名，恢复时只重跑挂起项。"""
    dag = _make_dag(tmp_path)
    executed: list[str] = []

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "ready"}, {"id": "review"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        executed.append(item["id"])
        if item["id"] == "review":
            return {"approval": ctx.checkpoint("editor", {"id": item["id"]})}
        return {"id": item["id"]}

    @dag.node("after", deps=("m",))
    def after(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        return {"ran": True}

    first = dag.run(run_id="map-approval")
    assert first.pending_checkpoints == ["editor@review"]
    assert first.skipped == ["after"]
    assert executed == ["ready", "review"]
    dag.approve("map-approval", "editor@review", {"ok": True})
    executed.clear()
    resumed = dag.run(run_id="map-approval")

    assert executed == ["review"]
    assert resumed.artifacts["m"]["items"]["review"] == {"approval": {"ok": True}}
    assert resumed.skipped == []

    executed.clear()
    fresh = dag.run(run_id="map-approval-fresh")
    assert fresh.pending_checkpoints == ["editor@review"]
    assert fresh.skipped == ["after"]
    assert executed == ["review"]


def test_scan_checkpoint_approval_does_not_leak_through_item_cache(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    executed: list[str] = []

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "ready"}, {"id": "review"}]}

    @dag.scan("review", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def review(
        item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any
    ) -> dict[str, Any]:
        executed.append(item["id"])
        if item["id"] == "review":
            return {"approval": ctx.checkpoint("editor", {"id": item["id"]})}
        return {"id": item["id"]}

    first = dag.run(run_id="scan-approval")
    assert first.pending_checkpoints == ["editor@review"]
    assert executed == ["ready", "review"]

    dag.approve(first.run_id, "editor@review", {"ok": True})
    executed.clear()
    resumed = dag.run(run_id=first.run_id)
    assert resumed.pending_checkpoints == []
    assert resumed.artifacts["review"]["items"]["review"] == {"approval": {"ok": True}}
    assert executed == ["review"]

    executed.clear()
    fresh = dag.run(run_id="scan-approval-fresh")
    assert fresh.pending_checkpoints == ["editor@review"]
    assert executed == ["review"]
