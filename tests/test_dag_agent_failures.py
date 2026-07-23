from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from kigumi.agents import (
    AgentCapabilities,
    AgentCompletion,
    AgentFileSelector,
    AgentPublish,
    AgentRunResult,
    AgentTask,
)
from tests._agent_helpers import make_agent_spec
from tests._dag_helpers import _make_dag


class FailingAdapter:
    def cache_identity(self) -> dict[str, str]:
        return {"adapter": "failing", "build": "1"}

    def capabilities(self) -> AgentCapabilities:
        return AgentCapabilities()

    def run(self, request: Any, context: Any) -> Any:
        context.emit_event({"type": "progress", "text": "started"})
        (context.workspace / "scratch.txt").write_text("partial", encoding="utf-8")
        raise RuntimeError("failed")


def test_agent_failure_keeps_bounded_run_local_evidence_and_cleans_workspace(
    tmp_path: Path,
) -> None:
    dag = _make_dag(tmp_path)
    spec = make_agent_spec(tmp_path / "agent")

    @dag.agent("broken", adapter=FailingAdapter(), spec=spec)
    def broken(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask("fail visibly")

    with pytest.raises(RuntimeError, match="failed"):
        dag.run()

    failures = list((tmp_path / "artifacts" / "runs").glob("*/failures/broken.json"))
    assert len(failures) == 1
    record = json.loads(failures[0].read_text(encoding="utf-8"))
    assert record["status"] == "failed"
    assert record["error_type"] == "RuntimeError"
    assert record["trajectory"]["events"] == 1
    assert not list((tmp_path / "artifacts" / "_workspaces").iterdir())
    assert not list((tmp_path / "artifacts" / "_cache" / "nodes").glob("*.json"))


def test_adapter_response_cannot_smuggle_a_materializable_blob(tmp_path: Path) -> None:
    class SmugglingAdapter(FailingAdapter):
        def run(self, request: Any, context: Any) -> AgentRunResult:
            return AgentRunResult(
                AgentCompletion(
                    "completed",
                    "done",
                    metrics={
                        "hidden": {
                            "kigumi_blob": "0" * 64,
                            "path": "smuggled.txt",
                        }
                    },
                )
            )

    dag = _make_dag(tmp_path)
    spec = make_agent_spec(tmp_path / "agent")

    @dag.agent("smuggle", adapter=SmugglingAdapter(), spec=spec)
    def smuggle(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask("try to escape publication")

    with pytest.raises(RuntimeError, match="exact publications"):
        dag.run()

    assert not (tmp_path / "smuggled.txt").exists()


@pytest.mark.parametrize("outputs", [(), ("missing.md",)])
def test_completion_must_cover_exact_collected_publish_sources(
    tmp_path: Path, outputs: tuple[str, ...]
) -> None:
    class Adapter(FailingAdapter):
        def run(self, request: Any, context: Any) -> AgentRunResult:
            (context.workspace / "draft.md").write_text("draft", encoding="utf-8")
            return AgentRunResult(AgentCompletion("completed", "done", outputs))

    dag = _make_dag(tmp_path)
    spec = make_agent_spec(tmp_path / "agent")

    @dag.agent("invalid", adapter=Adapter(), spec=spec)
    def invalid(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask(
            "write",
            collect=(AgentFileSelector("draft.md"),),
            publish=(AgentPublish("draft.md", "out.md"),),
        )

    with pytest.raises(RuntimeError, match="completion outputs"):
        dag.run()
    assert not (tmp_path / "out.md").exists()
