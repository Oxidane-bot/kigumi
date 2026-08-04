from __future__ import annotations

from typing import Any

import pytest

from kigumi import AmbiguousAttemptError
from kigumi.agents import (
    AgentCapabilities,
    AgentCompletion,
    AgentRunResult,
    AgentTask,
)
from tests._agent_helpers import make_agent_spec
from tests._dag_helpers import _make_dag


class SessionAdapter:
    session_carry = True

    def __init__(self) -> None:
        self.runs = 0
        self.session_inputs: list[bytes | None] = []

    def cache_identity(self) -> dict[str, Any]:
        return {"adapter": "session-fake", "schema": 1}

    def capabilities(self) -> AgentCapabilities:
        return AgentCapabilities(filesystem=True)

    def run(self, request: Any, context: Any) -> AgentRunResult:
        self.runs += 1
        self.session_inputs.append(context.session_in)
        previous = context.session_in or b""
        context.record_session(previous + request.task.instruction.encode("utf-8"))
        return AgentRunResult(AgentCompletion("completed", "done"))


class CrashOnceAdapter:
    def __init__(self, marker: Any) -> None:
        self.runs = 0
        self.marker = marker

    def cache_identity(self) -> dict[str, Any]:
        return {"adapter": "scan-crash", "schema": 1}

    def capabilities(self) -> AgentCapabilities:
        return AgentCapabilities()

    def run(self, request: Any, context: Any) -> AgentRunResult:
        del request, context
        self.runs += 1
        self.marker.write_text("external effect", encoding="utf-8")
        if self.runs == 1:
            raise KeyboardInterrupt("agent scan process stopped")
        return AgentRunResult(AgentCompletion("completed", "done"))


def _build_session_scan(tmp_path: Any, adapter: SessionAdapter) -> Any:
    dag = _make_dag(tmp_path)
    spec = make_agent_spec(tmp_path / f"agent-{adapter.runs}")

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.agent_scan(
        "revise",
        adapter=adapter,
        spec=spec,
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
        carry_fn=lambda artifact: artifact["session"],
    )
    def revise(
        item: dict[str, str],
        carry: dict[str, Any] | None,
        inputs: dict[str, Any],
        ctx: Any,
    ) -> AgentTask:
        del carry, inputs, ctx
        return AgentTask(item["id"])

    return dag


def test_default_agent_scan_crash_is_ambiguous_before_resume(tmp_path: Any) -> None:
    adapter = CrashOnceAdapter(tmp_path / "agent-scan-effect.txt")
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "a"}]}

    @dag.agent_scan(
        "revise",
        adapter=adapter,
        spec=make_agent_spec(tmp_path / "scan-agent-spec"),
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
    )
    def revise(
        item: dict[str, str],
        carry: Any,
        inputs: dict[str, Any],
        ctx: Any,
    ) -> AgentTask:
        del item, carry, inputs, ctx
        return AgentTask("crash")

    with pytest.raises(KeyboardInterrupt):
        dag.run(run_id="agent-scan-default-crash")

    with pytest.raises(AmbiguousAttemptError):
        dag.resume("agent-scan-default-crash")
    assert adapter.runs == 1


def test_agent_scan_carries_session_blob_and_replays_each_item_cache(tmp_path: Any) -> None:
    adapter = SessionAdapter()
    first = _build_session_scan(tmp_path, adapter).run()
    first_items = first.artifacts["revise"]["items"]

    assert adapter.session_inputs == [None, b"a"]
    assert adapter.runs == 2
    assert set(first_items["a"]["session"]) == {
        "bytes",
        "kigumi_attachment",
        "media_type",
    }
    assert first_items["a"]["session"]["bytes"] == 1
    assert first_items["b"]["session"]["bytes"] == 2

    second = _build_session_scan(tmp_path, adapter).run()

    assert second.map_items["revise"] == {"a": "hit", "b": "hit"}
    assert second.artifacts["revise"] == first.artifacts["revise"]
    assert adapter.runs == 2


def test_agent_scan_force_prefix_recomputes_session_dependent_suffix(tmp_path: Any) -> None:
    adapter = SessionAdapter()
    dag = _build_session_scan(tmp_path, adapter)
    dag.run()

    result = dag.run(force=("revise@a",))

    assert result.map_items["revise"] == {"a": "miss", "b": "hit"}
    assert adapter.runs == 3
