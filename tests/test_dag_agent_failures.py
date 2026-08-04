from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from kigumi import (
    AgentExecutionFailure,
    AgentRuntimeFailureCode,
    Message,
    PromptResolution,
    ProviderFailure,
    ProviderFailureKind,
    ProviderFailureStage,
    ResolvedPrompt,
)
from kigumi.agents import (
    AgentCapabilities,
    AgentCompletion,
    AgentFileSelector,
    AgentPublish,
    AgentResultError,
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

    with pytest.raises(AgentExecutionFailure) as raised:
        dag.run()
    assert raised.value.runtime_code is AgentRuntimeFailureCode.PROTOCOL

    failures = list((tmp_path / "artifacts" / "runs").glob("*/failures/broken.json"))
    assert len(failures) == 1
    record = json.loads(failures[0].read_text(encoding="utf-8"))
    assert record["status"] == "failed"
    assert record["failure"]["runtime_code"] == "protocol"
    assert record["trajectory"]["events"] == 1
    assert not list((tmp_path / "artifacts" / "_workspaces").iterdir())
    assert not list((tmp_path / "artifacts" / "_cache" / "nodes").glob("*.json"))


def test_agent_provider_failure_keeps_shared_typed_failure(tmp_path: Path) -> None:
    class ProviderAdapter(FailingAdapter):
        def run(self, request: Any, context: Any) -> Any:
            raise ProviderFailure(
                provider="fake",
                stage=ProviderFailureStage.PROVIDER,
                kind=ProviderFailureKind.RATE_LIMIT,
                status_code=429,
                retry_after_ms=1000,
                provider_request_id="request-1",
                message_digest="a" * 64,
                retryable_hint=True,
            )

    dag = _make_dag(tmp_path)
    spec = make_agent_spec(tmp_path / "agent")

    @dag.agent("provider-failure", adapter=ProviderAdapter(), spec=spec)
    def provider_failure(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask("fail at provider")

    with pytest.raises(AgentExecutionFailure) as raised:
        dag.run()
    assert raised.value.provider_failure is not None
    assert raised.value.provider_failure.kind is ProviderFailureKind.RATE_LIMIT
    record_path = next((tmp_path / "artifacts" / "runs").glob("*/failures/provider-failure.json"))
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["failure"]["provider_failure"]["kind"] == "rate_limit"


def test_invalid_agent_prompt_resolution_fails_before_agent_side_effect(
    tmp_path: Path,
) -> None:
    class CountingAdapter(FailingAdapter):
        def __init__(self) -> None:
            self.runs = 0

        def run(self, request: Any, context: Any) -> AgentRunResult:
            self.runs += 1
            return AgentRunResult(AgentCompletion("completed", "unexpected"))

    adapter = CountingAdapter()
    dag = _make_dag(tmp_path)
    spec = make_agent_spec(tmp_path / "agent")
    incomplete = PromptResolution(
        spec_name="managed",
        structure_digest="structure",
        base={},
        layers=(),
        axes=(),
        materials=(),
        rendered_sha256="rendered",
        rendered_bytes=0,
        messages=[Message("user", ["incomplete"])],
    )

    @dag.agent("invalid", adapter=adapter, spec=spec)
    def invalid(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        del inputs, ctx
        return AgentTask(ResolvedPrompt("incomplete", incomplete))

    with pytest.raises(AgentResultError, match="prompt resolution"):
        dag.run(run_id="invalid-resolution")

    assert adapter.runs == 0
    state_path = next(
        (tmp_path / "artifacts" / "runs" / "invalid-resolution" / "attempts").glob("*/state.json")
    )
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert "active_effect" not in state
    assert state["side_effect_started"] is False


def test_invalid_agent_scan_prompt_resolution_fails_before_agent_side_effect(
    tmp_path: Path,
) -> None:
    class CountingAdapter(FailingAdapter):
        def __init__(self) -> None:
            self.runs = 0

        def run(self, request: Any, context: Any) -> AgentRunResult:
            self.runs += 1
            return AgentRunResult(AgentCompletion("completed", "unexpected"))

    adapter = CountingAdapter()
    dag = _make_dag(tmp_path)
    spec = make_agent_spec(tmp_path / "agent")
    incomplete = PromptResolution(
        spec_name="managed",
        structure_digest="structure",
        base={},
        layers=(),
        axes=(),
        materials=(),
        rendered_sha256="rendered",
        rendered_bytes=0,
        messages=[Message("user", ["incomplete"])],
    )

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "one"}]}

    @dag.agent_scan(
        "invalid",
        adapter=adapter,
        spec=spec,
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
    )
    def invalid(item: Any, carry: Any, inputs: dict[str, Any], ctx: Any) -> AgentTask:
        del item, carry, inputs, ctx
        return AgentTask(ResolvedPrompt("incomplete", incomplete))

    with pytest.raises(AgentResultError, match="prompt resolution"):
        dag.run(run_id="invalid-scan-resolution")

    assert adapter.runs == 0
    state_path = next(
        (tmp_path / "artifacts" / "runs" / "invalid-scan-resolution" / "attempts").glob(
            "*/state.json"
        )
    )
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert "active_effect" not in state
    assert state["side_effect_started"] is False


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

    with pytest.raises(AgentExecutionFailure) as raised:
        dag.run()
    assert raised.value.runtime_code is AgentRuntimeFailureCode.PROTOCOL

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

    with pytest.raises(AgentExecutionFailure) as raised:
        dag.run()
    assert raised.value.runtime_code is AgentRuntimeFailureCode.PROTOCOL
    assert not (tmp_path / "out.md").exists()
