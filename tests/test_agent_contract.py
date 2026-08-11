from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from kigumi import AgentExecutionFailure, AgentRuntimeFailureCode
from kigumi.agents import (
    AgentCapabilities,
    AgentFileSelector,
    AgentPublish,
    AgentResultError,
    AgentRuntimeResultError,
    AgentTask,
    execute_agent_task,
)
from kigumi.failures import AgentRuntimeFailureSubCode
from tests._agent_helpers import make_agent_spec
from tests._dag_helpers import _make_dag


def test_agent_task_rejects_unsafe_or_duplicate_paths() -> None:
    for source in ("", "/absolute", "../escape", "a/../b"):
        try:
            AgentTask("write", collect=(AgentFileSelector(source),))
        except ValueError:
            pass
        else:
            raise AssertionError(f"unsafe selector was accepted: {source!r}")

    try:
        AgentTask(
            "write",
            collect=(AgentFileSelector("draft.md"),),
            publish=(
                AgentPublish("draft.md", "out.md"),
                AgentPublish("draft.md", "out.md"),
            ),
        )
    except ValueError as error:
        assert "duplicate" in str(error).lower()
    else:
        raise AssertionError("duplicate publish destination was accepted")


def test_agent_task_rejects_invalid_prompt_resolution_before_execution() -> None:
    with pytest.raises(AgentResultError, match="prompt resolution"):
        execute_agent_task(
            node_name="agent",
            run_id="run",
            task=AgentTask("work"),
            inputs={},
            declared_files=(),
            resolve=lambda path: path,
            artifacts_path=None,  # type: ignore[arg-type]
            blob_store=None,  # type: ignore[arg-type]
            adapter=None,  # type: ignore[arg-type]
            adapter_identity={},
            spec=None,  # type: ignore[arg-type]
            prompt_resolution={"prompt_resolution_schema": 1},
        )


class _RaisingAdapter:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def cache_identity(self) -> dict[str, str]:
        return {"adapter": "raising", "version": "runtime-subcodes"}

    def capabilities(self) -> AgentCapabilities:
        return AgentCapabilities()

    def run(self, request: Any, context: Any) -> Any:
        del request, context
        raise self.error


@pytest.mark.parametrize(
    ("kind", "runtime_code", "runtime_subcode"),
    (
        (
            "envelope",
            AgentRuntimeFailureCode.PROTOCOL,
            AgentRuntimeFailureSubCode.ENVELOPE,
        ),
        (
            "bridge",
            AgentRuntimeFailureCode.POLICY,
            AgentRuntimeFailureSubCode.BRIDGE_POLICY,
        ),
        (
            "submit",
            AgentRuntimeFailureCode.PROTOCOL,
            AgentRuntimeFailureSubCode.SUBMIT_CONTRACT,
        ),
        ("unknown", AgentRuntimeFailureCode.PROTOCOL, None),
    ),
)
def test_execute_agent_task_preserves_known_runtime_sources_and_redacts_unknown(
    tmp_path: Path,
    kind: str,
    runtime_code: AgentRuntimeFailureCode,
    runtime_subcode: AgentRuntimeFailureSubCode | None,
) -> None:
    if runtime_subcode is None:
        error: BaseException = RuntimeError("unknown-agent-secret")
    else:
        error = AgentRuntimeResultError(
            f"diagnostic-{kind}-unknown-agent-secret",
            runtime_subcode=runtime_subcode,
        )
    dag = _make_dag(tmp_path)
    spec = make_agent_spec(tmp_path / "agent")

    @dag.agent(
        "agent",
        adapter=_RaisingAdapter(error),
        spec=spec,
        cache="off",
    )
    def agent(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        del inputs, ctx
        return AgentTask("execute")

    with pytest.raises(AgentExecutionFailure) as raised:
        dag.run()

    failure = raised.value
    assert failure.runtime_code is runtime_code
    assert failure.runtime_subcode is runtime_subcode
    failure_path = next((tmp_path / "artifacts" / "runs").glob("*/failures/agent.json"))
    record = json.loads(failure_path.read_text(encoding="utf-8"))
    durable = json.dumps(record, ensure_ascii=False, sort_keys=True)
    assert "unknown-agent-secret" not in durable
    assert record["failure"]["runtime_code"] == runtime_code.value
    assert record["failure"]["runtime_subcode"] == (
        runtime_subcode.value if runtime_subcode is not None else None
    )
    assert failure.exception_type == type(error).__name__
    assert failure.message_digest == hashlib.sha256(str(error).encode()).hexdigest()
    assert record["failure"]["exception_type"] == type(error).__name__
    assert record["failure"]["message_digest"] == hashlib.sha256(str(error).encode()).hexdigest()
