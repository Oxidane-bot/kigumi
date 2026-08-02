from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from kigumi import AmbiguousAttemptError, LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.transport import Response


class _SequenceTransport:
    def __init__(self, outcomes: list[BaseException | Response]) -> None:
        self.outcomes = list(outcomes)
        self.requests = 0

    def resolve(self, model: str) -> str:
        return model

    def complete(self, messages: Any, model: str, **params: Any) -> Response:
        del messages, model, params
        self.requests += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _dag(tmp_path: Path, transport: _SequenceTransport) -> Dag:
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(transport, tmp_path / "llm"),
    )

    @dag.node("ask")
    def ask(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs
        return {"answer": ctx.call("hello")}

    return dag


def _state_path(tmp_path: Path, run_id: str) -> Path:
    return next((tmp_path / "artifacts" / "runs" / run_id / "attempts").glob("*/state.json"))


def test_node_without_retry_persists_side_effect_boundary(tmp_path: Path) -> None:
    dag = _dag(tmp_path, _SequenceTransport([Response("done", {}, "stop")]))

    dag.run(run_id="no-retry")

    state = json.loads(_state_path(tmp_path, "no-retry").read_text(encoding="utf-8"))
    assert state["side_effect_started"] is True


def test_provider_crash_without_retry_is_recovered_as_ambiguous(
    tmp_path: Path,
) -> None:
    dag = _dag(
        tmp_path,
        _SequenceTransport(
            [KeyboardInterrupt("provider disconnected"), Response("unused", {}, "stop")]
        ),
    )

    with pytest.raises(KeyboardInterrupt):
        dag.run(run_id="crashed")

    state_path = _state_path(tmp_path, "crashed")
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["side_effect_started"] is True

    with pytest.raises(AmbiguousAttemptError):
        dag.resume("crashed")
