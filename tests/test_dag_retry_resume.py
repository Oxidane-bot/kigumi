from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError

import pytest

import kigumi._execution as execution_module
from kigumi import (
    AmbiguousAttemptError,
    EvidencePolicy,
    LLMCaller,
    ProviderFailure,
    ProviderFailureKind,
    ProviderFailureStage,
    RetryExhausted,
    RetryPolicy,
)
from kigumi.agents import (
    AgentCapabilities,
    AgentCompletion,
    AgentRunResult,
    AgentTask,
)
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.transport import Response
from tests._agent_helpers import make_agent_spec


class _SequenceTransport:
    def __init__(self, outcomes: list[BaseException | Response]) -> None:
        self.outcomes = list(outcomes)
        self.requests = 0

    def resolve(self, model: str) -> str:
        return model

    def complete(self, messages, model: str, **params) -> Response:
        del messages, model, params
        self.requests += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _retry_dag(tmp_path: Path, transport: Any, policy: RetryPolicy) -> Dag:
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    dag = Dag(
        config,
        LLMCaller(
            transport,
            tmp_path / "llm",
            evidence_policy=EvidencePolicy(response="redacted"),
        ),
    )

    @dag.node("ask", retry=policy)
    def ask(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs
        return {"answer": ctx.call("hello", model="provider/model")}

    return dag


def _rate_limit_failure() -> ProviderFailure:
    return ProviderFailure(
        provider="test",
        stage=ProviderFailureStage.PROVIDER,
        kind=ProviderFailureKind.RATE_LIMIT,
        status_code=429,
        retry_after_ms=0,
        provider_request_id=None,
        message_digest="b" * 64,
        retryable_hint=None,
    )


def _authentication_failure() -> ProviderFailure:
    return ProviderFailure(
        provider="test",
        stage=ProviderFailureStage.PROVIDER,
        kind=ProviderFailureKind.AUTHENTICATION,
        status_code=401,
        retry_after_ms=None,
        provider_request_id=None,
        message_digest="c" * 64,
        retryable_hint=False,
    )


def test_retry_is_durable_pending_and_resume_runs_only_when_due(tmp_path: Path) -> None:
    transport = _SequenceTransport(
        [
            HTTPError("https://provider.invalid", 429, "untrusted", {"Retry-After": "0"}, None),
            Response("done", {"total_tokens": 1}, "stop"),
        ]
    )
    dag = _retry_dag(
        tmp_path,
        transport,
        RetryPolicy(initial_delay_seconds=0, jitter="none"),
    )

    first = dag.run(run_id="durable")

    assert first.pending_retries == ["ask"]
    assert first.run_status == "pending_retry"
    assert transport.requests == 1
    completed = dag.resume("durable")
    assert completed.artifacts["ask"] == {"answer": "done"}
    assert completed.run_status == "completed"
    assert transport.requests == 2
    attempts = tmp_path / "artifacts" / "runs" / "durable" / "attempts"
    target = next(attempts.iterdir())
    assert json.loads((target / "attempt-0001.json").read_text())["status"] == "retry_scheduled"
    assert json.loads((target / "attempt-0002.json").read_text())["status"] == "completed"


def test_resume_before_retry_due_does_not_sleep_or_request_provider(
    tmp_path: Path,
) -> None:
    delayed = _rate_limit_failure()
    delayed = ProviderFailure(
        provider=delayed.provider,
        stage=delayed.stage,
        kind=delayed.kind,
        status_code=delayed.status_code,
        retry_after_ms=60_000,
        provider_request_id=delayed.provider_request_id,
        message_digest=delayed.message_digest,
        retryable_hint=delayed.retryable_hint,
    )
    transport = _SequenceTransport([delayed])
    dag = _retry_dag(
        tmp_path,
        transport,
        RetryPolicy(initial_delay_seconds=0, jitter="none"),
    )

    first = dag.run(run_id="not-due")
    resumed = dag.resume("not-due")

    assert first.run_status == "pending_retry"
    assert resumed.run_status == "pending_retry"
    assert resumed.pending_retries == ["ask"]
    assert transport.requests == 1


def test_retry_exhaustion_is_typed_and_marks_run_failed(tmp_path: Path) -> None:
    transport = _SequenceTransport([_rate_limit_failure(), _rate_limit_failure()])
    dag = _retry_dag(
        tmp_path,
        transport,
        RetryPolicy(max_attempts=2, initial_delay_seconds=0, jitter="none"),
    )

    assert dag.run(run_id="exhausted").run_status == "pending_retry"
    with pytest.raises(RetryExhausted) as raised:
        dag.resume("exhausted")

    assert raised.value.attempts == 2
    assert transport.requests == 2
    manifest = json.loads((tmp_path / "artifacts" / "runs" / "exhausted" / "_run.json").read_text())
    assert manifest["status"] == "failed"
    assert manifest["failure"]["failure_type"] == "runtime"
    assert manifest["failure"]["exception_type"] == "RetryExhausted"


def test_non_retryable_provider_failure_is_terminal_on_first_attempt(
    tmp_path: Path,
) -> None:
    transport = _SequenceTransport([_authentication_failure()])
    dag = _retry_dag(
        tmp_path,
        transport,
        RetryPolicy(max_attempts=3, initial_delay_seconds=0, jitter="none"),
    )

    with pytest.raises(ProviderFailure) as raised:
        dag.run(run_id="authentication")

    assert raised.value.kind is ProviderFailureKind.AUTHENTICATION
    assert transport.requests == 1
    state_path = next(
        (tmp_path / "artifacts" / "runs" / "authentication" / "attempts").glob("*/state.json")
    )
    assert json.loads(state_path.read_text())["status"] == "failed"


def test_call_node_evidence_miss_rebuilds_from_l1_without_provider_request(
    tmp_path: Path,
) -> None:
    transport = _SequenceTransport([Response("answer", {}, "stop")])

    def build(policy: EvidencePolicy) -> Dag:
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=[]),
            LLMCaller(
                transport,
                tmp_path / "llm",
                evidence_policy=policy,
            ),
        )

        @dag.node("ask")
        def ask(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"answer": ctx.call("hello")}

        return dag

    first = build(EvidencePolicy()).run()
    second = build(EvidencePolicy(request="redacted", response="hash_only")).run()
    assert transport.requests == 1
    assert second.cache_hits == []
    first_meta = json.loads(
        (tmp_path / "artifacts" / "runs" / first.run_id / "ask.json.meta.json").read_text()
    )
    second_meta = json.loads(
        (tmp_path / "artifacts" / "runs" / second.run_id / "ask.json.meta.json").read_text()
    )
    assert first_meta["cache_key"] == second_meta["cache_key"]
    assert second_meta["calls"][0]["cache"] == "hit"
    assert second_meta["calls"][0]["response_evidence"]["mode"] == "hash_only"


def test_durable_retry_rejects_hidden_transport_retries_before_side_effect(
    tmp_path: Path,
) -> None:
    class HiddenRetryTransport(_SequenceTransport):
        max_retries = 1
        max_length_retries = 0
        max_empty_retries = 0

    transport = HiddenRetryTransport([Response("must not happen", {}, "stop")])
    dag = _retry_dag(tmp_path, transport, RetryPolicy(initial_delay_seconds=0))

    with pytest.raises(ProviderFailure) as raised:
        dag.run(run_id="unsafe")

    assert raised.value.kind is ProviderFailureKind.UNKNOWN
    assert transport.requests == 0
    state = next((tmp_path / "artifacts" / "runs" / "unsafe" / "attempts").glob("*/state.json"))
    payload = json.loads(state.read_text())
    assert payload["side_effect_started"] is False
    assert payload["status"] == "failed"


@pytest.mark.parametrize("cache_policy", ["auto", "off"])
def test_success_candidate_resumes_without_reexecuting_node(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cache_policy: str
) -> None:
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    dag = Dag(config, LLMCaller(_SequenceTransport([]), tmp_path / "llm"))
    executions = 0

    @dag.node("work", cache=cache_policy, retry=RetryPolicy(max_attempts=1))
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        nonlocal executions
        executions += 1
        return {"value": 1}

    original = execution_module.ExecutionEnvelope.materialize
    crashed = False

    def crash_once(self, label, artifact, *, allow_item_owners=False):
        nonlocal crashed
        if not crashed:
            crashed = True
            raise RuntimeError("crash after candidate")
        return original(
            self,
            label,
            artifact,
            allow_item_owners=allow_item_owners,
        )

    monkeypatch.setattr(execution_module.ExecutionEnvelope, "materialize", crash_once)
    with pytest.raises(RuntimeError, match="crash after candidate"):
        dag.run(run_id="candidate")
    assert executions == 1

    resumed = dag.resume("candidate")
    assert resumed.artifacts["work"] == {"value": 1}
    assert executions == 1


def test_resume_reuses_run_local_cache_hit_after_l3_entry_is_removed(
    tmp_path: Path,
) -> None:
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(_SequenceTransport([]), tmp_path / "llm"),
    )
    executions = 0

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        nonlocal executions
        executions += 1
        return {"value": executions}

    assert dag.run(run_id="prime").artifacts["work"] == {"value": 1}
    replay = dag.run(run_id="replay")
    assert replay.cache_hits == ["work"]
    sidecar = json.loads(
        (tmp_path / "artifacts" / "runs" / "replay" / "work.json.meta.json").read_text()
    )
    cache_path = tmp_path / "artifacts" / "_cache" / "nodes" / f"{sidecar['cache_key']}.json"
    cache_path.unlink()

    resumed = dag.resume("replay")
    assert resumed.artifacts["work"] == {"value": 1}
    assert executions == 1


def test_side_effect_crash_is_ambiguous_until_explicit_resolution(tmp_path: Path) -> None:
    transport = _SequenceTransport([KeyboardInterrupt(), Response("resolved", {}, "stop")])
    dag = _retry_dag(
        tmp_path,
        transport,
        RetryPolicy(initial_delay_seconds=0, jitter="none"),
    )

    with pytest.raises(KeyboardInterrupt):
        dag.run(run_id="ambiguous")
    with pytest.raises(AmbiguousAttemptError):
        dag.resume("ambiguous")
    manifest_path = tmp_path / "artifacts" / "runs" / "ambiguous" / "_run.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["status"] == "ambiguous"
    assert manifest["ambiguous_attempts"][0]["target"] == "ask"
    assert "failure" not in manifest

    dag.retry_resolve(
        "ambiguous",
        "ask",
        attempt=1,
        action="retry",
        reason="operator verified no provider result was accepted",
    )
    completed = dag.resume("ambiguous")
    assert completed.artifacts["ask"] == {"answer": "resolved"}
    assert transport.requests == 2


def test_resume_fails_closed_when_declaration_changes(tmp_path: Path) -> None:
    transport = _SequenceTransport([Response("done", {}, "stop")])
    first = _retry_dag(tmp_path, transport, RetryPolicy(max_attempts=1))
    assert first.run(run_id="bound").run_status == "completed"

    changed = _retry_dag(tmp_path, transport, RetryPolicy(max_attempts=2))
    with pytest.raises(RuntimeError, match="declaration changed"):
        changed.resume("bound")


def test_legacy_run_without_manifest_is_read_only_and_cannot_resume(
    tmp_path: Path,
) -> None:
    dag = _retry_dag(
        tmp_path,
        _SequenceTransport([Response("unused", {}, "stop")]),
        RetryPolicy(max_attempts=1),
    )
    run = tmp_path / "artifacts" / "runs" / "legacy"
    run.mkdir(parents=True)
    (run / "ask.json").write_text('{"answer":"historical"}', encoding="utf-8")

    with pytest.raises(RuntimeError, match="cannot be resumed"):
        dag.resume("legacy")
    with pytest.raises(RuntimeError, match="predates run manifest schema 1"):
        dag.run(run_id="legacy")


def test_agent_provider_failure_retries_with_a_fresh_workspace(tmp_path: Path) -> None:
    class Adapter:
        def __init__(self) -> None:
            self.runs = 0
            self.workspaces: list[Path] = []

        def cache_identity(self) -> dict[str, object]:
            return {"adapter": "retry-agent", "version": 1}

        def capabilities(self) -> AgentCapabilities:
            return AgentCapabilities()

        def run(self, request: object, context: Any) -> AgentRunResult:
            del request
            self.runs += 1
            self.workspaces.append(context.workspace)
            if self.runs == 1:
                raise _rate_limit_failure()
            return AgentRunResult(AgentCompletion("completed", "done"))

    adapter = Adapter()
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(_SequenceTransport([]), tmp_path / "llm"),
    )

    @dag.agent(
        "agent",
        adapter=adapter,
        spec=make_agent_spec(tmp_path / "agent-spec"),
        retry=RetryPolicy(initial_delay_seconds=0, jitter="none"),
    )
    def agent(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask("retry")

    first = dag.run(run_id="agent-retry")
    assert first.pending_retries == ["agent"]
    completed = dag.resume("agent-retry")
    assert completed.run_status == "completed"
    assert adapter.runs == 2
    assert adapter.workspaces[0] != adapter.workspaces[1]
    assert all(not path.exists() for path in adapter.workspaces)


def test_map_retries_only_failed_item_and_reuses_cache_off_sibling(
    tmp_path: Path,
) -> None:
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(_SequenceTransport([]), tmp_path / "llm"),
    )
    attempts: dict[str, int] = {}

    @dag.node("source", cache="off")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.map(
        "mapped",
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
        cache="off",
        retry=RetryPolicy(initial_delay_seconds=0, jitter="none"),
    )
    def mapped(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        attempts[item["id"]] = attempts.get(item["id"], 0) + 1
        if item["id"] == "a" and attempts[item["id"]] == 1:
            raise _rate_limit_failure()
        return {"id": item["id"]}

    first = dag.run(run_id="map-retry", workers=2)
    assert first.pending_retries == ["mapped@a"]
    assert attempts == {"a": 1, "b": 1}

    resumed = dag.resume("map-retry", workers=2)
    assert resumed.artifacts["mapped"]["count"] == 2
    assert attempts == {"a": 2, "b": 1}


def test_scan_retry_reuses_verified_prefix_and_leaves_suffix_unexecuted(
    tmp_path: Path,
) -> None:
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(_SequenceTransport([]), tmp_path / "llm"),
    )
    attempts: dict[str, int] = {}

    @dag.node("source", cache="off")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "a"}, {"id": "b"}, {"id": "c"}]}

    @dag.scan(
        "chain",
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
        carry_fn=lambda artifact: artifact["carry"],
        cache="off",
        retry=RetryPolicy(initial_delay_seconds=0, jitter="none"),
    )
    def chain(
        item: dict[str, str],
        carry: dict[str, int] | None,
        inputs: dict[str, Any],
        ctx: Any,
    ) -> dict[str, Any]:
        attempts[item["id"]] = attempts.get(item["id"], 0) + 1
        if item["id"] == "b" and attempts[item["id"]] == 1:
            raise _rate_limit_failure()
        total = (carry or {"total": 0})["total"] + 1
        return {"id": item["id"], "carry": {"total": total}}

    first = dag.run(run_id="scan-retry")
    assert first.pending_retries == ["chain@b"]
    assert attempts == {"a": 1, "b": 1}

    resumed = dag.resume("scan-retry")
    assert resumed.artifacts["chain"]["items"]["c"]["carry"]["total"] == 3
    assert attempts == {"a": 1, "b": 2, "c": 1}


def _run_dag_cli(dag: Dag, argv: list[str]) -> int:
    with pytest.raises(SystemExit) as exited:
        dag.cli(argv)
    return int(exited.value.code)


def test_dag_cli_resolves_ambiguous_attempt_and_resumes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    transport = _SequenceTransport([KeyboardInterrupt(), Response("resolved-by-cli", {}, "stop")])
    dag = _retry_dag(
        tmp_path,
        transport,
        RetryPolicy(initial_delay_seconds=0, jitter="none"),
    )
    with pytest.raises(KeyboardInterrupt):
        dag.run(run_id="cli-ambiguous")

    assert (
        _run_dag_cli(
            dag,
            [
                "retry-resolve",
                "cli-ambiguous",
                "ask",
                "--attempt",
                "1",
                "--action",
                "retry",
                "--reason",
                "operator checked provider logs",
            ],
        )
        == 0
    )
    assert "resolved ask attempt=1 action=retry" in capsys.readouterr().out
    assert _run_dag_cli(dag, ["resume", "cli-ambiguous"]) == 0
    output = capsys.readouterr().out
    assert "status=completed" in output
    assert "run=cli-ambiguous" in output


def test_graph_shows_retry_attempt_runtime(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    dag = _retry_dag(
        tmp_path,
        _SequenceTransport([_rate_limit_failure()]),
        RetryPolicy(initial_delay_seconds=60, jitter="none"),
    )
    result = dag.run(run_id="graph-retry")
    assert result.run_status == "pending_retry"

    assert _run_dag_cli(dag, ["graph", "--run-id", "graph-retry"]) == 0
    output = capsys.readouterr().out
    assert "retry_pending" in output
    assert "attempt=1" in output
    assert "due=" in output
    assert "failure=rate_limit" in output
