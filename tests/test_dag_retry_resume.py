from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.error import HTTPError

import pytest

import kigumi._execution as execution_module
import kigumi._runstate as runstate_module
import kigumi.dag as dag_module
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
from kigumi._runstate import RunManifestError
from kigumi.agents import (
    AgentCapabilities,
    AgentCompletion,
    AgentRunResult,
    AgentTask,
)
from kigumi.artifacts import sha
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


def test_resume_rejects_post_validation_ordinary_run_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _SequenceTransport([Response("done", {"total_tokens": 1}, "stop")])
    dag = _retry_dag(
        tmp_path,
        transport,
        RetryPolicy(initial_delay_seconds=0, jitter="none"),
    )
    result = dag.run(run_id="ordinary-resume")
    run_path = tmp_path / "artifacts" / "runs" / result.run_id
    replacement = tmp_path / "external-resume"
    replacement.mkdir()
    forged_manifest = replacement / "_run.json"
    forged_manifest.write_text('{"status": "forged"}', encoding="utf-8")
    moved = tmp_path / "moved-resume"
    original_read = runstate_module.AttemptStore._read_owned_json
    swapped = False

    def read_then_replace(store: Any, path: Path) -> tuple[dict[str, Any] | None, bool]:
        nonlocal swapped
        value = original_read(store, path)
        if not swapped and Path(path) == run_path / "_run.json":
            swapped = True
            run_path.rename(moved)
            replacement.rename(run_path)
        return value

    monkeypatch.setattr(runstate_module.AttemptStore, "_read_owned_json", read_then_replace)

    with pytest.raises(RunManifestError, match="manifest|owned|durable"):
        dag.resume(result.run_id)

    assert swapped is True
    assert (run_path / "_run.json").read_text(encoding="utf-8") == '{"status": "forged"}'


def test_dag_manifest_prechecks_reject_external_manifest_symlink(tmp_path: Path) -> None:
    transport = _SequenceTransport([Response("done", {"total_tokens": 1}, "stop")])
    dag = _retry_dag(
        tmp_path,
        transport,
        RetryPolicy(max_attempts=1, initial_delay_seconds=0, jitter="none"),
    )
    dag.run(run_id="manifest-precheck")
    run_dir = tmp_path / "artifacts" / "runs" / "manifest-precheck"
    manifest_path = run_dir / "_run.json"
    original = manifest_path.read_bytes()
    external = tmp_path / "external-manifest.json"
    external.write_bytes(original)
    manifest_path.unlink()
    manifest_path.symlink_to(external)

    with pytest.raises(RunManifestError):
        dag.run(run_id="manifest-precheck")
    with pytest.raises(RunManifestError):
        dag.resume("manifest-precheck")
    with pytest.raises(ValueError):
        dag.recover(
            "manifest-precheck",
            "ask",
            from_attempt=1,
            decision="fail",
            reason="manifest must be read without following links",
        )
    with pytest.raises(RunManifestError):
        dag.retry_resolve(
            "manifest-precheck",
            "ask",
            attempt=1,
            action="fail",
            reason="manifest must be read without following links",
        )

    assert external.read_bytes() == original


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


def test_recovery_receipt_is_bound_to_the_scheduled_attempt_state(tmp_path: Path) -> None:
    transport = _SequenceTransport([_authentication_failure()])
    dag = _retry_dag(
        tmp_path,
        transport,
        RetryPolicy(max_attempts=1, initial_delay_seconds=0, jitter="none"),
    )

    with pytest.raises(ProviderFailure):
        dag.run(run_id="recovery-binding")

    receipt = dag.recover(
        "recovery-binding",
        "ask",
        from_attempt=1,
        decision="retry_not_started",
        reason="provider logs confirm the side effect never started",
    )
    run_dir = tmp_path / "artifacts" / "runs" / "recovery-binding"
    receipt_path = next(run_dir.glob("recovery-*.json"))
    receipt_payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    state_path = next((run_dir / "attempts").glob("*/state.json"))
    state = json.loads(state_path.read_text(encoding="utf-8"))

    assert receipt_payload["recovery_time"] == receipt.recovery_time
    assert state["status"] == "retry_scheduled"
    assert state["attempt"] == 2
    assert state["recovery"] == receipt_payload


def test_recover_wires_atomic_api_payload_and_inherited_nodes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    dag = Dag(config, LLMCaller(_SequenceTransport([]), tmp_path / "llm"))

    @dag.node("source", cache="off")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "source"}

    @dag.node(
        "ask",
        deps=("source",),
        cache="off",
        retry=RetryPolicy(max_attempts=1, initial_delay_seconds=0, jitter="none"),
    )
    def ask(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        raise RuntimeError("failed")

    with pytest.raises(RuntimeError, match="failed"):
        dag.run(run_id="atomic-api-wiring")

    calls: list[dict[str, Any]] = []
    original_record = dag_module.AttemptStore.record_recovery_decision

    def capture_record(self: Any, target: str, **kwargs: Any) -> dict[str, Any]:
        calls.append({"target": target, **kwargs})
        return original_record(self, target, **kwargs)

    monkeypatch.setattr(dag_module.AttemptStore, "record_recovery_decision", capture_record)

    receipt = dag.recover(
        "atomic-api-wiring",
        "ask",
        from_attempt=1,
        decision="retry_not_started",
        reason="the provider side effect was ruled out",
        evidence=["operator-log.txt"],
    )

    assert len(calls) == 1
    call = calls[0]
    assert call["target"] == "ask"
    assert call["from_attempt"] == 1
    assert call["decision"] == "retry_not_started"
    expected_payload = {
        "recovery_time": receipt.recovery_time,
        "from_attempt": 1,
        "to_attempt": 2,
        "decision": "retry_not_started",
        "reason": "the provider side effect was ruled out",
        "evidence_refs": ["operator-log.txt"],
        "recovered_by": receipt.recovered_by,
    }
    assert call["recovery"] == expected_payload
    assert call["recovery_receipt"] == expected_payload
    assert call["inherited_nodes"] == {
        "source": {
            "status": "inherited",
            "source": "source.json",
            "source_attempt": 1,
            "artifact_sha256": sha({"value": "source"}),
        }
    }


def test_concurrent_recover_has_one_receipt_and_one_queued_attempt(tmp_path: Path) -> None:
    first = _retry_dag(
        tmp_path,
        _SequenceTransport([_authentication_failure()]),
        RetryPolicy(max_attempts=1, initial_delay_seconds=0, jitter="none"),
    )
    with pytest.raises(ProviderFailure):
        first.run(run_id="concurrent-recovery")

    dags = [
        _retry_dag(
            tmp_path,
            _SequenceTransport([]),
            RetryPolicy(max_attempts=1, initial_delay_seconds=0, jitter="none"),
        )
        for _ in range(2)
    ]

    def recover(index: int) -> tuple[str, Any]:
        try:
            return (
                "ok",
                dags[index].recover(
                    "concurrent-recovery",
                    "ask",
                    from_attempt=1,
                    decision="retry_not_started",
                    reason=f"operator confirmation {index}",
                ),
            )
        except BaseException as error:
            return ("error", error)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [executor.submit(recover, index) for index in range(2)]
        outcomes = [future.result() for future in results]

    successes = [outcome for status, outcome in outcomes if status == "ok"]
    failures = [outcome for status, outcome in outcomes if status == "error"]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], (ValueError, RunManifestError))

    run_dir = tmp_path / "artifacts" / "runs" / "concurrent-recovery"
    assert len(list(run_dir.glob("recovery-*.json"))) == 1
    attempt_two = list((run_dir / "attempts").glob("*/attempt-0002.json"))
    assert len(attempt_two) == 1


def test_recover_rechecks_failed_manifest_inside_atomic_decision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = _retry_dag(
        tmp_path,
        _SequenceTransport([_authentication_failure()]),
        RetryPolicy(max_attempts=1, initial_delay_seconds=0, jitter="none"),
    )
    with pytest.raises(ProviderFailure):
        first.run(run_id="manifest-status-race")

    entered_atomic_decision = threading.Event()
    release_atomic_decision = threading.Event()
    original_record = dag_module.AttemptStore.record_recovery_decision

    def pause_before_atomic_decision(self: Any, target: str, **kwargs: Any) -> dict[str, Any]:
        entered_atomic_decision.set()
        release_atomic_decision.wait(timeout=5)
        return original_record(self, target, **kwargs)

    monkeypatch.setattr(
        dag_module.AttemptStore,
        "record_recovery_decision",
        pause_before_atomic_decision,
    )

    run_dir = tmp_path / "artifacts" / "runs" / "manifest-status-race"
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            first.recover,
            "manifest-status-race",
            "ask",
            1,
            "retry_not_started",
            "the side effect never started",
        )
        try:
            assert entered_atomic_decision.wait(timeout=5)
            dag_module.AttemptStore(run_dir, {}).update_manifest("running")
        finally:
            release_atomic_decision.set()

        with pytest.raises(ValueError, match="not in terminal failed state"):
            future.result(timeout=5)

    assert not list(run_dir.glob("recovery-*.json"))
    assert not list((run_dir / "attempts").glob("*/attempt-0002.json"))
    state_path = next((run_dir / "attempts").glob("*/state.json"))
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["status"] == "failed"
    assert state["attempt"] == 1
    manifest = json.loads((run_dir / "_run.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "running"
    assert manifest["recovery_decisions"] == {}


def test_mixed_fail_and_retry_recovery_is_one_atomic_decision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fail/retry race must commit only one recovery decision."""
    first = _retry_dag(
        tmp_path,
        _SequenceTransport([_authentication_failure()]),
        RetryPolicy(max_attempts=1, initial_delay_seconds=0, jitter="none"),
    )
    with pytest.raises(ProviderFailure):
        first.run(run_id="mixed-recovery")

    calls: list[dict[str, Any]] = []
    original_record = dag_module.AttemptStore.record_recovery_decision

    def capture_record(self: Any, target: str, **kwargs: Any) -> dict[str, Any]:
        calls.append({"target": target, **kwargs})
        return original_record(self, target, **kwargs)

    def reject_legacy_path(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise AssertionError("Dag.recover must use record_recovery_decision")

    def reject_preflight_state_for(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise AssertionError("Dag.recover must not preflight the target with state_for")

    monkeypatch.setattr(dag_module.AttemptStore, "record_recovery_decision", capture_record)
    monkeypatch.setattr(dag_module.AttemptStore, "write_recovery_receipt", reject_legacy_path)
    monkeypatch.setattr(dag_module.AttemptStore, "schedule_recovery", reject_legacy_path)
    monkeypatch.setattr(dag_module.AttemptStore, "state_for", reject_preflight_state_for)
    retry_dag = _retry_dag(
        tmp_path,
        _SequenceTransport([]),
        RetryPolicy(max_attempts=1, initial_delay_seconds=0, jitter="none"),
    )

    def run_fail() -> tuple[str, Any]:
        try:
            return (
                "ok",
                first.recover(
                    "mixed-recovery",
                    "ask",
                    from_attempt=1,
                    decision="fail",
                    reason="operator confirmed the failure is final",
                ),
            )
        except BaseException as error:
            return ("error", error)

    def run_retry() -> tuple[str, Any]:
        try:
            return (
                "ok",
                retry_dag.recover(
                    "mixed-recovery",
                    "ask",
                    from_attempt=1,
                    decision="retry_not_started",
                    reason="operator confirmed retry is safe",
                ),
            )
        except BaseException as error:
            return ("error", error)

    with ThreadPoolExecutor(max_workers=2) as executor:
        fail_future = executor.submit(run_fail)
        retry_future = executor.submit(run_retry)
        outcomes = [fail_future.result(timeout=5), retry_future.result(timeout=5)]

    successes = [value for status, value in outcomes if status == "ok"]
    failures = [value for status, value in outcomes if status == "error"]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], (ValueError, RunManifestError))
    assert len(calls) == 2
    assert {call["decision"] for call in calls} == {"fail", "retry_not_started"}

    run_dir = tmp_path / "artifacts" / "runs" / "mixed-recovery"
    assert len(list(run_dir.glob("recovery-*.json"))) == 1


def test_recovery_uses_atomic_api_after_injected_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    transport = _SequenceTransport([_authentication_failure()])
    dag = _retry_dag(
        tmp_path,
        transport,
        RetryPolicy(max_attempts=1, initial_delay_seconds=0, jitter="none"),
    )
    with pytest.raises(ProviderFailure):
        dag.run(run_id="recovery-crash")

    def reject_dag_level_write(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("Dag.recover must not write recovery receipts directly")

    monkeypatch.setattr(dag_module, "atomic_write_json", reject_dag_level_write)
    original_record = dag_module.AttemptStore.record_recovery_decision
    calls = 0

    def crash_after_decision(self: Any, target: str, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        original_record(self, target, **kwargs)
        raise KeyboardInterrupt("crash after recovery decision")

    monkeypatch.setattr(
        dag_module.AttemptStore,
        "record_recovery_decision",
        crash_after_decision,
    )
    with pytest.raises(KeyboardInterrupt, match="crash after recovery decision"):
        dag.recover(
            "recovery-crash",
            "ask",
            from_attempt=1,
            decision="fail",
            reason="operator confirmed the failure is final",
        )

    assert calls == 1
    run_dir = tmp_path / "artifacts" / "runs" / "recovery-crash"
    assert len(list(run_dir.glob("recovery-*.json"))) == 1
    assert not list((run_dir / "attempts").glob("*/attempt-0002.json"))


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


@pytest.mark.parametrize("changed_callable", ["aggregate", "key"])
def test_map_resume_binds_dynamic_callable_provenance(
    tmp_path: Path,
    changed_callable: str,
) -> None:
    def aggregate_v1(items: dict[str, dict[str, Any]], order: list[str]) -> dict[str, Any]:
        return {"count": len(order)}

    def aggregate_v2(items: dict[str, dict[str, Any]], order: list[str]) -> dict[str, Any]:
        return {"ids": order, "count": len(items)}

    def key_v1(item: dict[str, str]) -> str:
        return item["id"]

    def key_v2(item: dict[str, str]) -> str:
        return item["id"].strip()

    def build(
        transport: Any,
        *,
        aggregate_fn: Any,
        key_fn: Any,
    ) -> Dag:
        config = KigumiConfig(project_root=tmp_path, source_dirs=[])
        dag = Dag(config, LLMCaller(transport, tmp_path / "llm"))

        @dag.node("source", cache="off")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            del inputs, ctx
            return {"items": [{"id": "a"}, {"id": "b"}]}

        @dag.map(
            "mapped",
            items_from=("source", "items"),
            key_fn=key_fn,
            aggregate_fn=aggregate_fn,
            cache="off",
        )
        def mapped(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            del inputs
            return {"id": item["id"], "answer": ctx.call(item["id"], model="provider/model")}

        return dag

    first_transport = _SequenceTransport(
        [
            Response("a-result", {}, "stop"),
            Response("b-result", {}, "stop"),
        ]
    )
    first = build(first_transport, aggregate_fn=aggregate_v1, key_fn=key_v1).run(
        run_id="dynamic-callable"
    )
    assert first.run_status == "completed"

    changed_aggregate = aggregate_v2 if changed_callable == "aggregate" else aggregate_v1
    changed_key = key_v2 if changed_callable == "key" else key_v1
    changed = build(
        _SequenceTransport([]),
        aggregate_fn=changed_aggregate,
        key_fn=changed_key,
    )

    with pytest.raises(RunManifestError, match="declaration changed"):
        changed.resume("dynamic-callable")


def test_scan_resume_binds_aggregate_callable_provenance(tmp_path: Path) -> None:
    def aggregate_v1(items: dict[str, dict[str, Any]], order: list[str]) -> dict[str, Any]:
        return {"count": len(order)}

    def aggregate_v2(items: dict[str, dict[str, Any]], order: list[str]) -> dict[str, Any]:
        return {"ids": order, "count": len(items)}

    def build(transport: Any, aggregate_fn: Any) -> Dag:
        config = KigumiConfig(project_root=tmp_path, source_dirs=[])
        dag = Dag(config, LLMCaller(transport, tmp_path / "llm"))

        @dag.node("source", cache="off")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            del inputs, ctx
            return {"items": [{"id": "a"}, {"id": "b"}]}

        @dag.scan(
            "scanned",
            items_from=("source", "items"),
            key_fn=lambda item: item["id"],
            aggregate_fn=aggregate_fn,
            cache="off",
        )
        def scanned(
            item: dict[str, str],
            carry: Any,
            inputs: dict[str, Any],
            ctx: Any,
        ) -> dict[str, str]:
            del carry, inputs
            return {"id": item["id"], "answer": ctx.call(item["id"], model="provider/model")}

        return dag

    first = build(
        _SequenceTransport(
            [
                Response("a-result", {}, "stop"),
                Response("b-result", {}, "stop"),
            ]
        ),
        aggregate_v1,
    ).run(run_id="dynamic-scan-callable")
    assert first.run_status == "completed"

    changed = build(_SequenceTransport([]), aggregate_v2)
    with pytest.raises(RunManifestError, match="declaration changed"):
        changed.resume("dynamic-scan-callable")


@pytest.mark.parametrize("entry", ["resume", "run"])
def test_execution_entry_rejects_corrupt_workflow_profile_digest(
    tmp_path: Path,
    entry: str,
) -> None:
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(_SequenceTransport([]), tmp_path / "llm"),
    )

    @dag.node("work", cache="off")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    dag.run(run_id="profile-integrity")
    manifest_path = tmp_path / "artifacts" / "runs" / "profile-integrity" / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["workflow_profile"]["graph"]["nodes"][0]["cache"] = "tampered"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(RunManifestError, match="workflow_profile digest"):
        if entry == "resume":
            dag.resume("profile-integrity")
        else:
            dag.run(run_id="profile-integrity")


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
    with pytest.raises(RuntimeError, match="predates run manifest schema 2"):
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


def test_default_agent_crash_is_ambiguous_before_resume(tmp_path: Path) -> None:
    class Adapter:
        def __init__(self) -> None:
            self.runs = 0
            self.marker = tmp_path / "agent-effect.txt"

        def cache_identity(self) -> dict[str, object]:
            return {"adapter": "default-crash-agent", "version": 1}

        def capabilities(self) -> AgentCapabilities:
            return AgentCapabilities()

        def run(self, request: object, context: Any) -> AgentRunResult:
            del request, context
            self.runs += 1
            self.marker.write_text("external effect", encoding="utf-8")
            if self.runs == 1:
                raise KeyboardInterrupt("agent process stopped")
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
    )
    def agent(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        del inputs, ctx
        return AgentTask("crash")

    with pytest.raises(KeyboardInterrupt):
        dag.run(run_id="agent-default-crash")

    with pytest.raises(AmbiguousAttemptError):
        dag.resume("agent-default-crash")
    assert adapter.runs == 1


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
