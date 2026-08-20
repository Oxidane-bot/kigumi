from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from kigumi import (
    LLMCaller,
    ProviderFailure,
    ProviderFailureKind,
    ProviderFailureStage,
    RecoveryReceipt,
    RetryExhausted,
    RetryPolicy,
)
from kigumi._runstate import RunManifestError
from kigumi.artifacts import sha
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.transport import PreparedRequest


class _UnusedTransport:
    def cache_identity(self) -> dict[str, object]:
        return {"transport": "unused-recovery", "schema": 1}

    def prepare(
        self, messages: list[dict[str, Any]], model: str, params: dict[str, Any]
    ) -> PreparedRequest:
        return PreparedRequest(messages, model, params)

    def send(self, prepared: PreparedRequest) -> Any:
        del prepared
        raise AssertionError("recovery tests must not call a provider")


def _retryable_failure() -> ProviderFailure:
    return ProviderFailure(
        provider="recovery-test",
        stage=ProviderFailureStage.PROVIDER,
        kind=ProviderFailureKind.RATE_LIMIT,
        status_code=429,
        retry_after_ms=0,
        provider_request_id=None,
        message_digest="a" * 64,
        retryable_hint=True,
    )


def _dag(tmp_path: Path) -> Dag:
    return Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(_UnusedTransport(), tmp_path / "llm"),
    )


def _attempt_path(tmp_path: Path, run_id: str, target: str, attempt: int) -> Path:
    target_root = tmp_path / "artifacts" / "runs" / run_id / "attempts" / sha(target)
    return target_root / f"attempt-{attempt:04d}.json"


def _run_dag_cli(dag: Dag, argv: list[str]) -> int:
    with pytest.raises(SystemExit) as exited:
        dag.cli(argv)
    return int(exited.value.code)


def _terminal_failed_dag(tmp_path: Path, run_id: str = "cli-recovery") -> Dag:
    dag = _dag(tmp_path)

    @dag.node("work", cache="off")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        raise RuntimeError("terminal failure")

    with pytest.raises(RuntimeError, match="terminal failure"):
        dag.run(run_id=run_id)
    return dag


def test_dag_cli_recover_retry_writes_one_receipt_and_queues_one_retry(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    dag = _terminal_failed_dag(tmp_path)

    assert (
        _run_dag_cli(
            dag,
            [
                "recover",
                "cli-recovery",
                "work",
                "--attempt",
                "1",
                "--decision",
                "retry_not_started",
                "--reason",
                "the external side effect never started",
                "--evidence",
                "provider-log.txt",
                "--evidence",
                "ticket:PROJ-123",
            ],
        )
        == 0
    )
    output = capsys.readouterr().out

    receipts = sorted((tmp_path / "artifacts" / "runs" / "cli-recovery").glob("recovery-*.json"))
    assert len(receipts) == 1
    payload = json.loads(receipts[0].read_text())
    assert payload["from_attempt"] == 1
    assert payload["to_attempt"] == 2
    assert payload["decision"] == "retry_not_started"
    assert payload["evidence_refs"] == ["provider-log.txt", "ticket:PROJ-123"]
    assert (
        f"run=cli-recovery target=work from_attempt={payload['from_attempt']} "
        f"to_attempt={payload['to_attempt']} decision={payload['decision']} "
        f"evidence_count={len(payload['evidence_refs'])}"
    ) in output
    assert (
        json.loads(_attempt_path(tmp_path, "cli-recovery", "work", 2).read_text())["status"]
        == "retry_scheduled"
    )
    manifest = json.loads(
        (tmp_path / "artifacts" / "runs" / "cli-recovery" / "_run.json").read_text()
    )
    assert len(manifest["pending_retries"]) == 1
    assert manifest["pending_retries"][0]["target"] == "work"


def test_dag_cli_recover_fail_keeps_attempt_and_does_not_queue_retry(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    dag = _terminal_failed_dag(tmp_path, run_id="cli-fail")

    assert (
        _run_dag_cli(
            dag,
            [
                "recover",
                "cli-fail",
                "work",
                "--attempt",
                "1",
                "--decision",
                "fail",
                "--reason",
                "the final operator verdict",
            ],
        )
        == 0
    )
    output = capsys.readouterr().out
    receipts = sorted((tmp_path / "artifacts" / "runs" / "cli-fail").glob("recovery-*.json"))
    assert len(receipts) == 1
    payload = json.loads(receipts[0].read_text())
    assert payload["from_attempt"] == 1
    assert payload["to_attempt"] == payload["from_attempt"]
    assert payload["decision"] == "fail"
    assert "run=cli-fail target=work from_attempt=1 to_attempt=1" in output
    assert "decision=fail evidence_count=0" in output
    assert not _attempt_path(tmp_path, "cli-fail", "work", 2).exists()
    manifest = json.loads((tmp_path / "artifacts" / "runs" / "cli-fail" / "_run.json").read_text())
    assert manifest["status"] == "failed"
    assert manifest.get("pending_retries", []) == []


def test_dag_cli_recover_api_failure_is_an_error_without_success_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    dag = _terminal_failed_dag(tmp_path, run_id="cli-error")

    assert (
        _run_dag_cli(
            dag,
            [
                "recover",
                "cli-error",
                "work",
                "--attempt",
                "2",
                "--decision",
                "retry_not_started",
                "--reason",
                "wrong attempt should fail closed",
            ],
        )
        == 1
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("error: ")
    assert "not 2" in captured.err
    assert not list((tmp_path / "artifacts" / "runs" / "cli-error").glob("recovery-*.json"))


def test_failed_run_can_be_recovered_and_creates_new_attempt(tmp_path: Path) -> None:
    dag = _dag(tmp_path)
    should_fail = True
    executions = 0

    @dag.node("work", cache="off")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        nonlocal executions
        executions += 1
        if should_fail:
            raise RuntimeError("external tool unavailable")
        return {"status": "ok"}

    with pytest.raises(RuntimeError, match="external tool unavailable"):
        dag.run(run_id="recoverable")

    receipt = dag.recover(
        "recoverable",
        "work",
        1,
        "retry_not_started",
        "the failed process never crossed its external side-effect boundary",
    )

    assert isinstance(receipt, RecoveryReceipt)
    assert receipt.from_attempt == 1
    assert receipt.to_attempt == 2
    assert _attempt_path(tmp_path, "recoverable", "work", 2).is_file()

    should_fail = False
    result = dag.resume("recoverable")
    assert result.run_status == "completed"
    assert result.artifacts["work"] == {"status": "ok"}
    assert executions == 2


def test_old_attempts_are_preserved_across_recovery_retry(tmp_path: Path) -> None:
    dag = _dag(tmp_path)

    @dag.node(
        "work",
        cache="off",
        retry=RetryPolicy(max_attempts=2, initial_delay_seconds=0, jitter="none"),
    )
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        raise _retryable_failure()

    assert dag.run(run_id="append-only").run_status == "pending_retry"
    with pytest.raises(RetryExhausted):
        dag.resume("append-only")

    attempt_two = _attempt_path(tmp_path, "append-only", "work", 2)
    before_recovery = hashlib.sha256(attempt_two.read_bytes()).hexdigest()

    dag.recover(
        "append-only",
        "work",
        2,
        "retry_after_external_check",
        "the replacement binary was installed but the failure remains reproducible",
    )
    with pytest.raises(RetryExhausted):
        dag.resume("append-only")

    attempt_three = _attempt_path(tmp_path, "append-only", "work", 3)
    assert attempt_two.is_file()
    assert attempt_three.is_file()
    assert hashlib.sha256(attempt_two.read_bytes()).hexdigest() == before_recovery
    attempt_three_hash = hashlib.sha256(attempt_three.read_bytes()).hexdigest()
    dag.recover("append-only", "work", 3, "fail", "the third attempt is final")
    assert hashlib.sha256(attempt_three.read_bytes()).hexdigest() == attempt_three_hash
    assert json.loads(attempt_two.read_text(encoding="utf-8"))["attempt"] == 2
    assert json.loads(attempt_three.read_text(encoding="utf-8"))["attempt"] == 3


def test_recovery_receipt_persists_reason_evidence_and_identity(tmp_path: Path) -> None:
    dag = _dag(tmp_path)

    @dag.node("work", cache="off")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        raise RuntimeError("failed")

    with pytest.raises(RuntimeError, match="failed"):
        dag.run(run_id="receipt")

    receipt = dag.recover(
        "receipt",
        "work",
        1,
        "retry_after_external_check",
        "validated ffmpeg 7.1 against the failing scene",
        evidence=["validation-report.md", "https://ticket.example.com/PROJ-123"],
    )

    paths = sorted((tmp_path / "artifacts" / "runs" / "receipt").glob("recovery-*.json"))
    assert len(paths) == 1
    payload = json.loads(paths[0].read_text(encoding="utf-8"))
    assert payload == {
        "recovery_time": receipt.recovery_time,
        "from_attempt": 1,
        "to_attempt": 2,
        "decision": "retry_after_external_check",
        "reason": "validated ffmpeg 7.1 against the failing scene",
        "evidence_refs": [
            "validation-report.md",
            "https://ticket.example.com/PROJ-123",
        ],
        "recovered_by": receipt.recovered_by,
    }


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("run_manifest_schema", 1, "no valid schema-2 manifest"),
        ("targets", {"invalid": "bindings"}, "invalid target bindings"),
        ("force", {"invalid": "bindings"}, "invalid force bindings"),
    ],
)
def test_recover_rejects_untrusted_manifest_fields_with_run_manifest_error(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    run_id = f"untrusted-{field}"
    dag = _terminal_failed_dag(tmp_path, run_id=run_id)
    manifest_path = tmp_path / "artifacts" / "runs" / run_id / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest[field] = value
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(RunManifestError, match=message):
        dag.recover(run_id, "work", 1, "fail", "the durable manifest is untrusted")


def test_recover_keeps_value_error_for_trusted_preconditions(tmp_path: Path) -> None:
    successful = _dag(tmp_path / "successful")

    @successful.node("work")
    def successful_work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"status": "ok"}

    successful.run(run_id="done")
    with pytest.raises(ValueError, match="not in terminal failed state"):
        successful.recover("done", "work", 1, "retry_not_started", "not failed")

    failed = _terminal_failed_dag(tmp_path / "failed", run_id="failed")
    with pytest.raises(ValueError, match="not registered"):
        failed.recover("failed", "missing", 1, "fail", "target is not registered")


def test_recovery_inherits_successful_nodes_and_reruns_failed_branch_only(
    tmp_path: Path,
) -> None:
    dag = _dag(tmp_path)
    should_fail = True
    executions: dict[str, int] = {"A": 0, "B": 0, "C": 0}

    @dag.node("A", cache="off")
    def node_a(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        executions["A"] += 1
        return {"value": "a"}

    @dag.node("B", deps=("A",), cache="off")
    def node_b(inputs: dict[str, dict[str, str]], ctx: Any) -> dict[str, str]:
        del ctx
        executions["B"] += 1
        return {"value": inputs["A"]["value"] + "b"}

    @dag.node("C", deps=("B",), cache="off")
    def node_c(inputs: dict[str, dict[str, str]], ctx: Any) -> dict[str, str]:
        del ctx
        executions["C"] += 1
        if should_fail:
            raise RuntimeError("C is broken")
        return {"value": inputs["B"]["value"] + "c"}

    with pytest.raises(RuntimeError, match="C is broken"):
        dag.run(run_id="inheritance")

    dag.recover(
        "inheritance",
        "C",
        1,
        "retry_not_started",
        "C was fixed before its retry",
    )
    recovery_attempt = json.loads(
        _attempt_path(tmp_path, "inheritance", "C", 2).read_text(encoding="utf-8")
    )
    assert recovery_attempt["inherited_nodes"]["A"]["status"] == "inherited"
    assert recovery_attempt["inherited_nodes"]["B"]["status"] == "inherited"

    should_fail = False
    result = dag.resume("inheritance")
    assert result.run_status == "completed"
    assert executions == {"A": 1, "B": 1, "C": 2}


def test_invalid_recovery_is_rejected(tmp_path: Path) -> None:
    dag = _dag(tmp_path)

    with pytest.raises(ValueError, match="Run"):
        dag.recover(
            "missing",
            "work",
            1,
            "retry_not_started",
            "missing run",
        )

    @dag.node("work", cache="off")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        raise RuntimeError("failed")

    with pytest.raises(RuntimeError, match="failed"):
        dag.run(run_id="invalid")
    with pytest.raises(ValueError, match="attempt"):
        dag.recover("invalid", "work", 2, "retry_not_started", "wrong attempt")

    successful = _dag(tmp_path / "successful")

    @successful.node("work")
    def successful_work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"status": "ok"}

    successful.run(run_id="done")
    with pytest.raises(ValueError, match="terminal"):
        successful.recover("done", "work", 1, "retry_not_started", "not failed")
