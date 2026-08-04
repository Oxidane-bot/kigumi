from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from kigumi._runstate import AttemptStore, RunManifestError, StateIntegrityError
from kigumi.artifacts import sha
from kigumi.retry import AmbiguousAttemptError


def _store(tmp_path: Path) -> AttemptStore:
    store = AttemptStore(tmp_path / "run", {})
    store.initialize()
    return store


def _receipt_path(tmp_path: Path) -> Path:
    return tmp_path / "run" / "attempts" / sha("work") / "attempt-0001.json"


def _state_path(tmp_path: Path) -> Path:
    return tmp_path / "run" / "attempts" / sha("work") / "state.json"


def _store_with_missing_historical_receipt(tmp_path: Path) -> AttemptStore:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    store.record_failure("work", RuntimeError("terminal"), policy=None)
    recovery = {
        "recovery_time": "2026-08-04T12:00:00.000000Z",
        "from_attempt": 1,
        "to_attempt": 2,
        "decision": "retry_not_started",
        "reason": "the side effect never started",
        "evidence_refs": [],
        "recovered_by": "test",
    }
    store.schedule_recovery(
        "work",
        from_attempt=1,
        to_attempt=2,
        recovery=recovery,
        recovery_receipt=recovery,
        inherited_nodes={},
    )
    (tmp_path / "run" / "attempts" / sha("work") / "attempt-0001.json").unlink()
    return store


def test_corrupt_attempt_receipt_fails_prepare_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    receipt = _receipt_path(tmp_path)
    receipt.write_text("{not-json", encoding="utf-8")

    with pytest.raises(StateIntegrityError):
        store.prepare("work", policy=None, declaration_digest="decl")


def test_missing_attempt_receipt_is_not_started_case(tmp_path: Path) -> None:
    store = _store(tmp_path)

    prepared = store.prepare("work", policy=None, declaration_digest="decl")
    receipt = _receipt_path(tmp_path)
    receipt.unlink()

    resumed = store.prepare("work", policy=None, declaration_digest="decl")

    assert prepared["action"] == "run"
    assert resumed["action"] == "run"


def test_tampered_running_state_fails_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    store.mark_side_effect("work", {"kind": "provider"})

    state_path = _state_path(tmp_path)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["side_effect_started"] = False
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(StateIntegrityError):
        store.prepare("work", policy=None, declaration_digest="decl")


def test_tampered_attempt_receipt_fails_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")

    receipt = _receipt_path(tmp_path)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["status"] = "completed"
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(StateIntegrityError):
        store.prepare("work", policy=None, declaration_digest="decl")


def test_coordinated_state_and_receipt_rewrite_fails_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    store.mark_side_effect("work", {"kind": "provider"})

    state_path = _state_path(tmp_path)
    receipt_path = _receipt_path(tmp_path)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["side_effect_started"] = False
    state["state_sha256"] = sha(
        {key: value for key, value in state.items() if key != "state_sha256"}
    )
    payload = json.dumps(state)
    state_path.write_text(payload, encoding="utf-8")
    receipt_path.write_text(payload, encoding="utf-8")

    with pytest.raises(StateIntegrityError):
        store.prepare("work", policy=None, declaration_digest="decl")


def test_state_integrity_error_includes_path_and_parse_error(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    receipt = _receipt_path(tmp_path)
    receipt.write_text("{", encoding="utf-8")

    with pytest.raises(StateIntegrityError) as raised:
        store.prepare("work", policy=None, declaration_digest="decl")

    assert str(receipt) in str(raised.value)
    assert "Expecting property name" in str(raised.value)


def test_mark_resumed_is_atomic_across_processes(tmp_path: Path) -> None:
    _store(tmp_path)
    worker = tmp_path / "mark_resumed_worker.py"
    worker.write_text(
        """
import sys
import time
from pathlib import Path

import kigumi._runstate as runstate
from kigumi._runstate import AttemptStore

original_write = runstate.atomic_write_json

def slow_write(path, payload):
    if Path(path).name == "_run.json":
        time.sleep(0.01)
    original_write(path, payload)

runstate.atomic_write_json = slow_write
attempts = AttemptStore(Path(sys.argv[1]), {})
for _ in range(int(sys.argv[2])):
    attempts.mark_resumed()
""",
        encoding="utf-8",
    )
    processes = [
        subprocess.Popen(
            [sys.executable, str(worker), str(tmp_path / "run"), "12"],
            cwd=Path(__file__).resolve().parents[1],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    results = [process.communicate(timeout=30) for process in processes]
    assert all(process.returncode == 0 for process in processes), results

    manifest = json.loads((tmp_path / "run" / "_run.json").read_text(encoding="utf-8"))
    assert manifest["resume_count"] == 48


def test_live_target_lease_blocks_second_attempt_owner(tmp_path: Path) -> None:
    ready = tmp_path / "ready"
    release = tmp_path / "release"
    worker = tmp_path / "lease_worker.py"
    worker.write_text(
        """
import sys
import time
from pathlib import Path

from kigumi._runstate import AttemptStore

run_root = Path(sys.argv[1])
ready = Path(sys.argv[2])
release = Path(sys.argv[3])
attempts = AttemptStore(run_root, {})
attempts.initialize()
attempts.prepare("work", policy=None, declaration_digest="decl")
ready.touch()
while not release.exists():
    time.sleep(0.01)
""",
        encoding="utf-8",
    )
    process = subprocess.Popen(
        [sys.executable, str(worker), str(tmp_path / "run"), str(ready), str(release)],
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists(), process.communicate(timeout=5)

        second = AttemptStore(tmp_path / "run", {})
        second.initialize()
        with pytest.raises(RunManifestError, match="Target .* busy"):
            second.prepare("work", policy=None, declaration_digest="decl")
        with pytest.raises(RunManifestError, match="Target .* busy"):
            second.mark_side_effect("work", {"kind": "provider"})
        assert not list((tmp_path / "run").glob("*.lock"))
    finally:
        release.touch()
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, (stdout, stderr)


def test_target_lease_uses_one_stable_path_and_requires_active_owner_token(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    prepared = store.prepare("work", policy=None, declaration_digest="decl")

    owner_token = prepared["state"]["target_owner_token"]
    lock_path = store._target_lock_path("work")
    assert lock_path.name == f"{store._run_lock_path.name}.target-{sha('work')}.lock"
    assert owner_token not in lock_path.name

    state_path = _state_path(tmp_path)
    receipt_path = _receipt_path(tmp_path)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.pop("target_owner_token")
    state.pop("state_sha256")
    state["state_sha256"] = sha(state)
    receipt_path.write_text(json.dumps(state), encoding="utf-8")
    state_path.write_text(json.dumps(state), encoding="utf-8")
    manifest_path = tmp_path / "run" / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["attempt_receipt_chains"][sha("work")][-1]["state_sha256"] = state["state_sha256"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(StateIntegrityError, match="owner token"):
        store.prepare("work", policy=None, declaration_digest="decl")


def test_stale_executor_is_fenced_after_new_owner_and_manual_resolution(
    tmp_path: Path,
) -> None:
    first = _store(tmp_path)
    first.prepare("work", policy=None, declaration_digest="decl")
    first.mark_side_effect("work", {"kind": "provider"})
    first._release_target_lease("work")

    second = AttemptStore(tmp_path / "run", {})
    second.initialize()
    second.resolve(
        "work",
        attempt=1,
        action="retry",
        reason="operator confirmed the effect did not complete",
    )

    with pytest.raises(RunManifestError, match="lease|owner|state"):
        first.mark_completed("work", artifact_sha256="a" * 64)


def test_missing_current_receipt_after_side_effect_is_not_trusted_by_readers(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    store.mark_side_effect("work", {"kind": "provider"})
    _receipt_path(tmp_path).unlink()

    with pytest.raises(AmbiguousAttemptError):
        store.state_for("work")
    with pytest.raises(AmbiguousAttemptError):
        store.pending_retries()
    with pytest.raises(AmbiguousAttemptError):
        store.ambiguous_attempts()


def test_missing_historical_receipt_fails_closed_at_every_state_entrypoint(
    tmp_path: Path,
) -> None:
    store = _store_with_missing_historical_receipt(tmp_path)
    operations = [
        lambda: store.state_for("work"),
        store.pending_retries,
        store.ambiguous_attempts,
        lambda: store.prepare("work", policy=None, declaration_digest="decl"),
        lambda: store.mark_side_effect("work", {"kind": "provider"}),
        lambda: store.mark_checkpoint("work", "checkpoint"),
        lambda: store.save_candidate("work", {"value": "candidate"}),
        lambda: store.mark_completed("work", artifact_sha256="a" * 64),
        lambda: store.record_failure("work", RuntimeError("again"), policy=None),
        lambda: store.resolve(
            "work",
            attempt=2,
            action="retry",
            reason="operator confirmed the effect did not complete",
        ),
        lambda: store.schedule_recovery(
            "work",
            from_attempt=2,
            to_attempt=3,
            recovery={"decision": "retry_not_started"},
            inherited_nodes={},
            recovery_receipt={
                "recovery_time": "2026-08-04T12:00:01.000000Z",
                "decision": "retry_not_started",
            },
        ),
    ]

    for operation in operations:
        with pytest.raises(StateIntegrityError, match="historical attempt receipt"):
            operation()


def test_record_recovery_decision_makes_fail_and_retry_mutually_exclusive(
    tmp_path: Path,
) -> None:
    initial = _store(tmp_path)
    initial.prepare("work", policy=None, declaration_digest="decl")
    initial.record_failure("work", RuntimeError("terminal"), policy=None)
    barrier = threading.Barrier(2)

    def decide(decision: str) -> tuple[str, Any]:
        attempts = AttemptStore(tmp_path / "run", {})
        payload = {
            "recovery_time": f"2026-08-04T12:00:0{1 if decision == 'fail' else 2}.000000Z",
            "from_attempt": 1,
            "to_attempt": 1 if decision == "fail" else 2,
            "decision": decision,
            "reason": f"operator chose {decision}",
            "evidence_refs": [],
            "recovered_by": "test",
        }
        barrier.wait()
        try:
            state = attempts.record_recovery_decision(
                "work",
                from_attempt=1,
                decision=decision,
                recovery=payload,
                inherited_nodes={},
                recovery_receipt=payload,
            )
        except BaseException as error:
            return "error", error
        return "ok", state

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(decide, ("fail", "retry_not_started")))

    successes = [value for status, value in outcomes if status == "ok"]
    failures = [value for status, value in outcomes if status == "error"]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], (RunManifestError, ValueError))
    assert len(list((tmp_path / "run").glob("recovery-*.json"))) == 1

    state = json.loads(_state_path(tmp_path).read_text(encoding="utf-8"))
    if successes[0]["status"] == "retry_scheduled":
        assert state["attempt"] == 2
        assert (tmp_path / "run" / "attempts" / sha("work") / "attempt-0002.json").is_file()
    else:
        assert successes[0]["status"] == "failed"
        assert state["attempt"] == 1
        assert not (tmp_path / "run" / "attempts" / sha("work") / "attempt-0002.json").exists()
    assert state["recovery_receipt_file"]
    assert state["recovery_receipt_sha256"] == sha(
        json.loads((tmp_path / "run" / state["recovery_receipt_file"]).read_text(encoding="utf-8"))
    )


def test_recovery_receipt_is_bound_inside_schedule_recovery_transaction(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    store.record_failure("work", RuntimeError("terminal"), policy=None)
    payload = {
        "recovery_time": "2026-08-04T12:00:00.000000Z",
        "from_attempt": 1,
        "to_attempt": 2,
        "decision": "retry_not_started",
        "reason": "the side effect never started",
        "evidence_refs": [],
        "recovered_by": "test",
    }

    state = store.schedule_recovery(
        "work",
        from_attempt=1,
        to_attempt=2,
        recovery=payload,
        recovery_receipt=payload,
        inherited_nodes={},
    )

    receipt_path = tmp_path / "run" / state["recovery_receipt_file"]
    assert receipt_path.is_file()
    assert json.loads(receipt_path.read_text(encoding="utf-8")) == payload
    assert state["recovery_receipt_sha256"] == sha(payload)
    assert state["status"] == "retry_scheduled"
    receipt_path.unlink()
    with pytest.raises(StateIntegrityError, match="recovery receipt"):
        store.state_for("work")


def test_recovery_receipt_writer_is_exclusive_and_preserves_collisions(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    payload = {
        "recovery_time": "2026-08-04T12:00:00.000000Z",
        "from_attempt": 1,
        "to_attempt": 1,
        "decision": "fail",
        "reason": "operator rejected retry",
        "evidence_refs": [],
        "recovered_by": "test",
    }

    first = store.write_recovery_receipt(payload)
    second = store.write_recovery_receipt(payload)

    assert first != second
    assert first.read_text(encoding="utf-8") == second.read_text(encoding="utf-8")


def test_recovery_receipt_writer_is_exclusive_across_processes(tmp_path: Path) -> None:
    _store(tmp_path)
    worker = tmp_path / "recovery_receipt_worker.py"
    worker.write_text(
        """
import sys
from pathlib import Path

from kigumi._runstate import AttemptStore

payload = {
    "recovery_time": "2026-08-04T12:00:00.000000Z",
    "from_attempt": 1,
    "to_attempt": 1,
    "decision": "fail",
    "reason": "operator rejected retry",
    "evidence_refs": [],
    "recovered_by": "worker",
}
AttemptStore(Path(sys.argv[1]), {}).write_recovery_receipt(payload)
""",
        encoding="utf-8",
    )
    processes = [
        subprocess.Popen(
            [sys.executable, str(worker), str(tmp_path / "run")],
            cwd=Path(__file__).resolve().parents[1],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    results = [process.communicate(timeout=30) for process in processes]

    assert all(process.returncode == 0 for process in processes), results
    receipts = sorted((tmp_path / "run").glob("recovery-*.json"))
    assert len(receipts) == 4
    assert len({receipt.name for receipt in receipts}) == 4


def test_schedule_recovery_receipt_is_not_claimed_if_state_commit_crashes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    store.record_failure("work", RuntimeError("terminal"), policy=None)
    payload = {
        "recovery_time": "2026-08-04T12:00:00.000000Z",
        "from_attempt": 1,
        "to_attempt": 2,
        "decision": "retry_not_started",
        "reason": "the side effect never started",
        "evidence_refs": [],
        "recovered_by": "test",
    }

    def crash(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise OSError("crash while committing recovery state")

    monkeypatch.setattr(store, "_write_state", crash)
    with pytest.raises(OSError, match="committing recovery state"):
        store.schedule_recovery(
            "work",
            from_attempt=1,
            to_attempt=2,
            recovery=payload,
            recovery_receipt=payload,
            inherited_nodes={},
        )

    assert len(list((tmp_path / "run").glob("recovery-*.json"))) == 1
    assert not (tmp_path / "run" / "attempts" / sha("work") / "attempt-0002.json").exists()
    state = json.loads(_state_path(tmp_path).read_text(encoding="utf-8"))
    assert state["status"] == "failed"


def test_receipt_chain_is_monotonic_and_manifest_anchored(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    store.mark_side_effect("work", {"kind": "provider"})

    state = json.loads(_state_path(tmp_path).read_text(encoding="utf-8"))
    manifest = json.loads((tmp_path / "run" / "_run.json").read_text(encoding="utf-8"))
    chain = manifest["attempt_receipt_chains"][sha("work")]

    assert [entry["receipt_sequence"] for entry in chain] == [1, 2]
    assert chain[0]["previous_receipt_sha256"] is None
    assert chain[1]["previous_receipt_sha256"] == chain[0]["state_sha256"]
    assert state["receipt_sequence"] == chain[-1]["receipt_sequence"]
    assert state["state_sha256"] == chain[-1]["state_sha256"]
