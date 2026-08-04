from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from kigumi._runstate import SUCCESS_CANDIDATE_SCHEMA, AttemptStore, RunManifestError
from kigumi.artifacts import atomic_write_json, canonical_json, sha, write_artifact
from kigumi.inspect import durable_run_state, trace_run
from kigumi.profile import WorkflowProfileError, load_run_profile


def _profile() -> dict[str, Any]:
    return {
        "workflow_profile_schema": 2,
        "mode": "static",
        "resolution_status": "unresolved",
        "graph": {"nodes": [], "edges": [], "mounts": [], "models": {}},
        "prompts": {"specs": []},
        "run": None,
    }


def _add_profile(run_path: Path) -> None:
    manifest_path = run_path / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    static = _profile()
    manifest["workflow_profile"] = static
    manifest["workflow_profile_digest"] = sha(static)
    atomic_write_json(manifest_path, manifest)


def _recovery_payload(decision: str, to_attempt: int) -> dict[str, Any]:
    return {
        "recovery_time": f"2026-08-04T12:00:0{to_attempt}.000000Z",
        "from_attempt": 1,
        "to_attempt": to_attempt,
        "decision": decision,
        "reason": "the side effect did not complete",
        "evidence_refs": [],
        "recovered_by": "inspect-profile-test",
    }


def _make_run(
    tmp_path: Path,
    *,
    recovery: bool,
    run_path: Path | None = None,
) -> tuple[Path, AttemptStore]:
    run_path = run_path or (tmp_path / "run")
    store = AttemptStore(run_path, {})
    store.initialize()
    store.prepare("work", policy=None, declaration_digest="decl")
    if recovery:
        store.record_failure("work", RuntimeError("terminal"), policy=None)
        store.update_manifest("failed")
        payload = _recovery_payload("retry_not_started", 2)
        store.schedule_recovery(
            "work",
            from_attempt=1,
            to_attempt=2,
            recovery=payload,
            recovery_receipt=payload,
            inherited_nodes={},
        )
    _add_profile(run_path)
    return run_path, store


def _make_completed_run(
    tmp_path: Path, *, run_path: Path | None = None
) -> tuple[Path, AttemptStore]:
    run_path = run_path or (tmp_path / "run")
    store = AttemptStore(run_path, {})
    store.initialize()
    store.prepare("work", policy=None, declaration_digest="decl")
    artifact = {"value": "ok"}
    store.save_candidate(
        "work",
        {
            "candidate_schema": SUCCESS_CANDIDATE_SCHEMA,
            "artifact": artifact,
            "prompt_resolutions": {},
            "calls": [],
        },
    )
    artifact_digest = sha(artifact)
    origin = {"artifact_sha256": artifact_digest}
    write_artifact(
        run_path / "work.json",
        canonical_json(artifact),
        {
            "run_sidecar_schema": 2,
            "node": "work",
            "cache": "miss",
            "cache_key": "cache-key",
            "calls": [],
            "prompt_resolutions": {},
            "prompt_resolutions_digest": sha({}),
            "origin_provenance": origin,
            "origin_provenance_digest": sha(origin),
            "artifact_sha256": artifact_digest,
        },
    )
    store.mark_completed("work", artifact_sha256=artifact_digest)
    _add_profile(run_path)
    return run_path, store


def _readers() -> tuple[Callable[[Path], dict[str, Any]], ...]:
    return (durable_run_state, load_run_profile)


def test_inspect_and_profile_accept_a_valid_non_recovery_run(tmp_path: Path) -> None:
    run_path, _store = _make_run(tmp_path, recovery=False)

    for reader in _readers():
        result = reader(run_path)
        if reader is durable_run_state:
            assert result["resolution_status"] == "available"
        else:
            assert result["run"]["attempts"][0]["target"] == "work"


def test_inspect_and_profile_fail_closed_when_historical_receipt_is_deleted(
    tmp_path: Path,
) -> None:
    run_path, _store = _make_run(tmp_path, recovery=True)
    historical_receipt = run_path / "attempts" / sha("work") / "attempt-0001.json"
    historical_receipt.unlink()

    for reader in _readers():
        with pytest.raises(WorkflowProfileError, match="historical attempt receipt"):
            reader(run_path)


def test_inspect_and_profile_fail_closed_when_recovery_ledger_is_tampered(
    tmp_path: Path,
) -> None:
    run_path, _store = _make_run(tmp_path, recovery=True)
    manifest_path = run_path / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["recovery_decisions"][sha("work")][0]["decision"] = "fail"
    manifest["recovery_decisions_sha256"] = sha(manifest["recovery_decisions"])
    atomic_write_json(manifest_path, manifest)

    for reader in _readers():
        with pytest.raises(WorkflowProfileError, match="ledger|receipt"):
            reader(run_path)


def test_inspect_and_profile_fail_closed_when_recovery_ledger_digest_does_not_match(
    tmp_path: Path,
) -> None:
    run_path, _store = _make_run(tmp_path, recovery=True)
    manifest_path = run_path / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["recovery_decisions"][sha("work")][0]["decision"] = "fail"
    atomic_write_json(manifest_path, manifest)

    for reader in _readers():
        with pytest.raises(WorkflowProfileError, match="ledger digest"):
            reader(run_path)


def test_inspect_and_profile_reject_a_current_run_with_both_integrity_anchors_removed(
    tmp_path: Path,
) -> None:
    run_path, _store = _make_run(tmp_path, recovery=False)
    manifest_path = run_path / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("attempt_receipt_chains")
    manifest.pop("recovery_decisions")
    manifest.pop("recovery_decisions_sha256")
    atomic_write_json(manifest_path, manifest)

    for reader in _readers():
        with pytest.raises(WorkflowProfileError, match="attempt_receipt_chains|ledger"):
            reader(run_path)


def test_trace_run_fails_closed_on_the_same_untrusted_durable_state(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    run_path, _store = _make_run(
        tmp_path,
        recovery=True,
        run_path=artifacts / "runs" / "trace",
    )
    (run_path / "attempts" / sha("work") / "attempt-0001.json").unlink()

    with pytest.raises(WorkflowProfileError, match="historical attempt receipt"):
        trace_run(artifacts, tmp_path / "llm", "trace")


@pytest.mark.parametrize("broken", ["receipt", "candidate", "artifact", "sidecar"])
def test_durable_readers_share_fail_closed_materialization_validation(
    tmp_path: Path,
    broken: str,
) -> None:
    artifacts = tmp_path / "artifacts"
    run_path, _store = _make_completed_run(
        tmp_path,
        run_path=artifacts / "runs" / "integrity",
    )
    attempt_root = run_path / "attempts" / sha("work")
    if broken == "receipt":
        (attempt_root / "attempt-0001.json").unlink()
    elif broken == "candidate":
        (attempt_root / "candidate-0001.json").unlink()
    elif broken == "artifact":
        (run_path / "work.json").write_text('{"value":"tampered"}', encoding="utf-8")
    else:
        (run_path / "work.json.meta.json").unlink()

    readers = (
        lambda: durable_run_state(run_path),
        lambda: load_run_profile(run_path),
        lambda: trace_run(artifacts, tmp_path / "llm", "integrity"),
    )
    for reader in readers:
        with pytest.raises(WorkflowProfileError):
            reader()


def test_resume_prepare_fails_closed_when_completed_sidecar_is_missing(
    tmp_path: Path,
) -> None:
    run_path, store = _make_completed_run(tmp_path)
    (run_path / "work.json.meta.json").unlink()

    with pytest.raises(RunManifestError, match="run sidecar is missing"):
        store.prepare("work", policy=None, declaration_digest="decl")
