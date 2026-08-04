from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from kigumi._runstate import (
    SUCCESS_CANDIDATE_SCHEMA,
    AttemptStore,
    RunManifestError,
    StateIntegrityError,
)
from kigumi.artifacts import atomic_write_json, canonical_json, sha, write_artifact
from kigumi.inspect import diff_components, durable_run_state, trace_run
from kigumi.profile import WorkflowProfileError, load_run_profile
from tests._dag_helpers import _make_dag


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
            "cache_key": "cache-key",
            "key_components": {"source": "test"},
            "prompt_resolutions": {},
            "calls": [],
        },
    )
    artifact_digest = sha(artifact)
    origin = {
        "kind": "code",
        "artifact_sha256": artifact_digest,
        "calls": [],
        "agent": None,
        "prompt_resolutions": {},
        "prompt_sha256": None,
        "model": None,
        "params": {},
        "provider_response_id": None,
        "usage": None,
        "evidence_policy": {},
        "evidence_policy_digest": sha({}),
    }
    write_artifact(
        run_path / "work.json",
        canonical_json(artifact),
        {
            "run_sidecar_schema": 2,
            "node": "work",
            "cache": "miss",
            "cache_policy": "off",
            "cache_key": "cache-key",
            "key_components": {"source": "test"},
            "outputs": [],
            "calls": [],
            "execution_calls": [],
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


@pytest.mark.parametrize("symlink_kind", ["run", "runs"])
def test_all_read_surfaces_reject_symlinked_run_ownership(
    tmp_path: Path,
    symlink_kind: str,
) -> None:
    artifacts = tmp_path / "artifacts"
    external_runs = tmp_path / "external-runs"
    run_path, _store = _make_run(
        tmp_path,
        recovery=False,
        run_path=external_runs / "owned",
    )
    if symlink_kind == "run":
        runs = artifacts / "runs"
        runs.mkdir(parents=True)
        (runs / "owned").symlink_to(run_path, target_is_directory=True)
    else:
        artifacts.mkdir(parents=True)
        (artifacts / "runs").symlink_to(external_runs, target_is_directory=True)

    run_id = "owned"
    run_view = artifacts / "runs" / run_id
    readers = (
        lambda: durable_run_state(run_view),
        lambda: load_run_profile(run_view),
        lambda: trace_run(artifacts, tmp_path / "llm", run_id),
        lambda: diff_components(artifacts, run_id, run_id),
    )
    for reader in readers:
        with pytest.raises(WorkflowProfileError, match="symlink|durable|integrity"):
            reader()


@pytest.mark.parametrize("symlink_kind", ["run", "runs"])
def test_resume_rejects_symlinked_run_ownership_before_external_write(
    tmp_path: Path,
    symlink_kind: str,
) -> None:
    artifacts = tmp_path / "artifacts"
    dag = _make_dag(tmp_path)
    calls: list[str] = []

    @dag.node("work", cache="off")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        calls.append("work")
        return {"value": "ok"}

    result = dag.run(run_id="symlink-resume")
    run_path = artifacts / "runs" / result.run_id
    original_manifest = (run_path / "_run.json").read_bytes()
    external_root = tmp_path / "external-runs"
    external_root.mkdir()
    external_run = external_root / result.run_id
    if symlink_kind == "run":
        run_path.rename(external_run)
        run_path.symlink_to(external_run, target_is_directory=True)
    else:
        runs_path = artifacts / "runs"
        runs_path.rename(external_root)
        runs_path.symlink_to(external_root, target_is_directory=True)

    with pytest.raises(RunManifestError, match="symlink"):
        dag.resume(result.run_id)

    assert calls == ["work"]
    assert (external_run / "_run.json").read_bytes() == original_manifest


def test_manifest_completed_status_must_match_durable_attempt_state(
    tmp_path: Path,
) -> None:
    artifacts = tmp_path / "artifacts"
    run_path, store = _make_run(
        tmp_path,
        recovery=False,
        run_path=artifacts / "runs" / "status-mismatch",
    )
    manifest_path = run_path / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["status"] = "completed"
    atomic_write_json(manifest_path, manifest)

    readers = (
        lambda: durable_run_state(run_path),
        lambda: load_run_profile(run_path),
        lambda: trace_run(artifacts, tmp_path / "llm", "status-mismatch"),
        lambda: store.initialize(),
        lambda: store.state_for("work"),
        lambda: store.prepare("work", policy=None, declaration_digest="decl"),
    )
    for reader in readers:
        with pytest.raises((WorkflowProfileError, RunManifestError), match="status|completed"):
            reader()


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


def _write_orphan_materialization(run_path: Path, target: str = "work") -> None:
    artifact = {"value": "orphan"}
    artifact_digest = sha(artifact)
    origin = {
        "artifact_sha256": artifact_digest,
        "prompt_resolutions": {},
        "calls": [],
        "agent": None,
    }
    write_artifact(
        run_path / f"{target}.json",
        canonical_json(artifact),
        {
            "run_sidecar_schema": 2,
            "node": target,
            "cache": "miss",
            "cache_policy": "off",
            "cache_key": "orphan-cache-key",
            "key_components": {"source": "orphan"},
            "outputs": [],
            "calls": [],
            "execution_calls": [],
            "prompt_resolutions": {},
            "prompt_resolutions_digest": sha({}),
            "origin_provenance": origin,
            "origin_provenance_digest": sha(origin),
            "artifact_sha256": artifact_digest,
        },
    )


def test_orphan_artifact_and_sidecar_never_project_as_completed(
    tmp_path: Path,
) -> None:
    artifacts = tmp_path / "artifacts"
    run_path = artifacts / "runs" / "orphan"
    store = AttemptStore(run_path, {})
    store.initialize()
    _write_orphan_materialization(run_path)
    _add_profile(run_path)
    manifest_path = run_path / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["workflow_profile"]["graph"]["nodes"] = [{"name": "work"}]
    manifest["workflow_profile_digest"] = sha(manifest["workflow_profile"])
    atomic_write_json(manifest_path, manifest)
    store = AttemptStore(run_path, {})

    readers = (
        lambda: durable_run_state(run_path),
        lambda: load_run_profile(run_path),
        lambda: trace_run(artifacts, tmp_path / "llm", "orphan"),
        lambda: store.prepare("work", policy=None, declaration_digest="decl"),
        lambda: store.state_for("work"),
        store.initialize,
    )
    for reader in readers:
        with pytest.raises((WorkflowProfileError, RunManifestError)):
            reader()


def test_rogue_sidecar_is_not_exposed_as_a_runtime_node(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    run_path, _store = _make_completed_run(
        tmp_path,
        run_path=artifacts / "runs" / "rogue",
    )
    manifest_path = run_path / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["workflow_profile"]["graph"]["nodes"] = [{"name": "work"}]
    manifest["workflow_profile_digest"] = sha(manifest["workflow_profile"])
    atomic_write_json(manifest_path, manifest)
    _write_orphan_materialization(run_path, target="work@rogue")

    profile = load_run_profile(run_path)
    assert [node["target"] for node in profile["run"]["nodes"]] == ["work"]
    traced = trace_run(artifacts, tmp_path / "llm", "rogue")
    assert [node["name"] for node in traced["nodes"]] == ["work"]


@pytest.mark.parametrize(
    "field, replacement",
    [
        ("cache_key", "tampered-cache-key"),
        ("key_components", {"tampered": "component"}),
        ("outputs", ["tampered.txt"]),
        ("calls", [{"key": "tampered-call"}]),
        ("cache_policy", "refresh"),
    ],
)
def test_sidecar_key_and_execution_fields_are_bound_by_completed_integrity(
    tmp_path: Path,
    field: str,
    replacement: Any,
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("work", cache="off")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    result = dag.run(run_id="sidecar-fields")
    run_path = tmp_path / "artifacts" / "runs" / result.run_id
    sidecar_path = run_path / "work.json.meta.json"
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar[field] = replacement
    atomic_write_json(sidecar_path, sidecar)

    readers = (
        lambda: durable_run_state(run_path),
        lambda: load_run_profile(run_path),
        lambda: trace_run(tmp_path / "artifacts", tmp_path / "llm", result.run_id),
    )
    for reader in readers:
        with pytest.raises(WorkflowProfileError):
            reader()


def test_completed_state_requires_a_success_candidate_at_all_durable_entrypoints(
    tmp_path: Path,
) -> None:
    run_path, store = _make_completed_run(tmp_path)
    candidate_file = json.loads(
        (run_path / "attempts" / sha("work") / "state.json").read_text(encoding="utf-8")
    )["candidate_file"]
    (run_path / "attempts" / sha("work") / candidate_file).unlink()

    for operation in (
        lambda: store.state_for("work"),
        lambda: store.prepare("work", policy=None, declaration_digest="decl"),
        lambda: AttemptStore(run_path, {}).initialize(),
    ):
        with pytest.raises((RunManifestError, StateIntegrityError), match="candidate|artifact"):
            operation()


def test_missing_manifest_generation_is_not_a_legacy_zero(tmp_path: Path) -> None:
    run_path, _store = _make_run(tmp_path, recovery=False)
    manifest_path = run_path / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("manifest_generation")
    atomic_write_json(manifest_path, manifest)

    readers = (
        lambda: AttemptStore(run_path, {}).initialize(),
        lambda: durable_run_state(run_path),
        lambda: load_run_profile(run_path),
    )
    for reader in readers:
        with pytest.raises(
            (WorkflowProfileError, StateIntegrityError, RunManifestError), match="generation"
        ):
            reader()


def test_schema_two_run_without_receipt_anchors_is_not_read_only_legacy(
    tmp_path: Path,
) -> None:
    run_path = tmp_path / "run"
    run_path.mkdir(parents=True)
    manifest = {
        "run_manifest_schema": 2,
        "manifest_generation": 0,
        "status": "completed",
        "workflow_profile": _profile(),
    }
    manifest["workflow_profile_digest"] = sha(manifest["workflow_profile"])
    atomic_write_json(run_path / "_run.json", manifest)
    _write_orphan_materialization(run_path)

    with pytest.raises(WorkflowProfileError):
        load_run_profile(run_path)


def test_manifest_and_state_symlinks_fail_closed(tmp_path: Path) -> None:
    run_path, _store = _make_run(tmp_path, recovery=False)
    manifest_path = run_path / "_run.json"
    manifest_target = tmp_path / "manifest-target.json"
    manifest_target.write_bytes(manifest_path.read_bytes())
    manifest_path.unlink()
    os.symlink(manifest_target, manifest_path)

    with pytest.raises(WorkflowProfileError):
        load_run_profile(run_path)

    run_path, store = _make_run(tmp_path / "state", recovery=False)
    state_path = run_path / "attempts" / sha("work") / "state.json"
    state_target = tmp_path / "state-target.json"
    state_target.write_bytes(state_path.read_bytes())
    state_path.unlink()
    os.symlink(state_target, state_path)

    with pytest.raises(StateIntegrityError):
        store.state_for("work")


def test_completed_manifest_cannot_be_reopened_then_failed(tmp_path: Path) -> None:
    _run_path, store = _make_completed_run(tmp_path)
    store.update_manifest("completed")

    store.update_manifest("running")
    with pytest.raises(RunManifestError, match="completed|terminal"):
        store.update_manifest("failed", failure={"failure_type": "stale"})
