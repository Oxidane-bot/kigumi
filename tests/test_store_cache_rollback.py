from __future__ import annotations

import json
from pathlib import Path

import pytest

import kigumi.store as store_module
from kigumi import EvidencePolicy
from kigumi.artifacts import sha
from kigumi.blobs import BlobStore
from kigumi.store import materialize_artifact, node_cache_path, read_node_cache, write_node_cache


def _valid_origin(artifact: dict[str, object]) -> dict[str, object]:
    policy = EvidencePolicy()
    return {
        "kind": "code",
        "artifact_sha256": sha(artifact),
        "calls": [],
        "agent": None,
        "prompt_resolutions": {},
        "prompt_sha256": None,
        "model": None,
        "params": {},
        "provider_response_id": None,
        "usage": None,
        "evidence_policy": policy.canonical(),
        "evidence_policy_digest": policy.digest,
    }


def test_l3_cache_envelope_binds_the_requested_cache_key(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    artifact = {"answer": "key A"}
    source = node_cache_path(artifacts, "key-A")
    destination = node_cache_path(artifacts, "key-B")

    write_node_cache(artifacts, "key-A", artifact, _valid_origin(artifact))
    destination.write_bytes(source.read_bytes())

    lookup = read_node_cache(artifacts, "key-B")

    assert lookup.state == "CORRUPT"
    assert lookup.data is None
    assert lookup.reason is not None
    assert "cache key" in lookup.reason


def test_l3_cache_read_does_not_follow_a_symlink_installed_after_lstat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts = tmp_path / "artifacts"
    requested = node_cache_path(artifacts, "requested")
    target = node_cache_path(artifacts, "target")
    requested_artifact = {"answer": "requested"}
    target_artifact = {"answer": "target"}
    write_node_cache(artifacts, "requested", requested_artifact, _valid_origin(requested_artifact))
    write_node_cache(artifacts, "target", target_artifact, _valid_origin(target_artifact))

    original_lstat = Path.lstat
    raced = False

    def lstat_then_replace(path: Path) -> object:
        nonlocal raced
        result = original_lstat(path)
        if path == requested and not raced:
            raced = True
            path.unlink()
            path.symlink_to(target)
        return result

    monkeypatch.setattr(Path, "lstat", lstat_then_replace)

    lookup = read_node_cache(artifacts, "requested")

    assert raced
    assert lookup.state == "CORRUPT"
    assert lookup.data is None
    assert lookup.reason is not None
    assert "symlink" in lookup.reason


def test_failed_cache_output_restore_keeps_recoverable_rollback_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = tmp_path / "first.txt"
    first.write_text("original", encoding="utf-8")
    blocked = tmp_path / "blocked"
    blocked.mkdir()

    original_rename_at = store_module._rename_at

    def fail_restore(source, destination, **kwargs):
        source_directory = kwargs.get("source_directory")
        if source_directory is not None and source_directory.path.name.startswith(
            ".kigumi-rollback-"
        ):
            raise OSError("restore failed")
        return original_rename_at(source, destination, **kwargs)

    monkeypatch.setattr(store_module, "_rename_at", fail_restore)

    with pytest.raises(OSError, match="restore failed"):
        materialize_artifact(
            {"files": {"first.txt": "replacement", "blocked": "not written"}},
            "rollback-failure",
            lambda path: tmp_path / path,
            BlobStore(tmp_path / "artifacts" / "_cache" / "blobs"),
        )

    rollback_roots = list(tmp_path.glob(".kigumi-rollback-*"))
    assert len(rollback_roots) == 1
    rollback_root = rollback_roots[0]
    marker = rollback_root / "rollback.json"
    assert marker.is_file()
    assert json.loads(marker.read_text(encoding="utf-8"))["state"] == "recovery_required"
    assert (rollback_root / "0").read_text(encoding="utf-8") == "original"
    assert not first.exists()
    assert not list(tmp_path.glob(".kigumi-materialize-*"))
