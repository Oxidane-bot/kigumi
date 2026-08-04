from __future__ import annotations

import errno
import json
from pathlib import Path

import pytest

import kigumi._safe_io as safe_io
import kigumi.store as store_module
from kigumi import EvidencePolicy
from kigumi.artifacts import sha
from kigumi.blobs import BlobStore
from kigumi.calling import LLMCaller
from kigumi.store import materialize_artifact, node_cache_path, read_node_cache, write_node_cache
from kigumi.testing import FakeTransport


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


def test_node_cache_envelope_schema_is_bumped_and_schema_three_is_corrupt(
    tmp_path: Path,
) -> None:
    artifacts = tmp_path / "artifacts"
    artifact = {"answer": "schema boundary"}
    write_node_cache(artifacts, "schema-key", artifact, _valid_origin(artifact))
    path = node_cache_path(artifacts, "schema-key")

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["cache_schema"] == 4

    payload["cache_schema"] = 3
    path.write_text(json.dumps(payload), encoding="utf-8")

    lookup = read_node_cache(artifacts, "schema-key")

    assert lookup.state == "CORRUPT"
    assert lookup.data is None
    assert lookup.reason == "node cache schema is not 4"


@pytest.mark.parametrize("cache_kind", ["l1", "l3"])
def test_cache_writes_reject_symlinked_parent_without_writing_outside_project(
    tmp_path: Path, cache_kind: str
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()

    if cache_kind == "l1":
        cache_root = tmp_path / "llm-cache"
        cache_root.mkdir()
        (cache_root / "llm").symlink_to(outside, target_is_directory=True)

        with pytest.raises((OSError, ValueError)):
            LLMCaller(FakeTransport(), cache_root).call("safe cache write")

        assert not list(outside.iterdir())
        return

    artifacts = tmp_path / "artifacts"
    nodes_root = artifacts / "_cache" / "nodes"
    nodes_root.parent.mkdir(parents=True)
    nodes_root.symlink_to(outside, target_is_directory=True)
    artifact = {"answer": "must stay inside"}

    with pytest.raises((OSError, ValueError)):
        write_node_cache(artifacts, "outside-key", artifact, _valid_origin(artifact))

    assert not list(outside.iterdir())


@pytest.mark.parametrize("operation", ["put", "ingest"])
def test_blob_writes_reject_symlinked_store_root_without_writing_outside_project(
    tmp_path: Path, operation: str
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "blobs"
    root.symlink_to(outside, target_is_directory=True)
    source = tmp_path / "source.bin"
    source.write_bytes(b"must stay inside")
    blobs = BlobStore(root)

    with pytest.raises((OSError, ValueError)):
        if operation == "put":
            blobs.put(b"must stay inside")
        else:
            blobs.ingest(source)

    assert not list(outside.iterdir())


def test_secure_cache_writes_report_enotsup_when_directory_io_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(safe_io, "_secure_directory_supported", lambda: False)

    with pytest.raises(OSError) as raised:
        write_node_cache(
            tmp_path / "artifacts",
            "unsupported",
            {"answer": "no fallback"},
            _valid_origin({"answer": "no fallback"}),
        )

    assert raised.value.errno == errno.ENOTSUP
    assert not (tmp_path / "artifacts").exists()


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
