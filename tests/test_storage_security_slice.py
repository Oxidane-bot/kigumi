from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from kigumi.artifacts import sha
from kigumi.blobs import BlobStore
from kigumi.calling import read_call_cache
from kigumi.inspect import load_call
from kigumi.store import allocate_run_id, gc_cache, write_run_artifact


def _directory_symlink(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"directory symlinks are unavailable: {error}")


def test_l1_and_inspect_reject_a_symlinked_cache_root(tmp_path: Path) -> None:
    key = "a" * 64
    outside = tmp_path / "outside-llm"
    outside.mkdir()
    payload = {
        "meta": {"key": key},
        "response": "external response",
        "response_sha256": sha("external response"),
    }
    (outside / f"{key}.json").write_text(json.dumps(payload), encoding="utf-8")
    cache = tmp_path / "cache"
    cache.mkdir()
    _directory_symlink(cache / "llm", outside)

    lookup = read_call_cache(cache, key)

    assert lookup.state == "CORRUPT"
    with pytest.raises(ValueError, match="symlink"):
        load_call(cache, key[:8])
    assert (outside / f"{key}.json").read_text(encoding="utf-8") == json.dumps(payload)


def test_blob_root_symlink_cannot_read_materialize_or_gc_external_files(tmp_path: Path) -> None:
    data = b"external blob"
    digest = hashlib.sha256(data).hexdigest()
    outside = tmp_path / "outside-blobs"
    outside.mkdir()
    (outside / digest).write_bytes(data)
    sentinel = outside / "sentinel.txt"
    sentinel.write_text("keep", encoding="utf-8")
    root = tmp_path / "blobs"
    _directory_symlink(root, outside)
    store = BlobStore(root)

    with pytest.raises(ValueError, match="symlink"):
        store.read_verified(digest)
    with pytest.raises(ValueError, match="symlink"):
        store.materialize(digest, tmp_path / "materialized.bin")
    with pytest.raises(ValueError, match="symlink"):
        store.gc(set())

    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert (outside / digest).read_bytes() == data
    assert not (tmp_path / "materialized.bin").exists()


def test_blob_gc_never_deletes_through_a_child_symlink(tmp_path: Path) -> None:
    root = tmp_path / "blobs"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel.txt"
    sentinel.write_text("keep", encoding="utf-8")
    alias = root / ("b" * 64)
    _directory_symlink(alias, outside)

    assert BlobStore(root).gc(set()) == 0
    assert alias.is_symlink()
    assert sentinel.read_text(encoding="utf-8") == "keep"


@pytest.mark.parametrize("which_root", ["cache", "runs"])
def test_gc_cache_rejects_a_symlinked_managed_root_without_external_deletion(
    tmp_path: Path, which_root: str
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "old.json"
    sentinel.write_text("keep", encoding="utf-8")
    cache_root = tmp_path / "cache"
    runs_root = tmp_path / "runs"
    cache_root.mkdir()
    runs_root.mkdir()
    if which_root == "cache":
        cache_root.rmdir()
        _directory_symlink(cache_root, outside)
    else:
        runs_root.rmdir()
        _directory_symlink(runs_root, outside)

    with pytest.raises(ValueError, match="symlink"):
        gc_cache(cache_root, runs_root, keep_last=0)
    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_allocate_run_id_rejects_a_symlinked_runs_root_without_external_creation(
    tmp_path: Path,
) -> None:
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    outside = tmp_path / "outside-runs"
    outside.mkdir()
    _directory_symlink(artifacts / "runs", outside)

    with pytest.raises(ValueError, match="symlink"):
        allocate_run_id(artifacts)
    assert list(outside.iterdir()) == []


def test_archive_stale_rejects_a_symlinked_history_root(tmp_path: Path) -> None:
    run = tmp_path / "runs" / "run-0001"
    run.mkdir(parents=True)
    artifact_path = run / "node.json"
    artifact_path.write_text(json.dumps({"version": 1}), encoding="utf-8")
    outside = tmp_path / "outside-history"
    outside.mkdir()
    _directory_symlink(run / "history", outside)

    with pytest.raises(ValueError, match="symlink"):
        write_run_artifact(
            tmp_path,
            "run-0001",
            "node",
            {"version": 2},
            {},
            lambda: "0001",
        )
    assert json.loads(artifact_path.read_text(encoding="utf-8")) == {"version": 1}
    assert list(outside.iterdir()) == []


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="mkfifo is unavailable")
def test_gc_cache_skips_fifo_without_blocking(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    runs.mkdir()
    cache = tmp_path / "cache"
    cache.mkdir()
    fifo = cache / "not-a-cache.json"
    os.mkfifo(fifo)

    assert gc_cache(cache, runs, keep_last=0) == 0
    assert fifo.is_fifo()
