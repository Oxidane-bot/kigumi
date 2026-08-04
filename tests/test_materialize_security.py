"""TOCTOU regression tests for materialized project outputs."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import kigumi._safe_io as safe_io
import kigumi.store as store_module
from kigumi.blobs import BlobStore
from kigumi.store import materialize_artifact


def _symlink_directory(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support directory symlinks: {error}")


def test_text_materialize_does_not_follow_parent_symlink_installed_at_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    parent = project / "output"
    parent.mkdir()
    moved_parent = project / "output-original"
    outside = tmp_path / "outside"
    outside.mkdir()
    destination = parent / "result.txt"
    original_replace = Path.replace
    raced = False

    def replace_with_parent_race(path: Path, target: Path) -> Path:
        nonlocal raced
        if target == destination and not raced:
            raced = True
            parent.rename(moved_parent)
            _symlink_directory(parent, outside)
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", replace_with_parent_race)

    materialize_artifact(
        {"files": {"output/result.txt": "safe text"}},
        "text-race",
        lambda path: project / path,
        BlobStore(tmp_path / "blobs"),
    )

    assert raced is False
    assert (parent / "result.txt").read_text(encoding="utf-8") == "safe text"
    assert not (outside / "result.txt").exists()


def test_blob_materialize_does_not_follow_parent_symlink_installed_at_temp_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = BlobStore(tmp_path / "blobs")
    digest = store.put(b"safe blob")
    project = tmp_path / "project"
    project.mkdir()
    parent = project / "output"
    parent.mkdir()
    moved_parent = project / "output-original"
    outside = tmp_path / "outside"
    outside.mkdir()
    destination = parent / "result.bin"
    raced = False

    original_temporary = safe_io.SecureDirectory.temporary

    def temporary_with_parent_race(
        directory: safe_io.SecureDirectory, prefix: str
    ) -> tuple[int, str]:
        nonlocal raced
        if directory.path == parent and not raced:
            raced = True
            parent.rename(moved_parent)
            _symlink_directory(parent, outside)
        return original_temporary(directory, prefix)

    monkeypatch.setattr(safe_io.SecureDirectory, "temporary", temporary_with_parent_race)

    with pytest.raises(ValueError, match="changed|symlink"):
        store.materialize(digest, destination)

    assert raced is True
    assert not (moved_parent / "result.bin").exists()
    assert not (outside / "result.bin").exists()


def test_materialize_artifact_stages_on_the_project_filesystem(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    store = BlobStore(tmp_path / "blobs")
    calls: list[Path | None] = []
    rename_sources: list[Path] = []
    original_mkdtemp = store_module.tempfile.mkdtemp
    original_rename_at = store_module._rename_at

    def tracking_mkdtemp(*args: object, **kwargs: object) -> str:
        directory = kwargs.get("dir")
        calls.append(None if directory is None else Path(directory))
        return original_mkdtemp(*args, **kwargs)

    def tracking_rename_at(source, destination, **kwargs):
        if kwargs.get("source_directory") is None:
            rename_sources.append(Path(source))
        return original_rename_at(source, destination, **kwargs)

    monkeypatch.setattr(store_module.tempfile, "mkdtemp", tracking_mkdtemp)
    monkeypatch.setattr(store_module, "_rename_at", tracking_rename_at)

    materialize_artifact(
        {"files": {"output/result.txt": "same filesystem"}},
        "project-staging",
        lambda path: project / path,
        store,
    )

    assert calls
    assert calls[0] == project
    assert rename_sources
    assert all(source.is_relative_to(project) for source in rename_sources)
    assert (project / "output/result.txt").read_text(encoding="utf-8") == "same filesystem"


@pytest.mark.skipif(os.name == "nt", reason="directory symlink setup is privilege-dependent")
def test_materialize_rejects_a_parent_symlink_before_writing(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    project = tmp_path / "project"
    project.mkdir()
    parent = project / "output"
    _symlink_directory(parent, outside)

    with pytest.raises(ValueError, match="symlink"):
        materialize_artifact(
            {"files": {"output/result.txt": "must not escape"}},
            "symlink-parent",
            lambda path: project / path,
            BlobStore(tmp_path / "blobs"),
        )

    assert not (outside / "result.txt").exists()


def test_materialize_fails_closed_without_descriptor_relative_directory_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = BlobStore(tmp_path / "blobs")
    digest = store.put(b"platform boundary")
    destination = tmp_path / "result.bin"
    monkeypatch.setattr(safe_io, "_secure_directory_supported", lambda: False)

    with pytest.raises(OSError, match="descriptor-relative directory I/O"):
        store.materialize(digest, destination)

    assert not destination.exists()
