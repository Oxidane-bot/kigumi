from __future__ import annotations

import json
from pathlib import Path

import pytest

import kigumi.artifacts as artifacts
from kigumi.artifacts import (
    atomic_write_json,
    atomic_write_text,
    canonical_json,
    sha,
    sha256_file,
    write_artifact,
)


def test_canonical_json_byte_stable() -> None:
    """Same data must always serialize identically, regardless of input key order."""
    first = {"b": ["木", 2], "a": {"z": 1, "y": True}}
    second = {"a": {"y": True, "z": 1}, "b": ["木", 2]}

    assert canonical_json(first) == canonical_json(second)
    assert canonical_json(first) == json.dumps(first, ensure_ascii=False, sort_keys=True, indent=2)
    assert sha(first) == sha(second)


def test_sha256_file_rejects_truncation_during_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A file changing after a hash read must fail closed."""
    path = tmp_path / "changing.txt"
    path.write_bytes(b"payload" * 300_000)
    original_open = artifacts._open_regular_file_for_hash

    class TruncatingHandle:
        def __init__(self, handle) -> None:
            self._handle = handle
            self._truncated = False

        def __enter__(self):
            self._handle.__enter__()
            return self

        def __exit__(self, *args):
            return self._handle.__exit__(*args)

        def fileno(self) -> int:
            return self._handle.fileno()

        def read(self, size: int = -1) -> bytes:
            chunk = self._handle.read(size)
            if chunk and not self._truncated:
                self._truncated = True
                path.write_bytes(b"")
            return chunk

    def open_with_truncation(target):
        return TruncatingHandle(original_open(target))

    monkeypatch.setattr(artifacts, "_open_regular_file_for_hash", open_with_truncation)

    with pytest.raises(ValueError, match="Artifact file changed hashing"):
        sha256_file(path)


def test_atomic_write_and_sidecar_contents(tmp_path: Path) -> None:
    """Artifacts and metadata are independently atomically replaceable."""
    path = tmp_path / "nested" / "artifact.txt"

    atomic_write_text(path, "first")
    atomic_write_json(tmp_path / "data.json", {"b": 2, "a": 1})
    write_artifact(path, "final", {"model": "test-model"})

    assert path.read_text(encoding="utf-8") == "final"
    assert (tmp_path / "data.json").read_text(encoding="utf-8") == '{\n  "a": 1,\n  "b": 2\n}'
    sidecar = json.loads((tmp_path / "nested" / "artifact.txt.meta.json").read_text())
    assert sidecar["model"] == "test-model"
    assert sidecar["created_at"].endswith("+00:00")
