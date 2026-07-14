from __future__ import annotations

import json
from pathlib import Path

from kigumi.artifacts import (
    atomic_write_json,
    atomic_write_text,
    canonical_json,
    sha,
    write_artifact,
)


def test_canonical_json_byte_stable() -> None:
    """Same data must always serialize identically, regardless of input key order."""
    first = {"b": ["木", 2], "a": {"z": 1, "y": True}}
    second = {"a": {"y": True, "z": 1}, "b": ["木", 2]}

    assert canonical_json(first) == canonical_json(second)
    assert canonical_json(first) == json.dumps(first, ensure_ascii=False, sort_keys=True, indent=2)
    assert sha(first) == sha(second)


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
