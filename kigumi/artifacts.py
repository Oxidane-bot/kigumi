"""Deterministic serialization and atomic artifact writes."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any


def canonical_json(obj: Any) -> str:
    """Serialize data through the library's single deterministic JSON format."""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)


def sha(obj: Any) -> str:
    """Return the SHA-256 digest of text or its canonical JSON representation."""
    text = obj if isinstance(obj, str) else canonical_json(obj)
    return sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: str | Path) -> str:
    """Return a file's SHA-256 digest without loading the whole file into memory."""
    digest = sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_text(path: str | Path, text: str) -> None:
    """Atomically replace *path* with UTF-8 text, creating parents as needed."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def atomic_write_json(path: str | Path, obj: Any) -> None:
    """Atomically write an object using :func:`canonical_json`."""
    atomic_write_text(path, canonical_json(obj))


def write_artifact(path: str | Path, data: str, meta: Mapping[str, Any]) -> None:
    """Write artifact text and its timestamped metadata sidecar atomically."""
    artifact_path = Path(path)
    artifact_meta = dict(meta)
    artifact_meta.setdefault("created_at", datetime.now(UTC).isoformat())
    atomic_write_text(artifact_path, data)
    atomic_write_json(f"{artifact_path}.meta.json", artifact_meta)
