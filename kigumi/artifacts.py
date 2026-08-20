"""Deterministic serialization and atomic artifact writes."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from ._safe_io import (
    digest_open_file,
    open_regular_file,
    secure_atomic_write_json,
    secure_atomic_write_text,
)


def canonical_json(obj: Any) -> str:
    """Serialize data through the library's single deterministic JSON format."""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)


def sha(obj: Any) -> str:
    """Return the SHA-256 digest of text or its canonical JSON representation."""
    text = obj if isinstance(obj, str) else canonical_json(obj)
    return sha256(text.encode("utf-8")).hexdigest()


def _artifact_file_identity(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def _artifact_file_error(message: str, path: Path) -> ValueError:
    return ValueError(f"Artifact file {message}: {path}")


def _open_regular_file_for_hash(path: str | Path):
    """Open a regular artifact file through the shared safe read boundary."""
    return open_regular_file(
        Path(path),
        identity=_artifact_file_identity,
        expected_identity=None,
        phase="hashing",
        error=_artifact_file_error,
    )


def sha256_file(path: str | Path) -> str:
    """Return a file's SHA-256 digest without loading the whole file into memory."""
    artifact_path = Path(path)
    with _open_regular_file_for_hash(artifact_path) as handle:
        digest, _size, _data = digest_open_file(
            handle,
            artifact_path,
            identity=_artifact_file_identity,
            expected_identity=None,
            before_phase="hashing",
            during_phase="hashing",
            chunk_size=1024 * 1024,
            error=_artifact_file_error,
        )
    return digest


def atomic_write_text(path: str | Path, text: str) -> None:
    """Atomically replace *path* through the shared safe write boundary."""
    secure_atomic_write_text(path, text)


def atomic_write_json(path: str | Path, obj: Any) -> None:
    """Atomically write an object using :func:`canonical_json`."""
    secure_atomic_write_json(path, obj)


def write_artifact(path: str | Path, data: str, meta: Mapping[str, Any]) -> None:
    """Write artifact text and its metadata sidecar as separate atomic replacements.

    The pair is not a transaction: a crash between the two writes can leave the
    artifact newer than its sidecar.
    """
    artifact_path = Path(path)
    artifact_meta = dict(meta)
    artifact_meta.setdefault("created_at", datetime.now(UTC).isoformat())
    atomic_write_text(artifact_path, data)
    atomic_write_json(f"{artifact_path}.meta.json", artifact_meta)
