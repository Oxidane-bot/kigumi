from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from kigumi.artifacts import canonical_json
from kigumi.store import node_cache_path


def _envelope(tmp_path: Path) -> Any:
    from kigumi._execution import ExecutionEnvelope

    return ExecutionEnvelope(
        artifacts_path=tmp_path / "artifacts",
        run_id="run-0001",
        resolve=lambda path: tmp_path / path,
        blob_store=object(),
        ensure_archive_id=lambda: "0001",
        approval_path=lambda name: tmp_path / "approvals" / name,
    )


def test_lookup_respects_forced(tmp_path: Path) -> None:
    envelope = _envelope(tmp_path)
    artifact = envelope.seal({"answer": "cached"}, "key", label="Node 'work'")

    assert envelope.lookup("key", forced=False) == (artifact, True)
    assert envelope.lookup("key", forced=True) == (None, False)


def test_seal_rejects_non_dict_and_canonicalizes(tmp_path: Path) -> None:
    envelope = _envelope(tmp_path)

    try:
        envelope.seal(["not", "a", "dict"], "key", label="Map node 'work' item 'one'")
    except TypeError as error:
        assert str(error) == "Map node 'work' item 'one' must return a dict artifact"
    else:
        raise AssertionError("seal accepted a non-dict artifact")

    artifact = {"z": [2, 1], "a": {"second": 2, "first": 1}}
    sealed = envelope.seal(artifact, "key", label="Node 'work'")

    assert canonical_json(sealed) == canonical_json(json.loads(canonical_json(artifact)))
    cache_payload = json.loads(
        node_cache_path(tmp_path / "artifacts", "key").read_text(encoding="utf-8")
    )
    assert cache_payload["cache_schema"] == 2
    assert cache_payload["artifact"] == sealed
    assert cache_payload["artifact_sha256"] == cache_payload["origin_provenance"]["artifact_sha256"]


def test_write_sidecar_omits_none_key_components(tmp_path: Path) -> None:
    envelope = _envelope(tmp_path)

    envelope.write_sidecar(
        "work",
        {"answer": "ok"},
        "key",
        cache_hit=False,
        seconds=0.25,
        calls=[],
        key_components=None,
    )

    metadata = json.loads(
        (tmp_path / "artifacts" / "runs" / "run-0001" / "work.json.meta.json").read_text(
            encoding="utf-8"
        )
    )
    assert "key_components" not in metadata
