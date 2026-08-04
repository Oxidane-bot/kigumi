from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from kigumi import EvidencePolicy, store
from kigumi.artifacts import canonical_json, sha
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


def test_lookup_uses_one_cache_snapshot_for_evidence_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    envelope = _envelope(tmp_path)
    artifact = envelope.seal({"answer": "cached"}, "key", label="Node 'work'")

    def fail_if_called(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("cache origin must come from the same cache snapshot")

    monkeypatch.setattr(store, "read_node_cache_origin", fail_if_called)

    assert envelope.lookup(
        "key",
        forced=False,
        evidence_policy_digest=EvidencePolicy().digest,
    ) == (artifact, True)
    assert envelope.lookup(
        "key",
        forced=False,
        evidence_policy_digest="different-policy",
    ) == (None, False)


def test_lookup_does_not_downgrade_when_cache_is_replaced_during_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    envelope = _envelope(tmp_path)
    artifact = envelope.seal({"answer": "cached"}, "key", label="Node 'work'")
    cache_path = node_cache_path(tmp_path / "artifacts", "key")
    replacement = tmp_path / "replacement-cache.json"
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    payload["origin_provenance"]["evidence_policy_digest"] = "replacement-policy"
    payload["origin_sha256"] = sha(payload["origin_provenance"])
    replacement.write_text(json.dumps(payload), encoding="utf-8")

    original_open = Path.open
    replaced = False

    def open_then_replace(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal replaced
        handle = original_open(path, *args, **kwargs)
        if path == cache_path and not replaced:
            replaced = True
            replacement.replace(path)
        return handle

    monkeypatch.setattr(Path, "open", open_then_replace)

    assert envelope.lookup(
        "key",
        forced=False,
        evidence_policy_digest=EvidencePolicy().digest,
    ) == (artifact, True)


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
    assert cache_payload["cache_schema"] == 4
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


def test_write_sidecar_uses_cache_entry_snapshot_for_cache_hits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    envelope = _envelope(tmp_path)
    artifact = envelope.seal({"answer": "cached"}, "key", label="Node 'work'")
    cache_entry = store.read_cache_entry(envelope.artifacts_path, "key")

    def fail_if_called(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("cache hit with a supplied snapshot must not reread the key")

    monkeypatch.setattr(store, "read_cache_entry", fail_if_called)

    envelope.write_sidecar(
        "work",
        artifact,
        "key",
        cache_hit=True,
        seconds=0.25,
        calls=[],
        cache_entry=cache_entry,
    )


def test_write_sidecar_keeps_legacy_cache_key_read_when_snapshot_is_omitted(
    tmp_path: Path,
) -> None:
    envelope = _envelope(tmp_path)
    artifact = envelope.seal({"answer": "cached"}, "key", label="Node 'work'")

    envelope.write_sidecar(
        "work",
        artifact,
        "key",
        cache_hit=True,
        seconds=0.25,
        calls=[],
    )

    metadata = json.loads(
        (tmp_path / "artifacts" / "runs" / "run-0001" / "work.json.meta.json").read_text(
            encoding="utf-8"
        )
    )
    assert metadata["origin_provenance"]["artifact_sha256"] == sha(artifact)
