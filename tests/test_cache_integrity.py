from __future__ import annotations

import json
from pathlib import Path

import pytest

from kigumi import EvidencePolicy
from kigumi._execution import ExecutionEnvelope
from kigumi.artifacts import sha
from kigumi.calling import CacheIntegrityError, LLMCaller, read_call_cache
from kigumi.store import (
    node_cache_path,
    read_cache_entry,
    read_node_cache,
    write_node_cache,
)
from kigumi.testing import FakeTransport


def _l1_key(prompt: str) -> str:
    transport = FakeTransport()
    prepared = transport.prepare(
        [{"role": "user", "content": prompt}],
        "default",
        {},
    )
    return sha(
        {
            "transport": transport.cache_identity(),
            "prepared": prepared.canonical(),
            "seed": 0,
        }
    )


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


def test_corrupt_l3_node_cache_is_reported_as_corrupt(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    path = node_cache_path(artifacts, "node-key")
    path.parent.mkdir(parents=True)
    path.write_text("{torn", encoding="utf-8")

    lookup = read_node_cache(artifacts, "node-key")

    assert lookup.state == "CORRUPT"
    assert lookup.data is None
    assert lookup.reason is not None


def test_valid_l3_node_cache_reports_data_and_digests(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    artifact = {"answer": "cached"}
    write_node_cache(artifacts, "node-key", artifact, _valid_origin(artifact))

    lookup = read_node_cache(artifacts, "node-key")

    assert lookup.state == "VALID"
    assert lookup.data == artifact
    assert lookup.expected_sha256 == lookup.actual_sha256 == sha(artifact)


def test_missing_l3_node_cache_reports_missing(tmp_path: Path) -> None:
    lookup = read_node_cache(tmp_path / "artifacts", "missing")

    assert lookup.state == "MISSING"
    assert lookup.data is None


def test_dangling_node_cache_symlink_is_corrupt_not_missing(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    path = node_cache_path(artifacts, "dangling")
    path.parent.mkdir(parents=True)
    try:
        path.symlink_to("does-not-exist.json")
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support symlinks: {error}")

    lookup = read_node_cache(artifacts, "dangling")

    assert lookup.state == "CORRUPT"
    assert lookup.data is None
    assert lookup.reason is not None
    assert "symlink" in lookup.reason

    envelope = ExecutionEnvelope(
        artifacts_path=artifacts,
        run_id="run-0001",
        resolve=lambda value: tmp_path / value,
        blob_store=object(),
        ensure_archive_id=lambda: "0001",
        approval_path=lambda name: tmp_path / "approvals" / name,
    )
    with pytest.raises(CacheIntegrityError) as error:
        envelope.lookup("dangling", forced=False)
    assert error.value.lookup.state == "CORRUPT"


def test_cache_entry_returns_artifact_origin_and_state_as_one_snapshot(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    artifact = {"answer": "cached"}
    origin = _valid_origin(artifact)
    write_node_cache(artifacts, "node-key", artifact, origin)

    entry = read_cache_entry(artifacts, "node-key")

    assert entry.state == "VALID"
    assert entry.artifact == artifact
    assert entry.origin == origin
    assert entry.lookup.data == artifact
    assert entry.lookup.expected_sha256 == entry.lookup.actual_sha256 == sha(artifact)


@pytest.mark.parametrize(
    "missing_field",
    (
        "prompt_sha256",
        "model",
        "params",
        "provider_response_id",
        "usage",
        "evidence_policy",
        "evidence_policy_digest",
        "kind",
        "calls",
        "agent",
        "prompt_resolutions",
    ),
)
def test_cache_entry_rejects_incomplete_origin_provenance(
    tmp_path: Path, missing_field: str
) -> None:
    artifacts = tmp_path / "artifacts"
    artifact = {"answer": "cached"}
    origin = _valid_origin(artifact)
    del origin[missing_field]
    write_node_cache(artifacts, "node-key", artifact, origin)

    entry = read_cache_entry(artifacts, "node-key")

    assert entry.state == "CORRUPT"
    assert entry.artifact is None
    assert entry.origin is None
    assert entry.reason is not None
    assert missing_field in entry.reason
    assert read_node_cache(artifacts, "node-key").state == "CORRUPT"


def test_cache_entry_allows_contractual_none_provenance_values(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    artifact = {"answer": "cached"}
    write_node_cache(artifacts, "node-key", artifact, _valid_origin(artifact))

    entry = read_cache_entry(artifacts, "node-key")

    assert entry.state == "VALID"
    assert entry.origin is not None
    assert entry.origin["prompt_sha256"] is None
    assert entry.origin["model"] is None
    assert entry.origin["provider_response_id"] is None
    assert entry.origin["usage"] is None
    assert entry.origin["agent"] is None


def test_cache_entry_preserves_missing_and_corrupt_states(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"

    missing = read_cache_entry(artifacts, "missing")
    assert missing.state == "MISSING"
    assert missing.artifact is None
    assert missing.origin is None

    path = node_cache_path(artifacts, "corrupt")
    path.parent.mkdir(parents=True)
    path.write_text("{torn", encoding="utf-8")
    corrupt = read_cache_entry(artifacts, "corrupt")
    assert corrupt.state == "CORRUPT"
    assert corrupt.artifact is None
    assert corrupt.origin is None
    assert corrupt.reason is not None


def test_corrupt_l1_call_cache_is_reported_and_not_reexecuted(tmp_path: Path) -> None:
    caller = LLMCaller(FakeTransport(), tmp_path)
    key = _l1_key("hello")
    path = tmp_path / "llm" / f"{key}.json"
    path.parent.mkdir(parents=True)
    path.write_text("{torn", encoding="utf-8")

    lookup = read_call_cache(path)
    with pytest.raises(CacheIntegrityError):
        caller.call("hello")

    assert lookup.state == "CORRUPT"
    assert caller.transport.requests == []


def test_legacy_l1_call_cache_without_response_digest_is_corrupt_not_replayed(
    tmp_path: Path,
) -> None:
    caller = LLMCaller(FakeTransport(), tmp_path)
    key = _l1_key("hello")
    path = tmp_path / "llm" / f"{key}.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"response": "legacy cached"}), encoding="utf-8")

    lookup = read_call_cache(path)

    assert lookup.state == "CORRUPT"
    assert lookup.reason is not None
    assert "response_sha256" in lookup.reason
    with pytest.raises(CacheIntegrityError):
        caller.call("hello")
    assert caller.transport.requests == []


def test_new_l1_call_cache_with_response_digest_remains_valid_and_replayable(
    tmp_path: Path,
) -> None:
    caller = LLMCaller(FakeTransport(), tmp_path)
    key = _l1_key("hello")
    path = tmp_path / "llm" / f"{key}.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "meta": {"key": key},
                "response": "new cached",
                "response_sha256": sha("new cached"),
            }
        ),
        encoding="utf-8",
    )

    assert caller.call("hello") == "new cached"
    assert caller.calls[0]["cache"] == "hit"
    assert caller.transport.requests == []


def test_l1_cache_symlink_is_corrupt_not_replayable_or_missing(tmp_path: Path) -> None:
    caller = LLMCaller(FakeTransport(), tmp_path)
    key = _l1_key("hello")
    path = tmp_path / "llm" / f"{key}.json"
    path.parent.mkdir(parents=True)
    payload = {
        "meta": {"key": key},
        "response": "cached",
        "response_sha256": sha("cached"),
    }
    target = tmp_path / "target.json"
    target.write_text(json.dumps(payload), encoding="utf-8")
    try:
        path.symlink_to(target)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support symlinks: {error}")

    lookup = read_call_cache(path)

    assert lookup.state == "CORRUPT"
    assert "symlink" in (lookup.reason or "")
    with pytest.raises(CacheIntegrityError):
        caller.call("hello")
    assert caller.transport.requests == []


def test_l1_cache_dangling_symlink_is_corrupt_not_a_miss(tmp_path: Path) -> None:
    caller = LLMCaller(FakeTransport(), tmp_path)
    key = _l1_key("hello")
    path = tmp_path / "llm" / f"{key}.json"
    path.parent.mkdir(parents=True)
    try:
        path.symlink_to(tmp_path / "missing.json")
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support symlinks: {error}")

    lookup = read_call_cache(path)

    assert lookup.state == "CORRUPT"
    with pytest.raises(CacheIntegrityError):
        caller.call("hello")
    assert caller.transport.requests == []


def test_l1_cache_cross_key_payload_is_corrupt_not_replayed(tmp_path: Path) -> None:
    first_key = _l1_key("first")
    second_key = _l1_key("second")
    root = tmp_path / "llm"
    root.mkdir()
    (root / f"{first_key}.json").write_text(
        json.dumps(
            {
                "meta": {"key": first_key},
                "response": "first answer",
                "response_sha256": sha("first answer"),
            }
        ),
        encoding="utf-8",
    )
    (root / f"{second_key}.json").write_bytes((root / f"{first_key}.json").read_bytes())

    caller = LLMCaller(FakeTransport(), tmp_path)

    lookup = read_call_cache(root / f"{second_key}.json")

    assert lookup.state == "CORRUPT"
    assert "key" in (lookup.reason or "")
    with pytest.raises(CacheIntegrityError):
        caller.call("second")
    assert caller.transport.requests == []


def test_valid_and_missing_l1_call_cache_states(tmp_path: Path) -> None:
    valid_path = tmp_path / "llm" / "valid.json"
    valid_path.parent.mkdir(parents=True)
    valid_path.write_text(
        json.dumps(
            {
                "meta": {"key": "valid"},
                "response": "cached",
                "response_sha256": sha("cached"),
            }
        ),
        encoding="utf-8",
    )

    valid = read_call_cache(valid_path)
    missing = read_call_cache(tmp_path / "llm" / "missing.json")

    assert valid.state == "VALID"
    assert valid.data == {
        "meta": {"key": "valid"},
        "response": "cached",
        "response_sha256": sha("cached"),
    }
    assert valid.expected_sha256 == valid.actual_sha256 == sha("cached")
    assert missing.state == "MISSING"
