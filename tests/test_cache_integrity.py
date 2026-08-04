from __future__ import annotations

import json
from pathlib import Path

import pytest

from kigumi.artifacts import sha
from kigumi.calling import CacheIntegrityError, LLMCaller, read_call_cache
from kigumi.store import (
    node_cache_path,
    read_cache_entry,
    read_node_cache,
    write_node_cache,
)
from kigumi.testing import FakeTransport


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
    write_node_cache(artifacts, "node-key", artifact, {"artifact_sha256": sha(artifact)})

    lookup = read_node_cache(artifacts, "node-key")

    assert lookup.state == "VALID"
    assert lookup.data == artifact
    assert lookup.expected_sha256 == lookup.actual_sha256 == sha(artifact)


def test_missing_l3_node_cache_reports_missing(tmp_path: Path) -> None:
    lookup = read_node_cache(tmp_path / "artifacts", "missing")

    assert lookup.state == "MISSING"
    assert lookup.data is None


def test_cache_entry_returns_artifact_origin_and_state_as_one_snapshot(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    artifact = {"answer": "cached"}
    origin = {
        "artifact_sha256": sha(artifact),
        "evidence_policy_digest": "policy-a",
    }
    write_node_cache(artifacts, "node-key", artifact, origin)

    entry = read_cache_entry(artifacts, "node-key")

    assert entry.state == "VALID"
    assert entry.artifact == artifact
    assert entry.origin == origin
    assert entry.lookup.data == artifact
    assert entry.lookup.expected_sha256 == entry.lookup.actual_sha256 == sha(artifact)


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
    key = sha(
        {
            "messages": [{"role": "user", "content": "hello"}],
            "model": "default",
            "params": {},
            "seed": 0,
        }
    )
    path = tmp_path / "llm" / f"{key}.json"
    path.parent.mkdir(parents=True)
    path.write_text("{torn", encoding="utf-8")

    lookup = read_call_cache(path)
    with pytest.raises(CacheIntegrityError):
        caller.call("hello")

    assert lookup.state == "CORRUPT"
    assert caller.transport.requests == []


def test_valid_and_missing_l1_call_cache_states(tmp_path: Path) -> None:
    valid_path = tmp_path / "llm" / "valid.json"
    valid_path.parent.mkdir(parents=True)
    valid_path.write_text(
        json.dumps({"response": "cached", "response_sha256": sha("cached")}),
        encoding="utf-8",
    )

    valid = read_call_cache(valid_path)
    missing = read_call_cache(tmp_path / "llm" / "missing.json")

    assert valid.state == "VALID"
    assert valid.data == {"response": "cached", "response_sha256": sha("cached")}
    assert valid.expected_sha256 == valid.actual_sha256 == sha("cached")
    assert missing.state == "MISSING"
