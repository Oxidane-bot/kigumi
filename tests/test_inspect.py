from __future__ import annotations

import json
from pathlib import Path

import pytest

from kigumi._runstate import AttemptStore
from kigumi.artifacts import atomic_write_json, canonical_json, sha, write_artifact
from kigumi.inspect import diff_components, load_call, trace_run
from kigumi.profile import WorkflowProfileError


def _sidecar(
    root: Path,
    name: str,
    *,
    cache: str = "hit",
    components: dict[str, str] | None = None,
    calls: list[dict[str, object]] | None = None,
) -> None:
    artifact = {"name": name}
    artifact_digest = sha(artifact)
    origin = {
        "kind": "code",
        "artifact_sha256": artifact_digest,
        "calls": [],
        "agent": None,
        "prompt_resolutions": {},
        "prompt_sha256": None,
        "model": None,
        "params": {},
        "provider_response_id": None,
        "usage": None,
        "evidence_policy": {},
        "evidence_policy_digest": sha({}),
    }
    metadata: dict[str, object] = {
        "run_sidecar_schema": 2,
        "node": name,
        "cache_key": f"node-{name}",
        "cache": cache,
        "cache_policy": "auto",
        "outputs": [],
        "seconds": 1.25,
        "calls": calls or [],
        "execution_calls": calls or [],
        "prompt_resolutions": {},
        "prompt_resolutions_digest": sha({}),
        "origin_provenance": origin,
        "origin_provenance_digest": sha(origin),
        "artifact_sha256": artifact_digest,
    }
    if components is not None:
        metadata["key_components"] = components
    write_artifact(root / f"{name}.json", canonical_json(artifact), metadata)


def _trace_manifest(run_root: Path, nodes: list[str]) -> None:
    store = AttemptStore(run_root, {})
    store.initialize()
    profile = {
        "workflow_profile_schema": 2,
        "mode": "static",
        "resolution_status": "unresolved",
        "graph": {
            "nodes": [{"name": name} for name in nodes],
            "edges": [],
            "mounts": [],
            "models": {},
        },
        "prompts": {"specs": []},
        "run": None,
    }
    manifest_path = run_root / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["workflow_profile"] = profile
    manifest["workflow_profile_digest"] = sha(profile)
    dynamic_ledger = {"chapters": {"one": [], "two": []}}
    manifest["dynamic_files_ledger"] = dynamic_ledger
    manifest["dynamic_files_ledger_sha256"] = sha(dynamic_ledger)
    atomic_write_json(manifest_path, manifest)


def test_trace_run_groups_map_items_and_links_llm_payloads(tmp_path: Path) -> None:
    """Trace stays a read-only join over persisted run and L1 evidence."""
    artifacts = tmp_path / "artifacts"
    llm_cache = tmp_path / "caller-cache"
    run_root = artifacts / "runs" / "run-7"
    _trace_manifest(run_root, ["outline", "chapters"])
    call = {
        "key": "call-key-123",
        "model_alias": "fast",
        "model": "provider/model",
        "cache": "miss",
        "prompt_sha": "prompt-sha",
        "seconds": 0.5,
        "usage": {"total_tokens": 3},
    }
    _sidecar(run_root, "outline", components={"prompt": "prompt-sha"}, calls=[call])
    _sidecar(run_root, "chapters@two", components={"item": "two"})
    _sidecar(run_root, "chapters@one", cache="hit", components={"item": "one"})
    payload_path = llm_cache / "llm" / "call-key-123.json"
    atomic_write_json(
        payload_path,
        {
            "meta": call,
            "messages": [{"role": "user", "content": "hello"}],
            "response": "world",
            "reasoning": "because",
        },
    )

    traced = trace_run(artifacts, llm_cache, "run-7")

    assert traced["run_id"] == "run-7"
    assert [node["name"] for node in traced["nodes"]] == ["chapters", "outline"]
    chapters = traced["nodes"][0]
    assert [item["name"] for item in chapters["items"]] == ["chapters@one", "chapters@two"]
    traced_call = traced["nodes"][1]["calls"][0]
    assert traced_call["payload_path"] == str(payload_path.resolve())
    assert traced_call["model"] == "provider/model"
    assert traced["nodes"][1]["key_components"] == {"prompt": "prompt-sha"}
    assert "warnings" not in traced


def test_trace_run_warns_for_missing_payload_and_filters_node(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    _trace_manifest(artifacts / "runs" / "run-8", ["node"])
    _sidecar(
        artifacts / "runs" / "run-8",
        "node",
        calls=[{"key": "missing", "model": "model"}],
    )

    traced = trace_run(artifacts, tmp_path / "caller-cache", "run-8", node="node")

    assert [entry["name"] for entry in traced["nodes"]] == ["node"]
    assert traced["nodes"][0]["calls"][0]["payload_path"] is None
    assert "llm_cache_dir" in traced["warnings"][0]
    with pytest.raises(FileNotFoundError, match="run not found: missing"):
        trace_run(artifacts, tmp_path / "caller-cache", "missing")


def test_trace_run_rejects_a_manifestless_legacy_entry(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    _sidecar(artifacts / "runs" / "legacy", "node")

    with pytest.raises(WorkflowProfileError, match="manifest"):
        trace_run(artifacts, tmp_path / "caller-cache", "legacy")


def test_load_call_resolves_prefix_and_fails_visibly_for_missing_or_ambiguous(
    tmp_path: Path,
) -> None:
    cache = tmp_path / "caller-cache"
    atomic_write_json(cache / "llm" / "abc123.json", {"response": "one"})

    key, payload = load_call(cache, "abc")

    assert key == "abc123"
    assert payload == {"response": "one"}
    with pytest.raises(FileNotFoundError, match="caller-cache/llm"):
        load_call(cache, "missing")
    atomic_write_json(cache / "llm" / "abc456.json", {"response": "two"})
    with pytest.raises(ValueError, match="abc123, abc456"):
        load_call(cache, "abc")


def test_diff_components_reports_changes_unavailable_and_one_sided_nodes(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    run_a = artifacts / "runs" / "run-a"
    run_b = artifacts / "runs" / "run-b"
    _sidecar(run_a, "changed", components={"prompt": "a", "libs": "same"})
    _sidecar(run_b, "changed", components={"prompt": "b", "libs": "same"})
    _sidecar(run_a, "unavailable")
    _sidecar(run_b, "unavailable", components={"prompt": "b"})
    _sidecar(run_a, "only-a", components={})
    _sidecar(run_b, "only-b", components={})

    result = diff_components(artifacts, "run-a", "run-b")

    assert result["changed"] == {"changed": ["prompt"], "unchanged": ["libs"]}
    assert result["unavailable"] == "unavailable"
    assert result["only_in_a"] == ["only-a"]
    assert result["only_in_b"] == ["only-b"]
