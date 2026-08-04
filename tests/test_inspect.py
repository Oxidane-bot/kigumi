from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest

import kigumi.inspect as inspect_module
from kigumi._runstate import AttemptStore
from kigumi.artifacts import atomic_write_json, canonical_json, sha, write_artifact
from kigumi.calling import LLMCaller
from kigumi.errors import CacheIntegrityError
from kigumi.inspect import diff_components, diff_run_views, load_call, trace_run
from kigumi.profile import WorkflowProfileError
from kigumi.testing import FakeTransport


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
            "response_sha256": sha("world"),
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


def test_trace_keeps_attachment_direct_chat_unmanaged_with_lineage(tmp_path: Path) -> None:
    """Trace must use the explicit PromptSpec marker, not resolution presence."""
    artifacts = tmp_path / "artifacts"
    llm_cache = tmp_path / "caller-cache"
    run_root = artifacts / "runs" / "unmanaged-file-chat"
    source = tmp_path / "attachment.txt"
    contents = b"trace attachment"
    source.write_bytes(contents)

    caller = LLMCaller(FakeTransport(), llm_cache)
    caller.call([{"role": "user", "content": {"kigumi_file": str(source)}}])
    call = caller.calls[0]

    _trace_manifest(run_root, ["node"])
    _sidecar(run_root, "node", calls=[call])

    traced = trace_run(artifacts, llm_cache, "unmanaged-file-chat")
    traced_call = traced["nodes"][0]["calls"][0]

    assert traced_call["managed"] is False
    assert traced_call["prompt_resolution"] == call["prompt_resolution"]
    resolution = traced_call["prompt_resolution"]
    assert resolution["spec"] == "unmanaged"
    assert resolution["attachments"][0]["content_hash"] == sha256(contents).hexdigest()
    assert resolution["phase"] == "primary"
    assert resolution["repair_round"] == 0


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
    atomic_write_json(
        cache / "llm" / "abc123.json",
        {
            "meta": {"key": "abc123"},
            "response": "one",
            "response_sha256": sha("one"),
        },
    )

    key, payload = load_call(cache, "abc")

    assert key == "abc123"
    assert payload == {
        "meta": {"key": "abc123"},
        "response": "one",
        "response_sha256": sha("one"),
    }
    with pytest.raises(FileNotFoundError, match="caller-cache/llm"):
        load_call(cache, "missing")
    atomic_write_json(
        cache / "llm" / "abc456.json",
        {
            "meta": {"key": "abc456"},
            "response": "two",
            "response_sha256": sha("two"),
        },
    )
    with pytest.raises(ValueError, match="abc123, abc456"):
        load_call(cache, "abc")


def test_load_call_rejects_corrupt_l1_payload_with_cache_integrity_error(
    tmp_path: Path,
) -> None:
    cache = tmp_path / "caller-cache"
    atomic_write_json(
        cache / "llm" / "abc123.json",
        {"meta": {"key": "abc123"}, "response": "one"},
    )

    with pytest.raises(CacheIntegrityError):
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


def test_trace_fails_closed_after_directory_replacement(tmp_path: Path, monkeypatch) -> None:
    artifacts = tmp_path / "artifacts"
    run_path = artifacts / "runs" / "run-1"
    _trace_manifest(run_path, ["original"])
    _sidecar(run_path, "original", components={"source": "original"})
    replacement = tmp_path / "replacement" / "run-1"
    _trace_manifest(replacement, ["external"])
    _sidecar(replacement, "external", components={"source": "external"})
    moved = tmp_path / "original-run"
    original_profile = inspect_module._load_run_profile_owned
    swapped = False

    def replace_before_profile(path: Path, store: AttemptStore, **kwargs: object) -> dict:
        nonlocal swapped
        if not swapped:
            run_path.rename(moved)
            replacement.rename(run_path)
            swapped = True
        return original_profile(path, store, **kwargs)

    monkeypatch.setattr(inspect_module, "_load_run_profile_owned", replace_before_profile)

    with pytest.raises(WorkflowProfileError, match="no longer owned|changed"):
        trace_run(artifacts, tmp_path / "llm", "run-1")


def test_diff_run_views_fails_closed_after_directory_replacement(
    tmp_path: Path, monkeypatch
) -> None:
    artifacts = tmp_path / "artifacts"
    run_a = artifacts / "runs" / "run-a"
    run_b = artifacts / "runs" / "run-b"
    _sidecar(run_a, "node", components={"source": "original"})
    _sidecar(run_b, "node", components={"source": "other"})
    (run_a / "node.json").write_text('{"value": "original"}', encoding="utf-8")
    (run_b / "node.json").write_text('{"value": "other"}', encoding="utf-8")
    replacement = tmp_path / "replacement" / "run-a"
    _sidecar(replacement, "node", components={"source": "external"})
    (replacement / "node.json").write_text('{"value": "external"}', encoding="utf-8")
    moved = tmp_path / "original-run-a"
    original_components = inspect_module._key_components_owned
    swapped = False

    def replace_before_components(path: Path, store: AttemptStore) -> dict:
        nonlocal swapped
        if path == run_a and not swapped:
            run_a.rename(moved)
            replacement.rename(run_a)
            swapped = True
        return original_components(path, store)

    monkeypatch.setattr(inspect_module, "_key_components_owned", replace_before_components)

    with pytest.raises(WorkflowProfileError, match="no longer owned|changed"):
        diff_run_views(artifacts, "run-a", "run-b")
