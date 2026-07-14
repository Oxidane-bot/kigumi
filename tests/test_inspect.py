from __future__ import annotations

from pathlib import Path

import pytest

from kigumi.artifacts import atomic_write_json, canonical_json, write_artifact
from kigumi.inspect import diff_components, load_call, trace_run


def _sidecar(
    root: Path,
    name: str,
    *,
    cache: str = "miss",
    components: dict[str, str] | None = None,
    calls: list[dict[str, object]] | None = None,
) -> None:
    metadata: dict[str, object] = {
        "node": name,
        "cache_key": f"node-{name}",
        "cache": cache,
        "seconds": 1.25,
        "calls": calls or [],
    }
    if components is not None:
        metadata["key_components"] = components
    write_artifact(root / f"{name}.json", canonical_json({"name": name}), metadata)


def test_trace_run_groups_map_items_and_links_llm_payloads(tmp_path: Path) -> None:
    """Trace stays a read-only join over persisted run and L1 evidence."""
    artifacts = tmp_path / "artifacts"
    llm_cache = tmp_path / "caller-cache"
    run_root = artifacts / "runs" / "run-7"
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
