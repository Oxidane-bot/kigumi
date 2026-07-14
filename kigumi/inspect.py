"""Read-only joins over persisted run sidecars and L1 LLM payloads.

This module deliberately knows only the on-disk evidence layout.  It neither
imports the DAG runtime nor invokes a transport, so the project CLI can inspect
completed runs without reconstructing a caller or graph.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .store import run_directory


def trace_run(
    artifacts_path: Path, llm_cache_path: Path, run_id: str, node: str | None = None
) -> dict[str, Any]:
    """Join one run's sidecars to the corresponding L1 payload paths."""
    run_path = run_directory(artifacts_path, run_id)
    if not run_path.is_dir():
        raise FileNotFoundError(f"run not found: {run_id}")

    warnings: list[str] = []
    entries: dict[str, dict[str, Any]] = {}
    items_by_parent: dict[str, list[dict[str, Any]]] = {}
    for sidecar in sorted(run_path.glob("*.json.meta.json")):
        name = sidecar.name.removesuffix(".json.meta.json")
        if node is not None and name != node and not name.startswith(f"{node}@"):
            continue
        entry = _trace_entry(name, _read_json(sidecar), llm_cache_path, warnings)
        parent, separator, _item_id = name.partition("@")
        if separator:
            items_by_parent.setdefault(parent, []).append(entry)
        else:
            entries[name] = entry

    for parent, items in items_by_parent.items():
        entry = entries.get(parent)
        if entry is None:
            caches = {item["cache"] for item in items}
            policies = {item["cache_policy"] for item in items}
            entry = {
                "name": parent,
                "cache": caches.pop() if len(caches) == 1 else "mixed",
                "cache_policy": policies.pop() if len(policies) == 1 else "unknown",
                "outputs": sorted({output for item in items for output in item.get("outputs", [])}),
                "seconds": sum(item["seconds"] for item in items),
                "cache_key": None,
                "key_components": None,
                "calls": [],
            }
            entries[parent] = entry
        entry["items"] = sorted(items, key=lambda item: item["name"])

    if node is not None and not entries:
        raise FileNotFoundError(f"node not found in {run_id}: {node}")

    result: dict[str, Any] = {
        "run_id": run_id,
        "nodes": [entries[name] for name in sorted(entries)],
    }
    if warnings:
        result["warnings"] = warnings
    return result


def load_call(llm_cache_path: Path, key_prefix: str) -> tuple[str, dict[str, Any]]:
    """Load exactly one L1 payload selected by a cache-key prefix."""
    root = llm_cache_path / "llm"
    candidates = sorted(
        path for path in root.glob("*.json") if path.stem.startswith(key_prefix) and path.is_file()
    )
    if not candidates:
        raise FileNotFoundError(f"No LLM payload matching {key_prefix!r} under {root}")
    if len(candidates) > 1:
        keys = ", ".join(path.stem for path in candidates)
        raise ValueError(f"Ambiguous LLM cache key prefix {key_prefix!r}: {keys}")
    path = candidates[0]
    payload = _read_json(path)
    if not payload:
        raise ValueError(f"Invalid LLM payload: {path}")
    return path.stem, payload


def diff_components(artifacts_path: Path, run_a: str, run_b: str) -> dict[str, Any]:
    """Compare persisted key-component evidence without recomputing any keys."""
    components_a = _key_components_by_node(run_directory(artifacts_path, run_a))
    components_b = _key_components_by_node(run_directory(artifacts_path, run_b))
    shared = sorted(set(components_a) & set(components_b))
    result: dict[str, Any] = {}
    for name in shared:
        before, after = components_a[name], components_b[name]
        if before is None or after is None:
            result[name] = "unavailable"
            continue
        names = sorted(set(before) | set(after))
        result[name] = {
            "changed": [
                component for component in names if before.get(component) != after.get(component)
            ],
            "unchanged": [
                component for component in names if before.get(component) == after.get(component)
            ],
        }
    result["only_in_a"] = sorted(set(components_a) - set(components_b))
    result["only_in_b"] = sorted(set(components_b) - set(components_a))
    return result


def _trace_entry(
    name: str, metadata: dict[str, Any], llm_cache_path: Path, warnings: list[str]
) -> dict[str, Any]:
    calls = metadata.get("calls")
    traced_calls: list[dict[str, Any]] = []
    if isinstance(calls, list):
        for call in calls:
            if not isinstance(call, dict):
                continue
            key = call.get("key")
            payload_path: str | None = None
            if isinstance(key, str):
                candidate = llm_cache_path / "llm" / f"{key}.json"
                if candidate.is_file():
                    payload_path = str(candidate.resolve())
                else:
                    warnings.append(
                        f"LLM payload missing for key {key!r}; configure llm_cache_dir to match "
                        f"the LLMCaller cache_dir ({llm_cache_path})."
                    )
            else:
                warnings.append(
                    f"LLM call for node {name!r} has no key; cannot locate its payload under "
                    f"{llm_cache_path}."
                )
            traced_calls.append(
                {
                    "key": key,
                    "model_alias": call.get("model_alias"),
                    "model": call.get("model"),
                    "cache": call.get("cache"),
                    "prompt_sha": call.get("prompt_sha"),
                    "seconds": call.get("seconds"),
                    "usage": call.get("usage"),
                    "payload_path": payload_path,
                }
            )
    key_components = metadata.get("key_components")
    outputs = metadata.get("outputs")
    return {
        "name": name,
        "cache": metadata.get("cache", "unknown"),
        "cache_policy": metadata.get("cache_policy", "unknown"),
        "outputs": (
            sorted(output for output in outputs if isinstance(output, str))
            if isinstance(outputs, list)
            else []
        ),
        "seconds": metadata.get("seconds", 0),
        "cache_key": metadata.get("cache_key"),
        "key_components": key_components if isinstance(key_components, dict) else None,
        "calls": traced_calls,
    }


def _key_components_by_node(run_path: Path) -> dict[str, dict[str, Any] | None]:
    if not run_path.is_dir():
        return {}
    result: dict[str, dict[str, Any] | None] = {}
    for sidecar in run_path.glob("*.json.meta.json"):
        name = sidecar.name.removesuffix(".json.meta.json")
        key_components = _read_json(sidecar).get("key_components")
        result[name] = key_components if isinstance(key_components, dict) else None
    return result


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}
