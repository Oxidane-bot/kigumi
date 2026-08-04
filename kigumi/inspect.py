"""Read-only joins over persisted run sidecars and L1 LLM payloads.

This module deliberately knows only the on-disk evidence layout.  It neither
imports the DAG runtime nor invokes a transport, so the project CLI can inspect
completed runs without reconstructing a caller or graph.
"""

from __future__ import annotations

import copy
import json
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ._runstate import (
    ATTEMPT_RECEIPT_SCHEMA,
    RUN_MANIFEST_SCHEMA,
    AttemptStore,
    DurableRunSnapshot,
    RunManifestError,
)
from ._safe_io import SecureDirectory
from .artifacts import sha
from .calling import read_call_cache
from .errors import CacheIntegrityError
from .profile import (
    WORKFLOW_PROFILE_SCHEMA,
    WorkflowProfileError,
    _attach_runtime_prompt_resolutions,
    _attempts,
    _failures,
    _runtime_nodes,
)
from .retry import AmbiguousAttemptError
from .store import run_directory


def trace_run(
    artifacts_path: Path,
    llm_cache_path: Path,
    run_id: str,
    node: str | None = None,
    *,
    _store: AttemptStore | None = None,
) -> dict[str, Any]:
    """Join one run's sidecars to the corresponding L1 payload paths."""
    run_path = run_directory(artifacts_path, run_id)
    if _store is None:
        with _owned_run(run_path) as store:
            return _trace_run_owned(run_path, llm_cache_path, run_id, node, store)
    return _trace_run_owned(run_path, llm_cache_path, run_id, node, _store)


def _trace_run_owned(
    run_path: Path,
    llm_cache_path: Path,
    run_id: str,
    node: str | None,
    store: AttemptStore,
) -> dict[str, Any]:
    _require_run(store, run_id)
    snapshot = _snapshot_from_store(run_path, store)

    warnings: list[str] = []
    entries: dict[str, dict[str, Any]] = {}
    items_by_parent: dict[str, list[dict[str, Any]]] = {}
    sidecar_entries = (
        (name, pair["metadata"]) for name, pair in sorted(snapshot.materializations.items())
    )
    for name, metadata in sidecar_entries:
        if node is not None and name != node and not name.startswith(f"{node}@"):
            continue
        entry = _trace_entry(name, metadata, llm_cache_path, warnings)
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
    durable = _durable_run_state_owned(run_path, snapshot)
    if durable:
        result.update(durable)
    result["workflow_profile"] = _load_run_profile_owned(run_path, store, snapshot=snapshot)
    if warnings:
        result["warnings"] = warnings
    return result


def durable_run_state(
    run_path: Path,
    *,
    _snapshot: DurableRunSnapshot | None = None,
    _store: AttemptStore | None = None,
) -> dict[str, Any]:
    """Read supported durable run/attempt state without importing an executable DAG."""
    if _store is None:
        with _owned_run(run_path) as store:
            _require_run(store, run_path.name)
            snapshot = _snapshot or _snapshot_from_store(run_path, store)
            return _durable_run_state_owned(run_path, snapshot)
    _require_run(_store, run_path.name)
    snapshot = _snapshot or _snapshot_from_store(run_path, _store)
    return _durable_run_state_owned(run_path, snapshot)


def _durable_run_state_owned(
    run_path: Path,
    snapshot: DurableRunSnapshot,
) -> dict[str, Any]:
    if not snapshot.strict:
        raise WorkflowProfileError(f"Run {run_path.name!r} has no strict durable snapshot")
    manifest = snapshot.manifest
    attempts: list[dict[str, Any]] = []
    state_entries = (
        ((run_path / "attempts" / state["target_digest"] / "state.json"), state)
        for state in snapshot.states
    )
    for state_path, state in state_entries:
        if state.get("attempt_receipt_schema") != ATTEMPT_RECEIPT_SCHEMA:
            raise WorkflowProfileError(f"Attempt receipt {state_path} has unsupported schema")
        attempts.append(
            {
                key: state[key]
                for key in (
                    "target",
                    "attempt",
                    "status",
                    "side_effect_started",
                    "due_at",
                    "failure",
                    "policy_digest",
                    "resolution",
                    "active_effect",
                    "prompt_resolutions",
                )
                if key in state
            }
        )
    attempts.sort(key=lambda item: (str(item.get("target", "")), int(item.get("attempt", 0))))
    return {
        "run_status": manifest.get("status", "unknown"),
        "attempts": attempts,
        "retry_policy_digests": manifest.get("retry_policy_digests", {}),
        "evidence_policy_digests": manifest.get("evidence_policy_digests", {}),
        "pending_retries": manifest.get("pending_retries", []),
        "ambiguous_attempts": manifest.get("ambiguous_attempts", []),
        "resolution_status": "available",
    }


@contextmanager
def _owned_run(run_path: Path):
    """Keep one run's descriptor-bound ownership boundary for inspect reads."""
    try:
        store = AttemptStore(run_path, {})
    except (OSError, RunManifestError, ValueError) as error:
        raise WorkflowProfileError(
            f"Unable to inspect run {run_path.name!r} safely: {error}"
        ) from error
    try:
        yield store
    finally:
        for directory in (
            store._run_directory,  # noqa: SLF001
            store._runs_directory,  # noqa: SLF001
        ):
            if directory is not None:
                directory.close()


def _require_run(store: AttemptStore, run_id: str) -> None:
    if store._run_directory is None:  # noqa: SLF001
        raise FileNotFoundError(f"run not found: {run_id}")


def _snapshot_from_store(run_path: Path, store: AttemptStore) -> DurableRunSnapshot:
    """Validate all durable evidence through one already-bound run store."""
    manifest_path = run_path / "_run.json"
    try:
        manifest, corrupted = store._read_owned_json(manifest_path)  # noqa: SLF001
        if corrupted:
            raise store._owned_integrity_error(  # noqa: SLF001
                manifest_path,
                RUN_MANIFEST_SCHEMA,
            )
        if manifest is None:
            raise RunManifestError(f"Missing or invalid run manifest: {manifest_path}")
        if manifest.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA:
            raise RunManifestError(f"Run {run_path.name!r} has an unsupported manifest schema")
        validated_manifest = store._required_manifest()  # noqa: SLF001
        states = store._validate_all_attempt_receipts()  # noqa: SLF001
        candidates, materializations = store._validate_run_materializations(  # noqa: SLF001
            states,
            manifest=validated_manifest,
        )
    except (AmbiguousAttemptError, OSError, RunManifestError, ValueError) as error:
        raise WorkflowProfileError(
            f"Run {run_path.name!r} durable receipt integrity validation failed: {error}"
        ) from error
    return DurableRunSnapshot(
        validated_manifest,
        tuple(states),
        candidates,
        materializations,
        True,
    )


def _load_run_profile_owned(
    run_path: Path,
    store: AttemptStore,
    *,
    snapshot: DurableRunSnapshot | None = None,
    include_content: bool = False,
) -> dict[str, Any]:
    """Project a profile from one validated snapshot and its still-open store."""
    _require_run(store, run_path.name)
    snapshot = snapshot or _snapshot_from_store(run_path, store)
    if not snapshot.strict:
        raise WorkflowProfileError(f"Run {run_path.name!r} has no strict durable snapshot")
    manifest = snapshot.manifest
    if manifest.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA:
        raise WorkflowProfileError(
            f"Run {run_path.name!r} has no supported manifest for WorkflowProfile"
        )
    static = manifest.get("workflow_profile")
    if not isinstance(static, dict):
        raise WorkflowProfileError("0.7 run manifest is missing workflow_profile")
    if manifest.get("workflow_profile_digest") != sha(static):
        raise WorkflowProfileError("0.7 workflow_profile digest validation failed")
    if static.get("workflow_profile_schema") != WORKFLOW_PROFILE_SCHEMA:
        raise WorkflowProfileError("unsupported workflow_profile schema")
    runtime_nodes = _runtime_nodes(
        run_path,
        include_content=include_content,
        materializations=snapshot.materializations,
    )
    attempt_entries = _attempts(
        run_path,
        include_content=include_content,
        states=snapshot.states,
        candidates=snapshot.candidates,
    )
    failures = _failures(
        run_path,
        include_content=include_content,
        attempts=store,
    )
    result = copy.deepcopy(static)
    result["mode"] = "run"
    result["resolution_status"] = "available"
    _attach_runtime_prompt_resolutions(
        result.get("prompts"),
        runtime_nodes,
        attempt_entries,
    )
    result["run"] = {
        "run_id": run_path.name,
        "status": manifest.get("status", "unknown"),
        "resume_count": manifest.get("resume_count", 0),
        "last_resumed_at": manifest.get("last_resumed_at"),
        "nodes": runtime_nodes,
        "attempts": attempt_entries,
        "failures": failures,
        "pending_retries": [
            copy.deepcopy(attempt)
            for attempt in attempt_entries
            if attempt.get("status") == "retry_scheduled"
        ],
        "ambiguous_attempts": [
            copy.deepcopy(attempt)
            for attempt in attempt_entries
            if attempt.get("status") == "ambiguous"
        ],
    }
    return result


def load_call(llm_cache_path: Path, key_prefix: str) -> tuple[str, dict[str, Any]]:
    """Load exactly one L1 payload selected by a cache-key prefix."""
    root = llm_cache_path / "llm"
    try:
        with SecureDirectory(root, create=False) as directory:
            candidates = sorted(
                root / name
                for name in directory.names()
                if name.endswith(".json") and Path(name).stem.startswith(key_prefix)
            )
    except FileNotFoundError:
        candidates = []
    except OSError as error:
        raise ValueError(f"Unable to inspect LLM cache directory {root}: {error}") from error
    if not candidates:
        raise FileNotFoundError(f"No LLM payload matching {key_prefix!r} under {root}")
    if len(candidates) > 1:
        keys = ", ".join(path.stem for path in candidates)
        raise ValueError(f"Ambiguous LLM cache key prefix {key_prefix!r}: {keys}")
    path = candidates[0]
    lookup = read_call_cache(path)
    if lookup.state == "MISSING":
        raise FileNotFoundError(f"No LLM payload matching {key_prefix!r} under {root}")
    if lookup.state == "CORRUPT":
        raise CacheIntegrityError(path, lookup)
    assert isinstance(lookup.data, dict)
    return path.stem, lookup.data


def diff_components(artifacts_path: Path, run_a: str, run_b: str) -> dict[str, Any]:
    """Compare persisted key-component evidence without recomputing any keys."""
    with (
        _owned_run(run_directory(artifacts_path, run_a)) as store_a,
        _owned_run(run_directory(artifacts_path, run_b)) as store_b,
    ):
        components_a = _key_components_owned(
            run_directory(artifacts_path, run_a),
            store_a,
        )
        components_b = _key_components_owned(
            run_directory(artifacts_path, run_b),
            store_b,
        )
    return _component_diff(components_a, components_b)


def diff_run_views(
    artifacts_path: Path,
    run_a: str,
    run_b: str,
) -> tuple[dict[str, list[str]], dict[str, Any]]:
    """Read artifact and key-component diffs from two bound run snapshots."""
    run_path_a = run_directory(artifacts_path, run_a)
    run_path_b = run_directory(artifacts_path, run_b)
    with _owned_run(run_path_a) as store_a, _owned_run(run_path_b) as store_b:
        _require_run(store_a, run_a)
        _require_run(store_b, run_b)
        artifacts_a = _run_artifacts_owned(run_path_a, store_a)
        artifacts_b = _run_artifacts_owned(run_path_b, store_b)
        components_a = _key_components_owned(run_path_a, store_a)
        components_b = _key_components_owned(run_path_b, store_b)
    artifact_names_a = set(artifacts_a)
    artifact_names_b = set(artifacts_b)
    shared = sorted(artifact_names_a & artifact_names_b)
    result = {
        "changed": [name for name in shared if sha(artifacts_a[name]) != sha(artifacts_b[name])],
        "only_a": sorted(artifact_names_a - artifact_names_b),
        "only_b": sorted(artifact_names_b - artifact_names_a),
    }
    return result, _component_diff(components_a, components_b)


def _component_diff(
    components_a: dict[str, dict[str, Any] | None],
    components_b: dict[str, dict[str, Any] | None],
) -> dict[str, Any]:
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
                lookup = read_call_cache(candidate)
                if lookup.state == "VALID":
                    payload_path = str(candidate.resolve())
                elif lookup.state == "MISSING":
                    warnings.append(
                        f"LLM payload missing for key {key!r}; configure llm_cache_dir to match "
                        f"the LLMCaller cache_dir ({llm_cache_path})."
                    )
                else:
                    raise CacheIntegrityError(candidate, lookup)
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
                    "managed": isinstance(call.get("prompt_resolution"), dict),
                    "prompt_resolution": call.get("prompt_resolution"),
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
    """Compatibility wrapper that owns the run for the full component read."""
    with _owned_run(run_path) as store:
        return _key_components_owned(run_path, store)


def _key_components_owned(
    run_path: Path,
    store: AttemptStore,
) -> dict[str, dict[str, Any] | None]:
    result: dict[str, dict[str, Any] | None] = {}
    try:
        names = sorted(
            name for name in store._owned_names(run_path) if name.endswith(".json.meta.json")
        )  # noqa: SLF001
    except FileNotFoundError:
        return result
    except (OSError, RunManifestError, ValueError) as error:
        raise WorkflowProfileError(
            f"Run {run_path.name!r} sidecar directory is not owned: {error}"
        ) from error
    for name in names:
        sidecar = run_path / name
        try:
            info = store._owned_stat(sidecar)  # noqa: SLF001
        except FileNotFoundError:
            continue
        except (OSError, RunManifestError, ValueError) as error:
            raise WorkflowProfileError(
                f"Unable to inspect sidecar path {sidecar}: {error}"
            ) from error
        if stat.S_ISLNK(info.st_mode):
            raise WorkflowProfileError(f"Run sidecar must not be a symlink: {sidecar}")
        if not stat.S_ISREG(info.st_mode):
            raise WorkflowProfileError(f"Run sidecar must reference a regular file: {sidecar}")
        raw, read_error = store._read_owned_bytes(sidecar)  # noqa: SLF001
        if read_error is not None:
            raise WorkflowProfileError(f"Unable to read run sidecar {sidecar}: {read_error}")
        if raw is None:
            continue
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            value = {}
        key_components = value.get("key_components") if isinstance(value, dict) else None
        result[name.removesuffix(".json.meta.json")] = (
            key_components if isinstance(key_components, dict) else None
        )
    return result


def _run_artifacts_owned(
    run_path: Path,
    store: AttemptStore,
) -> dict[str, dict[str, Any]]:
    """Read artifact JSON while the owning run descriptor remains open."""
    artifacts: dict[str, dict[str, Any]] = {}
    try:
        names = sorted(store._owned_names(run_path))  # noqa: SLF001
    except FileNotFoundError:
        return artifacts
    except (OSError, RunManifestError, ValueError) as error:
        raise WorkflowProfileError(
            f"Run {run_path.name!r} artifact directory is not owned: {error}"
        ) from error
    for name in names:
        if name.startswith("_") or not name.endswith(".json"):
            continue
        if name.endswith(".json.meta.json"):
            continue
        path = run_path / name
        try:
            info = store._owned_stat(path)  # noqa: SLF001
        except FileNotFoundError:
            continue
        except (OSError, RunManifestError, ValueError) as error:
            raise WorkflowProfileError(
                f"Unable to inspect artifact path {path}: {error}"
            ) from error
        if stat.S_ISLNK(info.st_mode):
            raise WorkflowProfileError(f"Run artifact must not be a symlink: {path}")
        if not stat.S_ISREG(info.st_mode):
            raise WorkflowProfileError(f"Run artifact must reference a regular file: {path}")
        raw, read_error = store._read_owned_bytes(path)  # noqa: SLF001
        if read_error is not None:
            raise WorkflowProfileError(f"Unable to read run artifact {path}: {read_error}")
        if raw is None:
            continue
        try:
            artifact = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(artifact, dict):
            artifacts[Path(name).stem] = artifact
    return artifacts
