"""Canonical static and persisted runtime workflow profile projections."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from .artifacts import sha
from .prompt import PromptResolutionError, validate_prompt_resolution_record

WORKFLOW_PROFILE_SCHEMA = 1


class WorkflowProfileError(RuntimeError):
    """Raised when a schema-1 WorkflowProfile or 0.7 receipt fails closed."""


def load_run_profile(run_path: Path, *, include_content: bool = False) -> dict[str, Any]:
    """Load a runtime profile without importing or executing the registered DAG."""
    manifest = _read_object(run_path / "_run.json")
    schema = manifest.get("run_manifest_schema")
    if schema == 1:
        return {
            "workflow_profile_schema": WORKFLOW_PROFILE_SCHEMA,
            "mode": "legacy",
            "resolution_status": "unavailable_legacy",
            "graph": {"nodes": [], "edges": [], "mounts": [], "models": {}},
            "prompts": {"specs": []},
            "run": {
                "run_id": run_path.name,
                "status": manifest.get("status", "unknown"),
                "nodes": _legacy_nodes(run_path),
                "attempts": [],
            },
        }
    if schema != 2:
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
    runtime_nodes = _runtime_nodes(run_path, include_content=include_content)
    attempts = _attempts(run_path, include_content=include_content)
    failures = _failures(run_path, include_content=include_content)
    result = copy.deepcopy(static)
    result["mode"] = "run"
    result["resolution_status"] = "available"
    _attach_runtime_prompt_resolutions(
        result.get("prompts"),
        runtime_nodes,
        attempts,
    )
    result["run"] = {
        "run_id": run_path.name,
        "status": manifest.get("status", "unknown"),
        "resume_count": manifest.get("resume_count", 0),
        "last_resumed_at": manifest.get("last_resumed_at"),
        "nodes": runtime_nodes,
        "attempts": attempts,
        "failures": failures,
        "pending_retries": [
            copy.deepcopy(attempt)
            for attempt in attempts
            if attempt.get("status") == "retry_scheduled"
        ],
        "ambiguous_attempts": [
            copy.deepcopy(attempt) for attempt in attempts if attempt.get("status") == "ambiguous"
        ],
    }
    return result


def render_profile_mermaid(profile: dict[str, Any], *, prompts: bool = True) -> str:
    """Render graph and optional Prompt binding edges from one canonical profile."""
    graph = profile.get("graph", {})
    nodes = graph.get("nodes", []) if isinstance(graph, dict) else []
    node_ids = {
        str(node["name"]): f"n_{sha(str(node['name']))[:12]}"
        for node in nodes
        if isinstance(node, dict) and isinstance(node.get("name"), str)
    }
    lines = ["flowchart TD"]
    for node in nodes:
        if not isinstance(node, dict) or node.get("name") not in node_ids:
            continue
        name = str(node["name"])
        kind = str(node.get("kind", "node"))
        lines.append(f'{node_ids[name]}["{_mermaid_text(name)}<br/>[{_mermaid_text(kind)}]"]')
    edges = graph.get("edges", []) if isinstance(graph, dict) else []
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        source = edge.get("from")
        target = edge.get("to")
        role = edge.get("role")
        if source not in node_ids or target not in node_ids:
            continue
        if role in {"selector", "material"} and not prompts:
            continue
        label = str(role or "dependency")
        if edge.get("path"):
            label += ":" + "/".join(str(part) for part in edge["path"])
        style = "-." if role in {"items_from", "carry_from", "selector", "material"} else "--"
        if style == "--":
            lines.append(f"{node_ids[source]} --> {node_ids[target]}")
        else:
            lines.append(f'{node_ids[source]} -. "{_mermaid_text(label)}" .-> {node_ids[target]}')
    return "\n".join(lines)


def render_profile_markdown(profile: dict[str, Any]) -> str:
    """Render the canonical profile as Mermaid plus one complete Prompt table."""
    lines = [
        f"# Workflow Profile ({profile.get('mode', 'static')})",
        "",
        "```mermaid",
        render_profile_mermaid(profile, prompts=True),
        "```",
        "",
        "## Prompts",
        "",
        "| 节点 | PromptSpec | Base | Layers | Axes | Materials | Resolution |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    prompts = profile.get("prompts", {})
    specs = prompts.get("specs", []) if isinstance(prompts, dict) else []
    for entry in specs:
        if not isinstance(entry, dict):
            continue
        declaration = entry.get("declaration", {})
        spec = declaration.get("spec", {}) if isinstance(declaration, dict) else {}
        layers = spec.get("layers", []) if isinstance(spec, dict) else []
        materials = spec.get("materials", []) if isinstance(spec, dict) else []
        axes = [
            layer["source"]["name"]
            for layer in layers
            if isinstance(layer, dict)
            and isinstance(layer.get("source"), dict)
            and layer["source"].get("kind") == "axis"
        ]
        runtime = entry.get("runtime", [])
        actual = sorted(
            {
                f"{axis.get('name')}={axis.get('selected')}"
                for binding in runtime
                if isinstance(binding, dict)
                for resolution in (binding.get("current"), binding.get("origin"))
                if isinstance(resolution, dict)
                for axis in resolution.get("axes", [])
                if isinstance(axis, dict)
            }
        )
        resolution_status = str(entry.get("resolution_status", "unresolved"))
        if actual:
            resolution_status += " (" + ", ".join(actual) + ")"
        lines.append(
            "| "
            + " | ".join(
                _markdown_text(value)
                for value in (
                    entry.get("node", ""),
                    entry.get("name", ""),
                    spec.get("base", {}).get("name", "")
                    if isinstance(spec.get("base"), dict)
                    else "",
                    ", ".join(
                        str(layer.get("slot", "")) for layer in layers if isinstance(layer, dict)
                    ),
                    ", ".join(str(axis) for axis in axes),
                    ", ".join(
                        str(material.get("slot", ""))
                        for material in materials
                        if isinstance(material, dict)
                    ),
                    resolution_status,
                )
            )
            + " |"
        )
    return "\n".join(lines)


def _runtime_nodes(run_path: Path, *, include_content: bool) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    for sidecar in sorted(run_path.glob("*.json.meta.json")):
        name = sidecar.name.removesuffix(".json.meta.json")
        metadata = _read_object(sidecar)
        artifact = _read_object(run_path / f"{name}.json")
        artifact_digest = sha(artifact)
        origin = metadata.get("origin_provenance")
        if (
            metadata.get("run_sidecar_schema") != 2
            or metadata.get("artifact_sha256") != artifact_digest
            or not isinstance(origin, dict)
            or origin.get("artifact_sha256") != artifact_digest
            or metadata.get("origin_provenance_digest") != sha(origin)
            or metadata.get("prompt_resolutions_digest")
            != sha(metadata.get("prompt_resolutions", {}))
        ):
            raise WorkflowProfileError(
                f"Run receipt for {name!r} failed artifact or receipt digest binding"
            )
        current = _validated_resolutions(metadata.get("prompt_resolutions"), name)
        origin_resolutions = _validated_resolutions(origin.get("prompt_resolutions"), name)
        calls = _validated_calls(metadata.get("calls"), f"Run receipt for {name!r}")
        call_entries = _profile_calls(calls, include_content=include_content)
        parent, separator, item_id = name.partition("@")
        origin_calls = _validated_calls(
            origin.get("calls"),
            f"Origin provenance for {name!r}",
        )
        node_entry = {
            "name": parent,
            "target": name,
            "item": item_id if separator else None,
            "status": "success",
            "cache": metadata.get("cache", "unknown"),
            "cache_policy": metadata.get("cache_policy", "unknown"),
            "cache_key": copy.deepcopy(metadata.get("cache_key")),
            "current_prompt_resolutions": current,
            "origin_prompt_resolutions": origin_resolutions,
            "calls": call_entries,
            "origin_calls": _profile_calls(
                origin_calls,
                include_content=include_content,
            ),
            "artifact_sha256": artifact_digest,
            "seconds": metadata.get("seconds", 0),
        }
        agent = origin.get("agent")
        if isinstance(agent, dict):
            agent_resolution = agent.get("prompt_resolution")
            if agent_resolution is not None:
                _validate_resolution(agent_resolution, f"{name!r} Agent")
            node_entry["agent"] = {
                "executed": metadata.get("cache") != "hit",
                "managed": agent_resolution is not None,
                "resolution_status": ("managed" if agent_resolution is not None else "unmanaged"),
                "prompt_resolution": copy.deepcopy(agent_resolution),
                "instruction_sha256": agent.get("instruction_sha256"),
                "usage": copy.deepcopy(agent.get("usage")),
                "exit_reason": agent.get("exit_reason"),
            }
            if include_content:
                node_entry["agent"]["instruction_evidence"] = copy.deepcopy(
                    agent.get("instruction_evidence")
                )
        nodes.append(node_entry)
    return nodes


def _attempts(
    run_path: Path,
    *,
    include_content: bool,
) -> list[dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    for path in sorted((run_path / "attempts").glob("*/state.json")):
        state = _read_object(path)
        if state.get("attempt_receipt_schema") != 2:
            raise WorkflowProfileError(f"Attempt receipt {path} has unsupported schema")
        target = state.get("target")
        if not isinstance(target, str) or state.get("target_digest") != sha(target):
            raise WorkflowProfileError(f"Attempt receipt {path} has no target")
        _validated_resolutions(state.get("prompt_resolutions"), target)
        active = state.get("active_effect")
        if isinstance(active, dict) and active.get("prompt_resolution") is not None:
            _validate_resolution(active["prompt_resolution"], f"{target!r} active effect")
        calls = _validated_calls(state.get("calls", []), f"{target!r} attempt")
        candidate_file = state.get("candidate_file")
        candidate_calls: list[dict[str, Any]] = []
        if candidate_file is not None:
            candidate = _read_object(path.parent / str(candidate_file))
            if (
                candidate.get("candidate_schema") != 2
                or state.get("candidate_sha256") != sha(candidate)
                or not isinstance(candidate.get("artifact"), dict)
            ):
                raise WorkflowProfileError(f"Success candidate for {target!r} is corrupt")
            candidate_resolutions = _validated_resolutions(
                candidate.get("prompt_resolutions"),
                f"{target!r} success candidate",
            )
            if candidate_resolutions != state.get("prompt_resolutions"):
                raise WorkflowProfileError(
                    f"Success candidate for {target!r} changed Prompt resolutions"
                )
            candidate_calls = _validated_calls(
                candidate.get("calls", []),
                f"{target!r} success candidate",
            )
        if not calls and candidate_calls:
            calls = candidate_calls
        attempt = {
            key: copy.deepcopy(state[key])
            for key in (
                "target",
                "attempt",
                "status",
                "side_effect_started",
                "active_effect",
                "prompt_resolutions",
                "failure",
                "due_at",
                "resolution",
            )
            if key in state
        }
        attempt["calls"] = _profile_calls(calls, include_content=include_content)
        attempts.append(attempt)
    attempts.sort(key=lambda item: (str(item["target"]), int(item.get("attempt", 0))))
    return attempts


def _failures(run_path: Path, *, include_content: bool) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    for path in sorted((run_path / "failures").glob("*.json")):
        receipt = _read_object(path)
        if receipt.get("failure_schema") != 2:
            raise WorkflowProfileError(f"Failure receipt {path} has unsupported schema")
        resolution = receipt.get("prompt_resolution")
        if resolution is not None:
            _validate_resolution(resolution, f"{path.stem!r} failure")
        entry = {
            key: copy.deepcopy(receipt[key])
            for key in (
                "node",
                "status",
                "failure",
                "usage",
                "stop_reason",
                "duration_seconds",
                "instruction_sha256",
                "prompt_resolution",
                "evidence_policy",
                "evidence_policy_digest",
            )
            if key in receipt
        }
        entry["managed"] = resolution is not None
        entry["resolution_status"] = "managed" if resolution is not None else "unmanaged"
        if include_content:
            entry["instruction_evidence"] = copy.deepcopy(receipt.get("instruction_evidence"))
        failures.append(entry)
    return failures


def _legacy_nodes(run_path: Path) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    for sidecar in sorted(run_path.glob("*.json.meta.json")):
        try:
            metadata = _read_object(sidecar)
        except WorkflowProfileError:
            continue
        calls = metadata.get("calls", [])
        safe_calls = (
            _profile_calls(
                [call for call in calls if isinstance(call, dict)],
                include_content=False,
            )
            if isinstance(calls, list)
            else []
        )
        nodes.append(
            {
                "name": sidecar.name.removesuffix(".json.meta.json").partition("@")[0],
                "target": sidecar.name.removesuffix(".json.meta.json"),
                "cache": metadata.get("cache", "unknown"),
                "seconds": metadata.get("seconds", 0),
                "calls": safe_calls,
                "resolution_status": "unavailable_legacy",
            }
        )
    return nodes


def _validated_resolutions(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise WorkflowProfileError(f"Prompt resolutions for {context!r} are invalid")
    for resolution in value.values():
        _validate_resolution(resolution, context)
    return copy.deepcopy(value)


def _validated_calls(value: Any, context: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise WorkflowProfileError(f"{context} has invalid calls")
    calls: list[dict[str, Any]] = []
    for index, call in enumerate(value):
        if not isinstance(call, dict):
            raise WorkflowProfileError(f"{context} CALL {index} is invalid")
        resolution = call.get("prompt_resolution")
        if resolution is not None:
            _validate_resolution(resolution, f"{context} CALL {index}")
        calls.append(copy.deepcopy(call))
    return calls


def _profile_calls(
    calls: list[dict[str, Any]],
    *,
    include_content: bool,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for index, call in enumerate(calls):
        resolution = call.get("prompt_resolution")
        entry = {
            "call_index": index,
            "managed": resolution is not None,
            "resolution_status": "managed" if resolution is not None else "unmanaged",
            "prompt_resolution": copy.deepcopy(resolution),
            "prompt_sha": call.get("prompt_sha"),
            "model": call.get("model"),
            "cache": call.get("cache"),
            "usage": copy.deepcopy(call.get("usage")),
            "phase": resolution.get("phase") if isinstance(resolution, dict) else None,
            "repair_round": (
                resolution.get("repair_round") if isinstance(resolution, dict) else None
            ),
        }
        if include_content:
            entry["request_evidence"] = copy.deepcopy(call.get("request_evidence"))
            entry["response_evidence"] = copy.deepcopy(call.get("response_evidence"))
        entries.append(entry)
    return entries


def _attach_runtime_prompt_resolutions(
    prompts: Any,
    nodes: list[dict[str, Any]],
    attempts: list[dict[str, Any]],
) -> None:
    if not isinstance(prompts, dict) or not isinstance(prompts.get("specs"), list):
        raise WorkflowProfileError("0.7 workflow_profile has invalid Prompt declarations")
    attempts_by_target = {
        str(attempt["target"]): attempt
        for attempt in attempts
        if isinstance(attempt.get("target"), str)
    }
    for entry in prompts["specs"]:
        if not isinstance(entry, dict):
            raise WorkflowProfileError("0.7 workflow_profile has an invalid PromptSpec")
        node_name = entry.get("node")
        spec_name = entry.get("name")
        if not isinstance(node_name, str) or not isinstance(spec_name, str):
            raise WorkflowProfileError("0.7 workflow_profile has an invalid PromptSpec")
        runtime: list[dict[str, Any]] = []
        seen: set[str] = set()
        for node in nodes:
            if node.get("name") != node_name:
                continue
            current = node.get("current_prompt_resolutions", {})
            origin = node.get("origin_prompt_resolutions", {})
            current_resolution = current.get(spec_name) if isinstance(current, dict) else None
            origin_resolution = origin.get(spec_name) if isinstance(origin, dict) else None
            if current_resolution is None and origin_resolution is None:
                continue
            target = str(node.get("target", node_name))
            runtime.append(
                {
                    "target": target,
                    "current": copy.deepcopy(current_resolution),
                    "origin": copy.deepcopy(origin_resolution),
                }
            )
            seen.add(target)
        for target, attempt in attempts_by_target.items():
            if target in seen or target.partition("@")[0] != node_name:
                continue
            resolutions = attempt.get("prompt_resolutions", {})
            resolution = resolutions.get(spec_name) if isinstance(resolutions, dict) else None
            if resolution is not None:
                runtime.append(
                    {
                        "target": target,
                        "current": copy.deepcopy(resolution),
                        "origin": None,
                    }
                )
        runtime.sort(key=lambda item: item["target"])
        entry["runtime"] = runtime
        entry["resolution_status"] = "resolved" if runtime else "not_executed"


def _validate_resolution(value: Any, context: str) -> None:
    try:
        validate_prompt_resolution_record(value)
    except PromptResolutionError as error:
        raise WorkflowProfileError(f"Prompt resolution for {context}: {error}") from error


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError) as error:
        raise WorkflowProfileError(f"Missing or invalid JSON receipt: {path}") from error
    if not isinstance(value, dict):
        raise WorkflowProfileError(f"JSON receipt must be an object: {path}")
    return value


def _markdown_text(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", "<br/>")


def _mermaid_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
