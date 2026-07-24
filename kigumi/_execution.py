"""Private execution envelope shared by DAG node paths."""

from __future__ import annotations

import copy
import json
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from . import store
from ._declarations import CachePolicy
from .artifacts import atomic_write_json, canonical_json, sha
from .errors import OutputOwnershipError
from .evidence import EvidencePolicy

_DEFAULT_EVIDENCE_POLICY = EvidencePolicy()


class ExecutionEnvelope:
    """Own the shared cache, pending, materialization, and sidecar mechanics."""

    def __init__(
        self,
        *,
        artifacts_path: Path,
        run_id: str,
        resolve: Callable[[Path], Path],
        blob_store: Any,
        ensure_archive_id: Callable[[], str],
        approval_path: Callable[[str], Path],
    ) -> None:
        self.artifacts_path = artifacts_path
        self.run_id = run_id
        self.resolve = resolve
        self.blob_store = blob_store
        self.ensure_archive_id = ensure_archive_id
        self.approval_path = approval_path
        self._output_lock = threading.Lock()
        self._output_owners: list[tuple[Path, str]] = []

    def lookup(
        self,
        cache_key: str,
        *,
        forced: bool,
        cache_policy: CachePolicy = "auto",
        evidence_policy_digest: str | None = None,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Return a node-cache artifact unless this execution is forced."""
        if forced or cache_policy != "auto":
            return None, False
        artifact = store.read_node_cache(self.artifacts_path, cache_key)
        if artifact is not None and evidence_policy_digest is not None:
            origin = store.read_node_cache_origin(self.artifacts_path, cache_key)
            if origin is None or origin.get("evidence_policy_digest") != evidence_policy_digest:
                return None, False
        return artifact, artifact is not None

    def seal(
        self,
        artifact: Any,
        cache_key: str,
        *,
        label: str,
        calls: list[dict[str, Any]] | None = None,
        cache_policy: CachePolicy = "auto",
        evidence_policy: EvidencePolicy = _DEFAULT_EVIDENCE_POLICY,
        agent_provenance: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Validate, canonicalize, and persist one cacheable artifact."""
        if not isinstance(artifact, dict):
            raise TypeError(f"{label} must return a dict artifact")
        sealed = json.loads(canonical_json(artifact))
        if cache_policy != "off":
            store.write_node_cache(
                self.artifacts_path,
                cache_key,
                sealed,
                _origin_provenance(
                    sealed,
                    calls or [],
                    evidence_policy=evidence_policy,
                    agent_provenance=agent_provenance,
                ),
            )
        return sealed

    def record_pending(self, name: str, payload: Any) -> None:
        """Persist a checkpoint payload before returning it to the scheduler."""
        atomic_write_json(self.approval_path(name).with_suffix(".pending.json"), payload)

    def materialize(
        self, label: str, artifact: dict[str, Any], *, allow_item_owners: bool = False
    ) -> list[str]:
        """Materialize declared files and blobs for a completed artifact."""
        return store.materialize_artifact(
            artifact,
            label,
            self.resolve,
            self.blob_store,
            lambda outputs: self._claim_outputs(label, outputs, allow_item_owners),
        )

    def _claim_outputs(
        self, label: str, outputs: tuple[Path, ...], allow_item_owners: bool
    ) -> None:
        """Atomically reserve a complete artifact output set for this run."""
        with self._output_lock:
            for path in outputs:
                for owned_path, owner in self._output_owners:
                    if not store.output_paths_equivalent(path, owned_path):
                        continue
                    allowed_child = (
                        allow_item_owners
                        and owner.startswith(f"{label}@")
                        and len(owner) > len(label) + 1
                    )
                    if owner != label and not allowed_child:
                        relative_path = path.relative_to(self.resolve(Path("."))).as_posix()
                        raise OutputOwnershipError(
                            f"Output path {relative_path!r} is owned by {owner!r}; "
                            f"{label!r} cannot claim it"
                        )
            for path in outputs:
                if not any(
                    store.output_paths_equivalent(path, owned_path)
                    for owned_path, _owner in self._output_owners
                ):
                    self._output_owners.append((path, label))

    def write_sidecar(
        self,
        label: str,
        artifact: dict[str, Any],
        cache_key: str | list[str],
        *,
        cache_hit: bool,
        seconds: float,
        calls: list[dict[str, Any]],
        key_components: dict[str, str] | None = None,
        outputs: list[str] | tuple[str, ...] = (),
        cache_policy: CachePolicy = "auto",
        evidence_policy: EvidencePolicy = _DEFAULT_EVIDENCE_POLICY,
        origin_provenance: dict[str, Any] | None = None,
        agent_provenance: dict[str, Any] | None = None,
    ) -> None:
        """Persist one run artifact and its deterministic metadata shape."""
        origin = (
            store.read_node_cache_origin(self.artifacts_path, cache_key)
            if cache_hit and isinstance(cache_key, str)
            else origin_provenance
            or _origin_provenance(
                artifact,
                calls,
                evidence_policy=evidence_policy,
                agent_provenance=agent_provenance,
            )
        )
        if origin is None or origin.get("artifact_sha256") != sha(artifact):
            raise ValueError("run sidecar cannot resolve hash-bound origin provenance")
        metadata: dict[str, Any] = {
            "node": label,
            "cache_key": cache_key,
            "cache": "hit" if cache_hit else "miss",
            "cache_policy": cache_policy,
            "outputs": sorted(outputs),
            "seconds": seconds,
            "calls": copy.deepcopy(calls),
            "execution_calls": copy.deepcopy(calls),
            "origin_provenance": copy.deepcopy(origin),
            "artifact_sha256": origin["artifact_sha256"],
            "prompt_sha256": origin["prompt_sha256"],
            "model": origin["model"],
            "params": copy.deepcopy(origin["params"]),
            "provider_response_id": origin["provider_response_id"],
            "usage": copy.deepcopy(origin["usage"]),
        }
        if key_components is not None:
            metadata["key_components"] = key_components
        store.write_run_artifact(
            self.artifacts_path,
            self.run_id,
            label,
            artifact,
            metadata,
            self.ensure_archive_id,
        )


def _origin_provenance(
    artifact: dict[str, Any],
    calls: list[dict[str, Any]],
    *,
    evidence_policy: EvidencePolicy = _DEFAULT_EVIDENCE_POLICY,
    agent_provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    copied_calls = copy.deepcopy(calls)
    agent = artifact.get("agent")
    spec = agent.get("spec") if isinstance(agent, dict) else None
    if copied_calls:
        primary = copied_calls[0] if len(copied_calls) == 1 else None
        kind = "call"
        prompt_sha256 = primary.get("prompt_sha") if isinstance(primary, dict) else None
        model = primary.get("model") if isinstance(primary, dict) else None
        params = primary.get("params") if isinstance(primary, dict) else {}
        provider_response_id = (
            primary.get("provider_response_id") if isinstance(primary, dict) else None
        )
        usage = primary.get("usage") if isinstance(primary, dict) else None
        retained_agent_provenance = None
    elif artifact.get("agent_schema") == 2 and isinstance(spec, dict):
        kind = "agent"
        prompt_sha256 = (
            agent_provenance.get("instruction_sha256")
            if isinstance(agent_provenance, dict)
            else None
        )
        model = spec.get("model") if isinstance(spec.get("model"), str) else None
        params = {
            "provider": spec.get("provider"),
            "thinking": spec.get("thinking"),
            "tools": copy.deepcopy(spec.get("tools")),
            "limits": copy.deepcopy(spec.get("limits")),
        }
        provider_response_id = None
        usage = (
            copy.deepcopy(agent_provenance.get("usage"))
            if isinstance(agent_provenance, dict)
            else None
        )
        retained_agent_provenance = copy.deepcopy(agent_provenance)
    else:
        kind = "code"
        prompt_sha256 = None
        model = None
        params = {}
        provider_response_id = None
        usage = None
        retained_agent_provenance = None
    if not isinstance(params, dict):
        params = {}
    return {
        "kind": kind,
        "artifact_sha256": sha(artifact),
        "calls": copied_calls,
        "agent": retained_agent_provenance,
        "prompt_sha256": prompt_sha256,
        "model": model,
        "params": copy.deepcopy(params),
        "provider_response_id": provider_response_id,
        "usage": copy.deepcopy(usage),
        "evidence_policy": evidence_policy.canonical(),
        "evidence_policy_digest": evidence_policy.digest,
    }
