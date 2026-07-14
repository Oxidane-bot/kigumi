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
from .artifacts import atomic_write_json, canonical_json
from .errors import OutputOwnershipError


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
        self, cache_key: str, *, forced: bool, cache_policy: CachePolicy = "auto"
    ) -> tuple[dict[str, Any] | None, bool]:
        """Return a node-cache artifact unless this execution is forced."""
        if forced or cache_policy != "auto":
            return None, False
        artifact = store.read_node_cache(self.artifacts_path, cache_key)
        return artifact, artifact is not None

    def seal(
        self,
        artifact: Any,
        cache_key: str,
        *,
        label: str,
        cache_policy: CachePolicy = "auto",
    ) -> dict[str, Any]:
        """Validate, canonicalize, and persist one cacheable artifact."""
        if not isinstance(artifact, dict):
            raise TypeError(f"{label} must return a dict artifact")
        sealed = json.loads(canonical_json(artifact))
        if cache_policy != "off":
            store.write_node_cache(self.artifacts_path, cache_key, sealed)
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
    ) -> None:
        """Persist one run artifact and its deterministic metadata shape."""
        metadata: dict[str, Any] = {
            "node": label,
            "cache_key": cache_key,
            "cache": "hit" if cache_hit else "miss",
            "cache_policy": cache_policy,
            "outputs": sorted(outputs),
            "seconds": seconds,
            "calls": copy.deepcopy(calls),
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
