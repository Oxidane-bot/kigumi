"""存储布局层：管理 run、缓存、归档、物化和审批的文件系统约定。

本模块不理解 DAG 图、调度或缓存键，只接收已计算好的路径、artifact 和元数据。
依赖方向固定为 ``dag -> store``，因此这里不得导入 ``kigumi.dag``。
"""

from __future__ import annotations

import errno
import json
import os
import re
import shutil
import stat
import tempfile
from collections.abc import Callable, Iterable
from contextlib import ExitStack, suppress
from pathlib import Path
from typing import Any, Literal, NamedTuple

from ._safe_io import (
    SecureDirectory as _SecureDirectory,
)
from ._safe_io import (
    atomic_write_json,
    atomic_write_text,
    install_safe_artifact_writes,
    install_secure_blob_store_writes,
)
from .artifacts import canonical_json, sha, write_artifact
from .blobs import BlobStore, _rename_at
from .errors import OutputOwnershipError

_RUN_ID_PATTERN = re.compile(r"run-(\d+)")
_HISTORY_ID_PATTERN = re.compile(r"\d{4}")
_NODE_CACHE_ENVELOPE_SCHEMA = 4
_ROLLBACK_MARKER_NAME = "rollback.json"
_ORIGIN_PROVENANCE_FIELDS = (
    "artifact_sha256",
    "kind",
    "calls",
    "agent",
    "prompt_resolutions",
    "prompt_sha256",
    "model",
    "params",
    "provider_response_id",
    "usage",
    "evidence_policy",
    "evidence_policy_digest",
)
_ORIGIN_KINDS = frozenset(("call", "agent", "code"))
_EVIDENCE_MODES = frozenset(("full", "redacted", "hash_only"))

install_safe_artifact_writes()
install_secure_blob_store_writes(BlobStore)

CacheState = Literal["MISSING", "VALID", "CORRUPT"]


class CacheLookup(NamedTuple):
    """A cache read that preserves absence and integrity failure as distinct states."""

    state: CacheState
    data: Any | None
    expected_sha256: str | None
    actual_sha256: str | None
    reason: str | None


class CacheEntry(NamedTuple):
    """One immutable snapshot of a node cache artifact and its provenance."""

    state: CacheState
    artifact: dict[str, Any] | None
    origin: dict[str, Any] | None
    expected_sha256: str | None
    actual_sha256: str | None
    reason: str | None

    @property
    def lookup(self) -> CacheLookup:
        """Return the historical artifact-only view of this same snapshot."""
        return CacheLookup(
            self.state,
            self.artifact,
            self.expected_sha256,
            self.actual_sha256,
            self.reason,
        )


def runs_root(artifacts_path: Path) -> Path:
    """Return the stable root that contains every persisted run."""
    return artifacts_path / "runs"


def run_directory(artifacts_path: Path, run_id: str) -> Path:
    """Return one checked run directory without creating it."""
    _validate_path_component(run_id, "Run ID")
    return runs_root(artifacts_path) / run_id


def blob_store_root(artifacts_path: Path) -> Path:
    """Return the stable location for content-addressed binary blobs."""
    return artifacts_path / "_cache" / "blobs"


def node_cache_path(artifacts_path: Path, cache_key: str) -> Path:
    """Return the on-disk location for one content-addressed node cache entry."""
    _validate_path_component(cache_key, "Cache key")
    return artifacts_path / "_cache" / "nodes" / f"{cache_key}.json"


def read_node_cache(artifacts_path: Path, cache_key: str) -> CacheLookup:
    """Read one node cache entry without collapsing corruption into a miss."""
    return read_cache_entry(artifacts_path, cache_key).lookup


def read_cache_entry(artifacts_path: Path, cache_key: str) -> CacheEntry:
    """Read artifact, origin and integrity state from one cache-file snapshot."""
    payload = _read_node_cache_envelope(artifacts_path, cache_key)
    if payload.state != "VALID":
        return CacheEntry(
            payload.state,
            None,
            None,
            payload.expected_sha256,
            payload.actual_sha256,
            payload.reason,
        )
    envelope = payload.data
    if not isinstance(envelope, dict):
        return CacheEntry(
            "CORRUPT",
            None,
            None,
            payload.expected_sha256,
            payload.actual_sha256,
            "node cache envelope is not an object",
        )
    artifact = envelope.get("artifact")
    origin = envelope.get("origin_provenance")
    if not isinstance(artifact, dict) or not isinstance(origin, dict):
        return CacheEntry(
            "CORRUPT",
            None,
            None,
            payload.expected_sha256,
            payload.actual_sha256,
            "node cache artifact or origin is not an object",
        )
    origin_reason = _origin_provenance_error(origin, payload.expected_sha256)
    if origin_reason is not None:
        return CacheEntry(
            "CORRUPT",
            None,
            None,
            payload.expected_sha256,
            payload.actual_sha256,
            origin_reason,
        )
    return CacheEntry(
        "VALID",
        artifact,
        origin,
        payload.expected_sha256,
        payload.actual_sha256,
        payload.reason,
    )


def _origin_provenance_error(origin: dict[str, Any], artifact_sha256: str | None) -> str | None:
    """Return an integrity error for an incomplete or malformed cache origin."""
    missing = [field for field in _ORIGIN_PROVENANCE_FIELDS if field not in origin]
    if missing:
        return "node cache origin provenance missing required field(s): " + ", ".join(missing)
    if origin["artifact_sha256"] != artifact_sha256:
        return "node cache origin provenance artifact binding is invalid"
    kind = origin["kind"]
    if not isinstance(kind, str) or kind not in _ORIGIN_KINDS:
        return "node cache origin provenance kind is invalid"
    if not isinstance(origin["calls"], list) or not all(
        isinstance(call, dict) for call in origin["calls"]
    ):
        return "node cache origin provenance calls must be a list of objects"
    if origin["agent"] is not None and not isinstance(origin["agent"], dict):
        return "node cache origin provenance agent must be an object or null"
    if not isinstance(origin["prompt_resolutions"], dict):
        return "node cache origin provenance prompt_resolutions must be an object"
    for field in ("prompt_sha256", "model", "provider_response_id"):
        value = origin[field]
        if value is not None and not isinstance(value, str):
            return f"node cache origin provenance {field} must be a string or null"
    if not isinstance(origin["params"], dict):
        return "node cache origin provenance params must be an object"
    if origin["usage"] is not None and not isinstance(origin["usage"], dict):
        return "node cache origin provenance usage must be an object or null"
    policy = origin["evidence_policy"]
    if not isinstance(policy, dict) or set(policy) != {
        "request",
        "response",
        "stderr",
        "trajectory",
    }:
        return "node cache origin provenance evidence_policy schema is invalid"
    if any(not isinstance(mode, str) or mode not in _EVIDENCE_MODES for mode in policy.values()):
        return "node cache origin provenance evidence_policy mode is invalid"
    policy_digest = origin["evidence_policy_digest"]
    if not isinstance(policy_digest, str) or policy_digest != sha(policy):
        return "node cache origin provenance evidence_policy_digest is invalid"
    return None


def read_node_cache_origin(
    artifacts_path: Path,
    cache_key: str,
) -> dict[str, Any] | None:
    """Return immutable origin provenance for one valid node-cache entry."""
    return read_cache_entry(artifacts_path, cache_key).origin


def _read_node_cache_envelope(
    artifacts_path: Path,
    cache_key: str,
) -> CacheLookup:
    path = node_cache_path(artifacts_path, cache_key)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return CacheLookup("MISSING", None, None, None, "node cache file is missing")
    except OSError as error:
        return CacheLookup("CORRUPT", None, None, None, f"node cache file stat failed: {error}")
    if stat.S_ISLNK(info.st_mode):
        return CacheLookup("CORRUPT", None, None, None, "node cache file is a symlink")
    if not stat.S_ISREG(info.st_mode):
        return CacheLookup(
            "CORRUPT", None, None, None, "node cache file must reference a regular file"
        )

    try:
        nofollow = os.O_NOFOLLOW
    except AttributeError:
        return CacheLookup(
            "CORRUPT", None, None, None, "node cache requires no-follow descriptor I/O"
        )

    descriptor = -1
    try:
        with _SecureDirectory(path.parent, create=False) as cache_directory:
            cache_directory.verify_bound()
            flags = (
                os.O_RDONLY
                | getattr(os, "O_BINARY", 0)
                | getattr(os, "O_CLOEXEC", 0)
                | nofollow
                | getattr(os, "O_NONBLOCK", 0)
            )
            try:
                descriptor = os.open(path.name, flags, dir_fd=cache_directory.fd)
            except FileNotFoundError:
                return CacheLookup(
                    "CORRUPT", None, None, None, "node cache file changed during read"
                )
            except OSError as error:
                if error.errno == errno.ELOOP:
                    return CacheLookup("CORRUPT", None, None, None, "node cache file is a symlink")
                return CacheLookup(
                    "CORRUPT", None, None, None, f"node cache file open failed: {error}"
                )
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                return CacheLookup(
                    "CORRUPT", None, None, None, "node cache file must reference a regular file"
                )
            if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                return CacheLookup(
                    "CORRUPT", None, None, None, "node cache file changed during read"
                )
            with os.fdopen(descriptor, "r", encoding="utf-8", closefd=True) as handle:
                descriptor = -1
                payload = json.load(handle)
    except FileNotFoundError:
        return CacheLookup("CORRUPT", None, None, None, "node cache file changed during read")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        return CacheLookup("CORRUPT", None, None, None, f"node cache JSON read failed: {error}")
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)
    if not isinstance(payload, dict):
        return CacheLookup("CORRUPT", None, None, None, "node cache JSON is not an object")
    if payload.get("cache_schema") != _NODE_CACHE_ENVELOPE_SCHEMA:
        return CacheLookup(
            "CORRUPT",
            None,
            None,
            None,
            f"node cache schema is not {_NODE_CACHE_ENVELOPE_SCHEMA}",
        )
    if payload.get("cache_key") != cache_key:
        return CacheLookup(
            "CORRUPT", None, None, None, "node cache envelope is bound to a different cache key"
        )
    artifact = payload.get("artifact")
    origin = payload.get("origin_provenance")
    artifact_sha256 = payload.get("artifact_sha256")
    origin_sha256 = payload.get("origin_sha256")
    actual_artifact_sha256 = sha(artifact) if isinstance(artifact, dict) else None
    if (
        not isinstance(artifact, dict)
        or not isinstance(origin, dict)
        or not isinstance(artifact_sha256, str)
        or artifact_sha256 != actual_artifact_sha256
        or origin.get("artifact_sha256") != artifact_sha256
        or not isinstance(origin_sha256, str)
        or origin_sha256 != sha(origin)
    ):
        return CacheLookup(
            "CORRUPT",
            None,
            artifact_sha256 if isinstance(artifact_sha256, str) else None,
            actual_artifact_sha256,
            "node cache envelope digest or provenance validation failed",
        )
    return CacheLookup(
        "VALID",
        payload,
        artifact_sha256,
        actual_artifact_sha256,
        None,
    )


def write_node_cache(
    artifacts_path: Path,
    cache_key: str,
    artifact: dict[str, Any],
    origin_provenance: dict[str, Any],
) -> None:
    """Persist one canonical node artifact and immutable origin provenance."""

    artifact_sha256 = sha(artifact)
    if origin_provenance.get("artifact_sha256") != artifact_sha256:
        raise ValueError("node-cache origin provenance does not bind its artifact")
    atomic_write_json(
        node_cache_path(artifacts_path, cache_key),
        {
            "cache_schema": _NODE_CACHE_ENVELOPE_SCHEMA,
            "cache_key": cache_key,
            "artifact_sha256": artifact_sha256,
            "origin_sha256": sha(origin_provenance),
            "artifact": artifact,
            "origin_provenance": origin_provenance,
        },
    )


def allocate_run_id(artifacts_path: Path) -> str:
    """Allocate and create the next ``run-NNNN`` directory atomically."""
    root = runs_root(artifacts_path)
    root.mkdir(parents=True, exist_ok=True)
    sequence = [
        int(match.group(1))
        for path in root.iterdir()
        if path.is_dir() and (match := _RUN_ID_PATTERN.fullmatch(path.name))
    ]
    candidate = max(sequence, default=0) + 1
    # 扫描到分配之间另一进程可能占走同号;mkdir 是原子的,占不到就顺延。
    while True:
        run_id = f"run-{candidate:04d}"
        try:
            (root / run_id).mkdir()
        except FileExistsError:
            candidate += 1
        else:
            return run_id


def run_sort_key(path: Path) -> tuple[int, int | str]:
    """Sort conventional run IDs by number, while keeping other names deterministic."""
    match = _RUN_ID_PATTERN.fullmatch(path.name)
    return (1, int(match.group(1))) if match else (0, path.name)


def write_run_artifact(
    artifacts_path: Path,
    run_id: str,
    node_name: str,
    artifact: dict[str, Any],
    metadata: dict[str, Any],
    ensure_archive_id: Callable[[], str],
) -> None:
    """Archive a changed prior artifact, then write its artifact and meta sidecar."""
    artifact_path = run_directory(artifacts_path, run_id) / f"{node_name}.json"
    _archive_stale(artifact_path, artifact, ensure_archive_id)
    write_artifact(artifact_path, canonical_json(artifact), metadata)


def next_history_id(history_root: Path) -> str:
    """Return the next four-digit history directory name without creating it."""
    sequence = (
        [
            int(path.name)
            for path in history_root.iterdir()
            if path.is_dir() and _HISTORY_ID_PATTERN.fullmatch(path.name)
        ]
        if history_root.is_dir()
        else []
    )
    return f"{max(sequence, default=0) + 1:04d}"


def materialize_artifact(
    artifact: dict[str, Any],
    node_name: str,
    resolve: Callable[[Path], Path],
    blob_store: BlobStore,
    claim: Callable[[tuple[Path, ...]], None] | None = None,
) -> list[str]:
    """Materialize text files and nested blob references declared by an artifact."""
    text_outputs: list[tuple[Path, str]] = []
    files = artifact.get("files")
    if files is not None:
        if not isinstance(files, dict):
            raise TypeError("Artifact 'files' must be a mapping of relative paths to text")
        for relative_name, contents in files.items():
            relative_path = project_relative_path(relative_name)
            if not isinstance(contents, str):
                raise TypeError("Artifact file contents must be text")
            text_outputs.append((relative_path, contents))
    blob_outputs: list[tuple[Path, str]] = []
    for reference in _blob_references(artifact):
        digest = reference.get("kigumi_blob")
        relative_name = reference.get("path")
        if not isinstance(digest, str) or not isinstance(relative_name, str):
            raise TypeError("Blob references require string 'kigumi_blob' and 'path' fields")
        relative_path = project_relative_path(relative_name)
        blob_outputs.append((relative_path, digest))

    project_root = resolve(Path("."))
    resolved_text = [
        (path, _output_destination(path, resolve, project_root), contents)
        for path, contents in text_outputs
    ]
    resolved_blobs = [
        (path, _output_destination(path, resolve, project_root), digest)
        for path, digest in blob_outputs
    ]
    resolved_outputs = [*resolved_text, *resolved_blobs]
    duplicates: set[str] = set()
    for index, (relative_path, destination, _value) in enumerate(resolved_outputs):
        for other_relative, other_destination, _other_value in resolved_outputs[index + 1 :]:
            if output_paths_equivalent(destination, other_destination):
                duplicates.update((relative_path.as_posix(), other_relative.as_posix()))
    if duplicates:
        raise OutputOwnershipError(
            f"Artifact for {node_name!r} contains duplicate output path(s): "
            + ", ".join(sorted(duplicates))
        )
    # Keep staging private while anchoring it to the project filesystem so the final rename
    # remains atomic even when the system temporary directory is on another mount.
    staging_root = Path(tempfile.mkdtemp(prefix=".kigumi-materialize-", dir=project_root)).resolve()
    try:
        staged_outputs: list[tuple[Path, Path]] = []
        for relative_path, _destination, contents in resolved_text:
            staged = staging_root / relative_path
            atomic_write_text(staged, contents)
            staged_outputs.append((staged, _destination))
        for relative_path, _destination, digest in resolved_blobs:
            staged = staging_root / relative_path
            try:
                blob_store.materialize(digest, staged)
            except FileNotFoundError as error:
                raise FileNotFoundError(
                    f"Blob {digest} referenced by node {node_name!r} is missing"
                ) from error
            staged_outputs.append((staged, _destination))

        if claim is not None:
            claim(tuple(destination for _path, destination, _value in resolved_outputs))
        _commit_staged_outputs(staged_outputs, staging_root)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
    return sorted(path.as_posix() for path, _ in (*text_outputs, *blob_outputs))


def _commit_staged_outputs(staged_outputs: list[tuple[Path, Path]], staging_root: Path) -> None:
    """Replace all staged outputs through bound directories, restoring prior paths on failure."""
    rollback_root = Path(tempfile.mkdtemp(prefix=".kigumi-rollback-", dir=staging_root.parent))
    keep_rollback_root = False
    changes: list[tuple[_SecureDirectory, str, str | None, bool]] = []
    try:
        with ExitStack() as directories:
            rollback_directory = directories.enter_context(
                _SecureDirectory(rollback_root, create=False)
            )
            resolved: list[tuple[Path, _SecureDirectory, str]] = []
            destination_directories: list[_SecureDirectory] = []
            try:
                for staged, destination in staged_outputs:
                    parent = directories.enter_context(
                        _SecureDirectory(destination.parent, create=True)
                    )
                    destination_directories.append(parent)
                    parent.verify_bound()
                    resolved.append((staged, parent, destination.name))

                for index, (staged, destination_directory, destination_name) in enumerate(resolved):
                    destination_directory.verify_bound()
                    try:
                        destination_info = destination_directory.stat(destination_name)
                    except FileNotFoundError:
                        destination_info = None
                    if destination_info is not None and stat.S_ISLNK(destination_info.st_mode):
                        raise ValueError(
                            "Materialization destination must not be a symlink: "
                            f"{destination_directory.path / destination_name}"
                        )
                    if destination_info is not None and stat.S_ISDIR(destination_info.st_mode):
                        raise IsADirectoryError(
                            "Cannot replace output directory: "
                            f"{destination_directory.path / destination_name}"
                        )

                    backup_name: str | None = None
                    if destination_info is not None:
                        backup_name = str(index)
                        _rename_at(
                            destination_name,
                            backup_name,
                            source_directory=destination_directory,
                            destination_directory=rollback_directory,
                        )
                    changes.append((destination_directory, destination_name, backup_name, False))
                    _rename_at(
                        staged, destination_name, destination_directory=destination_directory
                    )
                    changes[-1] = (destination_directory, destination_name, backup_name, True)
            except BaseException:
                try:
                    _rollback_staged_outputs(changes, rollback_directory, destination_directories)
                except BaseException:
                    keep_rollback_root = True
                    raise
                raise
    finally:
        if not keep_rollback_root:
            shutil.rmtree(rollback_root)


def _rollback_staged_outputs(
    changes: list[tuple[_SecureDirectory, str, str | None, bool]],
    rollback_directory: _SecureDirectory,
    destination_directories: list[_SecureDirectory],
) -> None:
    """Undo committed replacements and remove directories created for staging targets."""
    failures: list[BaseException] = []
    try:
        for destination_directory, destination_name, backup_name, installed in reversed(changes):
            if installed:
                try:
                    destination_directory.unlink(destination_name)
                except FileNotFoundError:
                    pass
                except BaseException as error:
                    failures.append(error)
            if backup_name is not None:
                try:
                    _rename_at(
                        backup_name,
                        destination_name,
                        source_directory=rollback_directory,
                        destination_directory=destination_directory,
                    )
                except BaseException as error:
                    failures.append(error)
    finally:
        # A failed materialization must not leave directories that it created solely for outputs.
        seen: set[int] = set()
        for destination_directory in reversed(destination_directories):
            identity = id(destination_directory)
            if identity not in seen:
                destination_directory.remove_created()
                seen.add(identity)
    if failures:
        marker = rollback_directory.path / _ROLLBACK_MARKER_NAME
        marker_payload = {
            "state": "recovery_required",
            "rollback_root": str(rollback_directory.path),
            "changes": [
                {
                    "destination": str(destination_directory.path / destination_name),
                    "backup": backup_name,
                    "installed": installed,
                }
                for destination_directory, destination_name, backup_name, installed in changes
            ],
            "errors": [str(error) for error in failures],
        }
        try:
            atomic_write_json(marker, marker_payload)
        except BaseException as marker_error:
            failures.append(marker_error)
        detail = "; ".join(str(error) for error in failures)
        raise OSError(
            "Materialization rollback failed "
            f"({detail}); recovery required at {rollback_directory.path}"
        )


def _output_destination(
    relative_path: Path,
    resolve: Callable[[Path], Path],
    project_root: Path,
) -> Path:
    """Resolve symlinks and reject materialization outside the project root."""
    destination = resolve(relative_path)
    try:
        destination.relative_to(project_root)
    except ValueError as error:
        raise ValueError("Artifact output paths must resolve inside the project root") from error
    if destination == project_root:
        raise ValueError("Artifact output paths must name a file inside the project root")
    return destination


def output_paths_equivalent(first: Path, second: Path) -> bool:
    """Ask the target filesystem whether two unresolved output names identify one path."""
    if first == second:
        return True
    first_anchor, first_suffix = _existing_parent_and_suffix(first)
    second_anchor, second_suffix = _existing_parent_and_suffix(second)
    if not first_anchor.samefile(second_anchor):
        return False
    if first_suffix == second_suffix:
        return True

    probe_root = Path(tempfile.mkdtemp(prefix=".kigumi-output-probe-", dir=first_anchor))
    try:
        first_probe = probe_root.joinpath(*first_suffix)
        first_probe.parent.mkdir(parents=True, exist_ok=True)
        first_probe.touch()
        second_probe = probe_root.joinpath(*second_suffix)
        return second_probe.exists() and second_probe.samefile(first_probe)
    finally:
        shutil.rmtree(probe_root)


def _existing_parent_and_suffix(path: Path) -> tuple[Path, tuple[str, ...]]:
    """Split a destination into its nearest existing parent and unresolved name suffix."""
    parent = path.parent
    suffix = [path.name]
    while not parent.exists():
        suffix.insert(0, parent.name)
        parent = parent.parent
    return parent, tuple(suffix)


def project_relative_path(relative_name: str) -> Path:
    """Reject output paths that could escape the configured project root."""
    if not isinstance(relative_name, str):
        raise TypeError("Artifact file paths must be strings")
    relative_path = Path(relative_name)
    if not relative_name or relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError("Artifact file paths must be project-relative")
    return relative_path


def diff_runs(runs_root: Path, run_a: str, run_b: str) -> dict[str, list[str]]:
    """Compare two run directories by canonical node-artifact hashes."""
    artifacts_a = _run_artifacts(runs_root, run_a)
    artifacts_b = _run_artifacts(runs_root, run_b)
    shared = sorted(set(artifacts_a) & set(artifacts_b))
    return {
        "changed": [name for name in shared if sha(artifacts_a[name]) != sha(artifacts_b[name])],
        "only_a": sorted(set(artifacts_a) - set(artifacts_b)),
        "only_b": sorted(set(artifacts_b) - set(artifacts_a)),
    }


def approve_checkpoint(runs_root: Path, run_id: str, name: str, data: Any) -> None:
    """Record approval data bound to the pending payload hash for a checkpoint."""
    approval_path = checkpoint_path(runs_root, run_id, name)
    pending_path = approval_path.with_suffix(".pending.json")
    try:
        with pending_path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError as error:
        raise ValueError(f"No pending checkpoint {name!r} in run {run_id!r}") from error
    atomic_write_json(approval_path, {"payload_sha": sha(payload), "data": data})
    pending_path.unlink()


def checkpoint_path(runs_root: Path, run_id: str, name: str) -> Path:
    """Return a checked approval file path for a checkpoint name."""
    _validate_path_component(run_id, "Run ID")
    _validate_path_component(name, "Checkpoint name")
    return runs_root / run_id / "approvals" / f"{name}.json"


def _validate_path_component(value: Any, kind: str) -> str:
    """Reject identifiers that could escape a managed filesystem directory."""
    path = Path(value) if isinstance(value, str) else None
    if (
        path is None
        or not value
        or "/" in value
        or "\\" in value
        or path.name != value
        or value in {".", ".."}
    ):
        raise ValueError(f"{kind} must be a single non-empty relative path component")
    return value


def gc_cache(cache_root: Path, runs_root: Path, keep_last: int) -> int:
    """Delete cache files not referenced by the latest retained run directories."""
    if keep_last < 0:
        raise ValueError("keep_last must be non-negative")
    if not runs_root.is_dir():
        return 0
    runs = sorted((path for path in runs_root.iterdir() if path.is_dir()), key=run_sort_key)
    retained = runs[-keep_last:] if keep_last else []
    referenced: set[str] = set()
    for run in retained:
        for sidecar in run.glob("*.json.meta.json"):
            try:
                with sidecar.open(encoding="utf-8") as handle:
                    metadata = json.load(handle)
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(metadata, dict):
                continue
            cache_key = metadata.get("cache_key")
            if isinstance(cache_key, str):
                referenced.add(cache_key)
            elif isinstance(cache_key, list) and all(isinstance(key, str) for key in cache_key):
                # map 聚合 sidecar 的键列表是 gc 的契约来源;逐项 sidecar
                # 只是冗余,缺失时也不得误删 item 缓存。
                referenced.update(cache_key)

    removed = 0
    for cache_path in cache_root.glob("*.json"):
        if cache_path.stem not in referenced:
            cache_path.unlink()
            removed += 1
    return removed


def gc_artifacts(artifacts_path: Path, keep_last: int) -> int:
    """Delete unreferenced node caches and blobs for the retained run history."""
    root = runs_root(artifacts_path)
    cache_removed = gc_cache(artifacts_path / "_cache" / "nodes", root, keep_last)
    blob_removed = BlobStore(blob_store_root(artifacts_path)).gc(
        _referenced_blob_digests(root, keep_last)
    )
    return cache_removed + blob_removed


def _archive_stale(
    artifact_path: Path,
    artifact: dict[str, Any],
    ensure_archive_id: Callable[[], str],
) -> None:
    if not artifact_path.is_file():
        return
    try:
        with artifact_path.open(encoding="utf-8") as handle:
            previous = json.load(handle)
        changed = sha(previous) != sha(artifact)
    except (OSError, json.JSONDecodeError):
        changed = True
    if not changed:
        return
    destination = artifact_path.parent / "history" / ensure_archive_id()
    destination.mkdir(parents=True, exist_ok=True)
    shutil.move(str(artifact_path), destination / artifact_path.name)
    sidecar = Path(f"{artifact_path}.meta.json")
    if sidecar.is_file():
        shutil.move(str(sidecar), destination / sidecar.name)


def _referenced_blob_digests(runs_root: Path, keep_last: int) -> set[str]:
    """Collect blob digests from retained artifact JSON, including nested map items."""
    if keep_last < 0:
        raise ValueError("keep_last must be non-negative")
    if not runs_root.is_dir():
        return set()
    runs = sorted((path for path in runs_root.iterdir() if path.is_dir()), key=run_sort_key)
    referenced: set[str] = set()
    for run in runs[-keep_last:] if keep_last else []:
        # Retained sidecars, failures, and durable attempt receipts are all
        # evidence roots. Materialization still ignores attachment markers.
        retained_json = list(run.rglob("*.json"))
        for artifact_path in retained_json:
            try:
                with artifact_path.open(encoding="utf-8") as handle:
                    artifact = json.load(handle)
            except (OSError, json.JSONDecodeError):
                continue
            for reference in _blob_references(artifact):
                digest = reference.get("kigumi_blob")
                if isinstance(digest, str):
                    referenced.add(digest)
            for reference in _attachment_references(artifact):
                digest = reference.get("kigumi_attachment")
                if isinstance(digest, str):
                    referenced.add(digest)
    return referenced


def _blob_references(value: Any) -> Iterable[dict[str, Any]]:
    """Yield every nested dictionary that declares a blob reference."""
    if isinstance(value, dict):
        if "kigumi_blob" in value:
            yield value
        for child in value.values():
            yield from _blob_references(child)
    elif isinstance(value, list):
        for child in value:
            yield from _blob_references(child)


def _attachment_references(value: Any) -> Iterable[dict[str, Any]]:
    """Yield captured evidence without giving it materialization semantics."""
    if isinstance(value, dict):
        if "kigumi_attachment" in value:
            yield value
        for child in value.values():
            yield from _attachment_references(child)
    elif isinstance(value, list):
        for child in value:
            yield from _attachment_references(child)


def _run_artifacts(runs_root: Path, run_id: str) -> dict[str, dict[str, Any]]:
    _validate_path_component(run_id, "Run ID")
    run_path = runs_root / run_id
    artifacts: dict[str, dict[str, Any]] = {}
    if not run_path.is_dir():
        return artifacts
    for path in sorted(run_path.glob("*.json")):
        if path.name.startswith("_") or path.name.endswith(".json.meta.json"):
            continue
        try:
            with path.open(encoding="utf-8") as handle:
                artifact = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(artifact, dict):
            artifacts[path.stem] = artifact
    return artifacts
