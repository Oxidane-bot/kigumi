"""存储布局层：管理 run、缓存、归档、物化和审批的文件系统约定。

本模块不理解 DAG 图、调度或缓存键，只接收已计算好的路径、artifact 和元数据。
依赖方向固定为 ``dag -> store``，因此这里不得导入 ``kigumi.dag``。
"""

from __future__ import annotations

import json
import re
import shutil
import tempfile
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from .artifacts import atomic_write_json, atomic_write_text, canonical_json, sha, write_artifact
from .blobs import BlobStore
from .errors import OutputOwnershipError

_RUN_ID_PATTERN = re.compile(r"run-(\d+)")
_HISTORY_ID_PATTERN = re.compile(r"\d{4}")


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
    return artifacts_path / "_cache" / "nodes" / f"{cache_key}.json"


def read_node_cache(artifacts_path: Path, cache_key: str) -> dict[str, Any] | None:
    """Read a valid node cache artifact, treating torn or invalid files as misses."""
    try:
        with node_cache_path(artifacts_path, cache_key).open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    artifact = payload.get("artifact") if isinstance(payload, dict) else None
    return artifact if isinstance(artifact, dict) else None


def write_node_cache(artifacts_path: Path, cache_key: str, artifact: dict[str, Any]) -> None:
    """Persist one canonical node artifact in its existing cache envelope."""
    atomic_write_json(node_cache_path(artifacts_path, cache_key), {"artifact": artifact})


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
    if claim is not None:
        claim(tuple(destination for _path, destination, _value in resolved_outputs))

    for _relative_path, destination, contents in resolved_text:
        atomic_write_text(destination, contents)
    for _relative_path, destination, digest in resolved_blobs:
        try:
            blob_store.materialize(digest, destination)
        except FileNotFoundError as error:
            raise FileNotFoundError(
                f"Blob {digest} referenced by node {node_name!r} is missing"
            ) from error
    return sorted(path.as_posix() for path, _ in (*text_outputs, *blob_outputs))


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
        for artifact_path in run.glob("*.json"):
            if artifact_path.name.endswith(".json.meta.json"):
                continue
            try:
                with artifact_path.open(encoding="utf-8") as handle:
                    artifact = json.load(handle)
            except (OSError, json.JSONDecodeError):
                continue
            for reference in _blob_references(artifact):
                digest = reference.get("kigumi_blob")
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


def _run_artifacts(runs_root: Path, run_id: str) -> dict[str, dict[str, Any]]:
    _validate_path_component(run_id, "Run ID")
    run_path = runs_root / run_id
    artifacts: dict[str, dict[str, Any]] = {}
    if not run_path.is_dir():
        return artifacts
    for path in sorted(run_path.glob("*.json")):
        if path.name.endswith(".json.meta.json"):
            continue
        try:
            with path.open(encoding="utf-8") as handle:
                artifact = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(artifact, dict):
            artifacts[path.stem] = artifact
    return artifacts
