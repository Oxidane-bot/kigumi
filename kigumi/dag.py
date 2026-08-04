"""调度层：注册、校验、缓存键计算与 DAG 执行。

存储路径、artifact 落盘、归档、物化、审批和 GC 由 ``kigumi.store`` 负责；本模块仅依赖它。
"""

from __future__ import annotations

import argparse
import ast
import builtins
import copy
import functools
import getpass
import inspect
import io
import json
import os
import struct
import sys
import textwrap
import threading
import time
import types
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import contextmanager, nullcontext, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal

import pydantic

from . import profile as workflow_profile
from . import prompt, repair, store, views
from ._declarations import (
    CachePolicy,
    ConsumeFunction,
    ResourceRequest,
    external_fingerprint_digest,
    validate_cache_policy,
    validate_consumes,
    validate_segment,
)
from ._execution import ExecutionEnvelope
from ._runstate import (
    FAILURE_SCHEMA,
    RUN_MANIFEST_SCHEMA,
    RUN_SIDECAR_SCHEMA,
    SUCCESS_CANDIDATE_SCHEMA,
    AttemptStore,
    RunManifestError,
)
from ._safe_io import digest_open_file, open_regular_file
from .agents import (
    AGENT_EXECUTOR_SCHEMA,
    AgentAdapter,
    AgentBuildContext,
    AgentResultError,
    AgentResultView,
    AgentSpec,
    AgentTask,
    agent_external_identity,
    execute_agent_task,
    validate_agent_artifact,
    validate_agent_provenance,
)
from .artifacts import atomic_write_json, canonical_json, sha
from .blobs import BlobStore
from .calling import (
    BudgetExceeded,
    DryRunError,
    LLMCaller,
    durable_side_effect_boundary,
    file_snapshot_boundary,
    observe,
)
from .config import KigumiConfig
from .enforce import check_paths, check_raw_io_node_paths, check_raw_io_source, check_source
from .errors import CacheIntegrityError, OutputOwnershipError
from .evidence import EvidencePolicy, scrub_evidence
from .failures import (
    AgentExecutionFailure,
    AgentRuntimeFailureCode,
    ProviderFailure,
    canonical_failure,
    failure_provider_kind,
)
from .prompt import (
    PromptCatalogSnapshot,
    PromptResolutionError,
    PromptSpec,
    ResolvedPrompt,
    validate_prompt_bindings,
    validate_prompt_resolution_record,
    validate_prompt_specs,
)
from .retry import AmbiguousAttemptError, RetryExhausted, RetryPolicy
from .slots import FileSlots, SlotTimeoutError
from .subgraph import Subgraph

NodeFunction = Callable[[dict[str, dict[str, Any]], "NodeContext"], dict[str, Any]]
MapFunction = Callable[[Any, dict[str, dict[str, Any]], "NodeContext"], dict[str, Any]]
ScanFunction = Callable[[Any, Any, dict[str, dict[str, Any]], "NodeContext"], dict[str, Any]]
AggregateFunction = Callable[[dict[str, dict[str, Any]], list[str]], dict[str, Any]]
PostNodeHook = Callable[[str, dict[str, Any], bool], None]
_NO_CARRY = object()
_NO_ITEM = object()
_DYNAMIC_FILES_LEDGER_FIELD = "dynamic_files_ledger"
_DYNAMIC_FILES_LEDGER_DIGEST_FIELD = "dynamic_files_ledger_sha256"
# Increment when key derivation, prompt-byte generation, or artifact normalization changes.
CACHE_SCHEMA = 7
_DEFAULT_EVIDENCE_POLICY = EvidencePolicy()


class _ResourcePool:
    """Serialize multi-unit acquisition for one in-process semaphore."""

    def __init__(self, name: str | None, limit: int) -> None:
        self.name = name
        self.semaphore = threading.Semaphore(limit)
        self._acquire_lock = threading.Lock()

    def acquire(self, units: int, deadline: float | None) -> None:
        acquired = 0
        # Holding this lock while acquiring all units prevents two callers from
        # each taking part of a multi-unit request and waiting on one another.
        with self._acquire_lock:
            try:
                while acquired < units:
                    if deadline is None:
                        available = self.semaphore.acquire()
                    else:
                        available = self.semaphore.acquire(
                            timeout=max(0.0, deadline - time.monotonic())
                        )
                    if not available:
                        raise TimeoutError(f"Timed out waiting for resource {self.name!r}")
                    acquired += 1
            except BaseException:
                for _ in range(acquired):
                    self.semaphore.release()
                raise

    def release(self, units: int) -> None:
        for _ in range(units):
            self.semaphore.release()


class _PermitPlane:
    """One in-process permit plane shared by regular nodes and dynamic items."""

    def __init__(
        self,
        limits: Mapping[str | None, int],
        *,
        timeout_seconds: float | None = None,
    ) -> None:
        self._pools = {name: _ResourcePool(name, limit) for name, limit in limits.items()}
        self._timeout_seconds = timeout_seconds

    @contextmanager
    def acquire(self, requests: tuple[ResourceRequest, ...]) -> Any:
        grouped: dict[str | None, int] = {}
        effective_requests = requests or (None,)
        for request in effective_requests:
            name = request.name if isinstance(request, ResourceRequest) else request
            units = request.units if isinstance(request, ResourceRequest) else 1
            grouped[name] = grouped.get(name, 0) + units
        ordered = sorted(grouped, key=lambda name: "" if name is None else name)
        acquired: list[tuple[_ResourcePool, int]] = []
        deadline = (
            time.monotonic() + self._timeout_seconds if self._timeout_seconds is not None else None
        )
        try:
            for name in ordered:
                pool = self._pools[name]
                units = grouped[name]
                pool.acquire(units, deadline)
                acquired.append((pool, units))
            yield
        finally:
            for pool, units in reversed(acquired):
                pool.release(units)


def _kigumi_key_inputs() -> dict[str, Any]:
    """Return the versioned inputs for code that deterministically generates prompt bytes."""
    prompt_modules = sorted(
        (Path(prompt.__file__), Path(repair.__file__)), key=lambda path: path.name
    )
    return {
        "prompt_source": sha(
            [(path.name, _bytes_hash(path.read_bytes())) for path in prompt_modules]
        ),
        "schema": CACHE_SCHEMA,
        "pydantic": pydantic.__version__,
    }


def _dag_file_error(message: str, path: Path) -> ValueError:
    """Describe a declared DAG input rejected by the regular-file boundary."""
    return ValueError(f"DAG declared file {message}: {path}")


def _dag_file_identity(info: os.stat_result) -> tuple[int, ...]:
    """Return descriptor metadata used to detect a file changing mid-snapshot."""
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _read_snapshot_file(path: Path) -> bytes:
    """Read one declared input from a descriptor-bound regular file."""
    with open_regular_file(
        path,
        identity=_dag_file_identity,
        expected_identity=None,
        phase="before snapshot",
        error=_dag_file_error,
    ) as handle:
        _digest, _size, data = digest_open_file(
            handle,
            path,
            identity=_dag_file_identity,
            expected_identity=None,
            before_phase="before snapshot",
            during_phase="during snapshot",
            chunk_size=1024 * 1024,
            error=_dag_file_error,
            collect=True,
        )
    if data is None:
        raise _dag_file_error("returned no bytes", path)
    return data


class CheckpointPending(RuntimeError):
    """Raised by a node when it needs an explicit human approval to continue."""

    def __init__(self, name: str, payload: Any) -> None:
        super().__init__(f"Checkpoint pending: {name}")
        self.name = name
        self.payload = payload


class UndeclaredInputError(RuntimeError):
    """节点经由受控读取访问未声明文件时抛出。"""


class _MapCheckpointPending(RuntimeError):
    """Carry every item checkpoint back to the outer scheduler as one pending node."""

    def __init__(self, names: list[str]) -> None:
        super().__init__("Map checkpoint pending")
        self.names = names


class _MapRetryPending(RuntimeError):
    """Carry durable item retry targets back to the outer scheduler."""

    def __init__(self, names: list[str]) -> None:
        super().__init__("Dynamic item retry pending")
        self.names = names


@dataclass(frozen=True)
class RunResult:
    """Completed artifacts plus cache, checkpoint, and skip state for one run."""

    artifacts: dict[str, dict[str, Any]]
    cache_hits: list[str]
    pending_checkpoints: list[str]
    run_id: str
    skipped: list[str]
    map_items: dict[str, dict[str, str]]
    pending_retries: list[str]
    ambiguous_attempts: list[str]
    run_status: str


@dataclass(frozen=True)
class RecoveryReceipt:
    """Record of an explicit recovery decision and its supporting rationale."""

    recovery_time: str
    from_attempt: int
    to_attempt: int
    decision: Literal["retry_not_started", "retry_after_external_check", "fail"]
    reason: str
    evidence_refs: list[str]
    recovered_by: str


@dataclass(frozen=True)
class PlanResult:
    """Read-only cache forecast for a target closure.

    ``nodes`` uses ``map_name@item_id`` keys for expanded map items, alongside
    the map node's own aggregate status.  A checkpoint does not alter a cache
    key, so it is reported with the same hit/miss/unknown rules as any node.
    """

    nodes: dict[str, str]
    pending_on: dict[str, tuple[str, ...]]

    @property
    def misses(self) -> list[str]:
        """Return every node or expanded map item that may need work."""
        return [name for name, status in self.nodes.items() if status in {"miss", "unknown"}]

    @property
    def certain(self) -> list[str]:
        """Return nodes that certainly need recomputation, the cost lower bound."""
        return [name for name, status in self.nodes.items() if status == "miss"]

    @property
    def at_risk(self) -> list[str]:
        """Return nodes whose work depends on upstream content, the extra upper-bound risk."""
        return [name for name, status in self.nodes.items() if status == "unknown"]


@dataclass(frozen=True)
class ExplainResult:
    """解释某个节点当前缓存判断与历史运行记录的成分差异。

    这是基于注册声明和已落盘 sidecar 的 best-effort 诊断，不是执行契约；
    上游尚未能诚实取得内容时会返回 ``unknown``，不会猜测变化原因。
    """

    status: str
    changed: list[str]
    details: dict[str, dict[str, str]]
    pending_on: tuple[str, ...] = ()

    def __str__(self) -> str:
        """返回供终端直接阅读的中文缓存解释。"""
        lines = [f"缓存解释：{self.status}"]
        if self.changed:
            lines.append("变化成分：" + "、".join(self.changed))
            lines.extend(
                f"- {label}: {entry['old']} -> {entry['new']}"
                for label, entry in self.details.items()
            )
        if self.pending_on:
            lines.append("等待上游：" + "、".join(self.pending_on))
        if self.status == "no_entry":
            lines.append("所选运行没有该节点的 sidecar 记录。")
        return "\n".join(lines)


@dataclass(frozen=True)
class _NodeAstMetadata:
    """保存注册期尽力提取的节点 AST 摘要，不能作为运行时契约。"""

    validated_models: tuple[dict[str, Any], ...] = ()
    model_classes: tuple[type[pydantic.BaseModel], ...] = ()
    checkpoints: tuple[str, ...] = ()


@dataclass(frozen=True)
class _Node:
    name: str
    function: NodeFunction | MapFunction | ScanFunction
    deps: tuple[str, ...]
    prompt_specs: tuple[PromptSpec, ...]
    files: tuple[Path, ...]
    params: dict[str, Any]
    consumes: dict[str, ConsumeFunction]
    resources: tuple[ResourceRequest, ...] = ()
    items_from: tuple[str, str] | None = None
    key_fn: Callable[[Any], str] | None = None
    files_fn: Callable[[Any], Iterable[str | Path]] | None = None
    aggregate_fn: AggregateFunction | None = None
    scan: bool = False
    carry_from: tuple[str, str] | None = None
    carry_fn: Callable[[dict[str, Any]], Any] | None = None
    validated_models: tuple[dict[str, Any], ...] = ()
    model_classes: tuple[type[pydantic.BaseModel], ...] = ()
    checkpoints: tuple[str, ...] = ()
    cache: CachePolicy = "auto"
    external_fingerprint_digest: str | None = None
    input_bindings: tuple[tuple[str, str], ...] = ()
    local_items_source: str | None = None
    local_carry_source: str | None = None
    subgraph: str | None = None
    executor: str = "function"
    agent_adapter: AgentAdapter | None = None
    agent_spec: AgentSpec | None = None
    agent_identity: dict[str, Any] | None = None
    evidence_policy: EvidencePolicy | None = None
    retry: RetryPolicy | None = None


@dataclass(frozen=True)
class _FileSnapshot:
    """Immutable bytes captured for one run's declared-file descriptors."""

    project_root: Path
    _contents: Mapping[str, bytes]
    _resolved_contents: Mapping[Path, bytes]
    _declared_aliases: Mapping[str, tuple[str, ...]]

    @classmethod
    def capture(
        cls,
        project_root: Path,
        resolve: Callable[[str | Path], Path],
        paths: Iterable[str | Path] = (),
    ) -> _FileSnapshot:
        empty = cls(
            project_root.resolve(),
            types.MappingProxyType({}),
            types.MappingProxyType({}),
            types.MappingProxyType({}),
        )
        return empty.extend(paths, resolve)

    def extend(
        self,
        paths: Iterable[str | Path],
        resolve: Callable[[str | Path], Path],
    ) -> _FileSnapshot:
        contents = dict(self._contents)
        resolved_contents = dict(self._resolved_contents)
        declared_aliases = dict(self._declared_aliases)
        for raw_path in paths:
            declared = Path(raw_path)
            resolved = resolve(declared)
            # ``KigumiConfig.resolve`` is a logical project-boundary helper and
            # intentionally follows symlinks.  It is not a safe input read.  Open
            # the lexical path through the shared descriptor boundary so the
            # declaration itself cannot turn into a symlink or special file.
            data = _read_snapshot_file(self._lexical_absolute(declared))
            resolved_contents[resolved] = data
            aliases = self._aliases(declared, resolved)
            declared_aliases.setdefault(str(declared), aliases)
            for alias in aliases:
                contents.setdefault(alias, data)
        return _FileSnapshot(
            self.project_root,
            types.MappingProxyType(contents),
            types.MappingProxyType(resolved_contents),
            types.MappingProxyType(declared_aliases),
        )

    def contents(self, paths: Iterable[str | Path]) -> dict[str, bytes]:
        return {str(path): self.read(path) for path in paths}

    def is_declared(
        self,
        path: str | Path,
        declared_paths: Iterable[str | Path],
    ) -> bool:
        requested = set(self._aliases(Path(path)))
        return any(
            requested.intersection(
                self._declared_aliases.get(str(declared), self._aliases(Path(declared)))
            )
            for declared in declared_paths
        )

    def read(self, path: str | Path) -> bytes:
        for alias in self._aliases(Path(path)):
            try:
                return self._contents[alias]
            except KeyError:
                continue
        raise KeyError(str(path))

    def read_declared(
        self,
        path: str | Path,
        declared_paths: Iterable[str | Path],
    ) -> bytes:
        if not self.is_declared(path, declared_paths):
            raise KeyError(str(path))
        try:
            return self.read(path)
        except KeyError as error:
            raise RunManifestError(
                f"Declared file {path!r} is absent from the run file snapshot"
            ) from error

    def _aliases(self, path: Path, resolved: Path | None = None) -> tuple[str, ...]:
        aliases = [str(path), str(self._lexical_absolute(path))]
        if resolved is not None:
            aliases.append(str(resolved))
        return tuple(dict.fromkeys(aliases))

    def _lexical_absolute(self, path: Path) -> Path:
        candidate = path if path.is_absolute() else self.project_root / path
        return Path(os.path.abspath(candidate))


class NodeContext:
    """节点执行上下文；文件读取只能访问节点已声明的输入。"""

    def __init__(
        self,
        dag: Dag,
        node: _Node,
        run_id: str,
        *,
        checkpoint_suffix: str | None = None,
        item_files: tuple[Path, ...] = (),
        prompt_resolutions: Mapping[str, ResolvedPrompt] | None = None,
        file_snapshot: _FileSnapshot,
    ) -> None:
        self._dag = dag
        self._node = node
        self._run_id = run_id
        self._checkpoint_suffix = checkpoint_suffix
        self._item_files = item_files
        self._checkpoint_used = False
        self._prompt_resolutions = dict(prompt_resolutions or {})
        self._file_snapshot = file_snapshot

    @property
    def params(self) -> dict[str, Any]:
        """Return this node's declared parameters without exposing registry state."""
        return copy.deepcopy(self._node.params)

    @property
    def project_root(self) -> Path:
        """Return the resolved root configured for this DAG's project."""
        return self._dag.config.project_root.resolve()

    def read_text(self, path: str | Path, encoding: str = "utf-8") -> str:
        """读取已在 ``files`` 或当前项 ``files_fn`` 中声明的文本文件。"""
        data = self._checked_file(path)
        with io.TextIOWrapper(io.BytesIO(data), encoding=encoding) as handle:
            return handle.read()

    def read_bytes(self, path: str | Path) -> bytes:
        """读取已在 ``files`` 或当前项 ``files_fn`` 中声明的二进制文件。"""
        return self._checked_file(path)

    def _checked_file(self, path: str | Path) -> bytes:
        declared_paths = (*self._node.files, *self._item_files)
        if not self._file_snapshot.is_declared(path, declared_paths):
            resolved = self._dag.config.resolve(path)
            raise UndeclaredInputError(
                f"Node {self._node.name!r} attempted to read undeclared file {resolved}. "
                "在 files= 或 files_fn 中声明该文件。"
            )
        try:
            return self._file_snapshot.read(path)
        except KeyError as error:
            raise RunManifestError(
                f"Declared file {path!r} is absent from the run file snapshot"
            ) from error

    def llm(
        self,
        messages: list[dict[str, Any]] | str,
        model: str = "default",
        **params: Any,
    ) -> str:
        """Make one cached L1 call through the DAG's injected caller."""
        return self.call(messages, model=model, **params)

    def call(
        self,
        messages: list[dict[str, Any]] | str,
        model: str = "default",
        **params: Any,
    ) -> str:
        """Make one cached L1 call using the caller protocol expected by helpers."""
        with file_snapshot_boundary(self._checked_file):
            return self._dag.caller.call(messages, model=model, **params)

    def call_validated(self, prompt: str, model_cls: Any, **kwargs: Any) -> Any:
        """Call for a validated Pydantic model through this node's caller gate."""
        from .repair import call_validated

        return call_validated(self, prompt, model_cls, **kwargs)

    def repair(
        self,
        messages: list[dict[str, Any]] | str,
        validate: Callable[[str], Any],
        **kwargs: Any,
    ) -> Any:
        """Run a bounded validation-repair loop through this node's caller gate."""
        from .repair import repair_loop

        return repair_loop(self, messages, validate, **kwargs)

    def resolve_prompt(self, spec_name: str) -> ResolvedPrompt:
        """Return one PromptSpec resolved before this target's L3 lookup."""
        try:
            return self._prompt_resolutions[spec_name]
        except KeyError as error:
            raise ValueError(
                f"PromptSpec {spec_name!r} is not declared for node {self._node.name!r}"
            ) from error

    def checkpoint(self, name: str, payload: Any) -> Any:
        """Return approval data bound to this exact payload or stop for human review."""
        self._checkpoint_used = True
        qualifiers: list[str] = []
        if self._node.subgraph is not None:
            qualifiers.append(self._node.name)
        if self._checkpoint_suffix is not None:
            qualifiers.append(self._checkpoint_suffix)
        if qualifiers:
            name = "@".join((name, *qualifiers))
        approval_path = self._dag._approval_path(self._run_id, name)
        if approval_path.is_file():
            with approval_path.open(encoding="utf-8") as handle:
                record = json.load(handle)
            if isinstance(record, dict) and record.get("payload_sha") == sha(payload):
                return record["data"]
            # 审批绑定批准时的 payload 内容;内容变了旧批作废,重新挂起。
        raise CheckpointPending(name, payload)

    def emit_file(self, relative_path: str, data: bytes) -> dict[str, Any]:
        """Store binary output and return its JSON-serializable artifact reference."""
        relative = store.project_relative_path(relative_path)
        digest = self._dag.blob_store.put(data)
        return {"kigumi_blob": digest, "path": str(relative), "bytes": len(data)}

    def ingest_file(self, source: Path | str, relative_path: str) -> dict[str, Any]:
        """Copy a tool-written file into the blob store without moving its source."""
        relative = store.project_relative_path(relative_path)
        digest, size = self._dag.blob_store.ingest(Path(source))
        return {"kigumi_blob": digest, "path": str(relative), "bytes": size}

    def agent_result(self, artifact: Mapping[str, Any]) -> AgentResultView:
        """Open a verified read-only view of an upstream Agent artifact."""
        return AgentResultView(artifact, self._dag.blob_store)


class Dag:
    """Own a project-local node registry, content-addressed cache, and run history."""

    def __init__(
        self,
        config: KigumiConfig,
        caller: LLMCaller,
        *,
        post_node: PostNodeHook | None = None,
    ) -> None:
        self.config = config
        self.caller = caller
        self.post_node = post_node
        self._nodes: dict[str, _Node] = {}
        self._subgraphs: dict[str, dict[str, Any]] = {}
        self.blob_store = BlobStore(store.blob_store_root(self.config.artifacts_path))
        self.agent_slots = FileSlots(
            self.config.agent_lock_path,
            self.config.agent_slots,
        )

    def _caller_evidence_policy(self) -> EvidencePolicy:
        """Support lightweight test/dry-run callers without weakening the default."""
        policy = getattr(self.caller, "evidence_policy", _DEFAULT_EVIDENCE_POLICY)
        return policy if isinstance(policy, EvidencePolicy) else _DEFAULT_EVIDENCE_POLICY

    def node(
        self,
        name: str,
        deps: Iterable[str] = (),
        files: Iterable[str | Path] = (),
        params: dict[str, Any] | None = None,
        *,
        prompt_specs: Iterable[PromptSpec] = (),
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint: Any | None = None,
        resources: Iterable[ResourceRequest] | None = None,
        retry: RetryPolicy | None = None,
    ) -> Callable[[NodeFunction], NodeFunction]:
        """Register a deterministic node without sharing registry state with other DAGs."""
        _validate_name(name, "Node")
        if name in self._nodes:
            raise ValueError(f"Node {name!r} is already registered")
        node_deps = tuple(deps)
        node_prompt_specs = validate_prompt_specs(
            tuple(prompt_specs),
            dynamic_kind="node",
        )
        node_files = tuple(Path(path) for path in files)
        node_params = copy.deepcopy(params) if params is not None else {}
        node_cache = validate_cache_policy(cache)
        fingerprint_digest = external_fingerprint_digest(external_fingerprint)
        node_resources = _validate_resources(resources, "Node")
        retry_policy = _validate_retry_policy(retry)

        def register(function: NodeFunction) -> NodeFunction:
            metadata = _validate_registration(function)
            self._register_node(
                name,
                function,
                deps=node_deps,
                prompt_specs=node_prompt_specs,
                files=node_files,
                params=node_params,
                resources=node_resources,
                consumes=consumes,
                cache=node_cache,
                external_fingerprint_digest=fingerprint_digest,
                retry=retry_policy,
                metadata=metadata,
            )
            return function

        return register

    def agent(
        self,
        name: str,
        *,
        adapter: AgentAdapter,
        spec: AgentSpec,
        deps: Iterable[str] = (),
        prompt_specs: Iterable[PromptSpec] = (),
        files: Iterable[str | Path] = (),
        params: dict[str, Any] | None = None,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        evidence_policy: EvidencePolicy = _DEFAULT_EVIDENCE_POLICY,
        resources: Iterable[ResourceRequest] | None = None,
        retry: RetryPolicy | None = None,
    ) -> Callable[[Callable[[dict[str, dict[str, Any]], AgentBuildContext], AgentTask]], Any]:
        """Register an external-agent executor on the ordinary node scheduler."""
        _validate_name(name, "Agent node")
        if name in self._nodes:
            raise ValueError(f"Node {name!r} is already registered")
        node_cache = validate_cache_policy(cache)
        if not isinstance(evidence_policy, EvidencePolicy):
            raise TypeError("evidence_policy must be EvidencePolicy")
        node_resources = _validate_resources(resources, "Agent node")
        retry_policy = _validate_retry_policy(retry)
        node_prompt_specs = validate_prompt_specs(
            tuple(prompt_specs),
            dynamic_kind="node",
        )
        try:
            identity = agent_external_identity(adapter, spec)
        except (TypeError, ValueError) as error:
            if node_cache == "auto":
                raise ValueError(
                    "cache='auto' requires an Agent adapter with a stable identity; "
                    "use cache='refresh' or cache='off' only for intentionally unkeyed runs"
                ) from error
            identity = {
                "agent_executor_schema": AGENT_EXECUTOR_SCHEMA,
                "adapter": {"unkeyed": True},
                "spec": spec.identity(),
            }

        def register(function: Any) -> Any:
            metadata = _validate_registration(function)
            self._register_node(
                name,
                function,
                deps=tuple(deps),
                prompt_specs=node_prompt_specs,
                files=tuple(Path(path) for path in files),
                params=copy.deepcopy(params) if params is not None else {},
                resources=node_resources,
                consumes=consumes,
                cache=node_cache,
                external_fingerprint_digest=external_fingerprint_digest(identity),
                metadata=metadata,
                executor="agent",
                agent_adapter=adapter,
                agent_spec=spec,
                agent_identity=copy.deepcopy(identity),
                evidence_policy=evidence_policy,
                retry=retry_policy,
            )
            return function

        return register

    def agent_scan(
        self,
        name: str,
        *,
        adapter: AgentAdapter,
        spec: AgentSpec,
        items_from: tuple[str, str],
        key_fn: Callable[[Any], str] | None = None,
        carry_from: tuple[str, str] | None = None,
        carry_fn: Callable[[dict[str, Any]], Any] | None = None,
        deps: Iterable[str] = (),
        prompt_specs: Iterable[PromptSpec] = (),
        files: Iterable[str | Path] = (),
        files_fn: Callable[[Any], Iterable[str | Path]] | None = None,
        params: dict[str, Any] | None = None,
        aggregate_fn: AggregateFunction | None = None,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        evidence_policy: EvidencePolicy = _DEFAULT_EVIDENCE_POLICY,
        resources: Iterable[ResourceRequest] | None = None,
        retry: RetryPolicy | None = None,
    ) -> Callable[[Any], Any]:
        """Register a serial carry-dependent scan whose items execute through an Agent."""
        _validate_name(name, "Agent scan")
        _validate_artifact_locator(items_from, "items_from")
        if carry_from is not None:
            _validate_artifact_locator(carry_from, "carry_from")
        if name in self._nodes:
            raise ValueError(f"Node {name!r} is already registered")
        if not isinstance(evidence_policy, EvidencePolicy):
            raise TypeError("evidence_policy must be EvidencePolicy")
        node_resources = _validate_resources(resources, "Agent scan")
        node_cache = validate_cache_policy(cache)
        retry_policy = _validate_retry_policy(retry)
        node_prompt_specs = validate_prompt_specs(
            tuple(prompt_specs),
            dynamic_kind="scan",
        )
        try:
            identity = agent_external_identity(adapter, spec)
        except (TypeError, ValueError) as error:
            if node_cache == "auto":
                raise ValueError(
                    "cache='auto' requires an Agent adapter with a stable identity; "
                    "use cache='refresh' or cache='off' only for intentionally unkeyed runs"
                ) from error
            identity = {
                "agent_executor_schema": AGENT_EXECUTOR_SCHEMA,
                "adapter": {"unkeyed": True},
                "spec": spec.identity(),
            }
        source_name = items_from[0]
        all_deps = (*deps, source_name)
        if carry_from is not None:
            all_deps = (*all_deps, carry_from[0])

        def register(function: Any) -> Any:
            metadata = _validate_registration(function)
            self._register_node(
                name,
                function,
                deps=tuple(dict.fromkeys(all_deps)),
                prompt_specs=node_prompt_specs,
                files=tuple(Path(path) for path in files),
                params=copy.deepcopy(params) if params is not None else {},
                resources=node_resources,
                consumes=consumes,
                items_from=items_from,
                key_fn=key_fn,
                files_fn=files_fn,
                aggregate_fn=aggregate_fn,
                scan=True,
                carry_from=carry_from,
                carry_fn=carry_fn,
                cache=node_cache,
                external_fingerprint_digest=external_fingerprint_digest(identity),
                metadata=metadata,
                executor="agent",
                agent_adapter=adapter,
                agent_spec=spec,
                agent_identity=copy.deepcopy(identity),
                evidence_policy=evidence_policy,
                retry=retry_policy,
            )
            return function

        return register

    def map(
        self,
        name: str,
        *,
        items_from: tuple[str, str],
        key_fn: Callable[[Any], str] | None = None,
        deps: Iterable[str] = (),
        prompt_specs: Iterable[PromptSpec] = (),
        files: Iterable[str | Path] = (),
        files_fn: Callable[[Any], Iterable[str | Path]] | None = None,
        params: dict[str, Any] | None = None,
        aggregate_fn: AggregateFunction | None = None,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint: Any | None = None,
        resources: Iterable[ResourceRequest] | None = None,
        retry: RetryPolicy | None = None,
    ) -> Callable[[MapFunction], MapFunction]:
        """Register a runtime-data fan-out node while retaining one static graph vertex.

        ``aggregate_fn`` must be a pure function of item artifacts and their order.
        It controls only the downstream aggregate; item caching remains unchanged.
        """
        _validate_name(name, "Map node")
        _validate_artifact_locator(items_from, "items_from")
        source_name, artifact_key = items_from
        map_deps = tuple(dict.fromkeys((*deps, source_name)))
        map_prompt_specs = validate_prompt_specs(
            tuple(prompt_specs),
            dynamic_kind="map",
        )
        map_files = tuple(Path(path) for path in files)
        map_params = copy.deepcopy(params) if params is not None else {}
        map_cache = validate_cache_policy(cache)
        fingerprint_digest = external_fingerprint_digest(external_fingerprint)
        map_resources = _validate_resources(resources, "Map node")
        retry_policy = _validate_retry_policy(retry)

        def register(function: MapFunction) -> MapFunction:
            metadata = _validate_registration(function)
            self._register_node(
                name,
                function,
                deps=map_deps,
                prompt_specs=map_prompt_specs,
                files=map_files,
                params=map_params,
                resources=map_resources,
                consumes=consumes,
                items_from=(source_name, artifact_key),
                key_fn=key_fn,
                files_fn=files_fn,
                aggregate_fn=aggregate_fn,
                cache=map_cache,
                external_fingerprint_digest=fingerprint_digest,
                retry=retry_policy,
                metadata=metadata,
            )
            return function

        return register

    def scan(
        self,
        name: str,
        *,
        items_from: tuple[str, str],
        key_fn: Callable[[Any], str] | None = None,
        carry_from: tuple[str, str] | None = None,
        carry_fn: Callable[[dict[str, Any]], Any] | None = None,
        deps: Iterable[str] = (),
        prompt_specs: Iterable[PromptSpec] = (),
        files: Iterable[str | Path] = (),
        files_fn: Callable[[Any], Iterable[str | Path]] | None = None,
        params: dict[str, Any] | None = None,
        aggregate_fn: AggregateFunction | None = None,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint: Any | None = None,
        resources: Iterable[ResourceRequest] | None = None,
        retry: RetryPolicy | None = None,
    ) -> Callable[[ScanFunction], ScanFunction]:
        """Register a runtime list whose items form one carry-dependent serial chain."""
        _validate_name(name, "Scan node")
        _validate_artifact_locator(items_from, "items_from")
        if carry_from is not None:
            _validate_artifact_locator(carry_from, "carry_from")
        source_name, artifact_key = items_from
        carry_source = carry_from[0] if carry_from is not None else None
        all_deps = (*deps, source_name)
        if carry_source is not None:
            all_deps = (*all_deps, carry_source)
        scan_deps = tuple(dict.fromkeys(all_deps))
        scan_prompt_specs = validate_prompt_specs(
            tuple(prompt_specs),
            dynamic_kind="scan",
        )
        scan_files = tuple(Path(path) for path in files)
        scan_params = copy.deepcopy(params) if params is not None else {}
        scan_cache = validate_cache_policy(cache)
        fingerprint_digest = external_fingerprint_digest(external_fingerprint)
        scan_resources = _validate_resources(resources, "Scan node")
        retry_policy = _validate_retry_policy(retry)

        def register(function: ScanFunction) -> ScanFunction:
            metadata = _validate_registration(function)
            self._register_node(
                name,
                function,
                deps=scan_deps,
                prompt_specs=scan_prompt_specs,
                files=scan_files,
                params=scan_params,
                resources=scan_resources,
                consumes=consumes,
                items_from=(source_name, artifact_key),
                key_fn=key_fn,
                files_fn=files_fn,
                aggregate_fn=aggregate_fn,
                scan=True,
                carry_from=carry_from,
                carry_fn=carry_fn,
                cache=scan_cache,
                external_fingerprint_digest=fingerprint_digest,
                retry=retry_policy,
                metadata=metadata,
            )
            return function

        return register

    def foreach(
        self,
        name_template: str,
        items: Iterable[Any],
        *,
        deps: Iterable[str] | Callable[[Any], Iterable[str]] = (),
        prompt_specs: Iterable[PromptSpec] = (),
        files: Iterable[str | Path] = (),
        files_fn: Callable[[Any], Iterable[str | Path]] | None = None,
        params: dict[str, Any] | None = None,
        params_fn: Callable[[Any], dict[str, Any]] | None = None,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint: Any | None = None,
        resources: Iterable[ResourceRequest] | None = None,
        retry: RetryPolicy | None = None,
    ) -> Callable[[NodeFunction], NodeFunction]:
        """Register one node per item, fixing names, dependencies, and params immediately."""
        # 生成器只能消费一次;不先固定,第二个 item 起声明就静默变空。
        fixed_deps = deps if callable(deps) else tuple(deps)
        fixed_prompt_specs = validate_prompt_specs(
            tuple(prompt_specs),
            dynamic_kind="node",
        )
        fixed_files = tuple(Path(path) for path in files)
        fixed_params = copy.deepcopy(params) if params is not None else {}
        fixed_cache = validate_cache_policy(cache)
        fingerprint_digest = external_fingerprint_digest(external_fingerprint)
        fixed_resources = _validate_resources(resources, "Foreach node")
        retry_policy = _validate_retry_policy(retry)
        fixed_items: list[tuple[str, tuple[str, ...], tuple[Path, ...], dict[str, Any]]] = []
        for index, raw_item in enumerate(items):
            item = copy.deepcopy(raw_item)
            format_values = {"i": index}
            if isinstance(item, Mapping):
                format_values.update(item)
            node_name = name_template.format(**format_values)
            item_deps = tuple(fixed_deps(item)) if callable(fixed_deps) else fixed_deps
            item_files = (
                fixed_files + tuple(Path(path) for path in files_fn(item))
                if files_fn is not None
                else fixed_files
            )
            item_params = copy.deepcopy(fixed_params)
            if params_fn is not None:
                # 逐项参数优先，才能让共享默认值被 item 的明确声明覆盖。
                item_params.update(copy.deepcopy(params_fn(item)))
            fixed_items.append((node_name, item_deps, item_files, item_params))

        def register(function: NodeFunction) -> NodeFunction:
            # 同一函数对象逐项重复做 AST 校验是纯浪费;fan-out 只验一次。
            metadata = _validate_registration(function)
            for node_name, item_deps, item_files, item_params in fixed_items:
                self._register_node(
                    node_name,
                    function,
                    deps=item_deps,
                    prompt_specs=fixed_prompt_specs,
                    files=item_files,
                    params=item_params,
                    resources=fixed_resources,
                    consumes=consumes,
                    cache=fixed_cache,
                    external_fingerprint_digest=fingerprint_digest,
                    retry=retry_policy,
                    metadata=metadata,
                )
            return function

        return register

    def _register_node(
        self,
        name: str,
        function: NodeFunction | MapFunction | ScanFunction,
        *,
        deps: tuple[str, ...],
        prompt_specs: tuple[PromptSpec, ...],
        files: tuple[Path, ...],
        params: dict[str, Any],
        resources: tuple[ResourceRequest, ...] = (),
        consumes: Mapping[str, ConsumeFunction] | None = None,
        items_from: tuple[str, str] | None = None,
        key_fn: Callable[[Any], str] | None = None,
        files_fn: Callable[[Any], Iterable[str | Path]] | None = None,
        aggregate_fn: AggregateFunction | None = None,
        scan: bool = False,
        carry_from: tuple[str, str] | None = None,
        carry_fn: Callable[[dict[str, Any]], Any] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint_digest: str | None = None,
        input_bindings: tuple[tuple[str, str], ...] = (),
        local_items_source: str | None = None,
        local_carry_source: str | None = None,
        subgraph: str | None = None,
        metadata: _NodeAstMetadata | None = None,
        executor: str = "function",
        agent_adapter: AgentAdapter | None = None,
        agent_spec: AgentSpec | None = None,
        agent_identity: dict[str, Any] | None = None,
        evidence_policy: EvidencePolicy | None = None,
        retry: RetryPolicy | None = None,
    ) -> None:
        _validate_name(name, "Node")
        if name in self._nodes:
            raise ValueError(f"Node {name!r} is already registered")
        projections = validate_consumes(
            name,
            deps,
            consumes,
            items_from=items_from,
            carry_from=carry_from,
        )
        function_inputs = (
            {local for local, _actual in input_bindings} if input_bindings else set(deps)
        )
        if items_from is not None:
            function_inputs.discard(local_items_source or items_from[0])
        if scan and carry_from is not None:
            function_inputs.discard(local_carry_source or carry_from[0])
        validate_prompt_bindings(
            prompt_specs,
            inputs=function_inputs,
            params=set(params),
        )
        self._nodes[name] = _Node(
            name=name,
            function=function,
            deps=deps,
            prompt_specs=prompt_specs,
            files=files,
            params=params,
            consumes=projections,
            resources=resources,
            items_from=items_from,
            key_fn=key_fn,
            files_fn=files_fn,
            aggregate_fn=aggregate_fn,
            scan=scan,
            carry_from=carry_from,
            carry_fn=carry_fn,
            validated_models=metadata.validated_models if metadata is not None else (),
            model_classes=metadata.model_classes if metadata is not None else (),
            checkpoints=metadata.checkpoints if metadata is not None else (),
            cache=cache,
            external_fingerprint_digest=external_fingerprint_digest,
            input_bindings=input_bindings,
            local_items_source=local_items_source,
            local_carry_source=local_carry_source,
            subgraph=subgraph,
            executor=executor,
            agent_adapter=agent_adapter,
            agent_spec=agent_spec,
            agent_identity=agent_identity,
            evidence_policy=evidence_policy,
            retry=retry,
        )

    def mount(
        self,
        subgraph: Subgraph,
        namespace: str,
        *,
        inputs: Mapping[str, str],
    ) -> dict[str, str]:
        """Mount one frozen static template into this DAG's existing registry."""
        if not isinstance(subgraph, Subgraph):
            raise TypeError("subgraph must be a Subgraph")
        mounted_namespace = validate_segment(namespace, "Subgraph namespace")
        if mounted_namespace in self._subgraphs:
            raise ValueError(f"Subgraph namespace {mounted_namespace!r} is already mounted")

        bindings = dict(inputs)
        expected = set(subgraph.inputs)
        received = set(bindings)
        missing = sorted(expected - received)
        extra = sorted(received - expected)
        if missing or extra:
            details = []
            if missing:
                details.append("missing: " + ", ".join(missing))
            if extra:
                details.append("extra: " + ", ".join(extra))
            raise ValueError("Input bindings must exactly match ports (" + "; ".join(details) + ")")
        for port in subgraph.inputs:
            outer = bindings[port]
            if not isinstance(outer, str) or outer not in self._nodes:
                raise ValueError(
                    f"Subgraph input {port!r} must bind an existing outer Dag node, got {outer!r}"
                )

        local_nodes = subgraph._nodes
        valid_refs = set(subgraph.inputs) | set(local_nodes)
        for local_name, declaration in local_nodes.items():
            for reference in declaration.deps:
                if reference not in valid_refs:
                    raise ValueError(
                        f"Unknown local reference {reference!r} for subgraph node {local_name!r}"
                    )
        for port, target in subgraph.outputs.items():
            if target not in local_nodes:
                raise ValueError(
                    f"Unknown subgraph output target {target!r} for output port {port!r}"
                )

        state: dict[str, int] = {}

        def visit(local_name: str) -> None:
            if state.get(local_name) == 1:
                raise ValueError(f"Cycle detected at subgraph node {local_name!r}")
            if state.get(local_name) == 2:
                return
            state[local_name] = 1
            for reference in local_nodes[local_name].deps:
                if reference in local_nodes:
                    visit(reference)
            state[local_name] = 2

        for local_name in local_nodes:
            visit(local_name)

        qualified = {local_name: f"{mounted_namespace}.{local_name}" for local_name in local_nodes}
        collisions = [name for name in qualified.values() if name in self._nodes]
        if collisions:
            raise ValueError(f"Node {collisions[0]!r} is already registered")

        def resolve_local(reference: str) -> str:
            return bindings[reference] if reference in bindings else qualified[reference]

        metadata = {
            local_name: _validate_registration(declaration.function)
            for local_name, declaration in local_nodes.items()
        }
        pending_nodes: dict[str, _Node] = {}
        for local_name, declaration in local_nodes.items():
            actual_deps = tuple(
                dict.fromkeys(resolve_local(reference) for reference in declaration.deps)
            )
            items_from = (
                (resolve_local(declaration.items_from[0]), declaration.items_from[1])
                if declaration.items_from is not None
                else None
            )
            carry_from = (
                (resolve_local(declaration.carry_from[0]), declaration.carry_from[1])
                if declaration.carry_from is not None
                else None
            )
            ast_metadata = metadata[local_name]
            pending_nodes[qualified[local_name]] = _Node(
                name=qualified[local_name],
                function=declaration.function,
                deps=actual_deps,
                prompt_specs=declaration.prompt_specs,
                files=declaration.files,
                params=copy.deepcopy(declaration.params),
                consumes=dict(declaration.consumes),
                items_from=items_from,
                key_fn=declaration.key_fn,
                files_fn=declaration.files_fn,
                aggregate_fn=declaration.aggregate_fn,
                scan=declaration.scan,
                carry_from=carry_from,
                carry_fn=declaration.carry_fn,
                validated_models=ast_metadata.validated_models,
                model_classes=ast_metadata.model_classes,
                checkpoints=ast_metadata.checkpoints,
                cache=declaration.cache,
                external_fingerprint_digest=declaration.external_fingerprint_digest,
                input_bindings=tuple(
                    (reference, resolve_local(reference)) for reference in declaration.deps
                ),
                local_items_source=(
                    declaration.items_from[0] if declaration.items_from is not None else None
                ),
                local_carry_source=(
                    declaration.carry_from[0] if declaration.carry_from is not None else None
                ),
                subgraph=mounted_namespace,
            )

        output_bindings = {port: qualified[target] for port, target in subgraph.outputs.items()}
        mounted_description = {
            "inputs": {port: bindings[port] for port in subgraph.inputs},
            "outputs": copy.deepcopy(output_bindings),
            "nodes": list(pending_nodes),
        }
        subgraph._freeze()
        self._nodes.update(pending_nodes)
        self._subgraphs[mounted_namespace] = mounted_description
        return output_bindings

    def _build_permit_plane(
        self,
        order: Iterable[str],
        *,
        workers: int,
        resource_limits: Mapping[str | None, int] | None,
    ) -> tuple[_PermitPlane, int]:
        """Build one run-local plane and enough workers to serve its declared limits."""
        if resource_limits is not None and not isinstance(resource_limits, Mapping):
            raise TypeError("resource_limits must be a mapping or None")

        configured: dict[str | None, int] = {}
        if resource_limits is not None:
            for name, limit in resource_limits.items():
                if name is not None and not isinstance(name, str):
                    raise TypeError("resource_limits keys must be strings or None")
                if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
                    raise ValueError(f"resource_limits[{name!r}] must be a positive integer")
                configured[name] = limit

        used_names: set[str | None] = set()
        node_requests: dict[str, dict[str | None, int]] = {}
        for name in order:
            node = self._nodes[name]
            grouped: dict[str | None, int] = {}
            if node.resources:
                for request in node.resources:
                    grouped[request.name] = grouped.get(request.name, 0) + request.units
                used_names.update(grouped)
            else:
                grouped[None] = 1
                used_names.add(None)
            node_requests[name] = grouped

        limits = {name: configured.get(name, workers) for name in used_names}
        for node_name, grouped in node_requests.items():
            for name, units in grouped.items():
                limit = limits[name]
                if units > limit:
                    raise ValueError(
                        f"Node {node_name!r} requests {units} units of resource {name!r}, "
                        f"but its limit is {limit}"
                    )

        # With no resource_limits, workers retains its old global ceiling. When
        # limits are supplied, distinct pools may run in parallel, so the shared
        # executor needs the sum of their possible slots.
        execution_workers = (
            workers if resource_limits is None else max(workers, sum(limits.values(), 0))
        )
        return _PermitPlane(limits), execution_workers

    def run(
        self,
        targets: Iterable[str] | None = None,
        run_id: str | None = None,
        force: Iterable[str] = (),
        workers: int = 1,
        resource_limits: Mapping[str | None, int] | None = None,
    ) -> RunResult:
        """Run a topological target closure and persist every completed node artifact."""
        if type(workers) is not int or workers < 1:
            raise ValueError("workers must be a positive integer")
        requested_force = tuple(force)
        existing_manifest: dict[str, Any] | None = None
        dynamic_files_ledger: dict[str, dict[str, list[dict[str, str]]]] = {}
        if run_id is not None:
            manifest_path = store.run_directory(self.config.artifacts_path, run_id) / "_run.json"
            try:
                candidate_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (FileNotFoundError, OSError, json.JSONDecodeError):
                candidate_manifest = None
            if isinstance(candidate_manifest, dict):
                if candidate_manifest.get("run_manifest_schema") == RUN_MANIFEST_SCHEMA:
                    self._validate_execution_manifest_profile(run_id, candidate_manifest)
                    dynamic_files_ledger = self._validate_dynamic_files_ledger(
                        run_id, candidate_manifest
                    )
                existing_manifest = candidate_manifest
        selected = (
            tuple(existing_manifest.get("targets", ()))
            if targets is None and existing_manifest is not None
            else tuple(self._nodes)
            if targets is None
            else tuple(targets)
        )
        if not requested_force and existing_manifest is not None:
            requested_force = tuple(existing_manifest.get("force", ()))
        order = self._topological_order(selected)
        permit_plane, execution_workers = self._build_permit_plane(
            order,
            workers=workers,
            resource_limits=resource_limits,
        )
        current_run_id = (
            run_id if run_id is not None else store.allocate_run_id(self.config.artifacts_path)
        )
        artifacts: dict[str, dict[str, Any]] = {}
        cache_hits: list[str] = []
        map_items: dict[str, dict[str, str]] = {}
        pending_checkpoints: list[tuple[str, str]] = []
        pending_retry_targets: list[str] = []
        skipped: list[str] = []
        forced_nodes, forced_items = self._parse_forced(requested_force)
        state_lock = threading.RLock()
        archive_lock = threading.Lock()
        allocated_archive: list[str] = []
        run_dir = store.run_directory(self.config.artifacts_path, current_run_id)

        def ensure_archive_id() -> str:
            # 一次 run 只允许一个归档目录;并发节点必须经同一把锁拿同一个 id,
            # 否则两个线程会各自开一个 history 目录,把一次 run 的归档拆碎。
            with archive_lock:
                if not allocated_archive:
                    allocated_archive.append(store.next_history_id(run_dir / "history"))
                return allocated_archive[0]

        # 源码只在 run 开始读一次:中途改文件不得让同一 run 内的缓存键漂移。
        libs_identities = self._libs_identities(self._nodes[name] for name in order)
        libs_hashes = {name: identity.digest for name, identity in libs_identities.items()}
        prompt_snapshot = self._prompt_snapshot()
        file_snapshot = _FileSnapshot.capture(
            self.config.project_root,
            self.config.resolve,
            (path for name in order for path in self._nodes[name].files),
        )
        attempt_store = AttemptStore(
            run_dir,
            self._run_manifest_identity(
                current_run_id,
                selected,
                requested_force,
                order,
                libs_hashes,
                prompt_snapshot,
                file_snapshot,
                dynamic_files_ledger,
            ),
        )
        attempt_store.initialize()
        if existing_manifest is not None:
            attempt_store.mark_resumed()
        attempt_store.update_manifest("running")
        envelope = ExecutionEnvelope(
            artifacts_path=self.config.artifacts_path,
            run_id=current_run_id,
            resolve=self.config.resolve,
            blob_store=self.blob_store,
            ensure_archive_id=ensure_archive_id,
            approval_path=lambda name: self._approval_path(current_run_id, name),
        )

        states = {name: "waiting" for name in order}
        failures: dict[str, BaseException] = {}
        artifact_shas: dict[str, str] = {}
        budget_abort = threading.Event()
        budget = getattr(self.caller, "budget", None)
        budget_limited = budget is not None and budget.max_tokens is not None
        budget_window = max(1, workers - 1) if budget_limited else execution_workers

        def execute(node_name: str) -> tuple[str, str | None]:
            if budget_abort.is_set():
                return node_name, "skipped"
            node = self._nodes[node_name]
            libs_cache_reusable = libs_identities[node.name].cache_reusable
            evidence_policy = node.evidence_policy or self._caller_evidence_policy()
            agent_provenance: dict[str, Any] | None = None
            resumed_completed = False
            checkpoint_used = False
            with state_lock:
                inputs = {dependency: artifacts[dependency] for dependency in node.deps}
                upstream_shas = {dependency: artifact_shas[dependency] for dependency in node.deps}
            started = time.monotonic()
            cache_key: str | list[str]
            item_cache_keys: list[str] | None = None
            key_components: dict[str, str] | None = None
            prompt_resolutions: dict[str, ResolvedPrompt] = {}
            prompt_resolution_records: dict[str, Any] = {}
            function_inputs: dict[str, dict[str, Any]] = {}
            cache_entry: store.CacheEntry | None = None
            effective_cache_policy = node.cache if libs_cache_reusable else "off"
            if node.items_from is None:
                function_inputs = self._function_inputs(node, inputs)
                file_contents = self._file_contents(node, file_snapshot=file_snapshot)
                prompt_resolutions = self._resolve_prompt_specs(
                    node,
                    prompt_snapshot,
                    inputs,
                    function_inputs=function_inputs,
                    file_contents=file_contents,
                )
                prompt_resolution_records = {
                    name: resolved.resolution.canonical()
                    for name, resolved in prompt_resolutions.items()
                }
                key_components = self._key_components(
                    node,
                    upstream_shas,
                    libs_hashes[node.name],
                    upstream_artifacts=inputs,
                    prompt_snapshot=prompt_snapshot,
                    prompt_resolutions=prompt_resolutions,
                    projected_inputs=function_inputs,
                    file_contents=file_contents,
                )
                cache_key = sha(key_components)
                prior_state = (
                    attempt_store.state_for(node.name) if existing_manifest is not None else None
                )
                run_artifact = run_dir / f"{node.name}.json"
                run_sidecar = run_dir / f"{node.name}.json.meta.json"
                has_artifact = run_artifact.is_file()
                has_sidecar = run_sidecar.is_file()
                if existing_manifest is not None and (has_artifact or has_sidecar):
                    if not (has_artifact and has_sidecar):
                        raise RunManifestError(
                            f"Target {node.name!r} has an incomplete run artifact pair"
                        )
                    if prior_state is None:
                        raise RunManifestError(
                            f"Target {node.name!r} has no durable state/candidate ownership"
                        )
                    if prior_state.get("status") == "completed":
                        artifact, prior_metadata = self._resume_completed_artifact(
                            run_dir,
                            node.name,
                            key_components,
                            cache_key,
                            state=prior_state,
                            validate_agent=node.executor == "agent",
                            prompt_resolutions=prompt_resolution_records,
                        )
                        origin = prior_metadata.get("origin_provenance")
                        if isinstance(origin, dict) and isinstance(origin.get("agent"), dict):
                            agent_provenance = copy.deepcopy(origin["agent"])
                        cache_hit = prior_metadata.get("cache") == "hit"
                        resumed_completed = True
                    elif prior_state.get("status") == "success_candidate":
                        artifact, cache_hit = None, False
                    else:
                        raise RunManifestError(
                            f"Target {node.name!r} run artifact pair is not owned by a "
                            "completed state/candidate"
                        )
                elif prior_state is not None:
                    artifact, cache_hit = None, False
                else:
                    try:
                        cache_entry = self._cache_entry_for_lookup(
                            cache_key,
                            forced=node.name in forced_nodes,
                            cache_policy=(node.cache if libs_cache_reusable else "off"),
                            evidence_policy=evidence_policy,
                        )
                    except CacheIntegrityError as error:
                        assert key_components is not None
                        self._record_cache_lookup_failure(
                            attempt_store,
                            node.name,
                            error,
                            policy=node.retry,
                            declaration_digest=self._attempt_declaration_digest(
                                node,
                                key_components,
                                evidence_policy,
                            ),
                            prompt_resolutions=prompt_resolution_records,
                        )
                        raise
                    artifact = cache_entry.artifact if cache_entry is not None else None
                    cache_hit = cache_entry is not None
            else:
                artifact = None
                cache_hit = False
                cache_key = []
            if (
                node.executor == "agent"
                and artifact is not None
                and cache_hit
                and isinstance(cache_key, str)
                and agent_provenance is None
            ):
                origin = cache_entry.origin if cache_entry is not None else None
                retained_agent = origin.get("agent") if isinstance(origin, dict) else None
                if not isinstance(retained_agent, dict):
                    raise ValueError(
                        f"Agent cache entry for {node.name!r} has no origin provenance"
                    )
                agent_provenance = copy.deepcopy(retained_agent)
            try:
                with observe() as calls:
                    if node.items_from is not None:
                        execute_dynamic = self._execute_scan if node.scan else self._execute_map
                        (
                            artifact,
                            cache_hit,
                            item_cache_keys,
                            item_statuses,
                            effective_cache_policy,
                        ) = execute_dynamic(
                            node,
                            inputs,
                            upstream_shas,
                            current_run_id,
                            libs_hashes[node.name],
                            libs_cache_reusable,
                            workers,
                            permit_plane=permit_plane,
                            executor=execution_executor,
                            forced_all=node.name in forced_nodes,
                            forced_items=forced_items.get(node.name, set()),
                            envelope=envelope,
                            attempt_store=attempt_store,
                            prompt_snapshot=prompt_snapshot,
                            budget_abort=budget_abort,
                            file_snapshot=file_snapshot,
                            allow_new_dynamic_files_ledger=existing_manifest is None,
                        )
                        cache_key = item_cache_keys
                        with state_lock:
                            map_items[node.name] = item_statuses
                    elif artifact is None:
                        assert key_components is not None
                        if budget_abort.is_set():
                            return node_name, "skipped"
                        declaration_digest = self._attempt_declaration_digest(
                            node,
                            key_components,
                            evidence_policy,
                        )
                        prepared = attempt_store.prepare(
                            node.name,
                            policy=node.retry,
                            declaration_digest=declaration_digest,
                            prompt_resolutions=prompt_resolution_records,
                        )
                        action = prepared["action"]
                        if action == "pending":
                            with state_lock:
                                pending_retry_targets.append(node.name)
                            return node_name, "retry_pending"
                        if action == "failed":
                            raise RunManifestError(
                                f"Run target {node.name!r} is already terminally failed"
                            )
                        if action == "completed":
                            artifact, prior_metadata = self._resume_completed_artifact(
                                run_dir,
                                node.name,
                                key_components,
                                cache_key,
                                state=prepared["state"],
                                validate_agent=node.executor == "agent",
                                prompt_resolutions=prompt_resolution_records,
                            )
                            origin = prior_metadata.get("origin_provenance")
                            if isinstance(origin, dict) and isinstance(origin.get("agent"), dict):
                                agent_provenance = copy.deepcopy(origin["agent"])
                            resumed_completed = True
                        elif action == "candidate":
                            candidate = prepared["candidate"]
                            if (
                                candidate.get("candidate_schema") != SUCCESS_CANDIDATE_SCHEMA
                                or candidate.get("cache_key") != cache_key
                                or candidate.get("key_components") != key_components
                                or candidate.get("prompt_resolutions") != prompt_resolution_records
                                or not isinstance(candidate.get("artifact"), dict)
                            ):
                                raise RunManifestError(
                                    f"Success candidate for {node.name!r} no longer "
                                    "matches its declaration"
                                )
                            artifact = copy.deepcopy(candidate["artifact"])
                            saved_calls = candidate.get("calls", [])
                            if not isinstance(saved_calls, list):
                                raise RunManifestError(
                                    f"Success candidate calls for {node.name!r} are invalid"
                                )
                            calls.extend(copy.deepcopy(saved_calls))
                            saved_agent = candidate.get("agent_provenance")
                            agent_provenance = (
                                copy.deepcopy(saved_agent)
                                if isinstance(saved_agent, dict)
                                else None
                            )
                            started = time.monotonic() - float(candidate.get("seconds", 0.0))
                            checkpoint_used = candidate.get("checkpoint_used") is True
                        else:
                            context = NodeContext(
                                self,
                                node,
                                current_run_id,
                                prompt_resolutions=prompt_resolutions,
                                file_snapshot=file_snapshot,
                            )
                            try:
                                function_inputs = copy.deepcopy(function_inputs)
                                boundary = (
                                    durable_side_effect_boundary(
                                        lambda effect: attempt_store.mark_side_effect(
                                            node.name, effect
                                        )
                                    )
                                    if node.executor not in {"pure", "agent"}
                                    else nullcontext()
                                )
                                with permit_plane.acquire(node.resources), boundary:
                                    if node.executor == "agent":
                                        build_context = AgentBuildContext(
                                            node.params,
                                            context.read_text,
                                            context.read_bytes,
                                            context.resolve_prompt,
                                        )
                                        task = node.function(  # type: ignore[call-arg]
                                            function_inputs, build_context
                                        )
                                        if not isinstance(task, AgentTask):
                                            raise TypeError(
                                                f"Agent node {node.name!r} builder "
                                                "must return AgentTask"
                                            )
                                        instruction_resolution = (
                                            task.instruction.resolution.canonical()
                                            if isinstance(task.instruction, ResolvedPrompt)
                                            else None
                                        )
                                        try:
                                            lease_context = self.agent_slots.acquire(
                                                timeout_seconds=(
                                                    self.config.agent_slot_timeout_seconds
                                                )
                                            )
                                            with lease_context as lease:
                                                assert node.agent_adapter is not None
                                                assert node.agent_spec is not None
                                                assert node.agent_identity is not None
                                                attempt_store.mark_side_effect(
                                                    node.name,
                                                    {
                                                        "active_effect_schema": 1,
                                                        "kind": "agent",
                                                        "instruction_sha256": sha(
                                                            str(task.instruction)
                                                        ),
                                                        "managed": (
                                                            instruction_resolution is not None
                                                        ),
                                                        "prompt_resolution": (
                                                            instruction_resolution
                                                        ),
                                                    },
                                                )
                                                outcome = execute_agent_task(
                                                    node_name=node.name,
                                                    run_id=current_run_id,
                                                    task=task,
                                                    inputs=function_inputs,
                                                    declared_files=node.files,
                                                    declared_file_contents=file_contents,
                                                    resolve=self.config.resolve,
                                                    artifacts_path=(self.config.artifacts_path),
                                                    blob_store=self.blob_store,
                                                    adapter=node.agent_adapter,
                                                    adapter_identity=node.agent_identity,
                                                    spec=node.agent_spec,
                                                    evidence_policy=evidence_policy,
                                                    prompt_resolution=instruction_resolution,
                                                )
                                        except SlotTimeoutError:
                                            capacity_error = AgentExecutionFailure(
                                                runtime_code=(AgentRuntimeFailureCode.CAPACITY)
                                            )
                                            atomic_write_json(
                                                run_dir / "failures" / f"{node.name}.json",
                                                {
                                                    "failure_schema": FAILURE_SCHEMA,
                                                    "node": node.name,
                                                    "task_sha256": sha(task.canonical()),
                                                    "instruction_sha256": sha(
                                                        str(task.instruction)
                                                    ),
                                                    "instruction_evidence": scrub_evidence(
                                                        str(task.instruction),
                                                        mode=evidence_policy.request,
                                                    ),
                                                    "prompt_resolution": (instruction_resolution),
                                                    "status": "failed",
                                                    "failure": canonical_failure(capacity_error),
                                                    "usage": None,
                                                    "stop_reason": "capacity",
                                                    "duration_seconds": 0.0,
                                                    "workspace_manifest": [],
                                                    "attachments": [],
                                                    "published": [],
                                                    "trajectory": None,
                                                    "evidence": [],
                                                    "evidence_policy": (
                                                        evidence_policy.canonical()
                                                    ),
                                                    "evidence_policy_digest": (
                                                        evidence_policy.digest
                                                    ),
                                                },
                                            )
                                            raise capacity_error from None
                                        artifact = outcome.artifact
                                        agent_provenance = {
                                            **outcome.provenance,
                                            "queue_wait_seconds": lease.wait_seconds,
                                            "slot_identity": lease.slot_identity,
                                        }
                                    else:
                                        artifact = node.function(  # type: ignore[call-arg]
                                            function_inputs, context
                                        )
                            except CheckpointPending as pending:
                                envelope.record_pending(pending.name, pending.payload)
                                attempt_store.mark_checkpoint(node.name, pending.name)
                                with state_lock:
                                    pending_checkpoints.append((node_name, pending.name))
                                return node_name, "pending"
                            except Exception as error:
                                failure_outcome = attempt_store.record_failure(
                                    node.name,
                                    error,
                                    policy=node.retry,
                                    calls=calls,
                                )
                                if failure_outcome["action"] == "pending":
                                    with state_lock:
                                        pending_retry_targets.append(node.name)
                                    return node_name, "retry_pending"
                                if (
                                    node.retry is not None
                                    and node.retry.allows(failure_provider_kind(error))
                                    and int(failure_outcome["state"]["attempt"])
                                    >= node.retry.max_attempts
                                ):
                                    raise RetryExhausted(
                                        node.name,
                                        int(failure_outcome["state"]["attempt"]),
                                        canonical_failure(error),
                                    ) from error
                                raise
                            if not isinstance(artifact, dict):
                                raise TypeError(f"Node {node.name!r} must return a dict artifact")
                            artifact = json.loads(canonical_json(artifact))
                            checkpoint_used = context._checkpoint_used
                            attempt_store.save_candidate(
                                node.name,
                                {
                                    "candidate_schema": SUCCESS_CANDIDATE_SCHEMA,
                                    "artifact": artifact,
                                    "cache_key": cache_key,
                                    "key_components": key_components,
                                    "calls": copy.deepcopy(calls),
                                    "agent_provenance": copy.deepcopy(agent_provenance),
                                    "seconds": time.monotonic() - started,
                                    "checkpoint_used": checkpoint_used,
                                    "prompt_resolutions": prompt_resolution_records,
                                },
                            )
                if (
                    node.items_from is None
                    and artifact is not None
                    and cache_hit
                    and not resumed_completed
                ):
                    assert key_components is not None
                    declaration_digest = self._attempt_declaration_digest(
                        node,
                        key_components,
                        evidence_policy,
                    )
                    prepared = attempt_store.prepare(
                        node.name,
                        policy=node.retry,
                        declaration_digest=declaration_digest,
                        prompt_resolutions=prompt_resolution_records,
                    )
                    if prepared["action"] != "run":
                        raise RunManifestError(
                            f"Cache hit for {node.name!r} has an unexpected durable state"
                        )
                    attempt_store.save_candidate(
                        node.name,
                        {
                            "candidate_schema": SUCCESS_CANDIDATE_SCHEMA,
                            "artifact": artifact,
                            "cache_key": cache_key,
                            "key_components": key_components,
                            "calls": copy.deepcopy(calls),
                            "agent_provenance": copy.deepcopy(agent_provenance),
                            "seconds": time.monotonic() - started,
                            "checkpoint_used": False,
                            "prompt_resolutions": prompt_resolution_records,
                        },
                    )
                if node.items_from is None:
                    effective_cache_policy = (
                        "off" if checkpoint_used or not libs_cache_reusable else node.cache
                    )
                if node.items_from is None and not resumed_completed and not cache_hit:
                    # miss 路径喂下游的必须与命中路径同形态:命中读的是
                    # 排序后的磁盘 JSON,活字典键序不能让下游 prompt 漂移。
                    artifact = envelope.seal(
                        artifact,
                        cache_key,
                        label=f"Node {node.name!r}",
                        calls=calls,
                        cache_policy=effective_cache_policy,
                        evidence_policy=evidence_policy,
                        agent_provenance=agent_provenance,
                        prompt_resolutions=prompt_resolution_records,
                    )
                if node.executor == "agent" and node.items_from is None:
                    validate_agent_artifact(artifact, self.blob_store)
                    if isinstance(agent_provenance, dict):
                        validate_agent_provenance(
                            agent_provenance,
                            self.blob_store,
                        )
                elapsed = time.monotonic() - started
                with state_lock:
                    if cache_hit:
                        cache_hits.append(node.name)
                    outputs = envelope.materialize(
                        node.name,
                        artifact,
                        allow_item_owners=node.items_from is not None,
                    )
                    if self.post_node is not None and not resumed_completed:
                        self.post_node(node.name, artifact, cache_hit)
                    artifacts[node.name] = artifact
                    artifact_sha256 = sha(artifact)
                    artifact_shas[node.name] = artifact_sha256
                    if not resumed_completed:
                        envelope.write_sidecar(
                            node.name,
                            artifact,
                            cache_key,
                            # The dynamic aggregate is a derived view of
                            # item materializations, not an independently
                            # resumable attempt.  Its sidecar therefore
                            # has no node state/candidate ownership; mark
                            # it as the framework's state-less cache-hit
                            # shape while preserving the real item cache
                            # statuses in the result and item sidecars.
                            cache_hit=(cache_hit or node.items_from is not None),
                            cache_entry=cache_entry,
                            seconds=elapsed,
                            calls=calls,
                            key_components=key_components,
                            outputs=outputs,
                            cache_policy=effective_cache_policy,
                            evidence_policy=evidence_policy,
                            agent_provenance=agent_provenance,
                            prompt_resolutions=prompt_resolution_records,
                        )
                    if node.items_from is None and artifact is not None and not resumed_completed:
                        attempt_store.mark_completed(
                            node.name,
                            artifact_sha256=artifact_sha256,
                        )
                return node_name, "success"
            except BudgetExceeded:
                budget_abort.set()
                raise
            except _MapCheckpointPending as pending:
                with state_lock:
                    pending_checkpoints.extend((node_name, name) for name in pending.names)
                return node_name, "pending"
            except _MapRetryPending as pending:
                with state_lock:
                    pending_retry_targets.extend(pending.names)
                return node_name, "retry_pending"

        def classify_ready() -> list[str]:
            ready: list[str] = []
            changed = True
            while changed:
                changed = False
                for node_name in order:
                    if states[node_name] != "waiting":
                        continue
                    dependency_states = [
                        states[dependency] for dependency in self._nodes[node_name].deps
                    ]
                    if any(
                        state in {"pending", "retry_pending", "skipped"}
                        for state in dependency_states
                    ):
                        # 上游挂起或被跳过时下游不执行,但必须留下可见记录,不许静默消失。
                        states[node_name] = "skipped"
                        with state_lock:
                            skipped.append(node_name)
                        changed = True
                    elif all(state == "success" for state in dependency_states):
                        ready.append(node_name)
            return ready

        in_flight: dict[Future[tuple[str, str | None]], str] = {}
        ready = classify_ready()
        with ThreadPoolExecutor(max_workers=execution_workers) as execution_executor:
            while ready or in_flight:
                if not failures:
                    # Dynamic nodes run on the scheduler thread and submit their
                    # items to this same executor. That leaves every executor
                    # worker available to service map items, avoiding nested
                    # pools and the worker-squared thread explosion.
                    if not (budget_limited and in_flight):
                        index = 0
                        submission_limit = budget_window if budget_limited else execution_workers
                        while index < len(ready) and len(in_flight) < submission_limit:
                            node_name = ready[index]
                            if self._nodes[node_name].items_from is not None:
                                index += 1
                                continue
                            ready.pop(index)
                            states[node_name] = "running"
                            in_flight[execution_executor.submit(execute, node_name)] = node_name

                    dynamic_index = (
                        next(
                            (
                                index
                                for index, node_name in enumerate(ready)
                                if self._nodes[node_name].items_from is not None
                            ),
                            None,
                        )
                        if not (budget_limited and in_flight)
                        else None
                    )
                    if dynamic_index is not None:
                        node_name = ready.pop(dynamic_index)
                        states[node_name] = "running"
                        try:
                            _, outcome = execute(node_name)
                        except BaseException as error:
                            if isinstance(error, BudgetExceeded):
                                budget_abort.set()
                            states[node_name] = "failed"
                            failures[node_name] = error
                        else:
                            assert outcome is not None
                            states[node_name] = outcome
                        if not failures:
                            ready.extend(name for name in classify_ready() if name not in ready)
                        continue

                if not in_flight:
                    break
                done, _ = (
                    wait(in_flight)
                    if budget_limited
                    else wait(in_flight, return_when="FIRST_COMPLETED")
                )
                for future in done:
                    node_name = in_flight.pop(future)
                    try:
                        _, outcome = future.result()
                    except BaseException as error:
                        if isinstance(error, BudgetExceeded):
                            budget_abort.set()
                        states[node_name] = "failed"
                        failures[node_name] = error
                    else:
                        assert outcome is not None
                        states[node_name] = outcome
                if not failures:
                    ready.extend(name for name in classify_ready() if name not in ready)

        if failures:
            first_failure = min(failures, key=order.index)
            error = failures[first_failure]
            for node_name in (name for name in order if name in failures and name != first_failure):
                additional = failures[node_name]
                error.add_note(
                    "additional concurrent failure: "
                    f"{node_name}: {type(additional).__name__}: {additional}"
                )
            if isinstance(error, Exception):
                pending_retries = attempt_store.pending_retries()
                ambiguous_attempts = attempt_store.ambiguous_attempts()
                status = "ambiguous" if ambiguous_attempts else "failed"
                attempt_store.update_manifest(
                    status,
                    pending_retries=pending_retries,
                    ambiguous_attempts=ambiguous_attempts,
                    failure=canonical_failure(error) if status == "failed" else None,
                )
            raise error

        ordered_cache_hits = [name for name in order if name in cache_hits]
        ordered_pending = [
            pending_name
            for node_name in order
            for pending_node, pending_name in pending_checkpoints
            if pending_node == node_name
        ]
        ordered_skipped = [name for name in order if name in skipped]
        pending_retry_records = attempt_store.pending_retries()
        ambiguous_records = attempt_store.ambiguous_attempts()
        if ambiguous_records:
            run_status = "ambiguous"
        elif pending_retry_records:
            run_status = "pending_retry"
        elif ordered_pending:
            run_status = "checkpoint_pending"
        else:
            run_status = "completed"
        attempt_store.update_manifest(
            run_status,
            pending_retries=pending_retry_records,
            ambiguous_attempts=ambiguous_records,
        )
        return RunResult(
            artifacts,
            ordered_cache_hits,
            ordered_pending,
            current_run_id,
            ordered_skipped,
            {name: map_items[name] for name in order if name in map_items},
            [str(record["target"]) for record in pending_retry_records],
            [str(record["target"]) for record in ambiguous_records],
            run_status,
        )

    def resume(
        self,
        run_id: str,
        *,
        workers: int = 1,
        resource_limits: Mapping[str | None, int] | None = None,
    ) -> RunResult:
        """Resume one schema-2 run under its originally bound declaration."""
        run_dir = store.run_directory(self.config.artifacts_path, run_id)
        try:
            manifest = json.loads((run_dir / "_run.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError) as error:
            raise RunManifestError(
                f"Run {run_id!r} has no valid schema-2 run manifest and cannot be resumed"
            ) from error
        if (
            not isinstance(manifest, dict)
            or manifest.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA
        ):
            raise RunManifestError(
                f"Run {run_id!r} has no valid schema-2 manifest; it cannot be resumed"
            )
        self._validate_execution_manifest_profile(run_id, manifest)
        targets = manifest.get("targets")
        force = manifest.get("force")
        if not isinstance(targets, list) or not all(isinstance(name, str) for name in targets):
            raise RunManifestError(f"Run {run_id!r} has invalid target bindings")
        if not isinstance(force, list) or not all(isinstance(name, str) for name in force):
            raise RunManifestError(f"Run {run_id!r} has invalid force bindings")
        return self.run(
            targets=targets,
            run_id=run_id,
            force=force,
            workers=workers,
            resource_limits=resource_limits,
        )

    def recover(
        self,
        run_id: str,
        target: str,
        from_attempt: int,
        decision: Literal["retry_not_started", "retry_after_external_check", "fail"],
        reason: str,
        evidence: list[str] = [],  # noqa: B006 - copied before persistence
    ) -> RecoveryReceipt:
        """Recover a terminal failure with an explicit, append-only decision.

        A retry decision queues exactly one new attempt for ``target``.  Existing
        completed run artifacts remain bound to the run and are revalidated and
        inherited by :meth:`resume`; the failed attempt receipt is never rewritten.
        ``decision="fail"`` records the operator's final verdict without queuing work.
        """
        valid_decisions = {
            "retry_not_started",
            "retry_after_external_check",
            "fail",
        }
        if not isinstance(decision, str) or decision not in valid_decisions:
            raise ValueError(f"Unknown recovery decision: {decision!r}")
        if isinstance(from_attempt, bool) or not isinstance(from_attempt, int) or from_attempt < 1:
            raise ValueError("from_attempt must be a positive integer")
        if not isinstance(target, str) or not target.strip():
            raise ValueError("Recovery target must be a non-empty node name")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("Recovery reason must be non-empty")
        if not isinstance(evidence, list) or not all(
            isinstance(reference, str) for reference in evidence
        ):
            raise ValueError("Recovery evidence must be a list of strings")

        run_dir = store.run_directory(self.config.artifacts_path, run_id)
        manifest_path = run_dir / "_run.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError) as error:
            raise ValueError(f"Run {run_id!r} was not found or has no valid manifest") from error
        if not isinstance(manifest, dict):
            raise ValueError(f"Run {run_id!r} has an invalid manifest")
        if manifest.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA:
            raise ValueError(f"Run {run_id!r} has no valid schema-2 manifest")
        self._validate_execution_manifest_profile(run_id, manifest)
        dynamic_files_ledger = self._validate_dynamic_files_ledger(run_id, manifest)
        if manifest.get("status") != "failed":
            raise ValueError(f"Run {run_id!r} is not in terminal failed state")

        targets = manifest.get("targets")
        force = manifest.get("force")
        if not isinstance(targets, list) or not all(isinstance(name, str) for name in targets):
            raise ValueError(f"Run {run_id!r} has invalid target bindings")
        if not isinstance(force, list) or not all(isinstance(name, str) for name in force):
            raise ValueError(f"Run {run_id!r} has invalid force bindings")
        target_root = target.split("@", 1)[0]
        if target_root not in self._nodes:
            raise ValueError(f"Recovery target {target!r} is not registered in this graph")

        # Validate the immutable run identity before changing any mutable state.  A
        # recovery must use the same graph declaration as resume; otherwise it could
        # create a new attempt that the next resume correctly refuses to execute.
        try:
            order = self._topological_order(tuple(targets))
            libs_hashes = self._libs_hashes(self._nodes[name] for name in order)
            prompt_snapshot = self._prompt_snapshot()
            file_snapshot = _FileSnapshot.capture(
                self.config.project_root,
                self.config.resolve,
                (path for name in order for path in self._nodes[name].files),
            )
            attempts = AttemptStore(
                run_dir,
                self._run_manifest_identity(
                    run_id,
                    tuple(targets),
                    tuple(force),
                    order,
                    libs_hashes,
                    prompt_snapshot,
                    file_snapshot,
                    dynamic_files_ledger,
                ),
            )
            attempts.initialize()
        except (OSError, RunManifestError, ValueError) as error:
            raise ValueError(f"Run {run_id!r} declaration cannot be recovered: {error}") from error

        recovery_time = _recovery_time()
        recovered_by = _recovered_by()
        evidence_refs = list(evidence)
        normalized_reason = reason.strip()
        to_attempt = from_attempt if decision == "fail" else from_attempt + 1
        receipt = RecoveryReceipt(
            recovery_time=recovery_time,
            from_attempt=from_attempt,
            to_attempt=to_attempt,
            decision=decision,
            reason=normalized_reason,
            evidence_refs=evidence_refs,
            recovered_by=recovered_by,
        )
        receipt_payload = _recovery_payload(receipt)
        inherited_nodes = None
        if decision != "fail":
            inherited_nodes = self._recovery_inherited_nodes(
                run_dir,
                target_root,
                order,
                attempts=attempts,
            )

        # The runstate API owns the failed-state check, recovery decision
        # exclusion, receipt creation, target transition, and lease fencing.
        # Do not preflight with state_for() or compose the decision from the
        # separate receipt/scheduling APIs: those combinations can race with a
        # different operator choosing the opposite decision.
        try:
            attempts.record_recovery_decision(
                target,
                from_attempt=from_attempt,
                decision=decision,
                recovery=receipt_payload,
                inherited_nodes=inherited_nodes,
                recovery_receipt=receipt_payload,
            )
        except ValueError as error:
            # Keep the recover/CLI diagnostic that callers received before the
            # atomic API existed, while leaving the decision validation itself
            # owned by AttemptStore.
            if "is not the active terminal failure" in str(error):
                raise ValueError(
                    f"Recovery target {target!r} is at an unavailable attempt, not {from_attempt}"
                ) from error
            raise
        if decision != "fail":
            attempts.update_manifest(
                "pending_retry",
                pending_retries=attempts.pending_retries(),
                ambiguous_attempts=attempts.ambiguous_attempts(),
            )
        return receipt

    def _recovery_inherited_nodes(
        self,
        run_dir: Path,
        target: str,
        order: list[str],
        *,
        attempts: AttemptStore,
    ) -> dict[str, Any]:
        """Describe successful run-local artifacts that the retry will inherit."""
        downstream = _downstream_nodes(self._nodes, {target})
        inherited: dict[str, Any] = {}
        for name in order:
            if name == target or name in downstream:
                continue
            artifact_path = run_dir / f"{name}.json"
            sidecar_path = Path(f"{artifact_path}.meta.json")
            if not artifact_path.is_file() or not sidecar_path.is_file():
                continue
            try:
                artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
                metadata = json.loads(sidecar_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise ValueError(
                    f"Inherited node {name!r} has invalid persisted artifacts"
                ) from error
            if not isinstance(artifact, dict) or not isinstance(metadata, dict):
                raise ValueError(f"Inherited node {name!r} has invalid persisted artifacts")
            artifact_digest = sha(artifact)
            origin = metadata.get("origin_provenance")
            if (
                metadata.get("run_sidecar_schema") != RUN_SIDECAR_SCHEMA
                or metadata.get("artifact_sha256") != artifact_digest
                or not isinstance(origin, dict)
                or origin.get("artifact_sha256") != artifact_digest
                or metadata.get("origin_provenance_digest") != sha(origin)
            ):
                raise ValueError(f"Inherited node {name!r} failed artifact validation")
            state = attempts.state_for(name)
            if state is not None and state.get("status") != "completed":
                continue
            inherited[name] = {
                "status": "inherited",
                "source": f"{name}.json",
                "source_attempt": (
                    int(state["attempt"])
                    if state is not None and isinstance(state.get("attempt"), int)
                    else None
                ),
                "artifact_sha256": artifact_digest,
            }
        return inherited

    def retry_resolve(
        self,
        run_id: str,
        target: str,
        *,
        attempt: int,
        action: str,
        reason: str,
    ) -> None:
        """Persist an explicit operator verdict for one ambiguous attempt."""
        if action not in {"retry", "fail"}:
            raise ValueError("retry resolution action must be 'retry' or 'fail'")
        run_dir = store.run_directory(self.config.artifacts_path, run_id)
        manifest_path = run_dir / "_run.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError) as error:
            raise RunManifestError(f"Run {run_id!r} has no valid manifest") from error
        if (
            not isinstance(manifest, dict)
            or manifest.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA
        ):
            raise RunManifestError(f"Run {run_id!r} has no valid manifest")
        attempts = AttemptStore(run_dir, {})
        attempts.resolve(
            target,
            attempt=attempt,
            action=action,  # type: ignore[arg-type]
            reason=reason,
        )
        pending = attempts.pending_retries()
        ambiguous = attempts.ambiguous_attempts()
        status = "failed" if action == "fail" else "pending_retry"
        attempts.update_manifest(
            status,
            pending_retries=pending,
            ambiguous_attempts=ambiguous,
        )

    @staticmethod
    def _validate_execution_manifest_profile(
        run_id: str,
        manifest: Mapping[str, Any],
    ) -> None:
        """Validate the persisted profile before any resume/run state mutation."""
        profile = manifest.get("workflow_profile")
        if not isinstance(profile, dict):
            raise RunManifestError(f"Run {run_id!r} is missing workflow_profile")
        if profile.get("workflow_profile_schema") != workflow_profile.WORKFLOW_PROFILE_SCHEMA:
            raise RunManifestError(f"Run {run_id!r} has an unsupported workflow_profile schema")
        if manifest.get("workflow_profile_digest") != sha(profile):
            raise RunManifestError(f"Run {run_id!r} workflow_profile digest validation failed")

    @staticmethod
    def _validate_dynamic_files_ledger(
        run_id: str,
        manifest: Mapping[str, Any],
    ) -> dict[str, dict[str, list[dict[str, str]]]]:
        """Validate the immutable path/digest ledger owned by a run manifest."""
        ledger = manifest.get(_DYNAMIC_FILES_LEDGER_FIELD)
        digest = manifest.get(_DYNAMIC_FILES_LEDGER_DIGEST_FIELD)
        if not isinstance(ledger, dict) or digest != sha(ledger):
            raise RunManifestError(f"Run {run_id!r} has an invalid dynamic files snapshot ledger")
        for node_name, items in ledger.items():
            if not isinstance(node_name, str) or not isinstance(items, dict):
                raise RunManifestError(
                    f"Run {run_id!r} has an invalid dynamic files snapshot ledger"
                )
            for item_id, entries in items.items():
                if not isinstance(item_id, str) or not isinstance(entries, list):
                    raise RunManifestError(
                        f"Run {run_id!r} has an invalid dynamic files snapshot ledger"
                    )
                for entry in entries:
                    if (
                        not isinstance(entry, dict)
                        or set(entry) != {"path", "sha256"}
                        or not isinstance(entry.get("path"), str)
                        or not entry["path"]
                        or not isinstance(entry.get("sha256"), str)
                        or len(entry["sha256"]) != 64
                        or entry["sha256"] != entry["sha256"].lower()
                        or any(character not in "0123456789abcdef" for character in entry["sha256"])
                    ):
                        raise RunManifestError(
                            f"Run {run_id!r} has an invalid dynamic files snapshot ledger"
                        )
        return copy.deepcopy(ledger)

    @staticmethod
    def _dynamic_files_observation(
        node: _Node,
        item_files_by_id: Mapping[str, tuple[Path, ...]],
        file_snapshot: _FileSnapshot,
    ) -> dict[str, dict[str, list[dict[str, str]]]]:
        return {
            node.name: {
                item_id: [
                    {"path": str(path), "sha256": _bytes_hash(file_snapshot.read(path))}
                    for path in item_files
                ]
                for item_id, item_files in item_files_by_id.items()
            }
        }

    def _bind_dynamic_files_ledger(
        self,
        attempt_store: AttemptStore,
        run_id: str,
        node: _Node,
        observed: dict[str, dict[str, list[dict[str, str]]]],
        *,
        allow_new: bool,
    ) -> None:
        """Bind one dynamic node's complete files_fn result before any item runs."""
        with attempt_store._run_locked():  # noqa: SLF001 - manifest binding is DAG-owned
            manifest = attempt_store._required_manifest()  # noqa: SLF001
            ledger = self._validate_dynamic_files_ledger(run_id, manifest)
            current = observed[node.name]
            if node.name in ledger:
                if ledger[node.name] != current:
                    raise RunManifestError(
                        f"Run {run_id!r} dynamic files snapshot changed for node {node.name!r}"
                    )
                return
            if not allow_new and (
                manifest.get("status") == "completed"
                or self._dynamic_node_has_durable_evidence(attempt_store, node.name)
            ):
                raise RunManifestError(
                    f"Run {run_id!r} dynamic files snapshot ledger has no node {node.name!r}"
                )
            updated = copy.deepcopy(ledger)
            updated[node.name] = copy.deepcopy(current)
            updated_digest = sha(updated)
            manifest[_DYNAMIC_FILES_LEDGER_FIELD] = updated
            manifest[_DYNAMIC_FILES_LEDGER_DIGEST_FIELD] = updated_digest
            attempt_store.identity[_DYNAMIC_FILES_LEDGER_FIELD] = copy.deepcopy(updated)
            attempt_store.identity[_DYNAMIC_FILES_LEDGER_DIGEST_FIELD] = updated_digest
            attempt_store._commit_manifest(manifest)  # noqa: SLF001

    @staticmethod
    def _dynamic_node_has_durable_evidence(
        attempt_store: AttemptStore,
        node_name: str,
    ) -> bool:
        """Detect a previously started dynamic node before permitting first bind."""
        states = attempt_store._validate_all_attempt_receipts()  # noqa: SLF001
        if any(
            target == node_name or target.startswith(f"{node_name}@")
            for target in (state.get("target") for state in states)
        ):
            return True
        run_root = attempt_store.run_root
        if (run_root / f"{node_name}.json").exists() or (
            run_root / f"{node_name}.json.meta.json"
        ).exists():
            return True
        return any(run_root.glob(f"{node_name}@*.json"))

    def _run_manifest_identity(
        self,
        run_id: str,
        targets: tuple[str, ...],
        force: tuple[str, ...],
        order: list[str],
        libs_hashes: Mapping[str, str],
        prompt_snapshot: PromptCatalogSnapshot,
        file_snapshot: _FileSnapshot,
        dynamic_files_ledger: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        declarations: dict[str, Any] = {}
        retry_digests: dict[str, str | None] = {}
        evidence_digests: dict[str, str] = {}
        for name in order:
            node = self._nodes[name]
            evidence = node.evidence_policy or self._caller_evidence_policy()
            retry_digests[name] = node.retry.digest if node.retry is not None else None
            evidence_digests[name] = evidence.digest
            declarations[name] = {
                "source": _source_hash(node.function),
                "deps": list(node.deps),
                "prompt_specs": {
                    spec.name: prompt_snapshot.declaration(spec) for spec in node.prompt_specs
                },
                "files": {
                    path.as_posix(): _bytes_hash(file_snapshot.contents((path,))[str(path)])
                    for path in node.files
                },
                "params": sha(node.params),
                "consumes": sorted(node.consumes),
                "cache": node.cache,
                "external": node.external_fingerprint_digest,
                "executor": node.executor,
                "agent": copy.deepcopy(node.agent_identity),
                "items_from": list(node.items_from) if node.items_from is not None else None,
                "scan": node.scan,
                "carry_from": list(node.carry_from) if node.carry_from is not None else None,
                "dynamic_callables": {
                    "key_fn": _callable_provenance(node.key_fn),
                    "files_fn": _callable_provenance(node.files_fn),
                    "aggregate_fn": _callable_provenance(node.aggregate_fn),
                    "carry_fn": _callable_provenance(node.carry_fn),
                },
                "retry_policy_digest": retry_digests[name],
                "evidence_policy_digest": evidence.digest,
            }
        source_digest = sha(declarations)
        static_profile = self._static_workflow_profile(prompt_snapshot)
        libs_digest = sha(libs_hashes)
        dynamic_ledger = copy.deepcopy(dynamic_files_ledger or {})
        return {
            "run_id": run_id,
            "graph_identity": sha(
                {
                    "declarations": declarations,
                    "targets": list(targets),
                    "force": list(force),
                    "libs": libs_digest,
                }
            ),
            "targets": list(targets),
            "force": list(force),
            "source_digest": source_digest,
            "libs_digest": libs_digest,
            "retry_policy_digests": retry_digests,
            "evidence_policy_digests": evidence_digests,
            _DYNAMIC_FILES_LEDGER_FIELD: dynamic_ledger,
            _DYNAMIC_FILES_LEDGER_DIGEST_FIELD: sha(dynamic_ledger),
            "workflow_profile": static_profile,
            "workflow_profile_digest": sha(static_profile),
        }

    @staticmethod
    def _attempt_declaration_digest(
        node: _Node,
        key_components: dict[str, str],
        evidence_policy: EvidencePolicy,
    ) -> str:
        return sha(
            {
                "target": node.name,
                "key_components": key_components,
                "retry_policy_digest": (node.retry.digest if node.retry is not None else None),
                "evidence_policy_digest": evidence_policy.digest,
            }
        )

    def _resume_completed_artifact(
        self,
        run_dir: Path,
        label: str,
        key_components: dict[str, str],
        cache_key: str,
        *,
        state: Mapping[str, Any] | None = None,
        validate_agent: bool = False,
        prompt_resolutions: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        artifact_path = run_dir / f"{label}.json"
        sidecar_path = Path(f"{artifact_path}.meta.json")
        try:
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
            metadata = json.loads(sidecar_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError) as error:
            raise RunManifestError(
                f"Completed target {label!r} has missing or invalid run artifacts"
            ) from error
        if not isinstance(artifact, dict) or not isinstance(metadata, dict):
            raise RunManifestError(f"Completed target {label!r} has invalid run artifacts")
        artifact_digest = sha(artifact)
        if state is None or state.get("status") != "completed":
            raise RunManifestError(
                f"Completed target {label!r} has no durable state/candidate ownership"
            )
        if state.get("artifact_sha256") != artifact_digest:
            raise RunManifestError(
                f"Completed target {label!r} state does not own the run artifact"
            )
        candidate_file = state.get("candidate_file")
        if (
            not isinstance(candidate_file, str)
            or Path(candidate_file).is_absolute()
            or Path(candidate_file).parent != Path(".")
            or candidate_file != f"candidate-{state.get('attempt', 0):04d}.json"
        ):
            raise RunManifestError(
                f"Completed target {label!r} has no valid success candidate ownership"
            )
        candidate_path = run_dir / "attempts" / sha(label) / candidate_file
        try:
            candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError) as error:
            raise RunManifestError(
                f"Completed target {label!r} has a missing or invalid success candidate"
            ) from error
        if (
            not isinstance(candidate, dict)
            or candidate.get("candidate_schema") != SUCCESS_CANDIDATE_SCHEMA
            or state.get("candidate_sha256") != sha(candidate)
            or not isinstance(candidate.get("artifact"), dict)
            or sha(candidate["artifact"]) != artifact_digest
            or candidate.get("cache_key") != cache_key
            or candidate.get("key_components") != key_components
            or candidate.get("prompt_resolutions") != dict(prompt_resolutions or {})
        ):
            raise RunManifestError(
                f"Completed target {label!r} success candidate ownership validation failed"
            )
        origin = metadata.get("origin_provenance")
        if (
            metadata.get("run_sidecar_schema") != RUN_SIDECAR_SCHEMA
            or metadata.get("artifact_sha256") != artifact_digest
            or not isinstance(origin, dict)
            or origin.get("artifact_sha256") != artifact_digest
            or metadata.get("origin_provenance_digest") != sha(origin)
            or metadata.get("prompt_resolutions_digest")
            != sha(metadata.get("prompt_resolutions", {}))
            or metadata.get("key_components") != key_components
            or metadata.get("cache_key") != cache_key
            or (
                prompt_resolutions is not None
                and metadata.get("prompt_resolutions") != dict(prompt_resolutions)
            )
        ):
            raise RunManifestError(
                f"Completed target {label!r} failed artifact or declaration validation"
            )
        if validate_agent:
            validate_agent_artifact(artifact, self.blob_store)
        _validate_persisted_prompt_lineage(metadata, label)
        self._validate_materialized_outputs(label, artifact, metadata)
        return artifact, metadata

    def _validate_materialized_outputs(
        self,
        label: str,
        artifact: dict[str, Any],
        metadata: Mapping[str, Any],
    ) -> None:
        """Check completed output bytes before resume is allowed to rematerialize them."""
        expected: dict[str, bytes] = {}
        files = artifact.get("files")
        if files is not None:
            if not isinstance(files, dict):
                raise RunManifestError(f"Completed target {label!r} has invalid materialized files")
            for relative_name, contents in files.items():
                if not isinstance(relative_name, str) or not isinstance(contents, str):
                    raise RunManifestError(
                        f"Completed target {label!r} has invalid materialized files"
                    )
                try:
                    relative_path = store.project_relative_path(relative_name)
                except (TypeError, ValueError) as error:
                    raise RunManifestError(
                        f"Completed target {label!r} has an invalid materialized output path"
                    ) from error
                output_name = relative_path.as_posix()
                if output_name in expected:
                    raise RunManifestError(
                        f"Completed target {label!r} has duplicate materialized output "
                        f"{output_name!r}"
                    )
                expected[output_name] = contents.encode("utf-8")

        def blob_references(value: Any) -> Iterable[dict[str, Any]]:
            if isinstance(value, dict):
                if "kigumi_blob" in value:
                    yield value
                for child in value.values():
                    yield from blob_references(child)
            elif isinstance(value, list):
                for child in value:
                    yield from blob_references(child)

        for reference in blob_references(artifact):
            digest = reference.get("kigumi_blob")
            relative_name = reference.get("path")
            if not isinstance(digest, str) or not isinstance(relative_name, str):
                raise RunManifestError(
                    f"Completed target {label!r} has an invalid blob materialization"
                )
            try:
                relative_path = store.project_relative_path(relative_name)
                expected_bytes = self.blob_store.read_verified(digest)
            except (FileNotFoundError, OSError, TypeError, ValueError) as error:
                raise RunManifestError(
                    f"Completed target {label!r} has an invalid blob materialization"
                ) from error
            output_name = relative_path.as_posix()
            if output_name in expected:
                raise RunManifestError(
                    f"Completed target {label!r} has duplicate materialized output {output_name!r}"
                )
            expected[output_name] = expected_bytes

        recorded = metadata.get("outputs")
        if (
            not isinstance(recorded, list)
            or not all(isinstance(value, str) for value in recorded)
            or len(recorded) != len(set(recorded))
            or sorted(recorded) != sorted(expected)
        ):
            raise RunManifestError(
                f"Completed target {label!r} materialized output ledger is invalid"
            )

        project_root = self.config.project_root.resolve()
        for output_name, expected_bytes in expected.items():
            relative_path = Path(output_name)
            destination = project_root / relative_path
            try:
                resolved = self.config.resolve(relative_path)
                resolved.relative_to(project_root)
                actual = _read_snapshot_file(destination)
            except (OSError, ValueError) as error:
                raise RunManifestError(
                    f"Completed target {label!r} materialized output {output_name!r} "
                    "cannot be safely read"
                ) from error
            if actual != expected_bytes:
                raise RunManifestError(
                    f"Completed target {label!r} materialized output {output_name!r} "
                    f"digest mismatch (expected {_bytes_hash(expected_bytes)}, "
                    f"got {_bytes_hash(actual)})"
                )

    def _cache_entry_for_lookup(
        self,
        cache_key: str,
        *,
        forced: bool,
        cache_policy: CachePolicy,
        evidence_policy: EvidencePolicy,
    ) -> store.CacheEntry | None:
        """Return one accepted cache snapshot for plan and execution."""
        if forced or cache_policy != "auto":
            return None
        entry = store.read_cache_entry(self.config.artifacts_path, cache_key)
        if entry.state == "CORRUPT":
            raise CacheIntegrityError(
                store.node_cache_path(self.config.artifacts_path, cache_key),
                entry.lookup,
            )
        if (
            entry.state != "VALID"
            or entry.artifact is None
            or entry.origin is None
            or entry.origin.get("evidence_policy_digest") != evidence_policy.digest
        ):
            return None
        return entry

    @staticmethod
    def _record_cache_lookup_failure(
        attempt_store: AttemptStore,
        target: str,
        error: CacheIntegrityError,
        *,
        policy: RetryPolicy | None,
        declaration_digest: str,
        prompt_resolutions: Mapping[str, Any],
        calls: list[dict[str, Any]] | None = None,
    ) -> None:
        """Bind a cache read failure to durable target state before re-raising it.

        A corrupt cache can be discovered before normal attempt admission.  The
        run still needs a durable failed target when other cache hits have
        already completed; otherwise strict manifest publication quite rightly
        rejects a terminal failure with only completed states.
        """
        prepared = attempt_store.prepare(
            target,
            policy=policy,
            declaration_digest=declaration_digest,
            prompt_resolutions=dict(prompt_resolutions),
        )
        if prepared["action"] != "run":
            raise RunManifestError(f"Cache lookup for {target!r} has an unexpected durable state")
        attempt_store.record_failure(
            target,
            error,
            policy=policy,
            calls=calls,
        )

    def _lookup_cache(
        self,
        cache_key: str,
        *,
        forced: bool,
        cache_policy: CachePolicy,
        evidence_policy: EvidencePolicy,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Preserve the historical artifact/hit API over one cache snapshot."""
        entry = self._cache_entry_for_lookup(
            cache_key,
            forced=forced,
            cache_policy=cache_policy,
            evidence_policy=evidence_policy,
        )
        return (entry.artifact, True) if entry is not None else (None, False)

    def plan(
        self,
        run_id: str | None = None,
        targets: Iterable[str] | None = None,
        force: Iterable[str] = (),
        *,
        _prompt_snapshot: PromptCatalogSnapshot | None = None,
    ) -> PlanResult:
        """Forecast cache work without calling node functions or allocating a run directory.

        ``run_id`` is accepted for call-site symmetry with :meth:`run`; it is
        intentionally not read because L3 cache keys are content-addressed,
        not run-addressed.  ``force`` applies the same node/item notation as
        ``run`` and reports those entries as misses.
        """
        del run_id
        selected = tuple(self._nodes) if targets is None else tuple(targets)
        order = self._topological_order(selected)
        forced_nodes, forced_items = self._parse_forced(force)
        libs_identities = self._libs_identities(self._nodes[name] for name in order)
        libs_hashes = {name: identity.digest for name, identity in libs_identities.items()}
        prompt_snapshot = _prompt_snapshot or self._prompt_snapshot(
            self._nodes[name] for name in order
        )
        nodes: dict[str, str] = {}
        pending_on: dict[str, tuple[str, ...]] = {}
        artifact_shas: dict[str, str] = {}
        artifacts: dict[str, dict[str, Any]] = {}

        for node_name in order:
            node = self._nodes[node_name]
            libs_cache_reusable = libs_identities[node.name].cache_reusable
            evidence_policy = node.evidence_policy or self._caller_evidence_policy()
            if any(dependency not in artifact_shas for dependency in node.deps):
                waiting_on = tuple(
                    dependency
                    for dependency in node.deps
                    if nodes.get(dependency) in {"miss", "unknown"}
                )
                if node.items_from is not None and not node.scan:
                    source_name, _ = node.items_from
                    if source_name in artifacts:
                        entries = self._map_entries(node, {source_name: artifacts[source_name]})
                        for item_id, _ in entries:
                            expanded_name = f"{node_name}@{item_id}"
                            nodes[expanded_name] = "unknown"
                            pending_on[expanded_name] = waiting_on
                nodes[node_name] = "unknown"
                pending_on[node_name] = waiting_on
                continue
            inputs = {dependency: artifacts[dependency] for dependency in node.deps}
            upstream_shas = {dependency: artifact_shas[dependency] for dependency in node.deps}
            if node.items_from is None:
                cache_key = sha(
                    self._key_components(
                        node,
                        upstream_shas,
                        libs_hashes[node.name],
                        upstream_artifacts=inputs,
                        prompt_snapshot=prompt_snapshot,
                    )
                )
                artifact = None
                if node.cache == "auto" and libs_cache_reusable and node_name not in forced_nodes:
                    artifact, _ = self._lookup_cache(
                        cache_key,
                        forced=False,
                        cache_policy=node.cache,
                        evidence_policy=evidence_policy,
                    )
                if artifact is None:
                    nodes[node_name] = "miss"
                    continue
                nodes[node_name] = "hit"
                artifacts[node_name] = artifact
                artifact_shas[node_name] = sha(artifact)
                continue

            source_name, _ = node.items_from
            if source_name not in artifacts:
                nodes[node_name] = "unknown"
                pending_on[node_name] = tuple(
                    dependency
                    for dependency in node.deps
                    if nodes.get(dependency) in {"miss", "unknown"}
                )
                continue
            entries = self._map_entries(node, inputs)
            # plan 与 run 对打错的 force 项必须同样报错:预告静默忽略会让
            # 成本闸门看似通过,实跑才失败。
            unknown_forced = sorted(
                forced_items.get(node_name, set()) - {item_id for item_id, _ in entries}
            )
            if unknown_forced:
                forced_names = ", ".join(f"{node_name}@{item_id}" for item_id in unknown_forced)
                raise ValueError(f"Unknown forced map items: {forced_names}")
            if node.scan:
                carry = (
                    _resolve_items_from(
                        node.name,
                        node.carry_from[0],
                        node.carry_from[1],
                        inputs[node.carry_from[0]],
                    )
                    if node.carry_from is not None
                    else None
                )
                completed: dict[str, dict[str, Any]] = {}
                item_ids: list[str] = []
                previous_pending: str | None = None
                for item_id, item in entries:
                    expanded_name = f"{node_name}@{item_id}"
                    item_ids.append(item_id)
                    if previous_pending is not None:
                        nodes[expanded_name] = "unknown"
                        pending_on[expanded_name] = (previous_pending,)
                        previous_pending = expanded_name
                        continue
                    item_files = (
                        tuple(Path(path) for path in node.files_fn(item)) if node.files_fn else ()
                    )
                    cache_key = sha(
                        self._key_components(
                            node,
                            upstream_shas,
                            libs_hashes[node.name],
                            upstream_artifacts=inputs,
                            item=item,
                            item_files=item_files,
                            carry=carry,
                            prompt_snapshot=prompt_snapshot,
                        )
                    )
                    artifact = None
                    if (
                        node.cache == "auto"
                        and libs_cache_reusable
                        and node_name not in forced_nodes
                        and item_id not in forced_items.get(node_name, set())
                    ):
                        artifact, _ = self._lookup_cache(
                            cache_key,
                            forced=False,
                            cache_policy=node.cache,
                            evidence_policy=evidence_policy,
                        )
                    if artifact is None:
                        nodes[expanded_name] = "miss"
                        previous_pending = expanded_name
                        continue
                    nodes[expanded_name] = "hit"
                    completed[item_id] = artifact
                    # 与 _execute_scan 的错误包装保持一致:同一个 carry_fn 故障
                    # 在 plan 与 run 里必须给出相同形态的节点+项上下文。
                    try:
                        carry = node.carry_fn(artifact) if node.carry_fn is not None else artifact
                    except Exception as error:
                        raise RuntimeError(
                            f"Scan node {node_name!r} failed item {item_id!r}: "
                            f"{type(error).__name__}: {error}"
                        ) from error
                if previous_pending is None and entries:
                    aggregate = self._aggregate_map_artifact(node, completed, item_ids)
                    nodes[node_name] = "hit"
                    artifacts[node_name] = aggregate
                    artifact_shas[node_name] = sha(aggregate)
                else:
                    nodes[node_name] = "miss"
                continue
            completed: dict[str, dict[str, Any]] = {}
            item_statuses: list[str] = []
            item_ids: list[str] = []
            for item_id, item in entries:
                item_files = (
                    tuple(Path(path) for path in node.files_fn(item)) if node.files_fn else ()
                )
                key_components = self._key_components(
                    node,
                    upstream_shas,
                    libs_hashes[node.name],
                    upstream_artifacts=inputs,
                    item=item,
                    item_files=item_files,
                    prompt_snapshot=prompt_snapshot,
                )
                cache_key = sha(key_components)
                artifact = None
                if (
                    node.cache == "auto"
                    and libs_cache_reusable
                    and node_name not in forced_nodes
                    and item_id not in forced_items.get(node_name, set())
                ):
                    artifact, _ = self._lookup_cache(
                        cache_key,
                        forced=False,
                        cache_policy=node.cache,
                        evidence_policy=evidence_policy,
                    )
                status = "hit" if artifact is not None else "miss"
                nodes[f"{node_name}@{item_id}"] = status
                item_statuses.append(status)
                item_ids.append(item_id)
                if artifact is not None:
                    completed[item_id] = artifact
            if (
                node.cache == "auto"
                and libs_cache_reusable
                and node_name not in forced_nodes
                and item_statuses
                and all(status == "hit" for status in item_statuses)
            ):
                aggregate = self._aggregate_map_artifact(node, completed, item_ids)
                nodes[node_name] = "hit"
                artifacts[node_name] = aggregate
                artifact_shas[node_name] = sha(aggregate)
            else:
                nodes[node_name] = "miss"

        return PlanResult(nodes, pending_on)

    def explain(self, name: str, run_id: str | None = None) -> ExplainResult:
        """解释节点相对某次运行为何命中、失效或无法诚实判断。

        只读取缓存、声明输入和运行 sidecar，不调用节点函数或模型。对 map/scan
        项使用 ``"node@item_id"``；上游未命中时沿用 :meth:`plan` 的 ``unknown``
        语义，不把无法取得的内容变化臆测为某一项输入变化。
        """
        node_name, item_id = self._parse_explain_name(name)
        prompt_snapshot = self._prompt_snapshot()
        forecast = self.plan(_prompt_snapshot=prompt_snapshot)
        status = forecast.nodes.get(name)
        if status is None:
            if item_id is not None and forecast.nodes.get(node_name) == "unknown":
                status = "unknown"
            else:
                raise ValueError(f"Unknown node or map item: {name!r}")

        metadata = self._read_explain_sidecar(name, run_id)
        if metadata is None:
            return ExplainResult("no_entry", [], {})
        if item_id is None and self._nodes[node_name].items_from is not None:
            # Dynamic aggregates have an ordered list of item keys, not one
            # singular cache key/components object.  Their item sidecars carry
            # the actionable components; aggregate explain only reports the
            # forecast status and an empty diff.
            return ExplainResult(status, [], {})
        previous = metadata.get("key_components")
        if not isinstance(previous, dict) or not all(
            isinstance(label, str) and isinstance(digest, str) for label, digest in previous.items()
        ):
            raise ValueError(f"Run sidecar for {name!r} has invalid key_components")
        if status == "unknown":
            return ExplainResult(
                "unknown",
                [],
                {},
                forecast.pending_on.get(name, forecast.pending_on.get(node_name, ())),
            )

        current = self._current_key_components(
            node_name,
            item_id,
            prompt_snapshot=prompt_snapshot,
        )
        changed = sorted(
            label
            for label in set(previous) | set(current)
            if previous.get(label) != current.get(label)
        )
        details = {
            label: {
                "old": str(previous.get(label, "<缺失>")),
                "new": str(current.get(label, "<缺失>")),
            }
            for label in changed
        }
        return ExplainResult(status, changed, details)

    def _parse_explain_name(self, name: str) -> tuple[str, str | None]:
        """校验普通节点或 map/scan 单项寻址。"""
        if "@" not in name:
            if name not in self._nodes:
                raise ValueError(f"Unknown dependency or target node: {name!r}")
            return name, None
        node_name, item_id = name.split("@", 1)
        node = self._nodes.get(node_name)
        if node is None or node.items_from is None or not item_id:
            raise ValueError(f"Unknown node or map item: {name!r}")
        return node_name, item_id

    def _read_explain_sidecar(self, name: str, run_id: str | None) -> dict[str, Any] | None:
        """读取指定运行 sidecar；未指定时选择最新运行。"""
        root = store.runs_root(self.config.artifacts_path)
        if run_id is None:
            runs = (
                sorted((path for path in root.glob("*") if path.is_dir()), key=store.run_sort_key)
                if root.is_dir()
                else []
            )
            if not runs:
                return None
            run_dir = runs[-1]
        else:
            run_dir = store.run_directory(self.config.artifacts_path, run_id)
            if not run_dir.is_dir():
                raise ValueError(f"Run {run_id!r} does not exist")
        try:
            with (run_dir / f"{name}.json.meta.json").open(encoding="utf-8") as handle:
                metadata = json.load(handle)
        except FileNotFoundError:
            return None
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"Invalid sidecar for {name!r} in run {run_dir.name!r}") from error
        if not isinstance(metadata, dict):
            raise ValueError(f"Invalid sidecar for {name!r} in run {run_dir.name!r}")
        return metadata

    def _current_key_components(
        self,
        node_name: str,
        item_id: str | None,
        *,
        prompt_snapshot: PromptCatalogSnapshot | None = None,
    ) -> dict[str, str]:
        """重建一个 explain 目标的当前键成分，不执行节点函数。"""
        memo: dict[str, dict[str, Any]] = {}
        components: dict[str, dict[str, str]] = {}
        libs_hashes = self._libs_hashes(self._nodes.values())
        prompt_snapshot = prompt_snapshot or self._prompt_snapshot()

        def artifact_for(name: str) -> dict[str, Any]:
            if name in memo:
                return memo[name]
            node = self._nodes[name]
            inputs = {dependency: artifact_for(dependency) for dependency in node.deps}
            upstream_shas = {dependency: sha(artifact) for dependency, artifact in inputs.items()}
            if node.items_from is None:
                component = self._key_components(
                    node,
                    upstream_shas,
                    libs_hashes[node.name],
                    upstream_artifacts=inputs,
                    prompt_snapshot=prompt_snapshot,
                )
                artifact, _ = self._lookup_cache(
                    sha(component),
                    forced=False,
                    cache_policy="auto",
                    evidence_policy=node.evidence_policy or self._caller_evidence_policy(),
                )
                if artifact is None:
                    raise RuntimeError(f"Cannot read current cached artifact for node {name!r}")
                components[name] = component
                memo[name] = artifact
                return artifact

            assert node.items_from is not None
            entries = self._map_entries(node, inputs)
            carry = (
                _resolve_items_from(
                    node.name,
                    node.carry_from[0],
                    node.carry_from[1],
                    inputs[node.carry_from[0]],
                )
                if node.scan and node.carry_from is not None
                else None
                if node.scan
                else _NO_CARRY
            )
            completed: dict[str, dict[str, Any]] = {}
            for current_item_id, item in entries:
                item_files = (
                    tuple(Path(path) for path in node.files_fn(item)) if node.files_fn else ()
                )
                component = self._key_components(
                    node,
                    upstream_shas,
                    libs_hashes[node.name],
                    upstream_artifacts=inputs,
                    item=item,
                    item_files=item_files,
                    carry=carry,
                    prompt_snapshot=prompt_snapshot,
                )
                artifact, _ = self._lookup_cache(
                    sha(component),
                    forced=False,
                    cache_policy="auto",
                    evidence_policy=node.evidence_policy or self._caller_evidence_policy(),
                )
                if artifact is None:
                    raise RuntimeError(
                        "Cannot read current cached artifact for map item "
                        f"{name}@{current_item_id!r}"
                    )
                components[f"{name}@{current_item_id}"] = component
                completed[current_item_id] = artifact
                if node.scan:
                    carry = node.carry_fn(artifact) if node.carry_fn is not None else artifact
            memo[name] = self._aggregate_map_artifact(
                node, completed, [current_item_id for current_item_id, _ in entries]
            )
            return memo[name]

        target_node = self._nodes[node_name]
        target_inputs = {dependency: artifact_for(dependency) for dependency in target_node.deps}
        target_upstream_shas = {
            dependency: sha(artifact) for dependency, artifact in target_inputs.items()
        }
        if item_id is None:
            if target_node.items_from is not None:
                raise RuntimeError(f"Map node {node_name!r} has no singular cache key")
            return self._key_components(
                target_node,
                target_upstream_shas,
                libs_hashes[target_node.name],
                upstream_artifacts=target_inputs,
                prompt_snapshot=prompt_snapshot,
            )

        assert target_node.items_from is not None
        entries = self._map_entries(target_node, target_inputs)
        carry = (
            _resolve_items_from(
                target_node.name,
                target_node.carry_from[0],
                target_node.carry_from[1],
                target_inputs[target_node.carry_from[0]],
            )
            if target_node.scan and target_node.carry_from is not None
            else None
            if target_node.scan
            else _NO_CARRY
        )
        for current_item_id, item in entries:
            item_files = (
                tuple(Path(path) for path in target_node.files_fn(item))
                if target_node.files_fn
                else ()
            )
            component = self._key_components(
                target_node,
                target_upstream_shas,
                libs_hashes[target_node.name],
                upstream_artifacts=target_inputs,
                item=item,
                item_files=item_files,
                carry=carry,
                prompt_snapshot=prompt_snapshot,
            )
            if current_item_id == item_id:
                return component
            if target_node.scan:
                artifact, _ = self._lookup_cache(
                    sha(component),
                    forced=False,
                    cache_policy="auto",
                    evidence_policy=target_node.evidence_policy or self._caller_evidence_policy(),
                )
                if artifact is None:
                    raise RuntimeError(
                        "Cannot read current cached artifact for earlier scan item "
                        f"{node_name}@{current_item_id!r}"
                    )
                carry = (
                    target_node.carry_fn(artifact) if target_node.carry_fn is not None else artifact
                )
        raise ValueError(f"Unknown node or map item: {node_name}@{item_id}")

    def describe(self) -> dict[str, Any]:
        """返回 DAG 的声明性结构摘要，不读缓存、不执行节点。

        ``validated_models`` 与 ``checkpoints`` 是注册期 AST 的尽力检测结果，
        仅供人和工具审阅，不构成运行时契约。
        """
        profile = self._static_workflow_profile()
        graph = profile["graph"]
        description = {
            entry["name"]: copy.deepcopy(entry["declaration"]) for entry in graph["nodes"]
        }
        description["subgraphs"] = {
            mount["namespace"]: {
                key: copy.deepcopy(value) for key, value in mount.items() if key != "namespace"
            }
            for mount in graph["mounts"]
        }
        description["models"] = copy.deepcopy(graph["models"])
        return description

    def _node_profile_declaration(self, name: str, node: _Node) -> dict[str, Any]:
        """Build the declaration projection embedded in the canonical profile IR."""
        del name
        kind = "scan" if node.scan else "map" if node.items_from is not None else "node"
        declaration: dict[str, Any] = {
            "kind": kind,
            "executor": node.executor,
            "doc": _function_doc(node.function),
            "deps": list(node.deps),
            "items_from": _locator_description(node.items_from),
            "carry_from": _locator_description(node.carry_from),
            "prompt_specs": [spec.canonical() for spec in node.prompt_specs],
            "files": [str(path) for path in node.files],
            "params": {
                key: f"<{type(node.params[key]).__name__} sha256={sha(node.params[key])}>"
                for key in sorted(node.params)
            },
            "has_files_fn": node.files_fn is not None,
            "has_carry_fn": node.carry_fn is not None,
            "has_aggregate_fn": node.aggregate_fn is not None,
            "cache": node.cache,
            "has_external_fingerprint": node.external_fingerprint_digest is not None,
            "retry_policy": node.retry.canonical() if node.retry is not None else None,
            "retry_policy_digest": node.retry.digest if node.retry is not None else None,
            "evidence_policy": (node.evidence_policy or self._caller_evidence_policy()).canonical(),
            "evidence_policy_digest": (
                node.evidence_policy or self._caller_evidence_policy()
            ).digest,
            "subgraph": node.subgraph,
            "validated_models": copy.deepcopy(list(node.validated_models)),
            "checkpoints": list(node.checkpoints),
        }
        if node.executor == "agent":
            assert node.agent_adapter is not None
            assert node.agent_identity is not None
            declaration["agent"] = copy.deepcopy(node.agent_identity)
        if node.consumes:
            declaration["consumes"] = list(node.consumes)
        return declaration

    def _static_workflow_profile(
        self,
        prompt_snapshot: PromptCatalogSnapshot | None = None,
    ) -> dict[str, Any]:
        """Build the one canonical content-free IR used by all profile surfaces."""
        snapshot = prompt_snapshot or self._prompt_snapshot()
        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []
        specs: list[dict[str, Any]] = []
        for name, node in self._nodes.items():
            declaration = self._node_profile_declaration(name, node)
            nodes.append(
                {
                    "name": name,
                    "kind": declaration["kind"],
                    "executor": node.executor,
                    "subgraph": node.subgraph,
                    "cache": node.cache,
                    "params_digest": sha(node.params),
                    "retry_policy": (node.retry.canonical() if node.retry is not None else None),
                    "retry_policy_digest": (node.retry.digest if node.retry is not None else None),
                    "evidence_policy": (
                        node.evidence_policy or self._caller_evidence_policy()
                    ).canonical(),
                    "evidence_policy_digest": (
                        node.evidence_policy or self._caller_evidence_policy()
                    ).digest,
                    "declaration": declaration,
                }
            )
            locator_sources = {
                locator[0] for locator in (node.items_from, node.carry_from) if locator is not None
            }
            edges.extend(
                {"from": dependency, "to": name, "role": "dependency", "path": []}
                for dependency in node.deps
                if dependency not in locator_sources
            )
            if node.items_from is not None:
                edges.append(
                    {
                        "from": node.items_from[0],
                        "to": name,
                        "role": "items_from",
                        "path": [node.items_from[1]],
                    }
                )
            if node.carry_from is not None:
                edges.append(
                    {
                        "from": node.carry_from[0],
                        "to": name,
                        "role": "carry_from",
                        "path": [node.carry_from[1]],
                    }
                )
            bindings = dict(node.input_bindings)

            def add_prompt_edge(
                source: dict[str, Any],
                *,
                role: str,
                prompt_spec: str,
                _bindings: dict[str, str] = bindings,
                _node: _Node = node,
                _name: str = name,
                **details: Any,
            ) -> None:
                path_source = source.get("path_from", source)
                if not isinstance(path_source, dict):
                    raise ValueError(f"Invalid Prompt material source: {source!r}")
                source_kind = path_source["kind"]
                source_node: str | None
                binding: dict[str, Any]
                if source_kind == "input":
                    local = path_source["name"]
                    source_node = _bindings.get(local, local)
                    binding = {"input": local}
                elif source_kind == "item":
                    source_node = _node.items_from[0] if _node.items_from is not None else None
                    binding = {
                        "items_from": _locator_description(_node.items_from),
                    }
                elif source_kind == "carry":
                    source_node = _node.carry_from[0] if _node.carry_from is not None else None
                    binding = {
                        "carry_from": _locator_description(_node.carry_from),
                    }
                else:
                    source_node = None
                    binding = {"param": path_source["name"]}
                if source_kind not in {"input", "item", "carry", "param"}:
                    raise ValueError(f"Unsupported Prompt material source: {source!r}")
                if source.get("kind") == "file_ref":
                    binding["file_ref"] = True
                edges.append(
                    {
                        "from": source_node,
                        "to": _name,
                        "role": role,
                        "prompt_spec": prompt_spec,
                        "path": copy.deepcopy(path_source.get("path", [])),
                        "source": copy.deepcopy(source),
                        "binding": binding,
                        **details,
                    }
                )

            for spec in node.prompt_specs:
                declaration = snapshot.declaration(spec)
                specs.append(
                    {
                        "id": f"{name}:{spec.name}",
                        "node": name,
                        "name": spec.name,
                        "declaration": declaration,
                        "resolution_status": "unresolved",
                    }
                )
                canonical = spec.canonical()
                for layer in canonical["layers"]:
                    source = layer["source"]
                    if source["kind"] != "axis":
                        continue
                    selector = source["selector"]
                    add_prompt_edge(
                        selector,
                        role="selector",
                        prompt_spec=spec.name,
                        axis=source["name"],
                    )
                for material in canonical["materials"]:
                    source = material["source"]
                    add_prompt_edge(
                        source,
                        role="material",
                        prompt_spec=spec.name,
                        slot=material["slot"],
                    )
        return {
            "workflow_profile_schema": workflow_profile.WORKFLOW_PROFILE_SCHEMA,
            "mode": "static",
            "resolution_status": "unresolved",
            "graph": {
                "nodes": nodes,
                "edges": edges,
                "mounts": [
                    {"namespace": namespace, **copy.deepcopy(declaration)}
                    for namespace, declaration in self._subgraphs.items()
                ],
                "models": _describe_models(self._nodes.values()),
            },
            "prompts": {"specs": specs},
            "run": None,
        }

    def profile(
        self,
        run_id: str | None = None,
        *,
        include_content: bool = False,
    ) -> dict[str, Any]:
        """Return a static or persisted runtime WorkflowProfile without execution."""
        if run_id is None:
            return self._static_workflow_profile()
        run_path = store.run_directory(self.config.artifacts_path, run_id)
        if not run_path.is_dir():
            raise ValueError(f"Run {run_id!r} does not exist")
        return workflow_profile.load_run_profile(
            run_path,
            include_content=include_content,
        )

    def render_summary(self) -> str:
        """渲染每节点一行的 Markdown 声明表，不读取运行状态。"""
        return views.render_summary(self.describe())

    def render_mermaid(self, run_id: str | None = None) -> str:
        """渲染可由 GitHub Mermaid 解释的 DAG 图；可选叠加已落盘运行状态。"""
        return views.render_mermaid(self.describe(), self._render_runtime(run_id))

    def render_pipeline(self, run_id: str | None = None) -> str:
        """渲染自包含 HTML 工位架，可选叠加已落盘运行状态。"""
        title = self.config.project_root.name or "pipeline"
        return views.render_pipeline(self.describe(), title, self._render_runtime(run_id))

    def render_pipeline_text(self, run_id: str | None = None) -> str:
        """渲染可直接打印到终端的 Unicode 工位架，可选叠加运行状态。"""
        title = self.config.project_root.name or "pipeline"
        return views.render_pipeline_text(self.describe(), title, self._render_runtime(run_id))

    def _render_runtime(self, run_id: str | None) -> dict[str, Any] | None:
        """一次读取 sidecar，供所有只读渲染复用同一份运行态。"""
        if run_id is None:
            return None
        run_directory = store.run_directory(self.config.artifacts_path, run_id)
        if not run_directory.is_dir():
            raise ValueError(f"Run {run_id!r} does not exist")
        metadata = _read_run_metadata(run_directory)
        approvals = run_directory / "approvals"
        pending_names = (
            {path.name[: -len(".pending.json")] for path in approvals.glob("*.pending.json")}
            if approvals.is_dir()
            else set()
        )
        pending_nodes = {
            name
            for name, node in self._nodes.items()
            if any(
                _pending_checkpoint_belongs_to_node(node, checkpoint, pending_name)
                for checkpoint in node.checkpoints
                for pending_name in pending_names
            )
        }
        attempt_states: dict[str, list[dict[str, Any]]] = {}
        for state_path in sorted((run_directory / "attempts").glob("*/state.json")):
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            target = state.get("target") if isinstance(state, dict) else None
            if not isinstance(target, str):
                continue
            node_name = target.partition("@")[0]
            attempt_states.setdefault(node_name, []).append(state)
        retry_nodes = {
            name
            for name, states in attempt_states.items()
            if any(state.get("status") == "retry_scheduled" for state in states)
        }
        ambiguous_nodes = {
            name
            for name, states in attempt_states.items()
            if any(state.get("status") == "ambiguous" for state in states)
        }
        failed_nodes = {
            name
            for name, states in attempt_states.items()
            if any(state.get("status") == "failed" for state in states)
        }
        blocked_nodes = pending_nodes | retry_nodes | ambiguous_nodes | failed_nodes
        return {
            "metadata": metadata,
            "pending_names": pending_names,
            "pending_nodes": pending_nodes,
            "retry_nodes": retry_nodes,
            "ambiguous_nodes": ambiguous_nodes,
            "failed_nodes": failed_nodes,
            "attempt_states": attempt_states,
            "skipped_nodes": _downstream_nodes(self._nodes, blocked_nodes),
        }

    def cli(self, argv: list[str] | None = None) -> None:
        """Run the graph CLI and exit with its status code."""
        sys.exit(self.run_command(_build_cli_parser().parse_args(argv)))

    def run_command(self, args: argparse.Namespace) -> int:
        """Dispatch one already-parsed graph command and return its exit code.

        Both entry points land here: ``dag <command>`` via :meth:`cli`, and
        ``kigumi <command>`` after it imports the configured ``dag_entry``. Neither
        owns a second copy of the dispatch table.
        """
        handlers = {
            "check": self._cli_check,
            "plan": self._cli_plan,
            "graph": self._cli_graph,
            "profile": self._cli_profile,
            "explain": self._cli_explain,
            "describe": self._cli_describe,
            "resume": self._cli_resume,
            "retry-resolve": self._cli_retry_resolve,
            "recover": self._cli_recover,
        }
        return handlers[args.command](args)

    def _cli_check(self, args: argparse.Namespace) -> int:
        """Print static declaration and source-guard findings without running nodes."""
        del args
        guard_findings: list[Any] = []
        if self.config.source_dirs:
            guard_findings.extend(check_paths(self.config.source_paths))
            guard_findings.extend(check_raw_io_node_paths(self.config.source_paths))
        guard_violations = [finding for finding in guard_findings if not finding.waived]
        guard_waivers = [finding for finding in guard_findings if finding.waived]

        errors = [
            (
                f"{_cli_display_path(self.config.project_root, finding.path)}:{finding.lineno}: "
                f"{finding.snippet} [violation]"
            )
            for finding in guard_violations
        ]
        for name, node in self._nodes.items():
            for declared_path in node.files:
                if not self.config.resolve(declared_path).is_file():
                    errors.append(f"{name}: missing declared file {declared_path}")

        warnings = [
            f"{name}: missing docstring"
            for name, node in self._nodes.items()
            if _function_doc(node.function) is None
        ]
        models = _describe_models(self._nodes.values())
        for model_name, fields in models.items():
            warnings.extend(
                f"{model_name}.{field['name']}: missing field description"
                for field in fields
                if not field["description"]
            )

        print(
            f"check: {self.config.project_root.name or 'pipeline'} "
            f"({len(self._nodes)} nodes, {len(models)} models)"
        )
        if errors:
            print("\nerrors:")
            for message in errors:
                print(f"  {message}")
        if warnings:
            print("\nwarnings:")
            for message in warnings:
                print(f"  {message}")
        if guard_waivers:
            print("\nguard findings:")
            for finding in guard_waivers:
                location = _cli_display_path(self.config.project_root, finding.path)
                print(f"  {location}:{finding.lineno}: {finding.snippet} [waived]")
        print(f"\nguards: {len(guard_violations)} violations, {len(guard_waivers)} waived")
        print(f"\nsummary: {len(errors)} errors, {len(warnings)} warnings")
        return 1 if errors else 0

    def _cli_plan(self, args: argparse.Namespace) -> int:
        """Print a read-only cache forecast for optionally selected targets."""
        targets = (
            tuple(target.strip() for target in args.targets.split(",") if target.strip())
            if args.targets is not None
            else None
        )
        forecast = self.plan(targets=targets)
        hits = [name for name, status in forecast.nodes.items() if status == "hit"]
        print(
            f"plan: {len(forecast.nodes)} nodes, {len(forecast.certain)} certain, "
            f"{len(forecast.at_risk)} at_risk, {len(hits)} hit"
        )
        _cli_print_names("certain", forecast.certain)
        _cli_print_names("at_risk", forecast.at_risk)
        return 0

    def _cli_graph(self, args: argparse.Namespace) -> int:
        """Print the terminal graph or write the self-contained HTML graph."""
        if args.prompts:
            print(
                workflow_profile.render_profile_mermaid(
                    self.profile(args.run_id),
                    prompts=True,
                )
            )
        elif args.html is None:
            print(self.render_pipeline_text(run_id=args.run_id))
        else:
            output = Path(args.html)
            output.write_text(self.render_pipeline(run_id=args.run_id), encoding="utf-8")
            print(output)
        return 0

    def _cli_profile(self, args: argparse.Namespace) -> int:
        value = self.profile(args.run_id, include_content=args.include_content)
        if args.format == "json":
            print(json.dumps(value, ensure_ascii=False, indent=2))
        else:
            print(workflow_profile.render_profile_markdown(value))
        return 0

    def _cli_explain(self, args: argparse.Namespace) -> int:
        """Print the existing cache explanation for one node or map item."""
        print(str(self.explain(args.node_name, run_id=args.run_id)))
        return 0

    def _cli_describe(self, args: argparse.Namespace) -> int:
        """Print the existing Markdown or JSON declaration summary."""
        if args.format == "json":
            print(json.dumps(self.describe(), ensure_ascii=False, indent=2))
        else:
            print(self.render_summary())
        return 0

    def _cli_resume(self, args: argparse.Namespace) -> int:
        """Resume a bound 0.7 run and report its durable terminal/pending state."""
        try:
            result = self.resume(args.run_id, workers=args.workers)
        except Exception as error:
            print(f"error: {error}", file=sys.stderr)
            return 1
        print(
            f"run={result.run_id} status={result.run_status} "
            f"artifacts={len(result.artifacts)} retries={len(result.pending_retries)} "
            f"ambiguous={len(result.ambiguous_attempts)}"
        )
        return 0

    def _cli_retry_resolve(self, args: argparse.Namespace) -> int:
        """Persist an explicit operator verdict for an ambiguous attempt."""
        try:
            self.retry_resolve(
                args.run_id,
                args.target,
                attempt=args.attempt,
                action=args.action,
                reason=args.reason,
            )
        except Exception as error:
            print(f"error: {error}", file=sys.stderr)
            return 1
        print(
            f"resolved {args.target} attempt={args.attempt} action={args.action} run={args.run_id}"
        )
        return 0

    def _cli_recover(self, args: argparse.Namespace) -> int:
        """Persist an explicit decision for a terminal failed run."""
        try:
            receipt = self.recover(
                args.run_id,
                args.target,
                from_attempt=args.attempt,
                decision=args.decision,
                reason=args.reason,
                evidence=args.evidence,
            )
        except Exception as error:
            print(f"error: {error}", file=sys.stderr)
            return 1
        print(
            f"run={args.run_id} target={args.target} "
            f"from_attempt={receipt.from_attempt} to_attempt={receipt.to_attempt} "
            f"decision={receipt.decision} evidence_count={len(receipt.evidence_refs)}"
        )
        return 0

    def approve(self, run_id: str, name: str, data: Any) -> None:
        """Record approval bound to the pending payload; content changes void it."""
        store.approve_checkpoint(self.config.artifacts_path / "runs", run_id, name, data)

    def diff(self, run_a: str, run_b: str) -> dict[str, list[str]]:
        """Compare persisted node artifacts by canonical content hash."""
        return store.diff_runs(store.runs_root(self.config.artifacts_path), run_a, run_b)

    def gc(self, keep_last: int) -> int:
        """Delete unreferenced cache entries and blobs, returning their combined count."""
        return store.gc_artifacts(self.config.artifacts_path, keep_last)

    def _topological_order(self, targets: tuple[str, ...]) -> list[str]:
        order: list[str] = []
        state: dict[str, int] = {}

        def visit(name: str) -> None:
            if name not in self._nodes:
                raise ValueError(f"Unknown dependency or target node: {name!r}")
            if state.get(name) == 1:
                raise ValueError(f"Cycle detected at node {name!r}")
            if state.get(name) == 2:
                return
            state[name] = 1
            for dependency in self._nodes[name].deps:
                if dependency not in self._nodes:
                    raise ValueError(f"Unknown dependency {dependency!r} for node {name!r}")
                visit(dependency)
            state[name] = 2
            order.append(name)

        for target in targets:
            visit(target)
        return order

    def _parse_forced(self, force: Iterable[str]) -> tuple[set[str], dict[str, set[str]]]:
        """Validate run/plan force selectors in one shared code path."""
        forced = set(force)
        forced_nodes = {name for name in forced if "@" not in name}
        forced_items: dict[str, set[str]] = {}
        for name in forced - forced_nodes:
            map_name, item_id = name.split("@", 1)
            forced_items.setdefault(map_name, set()).add(item_id)
        unknown_forced = forced_nodes - self._nodes.keys()
        unknown_item_maps = {
            name
            for name in forced_items
            if name not in self._nodes or self._nodes[name].items_from is None
        }
        unknown_forced.update(
            f"{name}@{item_id}" for name in unknown_item_maps for item_id in forced_items[name]
        )
        if unknown_forced:
            # force 名字打错不能静默全量命中缓存——那看起来像成功,实际什么都没重算。
            raise ValueError(f"Unknown forced nodes: {', '.join(sorted(unknown_forced))}")
        return forced_nodes, forced_items

    @staticmethod
    def _consumed_view(
        node: _Node,
        dependency: str,
        artifact: dict[str, Any],
    ) -> dict[str, Any]:
        """Return one declared edge view in the same canonical shape used by cache reads."""
        projector = node.consumes[dependency]
        try:
            view = projector(copy.deepcopy(artifact))
        except Exception as error:
            raise RuntimeError(
                f"Node {node.name!r} consumes dependency {dependency!r} failed: "
                f"{type(error).__name__}: {error}"
            ) from error
        if not isinstance(view, dict):
            raise RuntimeError(
                f"Node {node.name!r} consumes dependency {dependency!r} failed: "
                "TypeError: projection must return a dict"
            )
        try:
            return json.loads(canonical_json(view))
        except Exception as error:
            raise RuntimeError(
                f"Node {node.name!r} consumes dependency {dependency!r} failed: "
                f"{type(error).__name__}: projection must be JSON serializable"
            ) from error

    @classmethod
    def _function_inputs(
        cls,
        node: _Node,
        inputs: Mapping[str, dict[str, Any]],
        *,
        omitted_local: set[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Expose mounted dependencies under local names while preserving ordinary DAG inputs."""
        omitted = omitted_local or set()
        if not node.input_bindings:
            return {
                name: cls._consumed_view(node, name, artifact)
                if name in node.consumes
                else artifact
                for name, artifact in inputs.items()
                if name not in omitted
            }
        return {
            local: cls._consumed_view(node, local, inputs[actual])
            if local in node.consumes
            else inputs[actual]
            for local, actual in node.input_bindings
            if local not in omitted
        }

    def _map_entries(
        self, node: _Node, inputs: Mapping[str, dict[str, Any]]
    ) -> list[tuple[str, Any]]:
        """Resolve, validate, and name one map's runtime input list."""
        assert node.items_from is not None
        source_name, artifact_path = node.items_from
        raw_items = _resolve_items_from(node.name, source_name, artifact_path, inputs[source_name])
        if not isinstance(raw_items, list):
            raise ValueError(
                f"Map node {node.name!r} items_from {source_name!r}.{artifact_path!r} "
                f"must be a list, got {type(raw_items).__name__}"
            )
        entries: list[tuple[str, Any]] = []
        ids: list[str] = []
        for item in raw_items:
            try:
                canonical_json(item)
            except (TypeError, ValueError) as error:
                raise ValueError(f"Map node {node.name!r} item is not JSON serializable") from error
            item_id = node.key_fn(item) if node.key_fn is not None else sha(item)[:12]
            item_path = Path(item_id) if isinstance(item_id, str) else None
            if (
                item_path is None
                or not item_id
                or "@" in item_id
                or "/" in item_id
                or "\\" in item_id
                or item_path.name != item_id
                or item_id in {".", ".."}
            ):
                raise ValueError(
                    f"Map node {node.name!r} item_id must be a non-empty str without '@' "
                    f"and a single relative path component: {item_id!r}"
                )
            entries.append((item_id, item))
            ids.append(item_id)
        duplicates = sorted({item_id for item_id in ids if ids.count(item_id) > 1})
        if duplicates:
            raise ValueError(
                f"Map node {node.name!r} has duplicate item_id values: {', '.join(duplicates)}"
            )
        return entries

    def _execute_map(
        self,
        node: _Node,
        inputs: dict[str, dict[str, Any]],
        upstream_shas: Mapping[str, str],
        run_id: str,
        libs_hash: str,
        libs_cache_reusable: bool,
        workers: int,
        *,
        permit_plane: _PermitPlane,
        executor: ThreadPoolExecutor,
        forced_all: bool,
        forced_items: set[str],
        envelope: ExecutionEnvelope,
        attempt_store: AttemptStore,
        prompt_snapshot: PromptCatalogSnapshot,
        budget_abort: threading.Event,
        file_snapshot: _FileSnapshot,
        allow_new_dynamic_files_ledger: bool,
    ) -> tuple[dict[str, Any], bool, list[str], dict[str, str], str]:
        """Run a map's runtime list without exposing its items as graph vertices."""
        assert node.items_from is not None
        source_name, _ = node.items_from
        entries = self._map_entries(node, inputs)
        ids = [item_id for item_id, _ in entries]
        unknown_forced = sorted(forced_items - set(ids))
        if unknown_forced:
            forced_names = ", ".join(f"{node.name}@{item_id}" for item_id in unknown_forced)
            raise ValueError(f"Unknown forced map items: {forced_names}")

        item_files_by_id = {
            item_id: (tuple(Path(path) for path in node.files_fn(item)) if node.files_fn else ())
            for item_id, item in entries
        }
        file_snapshot = file_snapshot.extend(
            (path for item_files in item_files_by_id.values() for path in item_files),
            self.config.resolve,
        )
        self._bind_dynamic_files_ledger(
            attempt_store,
            run_id,
            node,
            self._dynamic_files_observation(node, item_files_by_id, file_snapshot),
            allow_new=allow_new_dynamic_files_ledger,
        )

        shared_inputs = self._function_inputs(
            node,
            inputs,
            omitted_local={node.local_items_source or source_name},
        )

        def execute_item(item_id: str, item: Any) -> dict[str, Any]:
            if budget_abort.is_set():
                return {"id": item_id, "status": "aborted"}
            started = time.monotonic()
            target = f"{node.name}@{item_id}"
            checkpoint_used = False
            try:
                with observe() as calls:
                    item_files = item_files_by_id[item_id]
                    file_contents = self._file_contents(
                        node,
                        item_files,
                        file_snapshot=file_snapshot,
                    )
                    prompt_resolutions = self._resolve_prompt_specs(
                        node,
                        prompt_snapshot,
                        inputs,
                        item=item,
                        function_inputs=shared_inputs,
                        file_contents=file_contents,
                    )
                    prompt_resolution_records = {
                        name: resolved.resolution.canonical()
                        for name, resolved in prompt_resolutions.items()
                    }
                    key_components = self._key_components(
                        node,
                        upstream_shas,
                        libs_hash,
                        upstream_artifacts=inputs,
                        item=item,
                        item_files=item_files,
                        prompt_snapshot=prompt_snapshot,
                        prompt_resolutions=prompt_resolutions,
                        projected_inputs=shared_inputs,
                        file_contents=file_contents,
                    )
                    cache_key = sha(key_components)
                    declaration_digest = self._attempt_declaration_digest(
                        node,
                        key_components,
                        self._caller_evidence_policy(),
                    )
                    cache_entry: store.CacheEntry | None = None
                    resumed_completed = False
                    run_root = envelope.artifacts_path / "runs" / run_id
                    prior_state = attempt_store.state_for(target)
                    run_artifact = run_root / f"{target}.json"
                    run_sidecar = run_root / f"{target}.json.meta.json"
                    has_artifact = run_artifact.is_file()
                    has_sidecar = run_sidecar.is_file()
                    if has_artifact or has_sidecar:
                        if not (has_artifact and has_sidecar):
                            raise RunManifestError(
                                f"Target {target!r} has an incomplete run artifact pair"
                            )
                        if prior_state is None:
                            raise RunManifestError(
                                f"Target {target!r} has no durable state/candidate ownership"
                            )
                        if prior_state.get("status") == "completed":
                            artifact, prior_metadata = self._resume_completed_artifact(
                                run_root,
                                target,
                                key_components,
                                cache_key,
                                state=prior_state,
                                prompt_resolutions=prompt_resolution_records,
                            )
                            cache_hit = prior_metadata.get("cache") == "hit"
                            if prior_metadata.get("cache_policy") == "off":
                                checkpoint_used = True
                            resumed_completed = True
                        elif prior_state.get("status") == "success_candidate":
                            artifact, cache_hit = None, False
                        else:
                            raise RunManifestError(
                                f"Target {target!r} run artifact pair is not owned by a "
                                "completed state/candidate"
                            )
                    elif prior_state is not None:
                        artifact, cache_hit = None, False
                    else:
                        try:
                            cache_entry = self._cache_entry_for_lookup(
                                cache_key,
                                forced=forced_all or item_id in forced_items,
                                cache_policy=(node.cache if libs_cache_reusable else "off"),
                                evidence_policy=(
                                    node.evidence_policy or self._caller_evidence_policy()
                                ),
                            )
                        except CacheIntegrityError as error:
                            self._record_cache_lookup_failure(
                                attempt_store,
                                target,
                                error,
                                policy=node.retry,
                                declaration_digest=declaration_digest,
                                prompt_resolutions=prompt_resolution_records,
                                calls=calls,
                            )
                            raise
                        artifact = cache_entry.artifact if cache_entry is not None else None
                        cache_hit = cache_entry is not None
                    if artifact is None:
                        # No receipt is created for work that was not admitted after
                        # the budget abort. Once prepare() starts, the item is in
                        # flight and is allowed to finish with a durable outcome.
                        if budget_abort.is_set():
                            return {"id": item_id, "status": "aborted"}
                        prepared = attempt_store.prepare(
                            target,
                            policy=node.retry,
                            declaration_digest=declaration_digest,
                            prompt_resolutions=prompt_resolution_records,
                        )
                        action = prepared["action"]
                        if action == "pending":
                            return {
                                "id": item_id,
                                "status": "retry_pending",
                                "target": target,
                            }
                        if action == "failed":
                            raise RunManifestError(
                                f"Map target {target!r} is already terminally failed"
                            )
                        if action == "completed":
                            artifact, _metadata = self._resume_completed_artifact(
                                envelope.artifacts_path / "runs" / run_id,
                                target,
                                key_components,
                                cache_key,
                                state=prepared["state"],
                                prompt_resolutions=prompt_resolution_records,
                            )
                            resumed_completed = True
                        elif action == "candidate":
                            candidate = prepared["candidate"]
                            if (
                                candidate.get("candidate_schema") != SUCCESS_CANDIDATE_SCHEMA
                                or candidate.get("cache_key") != cache_key
                                or candidate.get("key_components") != key_components
                                or candidate.get("prompt_resolutions") != prompt_resolution_records
                                or not isinstance(candidate.get("artifact"), dict)
                            ):
                                raise RunManifestError(f"Success candidate for {target!r} changed")
                            artifact = copy.deepcopy(candidate["artifact"])
                            saved_calls = candidate.get("calls", [])
                            if not isinstance(saved_calls, list):
                                raise RunManifestError(
                                    f"Success candidate calls for {target!r} are invalid"
                                )
                            calls.extend(copy.deepcopy(saved_calls))
                            checkpoint_used = candidate.get("checkpoint_used") is True
                            started = time.monotonic() - float(candidate.get("seconds", 0.0))
                        else:
                            context = NodeContext(
                                self,
                                node,
                                run_id,
                                checkpoint_suffix=item_id,
                                item_files=item_files,
                                prompt_resolutions=prompt_resolutions,
                                file_snapshot=file_snapshot,
                            )
                            boundary = (
                                durable_side_effect_boundary(
                                    lambda effect, attempt_target=target: (
                                        attempt_store.mark_side_effect(attempt_target, effect)
                                    )
                                )
                                if node.executor not in {"pure", "agent"}
                                else nullcontext()
                            )
                            try:
                                with permit_plane.acquire(node.resources), boundary:
                                    artifact = node.function(  # type: ignore[call-arg]
                                        item,
                                        copy.deepcopy(shared_inputs),
                                        context,
                                    )
                            except CheckpointPending as pending:
                                envelope.record_pending(pending.name, pending.payload)
                                attempt_store.mark_checkpoint(target, pending.name)
                                return {
                                    "id": item_id,
                                    "status": "pending",
                                    "pending": pending.name,
                                }
                            except Exception as error:
                                failure_outcome = attempt_store.record_failure(
                                    target,
                                    error,
                                    policy=node.retry,
                                    calls=calls,
                                )
                                if failure_outcome["action"] == "pending":
                                    return {
                                        "id": item_id,
                                        "status": "retry_pending",
                                        "target": target,
                                    }
                                if (
                                    node.retry is not None
                                    and node.retry.allows(failure_provider_kind(error))
                                    and int(failure_outcome["state"]["attempt"])
                                    >= node.retry.max_attempts
                                ):
                                    raise RetryExhausted(
                                        target,
                                        int(failure_outcome["state"]["attempt"]),
                                        canonical_failure(error),
                                    ) from error
                                raise
                            if not isinstance(artifact, dict):
                                raise TypeError(
                                    f"Map node {node.name!r} item {item_id!r} "
                                    "must return a dict artifact"
                                )
                            artifact = json.loads(canonical_json(artifact))
                            checkpoint_used = context._checkpoint_used
                            attempt_store.save_candidate(
                                target,
                                {
                                    "candidate_schema": SUCCESS_CANDIDATE_SCHEMA,
                                    "artifact": artifact,
                                    "cache_key": cache_key,
                                    "key_components": key_components,
                                    "calls": copy.deepcopy(calls),
                                    "agent_provenance": None,
                                    "seconds": time.monotonic() - started,
                                    "checkpoint_used": checkpoint_used,
                                    "prompt_resolutions": prompt_resolution_records,
                                },
                            )
                        if not resumed_completed:
                            artifact = envelope.seal(
                                artifact,
                                cache_key,
                                label=(f"Map node {node.name!r} item {item_id!r}"),
                                calls=calls,
                                cache_policy=(
                                    "off"
                                    if checkpoint_used or not libs_cache_reusable
                                    else node.cache
                                ),
                                evidence_policy=self._caller_evidence_policy(),
                                prompt_resolutions=prompt_resolution_records,
                            )
                    if artifact is not None and cache_hit and not resumed_completed:
                        prepared = attempt_store.prepare(
                            target,
                            policy=node.retry,
                            declaration_digest=declaration_digest,
                            prompt_resolutions=prompt_resolution_records,
                        )
                        if prepared["action"] != "run":
                            raise RunManifestError(
                                f"Cache hit for {target!r} has an unexpected durable state"
                            )
                        attempt_store.save_candidate(
                            target,
                            {
                                "candidate_schema": SUCCESS_CANDIDATE_SCHEMA,
                                "artifact": artifact,
                                "cache_key": cache_key,
                                "key_components": key_components,
                                "calls": copy.deepcopy(calls),
                                "agent_provenance": None,
                                "seconds": time.monotonic() - started,
                                "checkpoint_used": False,
                                "prompt_resolutions": prompt_resolution_records,
                            },
                        )
                    return {
                        "id": item_id,
                        "status": "success",
                        "artifact": artifact,
                        "cache": "hit" if cache_hit else "miss",
                        "cache_key": cache_key,
                        "key_components": key_components,
                        "seconds": time.monotonic() - started,
                        "calls": calls,
                        "resumed_completed": resumed_completed,
                        "target": target,
                        "prompt_resolutions": prompt_resolution_records,
                        "cache_entry": cache_entry,
                        "cache_policy": (
                            "off" if checkpoint_used or not libs_cache_reusable else node.cache
                        ),
                    }
            except Exception as error:
                # KeyboardInterrupt/SystemExit 不许伪装成单项失败被聚合吞掉:
                # 让它按原类型冲出去,调度器 drain 后原样重抛。
                if isinstance(error, BudgetExceeded):
                    budget_abort.set()
                return {"id": item_id, "status": "failed", "error": error}

        outcomes: list[dict[str, Any]] = []
        if len(entries) <= 1 or workers == 1:
            for item_id, item in entries:
                outcome = execute_item(item_id, item)
                outcomes.append(outcome)
                if outcome["status"] == "aborted":
                    break
                if outcome["status"] == "failed" and isinstance(
                    outcome.get("error"), BudgetExceeded
                ):
                    break
        else:
            budget = getattr(self.caller, "budget", None)
            budget_limited = budget is not None and budget.max_tokens is not None
            # A finite budget uses closed batches. The last slot is held back so
            # a three-item map at workers=3 cannot admit item three before the
            # first two admissions have been observed. Items in a submitted
            # batch are in flight and are drained before aborting later batches.
            batch_size = max(1, workers - 1) if budget_limited else workers
            for start in range(0, len(entries), batch_size):
                if budget_abort.is_set():
                    break
                batch = entries[start : start + batch_size]
                futures = [executor.submit(execute_item, item_id, item) for item_id, item in batch]
                batch_outcomes = [future.result() for future in futures]
                outcomes.extend(batch_outcomes)
                if any(
                    outcome["status"] == "failed"
                    and isinstance(outcome.get("error"), BudgetExceeded)
                    for outcome in batch_outcomes
                ):
                    budget_abort.set()
                    break

        completed: dict[str, dict[str, Any]] = {}
        cache_keys: list[str] = []
        item_cache_statuses: dict[str, str] = {}
        pending: list[str] = []
        retry_pending: list[str] = []
        failures: list[dict[str, Any]] = []
        for outcome in outcomes:
            if outcome["status"] == "success":
                item_id = outcome["id"]
                artifact = outcome["artifact"]
                cache_keys.append(outcome["cache_key"])
                item_cache_statuses[item_id] = outcome["cache"]
                completed[item_id] = artifact
                label = f"{node.name}@{item_id}"
                outputs = envelope.materialize(label, artifact)
                if not outcome["resumed_completed"]:
                    envelope.write_sidecar(
                        label,
                        artifact,
                        outcome["cache_key"],
                        cache_hit=outcome["cache"] == "hit",
                        cache_entry=outcome["cache_entry"],
                        seconds=outcome["seconds"],
                        calls=outcome["calls"],
                        key_components=outcome["key_components"],
                        outputs=outputs,
                        cache_policy=outcome["cache_policy"],
                        evidence_policy=self._caller_evidence_policy(),
                        prompt_resolutions=outcome["prompt_resolutions"],
                    )
                if not outcome["resumed_completed"]:
                    attempt_store.mark_completed(
                        outcome["target"],
                        artifact_sha256=sha(artifact),
                    )
            elif outcome["status"] == "pending":
                pending.append(outcome["pending"])
            elif outcome["status"] == "retry_pending":
                retry_pending.append(outcome["target"])
            elif outcome["status"] == "aborted":
                # An item that was never admitted has no attempt, receipt, or
                # failure. It is intentionally absent from all public item
                # bookkeeping collections.
                continue
            else:
                failures.append(outcome)
        if failures:
            budget_failures = [
                outcome for outcome in failures if isinstance(outcome.get("error"), BudgetExceeded)
            ]
            first = budget_failures[0]["error"] if budget_failures else failures[0]["error"]
            if isinstance(first, DryRunError):
                raise first
            if isinstance(
                first,
                (
                    AmbiguousAttemptError,
                    CacheIntegrityError,
                    RetryExhausted,
                    ProviderFailure,
                    BudgetExceeded,
                ),
            ):
                raise first
            details = ", ".join(
                f"{outcome['id']} ({type(outcome['error']).__name__}: {outcome['error']})"
                for outcome in failures
            )
            raise RuntimeError(f"Map node {node.name!r} failed items: {details}") from first
        if pending:
            raise _MapCheckpointPending(pending)
        if retry_pending:
            raise _MapRetryPending(retry_pending)

        artifact = self._aggregate_map_artifact(node, completed, ids)
        effective_cache_policy = (
            "off"
            if not libs_cache_reusable
            or any(
                outcome.get("cache_policy") == "off"
                for outcome in outcomes
                if outcome["status"] == "success"
            )
            else node.cache
        )
        return (
            artifact,
            node.cache == "auto"
            and libs_cache_reusable
            and not forced_all
            and bool(outcomes)
            and all(
                outcome["status"] == "success" and outcome["cache"] == "hit" for outcome in outcomes
            ),
            cache_keys,
            item_cache_statuses,
            effective_cache_policy,
        )

    def _aggregate_map_artifact(
        self, node: _Node, items: dict[str, dict[str, Any]], order: list[str]
    ) -> dict[str, Any]:
        """Build one canonical map aggregate for both execution and cache forecasting."""
        if node.aggregate_fn is None:
            aggregate: Any = {"items": items, "order": order, "count": len(order)}
        else:
            aggregate = node.aggregate_fn(items, order)
        if not isinstance(aggregate, dict):
            raise TypeError(f"Map node {node.name!r} aggregate_fn must return a dict artifact")
        # 聚合也要走规范 JSON，命中和未命中向下游传递的字节形态才一致。
        return json.loads(canonical_json(aggregate))

    def _execute_scan(
        self,
        node: _Node,
        inputs: dict[str, dict[str, Any]],
        upstream_shas: Mapping[str, str],
        run_id: str,
        libs_hash: str,
        libs_cache_reusable: bool,
        workers: int,
        *,
        permit_plane: _PermitPlane,
        executor: ThreadPoolExecutor,
        forced_all: bool,
        forced_items: set[str],
        envelope: ExecutionEnvelope,
        attempt_store: AttemptStore,
        prompt_snapshot: PromptCatalogSnapshot,
        budget_abort: threading.Event,
        file_snapshot: _FileSnapshot,
        allow_new_dynamic_files_ledger: bool,
    ) -> tuple[dict[str, Any], bool, list[str], dict[str, str], str]:
        """Run one carry chain serially while retaining map's item cache and sidecar contract."""
        del workers, executor  # scan 的每项都依赖前一项 carry，串行是语义而非调度偏好。
        assert node.items_from is not None
        source_name, _ = node.items_from
        entries = self._map_entries(node, inputs)
        ids = [item_id for item_id, _ in entries]
        unknown_forced = sorted(forced_items - set(ids))
        if unknown_forced:
            forced_names = ", ".join(f"{node.name}@{item_id}" for item_id in unknown_forced)
            raise ValueError(f"Unknown forced scan items: {forced_names}")

        item_files_by_id = {
            item_id: (tuple(Path(path) for path in node.files_fn(item)) if node.files_fn else ())
            for item_id, item in entries
        }
        file_snapshot = file_snapshot.extend(
            (path for item_files in item_files_by_id.values() for path in item_files),
            self.config.resolve,
        )
        self._bind_dynamic_files_ledger(
            attempt_store,
            run_id,
            node,
            self._dynamic_files_observation(node, item_files_by_id, file_snapshot),
            allow_new=allow_new_dynamic_files_ledger,
        )

        omitted_local = {node.local_items_source or source_name}
        if node.carry_from is not None:
            omitted_local.add(node.local_carry_source or node.carry_from[0])
        shared_inputs = self._function_inputs(node, inputs, omitted_local=omitted_local)
        carry = (
            _resolve_items_from(
                node.name,
                node.carry_from[0],
                node.carry_from[1],
                inputs[node.carry_from[0]],
            )
            if node.carry_from is not None
            else None
        )
        completed: dict[str, dict[str, Any]] = {}
        cache_keys: list[str] = []
        item_cache_statuses: dict[str, str] = {}
        effective_cache_policy = node.cache if libs_cache_reusable else "off"

        for item_id, item in entries:
            started = time.monotonic()
            target = f"{node.name}@{item_id}"
            checkpoint_used = False
            evidence_policy = (
                node.evidence_policy
                if node.executor == "agent" and node.evidence_policy is not None
                else self._caller_evidence_policy()
            )
            agent_provenance: dict[str, Any] | None = None
            try:
                with observe() as calls:
                    item_files = item_files_by_id[item_id]
                    file_contents = self._file_contents(
                        node,
                        item_files,
                        file_snapshot=file_snapshot,
                    )
                    prompt_resolutions = self._resolve_prompt_specs(
                        node,
                        prompt_snapshot,
                        inputs,
                        item=item,
                        carry=carry,
                        function_inputs=shared_inputs,
                        file_contents=file_contents,
                    )
                    prompt_resolution_records = {
                        name: resolved.resolution.canonical()
                        for name, resolved in prompt_resolutions.items()
                    }
                    key_components = self._key_components(
                        node,
                        upstream_shas,
                        libs_hash,
                        upstream_artifacts=inputs,
                        item=item,
                        item_files=item_files,
                        carry=carry,
                        prompt_snapshot=prompt_snapshot,
                        prompt_resolutions=prompt_resolutions,
                        projected_inputs=shared_inputs,
                        file_contents=file_contents,
                    )
                    cache_key = sha(key_components)
                    declaration_digest = self._attempt_declaration_digest(
                        node,
                        key_components,
                        evidence_policy,
                    )
                    cache_entry: store.CacheEntry | None = None
                    resumed_completed = False
                    run_root = envelope.artifacts_path / "runs" / run_id
                    prior_state = attempt_store.state_for(target)
                    run_artifact = run_root / f"{target}.json"
                    run_sidecar = run_root / f"{target}.json.meta.json"
                    has_artifact = run_artifact.is_file()
                    has_sidecar = run_sidecar.is_file()
                    if has_artifact or has_sidecar:
                        if not (has_artifact and has_sidecar):
                            raise RunManifestError(
                                f"Target {target!r} has an incomplete run artifact pair"
                            )
                        if prior_state is None:
                            raise RunManifestError(
                                f"Target {target!r} has no durable state/candidate ownership"
                            )
                        if prior_state.get("status") == "completed":
                            artifact, prior_metadata = self._resume_completed_artifact(
                                run_root,
                                target,
                                key_components,
                                cache_key,
                                state=prior_state,
                                validate_agent=node.executor == "agent",
                                prompt_resolutions=prompt_resolution_records,
                            )
                            origin = prior_metadata.get("origin_provenance")
                            if isinstance(origin, dict) and isinstance(origin.get("agent"), dict):
                                agent_provenance = copy.deepcopy(origin["agent"])
                            cache_hit = prior_metadata.get("cache") == "hit"
                            if prior_metadata.get("cache_policy") == "off":
                                effective_cache_policy = "off"
                            resumed_completed = True
                        elif prior_state.get("status") == "success_candidate":
                            artifact, cache_hit = None, False
                        else:
                            raise RunManifestError(
                                f"Target {target!r} run artifact pair is not owned by a "
                                "completed state/candidate"
                            )
                    elif prior_state is not None:
                        artifact, cache_hit = None, False
                    else:
                        try:
                            cache_entry = self._cache_entry_for_lookup(
                                cache_key,
                                forced=forced_all or item_id in forced_items,
                                cache_policy=(node.cache if libs_cache_reusable else "off"),
                                evidence_policy=evidence_policy,
                            )
                        except CacheIntegrityError as error:
                            self._record_cache_lookup_failure(
                                attempt_store,
                                target,
                                error,
                                policy=node.retry,
                                declaration_digest=declaration_digest,
                                prompt_resolutions=prompt_resolution_records,
                                calls=calls,
                            )
                            raise
                        artifact = cache_entry.artifact if cache_entry is not None else None
                        cache_hit = cache_entry is not None
                    if artifact is None:
                        prepared = attempt_store.prepare(
                            target,
                            policy=node.retry,
                            declaration_digest=declaration_digest,
                            prompt_resolutions=prompt_resolution_records,
                        )
                        action = prepared["action"]
                        if action == "pending":
                            raise _MapRetryPending([target])
                        if action == "failed":
                            raise RunManifestError(
                                f"Scan target {target!r} is already terminally failed"
                            )
                        if action == "completed":
                            artifact, prior_metadata = self._resume_completed_artifact(
                                envelope.artifacts_path / "runs" / run_id,
                                target,
                                key_components,
                                cache_key,
                                state=prepared["state"],
                                validate_agent=node.executor == "agent",
                                prompt_resolutions=prompt_resolution_records,
                            )
                            origin = prior_metadata.get("origin_provenance")
                            if isinstance(origin, dict) and isinstance(origin.get("agent"), dict):
                                agent_provenance = copy.deepcopy(origin["agent"])
                            resumed_completed = True
                        elif action == "candidate":
                            candidate = prepared["candidate"]
                            if (
                                candidate.get("candidate_schema") != SUCCESS_CANDIDATE_SCHEMA
                                or candidate.get("cache_key") != cache_key
                                or candidate.get("key_components") != key_components
                                or candidate.get("prompt_resolutions") != prompt_resolution_records
                                or not isinstance(candidate.get("artifact"), dict)
                            ):
                                raise RunManifestError(f"Success candidate for {target!r} changed")
                            artifact = copy.deepcopy(candidate["artifact"])
                            saved_calls = candidate.get("calls", [])
                            if not isinstance(saved_calls, list):
                                raise RunManifestError(
                                    f"Success candidate calls for {target!r} are invalid"
                                )
                            calls.extend(copy.deepcopy(saved_calls))
                            saved_agent = candidate.get("agent_provenance")
                            agent_provenance = (
                                copy.deepcopy(saved_agent)
                                if isinstance(saved_agent, dict)
                                else None
                            )
                            checkpoint_used = candidate.get("checkpoint_used") is True
                            started = time.monotonic() - float(candidate.get("seconds", 0.0))
                        else:
                            context = NodeContext(
                                self,
                                node,
                                run_id,
                                checkpoint_suffix=item_id,
                                item_files=item_files,
                                prompt_resolutions=prompt_resolutions,
                                file_snapshot=file_snapshot,
                            )
                            boundary = (
                                durable_side_effect_boundary(
                                    lambda effect, attempt_target=target: (
                                        attempt_store.mark_side_effect(attempt_target, effect)
                                    )
                                )
                                if node.executor not in {"pure", "agent"}
                                else nullcontext()
                            )
                            try:
                                with permit_plane.acquire(node.resources), boundary:
                                    if node.executor == "agent":
                                        build_context = AgentBuildContext(
                                            node.params,
                                            context.read_text,
                                            context.read_bytes,
                                            context.resolve_prompt,
                                        )
                                        task = node.function(  # type: ignore[call-arg]
                                            item,
                                            copy.deepcopy(carry),
                                            copy.deepcopy(shared_inputs),
                                            build_context,
                                        )
                                        if not isinstance(task, AgentTask):
                                            raise TypeError(
                                                f"Agent scan {node.name!r} builder "
                                                "must return AgentTask"
                                            )
                                        instruction_resolution = (
                                            task.instruction.resolution.canonical()
                                            if isinstance(task.instruction, ResolvedPrompt)
                                            else None
                                        )
                                        try:
                                            lease_context = self.agent_slots.acquire(
                                                timeout_seconds=(
                                                    self.config.agent_slot_timeout_seconds
                                                )
                                            )
                                            with lease_context as lease:
                                                assert node.agent_adapter is not None
                                                assert node.agent_spec is not None
                                                assert node.agent_identity is not None
                                                attempt_store.mark_side_effect(
                                                    target,
                                                    {
                                                        "active_effect_schema": 1,
                                                        "kind": "agent",
                                                        "instruction_sha256": sha(
                                                            str(task.instruction)
                                                        ),
                                                        "managed": (
                                                            instruction_resolution is not None
                                                        ),
                                                        "prompt_resolution": (
                                                            instruction_resolution
                                                        ),
                                                    },
                                                )
                                                session_in = None
                                                if getattr(
                                                    node.agent_adapter,
                                                    "session_carry",
                                                    False,
                                                ):
                                                    if carry is not None and (
                                                        not isinstance(carry, Mapping)
                                                        or set(carry)
                                                        != {
                                                            "kigumi_attachment",
                                                            "bytes",
                                                            "media_type",
                                                        }
                                                    ):
                                                        raise AgentResultError(
                                                            "Agent session carry must be a "
                                                            "session attachment; declare "
                                                            "carry_fn=lambda artifact: "
                                                            'artifact["session"]'
                                                        )
                                                    session_in = carry
                                                outcome = execute_agent_task(
                                                    node_name=target,
                                                    run_id=run_id,
                                                    task=task,
                                                    inputs=copy.deepcopy(shared_inputs),
                                                    declared_files=(
                                                        *node.files,
                                                        *item_files,
                                                    ),
                                                    declared_file_contents=file_contents,
                                                    resolve=self.config.resolve,
                                                    artifacts_path=self.config.artifacts_path,
                                                    blob_store=self.blob_store,
                                                    adapter=node.agent_adapter,
                                                    adapter_identity=node.agent_identity,
                                                    spec=node.agent_spec,
                                                    evidence_policy=evidence_policy,
                                                    prompt_resolution=instruction_resolution,
                                                    session_in=session_in,
                                                )
                                        except SlotTimeoutError:
                                            raise AgentExecutionFailure(
                                                runtime_code=AgentRuntimeFailureCode.CAPACITY
                                            ) from None
                                        artifact = outcome.artifact
                                        agent_provenance = {
                                            **outcome.provenance,
                                            "queue_wait_seconds": lease.wait_seconds,
                                            "slot_identity": lease.slot_identity,
                                        }
                                    else:
                                        artifact = node.function(  # type: ignore[call-arg]
                                            item,
                                            copy.deepcopy(carry),
                                            copy.deepcopy(shared_inputs),
                                            context,
                                        )
                            except CheckpointPending as pending:
                                envelope.record_pending(pending.name, pending.payload)
                                attempt_store.mark_checkpoint(target, pending.name)
                                raise _MapCheckpointPending([pending.name]) from pending
                            except Exception as error:
                                failure_outcome = attempt_store.record_failure(
                                    target,
                                    error,
                                    policy=node.retry,
                                    calls=calls,
                                )
                                if failure_outcome["action"] == "pending":
                                    raise _MapRetryPending([target]) from error
                                if (
                                    node.retry is not None
                                    and node.retry.allows(failure_provider_kind(error))
                                    and int(failure_outcome["state"]["attempt"])
                                    >= node.retry.max_attempts
                                ):
                                    raise RetryExhausted(
                                        target,
                                        int(failure_outcome["state"]["attempt"]),
                                        canonical_failure(error),
                                    ) from error
                                raise
                            if not isinstance(artifact, dict):
                                raise TypeError(
                                    f"Scan node {node.name!r} item {item_id!r} "
                                    "must return a dict artifact"
                                )
                            artifact = json.loads(canonical_json(artifact))
                            checkpoint_used = context._checkpoint_used
                            attempt_store.save_candidate(
                                target,
                                {
                                    "candidate_schema": SUCCESS_CANDIDATE_SCHEMA,
                                    "artifact": artifact,
                                    "cache_key": cache_key,
                                    "key_components": key_components,
                                    "calls": copy.deepcopy(calls),
                                    "agent_provenance": copy.deepcopy(agent_provenance),
                                    "seconds": time.monotonic() - started,
                                    "checkpoint_used": checkpoint_used,
                                    "prompt_resolutions": prompt_resolution_records,
                                },
                            )
                        if not resumed_completed:
                            artifact = envelope.seal(
                                artifact,
                                cache_key,
                                label=(f"Scan node {node.name!r} item {item_id!r}"),
                                calls=calls,
                                cache_policy=(
                                    "off"
                                    if checkpoint_used or not libs_cache_reusable
                                    else node.cache
                                ),
                                evidence_policy=evidence_policy,
                                agent_provenance=agent_provenance,
                                prompt_resolutions=prompt_resolution_records,
                            )
                    if artifact is not None and cache_hit and not resumed_completed:
                        prepared = attempt_store.prepare(
                            target,
                            policy=node.retry,
                            declaration_digest=declaration_digest,
                            prompt_resolutions=prompt_resolution_records,
                        )
                        if prepared["action"] != "run":
                            raise RunManifestError(
                                f"Cache hit for {target!r} has an unexpected durable state"
                            )
                        attempt_store.save_candidate(
                            target,
                            {
                                "candidate_schema": SUCCESS_CANDIDATE_SCHEMA,
                                "artifact": artifact,
                                "cache_key": cache_key,
                                "key_components": key_components,
                                "calls": copy.deepcopy(calls),
                                "agent_provenance": copy.deepcopy(agent_provenance),
                                "seconds": time.monotonic() - started,
                                "checkpoint_used": False,
                                "prompt_resolutions": prompt_resolution_records,
                            },
                        )
                    if node.executor == "agent":
                        validate_agent_artifact(artifact, self.blob_store)
                        if isinstance(agent_provenance, dict):
                            validate_agent_provenance(agent_provenance, self.blob_store)
                    completed[item_id] = artifact
                    if checkpoint_used:
                        effective_cache_policy = "off"
                    cache_keys.append(cache_key)
                    item_cache_statuses[item_id] = "hit" if cache_hit else "miss"
                    label = f"{node.name}@{item_id}"
                    outputs = envelope.materialize(label, artifact)
                    if not resumed_completed:
                        envelope.write_sidecar(
                            label,
                            artifact,
                            cache_key,
                            cache_hit=cache_hit,
                            cache_entry=cache_entry,
                            seconds=time.monotonic() - started,
                            calls=calls,
                            key_components=key_components,
                            outputs=outputs,
                            cache_policy=(
                                "off" if checkpoint_used or not libs_cache_reusable else node.cache
                            ),
                            evidence_policy=evidence_policy,
                            agent_provenance=agent_provenance,
                            prompt_resolutions=prompt_resolution_records,
                        )
                    if not resumed_completed:
                        attempt_store.mark_completed(
                            target,
                            artifact_sha256=sha(artifact),
                        )
                    carry = node.carry_fn(artifact) if node.carry_fn is not None else artifact
            except (_MapCheckpointPending, _MapRetryPending):
                raise
            except CacheIntegrityError:
                raise
            except OutputOwnershipError:
                raise
            except Exception as error:
                if isinstance(error, BudgetExceeded):
                    budget_abort.set()
                    raise
                if isinstance(
                    error,
                    (
                        AmbiguousAttemptError,
                        RetryExhausted,
                        ProviderFailure,
                        AgentExecutionFailure,
                    ),
                ):
                    raise
                raise RuntimeError(
                    f"Scan node {node.name!r} failed item {item_id!r}: "
                    f"{type(error).__name__}: {error}"
                ) from error

        artifact = self._aggregate_map_artifact(node, completed, ids)
        return (
            artifact,
            node.cache == "auto"
            and libs_cache_reusable
            and not forced_all
            and bool(item_cache_statuses)
            and all(status == "hit" for status in item_cache_statuses.values()),
            cache_keys,
            item_cache_statuses,
            effective_cache_policy,
        )

    def _key_components(
        self,
        node: _Node,
        upstream_shas: Mapping[str, str],
        libs_hash: str,
        *,
        upstream_artifacts: Mapping[str, dict[str, Any]] | None = None,
        item: Any = _NO_ITEM,
        item_files: tuple[Path, ...] = (),
        carry: Any = _NO_CARRY,
        prompt_snapshot: PromptCatalogSnapshot | None = None,
        prompt_resolutions: Mapping[str, ResolvedPrompt] | None = None,
        projected_inputs: Mapping[str, dict[str, Any]] | None = None,
        file_contents: Mapping[str, bytes] | None = None,
    ) -> dict[str, str]:
        """从注入的上游摘要推导普通节点、map 或 scan 项的精确键成分。

        ``upstream_shas`` 是消费方各自的完整上游产物摘要；声明 consumes 的边
        改由 ``upstream_artifacts`` 计算实际投影视图摘要。动态节点的
        items_from（以及 scan 的 carry_from）不属于共享上游；前者由 ``item``
        入键，后者由本项实际收到的 ``carry`` 入键。
        """
        components = {
            "source": _source_hash(node.function),
            "libs": libs_hash,
            "params": sha(node.params),
            "kigumi": sha(_kigumi_key_inputs()),
        }
        if node.external_fingerprint_digest is not None:
            components["external"] = node.external_fingerprint_digest
        snapshot = prompt_snapshot or self._prompt_snapshot((node,))
        captured_files = (
            dict(file_contents)
            if file_contents is not None
            else self._file_contents(node, item_files)
        )
        resolutions = (
            dict(prompt_resolutions)
            if prompt_resolutions is not None
            else self._resolve_prompt_specs(
                node,
                snapshot,
                upstream_artifacts or {},
                item=item,
                carry=carry,
                item_files=item_files,
                file_contents=captured_files,
            )
        )
        excluded_upstreams: set[str] = set()
        if item is not _NO_ITEM:
            assert node.items_from is not None
            excluded_upstreams.add(node.local_items_source or node.items_from[0])
            if node.scan and node.carry_from is not None:
                excluded_upstreams.add(node.local_carry_source or node.carry_from[0])
            components["item"] = sha(item)

        def upstream_digest(local: str, actual: str) -> str:
            if local not in node.consumes:
                return upstream_shas[actual]
            if projected_inputs is not None:
                return sha(projected_inputs[local])
            if upstream_artifacts is None:
                raise RuntimeError(
                    f"Node {node.name!r} consumes dependency {local!r} "
                    "requires its upstream artifact"
                )
            return sha(self._consumed_view(node, local, upstream_artifacts[actual]))

        if node.input_bindings:
            components.update(
                (f"upstream:{local}", upstream_digest(local, actual))
                for local, actual in node.input_bindings
                if local not in excluded_upstreams
            )
        else:
            components.update(
                (f"upstream:{name}", upstream_digest(name, name))
                for name in upstream_shas
                if name not in excluded_upstreams
            )
        components.update(
            (f"prompt_specs:{name}", resolved.resolution.digest)
            for name, resolved in resolutions.items()
        )
        components.update(
            (f"files:{path}", _bytes_hash(captured_files[str(path)])) for path in node.files
        )
        if item is not _NO_ITEM:
            components.update(
                (f"item_files:{path}", _bytes_hash(captured_files[str(path)]))
                for path in item_files
            )
        if carry is not _NO_CARRY:
            # carry_fn 的源码不入键；只有本项实际收到的内容才是输入事实。
            components["carry"] = sha(carry)
        return dict(sorted(components.items()))

    def _prompt_snapshot(self, nodes: Iterable[_Node] | None = None) -> PromptCatalogSnapshot:
        selected = tuple(self._nodes.values()) if nodes is None else tuple(nodes)
        specs = tuple(spec for node in selected for spec in node.prompt_specs)
        return PromptCatalogSnapshot.capture(
            self.config.prompts_path,
            prompt_specs=specs,
        )

    def _resolve_prompt_specs(
        self,
        node: _Node,
        snapshot: PromptCatalogSnapshot,
        upstream_artifacts: Mapping[str, dict[str, Any]],
        *,
        item: Any = _NO_ITEM,
        carry: Any = _NO_CARRY,
        function_inputs: Mapping[str, dict[str, Any]] | None = None,
        item_files: tuple[Path, ...] = (),
        file_contents: Mapping[str, bytes] | None = None,
    ) -> dict[str, ResolvedPrompt]:
        if not node.prompt_specs:
            return {}
        omitted_local: set[str] = set()
        if item is not _NO_ITEM:
            assert node.items_from is not None
            omitted_local.add(node.local_items_source or node.items_from[0])
            if node.scan and node.carry_from is not None:
                omitted_local.add(node.local_carry_source or node.carry_from[0])
        resolved_inputs = (
            dict(function_inputs)
            if function_inputs is not None
            else self._function_inputs(
                node,
                upstream_artifacts,
                omitted_local=omitted_local,
            )
        )
        captured_files = (
            dict(file_contents)
            if file_contents is not None
            else self._file_contents(node, item_files)
        )
        return {
            spec.name: snapshot.resolve(
                spec,
                inputs=resolved_inputs,
                params=node.params,
                item=None if item is _NO_ITEM else item,
                carry=None if carry is _NO_CARRY else carry,
                has_item=item is not _NO_ITEM,
                has_carry=carry is not _NO_CARRY,
                file_contents=captured_files,
            )
            for spec in node.prompt_specs
        }

    def _file_contents(
        self,
        node: _Node,
        item_files: tuple[Path, ...] = (),
        *,
        file_snapshot: _FileSnapshot | None = None,
    ) -> dict[str, bytes]:
        """Capture each declared file once for prompt resolution and cache keying."""
        paths = (*node.files, *item_files)
        snapshot = file_snapshot or _FileSnapshot.capture(
            self.config.project_root,
            self.config.resolve,
            paths,
        )
        return snapshot.contents(paths)

    def _libs_hash(self, node: _Node) -> str:
        """Hash the configured library files statically reachable from one node."""
        return self._libs_identities((node,))[node.name].digest

    def _libs_hashes(self, nodes: Iterable[_Node]) -> dict[str, str]:
        """Compute deterministic per-node library digests for durable identity."""
        return {name: identity.digest for name, identity in self._libs_identities(nodes).items()}

    def _libs_identities(self, nodes: Iterable[_Node]) -> dict[str, _LibsIdentity]:
        """Compute per-node digests and keep L3 reusability outside key material."""
        selected = tuple(nodes)
        snapshot = _capture_libs_source_snapshot(
            self.config.source_paths,
            project_root=self.config.project_root,
        )
        all_files_hash = _all_libs_hash(snapshot.entries, snapshot.entry_identities)
        analyzer = _StaticLibsAnalyzer(
            self.config.project_root,
            self.config.source_paths,
            snapshot,
        )
        identities_by_function: dict[int, _LibsIdentity] = {}
        identities: dict[str, _LibsIdentity] = {}
        for node in selected:
            function_identity = id(node.function)
            identity = identities_by_function.get(function_identity)
            if identity is None:
                identity = (
                    analyzer.fallback_identity_for(node.function, all_files_hash)
                    if snapshot.has_syntax_error
                    else analyzer.identity_for(node.function, all_files_hash)
                )
                identities_by_function[function_identity] = identity
            identities[node.name] = identity
        return identities

    def _prompt_path(self, template_name: str) -> Path:
        return self.config.prompts_path / f"{template_name}.md"

    def _approval_path(self, run_id: str, name: str) -> Path:
        return store.checkpoint_path(store.runs_root(self.config.artifacts_path), run_id, name)


GRAPH_COMMAND_HELP: dict[str, str] = {
    "check": "validate declarations, declared files, guards and docstrings",
    "plan": "show which nodes would recompute (and cost money) on the next run",
    "explain": "explain why one node or map/scan item hit or missed the cache",
    "describe": "describe the registered graph: nodes, edges, models, prompts",
    "graph": "render the graph shape as Mermaid or standalone HTML",
    "profile": "emit the canonical workflow IR for the graph or a past run",
    "resume": "continue a run that stopped for retry or approval",
    "retry-resolve": "rule on an ambiguous retry attempt",
    "recover": "record an explicit decision for a terminal failed run",
}
"""One line per graph command, shared by both entry points' help output."""


def _build_cli_parser() -> argparse.ArgumentParser:
    """Build the stdlib parser shared by every ``Dag.cli`` invocation."""
    parser = argparse.ArgumentParser(prog="dag")
    commands = parser.add_subparsers(dest="command", required=True)
    register_graph_commands(commands)
    return parser


GRAPH_ARG_HELP = (
    "pass one key=value to the dag_entry factory; repeat per parameter. "
    "Use it when the graph's shape or params depend on runtime input, so that "
    "plan/explain/check report the same graph a real run would build"
)


def register_graph_commands(
    commands: argparse._SubParsersAction,
    *,
    graph_args: bool = False,
) -> None:
    """Register the graph subcommands onto an existing subparser action.

    The ``dag`` and ``kigumi`` parsers both call this, so a flag added here reaches
    both entry points and cannot drift between them.

    ``graph_args`` adds ``--graph-arg`` to every command. Only ``kigumi`` sets it:
    that entry point constructs the graph from ``dag_entry``, so it is the only one
    that can forward arguments. ``Dag.cli`` is handed an already-built graph, where
    the flag could not do anything.
    """

    def add(name: str) -> argparse.ArgumentParser:
        parser = commands.add_parser(name, help=GRAPH_COMMAND_HELP[name])
        if graph_args:
            parser.add_argument(
                "--graph-arg",
                action="append",
                default=[],
                metavar="KEY=VALUE",
                help=GRAPH_ARG_HELP,
            )
        return parser

    add("check")

    plan = add("plan")
    plan.add_argument("--targets")

    graph = add("graph")
    graph.add_argument("--html")
    graph.add_argument("--run-id")
    graph.add_argument("--prompts", action="store_true")

    profile = add("profile")
    profile.add_argument("--run-id")
    profile.add_argument("--format", choices=("json", "md"), default="md")
    profile.add_argument("--include-content", action="store_true")

    explain = add("explain")
    explain.add_argument("node_name")
    explain.add_argument("--run-id")

    describe = add("describe")
    describe.add_argument("--format", choices=("md", "json"), default="md")

    resume = add("resume")
    resume.add_argument("run_id")
    resume.add_argument("--workers", type=int, default=1)

    resolve = add("retry-resolve")
    resolve.add_argument("run_id")
    resolve.add_argument("target")
    resolve.add_argument("--attempt", type=int, required=True)
    resolve.add_argument("--action", choices=("retry", "fail"), required=True)
    resolve.add_argument("--reason", required=True)

    recover = add("recover")
    recover.add_argument("run_id")
    recover.add_argument("target")
    recover.add_argument("--attempt", type=int, required=True)
    recover.add_argument(
        "--decision",
        choices=("retry_not_started", "retry_after_external_check", "fail"),
        required=True,
    )
    recover.add_argument("--reason", required=True)
    recover.add_argument("--evidence", action="append", default=[], metavar="REF")


def _cli_display_path(root: Path, path: Path) -> str:
    """Render source locations project-relative when possible."""
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _cli_print_names(label: str, names: list[str]) -> None:
    """Print one stable list section, including an explicit empty result."""
    print(f"{label}:")
    if names:
        for name in names:
            print(f"  {name}")
    else:
        print("  (none)")


def _locator_description(locator: tuple[str, str] | None) -> dict[str, str] | None:
    """将节点与 artifact 路径拆开，避免图描述依赖含糊的拼接字符串。"""
    if locator is None:
        return None
    return {"node": locator[0], "path": locator[1]}


def _function_doc(function: Callable[..., Any]) -> str | None:
    """读取注册函数清理后的首行 docstring，缺席时保持为空。"""
    doc = inspect.getdoc(function)
    if doc is None:
        return None
    first_line = doc.splitlines()[0].strip()
    return first_line or None


def _describe_models(nodes: Iterable[_Node]) -> dict[str, list[dict[str, str | None]]]:
    """汇总 AST 已确认模型的字段说明，按模型名稳定排序。"""
    models: dict[str, type[pydantic.BaseModel]] = {}
    for node in nodes:
        for model in node.model_classes:
            models.setdefault(model.__name__, model)
    return {
        name: [
            {
                "name": field_name,
                "type": _annotation_string(field.annotation),
                "description": field.description,
            }
            for field_name, field in model.model_fields.items()
        ]
        for name, model in sorted(models.items())
    }


def _read_run_metadata(run_directory: Path) -> dict[str, dict[str, Any]]:
    """读取可用 sidecar；损坏 sidecar 不足以让纯渲染失败。"""
    metadata: dict[str, dict[str, Any]] = {}
    for sidecar in run_directory.glob("*.json.meta.json"):
        try:
            with sidecar.open(encoding="utf-8") as handle:
                candidate = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(candidate, dict) and isinstance(candidate.get("node"), str):
            metadata[candidate["node"]] = candidate
    return metadata


def _validate_persisted_prompt_lineage(
    metadata: Mapping[str, Any],
    label: str,
) -> None:
    """Fail closed when a resumable sidecar contains internally corrupt lineage."""

    def validate_resolution(value: Any, context: str) -> None:
        if value is None:
            return
        try:
            validate_prompt_resolution_record(value)
        except PromptResolutionError as error:
            raise RunManifestError(
                f"Completed target {label!r} has invalid {context}: {error}"
            ) from error

    def validate_resolutions(value: Any, context: str) -> None:
        if not isinstance(value, Mapping):
            raise RunManifestError(f"Completed target {label!r} has invalid {context}")
        for resolution in value.values():
            validate_resolution(resolution, context)

    def validate_calls(value: Any, context: str) -> None:
        if not isinstance(value, list) or not all(isinstance(call, Mapping) for call in value):
            raise RunManifestError(f"Completed target {label!r} has invalid {context}")
        for call in value:
            validate_resolution(call.get("prompt_resolution"), context)

    validate_resolutions(metadata.get("prompt_resolutions"), "current Prompt resolutions")
    validate_calls(metadata.get("calls"), "current CALL lineage")
    origin = metadata.get("origin_provenance")
    if not isinstance(origin, Mapping):
        raise RunManifestError(f"Completed target {label!r} has invalid origin provenance")
    validate_resolutions(origin.get("prompt_resolutions"), "origin Prompt resolutions")
    validate_calls(origin.get("calls"), "origin CALL lineage")
    agent = origin.get("agent")
    if isinstance(agent, Mapping):
        validate_resolution(agent.get("prompt_resolution"), "origin Agent lineage")


def _pending_checkpoint_belongs_to_node(
    node: _Node,
    checkpoint: str,
    pending_name: str,
) -> bool:
    """Match persisted checkpoint names to their exact node declaration."""
    if node.subgraph is not None:
        prefix = f"{checkpoint}@{node.name}"
        return pending_name == prefix or pending_name.startswith(f"{prefix}@")
    return pending_name == checkpoint or pending_name.startswith(f"{checkpoint}@")


def _downstream_nodes(nodes: Mapping[str, _Node], roots: set[str]) -> set[str]:
    """找到因本次已知检查点挂起而未执行的直接或传递下游。"""
    skipped: set[str] = set()
    frontier = set(roots)
    while frontier:
        frontier = {
            name
            for name, node in nodes.items()
            if name not in roots | skipped
            and any(dependency in frontier for dependency in node.deps)
        }
        skipped.update(frontier)
    return skipped


def _recovery_time() -> str:
    """Return a sortable UTC recovery timestamp in the public receipt shape."""
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _recovered_by() -> str:
    """Return an operator identity without adding it to run or cache identity."""
    for name in ("KIGUMI_RECOVERED_BY", "USER"):
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    try:
        return getpass.getuser()
    except OSError:
        return "unknown"


def _recovery_payload(receipt: RecoveryReceipt) -> dict[str, Any]:
    """Project a recovery dataclass to its canonical persisted JSON shape."""
    return {
        "recovery_time": receipt.recovery_time,
        "from_attempt": receipt.from_attempt,
        "to_attempt": receipt.to_attempt,
        "decision": receipt.decision,
        "reason": receipt.reason,
        "evidence_refs": list(receipt.evidence_refs),
        "recovered_by": receipt.recovered_by,
    }


def _recovery_decision_exists(run_dir: Path, from_attempt: int) -> bool:
    """Return whether this failed attempt already has an append-only decision."""
    for path in sorted(run_dir.glob("recovery-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"Recovery receipt {path.name!r} is not valid JSON") from error
        if not isinstance(payload, dict):
            raise ValueError(f"Recovery receipt {path.name!r} is not a JSON object")
        if payload.get("from_attempt") == from_attempt:
            return True
    return False


class _DocstringStripper(ast.NodeTransformer):
    def _strip(self, node: ast.AST) -> ast.AST:
        body = getattr(node, "body", None)
        if isinstance(body, list) and body and _is_docstring(body[0]):
            del body[0]
        return self.generic_visit(node)

    def visit_Module(self, node: ast.Module) -> ast.AST:
        return self._strip(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.AST:
        return self._strip(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AST:
        return self._strip(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> ast.AST:
        return self._strip(node)


def _is_docstring(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    )


def _module_code_text(text: str) -> str:
    """把模块源码归一化为纯代码事实，与节点 source 哈希同一粒度。

    注释与 docstring 不入 libs 键；语法暂时残破的文件退回原文——读不懂的
    内容本身就是输入事实，只读的 plan/explain 不该因此崩溃。
    """
    try:
        parsed = ast.parse(text)
    except SyntaxError:
        return text
    normalized = _DocstringStripper().visit(parsed)
    return ast.dump(normalized, annotate_fields=True, include_attributes=False)


_MODULE_IDENTITY_NAMES = frozenset(
    {"__name__", "__package__", "__file__", "__spec__", "__loader__"}
)
_MODULE_IDENTITY_REFLECTION_NAMES = frozenset(
    {"compile", "eval", "exec", "globals", "locals", "vars"}
)
_MODULE_IDENTITY_LOOKUP_CALLS = frozenset({"__getattribute__", "getattr"})
_MODULE_IDENTITY_FUNCTION_ATTRIBUTES = frozenset({"__globals__", "__module__"})
_MODULE_IDENTITY_REFLECTION_CALLABLES = tuple(
    getattr(builtins, name) for name in _MODULE_IDENTITY_REFLECTION_NAMES
)
_MODULE_IDENTITY_LOOKUP_CALLABLES = (builtins.getattr, object.__getattribute__)
_UNRESOLVED_RUNTIME_VALUE = object()
_SAFE_FUNCTION_RUNTIME_ATTRIBUTES = frozenset(
    {
        "__code__",
        "__defaults__",
        "__globals__",
        "__kwdefaults__",
        "__module__",
        "__name__",
        "__qualname__",
        "__wrapped__",
    }
)
_SAFE_PARTIAL_RUNTIME_ATTRIBUTES = frozenset({"args", "func", "keywords"})


def _type_has_base(value: Any, base: type[Any]) -> bool:
    """Check a runtime type hierarchy without consulting ``value.__class__``."""
    value_type = type(value)
    if value_type is base:
        return True
    raw_mro = _safe_type_attribute(value_type, "__mro__")
    if type(raw_mro) is not tuple:
        return False
    return any(item is base for item in raw_mro)


def _is_function(value: Any) -> bool:
    return type(value) is types.FunctionType


def _is_method(value: Any) -> bool:
    return type(value) is types.MethodType


def _is_code(value: Any) -> bool:
    return type(value) is types.CodeType


def _is_module(value: Any) -> bool:
    return type(value) is types.ModuleType


def _safe_sys_modules() -> dict[str, Any] | None:
    """Return the process registry only when it is the exact built-in dict."""
    registry = sys.modules
    return registry if type(registry) is dict else None


def _is_class(value: Any) -> bool:
    value_type = type(value)
    if value_type is type:
        return True
    raw_mro = _safe_type_attribute(value_type, "__mro__")
    if type(raw_mro) is not tuple:
        return False
    return any(item is type for item in raw_mro)


def _is_builtin(value: Any) -> bool:
    value_type = type(value)
    return value_type is types.BuiltinFunctionType or value_type is types.BuiltinMethodType


def _safe_type_attribute(value: type[Any], name: str) -> Any:
    """Read an exact ``type`` slot descriptor without metaclass dispatch.

    Both descriptor kinds are exact C-level slot readers taken from
    ``type.__dict__``, so neither can route through user code. Accepting
    ``member_descriptor`` as well as ``getset_descriptor`` is required because
    ``type.__mro__`` is a ``member_descriptor`` on Python 3.11 and a
    ``getset_descriptor`` from 3.12 onward; rejecting it made every runtime
    state capture fail closed on 3.11 and silently lost cache reuse.
    """
    descriptor = type.__dict__.get(name)
    if type(descriptor) not in (types.GetSetDescriptorType, types.MemberDescriptorType):
        return _UNRESOLVED_RUNTIME_VALUE
    try:
        return descriptor.__get__(value, type(value))
    except (AttributeError, TypeError):
        return _UNRESOLVED_RUNTIME_VALUE


def _safe_type_mro(value: type[Any]) -> tuple[type[Any], ...] | None:
    raw_mro = _safe_type_attribute(value, "__mro__")
    if type(raw_mro) is not tuple or len(raw_mro) > 4096:
        return None
    for item in raw_mro:
        if not isinstance(item, type):
            return None
    return raw_mro


def _runtime_type_identity(value_type: type[Any]) -> dict[str, str]:
    """Return a JSON-safe identity for a runtime type without metaclass dispatch."""
    namespace = _safe_class_namespace(value_type)
    module_name = namespace.get("__module__") if namespace is not None else None
    qualname = namespace.get("__qualname__") if namespace is not None else None
    if not isinstance(module_name, str):
        module_name = _safe_type_attribute(value_type, "__module__")
    if not isinstance(qualname, str):
        qualname = _safe_type_attribute(value_type, "__qualname__")
    return {
        "module": module_name if isinstance(module_name, str) else "",
        "qualname": qualname if isinstance(qualname, str) else "",
    }


def _runtime_type_state_identity(value_type: type[Any]) -> dict[str, str]:
    """Identify a state shape without binding an otherwise unobservable module alias."""
    identity = _runtime_type_identity(value_type)
    return {"qualname": identity["qualname"]}


def _function_globals(function: Any) -> dict[str, Any] | None:
    """Return a real function's globals without invoking user attributes."""
    if _is_function(function):
        globals_map = function.__globals__
        return globals_map if type(globals_map) is dict else None
    if _is_method(function):
        globals_map = function.__func__.__globals__
        return globals_map if type(globals_map) is dict else None
    return None


def _function_code(function: Any) -> types.CodeType | None:
    """Return a real function or bound method's code object directly."""
    if _is_function(function):
        return function.__code__
    if _is_method(function):
        return function.__func__.__code__
    return None


def _safe_class_namespace(value: Any) -> Mapping[str, Any] | None:
    """Read a class namespace through the exact ``type`` getset descriptor."""
    if not _is_class(value):
        return None
    namespace = _safe_type_attribute(value, "__dict__")
    return namespace if type(namespace) is types.MappingProxyType else None


def _safe_module_namespace(value: Any) -> dict[str, Any] | None:
    """Read a module dictionary through the built-in module implementation."""
    if not _is_module(value):
        return None
    try:
        namespace = types.ModuleType.__getattribute__(value, "__dict__")
    except (AttributeError, TypeError):
        return None
    return namespace if type(namespace) is dict else None


def _safe_instance_dict(value: Any) -> dict[str, Any] | None:
    """Read an ordinary instance dictionary through its built-in getset descriptor."""
    value_type = type(value)
    raw_mro = _safe_type_mro(value_type)
    if raw_mro is None:
        return None
    for owner in raw_mro:
        namespace = _safe_class_namespace(owner)
        static_dict = namespace.get("__dict__") if namespace is not None else None
        if type(static_dict) is not types.GetSetDescriptorType:
            continue
        try:
            instance_dict = static_dict.__get__(value, value_type)
        except (AttributeError, TypeError):
            return None
        return instance_dict if type(instance_dict) is dict else None
    return None


def _safe_partial_attribute(value: Any, name: str) -> Any:
    """Read a partial's built-in member descriptor without subclass hooks."""
    if not _type_has_base(value, functools.partial) or name not in _SAFE_PARTIAL_RUNTIME_ATTRIBUTES:
        return _UNRESOLVED_RUNTIME_VALUE
    descriptor = functools.partial.__dict__.get(name)
    if type(descriptor) is not types.MemberDescriptorType:
        return _UNRESOLVED_RUNTIME_VALUE
    try:
        return descriptor.__get__(value, type(value))
    except (AttributeError, TypeError):
        return _UNRESOLVED_RUNTIME_VALUE


def _exact_descriptor_function(value: Any) -> tuple[str, Any] | None:
    """Read only the two exact CPython function descriptors."""
    value_type = type(value)
    if value_type is classmethod:
        return "classmethod", value.__func__
    if value_type is staticmethod:
        return "staticmethod", value.__func__
    return None


def _has_static_descriptor_protocol(value: Any) -> bool:
    """Inspect descriptor hooks across the complete static type MRO."""
    value_type = type(value)
    raw_mro = _safe_type_mro(value_type)
    if raw_mro is None:
        return True
    for owner in raw_mro:
        namespace = _safe_class_namespace(owner)
        if namespace is None:
            return True
        if "__get__" in namespace or "__set__" in namespace or "__delete__" in namespace:
            return True
    return False


def _is_builtin_descriptor(value: Any) -> bool:
    """Return whether a member is an exact built-in descriptor we can skip."""
    value_type = type(value)
    return (
        value_type is types.MemberDescriptorType
        or value_type is types.GetSetDescriptorType
        or value_type is types.WrapperDescriptorType
        or value_type is types.MethodDescriptorType
        or value_type is types.ClassMethodDescriptorType
    )


def _safe_runtime_attribute(value: Any, name: str) -> Any:
    """Read only known built-in attributes or static data, never descriptors."""
    if _is_function(value):
        if name in _SAFE_FUNCTION_RUNTIME_ATTRIBUTES:
            return getattr(value, name, _UNRESOLVED_RUNTIME_VALUE)
        return _UNRESOLVED_RUNTIME_VALUE
    if _is_method(value):
        if name == "__func__":
            return value.__func__
        if name == "__self__":
            return value.__self__
        if name in {"__module__", "__name__", "__qualname__", "__wrapped__"}:
            return _safe_runtime_attribute(value.__func__, name)
        return _UNRESOLVED_RUNTIME_VALUE
    if _is_code(value):
        if name.startswith("co_"):
            return getattr(value, name, _UNRESOLVED_RUNTIME_VALUE)
        return _UNRESOLVED_RUNTIME_VALUE
    if _is_builtin(value):
        if name in {"__module__", "__name__", "__qualname__"}:
            return getattr(value, name, _UNRESOLVED_RUNTIME_VALUE)
        return _UNRESOLVED_RUNTIME_VALUE
    if _type_has_base(value, functools.partial):
        return _safe_partial_attribute(value, name)
    if _is_module(value):
        namespace = _safe_module_namespace(value)
        if namespace is not None:
            if name in namespace:
                return namespace[name]
            # A module-level __getattr__ can synthesize an otherwise absent
            # attribute.  Do not execute it, and keep that uncertainty visible
            # to callers that validate loaded runtime provenance.
            if "__getattr__" in namespace:
                return _UNRESOLVED_RUNTIME_VALUE
            return None
        return _UNRESOLVED_RUNTIME_VALUE
    if _is_class(value):
        namespace = _safe_class_namespace(value)
        if namespace is not None and name in namespace:
            return namespace[name]
        if name in {"__module__", "__name__", "__qualname__"}:
            return _safe_type_attribute(value, name)
        return _UNRESOLVED_RUNTIME_VALUE
    if name == "__dict__":
        instance_dict = _safe_instance_dict(value)
        if instance_dict is not None:
            return instance_dict
        return _UNRESOLVED_RUNTIME_VALUE

    return _UNRESOLVED_RUNTIME_VALUE


def _safe_runtime_subscript(value: Any, key: Any) -> Any:
    """Resolve exact containers only when lookup cannot call user equality/hash hooks."""
    try:
        if type(value) is dict and type(key) is str:
            return value[key]
        value_type = type(value)
        if (
            value_type is tuple or value_type is list or value_type is str or value_type is bytes
        ) and type(key) is int:
            return value[key]
    except (KeyError, IndexError, TypeError):
        return _UNRESOLVED_RUNTIME_VALUE
    return _UNRESOLVED_RUNTIME_VALUE


def _safe_runtime_path_entries(value: Any) -> tuple[str, ...] | object:
    """Copy exact string path entries without invoking ``__fspath__`` or truthiness."""
    value_type = type(value)
    if value_type is list or value_type is tuple:
        if len(value) > 4096:
            return _UNRESOLVED_RUNTIME_VALUE
        entries: list[str] = []
        for item in value:
            if type(item) is not str:
                return _UNRESOLVED_RUNTIME_VALUE
            entries.append(item)
        return tuple(entries)
    return _UNRESOLVED_RUNTIME_VALUE


def _ast_bound_names(node: ast.AST) -> set[str]:
    """Return simple comprehension/assignment target names."""
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, (ast.Tuple, ast.List)):
        names: set[str] = set()
        for element in node.elts:
            names.update(_ast_bound_names(element))
        return names
    if isinstance(node, ast.Starred):
        return _ast_bound_names(node.value)
    return set()


def _function_scope_bindings(tree: ast.AST) -> tuple[set[str], set[str]]:
    """Return local and explicit-global names for one function source tree."""
    root: ast.FunctionDef | ast.AsyncFunctionDef | None
    if isinstance(tree, (ast.FunctionDef, ast.AsyncFunctionDef)):
        root = tree
    else:
        root = (
            next(
                (
                    node
                    for node in tree.body
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                ),
                None,
            )
            if isinstance(tree, ast.Module)
            else None
        )
    if root is None:
        return set(), set()

    local_names = {
        argument.arg
        for argument in (
            *root.args.posonlyargs,
            *root.args.args,
            *root.args.kwonlyargs,
        )
    }
    if root.args.vararg is not None:
        local_names.add(root.args.vararg.arg)
    if root.args.kwarg is not None:
        local_names.add(root.args.kwarg.arg)
    global_names: set[str] = set()

    class BindingVisitor(ast.NodeVisitor):
        """Collect bindings in the function scope, not nested scopes."""

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            if node is root:
                self._visit_body(node.body)
            else:
                local_names.add(node.name)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            if node is root:
                self._visit_body(node.body)
            else:
                local_names.add(node.name)

        def visit_Lambda(self, node: ast.Lambda) -> None:
            del node

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            local_names.add(node.name)

        def visit_Global(self, node: ast.Global) -> None:
            global_names.update(node.names)

        def visit_Nonlocal(self, node: ast.Nonlocal) -> None:
            for name in node.names:
                local_names.discard(name)

        def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
            if isinstance(node.name, str):
                local_names.add(node.name)
            self.generic_visit(node)

        def visit_Import(self, node: ast.Import) -> None:
            for alias in node.names:
                local_names.add(alias.asname or alias.name.split(".", 1)[0])

        def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
            for alias in node.names:
                if alias.name != "*":
                    local_names.add(alias.asname or alias.name)

        def _visit_comprehension(
            self, generators: list[ast.comprehension], tail: Iterable[ast.AST]
        ) -> None:
            for generator in generators:
                self.visit(generator.iter)
                for condition in generator.ifs:
                    self.visit(condition)
            for expression in tail:
                self.visit(expression)

        def visit_ListComp(self, node: ast.ListComp) -> None:
            self._visit_comprehension(node.generators, (node.elt,))

        def visit_SetComp(self, node: ast.SetComp) -> None:
            self._visit_comprehension(node.generators, (node.elt,))

        def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
            self._visit_comprehension(node.generators, (node.elt,))

        def visit_DictComp(self, node: ast.DictComp) -> None:
            self._visit_comprehension(node.generators, (node.key, node.value))

        def visit_Name(self, node: ast.Name) -> None:
            if isinstance(node.ctx, (ast.Store, ast.Del)):
                local_names.add(node.id)

        def _visit_body(self, body: Iterable[ast.stmt]) -> None:
            for statement in body:
                self.visit(statement)

    BindingVisitor().visit(root)
    local_names.difference_update(global_names)
    return local_names, global_names


def _function_global_value(function: Callable[..., Any], name: str) -> Any:
    """Resolve one function closure/global without invoking user mapping hooks."""
    target = function.__func__ if _is_method(function) else function
    if _is_function(target):
        closure = target.__closure__ or ()
        for free_name, cell in zip(target.__code__.co_freevars, closure, strict=True):
            if free_name != name:
                continue
            try:
                return cell.cell_contents
            except ValueError:
                return _UNRESOLVED_RUNTIME_VALUE
    globals_map = _function_globals(function)
    if type(globals_map) is dict and name in globals_map:
        return globals_map[name]
    return builtins.__dict__.get(name, _UNRESOLVED_RUNTIME_VALUE)


def _unwrap_reflection_callable(value: Any) -> Any:
    """Unwrap only a partial when identifying a builtin reflection alias."""
    if not _type_has_base(value, functools.partial):
        return value
    return _safe_partial_attribute(value, "func")


def _is_builtin_reflection_callable(value: Any) -> bool:
    """Return whether a runtime value is one of the supported builtin reflectors."""
    unwrapped = _unwrap_reflection_callable(value)
    return any(unwrapped is primitive for primitive in _MODULE_IDENTITY_REFLECTION_CALLABLES)


def _is_builtin_lookup_callable(value: Any) -> bool:
    """Return whether a runtime value is the builtin dynamic attribute lookup."""
    unwrapped = _unwrap_reflection_callable(value)
    return any(unwrapped is primitive for primitive in _MODULE_IDENTITY_LOOKUP_CALLABLES)


def _lookup_callable_bound_args(value: Any) -> tuple[Any, ...]:
    """Return safely bound positional arguments from nested partial wrappers."""
    bound: list[Any] = []
    current = value
    seen: set[int] = set()
    while _type_has_base(current, functools.partial):
        if len(seen) >= _RUNTIME_STATE_MAX_NODES:
            return ()
        identity = id(current)
        if identity in seen:
            return ()
        seen.add(identity)
        raw_args = _safe_partial_attribute(current, "args")
        target = _safe_partial_attribute(current, "func")
        if type(raw_args) is not tuple or target is _UNRESOLVED_RUNTIME_VALUE:
            return ()
        if len(bound) + len(raw_args) > _RUNTIME_PROVENANCE_MAX_MEMBERS:
            return ()
        bound.extend(raw_args)
        current = target
    return tuple(bound)


def _lookup_accesses_function_identity(
    function: Callable[..., Any],
    receiver: Any,
    node: ast.Call,
    bound_args: tuple[Any, ...],
    local_names: set[str],
) -> bool:
    """Recognize safe getattr/object.__getattribute__ access on the node function."""
    if not _registered_function_value(function, receiver):
        return False
    offset = 1 if not bound_args else 0
    if bound_args and len(bound_args) > 1:
        attribute = bound_args[1]
    elif len(node.args) > offset:
        attribute = _resolve_function_runtime_value(node.args[offset], function, local_names)
    else:
        return True
    if attribute is _UNRESOLVED_RUNTIME_VALUE:
        return True
    return type(attribute) is not str or attribute in _MODULE_IDENTITY_FUNCTION_ATTRIBUTES


def _resolve_function_runtime_value(
    node: ast.AST,
    function: Callable[..., Any] | None,
    local_names: set[str],
) -> Any:
    """Resolve simple runtime receivers without executing arbitrary user code."""
    if function is None:
        return _UNRESOLVED_RUNTIME_VALUE
    if isinstance(node, ast.Name):
        if not isinstance(node.ctx, ast.Load) or node.id in local_names:
            return _UNRESOLVED_RUNTIME_VALUE
        return _function_global_value(function, node.id)
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Attribute):
        base = _resolve_function_runtime_value(node.value, function, local_names)
        if base is _UNRESOLVED_RUNTIME_VALUE:
            return base
        return _safe_runtime_attribute(base, node.attr)
    if isinstance(node, ast.Subscript):
        base = _resolve_function_runtime_value(node.value, function, local_names)
        if base is _UNRESOLVED_RUNTIME_VALUE:
            return base
        key_node: ast.AST = node.slice
        key = _resolve_function_runtime_value(key_node, function, local_names)
        if key is _UNRESOLVED_RUNTIME_VALUE:
            return _UNRESOLVED_RUNTIME_VALUE
        return _safe_runtime_subscript(base, key)
    return _UNRESOLVED_RUNTIME_VALUE


def _owner_module_value(function: Callable[..., Any], value: Any) -> bool:
    """Return whether a runtime value is this function's actual module namespace."""
    globals_map = _function_globals(function)
    if value is globals_map:
        return True
    facts = _function_owner_facts(function)
    if not facts.consistent or facts.module_name is None:
        return False
    if not _is_module(value):
        return False
    value_name = _safe_runtime_attribute(value, "__name__")
    if value_name != facts.module_name:
        return False
    module_file = _safe_runtime_attribute(value, "__file__")
    if module_file is _UNRESOLVED_RUNTIME_VALUE:
        module_file = None
    if module_file is None or facts.module_path is None:
        return module_file is None and facts.module_path is None
    if type(module_file) is not str:
        return False
    try:
        return Path(module_file).resolve() == facts.module_path
    except (OSError, RuntimeError):
        return False


def _registered_function_value(function: Callable[..., Any], value: Any) -> bool:
    """Return whether a receiver resolves to the registered function itself."""
    return value is function


@dataclass
class _CallGraphBudget:
    """Bound recursive Python-callable owner analysis."""

    seen: set[int]
    nodes: int = 0
    max_nodes: int = 256
    max_depth: int = 64
    overflow: bool = False

    def enter(self, function: Any, depth: int) -> bool:
        if self.overflow:
            return False
        if depth > self.max_depth or self.nodes >= self.max_nodes:
            self.overflow = True
            return False
        identity = id(function)
        if identity in self.seen:
            return False
        self.seen.add(identity)
        self.nodes += 1
        return True


def _reached_nested_callable_nodes(
    root_node: ast.AST | None,
) -> tuple[set[int], bool]:
    """Resolve the small, statically visible local-call shapes we can prove."""
    if root_node is None:
        return set(), False
    functions: dict[str, ast.AST] = {}
    classes: dict[str, ast.ClassDef] = {}
    members: dict[tuple[str, str], ast.AST] = {}
    aliases: dict[str, str] = {}
    calls: set[str] = set()
    member_calls: set[tuple[str, str]] = set()
    visited = 0
    overflow = False

    def resolve_name(name: str) -> str:
        seen: set[str] = set()
        current = name
        while current in aliases and current not in seen:
            seen.add(current)
            current = aliases[current]
        return current

    class Discovery(ast.NodeVisitor):
        def _claim(self) -> bool:
            nonlocal visited, overflow
            visited += 1
            if visited > _RUNTIME_STATE_MAX_NODES:
                overflow = True
                return False
            return True

        def generic_visit(self, node: ast.AST) -> None:
            # Custom visitors below claim their semantic node and then call
            # generic_visit; claiming here also bounds otherwise uninteresting
            # expression/constant subtrees.
            if not self._claim():
                return
            super().generic_visit(node)

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            if not self._claim():
                return
            if node is root_node:
                for statement in node.body:
                    self.visit(statement)
            else:
                functions.setdefault(node.name, node)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            if not self._claim():
                return
            if node is root_node:
                for statement in node.body:
                    self.visit(statement)
            else:
                functions.setdefault(node.name, node)

        def visit_Lambda(self, node: ast.Lambda) -> None:
            if not self._claim():
                return
            if node is root_node:
                self.visit(node.body)

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            if not self._claim():
                return
            classes.setdefault(node.name, node)
            for statement in node.body:
                if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    members.setdefault((node.name, statement.name), statement)
                else:
                    self.visit(statement)

        def visit_Assign(self, node: ast.Assign) -> None:
            if not self._claim():
                return
            value = node.value
            source_name: str | None = value.id if isinstance(value, ast.Name) else None
            for target in node.targets:
                if not isinstance(target, ast.Name):
                    continue
                if source_name is not None:
                    aliases[target.id] = source_name
                elif isinstance(value, ast.Lambda):
                    functions[target.id] = value
                elif isinstance(value, ast.Name) and value.id in classes:
                    aliases[target.id] = value.id
            if not isinstance(value, ast.Lambda):
                self.generic_visit(node)

        def visit_Call(self, node: ast.Call) -> None:
            if not self._claim():
                return
            if isinstance(node.func, ast.Name):
                calls.add(resolve_name(node.func.id))
            elif isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
                member_calls.add((resolve_name(node.func.value.id), node.func.attr))
            self.generic_visit(node)

        def visit_callable_body(self, node: ast.AST) -> None:
            """Scan only a callable already proven to be reached by a call."""
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for statement in node.body:
                    self.visit(statement)
            elif isinstance(node, ast.Lambda):
                self.visit(node.body)

    try:
        discovery = Discovery()
        discovery.visit(root_node)
    except RecursionError:
        return set(), True

    reached: set[int] = set()
    expanded: set[int] = set()
    while not overflow:
        pending: list[ast.AST] = []
        for name in tuple(calls):
            target = functions.get(resolve_name(name))
            if target is not None:
                target_id = id(target)
                if target_id not in reached:
                    reached.add(target_id)
                if target_id not in expanded:
                    pending.append(target)
        for class_name, member_name in tuple(member_calls):
            target = members.get((resolve_name(class_name), member_name))
            if target is not None:
                target_id = id(target)
                if target_id not in reached:
                    reached.add(target_id)
                if target_id not in expanded:
                    pending.append(target)
        if not pending:
            break
        for target in pending:
            target_id = id(target)
            if target_id in expanded:
                continue
            expanded.add(target_id)
            try:
                discovery.visit_callable_body(target)
            except RecursionError:
                return set(), True
            if overflow:
                break
    return reached, overflow


def _tree_uses_module_identity(
    tree: ast.AST,
    function: Callable[..., Any] | None = None,
    seen_functions: set[int] | None = None,
    call_budget: _CallGraphBudget | None = None,
    call_depth: int = 0,
) -> bool:
    """Return whether executable source can observe its owner identity.

    Name load/store context and the registered function's runtime globals are both
    required here.  Merely seeing an attribute called ``globals`` on an unrelated
    object is not evidence that the node observes its owner module.
    """
    normalized = _DocstringStripper().visit(copy.deepcopy(tree))
    local_names, _global_names = _function_scope_bindings(normalized)

    def resolve(node: ast.AST) -> Any:
        return _resolve_function_runtime_value(node, function, local_names)

    def is_owner_receiver(node: ast.AST) -> bool:
        return function is not None and _owner_module_value(function, resolve(node))

    def is_function_receiver(node: ast.AST) -> bool:
        return function is not None and _registered_function_value(function, resolve(node))

    root_node: ast.AST | None = (
        normalized
        if isinstance(normalized, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda))
        else next(
            (
                node
                for node in getattr(normalized, "body", ())
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            ),
            None,
        )
    )
    if root_node is None and function is not None:
        code = _function_code(function)
        if _is_code(code) and code.co_name == "<lambda>":
            root_node = next(
                (node for node in ast.walk(normalized) if isinstance(node, ast.Lambda)),
                None,
            )

    reached_nodes, reached_overflow = _reached_nested_callable_nodes(root_node)

    class IdentityVisitor(ast.NodeVisitor):
        found = False

        def __init__(self) -> None:
            super().__init__()
            self.comprehension_names: set[str] = set()

        def _is_local(self, name: str) -> bool:
            return name in local_names or name in self.comprehension_names

        def _visit_comprehension(
            self, generators: list[ast.comprehension], tail: Iterable[ast.AST]
        ) -> None:
            previous = self.comprehension_names
            self.comprehension_names = set(previous)
            try:
                for generator in generators:
                    self.visit(generator.iter)
                    self.comprehension_names.update(_ast_bound_names(generator.target))
                    for condition in generator.ifs:
                        self.visit(condition)
                for expression in tail:
                    self.visit(expression)
            finally:
                self.comprehension_names = previous

        def visit_ListComp(self, node: ast.ListComp) -> None:
            self._visit_comprehension(node.generators, (node.elt,))

        def visit_SetComp(self, node: ast.SetComp) -> None:
            self._visit_comprehension(node.generators, (node.elt,))

        def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
            self._visit_comprehension(node.generators, (node.elt,))

        def visit_DictComp(self, node: ast.DictComp) -> None:
            self._visit_comprehension(node.generators, (node.key, node.value))

        def _visit_function_signature(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
            for decorator in node.decorator_list:
                self.visit(decorator)
            for default in (*node.args.defaults, *node.args.kw_defaults):
                if default is not None:
                    self.visit(default)
            for argument in (
                *node.args.posonlyargs,
                *node.args.args,
                *node.args.kwonlyargs,
            ):
                if argument.annotation is not None:
                    self.visit(argument.annotation)
            if node.args.vararg is not None and node.args.vararg.annotation is not None:
                self.visit(node.args.vararg.annotation)
            if node.args.kwarg is not None and node.args.kwarg.annotation is not None:
                self.visit(node.args.kwarg.annotation)
            if node.returns is not None:
                self.visit(node.returns)

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            if node is root_node or id(node) in reached_nodes:
                self.generic_visit(node)
            else:
                self._visit_function_signature(node)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            if node is root_node or id(node) in reached_nodes:
                self.generic_visit(node)
            else:
                self._visit_function_signature(node)

        def visit_Lambda(self, node: ast.Lambda) -> None:
            for default in (*node.args.defaults, *node.args.kw_defaults):
                if default is not None:
                    self.visit(default)
            if node is root_node or id(node) in reached_nodes:
                self.visit(node.body)

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            for decorator in node.decorator_list:
                self.visit(decorator)
            for base in node.bases:
                self.visit(base)
            for keyword in node.keywords:
                self.visit(keyword.value)
            for statement in node.body:
                self.visit(statement)

        def visit_Name(self, node: ast.Name) -> None:
            if self.found or not isinstance(node.ctx, ast.Load) or self._is_local(node.id):
                return
            if node.id in _MODULE_IDENTITY_NAMES:
                self.found = True
                return
            if node.id in _MODULE_IDENTITY_REFLECTION_NAMES:
                value = (
                    _function_global_value(function, node.id)
                    if function is not None
                    else _UNRESOLVED_RUNTIME_VALUE
                )
                if function is None or _is_builtin_reflection_callable(value):
                    self.found = True

        def visit_Attribute(self, node: ast.Attribute) -> None:
            if self.found:
                return
            receiver_is_owner = is_owner_receiver(node.value)
            receiver_is_function = is_function_receiver(node.value)
            receiver_is_builtins = function is not None and resolve(node.value) is builtins
            is_function_code_filename = (
                node.attr == "co_filename"
                and isinstance(node.value, ast.Attribute)
                and node.value.attr == "__code__"
                and is_function_receiver(node.value.value)
            )
            if is_function_code_filename:
                self.found = True
            elif node.attr in _MODULE_IDENTITY_FUNCTION_ATTRIBUTES:
                if receiver_is_function or function is None:
                    self.found = True
            elif node.attr in _MODULE_IDENTITY_NAMES:
                if receiver_is_owner or function is None:
                    self.found = True
            elif node.attr in _MODULE_IDENTITY_REFLECTION_NAMES:
                if receiver_is_owner or receiver_is_builtins or function is None:
                    self.found = True
            elif node.attr in {"__dict__", "__getattribute__", "__builtins__"} and (
                receiver_is_owner or function is None
            ):
                self.found = True
            self.generic_visit(node)

        def visit_Call(self, node: ast.Call) -> None:
            if self.found:
                return
            call_value = resolve(node.func)
            if _is_builtin_reflection_callable(call_value):
                self.found = True
                return
            if _is_builtin_lookup_callable(call_value):
                bound_args = _lookup_callable_bound_args(call_value)
                receiver = (
                    bound_args[0]
                    if bound_args
                    else resolve(node.args[0])
                    if node.args
                    else _UNRESOLVED_RUNTIME_VALUE
                )
                if (
                    function is None
                    or _owner_module_value(function, receiver)
                    or (
                        _lookup_accesses_function_identity(
                            function,
                            receiver,
                            node,
                            bound_args,
                            local_names,
                        )
                    )
                ):
                    self.found = True
                    return
            if isinstance(node.func, ast.Subscript) and call_value is _UNRESOLVED_RUNTIME_VALUE:
                # A dynamically selected callable can hide globals/getattr aliases.  Bind
                # owner identity rather than proving a brittle negative.
                self.found = True
                return
            if isinstance(node.func, ast.Attribute) and call_value is _UNRESOLVED_RUNTIME_VALUE:
                self.found = True
                return
            if (
                function is not None
                and (_is_function(call_value) or _is_method(call_value))
                and call_value is not function
                and _function_uses_module_identity(
                    call_value,
                    seen_functions,
                    call_budget,
                    call_depth + 1,
                )
            ):
                self.found = True
                return
            if isinstance(node.func, ast.Attribute):
                receiver = node.func.value
                receiver_is_owner = is_owner_receiver(receiver)
                receiver_is_builtins = function is not None and resolve(receiver) is builtins
                if node.func.attr in _MODULE_IDENTITY_REFLECTION_NAMES and (
                    receiver_is_owner or receiver_is_builtins or function is None
                ):
                    self.found = True
                    return
            self.generic_visit(node)

    visitor = IdentityVisitor()
    visitor.visit(normalized)
    return visitor.found or reached_overflow


def _tree_observes_all_globals(
    tree: ast.AST,
    function: Callable[..., Any],
    seen_functions: set[int] | None = None,
    call_budget: _CallGraphBudget | None = None,
    call_depth: int = 0,
) -> bool:
    """Return whether the registered function can dynamically inspect its globals."""
    normalized = _DocstringStripper().visit(copy.deepcopy(tree))
    local_names, _global_names = _function_scope_bindings(normalized)

    def resolve(node: ast.AST) -> Any:
        return _resolve_function_runtime_value(node, function, local_names)

    root_node = (
        normalized
        if isinstance(normalized, (ast.FunctionDef, ast.AsyncFunctionDef))
        else next(
            (
                node
                for node in getattr(normalized, "body", ())
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            ),
            None,
        )
    )
    reached_nodes, reached_overflow = _reached_nested_callable_nodes(root_node)

    class GlobalObservationVisitor(ast.NodeVisitor):
        found = False

        def __init__(self) -> None:
            super().__init__()
            self.comprehension_names: set[str] = set()

        def _is_local(self, name: str) -> bool:
            return name in local_names or name in self.comprehension_names

        def _visit_comprehension(
            self, generators: list[ast.comprehension], tail: Iterable[ast.AST]
        ) -> None:
            previous = self.comprehension_names
            self.comprehension_names = set(previous)
            try:
                for generator in generators:
                    self.visit(generator.iter)
                    self.comprehension_names.update(_ast_bound_names(generator.target))
                    for condition in generator.ifs:
                        self.visit(condition)
                for expression in tail:
                    self.visit(expression)
            finally:
                self.comprehension_names = previous

        def visit_ListComp(self, node: ast.ListComp) -> None:
            self._visit_comprehension(node.generators, (node.elt,))

        def visit_SetComp(self, node: ast.SetComp) -> None:
            self._visit_comprehension(node.generators, (node.elt,))

        def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
            self._visit_comprehension(node.generators, (node.elt,))

        def visit_DictComp(self, node: ast.DictComp) -> None:
            self._visit_comprehension(node.generators, (node.key, node.value))

        def _visit_function_signature(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
            for decorator in node.decorator_list:
                self.visit(decorator)
            for default in (*node.args.defaults, *node.args.kw_defaults):
                if default is not None:
                    self.visit(default)
            if node.returns is not None:
                self.visit(node.returns)

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            if node is root_node or id(node) in reached_nodes:
                self.generic_visit(node)
            else:
                self._visit_function_signature(node)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            if node is root_node or id(node) in reached_nodes:
                self.generic_visit(node)
            else:
                self._visit_function_signature(node)

        def visit_Lambda(self, node: ast.Lambda) -> None:
            for default in (*node.args.defaults, *node.args.kw_defaults):
                if default is not None:
                    self.visit(default)
            if node is root_node or id(node) in reached_nodes:
                self.visit(node.body)

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            for decorator in node.decorator_list:
                self.visit(decorator)
            for base in node.bases:
                self.visit(base)
            for keyword in node.keywords:
                self.visit(keyword.value)
            for statement in node.body:
                self.visit(statement)

        def visit_Name(self, node: ast.Name) -> None:
            if self.found or not isinstance(node.ctx, ast.Load) or self._is_local(node.id):
                return
            if node.id == "__builtins__" or (
                node.id in _MODULE_IDENTITY_REFLECTION_NAMES
                and _is_builtin_reflection_callable(resolve(node))
            ):
                self.found = True

        def visit_Attribute(self, node: ast.Attribute) -> None:
            if self.found:
                return
            receiver = resolve(node.value)
            if (node.attr == "__globals__" and _registered_function_value(function, receiver)) or (
                node.attr == "__dict__" and _owner_module_value(function, receiver)
            ):
                self.found = True
            self.generic_visit(node)

        def visit_Call(self, node: ast.Call) -> None:
            if self.found:
                return
            call_value = resolve(node.func)
            if _is_builtin_reflection_callable(call_value):
                self.found = True
                return
            if _is_builtin_lookup_callable(call_value):
                bound_args = _lookup_callable_bound_args(call_value)
                receiver = (
                    bound_args[0]
                    if bound_args
                    else resolve(node.args[0])
                    if node.args
                    else _UNRESOLVED_RUNTIME_VALUE
                )
                if _owner_module_value(function, receiver):
                    self.found = True
                    return
            if isinstance(node.func, ast.Subscript) and call_value is _UNRESOLVED_RUNTIME_VALUE:
                self.found = True
                return
            if (
                (_is_function(call_value) or _is_method(call_value))
                and call_value is not function
                and _function_observes_all_globals(
                    call_value,
                    seen_functions,
                    call_budget,
                    call_depth + 1,
                )
            ):
                self.found = True
                return
            if isinstance(node.func, ast.Attribute):
                receiver = resolve(node.func.value)
                if node.func.attr in _MODULE_IDENTITY_REFLECTION_NAMES and (
                    receiver is builtins or _owner_module_value(function, receiver)
                ):
                    self.found = True
                    return
            self.generic_visit(node)

    visitor = GlobalObservationVisitor()
    visitor.visit(normalized)
    return visitor.found or reached_overflow


def _function_uses_module_identity(
    function: Callable[..., Any],
    seen_functions: set[int] | None = None,
    call_budget: _CallGraphBudget | None = None,
    call_depth: int = 0,
) -> bool:
    """Inspect the called Python-function graph for owner identity observation."""
    if seen_functions is None:
        seen_functions = set()
    if call_budget is None:
        call_budget = _CallGraphBudget(seen_functions)
    target = function.__func__ if _is_method(function) else function
    identity = id(target)
    if identity in seen_functions:
        return False
    if not call_budget.enter(target, call_depth):
        return True
    try:
        source = textwrap.dedent(inspect.getsource(function))
        tree = ast.parse(source)
    except (OSError, SyntaxError, TypeError, ValueError, RecursionError):
        return True
    return _tree_uses_module_identity(
        tree,
        function,
        seen_functions,
        call_budget,
        call_depth,
    )


def _function_observes_all_globals(
    function: Callable[..., Any],
    seen_functions: set[int] | None = None,
    call_budget: _CallGraphBudget | None = None,
    call_depth: int = 0,
) -> bool:
    """Decide whether the called Python-function graph can inspect all globals."""
    if seen_functions is None:
        seen_functions = set()
    if call_budget is None:
        call_budget = _CallGraphBudget(seen_functions)
    target = function.__func__ if _is_method(function) else function
    identity = id(target)
    if identity in seen_functions:
        return False
    if not call_budget.enter(target, call_depth):
        return True
    try:
        source = textwrap.dedent(inspect.getsource(function))
        tree = ast.parse(source)
    except (OSError, SyntaxError, TypeError, ValueError, RecursionError):
        return True
    return _tree_observes_all_globals(
        tree,
        function,
        seen_functions,
        call_budget,
        call_depth,
    )


def _referenced_global_names(
    code: Any,
    overflow: list[bool] | None = None,
) -> set[str]:
    """Collect global names from executable code and nested callables."""
    names: set[str] = set()
    pending: list[tuple[Any, int]] = [(code, 0)]
    seen: set[int] = set()
    visited = 0
    members = 0
    while pending:
        current, depth = pending.pop()
        if not _is_code(current) or id(current) in seen:
            continue
        if depth > _RUNTIME_STATE_MAX_DEPTH or visited >= _RUNTIME_STATE_MAX_NODES:
            if overflow is not None:
                overflow[0] = True
            break
        visited += 1
        seen.add(id(current))
        for name in current.co_names:
            if members >= _RUNTIME_PROVENANCE_MAX_MEMBERS:
                if overflow is not None:
                    overflow[0] = True
                return names
            names.add(name)
            members += 1
        for constant in current.co_consts:
            if members >= _RUNTIME_PROVENANCE_MAX_MEMBERS:
                if overflow is not None:
                    overflow[0] = True
                return names
            members += 1
            if _is_code(constant):
                pending.append((constant, depth + 1))
    return names


def _code_constant_material(
    value: Any,
    memo: dict[int, str],
    depth: int,
    constant_memo: dict[int, int] | None = None,
    constant_nodes: list[int] | None = None,
    overflow: list[bool] | None = None,
) -> Any:
    """Return bounded deterministic material for constants embedded in code."""
    if depth > _RUNTIME_STATE_MAX_DEPTH:
        if overflow is not None:
            overflow[0] = True
        return {"type": "depth-limit"}
    if constant_memo is None:
        constant_memo = {}
    if constant_nodes is None:
        constant_nodes = [0]
    if _is_code(value):
        return {
            "type": "code",
            "code": _code_object_digest(
                value,
                memo,
                depth + 1,
                constant_memo=constant_memo,
                constant_nodes=constant_nodes,
                overflow=overflow,
            ),
        }
    value_type = type(value)
    if value is None or value_type is bool or value_type is int or value_type is str:
        return {"type": type(value).__name__, "value": value}
    if value_type is float:
        return {"type": "float", "bits": struct.pack(">d", value).hex()}
    if value_type is complex:
        return {
            "type": "complex",
            "bits": struct.pack(">dd", value.real, value.imag).hex(),
        }
    if value_type is bytes:
        return {
            "type": "bytes",
            "size": len(value),
            "sha256": sha256(value).hexdigest(),
        }
    if value_type is tuple or value_type is frozenset:
        identity = id(value)
        prior = constant_memo.get(identity)
        if prior is not None:
            return {"type": "reference", "ref": prior}
        if constant_nodes[0] >= _RUNTIME_STATE_MAX_NODES:
            if overflow is not None:
                overflow[0] = True
            return {"type": "node-limit"}
        reference = len(constant_memo)
        constant_memo[identity] = reference
        constant_nodes[0] += 1
        items: list[Any] = []
        for item in value:
            if constant_nodes[0] >= _RUNTIME_STATE_MAX_NODES:
                if overflow is not None:
                    overflow[0] = True
                return {"type": "node-limit"}
            constant_nodes[0] += 1
            material = _code_constant_material(
                item,
                memo,
                depth + 1,
                constant_memo,
                constant_nodes,
                overflow,
            )
            items.append(material)
            if material.get("type") == "node-limit" or material.get("type") == "depth-limit":
                return {"type": "node-limit"}
        if value_type is frozenset:
            items.sort(key=canonical_json)
        return {"type": value_type.__name__, "ref": reference, "items": items}
    return {"type": type(value).__qualname__}


def _code_constants_material(
    constants: tuple[Any, ...],
    memo: dict[int, str],
    depth: int,
    constant_memo: dict[int, int],
    constant_nodes: list[int],
    *,
    ignore_docstring: bool,
    overflow: list[bool] | None,
) -> list[Any]:
    """Materialize code constants incrementally, ignoring a code docstring."""
    material: list[Any] = []
    for index, value in enumerate(constants):
        if constant_nodes[0] >= _RUNTIME_STATE_MAX_NODES:
            if overflow is not None:
                overflow[0] = True
            material.append({"type": "node-limit"})
            break
        constant_nodes[0] += 1
        if ignore_docstring and index == 0 and type(value) is str:
            # co_consts[0] is the function/module/class docstring.  It is
            # intentionally outside executable code identity, matching
            # _module_code_text().
            material.append({"type": "docstring"})
            continue
        material.append(
            _code_constant_material(
                value,
                memo,
                depth,
                constant_memo,
                constant_nodes,
                overflow,
            )
        )
        if material[-1].get("type") in {"node-limit", "depth-limit"}:
            break
    return material


def _code_object_digest(
    code: Any,
    memo: dict[int, str] | None = None,
    depth: int = 0,
    *,
    constant_memo: dict[int, int] | None = None,
    constant_nodes: list[int] | None = None,
    overflow: list[bool] | None = None,
) -> str:
    """Hash executable code with per-walk DAG memoization and a depth bound."""
    if not _is_code(code) or depth > _RUNTIME_STATE_MAX_DEPTH:
        if overflow is not None:
            overflow[0] = True
        return ""
    if memo is None:
        memo = {}
    if constant_memo is None:
        constant_memo = {}
    if constant_nodes is None:
        constant_nodes = [0]
    identity = id(code)
    cached = memo.get(identity)
    if cached is not None:
        return cached
    # Code constants form an acyclic compiler-owned graph, but reserve the identity
    # before descending so crafted shared DAGs cannot expand exponentially.
    memo[identity] = "pending"
    exception_table = getattr(code, "co_exceptiontable", b"")
    digest = sha(
        {
            "argcount": code.co_argcount,
            "posonlyargcount": code.co_posonlyargcount,
            "kwonlyargcount": code.co_kwonlyargcount,
            "flags": code.co_flags,
            "bytecode": sha256(code.co_code).hexdigest(),
            "exception_table": sha256(exception_table).hexdigest(),
            "constants": _code_constants_material(
                code.co_consts,
                memo,
                depth + 1,
                constant_memo,
                constant_nodes,
                ignore_docstring=code.co_name != "<lambda>",
                overflow=overflow,
            ),
            "names": code.co_names,
            "varnames": code.co_varnames,
            "freevars": code.co_freevars,
            "cellvars": code.co_cellvars,
        }
    )
    if overflow is not None and overflow[0]:
        return digest
    memo[identity] = digest
    return digest


_UNREPRESENTABLE_RUNTIME_STATE = object()
_RUNTIME_STATE_MAX_NODES = 4096
_RUNTIME_STATE_MAX_DEPTH = 96
_RUNTIME_PROVENANCE_MAX_MEMBERS = 4096
_RUNTIME_STATE_STRUCTURAL_CLASS_NAMES = frozenset(
    {
        "__dict__",
        "__doc__",
        "__firstlineno__",
        "__module__",
        "__qualname__",
        "__slots__",
        "__static_attributes__",
        "__weakref__",
    }
)


@dataclass
class _RuntimeStateContext:
    """Share deterministic references and hard traversal bounds for one identity."""

    references: dict[int, int]
    nodes: int = 0
    members: int = 0
    max_nodes: int = _RUNTIME_STATE_MAX_NODES
    max_depth: int = _RUNTIME_STATE_MAX_DEPTH
    max_members: int = _RUNTIME_PROVENANCE_MAX_MEMBERS
    overflow: bool = False

    def claim(self, value: Any, depth: int) -> tuple[int, bool] | None:
        if self.overflow:
            return None
        if depth > self.max_depth:
            self.overflow = True
            return None
        identity = id(value)
        existing = self.references.get(identity)
        if existing is not None:
            return existing, False
        if self.nodes >= self.max_nodes:
            self.overflow = True
            return None
        reference = len(self.references)
        self.references[identity] = reference
        self.nodes += 1
        return reference, True

    def claim_member(self) -> bool:
        if self.overflow:
            return False
        if self.members >= self.max_members:
            self.overflow = True
            return False
        self.members += 1
        return True


@dataclass
class _RuntimeTraversalContext:
    """Shared bounded work budget for provenance and callable discovery."""

    expanded: set[int]
    nodes: int = 0
    members: int = 0
    max_nodes: int = _RUNTIME_STATE_MAX_NODES
    max_depth: int = _RUNTIME_STATE_MAX_DEPTH
    max_members: int = _RUNTIME_PROVENANCE_MAX_MEMBERS
    overflow: bool = False

    def claim(self, value: Any, depth: int) -> bool:
        if self.overflow:
            return False
        if depth > self.max_depth or self.nodes >= self.max_nodes:
            self.overflow = True
            return False
        identity = id(value)
        if identity in self.expanded:
            return False
        self.expanded.add(identity)
        self.nodes += 1
        return True

    def claim_member(self) -> bool:
        if self.overflow:
            return False
        if self.members >= self.max_members:
            self.overflow = True
            return False
        self.members += 1
        return True


@dataclass
class _RuntimeStateSortContext:
    """Bound and memoize one nested set-ordering walk."""

    memo: dict[tuple[int, int], tuple[Any, ...]] = field(default_factory=dict)
    nodes: int = 0
    max_nodes: int = _RUNTIME_STATE_MAX_NODES
    max_depth: int = _RUNTIME_STATE_MAX_DEPTH
    max_members: int = _RUNTIME_PROVENANCE_MAX_MEMBERS
    overflow: bool = False


def _runtime_state_sort_key(
    value: Any,
    depth: int = 0,
    context: _RuntimeStateSortContext | None = None,
) -> tuple[Any, ...]:
    """Return a safe ordering key for exact set members."""
    if context is None:
        context = _RuntimeStateSortContext()
    if context.overflow or depth > context.max_depth:
        context.overflow = True
        return (5, "depth-limit")
    value_type = type(value)
    if value is None:
        return (0, "")
    if value_type is bool or value_type is int or value_type is str:
        return (1, value_type.__name__, value)
    if value_type is float:
        return (1, "float", struct.pack(">d", value).hex())
    if value_type is complex:
        return (1, "complex", struct.pack(">dd", value.real, value.imag).hex())
    if value_type is bytes:
        return (1, "bytes", value.hex())
    if value_type is tuple:
        if len(value) > context.max_members:
            context.overflow = True
            return (5, "member-limit")
        memo_key = (id(value), depth)
        cached = context.memo.get(memo_key)
        if cached is not None:
            return cached
        if context.nodes >= context.max_nodes:
            context.overflow = True
            return (5, "node-limit")
        context.nodes += 1
        result = (2, tuple(_runtime_state_sort_key(item, depth + 1, context) for item in value))
        context.memo[memo_key] = result
        return result
    if value_type is frozenset:
        if len(value) > context.max_members:
            context.overflow = True
            return (5, "member-limit")
        memo_key = (id(value), depth)
        cached = context.memo.get(memo_key)
        if cached is not None:
            return cached
        if context.nodes >= context.max_nodes:
            context.overflow = True
            return (5, "node-limit")
        context.nodes += 1
        member_keys = tuple(_runtime_state_sort_key(item, depth + 1, context) for item in value)
        result = (2, tuple(sorted(member_keys)))
        context.memo[memo_key] = result
        return result
    if _is_function(value):
        return (3, _code_object_digest(value.__code__))
    if _is_method(value):
        return (3, _code_object_digest(value.__func__.__code__))
    identity = _runtime_type_identity(value_type)
    return (4, identity["module"], identity["qualname"])


def _runtime_sort_key_has_overflow(value: tuple[Any, ...]) -> bool:
    """Check a bounded sort key for a nested overflow marker."""
    pending = [value]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        identity = id(current)
        if identity in seen:
            continue
        seen.add(identity)
        if current and current[0] == 5:
            return True
        if current and current[0] == 2 and len(current) > 1 and type(current[1]) is tuple:
            pending.extend(current[1])
    return False


def _runtime_state_value_material(
    value: Any,
    context: _RuntimeStateContext | None = None,
    depth: int = 0,
) -> Any:
    """Materialize a bounded, safe runtime object graph without user dispatch."""
    if context is None:
        context = _RuntimeStateContext({})
    if context.overflow:
        return _UNREPRESENTABLE_RUNTIME_STATE
    claimed = context.claim(value, depth)
    if claimed is None:
        return _UNREPRESENTABLE_RUNTIME_STATE
    reference, first_visit = claimed
    if not first_visit:
        return {"type": "reference", "ref": reference}

    value_type = type(value)
    if value is None or value_type is bool or value_type is int or value_type is str:
        return {"type": value_type.__name__, "ref": reference, "value": value}
    if value_type is float:
        return {
            "type": "float",
            "ref": reference,
            "bits": struct.pack(">d", value).hex(),
        }
    if value_type is bytes:
        return {
            "type": "bytes",
            "ref": reference,
            "size": len(value),
            "sha256": sha256(value).hexdigest(),
        }
    if value_type is list or value_type is tuple:
        items: list[Any] = []
        for item in value:
            if context.overflow:
                return _UNREPRESENTABLE_RUNTIME_STATE
            if not context.claim_member():
                return _UNREPRESENTABLE_RUNTIME_STATE
            material = _runtime_state_value_material(item, context, depth + 1)
            if material is _UNREPRESENTABLE_RUNTIME_STATE:
                return material
            items.append(material)
        return {"type": value_type.__name__, "ref": reference, "items": items}
    if value_type is dict:
        items: list[Any] = []
        string_keys = True
        for key, nested in value.items():
            if context.overflow:
                return _UNREPRESENTABLE_RUNTIME_STATE
            if not context.claim_member():
                return _UNREPRESENTABLE_RUNTIME_STATE
            if type(key) is not str:
                string_keys = False
                key_material = _runtime_state_value_material(key, context, depth + 1)
                if key_material is _UNREPRESENTABLE_RUNTIME_STATE:
                    return key_material
            else:
                key_material = key
            material = _runtime_state_value_material(nested, context, depth + 1)
            if material is _UNREPRESENTABLE_RUNTIME_STATE:
                return material
            items.append((key_material, material))
        return {
            "type": "dict" if string_keys else "dict-unsupported-key",
            "ref": reference,
            "items": items,
        }
    if value_type is set or value_type is frozenset:
        if len(value) > context.max_nodes:
            context.overflow = True
            return _UNREPRESENTABLE_RUNTIME_STATE
        members: list[Any] = []
        ordered_values = list(value)
        sort_context = _RuntimeStateSortContext(
            max_nodes=context.max_nodes,
            max_depth=context.max_depth,
            max_members=context.max_members,
        )
        ordered_keys = [
            _runtime_state_sort_key(item, depth + 1, sort_context) for item in ordered_values
        ]
        if sort_context.overflow or any(
            _runtime_sort_key_has_overflow(key) for key in ordered_keys
        ):
            context.overflow = True
            return _UNREPRESENTABLE_RUNTIME_STATE
        ordered_values = [
            item
            for _key, item in sorted(
                zip(ordered_keys, ordered_values, strict=True), key=lambda pair: pair[0]
            )
        ]
        for item in ordered_values:
            if context.overflow:
                return _UNREPRESENTABLE_RUNTIME_STATE
            if not context.claim_member():
                return _UNREPRESENTABLE_RUNTIME_STATE
            material = _runtime_state_value_material(item, context, depth + 1)
            if material is _UNREPRESENTABLE_RUNTIME_STATE:
                return material
            members.append(material)
        return {"type": value_type.__name__, "ref": reference, "items": members}
    if _is_code(value):
        digest_overflow = [False]
        digest = _code_object_digest(value, overflow=digest_overflow)
        if digest_overflow[0]:
            context.overflow = True
            return _UNREPRESENTABLE_RUNTIME_STATE
        return {
            "type": "code",
            "ref": reference,
            "digest": digest,
        }
    if _type_has_base(value, functools.partial):
        raw_args = _safe_partial_attribute(value, "args")
        raw_keywords = _safe_partial_attribute(value, "keywords")
        raw_target = _safe_partial_attribute(value, "func")
        if (
            raw_args is _UNRESOLVED_RUNTIME_VALUE
            or raw_keywords is _UNRESOLVED_RUNTIME_VALUE
            or raw_target is _UNRESOLVED_RUNTIME_VALUE
        ):
            return _UNREPRESENTABLE_RUNTIME_STATE
        entries = {
            "args": _runtime_state_value_material(raw_args, context, depth + 1),
            "keywords": _runtime_state_value_material(raw_keywords, context, depth + 1),
            "target": _runtime_state_value_material(raw_target, context, depth + 1),
            "instance": _runtime_instance_state_material(value, context, depth + 1),
        }
        for item in entries.values():
            if item is _UNREPRESENTABLE_RUNTIME_STATE:
                return item
        return {"type": "partial", "ref": reference, **entries}
    if _is_function(value):
        closure_material: list[Any] = []
        if value.__closure__ is not None:
            for cell in value.__closure__:
                if not context.claim_member():
                    return _UNREPRESENTABLE_RUNTIME_STATE
                try:
                    cell_value = cell.cell_contents
                except ValueError:
                    closure_material.append({"type": "empty-cell"})
                    continue
                material = _runtime_state_value_material(cell_value, context, depth + 1)
                if material is _UNREPRESENTABLE_RUNTIME_STATE:
                    return _UNREPRESENTABLE_RUNTIME_STATE
                closure_material.append(material)
        entries = {
            "defaults": _runtime_state_value_material(value.__defaults__, context, depth + 1),
            "kwdefaults": _runtime_state_value_material(value.__kwdefaults__, context, depth + 1),
            "annotations": _runtime_state_value_material(value.__annotations__, context, depth + 1),
            "dict": _runtime_state_value_material(value.__dict__, context, depth + 1),
        }
        wrapped = _safe_runtime_attribute(value, "__wrapped__")
        entries["wrapped"] = (
            None
            if wrapped is _UNRESOLVED_RUNTIME_VALUE
            else _runtime_state_value_material(wrapped, context, depth + 1)
        )
        for item in entries.values():
            if item is _UNREPRESENTABLE_RUNTIME_STATE:
                return item
        return {
            "type": "function",
            "ref": reference,
            "code": _runtime_state_value_material(value.__code__, context, depth + 1),
            "closure": closure_material,
            **entries,
        }
    if _is_method(value):
        function_material = _runtime_state_value_material(value.__func__, context, depth + 1)
        receiver_material = _runtime_state_value_material(value.__self__, context, depth + 1)
        if (
            function_material is _UNREPRESENTABLE_RUNTIME_STATE
            or receiver_material is _UNREPRESENTABLE_RUNTIME_STATE
        ):
            return _UNREPRESENTABLE_RUNTIME_STATE
        return {
            "type": "bound-method",
            "ref": reference,
            "function": function_material,
            "receiver": receiver_material,
        }
    if _is_builtin(value):
        module_name = _safe_runtime_attribute(value, "__module__")
        qualname = _safe_runtime_attribute(value, "__qualname__")
        if not isinstance(module_name, str) or not isinstance(qualname, str):
            return _UNREPRESENTABLE_RUNTIME_STATE
        return {
            "type": "builtin",
            "ref": reference,
            "module": module_name,
            "qualname": qualname,
        }
    if _is_module(value):
        module_name = _safe_runtime_attribute(value, "__name__")
        module_file = _safe_runtime_attribute(value, "__file__")
        if not isinstance(module_name, str) or (
            module_file is not None
            and module_file is not _UNRESOLVED_RUNTIME_VALUE
            and type(module_file) is not str
        ):
            return _UNREPRESENTABLE_RUNTIME_STATE
        return {
            "type": "module",
            "ref": reference,
            "name": module_name,
            "file": module_file if type(module_file) is str else None,
        }
    if _is_class(value):
        class_state = _runtime_class_state_material(value, context, depth + 1)
        if class_state is _UNREPRESENTABLE_RUNTIME_STATE:
            return _UNREPRESENTABLE_RUNTIME_STATE
        metaclass = type(value)
        metaclass_state: Any = None
        if metaclass is not type:
            metaclass_state = _runtime_state_value_material(metaclass, context, depth + 1)
            if metaclass_state is _UNREPRESENTABLE_RUNTIME_STATE:
                return _UNREPRESENTABLE_RUNTIME_STATE
        return {
            "type": "class",
            "ref": reference,
            "identity": _runtime_type_state_identity(value),
            "state": class_state,
            "metaclass": metaclass_state,
        }
    if (
        _type_has_base(value, list)
        or _type_has_base(value, tuple)
        or _type_has_base(value, dict)
        or _type_has_base(value, set)
        or _type_has_base(value, frozenset)
        or _type_has_base(value, bytearray)
    ):
        return _UNREPRESENTABLE_RUNTIME_STATE
    instance_material = _runtime_instance_state_material(value, context, depth + 1)
    if instance_material is _UNREPRESENTABLE_RUNTIME_STATE:
        return _UNREPRESENTABLE_RUNTIME_STATE
    return {
        "type": "callable-instance" if callable(value) else "instance",
        "ref": reference,
        "class": _runtime_type_state_identity(type(value)),
        "state": instance_material,
    }


def _transactional_runtime_state_material(
    value: Any,
    context: _RuntimeStateContext,
) -> Any:
    """Commit shared references only when one complete root is representable."""
    trial = _RuntimeStateContext(
        dict(context.references),
        nodes=context.nodes,
        members=context.members,
        max_nodes=context.max_nodes,
        max_depth=context.max_depth,
        max_members=context.max_members,
    )
    material = _runtime_state_value_material(value, trial)
    context.overflow = context.overflow or trial.overflow
    if material is not _UNREPRESENTABLE_RUNTIME_STATE:
        context.references = trial.references
        context.nodes = trial.nodes
        context.members = trial.members
    return material


def _runtime_class_state_material(
    value_type: type[Any],
    context: _RuntimeStateContext,
    depth: int,
) -> Any:
    """Capture bounded class and base-class state without metaclass dispatch."""
    raw_mro = _safe_type_mro(value_type)
    if raw_mro is None:
        return _UNREPRESENTABLE_RUNTIME_STATE
    classes: list[dict[str, Any]] = []
    for owner in raw_mro:
        if owner is object:
            continue
        namespace = _safe_class_namespace(owner)
        if namespace is None:
            return _UNREPRESENTABLE_RUNTIME_STATE
        fields: list[tuple[str, Any]] = []
        for name, raw_member in namespace.items():
            if not context.claim_member():
                return _UNREPRESENTABLE_RUNTIME_STATE
            if name in _RUNTIME_STATE_STRUCTURAL_CLASS_NAMES:
                continue
            descriptor = _exact_descriptor_function(raw_member)
            member = descriptor[1] if descriptor is not None else raw_member
            if _is_builtin_descriptor(raw_member):
                continue
            if type(raw_member) is property or (
                descriptor is None
                and not _is_function(member)
                and not _is_builtin(member)
                and _has_static_descriptor_protocol(raw_member)
            ):
                return _UNREPRESENTABLE_RUNTIME_STATE
            material = _runtime_state_value_material(member, context, depth + 1)
            if material is _UNREPRESENTABLE_RUNTIME_STATE:
                return _UNREPRESENTABLE_RUNTIME_STATE
            if descriptor is not None:
                material = {
                    "type": "descriptor",
                    "kind": descriptor[0],
                    "value": material,
                }
            fields.append((name, material))
        if fields:
            classes.append({"class": _runtime_type_state_identity(owner), "fields": fields})
    return {"type": "class-state", "classes": classes}


def _runtime_instance_state_material(
    value: Any,
    context: _RuntimeStateContext,
    depth: int,
) -> Any:
    """Read exact instance dictionaries and each MRO slot descriptor once."""
    if _is_module(value) or _is_class(value):
        return _UNREPRESENTABLE_RUNTIME_STATE

    fields: list[Any] = []
    instance_dict = _safe_instance_dict(value)
    if instance_dict is not None:
        material = _runtime_state_value_material(instance_dict, context, depth + 1)
        if material is _UNREPRESENTABLE_RUNTIME_STATE:
            return _UNREPRESENTABLE_RUNTIME_STATE
        fields.append({"owner": None, "name": "__dict__", "value": material})

    value_type = type(value)
    raw_mro = _safe_type_mro(value_type)
    if raw_mro is None:
        return _UNREPRESENTABLE_RUNTIME_STATE
    for owner in raw_mro:
        namespace = _safe_class_namespace(owner)
        if namespace is None:
            return _UNREPRESENTABLE_RUNTIME_STATE
        if context.overflow:
            return _UNREPRESENTABLE_RUNTIME_STATE
        custom_getattribute = namespace.get("__getattribute__")
        if custom_getattribute is not None and custom_getattribute is not object.__getattribute__:
            return _UNREPRESENTABLE_RUNTIME_STATE
        if "__getattr__" in namespace:
            return _UNREPRESENTABLE_RUNTIME_STATE
        for member_name, member in namespace.items():
            if not context.claim_member():
                return _UNREPRESENTABLE_RUNTIME_STATE
            if member_name in {"__dict__", "__weakref__"}:
                continue
            if type(member) is property:
                return _UNREPRESENTABLE_RUNTIME_STATE
            if type(member) is types.MemberDescriptorType:
                try:
                    slot_value = member.__get__(value, value_type)
                except (AttributeError, TypeError):
                    slot_material: Any = {"type": "empty-slot"}
                else:
                    slot_material = _runtime_state_value_material(slot_value, context, depth + 1)
                    if slot_material is _UNREPRESENTABLE_RUNTIME_STATE:
                        return _UNREPRESENTABLE_RUNTIME_STATE
                fields.append(
                    {
                        "owner": _runtime_type_state_identity(owner),
                        "name": member_name,
                        "value": slot_material,
                    }
                )
                continue
            if (
                not _is_function(member)
                and not _is_builtin(member)
                and _exact_descriptor_function(member) is None
                and not _is_builtin_descriptor(member)
                and _has_static_descriptor_protocol(member)
            ):
                return _UNREPRESENTABLE_RUNTIME_STATE

    class_material = _runtime_state_value_material(value_type, context, depth + 1)
    if class_material is _UNREPRESENTABLE_RUNTIME_STATE:
        return _UNREPRESENTABLE_RUNTIME_STATE
    return {"type": "instance-state", "fields": fields, "class": class_material}


@dataclass(frozen=True)
class _LibsIdentity:
    """Deterministic libs digest plus whether L3 reuse is safe."""

    digest: str
    cache_reusable: bool = True


@dataclass(frozen=True)
class _LibsSourceSnapshot:
    """One read-only view of configured source files for one keying operation."""

    entries: tuple[tuple[Path, str], ...]
    entry_identities: tuple[tuple[int, str, str], ...]
    path_identities: dict[Path, tuple[tuple[int, str, str], ...]]
    texts: dict[Path, str]
    trees: dict[Path, ast.Module | None]
    source_files: frozenset[Path]
    has_syntax_error: bool


@dataclass(frozen=True)
class _ImportResolution:
    """Source files found for one import, or an ambiguity that requires fallback."""

    paths: tuple[Path, ...] = ()
    ambiguous: bool = False
    candidate: Path | None = None
    module_names: tuple[tuple[Path, str], ...] = ()


_RuntimeModuleRecord = tuple[
    str,
    str,
    str,
    tuple[tuple[int, str, str], ...],
    tuple[tuple[int, str, str], ...],
    tuple[str, str],
]


def _capture_libs_source_snapshot(
    source_dirs: Iterable[Path],
    *,
    project_root: Path | None = None,
) -> _LibsSourceSnapshot:
    """Read configured Python files once, retaining ordered stable source identities."""
    resolved_project_root = project_root.resolve() if project_root is not None else None
    resolved_source_dirs = tuple(Path(source_dir).resolve() for source_dir in source_dirs)
    entries: list[tuple[Path, str]] = []
    entry_identities: list[tuple[int, str, str]] = []
    path_identities: dict[Path, list[tuple[int, str, str]]] = {}
    texts: dict[Path, str] = {}
    for source_index, source_dir in enumerate(resolved_source_dirs):
        if not source_dir.is_dir():
            continue
        if resolved_project_root is not None:
            try:
                source_identity = source_dir.relative_to(resolved_project_root).as_posix()
            except ValueError:
                source_identity = source_dir.as_posix()
        else:
            # An absolute configured source outside project_root has no shorter
            # project-relative spelling; retaining its configured identity is
            # necessary to distinguish two otherwise identical external roots.
            source_identity = source_dir.as_posix()
        for raw_path in sorted(source_dir.rglob("*.py")):
            path = raw_path.resolve()
            text = texts.get(path)
            if text is None:
                text = path.read_text(encoding="utf-8")
                texts[path] = text
            entries.append((path, text))
            try:
                relative_path = path.relative_to(source_dir).as_posix()
            except ValueError:
                # Symlinked files outside the configured root are unusual, but
                # their absolute path is the only collision-free identity.
                relative_path = path.as_posix()
            identity = (source_index, source_identity, relative_path)
            entry_identities.append(identity)
            path_identities.setdefault(path, []).append(identity)

    trees: dict[Path, ast.Module | None] = {}
    has_syntax_error = False
    for path, text in texts.items():
        try:
            trees[path] = ast.parse(text)
        except SyntaxError:
            trees[path] = None
            has_syntax_error = True
    return _LibsSourceSnapshot(
        tuple(entries),
        tuple(entry_identities),
        {path: tuple(identities) for path, identities in path_identities.items()},
        texts,
        trees,
        frozenset(texts),
        has_syntax_error,
    )


def _all_libs_hash(
    entries: Iterable[tuple[Path, str]],
    identities: Iterable[tuple[int, str, str]] | None = None,
) -> str:
    """Hash normalized source with ordered configured-root/file identity."""
    material = tuple(entries)
    source_identities = (
        tuple(identities)
        if identities is not None
        else tuple((0, "", path.as_posix()) for path, _ in material)
    )
    if len(source_identities) != len(material):
        raise ValueError("libs source identities must align with source entries")
    return sha(
        [
            {
                "identity": identity,
                "source": _module_code_text(text),
            }
            for identity, (_path, text) in zip(source_identities, material, strict=True)
        ]
    )


class _StaticLibsAnalyzer:
    """Resolve a conservative, filesystem-only import closure for one node module."""

    _DYNAMIC_CALLS = frozenset(
        {"__import__", "compile", "eval", "exec", "find_spec", "import_module"}
    )
    _IMPORT_LOOKUP_ATTRIBUTES = frozenset({"__import__", "find_spec", "import_module"})

    def __init__(
        self,
        project_root: Path,
        source_dirs: Iterable[Path],
        snapshot: _LibsSourceSnapshot,
    ):
        self.project_root = project_root.resolve()
        self.source_dirs = tuple(dict.fromkeys(path.resolve() for path in source_dirs))
        self._source_identities = tuple(
            (
                index,
                (
                    source_dir.relative_to(self.project_root).as_posix()
                    if _path_is_within(source_dir, self.project_root)
                    else source_dir.as_posix()
                ),
                source_dir,
            )
            for index, source_dir in enumerate(self.source_dirs)
        )
        self.source_files = snapshot.source_files
        self._path_identities = dict(snapshot.path_identities)
        self._texts = dict(snapshot.texts)
        self._normalized_texts = {
            path: _module_code_text(text) for path, text in self._texts.items()
        }
        self._trees = dict(snapshot.trees)
        self._reachable_cache: dict[tuple[Path, str], dict[Path, str] | None] = {}
        self._registry_records: set[_RuntimeModuleRecord] | None = None
        self._registry_records_overflow = False

    def _owner_module_identity(
        self,
        function: Callable[..., Any],
        module_path: Path | None,
        module_name: str | None,
    ) -> dict[str, str] | None:
        """Bind a stable owner identity only when the node function can observe it."""
        facts = _function_owner_facts(function)
        if (
            not facts.consistent
            or module_path is None
            or module_name is None
            or facts.module_path != module_path
            or facts.module_name != module_name
            or not _function_uses_module_identity(function)
        ):
            return None
        try:
            stable_path = module_path.resolve().relative_to(self.project_root).as_posix()
        except ValueError:
            stable_path = module_path.resolve().as_posix()
        return {"module": module_name, "path": stable_path}

    def fallback_identity_for(
        self,
        function: Callable[..., Any],
        fallback: str,
    ) -> _LibsIdentity:
        """Bind deterministic fallback material and retain L3 reusability separately."""
        module_path = _function_module_path(function)
        module_name = _function_module_name(function)
        owner_module = self._owner_module_identity(function, module_path, module_name)
        runtime, cache_reusable = self._runtime_selection_material(function)
        return _LibsIdentity(
            sha(
                {
                    "fallback": fallback,
                    "owner_module": owner_module,
                    "runtime": runtime,
                }
            ),
            cache_reusable=cache_reusable,
        )

    def fallback_hash_for(self, function: Callable[..., Any], fallback: str) -> str:
        """Return the deterministic digest portion of a fallback identity."""
        return self.fallback_identity_for(function, fallback).digest

    def identity_for(self, function: Callable[..., Any], fallback: str) -> _LibsIdentity:
        """Return selected-files identity, or runtime-bound fallback on uncertainty."""
        module_path = _function_module_path(function)
        module_name = _function_module_name(function)
        if module_path is None or module_name is None:
            return self.fallback_identity_for(function, fallback)
        reachable = self._reachable_paths(module_path, module_name)
        if reachable is None:
            return self.fallback_identity_for(function, fallback)
        material: list[dict[str, Any]] = []
        for path, reachable_module_name in sorted(
            reachable.items(), key=lambda item: item[0].as_posix()
        ):
            identities = self._path_identities.get(path, ())
            if len(identities) != 1:
                return self.fallback_identity_for(function, fallback)
            material.append(
                {
                    "identity": identities[0],
                    "module": reachable_module_name,
                    "source": self._normalized_texts[path],
                }
            )
        owner_module = self._owner_module_identity(function, module_path, module_name)
        selected_identity = {"selected": material}
        if owner_module is not None:
            selected_identity["owner_module"] = owner_module
        runtime, cache_reusable = self._runtime_selection_material(function)
        selected_identity["runtime"] = runtime
        return _LibsIdentity(sha(selected_identity), cache_reusable=cache_reusable)

    def hash_for(self, function: Callable[..., Any], fallback: str) -> str:
        """Return the deterministic digest portion of a libs identity."""
        return self.identity_for(function, fallback).digest

    def _reachable_paths(self, root: Path, module_name: str) -> dict[Path, str] | None:
        cache_key = (root, module_name)
        if cache_key in self._reachable_cache:
            cached = self._reachable_cache[cache_key]
            return None if cached is None else dict(cached)

        initial: list[tuple[Path, str]] = [(root, module_name)]
        if root in self.source_files:
            module_resolution = self._resolve_absolute(tuple(module_name.split(".")))
            if module_resolution.ambiguous:
                self._reachable_cache[cache_key] = None
                return None
            if root in module_resolution.paths:
                initial_bindings = self._module_bindings(
                    tuple(module_name.split(".")), module_resolution
                )
                if initial_bindings is None or initial_bindings.get(root) != module_name:
                    self._reachable_cache[cache_key] = None
                    return None
                initial.extend(initial_bindings.items())
            else:
                for ancestor in self._ancestor_package_inits(root):
                    ancestor_name = self._ancestor_module_name(root, module_name, ancestor)
                    if ancestor_name is None:
                        self._reachable_cache[cache_key] = None
                        return None
                    initial.append((ancestor, ancestor_name))
        queue = list(initial)
        seen: dict[Path, str] = {}
        reachable: dict[Path, str] = {}
        while queue:
            path, current_module_name = queue.pop()
            prior_name = seen.get(path)
            if prior_name is not None:
                if prior_name != current_module_name:
                    self._reachable_cache[cache_key] = None
                    return None
                continue
            seen[path] = current_module_name
            tree = self._tree(path)
            if tree is None or _module_imports_are_ambiguous(tree):
                self._reachable_cache[cache_key] = None
                return None
            if path in self.source_files:
                reachable[path] = current_module_name
            for statement in tree.body:
                if not isinstance(statement, (ast.Import, ast.ImportFrom)):
                    continue
                resolution = self._resolve_import(path, current_module_name, statement)
                if resolution.ambiguous or set(resolution.paths) != {
                    target_path for target_path, _target_name in resolution.module_names
                }:
                    self._reachable_cache[cache_key] = None
                    return None
                queue.extend(resolution.module_names)

        self._reachable_cache[cache_key] = dict(reachable)
        return reachable

    def _configured_location_identities(self, path: Path) -> tuple[tuple[int, str, str], ...]:
        """Return stable configured-root identities for one runtime path."""
        resolved = path.resolve()
        exact = self._path_identities.get(resolved)
        if exact is not None:
            return exact
        identities: list[tuple[int, str, str]] = []
        for source_index, source_identity, source_dir in self._source_identities:
            try:
                relative = resolved.relative_to(source_dir).as_posix()
            except ValueError:
                continue
            identities.append((source_index, source_identity, relative))
        return tuple(identities)

    def _runtime_module_record(
        self,
        scope: str,
        binding: str,
        value: Any,
        seen: frozenset[int] | None = None,
        traversal: _RuntimeTraversalContext | None = None,
    ) -> _RuntimeModuleRecord | None:
        """Describe runtime module or binding provenance touching configured sources."""
        if seen is None:
            seen = frozenset()
        if id(value) in seen:
            return None
        is_module = _is_module(value)
        if is_module:
            module_name = _safe_runtime_attribute(value, "__name__")
        else:
            module_name = _safe_runtime_attribute(value, "__module__")
            if not isinstance(module_name, str):
                module_name = _safe_runtime_attribute(type(value), "__module__")
        if not isinstance(module_name, str):
            module_name = ""
        callable_name = ""
        callable_code_digests: set[str] = set()
        if not is_module:
            raw_callable_name = _safe_runtime_attribute(value, "__qualname__")
            if not isinstance(raw_callable_name, str):
                raw_callable_name = _safe_runtime_attribute(type(value), "__qualname__")
            if isinstance(raw_callable_name, str):
                callable_name = raw_callable_name

        file_identities: set[tuple[int, str, str]] = set()
        source_candidates: list[str] = []
        if is_module:
            module_file = _safe_runtime_attribute(value, "__file__")
            if isinstance(module_file, str) and module_file:
                source_candidates.append(module_file)
        else:
            code = _function_code(value)
            code_file = _safe_runtime_attribute(code, "co_filename")
            if isinstance(code_file, str) and code_file:
                source_candidates.append(code_file)
                with suppress(OSError, RuntimeError):
                    file_identities.update(self._configured_location_identities(Path(code_file)))
            code_overflow = [False]
            code_digest = (
                _code_object_digest(code, overflow=code_overflow) if _is_code(code) else ""
            )
            if code_overflow[0]:
                if not file_identities:
                    return None
                return (
                    scope,
                    binding,
                    module_name,
                    (),
                    ((-1, "", "invalid"),),
                    (callable_name, ""),
                )
            if code_digest:
                callable_code_digests.add(code_digest)
            registry = _safe_sys_modules()
            owner_module = registry.get(module_name) if registry is not None else None
            owner_file = _safe_runtime_attribute(owner_module, "__file__")
            if isinstance(owner_file, str) and owner_file:
                source_candidates.append(owner_file)
                with suppress(OSError, RuntimeError):
                    file_identities.update(self._configured_location_identities(Path(owner_file)))
            if module_name:
                resolution = self._resolve_absolute(tuple(module_name.split(".")))
                if not resolution.ambiguous:
                    for path in resolution.paths:
                        try:
                            file_identities.update(self._configured_location_identities(path))
                        except (OSError, RuntimeError):
                            continue
            provenance_type = value if _is_class(value) else type(value)
            static_namespace = _safe_class_namespace(provenance_type)
            if (
                static_namespace is not None
                and len(static_namespace) > _RUNTIME_PROVENANCE_MAX_MEMBERS
            ):
                if not file_identities:
                    return None
                return (
                    scope,
                    binding,
                    module_name,
                    (),
                    ((-1, "", "invalid"),),
                    (callable_name, ""),
                )
            members = tuple(static_namespace.values()) if static_namespace is not None else ()
            for raw_member in members:
                if traversal is not None and not traversal.claim_member():
                    if not file_identities:
                        return None
                    return (
                        scope,
                        binding,
                        module_name,
                        (),
                        ((-1, "", "invalid"),),
                        (callable_name, ""),
                    )
                descriptor = _exact_descriptor_function(raw_member)
                member = descriptor[1] if descriptor is not None else raw_member
                member_code = _function_code(member)
                member_file = _safe_runtime_attribute(member_code, "co_filename")
                member_overflow = [False]
                member_digest = (
                    _code_object_digest(member_code, overflow=member_overflow)
                    if _is_code(member_code)
                    else ""
                )
                if member_overflow[0]:
                    if not file_identities:
                        return None
                    return (
                        scope,
                        binding,
                        module_name,
                        (),
                        ((-1, "", "invalid"),),
                        (callable_name, ""),
                    )
                if member_digest:
                    callable_code_digests.add(member_digest)
                if isinstance(member_file, str) and member_file:
                    source_candidates.append(member_file)
                    with suppress(OSError, RuntimeError):
                        file_identities.update(
                            self._configured_location_identities(Path(member_file))
                        )
            registry = _safe_sys_modules()
            owner_module = registry.get(module_name) if registry is not None else None
            owner_file = _safe_runtime_attribute(owner_module, "__file__")
            if isinstance(owner_file, str) and owner_file:
                source_candidates.append(owner_file)
            if module_name:
                resolution = self._resolve_absolute(tuple(module_name.split(".")))
                if not resolution.ambiguous:
                    for path in resolution.paths:
                        try:
                            file_identities.update(self._configured_location_identities(path))
                        except (OSError, RuntimeError):
                            continue

        for source_candidate in source_candidates:
            try:
                file_identities.update(self._configured_location_identities(Path(source_candidate)))
            except (OSError, RuntimeError):
                continue

        search_identities: set[tuple[int, str, str]] = set()
        if is_module:
            runtime_search_path = _safe_runtime_attribute(value, "__path__")
            if runtime_search_path is not None:
                path_entries = _safe_runtime_path_entries(runtime_search_path)
                if path_entries is _UNRESOLVED_RUNTIME_VALUE:
                    if not file_identities:
                        return None
                    return (
                        scope,
                        binding,
                        module_name,
                        tuple(sorted(file_identities)),
                        ((-1, "", "invalid"),),
                        (
                            callable_name,
                            sha(sorted(callable_code_digests)) if callable_code_digests else "",
                        ),
                    )
                try:
                    for entry in path_entries:
                        search_identities.update(self._configured_location_identities(Path(entry)))
                except (OSError, RuntimeError, TypeError):
                    if not file_identities:
                        return None
                    return (
                        scope,
                        binding,
                        module_name,
                        tuple(sorted(file_identities)),
                        ((-1, "", "invalid"),),
                        (
                            callable_name,
                            sha(sorted(callable_code_digests)) if callable_code_digests else "",
                        ),
                    )

        if not file_identities and not search_identities:
            return None
        return (
            scope,
            binding,
            module_name,
            tuple(sorted(file_identities)),
            tuple(sorted(search_identities)),
            (callable_name, sha(sorted(callable_code_digests)) if callable_code_digests else ""),
        )

    def _runtime_module_records(
        self,
        scope: str,
        binding: str,
        value: Any,
        traversal: _RuntimeTraversalContext | None = None,
    ) -> set[_RuntimeModuleRecord]:
        """Collect outer and wrapped callable provenance without following cycles."""
        records: set[_RuntimeModuleRecord] = set()
        pending = [(value, 0)]
        seen: set[int] = set()
        while pending:
            if traversal is not None and traversal.overflow:
                break
            current, depth = pending.pop()
            identity = id(current)
            if identity in seen:
                continue
            seen.add(identity)
            newly_expanded = True
            if traversal is not None and identity not in traversal.expanded:
                if not traversal.claim(current, depth):
                    continue
            elif traversal is not None:
                newly_expanded = False
            record = self._runtime_module_record(scope, binding, current, traversal=traversal)
            if record is not None:
                records.add(record)
            if not newly_expanded:
                continue
            if _type_has_base(current, functools.partial):
                partial_target = _safe_partial_attribute(current, "func")
                if partial_target is not _UNRESOLVED_RUNTIME_VALUE:
                    pending.append((partial_target, depth + 1))
            wrapped = _safe_runtime_attribute(current, "__wrapped__")
            if wrapped is not _UNRESOLVED_RUNTIME_VALUE:
                pending.append((wrapped, depth + 1))
            for child in self._runtime_callable_functions(current, traversal):
                pending.append((child, depth + 1))
        return records

    @staticmethod
    def _runtime_callable_functions(
        value: Any,
        traversal: _RuntimeTraversalContext | None = None,
    ) -> tuple[Callable[..., Any], ...]:
        """Return statically stored Python function state for a callable graph node."""
        if _is_function(value):
            return (value,)
        if _is_method(value):
            return (value.__func__,)
        if not callable(value):
            return ()

        value_type = value if _is_class(value) else type(value)
        owners: list[type[Any]] = []
        raw_mro = _safe_type_mro(value_type)
        if raw_mro is not None:
            owners.extend(raw_mro)
        if _is_class(value):
            metaclass_mro = _safe_type_mro(type(value))
            if metaclass_mro is not None:
                owners.extend(metaclass_mro)

        functions: list[Callable[..., Any]] = []
        seen: set[int] = set()
        for owner in owners:
            if traversal is not None and not traversal.claim_member():
                return ()
            namespace = _safe_class_namespace(owner)
            if namespace is None:
                continue
            if len(namespace) > _RUNTIME_PROVENANCE_MAX_MEMBERS:
                return ()
            for raw_member in namespace.values():
                if traversal is not None and not traversal.claim_member():
                    return ()
                descriptor = _exact_descriptor_function(raw_member)
                member = descriptor[1] if descriptor is not None else raw_member
                if not _is_function(member) or id(member) in seen:
                    continue
                seen.add(id(member))
                functions.append(member)
        return tuple(functions)

    def _runtime_nested_callables(
        self,
        value: Any,
        traversal: _RuntimeTraversalContext | None = None,
        depth: int = 0,
    ) -> tuple[Any, ...]:
        """Find nested callable values without crossing opaque external boundaries."""
        found: list[Any] = []
        pending = [(value, depth)]
        seen: set[int] = set()

        def has_container_protocol(current: Any) -> bool:
            current_type = type(current)
            raw_mro = _safe_type_mro(current_type)
            if raw_mro is None:
                return False
            return any(
                (namespace := _safe_class_namespace(owner)) is not None
                and ("__iter__" in namespace or "values" in namespace)
                for owner in raw_mro
            )

        def has_configured_provenance(current: Any) -> bool:
            record = self._runtime_module_record("nested", "container", current)
            return record is not None and bool(record[3] or record[4])

        while pending:
            if traversal is not None and traversal.overflow:
                break
            current, current_depth = pending.pop()
            identity = id(current)
            if identity in seen:
                continue
            seen.add(identity)
            if (
                traversal is not None
                and id(current) not in traversal.expanded
                and not traversal.claim(current, current_depth)
            ):
                continue
            if callable(current) and not _is_module(current):
                found.append(current)
                continue
            current_type = type(current)
            if current_type is list or current_type is tuple:
                for item in current:
                    if traversal is not None and not traversal.claim_member():
                        break
                    pending.append((item, current_depth + 1))
            elif current_type is dict:
                for key, nested in current.items():
                    if traversal is not None and not traversal.claim_member():
                        break
                    pending.append((nested, current_depth + 1))
                    pending.append((key, current_depth + 1))
            elif current_type is set or current_type is frozenset:
                if len(current) > _RUNTIME_PROVENANCE_MAX_MEMBERS:
                    if traversal is not None:
                        traversal.overflow = True
                    break
                for item in current:
                    if traversal is not None and not traversal.claim_member():
                        break
                    pending.append((item, current_depth + 1))
            elif (
                traversal is not None
                and has_container_protocol(current)
                and has_configured_provenance(current)
            ):
                # Exact protocol execution would call user code.  A configured
                # opaque container must therefore fail closed instead of hiding
                # a callable that can change the node's result.
                traversal.overflow = True
                break
        return tuple(found)

    def _runtime_callable_children(
        self,
        value: Any,
        traversal: _RuntimeTraversalContext | None = None,
        depth: int = 0,
    ) -> tuple[Any, ...]:
        """Return wrapper, closure, partial, dictionary, and slot-held callables."""
        children: list[Any] = []
        if _type_has_base(value, functools.partial):
            partial_target = _safe_partial_attribute(value, "func")
            raw_args = _safe_partial_attribute(value, "args")
            raw_keywords = _safe_partial_attribute(value, "keywords")
            if partial_target is not _UNRESOLVED_RUNTIME_VALUE:
                children.append(partial_target)
            if raw_args is not _UNRESOLVED_RUNTIME_VALUE:
                children.extend(self._runtime_nested_callables(raw_args, traversal, depth + 1))
            if raw_keywords is not _UNRESOLVED_RUNTIME_VALUE:
                children.extend(self._runtime_nested_callables(raw_keywords, traversal, depth + 1))
        wrapped = _safe_runtime_attribute(value, "__wrapped__")
        if wrapped is not _UNRESOLVED_RUNTIME_VALUE:
            children.append(wrapped)
        if _is_function(value):
            if value.__closure__ is not None:
                for cell in value.__closure__:
                    if traversal is not None and not traversal.claim_member():
                        break
                    try:
                        children.extend(
                            self._runtime_nested_callables(cell.cell_contents, traversal, depth + 1)
                        )
                    except ValueError:
                        continue
            children.extend(
                self._runtime_nested_callables(value.__defaults__, traversal, depth + 1)
            )
            children.extend(
                self._runtime_nested_callables(value.__kwdefaults__, traversal, depth + 1)
            )
            for key, nested in value.__dict__.items():
                if traversal is not None and not traversal.claim_member():
                    break
                if key != "__wrapped__":
                    children.extend(self._runtime_nested_callables(nested, traversal, depth + 1))
        elif _is_method(value):
            children.extend(self._runtime_nested_callables(value.__self__, traversal, depth + 1))
        elif callable(value) and not _is_class(value):
            instance_dict = _safe_instance_dict(value)
            if instance_dict is not None:
                children.extend(self._runtime_nested_callables(instance_dict, traversal, depth + 1))
            raw_mro = _safe_type_mro(type(value))
            if raw_mro is not None:
                for owner in raw_mro:
                    namespace = _safe_class_namespace(owner)
                    if namespace is None:
                        continue
                    for member in namespace.values():
                        if traversal is not None and not traversal.claim_member():
                            break
                        if type(member) is not types.MemberDescriptorType:
                            continue
                        try:
                            slot_value = member.__get__(value, type(value))
                        except (AttributeError, TypeError):
                            continue
                        children.extend(
                            self._runtime_nested_callables(slot_value, traversal, depth + 1)
                        )
        return tuple(children)

    def _runtime_callable_owner_material(
        self, function: Callable[..., Any]
    ) -> dict[str, Any] | None:
        """Bind owner facts for a retained callable only when its code observes them."""
        if not _function_uses_module_identity(function):
            return None
        facts = _function_owner_facts(function)
        if not facts.consistent:
            return self._inconsistent_owner_fact_material(function)
        if facts.module_name is None or facts.module_path is None:
            return None
        try:
            stable_path = facts.module_path.relative_to(self.project_root).as_posix()
        except ValueError:
            stable_path = facts.module_path.as_posix()
        return {"module": facts.module_name, "path": stable_path}

    def _collect_runtime_callable(
        self,
        scope: str,
        binding: str,
        value: Any,
        records: set[_RuntimeModuleRecord],
        callable_states: list[dict[str, Any]],
        global_values: list[tuple[str, Any]],
        seen_callables: set[int],
        uncacheable: list[bool],
        state_context: _RuntimeStateContext,
        traversal: _RuntimeTraversalContext | None = None,
    ) -> None:
        """Collect callable provenance and bind configured-source runtime state."""
        if not callable(value) or _is_module(value):
            return
        if traversal is None:
            traversal = _RuntimeTraversalContext(set())
        pending: list[tuple[str, str, Any, int]] = [(scope, binding, value, 0)]
        while pending:
            if traversal.overflow:
                uncacheable[0] = True
                break
            current_scope, current_binding, current, depth = pending.pop()
            if not callable(current) or _is_module(current):
                continue
            identity = id(current)
            runtime_records = self._runtime_module_records(
                current_scope,
                current_binding,
                current,
                traversal,
            )
            records.update(runtime_records)
            has_configured_provenance = any(record[3] or record[4] for record in runtime_records)
            callable_functions = self._runtime_callable_functions(current, traversal)
            if has_configured_provenance:
                state = _transactional_runtime_state_material(current, state_context)
                if state is _UNREPRESENTABLE_RUNTIME_STATE:
                    uncacheable[0] = True
                    state = {
                        "type": "unrepresentable",
                        "class": _runtime_type_identity(type(current)),
                    }
                owner_materials = [
                    owner
                    for callable_function in callable_functions
                    if (owner := self._runtime_callable_owner_material(callable_function))
                    is not None
                ]
                entry: dict[str, Any] = {
                    "scope": current_scope,
                    "binding": current_binding,
                    "state": state,
                }
                if owner_materials:
                    entry["owners"] = owner_materials
                callable_states.append(entry)

            if identity in seen_callables:
                continue
            seen_callables.add(identity)
            if has_configured_provenance:
                for callable_function in callable_functions:
                    self._collect_callable_globals(
                        f"{current_binding}:function",
                        callable_function,
                        records,
                        callable_states,
                        global_values,
                        seen_callables,
                        uncacheable,
                        state_context,
                        traversal,
                        pending,
                        depth + 1,
                    )
            children = self._runtime_callable_children(current, traversal, depth)
            for index, child in enumerate(children):
                pending.append(
                    (
                        f"{current_scope}:child",
                        f"{current_binding}:{index}",
                        child,
                        depth + 1,
                    )
                )

    def _collect_callable_globals(
        self,
        binding: str,
        function: Callable[..., Any],
        records: set[_RuntimeModuleRecord],
        callable_states: list[dict[str, Any]],
        global_values: list[tuple[str, Any]],
        seen_callables: set[int],
        uncacheable: list[bool],
        state_context: _RuntimeStateContext,
        traversal: _RuntimeTraversalContext,
        pending: list[tuple[str, str, Any, int]],
        depth: int,
    ) -> None:
        """Capture referenced globals with one shared bounded object-graph context."""
        globals_map = _function_globals(function)
        code = _function_code(function)
        if globals_map is None or not _is_code(code):
            if globals_map is None:
                uncacheable[0] = True
            return
        referenced_overflow = [False]
        referenced_names = (
            _referenced_global_names(code, referenced_overflow) - _MODULE_IDENTITY_NAMES
        )
        if referenced_overflow[0]:
            uncacheable[0] = True
        all_globals_observable = _function_observes_all_globals(function)
        safe_global_names: set[str] = set()
        for name in globals_map:
            if len(safe_global_names) >= _RUNTIME_PROVENANCE_MAX_MEMBERS:
                uncacheable[0] = True
                break
            if type(name) is str:
                safe_global_names.add(name)
            else:
                uncacheable[0] = True
        if len(safe_global_names) != len(globals_map):
            uncacheable[0] = True
        if all_globals_observable:
            uncacheable[0] = True
            selected_names = sorted(safe_global_names)
        else:
            selected_names = sorted(referenced_names & safe_global_names)
        for name in selected_names:
            if traversal.overflow:
                uncacheable[0] = True
                break
            value = globals_map[name]
            runtime_records = self._runtime_module_records(
                "callable-global",
                f"{binding}:{name}",
                value,
                traversal,
            )
            records.update(runtime_records)
            has_configured_provenance = any(record[3] or record[4] for record in runtime_records)
            material = _transactional_runtime_state_material(value, state_context)
            if material is _UNREPRESENTABLE_RUNTIME_STATE:
                if has_configured_provenance or all_globals_observable:
                    uncacheable[0] = True
                    material = {
                        "type": "unrepresentable",
                        "class": _runtime_type_identity(type(value)),
                    }
                else:
                    material = None
            if material is not None:
                global_values.append((f"{binding}:{name}", material))
            nested_callables = self._runtime_nested_callables(value, traversal, depth + 1)
            for index, child in enumerate(nested_callables):
                if not _is_module(child):
                    pending.append(
                        (
                            "callable-global",
                            f"{binding}:{name}:{index}",
                            child,
                            depth + 1,
                        )
                    )

    def _inconsistent_owner_fact_material(
        self,
        function: Callable[..., Any],
    ) -> dict[str, Any] | None:
        """Expose conflicting retained facts only as fail-closed fallback material."""
        facts = _function_owner_facts(function)
        if facts.consistent:
            return None

        def stable_path(path: Path) -> str:
            try:
                return path.relative_to(self.project_root).as_posix()
            except ValueError:
                return path.as_posix()

        return {
            "function_module": facts.function_module,
            "globals_name": facts.globals_name,
            "paths": tuple(stable_path(path) for path in facts.path_facts),
        }

    def _runtime_registry_records(
        self,
        traversal: _RuntimeTraversalContext | None = None,
    ) -> set[_RuntimeModuleRecord]:
        """Snapshot configured-source provenance in ``sys.modules`` once per query."""
        if self._registry_records is not None:
            if traversal is not None and self._registry_records_overflow:
                traversal.overflow = True
            return set(self._registry_records)

        records: set[_RuntimeModuleRecord] = set()
        if not self.source_dirs:
            self._registry_records = records
            return set(records)
        registry = _safe_sys_modules()
        if registry is None:
            if traversal is not None:
                traversal.overflow = True
                self._registry_records_overflow = True
            self._registry_records = records
            return set(records)
        try:
            for registry_name, module in registry.items():
                if traversal is not None and not traversal.claim_member():
                    break
                if module is None:
                    continue
                # The registry is an index for configured-source
                # provenance, not a reason to inspect every unrelated
                # module in the process. Filtering on exact string paths
                # also prevents an opaque external module from consuming
                # the shared member budget before a managed value is
                # examined.
                configured = False
                module_file = _safe_runtime_attribute(module, "__file__")
                if type(module_file) is str and module_file:
                    try:
                        configured = (
                            len(self._configured_location_identities(Path(module_file))) != 0
                        )
                    except (OSError, RuntimeError):
                        configured = False
                if not configured:
                    module_path = _safe_runtime_attribute(module, "__path__")
                    path_entries = _safe_runtime_path_entries(module_path)
                    if path_entries is not _UNRESOLVED_RUNTIME_VALUE:
                        for path_entry in path_entries:
                            try:
                                if len(self._configured_location_identities(Path(path_entry))) != 0:
                                    configured = True
                                    break
                            except (OSError, RuntimeError):
                                continue
                if not configured:
                    continue
                records.update(
                    self._runtime_module_records(
                        "registry",
                        registry_name,
                        module,
                        traversal,
                    )
                )
        except RuntimeError:
            if traversal is not None:
                traversal.overflow = True
        self._registry_records_overflow = traversal is not None and traversal.overflow
        self._registry_records = records
        return set(records)

    def _runtime_selection_material(
        self,
        function: Callable[..., Any],
    ) -> tuple[dict[str, Any], bool]:
        """Capture deterministic runtime choices and whether their L3 cache is reusable."""
        traversal = _RuntimeTraversalContext(set())
        records: set[_RuntimeModuleRecord] = self._runtime_registry_records(traversal)
        callable_states: list[dict[str, Any]] = []
        seen_callables: set[int] = set()
        uncacheable = [False]
        global_values: list[tuple[str, Any]] = []
        state_context = _RuntimeStateContext({})

        self._collect_runtime_callable(
            "registered",
            "function",
            function,
            records,
            callable_states,
            global_values,
            seen_callables,
            uncacheable,
            state_context,
            traversal,
        )

        globals_map = _function_globals(function)
        code = _function_code(function)
        referenced_overflow = [False]
        referenced_globals = (
            _referenced_global_names(code, referenced_overflow) - _MODULE_IDENTITY_NAMES
        )
        if referenced_overflow[0]:
            uncacheable[0] = True
        all_globals_observable = _function_observes_all_globals(function)
        if globals_map is None:
            uncacheable[0] = True
        else:
            safe_global_names: set[str] = set()
            for name in globals_map:
                if len(safe_global_names) >= _RUNTIME_PROVENANCE_MAX_MEMBERS:
                    uncacheable[0] = True
                    break
                if type(name) is str:
                    safe_global_names.add(name)
                else:
                    uncacheable[0] = True
            if len(safe_global_names) != len(globals_map):
                uncacheable[0] = True
            if all_globals_observable:
                uncacheable[0] = True
                selected_names = sorted(safe_global_names)
            else:
                selected_names = sorted(referenced_globals & safe_global_names)
            for binding_name in selected_names:
                if traversal.overflow:
                    uncacheable[0] = True
                    break
                value = globals_map[binding_name]
                runtime_records = self._runtime_module_records(
                    "global",
                    binding_name,
                    value,
                    traversal,
                )
                records.update(runtime_records)
                has_configured_provenance = any(
                    record[3] or record[4] for record in runtime_records
                )
                value_material = _transactional_runtime_state_material(value, state_context)
                if value_material is _UNREPRESENTABLE_RUNTIME_STATE:
                    if has_configured_provenance or all_globals_observable:
                        uncacheable[0] = True
                        value_material = {
                            "type": "unrepresentable",
                            "class": _runtime_type_identity(type(value)),
                        }
                    else:
                        value_material = None
                if value_material is not None:
                    global_values.append((binding_name, value_material))
                nested_callables = self._runtime_nested_callables(value, traversal, 1)
                for index, child in enumerate(nested_callables):
                    if not _is_module(child):
                        self._collect_runtime_callable(
                            "global",
                            f"{binding_name}:{index}",
                            child,
                            records,
                            callable_states,
                            global_values,
                            seen_callables,
                            uncacheable,
                            state_context,
                            traversal,
                        )

        source_order: list[tuple[str, int, str]] = []
        source_by_selector: dict[Path, list[tuple[str, int, str]]] = {}
        for source_index, source_identity, source_dir in self._source_identities:
            source_by_selector.setdefault(source_dir, []).append(
                ("source", source_index, source_identity)
            )
            source_by_selector.setdefault(source_dir.parent, []).append(
                ("package-parent", source_index, source_identity)
            )
        runtime_path = sys.path
        if (
            not (type(runtime_path) is list or type(runtime_path) is tuple)
            or len(runtime_path) > _RUNTIME_PROVENANCE_MAX_MEMBERS
        ):
            uncacheable[0] = True
        else:
            for raw_entry in runtime_path:
                if type(raw_entry) is not str:
                    uncacheable[0] = True
                    continue
                try:
                    entry = Path(os.getcwd() if raw_entry == "" else raw_entry).resolve()
                except (OSError, RuntimeError):
                    uncacheable[0] = True
                    continue
                source_order.extend(source_by_selector.get(entry, ()))

        if any((-1, "", "invalid") in record[4] for record in records):
            uncacheable[0] = True
        if traversal.overflow or state_context.overflow:
            uncacheable[0] = True
        material: dict[str, Any] = {
            "configured_modules": sorted(records),
            "global_values": sorted(global_values, key=lambda item: item[0]),
            "callable_states": sorted(
                callable_states,
                key=lambda entry: (entry["scope"], entry["binding"]),
            ),
            "source_path_order": source_order,
        }
        if uncacheable[0]:
            material["uncacheable"] = True
        owner_facts = self._inconsistent_owner_fact_material(function)
        if owner_facts is not None:
            material["owner_facts"] = owner_facts
        return material, not uncacheable[0]

    def _tree(self, path: Path) -> ast.Module | None:
        if path in self._trees:
            return self._trees[path]
        try:
            text = path.read_text(encoding="utf-8")
            tree = ast.parse(text)
        except (OSError, SyntaxError, UnicodeError):
            self._trees[path] = None
            return None
        self._texts[path] = text
        self._trees[path] = tree
        return tree

    def _ancestor_package_inits(self, path: Path) -> set[Path]:
        """Include package initializers that execute before a source module."""
        result: set[Path] = set()
        parent = path.parent
        while parent != parent.parent:
            init = (parent / "__init__.py").resolve()
            if init in self.source_files:
                result.add(init)
            if parent == self.project_root:
                break
            parent = parent.parent
        return result

    def _module_bindings(
        self,
        parts: tuple[str, ...],
        resolution: _ImportResolution,
    ) -> dict[Path, str] | None:
        """Attach each resolved file to the module identity it executes as."""
        if resolution.ambiguous:
            return None
        if not resolution.paths:
            return {}
        candidate = resolution.candidate
        if candidate is None:
            return None
        package_parts = len(parts) if candidate.name == "__init__.py" else len(parts) - 1
        if package_parts < 0:
            return None
        bindings: dict[Path, str] = {}
        for path in resolution.paths:
            if path == candidate:
                name = ".".join(parts)
            elif path.name == "__init__.py":
                try:
                    distance = len(candidate.parent.relative_to(path.parent).parts)
                except ValueError:
                    return None
                prefix_length = package_parts - distance
                if prefix_length <= 0:
                    return None
                name = ".".join(parts[:prefix_length])
            else:
                return None
            prior = bindings.get(path)
            if prior is not None and prior != name:
                return None
            bindings[path] = name
        return bindings

    def _resolution_for(
        self,
        parts: tuple[str, ...],
        paths: Iterable[Path],
        candidate: Path | None,
    ) -> _ImportResolution:
        """Create a resolution only when all selected paths have stable names."""
        normalized_paths = tuple(sorted(set(paths)))
        resolution = _ImportResolution(normalized_paths, candidate=candidate)
        bindings = self._module_bindings(parts, resolution)
        if bindings is None:
            return _ImportResolution(ambiguous=True)
        return _ImportResolution(
            normalized_paths,
            candidate=candidate,
            module_names=tuple(sorted(bindings.items(), key=lambda item: item[0].as_posix())),
        )

    @staticmethod
    def _merge_resolution_bindings(
        bindings: dict[Path, str], resolution: _ImportResolution
    ) -> bool:
        """Merge a resolution while rejecting one file with competing identities."""
        if resolution.ambiguous:
            return False
        if set(resolution.paths) != {path for path, _name in resolution.module_names}:
            return False
        for path, name in resolution.module_names:
            prior = bindings.get(path)
            if prior is not None and prior != name:
                return False
            bindings[path] = name
        return True

    @staticmethod
    def _ancestor_module_name(path: Path, module_name: str, ancestor: Path) -> str | None:
        """Return the qualified name of an ancestor initializer for one module."""
        if ancestor.name != "__init__.py":
            return None
        module_parts = tuple(module_name.split("."))
        package_length = len(module_parts) if path.name == "__init__.py" else len(module_parts) - 1
        try:
            distance = len(path.parent.relative_to(ancestor.parent).parts)
        except ValueError:
            return None
        prefix_length = package_length - distance
        if prefix_length <= 0:
            return None
        return ".".join(module_parts[:prefix_length])

    def _module_proves_binding(self, path: Path, binding_name: str) -> bool:
        """Prove a direct ImportFrom attribute from a base module's top-level AST."""
        tree = self._tree(path)
        if tree is None or _module_imports_are_ambiguous(tree):
            return False

        for statement in tree.body:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                if statement.name == binding_name:
                    return True
            elif isinstance(statement, ast.Import):
                if any(
                    (alias.asname or alias.name.split(".", 1)[0]) == binding_name
                    for alias in statement.names
                ):
                    return True
            elif isinstance(statement, ast.ImportFrom):
                if any(
                    alias.name != "*" and (alias.asname or alias.name) == binding_name
                    for alias in statement.names
                ):
                    return True
            elif isinstance(statement, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
                targets: Iterable[ast.AST]
                if isinstance(statement, ast.Assign):
                    targets = statement.targets
                else:
                    targets = (statement.target,)
                if any(binding_name in self._assigned_names(target) for target in targets):
                    return True
        return False

    @staticmethod
    def _assigned_names(target: ast.AST) -> set[str]:
        """Return simple names bound by a top-level assignment target."""
        if isinstance(target, ast.Name):
            return {target.id}
        if isinstance(target, (ast.Tuple, ast.List)):
            names: set[str] = set()
            for element in target.elts:
                names.update(_StaticLibsAnalyzer._assigned_names(element))
            return names
        return set()

    def _validate_loaded_prefixes(
        self,
        parts: tuple[str, ...],
        importer_module_name: str,
        binding_name: str | None,
        bound_module_name: str | None = None,
    ) -> bool:
        """Validate every loaded configured module prefix of a dotted import."""
        for index in range(1, len(parts) + 1):
            prefix_parts = parts[:index]
            resolution = self._resolve_absolute(prefix_parts)
            if resolution.ambiguous:
                return False
            if not resolution.paths and self._loaded_prefix_reaches_source(prefix_parts):
                return False
            if resolution.paths and self._loaded_module_mismatch(
                (".".join(prefix_parts),),
                resolution,
                importer_module_name=importer_module_name,
                binding_name=binding_name,
                bound_module_name=bound_module_name,
                require_loaded=True,
            ):
                return False
        return True

    def _loaded_prefix_reaches_source(self, parts: tuple[str, ...]) -> bool:
        """Return whether a loaded dotted package prefix reaches configured sources."""
        registry = _safe_sys_modules()
        if registry is None:
            return True
        module = registry.get(".".join(parts))
        if module is None:
            return False
        runtime_search_path = _safe_runtime_attribute(module, "__path__")
        if runtime_search_path is None:
            return False
        path_entries = _safe_runtime_path_entries(runtime_search_path)
        if path_entries is _UNRESOLVED_RUNTIME_VALUE:
            return True
        try:
            search_paths = tuple(Path(entry).resolve() for entry in path_entries)
        except (OSError, RuntimeError, TypeError):
            return True
        return any(
            _path_is_within(path, source_dir) or _path_is_within(source_dir, path)
            for path in search_paths
            for source_dir in self.source_dirs
        )

    def _resolve_import(
        self,
        importer: Path,
        importer_module_name: str,
        statement: ast.Import | ast.ImportFrom,
    ) -> _ImportResolution:
        if isinstance(statement, ast.Import):
            bindings: dict[Path, str] = {}
            for alias in statement.names:
                parts = tuple(alias.name.split("."))
                if not self._validate_loaded_prefixes(
                    parts,
                    importer_module_name,
                    alias.asname or parts[0],
                    alias.name if alias.asname else parts[0],
                ):
                    return _ImportResolution(ambiguous=True)
                resolution = self._resolve_absolute(parts)
                if resolution.ambiguous:
                    return resolution
                if not resolution.paths and not self._known_external(parts[0]):
                    return _ImportResolution(ambiguous=True)
                if not self._merge_resolution_bindings(bindings, resolution):
                    return _ImportResolution(ambiguous=True)
            return _ImportResolution(
                tuple(sorted(bindings)),
                module_names=tuple(sorted(bindings.items(), key=lambda item: item[0].as_posix())),
            )

        if any(alias.name == "*" for alias in statement.names):
            return _ImportResolution(ambiguous=True)
        module_parts = tuple(statement.module.split(".")) if statement.module else ()
        if statement.level:
            return self._resolve_relative_import(importer, importer_module_name, statement)

        base_resolution = self._resolve_absolute(module_parts)
        if base_resolution.ambiguous:
            return base_resolution
        if module_parts and not self._validate_loaded_prefixes(
            module_parts, importer_module_name, None
        ):
            return _ImportResolution(ambiguous=True)
        bindings: dict[Path, str] = {}
        if not self._merge_resolution_bindings(bindings, base_resolution):
            return _ImportResolution(ambiguous=True)
        for alias in statement.names:
            child_parts = (*module_parts, alias.name)
            child_resolution = self._resolve_absolute(child_parts)
            if child_resolution.ambiguous:
                return child_resolution
            if child_resolution.paths:
                if not self._validate_loaded_prefixes(
                    child_parts,
                    importer_module_name,
                    alias.asname or alias.name,
                    ".".join(child_parts),
                ):
                    return _ImportResolution(ambiguous=True)
                if not self._merge_resolution_bindings(bindings, child_resolution):
                    return _ImportResolution(ambiguous=True)
            elif (
                base_resolution.candidate is not None
                and self._module_proves_binding(base_resolution.candidate, alias.name)
            ) or (
                not base_resolution.paths and module_parts and self._known_external(module_parts[0])
            ):
                continue
            else:
                # A resolved package/module does not prove that an ImportFrom
                # child is merely an attribute.  It may be a runtime-supplied
                # submodule through an extended package path.
                return _ImportResolution(ambiguous=True)
        if not bindings and module_parts and not self._known_external(module_parts[0]):
            return _ImportResolution(ambiguous=True)
        return _ImportResolution(
            tuple(sorted(bindings)),
            module_names=tuple(sorted(bindings.items(), key=lambda item: item[0].as_posix())),
        )

    def _resolve_relative_import(
        self,
        importer: Path,
        importer_module_name: str,
        statement: ast.ImportFrom,
    ) -> _ImportResolution:
        """Resolve relative imports across every configured package root."""
        context = self._relative_package_context(importer, importer_module_name)
        if context is None:
            return _ImportResolution(ambiguous=True)
        identity_package_parts, filesystem_package_parts = context
        climb = statement.level - 1
        if climb >= len(identity_package_parts) or climb > len(filesystem_package_parts):
            return _ImportResolution(ambiguous=True)
        identity_base_parts = identity_package_parts[: len(identity_package_parts) - climb]
        filesystem_base_parts = filesystem_package_parts[: len(filesystem_package_parts) - climb]
        if statement.module:
            module_parts = tuple(statement.module.split("."))
            identity_base_parts = (*identity_base_parts, *module_parts)
            filesystem_base_parts = (*filesystem_base_parts, *module_parts)

        bindings: dict[Path, str] = {}
        roots = (self.project_root, *self.source_dirs)
        base_resolution = self._resolve_relative_target(
            identity_base_parts, filesystem_base_parts, roots
        )
        if base_resolution.ambiguous:
            return base_resolution
        if not self._merge_resolution_bindings(bindings, base_resolution):
            return _ImportResolution(ambiguous=True)
        if not self._validate_relative_loaded_prefixes(
            identity_base_parts,
            len(identity_package_parts) - climb,
            filesystem_base_parts,
            importer_module_name,
            None,
            roots=roots,
        ):
            return _ImportResolution(ambiguous=True)

        for alias in statement.names:
            identity_child_parts = (*identity_base_parts, alias.name)
            filesystem_child_parts = (*filesystem_base_parts, alias.name)
            child_resolution = self._resolve_relative_target(
                identity_child_parts, filesystem_child_parts, roots
            )
            if child_resolution.ambiguous:
                return child_resolution
            if child_resolution.paths:
                if not self._validate_relative_loaded_prefixes(
                    identity_child_parts,
                    len(identity_package_parts) - climb,
                    filesystem_base_parts,
                    importer_module_name,
                    alias.asname or alias.name,
                    ".".join(identity_child_parts),
                    roots,
                ):
                    return _ImportResolution(ambiguous=True)
                if not self._merge_resolution_bindings(bindings, child_resolution):
                    return _ImportResolution(ambiguous=True)
            elif base_resolution.candidate is not None and self._module_proves_binding(
                base_resolution.candidate, alias.name
            ):
                continue
            else:
                # A resolved package/module does not prove that an ImportFrom
                # child is merely an attribute.  It may be supplied through a
                # runtime package path extension.
                return _ImportResolution(ambiguous=True)

        if not bindings:
            return _ImportResolution(ambiguous=True)
        return _ImportResolution(
            tuple(sorted(bindings)),
            module_names=tuple(sorted(bindings.items(), key=lambda item: item[0].as_posix())),
        )

    def _resolve_relative_target(
        self,
        identity_parts: tuple[str, ...],
        filesystem_parts: tuple[str, ...],
        roots: Iterable[Path],
    ) -> _ImportResolution:
        """Resolve one relative target while retaining its qualified identity."""
        paths: set[Path] = set()
        candidates: set[Path] = set()
        for root in dict.fromkeys(roots):
            resolution = self._resolve_path(root.joinpath(*filesystem_parts).resolve(), root)
            if resolution.ambiguous:
                return resolution
            paths.update(resolution.paths)
            if resolution.candidate is not None:
                candidates.add(resolution.candidate)
                if len(candidates) > 1:
                    return _ImportResolution(ambiguous=True)
        return self._resolution_for(identity_parts, paths, next(iter(candidates), None))

    @staticmethod
    def _relative_filesystem_prefix(
        identity_parts: tuple[str, ...],
        package_length: int,
        filesystem_package_parts: tuple[str, ...],
        prefix_length: int,
    ) -> tuple[str, ...] | None:
        """Map a qualified relative prefix back to the selected root suffix."""
        if prefix_length > len(identity_parts):
            return None
        omitted = package_length - len(filesystem_package_parts)
        if prefix_length <= package_length:
            return identity_parts[omitted:prefix_length] if prefix_length > omitted else ()
        return (
            *filesystem_package_parts,
            *identity_parts[package_length:prefix_length],
        )

    def _validate_relative_loaded_prefixes(
        self,
        identity_parts: tuple[str, ...],
        package_length: int,
        filesystem_package_parts: tuple[str, ...],
        importer_module_name: str,
        binding_name: str | None,
        bound_module_name: str | None = None,
        roots: Iterable[Path] = (),
    ) -> bool:
        """Validate loaded modules against relative targets for every qualified prefix."""
        for index in range(1, len(identity_parts) + 1):
            prefix_filesystem_parts = self._relative_filesystem_prefix(
                identity_parts,
                package_length,
                filesystem_package_parts,
                index,
            )
            if prefix_filesystem_parts is None:
                return False
            resolution = self._resolve_relative_target(
                identity_parts[:index], prefix_filesystem_parts, roots
            )
            if resolution.ambiguous:
                return False
            if not resolution.paths and self._loaded_prefix_reaches_source(identity_parts[:index]):
                return False
            if resolution.paths and self._loaded_module_mismatch(
                (".".join(identity_parts[:index]),),
                resolution,
                importer_module_name=importer_module_name,
                binding_name=binding_name,
                bound_module_name=bound_module_name,
                require_loaded=True,
            ):
                return False
        return True

    def _relative_package_context(
        self, importer: Path, importer_module_name: str
    ) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
        """Return qualified package identity and the nearest filesystem suffix."""
        module_parts = tuple(importer_module_name.split("."))
        identity_package_parts = (
            module_parts if importer.name == "__init__.py" else module_parts[:-1]
        )
        if not identity_package_parts:
            return None

        candidates: list[tuple[int, int, Path, tuple[str, ...]]] = []
        roots = (self.project_root, *self.source_dirs)
        for index, root in enumerate(dict.fromkeys(roots)):
            try:
                relative = importer.relative_to(root)
            except ValueError:
                continue
            if not relative.parts:
                continue
            candidates.append((len(root.parts), -index, root, relative.parent.parts))
        if not candidates:
            return None
        depth = max(candidate[0] for candidate in candidates)
        deepest = [candidate for candidate in candidates if candidate[0] == depth]
        filesystem_suffixes = {candidate[3] for candidate in deepest}
        if len(filesystem_suffixes) != 1:
            return None
        _depth, _order, _root, filesystem_package_parts = max(deepest)
        return identity_package_parts, filesystem_package_parts

    def _loaded_module_mismatch(
        self,
        module_names: Iterable[str],
        resolution: _ImportResolution,
        *,
        importer_module_name: str | None = None,
        binding_name: str | None = None,
        bound_module_name: str | None = None,
        require_loaded: bool = False,
    ) -> bool:
        """Reject a loaded configured module whose file differs from static resolution."""
        static_paths = set(resolution.paths)
        registry = _safe_sys_modules()
        if registry is None:
            return True
        for module_name in module_names:
            module = registry.get(module_name)
            if module is None:
                if not require_loaded:
                    continue
                module = self._importer_binding(
                    importer_module_name,
                    module_name,
                    binding_name,
                    bound_module_name,
                )
                if module is None:
                    return True
            if self._loaded_package_path_mismatch(module, resolution):
                return True
            module_file = _safe_runtime_attribute(module, "__file__")
            if not isinstance(module_file, str) or not module_file:
                continue
            try:
                runtime_path = Path(module_file).resolve()
            except (OSError, RuntimeError):
                return True
            if runtime_path in static_paths:
                continue
            if self._is_source_universe_path(runtime_path):
                return True
        return False

    @staticmethod
    def _loaded_package_path_mismatch(module: Any, resolution: _ImportResolution) -> bool:
        """Reject a package search path that is not the statically selected one."""
        runtime_search_path = _safe_runtime_attribute(module, "__path__")
        if runtime_search_path is None:
            return False
        path_entries = _safe_runtime_path_entries(runtime_search_path)
        if path_entries is _UNRESOLVED_RUNTIME_VALUE:
            return True
        try:
            runtime_paths = {Path(entry).resolve() for entry in path_entries}
        except (OSError, RuntimeError, TypeError):
            return True
        expected_paths = (
            {resolution.candidate.parent}
            if resolution.candidate is not None and resolution.candidate.name == "__init__.py"
            else set()
        )
        return runtime_paths != expected_paths

    @staticmethod
    def _importer_binding(
        importer_module_name: str | None,
        module_name: str,
        binding_name: str | None,
        bound_module_name: str | None,
    ) -> Any | None:
        """Inspect a static importer binding when its canonical registry entry is absent."""
        if importer_module_name is None or binding_name is None or bound_module_name is None:
            return None
        registry = _safe_sys_modules()
        if registry is None:
            return None
        importer = registry.get(importer_module_name)
        if importer is None:
            return None
        namespace = _safe_runtime_attribute(importer, "__dict__")
        if type(namespace) is not dict:
            return None
        value = namespace.get(binding_name)
        if value is None:
            return None
        if module_name == bound_module_name:
            return value
        if not bound_module_name or not module_name.startswith(bound_module_name + "."):
            return None
        suffix = module_name[len(bound_module_name) + 1 :].split(".")
        for part in suffix:
            namespace = _safe_runtime_attribute(value, "__dict__")
            if type(namespace) is not dict:
                return None
            value = namespace.get(part)
            if value is None:
                return None
        return value

    def _is_source_universe_path(self, path: Path) -> bool:
        """Return whether a runtime module belongs to a configured source path."""
        return any(_path_is_within(path, source_dir) for source_dir in self.source_dirs)

    def _known_external(self, top_level: str) -> bool:
        """Accept only imports proven outside the configured project source universe."""
        registry = _safe_sys_modules()
        if registry is None:
            return False
        module = registry.get(top_level)
        if module is not None:
            runtime_search_path = _safe_runtime_attribute(module, "__path__")
            if runtime_search_path is not None:
                path_entries = _safe_runtime_path_entries(runtime_search_path)
                if path_entries is _UNRESOLVED_RUNTIME_VALUE:
                    return False
                try:
                    search_paths = tuple(Path(entry).resolve() for entry in path_entries)
                except (OSError, RuntimeError, TypeError):
                    return False
                if any(
                    _path_is_within(path, source_dir) or _path_is_within(source_dir, path)
                    for path in search_paths
                    for source_dir in self.source_dirs
                ):
                    return False
        if top_level in sys.builtin_module_names or top_level in sys.stdlib_module_names:
            return True
        if module is None:
            return False
        module_file = _safe_runtime_attribute(module, "__file__")
        if not isinstance(module_file, str) or not module_file:
            return False
        try:
            runtime_path = Path(module_file).resolve()
        except (OSError, RuntimeError):
            return False
        return not self._is_source_universe_path(runtime_path)

    def _resolve_absolute(self, parts: tuple[str, ...]) -> _ImportResolution:
        if not parts:
            return _ImportResolution()
        paths: set[Path] = set()
        candidates: set[Path] = set()
        roots = (self.project_root, *self.source_dirs)
        for root in dict.fromkeys(roots):
            resolution = self._resolve_path(root.joinpath(*parts).resolve(), root)
            if resolution.ambiguous:
                return resolution
            if resolution.candidate is not None:
                candidates.add(resolution.candidate)
                if len(candidates) > 1:
                    return _ImportResolution(ambiguous=True)
            paths.update(resolution.paths)
        return self._resolution_for(parts, paths, next(iter(candidates), None))

    def _resolve_path(self, base: Path, package_root: Path) -> _ImportResolution:
        file_path = (base.parent / f"{base.name}.py").resolve()
        package_path = (base / "__init__.py").resolve()
        matches = [path for path in (file_path, package_path) if path in self.source_files]
        if len(matches) > 1:
            return _ImportResolution(ambiguous=True)
        if not matches:
            return _ImportResolution()

        paths = {matches[0]}
        parent = matches[0].parent
        while parent != parent.parent and parent != package_root:
            init = (parent / "__init__.py").resolve()
            if init in self.source_files:
                paths.add(init)
            parent = parent.parent
        return _ImportResolution(tuple(sorted(paths)), candidate=matches[0])


_REFLECTION_NAMES = frozenset(
    {
        "getattr",
        "globals",
        "locals",
        "vars",
        "__dict__",
        "__getattribute__",
        "__builtins__",
    }
)
_REFLECTION_ATTRIBUTES = _REFLECTION_NAMES | {"modules"}


def _module_uses_ambiguous_reflection(tree: ast.Module) -> bool:
    """Reject common reflection because syntax-only analysis cannot prove its target."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id in _REFLECTION_NAMES:
            return True
        if isinstance(node, ast.Attribute) and node.attr in _REFLECTION_ATTRIBUTES:
            return True
        if isinstance(node, ast.Import):
            if any(
                alias.name == "builtins" or alias.name.startswith("builtins.")
                for alias in node.names
            ):
                return True
        elif isinstance(node, ast.ImportFrom):
            if node.module == "builtins":
                return True
            if node.module == "sys" and any(alias.name == "modules" for alias in node.names):
                return True
    # False positives only widen the cache input; a false negative can replay stale output.
    return False


def _module_imports_are_ambiguous(tree: ast.Module) -> bool:
    """Reject every import shape whose runtime reachability is not statically certain."""
    if _module_uses_ambiguous_reflection(tree):
        return True
    top_level_imports = {
        id(statement)
        for statement in tree.body
        if isinstance(statement, (ast.Import, ast.ImportFrom))
    }
    dynamic_calls = set(_StaticLibsAnalyzer._DYNAMIC_CALLS)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            if id(node) not in top_level_imports:
                return True
            if isinstance(node, ast.Import):
                if any(
                    alias.name == "importlib" or alias.name.startswith("importlib.")
                    for alias in node.names
                ):
                    return True
            elif node.module is not None and (
                node.module == "importlib" or node.module.startswith("importlib.")
            ):
                dynamic_calls.update(
                    alias.asname or alias.name
                    for alias in node.names
                    if alias.name in _StaticLibsAnalyzer._IMPORT_LOOKUP_ATTRIBUTES
                )
                return True
            elif node.module == "builtins":
                dynamic_calls.update(
                    alias.asname or alias.name for alias in node.names if alias.name == "__import__"
                )
            if isinstance(node, ast.ImportFrom) and any(alias.name == "*" for alias in node.names):
                return True

    for node in ast.walk(tree):
        if _is_dynamic_callable_reference(node, dynamic_calls):
            return True

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "__getattr__":
            return True
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets: Iterable[ast.AST] = (
                node.targets if isinstance(node, ast.Assign) else (node.target,)
            )
            if any(
                isinstance(target, ast.Name) and target.id == "__getattr__" for target in targets
            ):
                return True
    return False


def _is_dynamic_callable_reference(node: ast.AST, dynamic_calls: set[str]) -> bool:
    """Recognize dynamic-callable references at any AST position."""
    if isinstance(node, ast.Name):
        return node.id in dynamic_calls
    if isinstance(node, ast.Attribute):
        return node.attr in _StaticLibsAnalyzer._IMPORT_LOOKUP_ATTRIBUTES
    if isinstance(node, ast.Call):
        return (
            isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and _static_string(node.args[1]) in _StaticLibsAnalyzer._IMPORT_LOOKUP_ATTRIBUTES
        )
    if isinstance(node, ast.Subscript):
        return (
            isinstance(node.value, ast.Attribute)
            and node.value.attr == "__dict__"
            and _static_string(node.slice) in _StaticLibsAnalyzer._IMPORT_LOOKUP_ATTRIBUTES
        )
    return False


def _static_string(node: ast.AST) -> str | None:
    """Return a literal string value, keeping reflective lookup recognition syntax-only."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


@dataclass(frozen=True)
class _FunctionOwnerFacts:
    """Retained function facts used to validate owner identity after detachment."""

    module_name: str | None
    module_path: Path | None
    consistent: bool
    function_module: str | None
    globals_name: str | None
    path_facts: tuple[Path, ...]


def _function_owner_facts(function: Callable[..., Any]) -> _FunctionOwnerFacts:
    """Recover and cross-check a function's module/name/path facts.

    ``inspect.getmodule`` is only a live-registry convenience: it is unavailable
    for retained functions after their module is removed from ``sys.modules``.
    The function attributes, its globals namespace and its code/source path are
    retained facts; disagreement between them is treated as untrusted.
    """
    raw_module = _safe_runtime_attribute(function, "__module__")
    function_module = raw_module if isinstance(raw_module, str) and raw_module else None
    globals_map = _function_globals(function)
    raw_globals_name = globals_map.get("__name__") if type(globals_map) is dict else None
    globals_name = (
        raw_globals_name if isinstance(raw_globals_name, str) and raw_globals_name else None
    )
    module_name = function_module or globals_name
    consistent = function_module is None or globals_name is None or function_module == globals_name

    code = _function_code(function)
    candidate_paths: list[Path] = []
    code_filename = _safe_runtime_attribute(code, "co_filename")
    if isinstance(code_filename, str) and code_filename and not code_filename.startswith("<"):
        try:
            candidate_paths.append(Path(code_filename).resolve())
        except (OSError, RuntimeError):
            consistent = False
    try:
        source_filename = inspect.getsourcefile(function)
    except (OSError, TypeError, ValueError):
        source_filename = None
    if isinstance(source_filename, str) and source_filename and not source_filename.startswith("<"):
        try:
            candidate_paths.append(Path(source_filename).resolve())
        except (OSError, RuntimeError):
            consistent = False

    path_facts = tuple(dict.fromkeys(candidate_paths))
    if len(path_facts) > 1:
        consistent = False
    module_path = path_facts[0] if path_facts and path_facts[0].suffix == ".py" else None
    if path_facts and module_path is None:
        consistent = False

    registry = _safe_sys_modules()
    if registry is None:
        consistent = False
    for name in {function_module, globals_name} - {None}:
        module = registry.get(name) if registry is not None else None
        if module is None:
            continue
        if _safe_runtime_attribute(module, "__name__") not in {name, _UNRESOLVED_RUNTIME_VALUE}:
            consistent = False
        module_file = _safe_runtime_attribute(module, "__file__")
        if module_file is _UNRESOLVED_RUNTIME_VALUE:
            module_file = None
        if module_file is not None and type(module_file) is not str:
            consistent = False
            continue
        if module_path is None or module_file is None:
            continue
        try:
            if Path(module_file).resolve() != module_path:
                consistent = False
        except (OSError, RuntimeError):
            consistent = False

    return _FunctionOwnerFacts(
        module_name=module_name,
        module_path=module_path,
        consistent=consistent,
        function_module=function_module,
        globals_name=globals_name,
        path_facts=path_facts,
    )


def _function_module_path(function: Callable[..., Any]) -> Path | None:
    """Return a validated function module path, including detached functions."""
    facts = _function_owner_facts(function)
    if not facts.consistent or facts.module_name is None:
        return None
    return facts.module_path


def _function_module_name(function: Callable[..., Any]) -> str | None:
    """Return a validated function module name, including detached functions."""
    facts = _function_owner_facts(function)
    if not facts.consistent or facts.module_path is None:
        return None
    return facts.module_name


def _path_is_within(path: Path, directory: Path) -> bool:
    """Return whether a resolved path belongs to a resolved directory."""
    try:
        path.resolve().relative_to(directory.resolve())
    except ValueError:
        return False
    return True


def _source_hash(function: NodeFunction) -> str:
    try:
        source = textwrap.dedent(inspect.getsource(function))
    except (OSError, TypeError) as error:
        raise ValueError(f"Cannot inspect source for node {function!r}") from error
    parsed = ast.parse(source)
    normalized = _DocstringStripper().visit(parsed)
    return sha(ast.dump(normalized, annotate_fields=True, include_attributes=False))


def _callable_provenance(function: Callable[..., Any] | None) -> dict[str, Any] | None:
    """Return stable execution provenance for a dynamic map/scan callback."""
    if function is None:
        return None
    module = _safe_runtime_attribute(function, "__module__")
    qualname = _safe_runtime_attribute(function, "__qualname__")
    if not isinstance(module, str):
        module = None
    if not isinstance(qualname, str):
        qualname = None
    state = _transactional_runtime_state_material(function, _RuntimeStateContext({}))
    if state is _UNREPRESENTABLE_RUNTIME_STATE:
        state = {
            "type": "unrepresentable",
            "class": _runtime_type_identity(type(function)),
        }
    return {
        "module": module,
        "qualname": qualname,
        "state": state,
    }


def _resolve_items_from(
    node_name: str,
    source_name: str,
    artifact_path: str,
    source_artifact: Mapping[str, Any],
) -> Any:
    """Resolve a map list path by descending every dot-separated segment."""
    current: Any = source_artifact
    traversed: list[str] = []
    for segment in artifact_path.split("."):
        if not isinstance(current, Mapping):
            prefix = ".".join(traversed) or "<artifact>"
            raise ValueError(
                f"Map node {node_name!r} items_from path {artifact_path!r} from "
                f"{source_name!r} broke at {segment!r}: {prefix!r} is not a Mapping"
            )
        if segment not in current:
            raise ValueError(
                f"Map node {node_name!r} items_from path {artifact_path!r} from "
                f"{source_name!r} broke at {segment!r}: key is missing"
            )
        current = current[segment]
        traversed.append(segment)
    return current


def _validate_artifact_locator(value: tuple[str, str], name: str) -> None:
    """Require the shared ``(node_name, artifact_path)`` shape used by map and scan."""
    if not (
        isinstance(value, tuple)
        and len(value) == 2
        and all(isinstance(part, str) and part for part in value)
    ):
        raise ValueError(f"{name} must be a non-empty (node_name, artifact_key) tuple")


def _validate_registration(function: NodeFunction) -> _NodeAstMetadata:
    """执行注册期守卫，并尽力提取供 describe 使用的 AST 摘要。"""
    source_path = inspect.getsourcefile(function)
    if source_path is None:
        raise ValueError(f"Cannot inspect source for node {function!r}")
    try:
        source_lines, start_line = inspect.getsourcelines(function)
    except (OSError, TypeError) as error:
        raise ValueError(f"Cannot inspect source for node {function!r}") from error
    source = textwrap.dedent("".join(source_lines))
    findings = check_source(source, Path(source_path))
    violations = [finding for finding in findings if not finding.waived]
    if violations:
        locations = "\n".join(
            f"{finding.path}:{start_line + finding.lineno - 1}: {finding.snippet}"
            for finding in violations
        )
        message = "Raw LLM calls inside loops are not allowed in node registration:\n"
        raise ValueError(message + locations)
    parameters = tuple(inspect.signature(function).parameters)
    context_name = parameters[-1] if parameters else "ctx"
    raw_io_findings = check_raw_io_source(
        source,
        Path(source_path),
        context_name=context_name,
    )
    raw_io_violations = [finding for finding in raw_io_findings if not finding.waived]
    if raw_io_violations:
        locations = "\n".join(
            f"{finding.path}:{start_line + finding.lineno - 1}: {finding.snippet}"
            for finding in raw_io_violations
        )
        message = (
            "Raw file reads are not allowed in node registration; "
            "use ctx.read_text or ctx.read_bytes:\n"
        )
        raise ValueError(message + locations)
    return _extract_node_ast_metadata(source, function.__globals__)


def _extract_node_ast_metadata(source: str, globals_: Mapping[str, Any]) -> _NodeAstMetadata:
    """从节点函数源码提取可验证模型与检查点；解析失败时宁可缺席。"""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return _NodeAstMetadata()
    visitor = _NodeAstMetadataVisitor(globals_)
    visitor.visit(tree)
    return _NodeAstMetadata(
        tuple(visitor.models),
        tuple(visitor.model_classes),
        tuple(visitor.checkpoints),
    )


class _NodeAstMetadataVisitor(ast.NodeVisitor):
    """只收集稳定可判定的调用形态，动态值不做猜测。"""

    def __init__(self, globals_: Mapping[str, Any]) -> None:
        self.globals = globals_
        self.models: list[dict[str, Any]] = []
        self.model_classes: list[type[pydantic.BaseModel]] = []
        self.checkpoints: list[str] = []

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 -- ast visitor protocol.
        if isinstance(node.func, ast.Attribute) and node.func.attr == "call_validated":
            self._record_model(node)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "checkpoint":
            self._record_checkpoint(node)
        self.generic_visit(node)

    def _record_model(self, node: ast.Call) -> None:
        if len(node.args) < 2:
            return
        model = _resolve_global_reference(node.args[1], self.globals)
        if not isinstance(model, type) or not issubclass(model, pydantic.BaseModel):
            return
        summary = {
            "model": model.__name__,
            "fields": {
                name: _annotation_string(field.annotation)
                for name, field in model.model_fields.items()
            },
        }
        if summary not in self.models:
            self.models.append(summary)
        if model not in self.model_classes:
            self.model_classes.append(model)

    def _record_checkpoint(self, node: ast.Call) -> None:
        name = "<动态>"
        if (
            node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            name = node.args[0].value
        if name not in self.checkpoints:
            self.checkpoints.append(name)


def _resolve_global_reference(expression: ast.expr, globals_: Mapping[str, Any]) -> Any | None:
    """解析简单名称或模块属性；异常说明该 AST 结果不能诚实给出。"""
    try:
        if isinstance(expression, ast.Name):
            return globals_.get(expression.id)
        if isinstance(expression, ast.Attribute):
            parent = _resolve_global_reference(expression.value, globals_)
            return getattr(parent, expression.attr) if parent is not None else None
    except Exception:
        return None
    return None


def _annotation_string(annotation: Any) -> str:
    """把 Pydantic 字段注解化为紧凑且稳定的人类可读文本。"""
    try:
        forward_arg = getattr(annotation, "__forward_arg__", None)
        if isinstance(forward_arg, str):
            return forward_arg
        return annotation.__name__ if isinstance(annotation, type) else str(annotation)
    except Exception:
        return repr(annotation)


def _bytes_hash(contents: bytes) -> str:
    return sha256(contents).hexdigest()


def _validate_name(name: str, kind: str) -> None:
    if name in {"models", "subgraphs"}:
        raise ValueError(f"{kind} name {name!r} is reserved for declaration metadata")
    path = Path(name) if isinstance(name, str) else None
    if (
        path is None
        or not name
        or "@" in name
        or "/" in name
        or "\\" in name
        or path.name != name
        or name in {".", ".."}
    ):
        raise ValueError(
            f"{kind} names must be non-empty, contain no '@', and be a single relative path "
            "component"
        )


def _validate_retry_policy(retry: RetryPolicy | None) -> RetryPolicy | None:
    if retry is not None and not isinstance(retry, RetryPolicy):
        raise TypeError("retry must be RetryPolicy or None")
    return retry


def _validate_resources(
    resources: Iterable[ResourceRequest] | None,
    kind: str,
) -> tuple[ResourceRequest, ...]:
    """Freeze and validate the resource declarations attached to one node."""
    if resources is None:
        return ()
    try:
        frozen = tuple(resources)
    except TypeError as error:
        raise TypeError(f"{kind} resources must be an iterable of ResourceRequest") from error
    if not all(isinstance(resource, ResourceRequest) for resource in frozen):
        raise TypeError(f"{kind} resources must contain only ResourceRequest values")
    return frozen
