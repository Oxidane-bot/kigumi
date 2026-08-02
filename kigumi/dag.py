"""调度层：注册、校验、缓存键计算与 DAG 执行。

存储路径、artifact 落盘、归档、物化、审批和 GC 由 ``kigumi.store`` 负责；本模块仅依赖它。
"""

from __future__ import annotations

import argparse
import ast
import copy
import getpass
import inspect
import json
import os
import sys
import textwrap
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
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
    observe,
)
from .config import KigumiConfig
from .enforce import check_paths, check_raw_io_node_paths, check_raw_io_source, check_source
from .errors import OutputOwnershipError
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
from .retry import RetryExhausted, RetryPolicy
from .slots import FileSlots, SlotTimeoutError
from .subgraph import Subgraph

NodeFunction = Callable[[dict[str, dict[str, Any]], "NodeContext"], dict[str, Any]]
MapFunction = Callable[[Any, dict[str, dict[str, Any]], "NodeContext"], dict[str, Any]]
ScanFunction = Callable[[Any, Any, dict[str, dict[str, Any]], "NodeContext"], dict[str, Any]]
AggregateFunction = Callable[[dict[str, dict[str, Any]], list[str]], dict[str, Any]]
PostNodeHook = Callable[[str, dict[str, Any], bool], None]
_NO_CARRY = object()
_NO_ITEM = object()
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
    ) -> None:
        self._dag = dag
        self._node = node
        self._run_id = run_id
        self._checkpoint_suffix = checkpoint_suffix
        self._item_files = item_files
        self._checkpoint_used = False
        self._prompt_resolutions = dict(prompt_resolutions or {})

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
        return self._checked_path(path).read_text(encoding=encoding)

    def read_bytes(self, path: str | Path) -> bytes:
        """读取已在 ``files`` 或当前项 ``files_fn`` 中声明的二进制文件。"""
        return self._checked_path(path).read_bytes()

    def _checked_path(self, path: str | Path) -> Path:
        resolved = self._dag.config.resolve(path)
        declared = {
            self._dag.config.resolve(declared_path)
            for declared_path in (*self._node.files, *self._item_files)
        }
        if resolved not in declared:
            raise UndeclaredInputError(
                f"Node {self._node.name!r} attempted to read undeclared file {resolved}. "
                "在 files= 或 files_fn 中声明该文件。"
            )
        return resolved

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
        if workers < 1:
            raise ValueError("workers must be at least 1")
        requested_force = tuple(force)
        existing_manifest: dict[str, Any] | None = None
        if run_id is not None:
            manifest_path = store.run_directory(self.config.artifacts_path, run_id) / "_run.json"
            try:
                candidate_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (FileNotFoundError, OSError, json.JSONDecodeError):
                candidate_manifest = None
            if isinstance(candidate_manifest, dict):
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
        libs_hashes = self._libs_hashes(self._nodes[name] for name in order)
        prompt_snapshot = self._prompt_snapshot()
        attempt_store = AttemptStore(
            run_dir,
            self._run_manifest_identity(
                current_run_id,
                selected,
                requested_force,
                order,
                libs_hashes,
                prompt_snapshot,
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
            evidence_policy = node.evidence_policy or self._caller_evidence_policy()
            agent_provenance: dict[str, Any] | None = None
            resumed_completed = False
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
            if node.items_from is None:
                function_inputs = self._function_inputs(node, inputs)
                file_contents = self._file_contents(node)
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
                if (
                    existing_manifest is not None
                    and run_artifact.is_file()
                    and run_sidecar.is_file()
                ):
                    artifact, prior_metadata = self._resume_completed_artifact(
                        run_dir,
                        node.name,
                        key_components,
                        cache_key,
                        validate_agent=node.executor == "agent",
                        prompt_resolutions=prompt_resolution_records,
                    )
                    origin = prior_metadata.get("origin_provenance")
                    if isinstance(origin, dict) and isinstance(origin.get("agent"), dict):
                        agent_provenance = copy.deepcopy(origin["agent"])
                    cache_hit = prior_metadata.get("cache") == "hit"
                    resumed_completed = True
                elif prior_state is not None:
                    artifact, cache_hit = None, False
                else:
                    artifact, cache_hit = envelope.lookup(
                        cache_key,
                        forced=node.name in forced_nodes,
                        cache_policy=node.cache,
                        evidence_policy_digest=evidence_policy.digest,
                    )
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
                origin = store.read_node_cache_origin(
                    self.config.artifacts_path,
                    cache_key,
                )
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
                        artifact, cache_hit, item_cache_keys, item_statuses = execute_dynamic(
                            node,
                            inputs,
                            upstream_shas,
                            current_run_id,
                            libs_hashes[node.name],
                            workers,
                            permit_plane=permit_plane,
                            executor=execution_executor,
                            forced_all=node.name in forced_nodes,
                            forced_items=forced_items.get(node.name, set()),
                            envelope=envelope,
                            attempt_store=attempt_store,
                            prompt_snapshot=prompt_snapshot,
                            budget_abort=budget_abort,
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
                                                if node.retry is not None:
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
                        if not resumed_completed:
                            # miss 路径喂下游的必须与命中路径同形态:命中读的是
                            # 排序后的磁盘 JSON,活字典键序不能让下游 prompt 漂移。
                            artifact = envelope.seal(
                                artifact,
                                cache_key,
                                label=f"Node {node.name!r}",
                                calls=calls,
                                cache_policy=("off" if checkpoint_used else node.cache),
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
                                cache_hit=cache_hit,
                                seconds=elapsed,
                                calls=calls,
                                key_components=key_components,
                                outputs=outputs,
                                cache_policy=node.cache,
                                evidence_policy=evidence_policy,
                                agent_provenance=agent_provenance,
                                prompt_resolutions=prompt_resolution_records,
                            )
                        if node.items_from is None and artifact is not None and not cache_hit:
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
            attempts = AttemptStore(
                run_dir,
                self._run_manifest_identity(
                    run_id,
                    tuple(targets),
                    tuple(force),
                    order,
                    libs_hashes,
                    prompt_snapshot,
                ),
            )
            attempts.initialize()
        except (OSError, RunManifestError, ValueError) as error:
            raise ValueError(f"Run {run_id!r} declaration cannot be recovered: {error}") from error

        state = attempts.state_for(target)
        if state is None:
            raise ValueError(f"Recovery target {target!r} has no durable attempt state")
        if state.get("status") != "failed":
            raise ValueError(f"Recovery target {target!r} is not terminally failed")
        if state.get("attempt") != from_attempt:
            raise ValueError(
                f"Recovery target {target!r} is at attempt {state.get('attempt')}, "
                f"not {from_attempt}"
            )

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
        if decision == "fail":
            _write_recovery_receipt(run_dir, receipt_payload)
            return receipt

        inherited_nodes = self._recovery_inherited_nodes(
            run_dir,
            target_root,
            order,
        )
        _write_recovery_receipt(run_dir, receipt_payload)
        attempts.schedule_recovery(
            target,
            from_attempt=from_attempt,
            to_attempt=to_attempt,
            recovery=receipt_payload,
            inherited_nodes=inherited_nodes,
        )
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
    ) -> dict[str, Any]:
        """Describe successful run-local artifacts that the retry will inherit."""
        downstream = _downstream_nodes(self._nodes, {target})
        attempts = AttemptStore(run_dir, {})
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

    def _run_manifest_identity(
        self,
        run_id: str,
        targets: tuple[str, ...],
        force: tuple[str, ...],
        order: list[str],
        libs_hashes: Mapping[str, str],
        prompt_snapshot: PromptCatalogSnapshot,
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
                    path.as_posix(): _bytes_hash(self.config.resolve(path).read_bytes())
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
                "retry_policy_digest": retry_digests[name],
                "evidence_policy_digest": evidence.digest,
            }
        source_digest = sha(declarations)
        static_profile = self._static_workflow_profile(prompt_snapshot)
        libs_digest = sha(libs_hashes)
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
        return artifact, metadata

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
        libs_hashes = self._libs_hashes(self._nodes[name] for name in order)
        prompt_snapshot = _prompt_snapshot or self._prompt_snapshot(
            self._nodes[name] for name in order
        )
        nodes: dict[str, str] = {}
        pending_on: dict[str, tuple[str, ...]] = {}
        artifact_shas: dict[str, str] = {}
        artifacts: dict[str, dict[str, Any]] = {}

        for node_name in order:
            node = self._nodes[node_name]
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
                cache_lookup = store.read_node_cache(self.config.artifacts_path, cache_key)
                artifact = (
                    None
                    if node.cache != "auto" or node_name in forced_nodes
                    else cache_lookup.data
                    if cache_lookup.state == "VALID"
                    else None
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
                    cache_lookup = store.read_node_cache(self.config.artifacts_path, cache_key)
                    artifact = (
                        None
                        if node.cache != "auto"
                        or node_name in forced_nodes
                        or item_id in forced_items.get(node_name, set())
                        else cache_lookup.data
                        if cache_lookup.state == "VALID"
                        else None
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
                if previous_pending is None and (
                    entries or (node.cache == "auto" and node_name not in forced_nodes)
                ):
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
                cache_lookup = store.read_node_cache(self.config.artifacts_path, cache_key)
                artifact = (
                    None
                    if node.cache != "auto"
                    or node_name in forced_nodes
                    or item_id in forced_items.get(node_name, set())
                    else cache_lookup.data
                    if cache_lookup.state == "VALID"
                    else None
                )
                status = "hit" if artifact is not None else "miss"
                nodes[f"{node_name}@{item_id}"] = status
                item_statuses.append(status)
                item_ids.append(item_id)
                if artifact is not None:
                    completed[item_id] = artifact
            if (
                node.cache == "auto"
                and node_name not in forced_nodes
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
                cache_lookup = store.read_node_cache(self.config.artifacts_path, sha(component))
                artifact = cache_lookup.data if cache_lookup.state == "VALID" else None
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
                cache_lookup = store.read_node_cache(self.config.artifacts_path, sha(component))
                artifact = cache_lookup.data if cache_lookup.state == "VALID" else None
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
                cache_lookup = store.read_node_cache(self.config.artifacts_path, sha(component))
                artifact = cache_lookup.data if cache_lookup.state == "VALID" else None
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
    ) -> tuple[dict[str, Any], bool, list[str], dict[str, str]]:
        """Run a map's runtime list without exposing its items as graph vertices."""
        assert node.items_from is not None
        source_name, _ = node.items_from
        entries = self._map_entries(node, inputs)
        ids = [item_id for item_id, _ in entries]
        unknown_forced = sorted(forced_items - set(ids))
        if unknown_forced:
            forced_names = ", ".join(f"{node.name}@{item_id}" for item_id in unknown_forced)
            raise ValueError(f"Unknown forced map items: {forced_names}")

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
            try:
                with observe() as calls:
                    item_files = (
                        tuple(Path(path) for path in node.files_fn(item)) if node.files_fn else ()
                    )
                    file_contents = self._file_contents(node, item_files)
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
                    resumed_completed = False
                    run_root = envelope.artifacts_path / "runs" / run_id
                    if (run_root / f"{target}.json").is_file() and (
                        run_root / f"{target}.json.meta.json"
                    ).is_file():
                        artifact, prior_metadata = self._resume_completed_artifact(
                            run_root,
                            target,
                            key_components,
                            cache_key,
                            prompt_resolutions=prompt_resolution_records,
                        )
                        cache_hit = prior_metadata.get("cache") == "hit"
                        resumed_completed = True
                    elif attempt_store.state_for(target) is not None:
                        artifact, cache_hit = None, False
                    else:
                        artifact, cache_hit = envelope.lookup(
                            cache_key,
                            forced=forced_all or item_id in forced_items,
                            cache_policy=node.cache,
                            evidence_policy_digest=self._caller_evidence_policy().digest,
                        )
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
                                cache_policy=("off" if checkpoint_used else node.cache),
                                evidence_policy=self._caller_evidence_policy(),
                                prompt_resolutions=prompt_resolution_records,
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
                        seconds=outcome["seconds"],
                        calls=outcome["calls"],
                        key_components=outcome["key_components"],
                        outputs=outputs,
                        cache_policy=node.cache,
                        evidence_policy=self._caller_evidence_policy(),
                        prompt_resolutions=outcome["prompt_resolutions"],
                    )
                if outcome["cache"] != "hit" and not outcome["resumed_completed"]:
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
            if isinstance(first, (RetryExhausted, ProviderFailure, BudgetExceeded)):
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
        return (
            artifact,
            node.cache == "auto"
            and not forced_all
            and all(
                outcome["status"] == "success" and outcome["cache"] == "hit" for outcome in outcomes
            ),
            cache_keys,
            item_cache_statuses,
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
    ) -> tuple[dict[str, Any], bool, list[str], dict[str, str]]:
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

        for item_id, item in entries:
            started = time.monotonic()
            target = f"{node.name}@{item_id}"
            evidence_policy = (
                node.evidence_policy
                if node.executor == "agent" and node.evidence_policy is not None
                else self._caller_evidence_policy()
            )
            agent_provenance: dict[str, Any] | None = None
            try:
                with observe() as calls:
                    item_files = (
                        tuple(Path(path) for path in node.files_fn(item)) if node.files_fn else ()
                    )
                    file_contents = self._file_contents(node, item_files)
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
                    resumed_completed = False
                    run_root = envelope.artifacts_path / "runs" / run_id
                    if (run_root / f"{target}.json").is_file() and (
                        run_root / f"{target}.json.meta.json"
                    ).is_file():
                        artifact, prior_metadata = self._resume_completed_artifact(
                            run_root,
                            target,
                            key_components,
                            cache_key,
                            validate_agent=node.executor == "agent",
                            prompt_resolutions=prompt_resolution_records,
                        )
                        origin = prior_metadata.get("origin_provenance")
                        if isinstance(origin, dict) and isinstance(origin.get("agent"), dict):
                            agent_provenance = copy.deepcopy(origin["agent"])
                        cache_hit = prior_metadata.get("cache") == "hit"
                        resumed_completed = True
                    elif attempt_store.state_for(target) is not None:
                        artifact, cache_hit = None, False
                    else:
                        artifact, cache_hit = envelope.lookup(
                            cache_key,
                            forced=forced_all or item_id in forced_items,
                            cache_policy=node.cache,
                            evidence_policy_digest=evidence_policy.digest,
                        )
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
                                                if node.retry is not None:
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
                                cache_policy=("off" if checkpoint_used else node.cache),
                                evidence_policy=evidence_policy,
                                agent_provenance=agent_provenance,
                                prompt_resolutions=prompt_resolution_records,
                            )
                    if node.executor == "agent":
                        validate_agent_artifact(artifact, self.blob_store)
                        if isinstance(agent_provenance, dict):
                            validate_agent_provenance(agent_provenance, self.blob_store)
                    completed[item_id] = artifact
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
                            seconds=time.monotonic() - started,
                            calls=calls,
                            key_components=key_components,
                            outputs=outputs,
                            cache_policy=node.cache,
                            evidence_policy=evidence_policy,
                            agent_provenance=agent_provenance,
                            prompt_resolutions=prompt_resolution_records,
                        )
                    if not cache_hit and not resumed_completed:
                        attempt_store.mark_completed(
                            target,
                            artifact_sha256=sha(artifact),
                        )
                    carry = node.carry_fn(artifact) if node.carry_fn is not None else artifact
            except (_MapCheckpointPending, _MapRetryPending):
                raise
            except OutputOwnershipError:
                raise
            except Exception as error:
                if isinstance(error, BudgetExceeded):
                    budget_abort.set()
                    raise
                if isinstance(error, (RetryExhausted, ProviderFailure, AgentExecutionFailure)):
                    raise
                raise RuntimeError(
                    f"Scan node {node.name!r} failed item {item_id!r}: "
                    f"{type(error).__name__}: {error}"
                ) from error

        artifact = self._aggregate_map_artifact(node, completed, ids)
        return (
            artifact,
            node.cache == "auto"
            and not forced_all
            and all(status == "hit" for status in item_cache_statuses.values()),
            cache_keys,
            item_cache_statuses,
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
    ) -> dict[str, bytes]:
        """Capture each declared file once for prompt resolution and cache keying."""
        return {
            str(path): self.config.resolve(path).read_bytes() for path in (*node.files, *item_files)
        }

    def _libs_hash(self, node: _Node) -> str:
        """Hash the configured library files statically reachable from one node."""
        return self._libs_hashes((node,))[node.name]

    def _libs_hashes(self, nodes: Iterable[_Node]) -> dict[str, str]:
        """Compute one immutable per-node library snapshot for a run or read-only query."""
        selected = tuple(nodes)
        snapshot = _capture_libs_source_snapshot(self.config.source_paths)
        all_files_hash = _all_libs_hash(snapshot.entries)
        if snapshot.has_syntax_error:
            return {node.name: all_files_hash for node in selected}
        analyzer = _StaticLibsAnalyzer(
            self.config.project_root,
            self.config.source_paths,
            snapshot,
        )
        return {node.name: analyzer.hash_for(node.function, all_files_hash) for node in selected}

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


def _write_recovery_receipt(run_dir: Path, payload: dict[str, Any]) -> Path:
    """Persist one recovery record without replacing an earlier record."""
    stem = f"recovery-{payload['recovery_time']}"
    path = run_dir / f"{stem}.json"
    suffix = 1
    while path.exists():
        path = run_dir / f"{stem}-{suffix}.json"
        suffix += 1
    atomic_write_json(path, payload)
    return path


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


@dataclass(frozen=True)
class _LibsSourceSnapshot:
    """One read-only view of configured source files for one keying operation."""

    entries: tuple[tuple[Path, str], ...]
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


def _capture_libs_source_snapshot(source_dirs: Iterable[Path]) -> _LibsSourceSnapshot:
    """Read configured Python files once, retaining the legacy ordered all-files view."""
    entries: list[tuple[Path, str]] = []
    texts: dict[Path, str] = {}
    for source_dir in source_dirs:
        if not source_dir.is_dir():
            continue
        for raw_path in sorted(source_dir.rglob("*.py")):
            path = raw_path.resolve()
            text = texts.get(path)
            if text is None:
                text = path.read_text(encoding="utf-8")
                texts[path] = text
            entries.append((path, text))

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
        texts,
        trees,
        frozenset(texts),
        has_syntax_error,
    )


def _all_libs_hash(entries: Iterable[tuple[Path, str]]) -> str:
    """Preserve the pre-granularity all-files digest byte-for-byte in its inputs."""
    return sha([_module_code_text(text) for _, text in entries])


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
        self.source_files = snapshot.source_files
        self._texts = dict(snapshot.texts)
        self._trees = dict(snapshot.trees)
        self._reachable_cache: dict[tuple[Path, str], set[Path] | None] = {}

    def hash_for(self, function: Callable[..., Any], fallback: str) -> str:
        """Return a selected-files hash, or the exact all-files fallback on uncertainty."""
        module_path = _function_module_path(function)
        module_name = _function_module_name(function)
        if module_path is None or module_name is None:
            return fallback
        reachable = self._reachable_paths(module_path, module_name)
        if reachable is None:
            return fallback
        return sha([_module_code_text(self._texts[path]) for path in sorted(reachable)])

    def _reachable_paths(self, root: Path, module_name: str) -> set[Path] | None:
        cache_key = (root, module_name)
        if cache_key in self._reachable_cache:
            cached = self._reachable_cache[cache_key]
            return None if cached is None else set(cached)

        initial = {root}
        if root in self.source_files:
            module_resolution = self._resolve_absolute(tuple(module_name.split(".")))
            if module_resolution.ambiguous:
                self._reachable_cache[cache_key] = None
                return None
            if root in module_resolution.paths:
                initial.update(module_resolution.paths)
            else:
                initial.update(self._ancestor_package_inits(root))
        queue = list(initial)
        seen: set[Path] = set()
        reachable: set[Path] = set()
        while queue:
            path = queue.pop()
            if path in seen:
                continue
            seen.add(path)
            tree = self._tree(path)
            if tree is None or _module_imports_are_ambiguous(tree):
                self._reachable_cache[cache_key] = None
                return None
            if path in self.source_files:
                reachable.add(path)
            for statement in tree.body:
                if not isinstance(statement, (ast.Import, ast.ImportFrom)):
                    continue
                resolution = self._resolve_import(path, statement)
                if resolution.ambiguous:
                    self._reachable_cache[cache_key] = None
                    return None
                queue.extend(resolution.paths)

        self._reachable_cache[cache_key] = set(reachable)
        return reachable

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

    def _resolve_import(
        self,
        importer: Path,
        statement: ast.Import | ast.ImportFrom,
    ) -> _ImportResolution:
        if isinstance(statement, ast.Import):
            paths: set[Path] = set()
            for alias in statement.names:
                resolution = self._resolve_absolute(tuple(alias.name.split(".")))
                if resolution.ambiguous:
                    return resolution
                if self._loaded_module_mismatch((alias.name,), resolution):
                    return _ImportResolution(ambiguous=True)
                if not resolution.paths and not self._known_external(alias.name.split(".")[0]):
                    return _ImportResolution(ambiguous=True)
                paths.update(resolution.paths)
            return _ImportResolution(tuple(sorted(paths)))

        if any(alias.name == "*" for alias in statement.names):
            return _ImportResolution(ambiguous=True)
        module_parts = tuple(statement.module.split(".")) if statement.module else ()
        if statement.level:
            base_dir = importer.parent
            for _ in range(statement.level - 1):
                base_dir = base_dir.parent
            base_path = base_dir.joinpath(*module_parts).resolve()
            base_resolution = self._resolve_path(base_path, self.project_root)
            if base_resolution.ambiguous:
                return base_resolution
            paths = set(base_resolution.paths)
            for alias in statement.names:
                child_resolution = self._resolve_path(
                    (base_path / alias.name).resolve(), self.project_root
                )
                if child_resolution.ambiguous:
                    return child_resolution
                paths.update(child_resolution.paths)
            return _ImportResolution(tuple(sorted(paths)), ambiguous=not paths)

        base_resolution = self._resolve_absolute(module_parts)
        if base_resolution.ambiguous:
            return base_resolution
        runtime_names = [statement.module] if statement.module else []
        if self._loaded_module_mismatch(runtime_names, base_resolution):
            return _ImportResolution(ambiguous=True)
        paths = set(base_resolution.paths)
        for alias in statement.names:
            child_resolution = self._resolve_absolute((*module_parts, alias.name))
            if child_resolution.ambiguous:
                return child_resolution
            if module_parts and self._loaded_module_mismatch(
                (".".join((*module_parts, alias.name)),), child_resolution
            ):
                return _ImportResolution(ambiguous=True)
            paths.update(child_resolution.paths)
        if not paths and module_parts and not self._known_external(module_parts[0]):
            return _ImportResolution(ambiguous=True)
        return _ImportResolution(tuple(sorted(paths)))

    def _loaded_module_mismatch(
        self,
        module_names: Iterable[str],
        resolution: _ImportResolution,
    ) -> bool:
        """Reject a loaded configured module whose file differs from static resolution."""
        static_paths = set(resolution.paths)
        for module_name in module_names:
            module = sys.modules.get(module_name)
            if module is None:
                continue
            module_file = getattr(module, "__file__", None)
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

    def _is_source_universe_path(self, path: Path) -> bool:
        """Return whether a runtime module belongs to project or configured sources."""
        return _path_is_within(path, self.project_root) or any(
            _path_is_within(path, source_dir) for source_dir in self.source_dirs
        )

    def _known_external(self, top_level: str) -> bool:
        """Accept only imports proven outside the configured project source universe."""
        if top_level in sys.builtin_module_names or top_level in sys.stdlib_module_names:
            return True
        module = sys.modules.get(top_level)
        if module is None:
            return False
        module_file = getattr(module, "__file__", None)
        if not isinstance(module_file, str) or not module_file:
            return False
        return not self._is_source_universe_path(Path(module_file))

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
        return _ImportResolution(tuple(sorted(paths)))

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


def _function_module_path(function: Callable[..., Any]) -> Path | None:
    """Return a function's owning Python module path, or None when that fact is unavailable."""
    try:
        module = inspect.getmodule(function)
        source = inspect.getsourcefile(function)
    except (OSError, TypeError):
        return None
    if module is None or source is None:
        return None
    module_file = getattr(module, "__file__", None)
    if not isinstance(module_file, str) or not module_file:
        return None
    source_path = Path(source).resolve()
    module_path = Path(module_file).resolve()
    if source_path.suffix != ".py":
        return None
    if module_path.suffix == ".py" and module_path != source_path:
        return None
    return source_path


def _function_module_name(function: Callable[..., Any]) -> str | None:
    """Return the import name paired with a function's owning module path."""
    try:
        module = inspect.getmodule(function)
    except (OSError, TypeError):
        return None
    name = getattr(module, "__name__", None) if module is not None else None
    return name if isinstance(name, str) and name else None


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
