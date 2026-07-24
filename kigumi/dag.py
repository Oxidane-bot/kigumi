"""调度层：注册、校验、缓存键计算与 DAG 执行。

存储路径、artifact 落盘、归档、物化、审批和 GC 由 ``kigumi.store`` 负责；本模块仅依赖它。
"""

from __future__ import annotations

import argparse
import ast
import copy
import inspect
import json
import sys
import textwrap
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import nullcontext
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

import pydantic

from . import prompt, repair, store, views
from ._declarations import (
    CachePolicy,
    ConsumeFunction,
    external_fingerprint_digest,
    validate_cache_policy,
    validate_consumes,
    validate_segment,
)
from ._execution import ExecutionEnvelope
from ._runstate import AttemptStore, RunManifestError
from .agents import (
    AGENT_EXECUTOR_SCHEMA,
    AgentAdapter,
    AgentBuildContext,
    AgentResultView,
    AgentSpec,
    AgentTask,
    agent_external_identity,
    execute_agent_task,
    validate_agent_artifact,
    validate_agent_provenance,
)
from .artifacts import canonical_json, sha
from .blobs import BlobStore
from .calling import DryRunError, LLMCaller, durable_side_effect_boundary, observe
from .config import KigumiConfig
from .enforce import check_paths, check_raw_io_node_paths, check_raw_io_source, check_source
from .errors import OutputOwnershipError
from .evidence import EvidencePolicy
from .failures import (
    AgentExecutionFailure,
    AgentRuntimeFailureCode,
    ProviderFailure,
    canonical_failure,
    failure_provider_kind,
)
from .prompt import load_template, render_template
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
CACHE_SCHEMA = 4
_DEFAULT_EVIDENCE_POLICY = EvidencePolicy()


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
        if self.status == "legacy":
            lines.append("该运行缺少成分记录；重跑一次即可获得解释。")
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
    prompts: tuple[str, ...]
    files: tuple[Path, ...]
    params: dict[str, Any]
    consumes: dict[str, ConsumeFunction]
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
    ) -> None:
        self._dag = dag
        self._node = node
        self._run_id = run_id
        self._checkpoint_suffix = checkpoint_suffix
        self._item_files = item_files
        self._checkpoint_used = False

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

    def render(self, template_name: str, **slots: str) -> str:
        """Strictly render a template declared on this node."""
        if template_name not in self._node.prompts:
            raise ValueError(
                f"Template {template_name!r} is not declared for node {self._node.name!r}"
            )
        return render_template(load_template(self._dag._prompt_path(template_name)), slots)

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
        prompts: Iterable[str] = (),
        files: Iterable[str | Path] = (),
        params: dict[str, Any] | None = None,
        *,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint: Any | None = None,
        retry: RetryPolicy | None = None,
    ) -> Callable[[NodeFunction], NodeFunction]:
        """Register a deterministic node without sharing registry state with other DAGs."""
        _validate_name(name, "Node")
        if name in self._nodes:
            raise ValueError(f"Node {name!r} is already registered")
        node_deps = tuple(deps)
        node_prompts = tuple(prompts)
        node_files = tuple(Path(path) for path in files)
        node_params = copy.deepcopy(params) if params is not None else {}
        node_cache = validate_cache_policy(cache)
        fingerprint_digest = external_fingerprint_digest(external_fingerprint)
        retry_policy = _validate_retry_policy(retry)

        def register(function: NodeFunction) -> NodeFunction:
            metadata = _validate_registration(function)
            self._register_node(
                name,
                function,
                deps=node_deps,
                prompts=node_prompts,
                files=node_files,
                params=node_params,
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
        prompts: Iterable[str] = (),
        files: Iterable[str | Path] = (),
        params: dict[str, Any] | None = None,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        evidence_policy: EvidencePolicy = _DEFAULT_EVIDENCE_POLICY,
        retry: RetryPolicy | None = None,
    ) -> Callable[[Callable[[dict[str, dict[str, Any]], AgentBuildContext], AgentTask]], Any]:
        """Register an external-agent executor on the ordinary node scheduler."""
        _validate_name(name, "Agent node")
        if name in self._nodes:
            raise ValueError(f"Node {name!r} is already registered")
        node_cache = validate_cache_policy(cache)
        if not isinstance(evidence_policy, EvidencePolicy):
            raise TypeError("evidence_policy must be EvidencePolicy")
        retry_policy = _validate_retry_policy(retry)
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
                prompts=tuple(prompts),
                files=tuple(Path(path) for path in files),
                params=copy.deepcopy(params) if params is not None else {},
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

    def map(
        self,
        name: str,
        *,
        items_from: tuple[str, str],
        key_fn: Callable[[Any], str] | None = None,
        deps: Iterable[str] = (),
        prompts: Iterable[str] = (),
        files: Iterable[str | Path] = (),
        files_fn: Callable[[Any], Iterable[str | Path]] | None = None,
        params: dict[str, Any] | None = None,
        aggregate_fn: AggregateFunction | None = None,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint: Any | None = None,
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
        map_prompts = tuple(prompts)
        map_files = tuple(Path(path) for path in files)
        map_params = copy.deepcopy(params) if params is not None else {}
        map_cache = validate_cache_policy(cache)
        fingerprint_digest = external_fingerprint_digest(external_fingerprint)
        retry_policy = _validate_retry_policy(retry)

        def register(function: MapFunction) -> MapFunction:
            metadata = _validate_registration(function)
            self._register_node(
                name,
                function,
                deps=map_deps,
                prompts=map_prompts,
                files=map_files,
                params=map_params,
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
        prompts: Iterable[str] = (),
        files: Iterable[str | Path] = (),
        files_fn: Callable[[Any], Iterable[str | Path]] | None = None,
        params: dict[str, Any] | None = None,
        aggregate_fn: AggregateFunction | None = None,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint: Any | None = None,
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
        scan_prompts = tuple(prompts)
        scan_files = tuple(Path(path) for path in files)
        scan_params = copy.deepcopy(params) if params is not None else {}
        scan_cache = validate_cache_policy(cache)
        fingerprint_digest = external_fingerprint_digest(external_fingerprint)
        retry_policy = _validate_retry_policy(retry)

        def register(function: ScanFunction) -> ScanFunction:
            metadata = _validate_registration(function)
            self._register_node(
                name,
                function,
                deps=scan_deps,
                prompts=scan_prompts,
                files=scan_files,
                params=scan_params,
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
        prompts: Iterable[str] = (),
        files: Iterable[str | Path] = (),
        files_fn: Callable[[Any], Iterable[str | Path]] | None = None,
        params: dict[str, Any] | None = None,
        params_fn: Callable[[Any], dict[str, Any]] | None = None,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint: Any | None = None,
        retry: RetryPolicy | None = None,
    ) -> Callable[[NodeFunction], NodeFunction]:
        """Register one node per item, fixing names, dependencies, and params immediately."""
        # 生成器只能消费一次;不先固定,第二个 item 起声明就静默变空。
        fixed_deps = deps if callable(deps) else tuple(deps)
        fixed_prompts = tuple(prompts)
        fixed_files = tuple(Path(path) for path in files)
        fixed_params = copy.deepcopy(params) if params is not None else {}
        fixed_cache = validate_cache_policy(cache)
        fingerprint_digest = external_fingerprint_digest(external_fingerprint)
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
                    prompts=fixed_prompts,
                    files=item_files,
                    params=item_params,
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
        prompts: tuple[str, ...],
        files: tuple[Path, ...],
        params: dict[str, Any],
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
        self._nodes[name] = _Node(
            name=name,
            function=function,
            deps=deps,
            prompts=prompts,
            files=files,
            params=params,
            consumes=projections,
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
                prompts=declaration.prompts,
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

    def run(
        self,
        targets: Iterable[str] | None = None,
        run_id: str | None = None,
        force: Iterable[str] = (),
        workers: int = 1,
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
        libs_hash = self._libs_hash()
        attempt_store = AttemptStore(
            run_dir,
            self._run_manifest_identity(
                current_run_id,
                selected,
                requested_force,
                order,
                libs_hash,
            ),
        )
        attempt_store.initialize()
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

        def execute(node_name: str) -> tuple[str, str | None]:
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
            if node.items_from is None:
                key_components = self._key_components(
                    node,
                    upstream_shas,
                    libs_hash,
                    upstream_artifacts=inputs,
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
                            libs_hash,
                            workers,
                            forced_all=node.name in forced_nodes,
                            forced_items=forced_items.get(node.name, set()),
                            envelope=envelope,
                            attempt_store=attempt_store,
                        )
                        cache_key = item_cache_keys
                        with state_lock:
                            map_items[node.name] = item_statuses
                    elif artifact is None:
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
                            )
                            origin = prior_metadata.get("origin_provenance")
                            if isinstance(origin, dict) and isinstance(origin.get("agent"), dict):
                                agent_provenance = copy.deepcopy(origin["agent"])
                            resumed_completed = True
                        elif action == "candidate":
                            candidate = prepared["candidate"]
                            if (
                                candidate.get("candidate_schema") != 1
                                or candidate.get("cache_key") != cache_key
                                or candidate.get("key_components") != key_components
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
                            context = NodeContext(self, node, current_run_id)
                            try:
                                function_inputs = copy.deepcopy(self._function_inputs(node, inputs))
                                boundary = (
                                    durable_side_effect_boundary(
                                        lambda: attempt_store.mark_side_effect(node.name)
                                    )
                                    if node.retry is not None and node.executor != "agent"
                                    else nullcontext()
                                )
                                with boundary:
                                    if node.executor == "agent":
                                        try:
                                            lease_context = self.agent_slots.acquire(
                                                timeout_seconds=(
                                                    self.config.agent_slot_timeout_seconds
                                                )
                                            )
                                            with lease_context as lease:
                                                build_context = AgentBuildContext(
                                                    node.params,
                                                    context.read_text,
                                                    context.read_bytes,
                                                    context.render,
                                                )
                                                task = node.function(  # type: ignore[call-arg]
                                                    function_inputs, build_context
                                                )
                                                if not isinstance(task, AgentTask):
                                                    raise TypeError(
                                                        f"Agent node {node.name!r} builder "
                                                        "must return AgentTask"
                                                    )
                                                assert node.agent_adapter is not None
                                                assert node.agent_spec is not None
                                                assert node.agent_identity is not None
                                                if node.retry is not None:
                                                    attempt_store.mark_side_effect(node.name)
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
                                                )
                                        except SlotTimeoutError:
                                            raise AgentExecutionFailure(
                                                runtime_code=(AgentRuntimeFailureCode.CAPACITY)
                                            ) from None
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
                                    "candidate_schema": 1,
                                    "artifact": artifact,
                                    "cache_key": cache_key,
                                    "key_components": key_components,
                                    "calls": copy.deepcopy(calls),
                                    "agent_provenance": copy.deepcopy(agent_provenance),
                                    "seconds": time.monotonic() - started,
                                    "checkpoint_used": checkpoint_used,
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
                            )
                    if node.executor == "agent":
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
                            )
                        if node.items_from is None and artifact is not None and not cache_hit:
                            attempt_store.mark_completed(
                                node.name,
                                artifact_sha256=artifact_sha256,
                            )
                    return node_name, "success"
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
        with ThreadPoolExecutor(max_workers=workers) as executor:
            while ready or in_flight:
                while ready and len(in_flight) < workers and not failures:
                    node_name = ready.pop(0)
                    states[node_name] = "running"
                    in_flight[executor.submit(execute, node_name)] = node_name
                if not in_flight:
                    break
                done, _ = wait(in_flight, return_when="FIRST_COMPLETED")
                for future in done:
                    node_name = in_flight.pop(future)
                    try:
                        _, outcome = future.result()
                    except BaseException as error:
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

    def resume(self, run_id: str, *, workers: int = 1) -> RunResult:
        """Resume one schema-1 run under its originally bound declaration."""
        run_dir = store.run_directory(self.config.artifacts_path, run_id)
        try:
            manifest = json.loads((run_dir / "_run.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError) as error:
            raise RunManifestError(
                f"Run {run_id!r} has no valid 0.6 run manifest and cannot be resumed"
            ) from error
        if not isinstance(manifest, dict) or manifest.get("run_manifest_schema") != 1:
            raise RunManifestError(
                f"Run {run_id!r} has no valid 0.6 run manifest and cannot be resumed"
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
        )

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
        if not isinstance(manifest, dict) or manifest.get("run_manifest_schema") != 1:
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
        libs_hash: str,
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
                "prompts": {
                    prompt_name: sha(load_template(self._prompt_path(prompt_name)))
                    for prompt_name in node.prompts
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
        return {
            "run_id": run_id,
            "graph_identity": sha(
                {
                    "declarations": declarations,
                    "targets": list(targets),
                    "force": list(force),
                    "libs": libs_hash,
                }
            ),
            "targets": list(targets),
            "force": list(force),
            "source_digest": source_digest,
            "libs_digest": libs_hash,
            "retry_policy_digests": retry_digests,
            "evidence_policy_digests": evidence_digests,
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
            metadata.get("artifact_sha256") != artifact_digest
            or not isinstance(origin, dict)
            or origin.get("artifact_sha256") != artifact_digest
            or metadata.get("key_components") != key_components
            or metadata.get("cache_key") != cache_key
        ):
            raise RunManifestError(
                f"Completed target {label!r} failed artifact or declaration validation"
            )
        if validate_agent:
            validate_agent_artifact(artifact, self.blob_store)
        return artifact, metadata

    def plan(
        self,
        run_id: str | None = None,
        targets: Iterable[str] | None = None,
        force: Iterable[str] = (),
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
        libs_hash = self._libs_hash()
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
                        libs_hash,
                        upstream_artifacts=inputs,
                    )
                )
                artifact = (
                    None
                    if node.cache != "auto" or node_name in forced_nodes
                    else store.read_node_cache(self.config.artifacts_path, cache_key)
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
                            libs_hash,
                            upstream_artifacts=inputs,
                            item=item,
                            item_files=item_files,
                            carry=carry,
                        )
                    )
                    artifact = (
                        None
                        if node.cache != "auto"
                        or node_name in forced_nodes
                        or item_id in forced_items.get(node_name, set())
                        else store.read_node_cache(self.config.artifacts_path, cache_key)
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
                    libs_hash,
                    upstream_artifacts=inputs,
                    item=item,
                    item_files=item_files,
                )
                cache_key = sha(key_components)
                artifact = (
                    None
                    if node.cache != "auto"
                    or node_name in forced_nodes
                    or item_id in forced_items.get(node_name, set())
                    else store.read_node_cache(self.config.artifacts_path, cache_key)
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
        forecast = self.plan()
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
            return ExplainResult("legacy", [], {})
        if status == "unknown":
            return ExplainResult(
                "unknown",
                [],
                {},
                forecast.pending_on.get(name, forecast.pending_on.get(node_name, ())),
            )

        current = self._current_key_components(node_name, item_id)
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

    def _current_key_components(self, node_name: str, item_id: str | None) -> dict[str, str]:
        """重建一个 explain 目标的当前键成分，不执行节点函数。"""
        memo: dict[str, dict[str, Any]] = {}
        components: dict[str, dict[str, str]] = {}
        libs_hash = self._libs_hash()

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
                    libs_hash,
                    upstream_artifacts=inputs,
                )
                artifact = store.read_node_cache(self.config.artifacts_path, sha(component))
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
                    libs_hash,
                    upstream_artifacts=inputs,
                    item=item,
                    item_files=item_files,
                    carry=carry,
                )
                artifact = store.read_node_cache(self.config.artifacts_path, sha(component))
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
                libs_hash,
                upstream_artifacts=target_inputs,
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
                libs_hash,
                upstream_artifacts=target_inputs,
                item=item,
                item_files=item_files,
                carry=carry,
            )
            if current_item_id == item_id:
                return component
            if target_node.scan:
                artifact = store.read_node_cache(self.config.artifacts_path, sha(component))
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
        description: dict[str, Any] = {}
        for name, node in self._nodes.items():
            kind = "scan" if node.scan else "map" if node.items_from is not None else "node"
            description[name] = {
                "kind": kind,
                "executor": node.executor,
                "doc": _function_doc(node.function),
                "deps": list(node.deps),
                "items_from": _locator_description(node.items_from),
                "carry_from": _locator_description(node.carry_from),
                "prompts": list(node.prompts),
                "files": [str(path) for path in node.files],
                "params": {key: _short_repr(node.params[key]) for key in sorted(node.params)},
                "has_files_fn": node.files_fn is not None,
                "has_carry_fn": node.carry_fn is not None,
                "has_aggregate_fn": node.aggregate_fn is not None,
                "cache": node.cache,
                "has_external_fingerprint": node.external_fingerprint_digest is not None,
                "retry_policy": (node.retry.canonical() if node.retry is not None else None),
                "retry_policy_digest": (node.retry.digest if node.retry is not None else None),
                "evidence_policy": (
                    node.evidence_policy or self._caller_evidence_policy()
                ).canonical(),
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
                description[name]["agent"] = copy.deepcopy(node.agent_identity)
            if node.consumes:
                description[name]["consumes"] = list(node.consumes)
        description["subgraphs"] = copy.deepcopy(self._subgraphs)
        description["models"] = _describe_models(self._nodes.values())
        return description

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
        """Run the read-only DAG inspection CLI and exit with its status code."""
        args = _build_cli_parser().parse_args(argv)
        handlers = {
            "check": self._cli_check,
            "plan": self._cli_plan,
            "graph": self._cli_graph,
            "explain": self._cli_explain,
            "describe": self._cli_describe,
            "resume": self._cli_resume,
            "retry-resolve": self._cli_retry_resolve,
        }
        sys.exit(handlers[args.command](args))

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
        if args.html is None:
            print(self.render_pipeline_text(run_id=args.run_id))
        else:
            output = Path(args.html)
            output.write_text(self.render_pipeline(run_id=args.run_id), encoding="utf-8")
            print(output)
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
        """Resume a bound 0.6 run and report its durable terminal/pending state."""
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
        forced_all: bool,
        forced_items: set[str],
        envelope: ExecutionEnvelope,
        attempt_store: AttemptStore,
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
            started = time.monotonic()
            target = f"{node.name}@{item_id}"
            try:
                with observe() as calls:
                    item_files = (
                        tuple(Path(path) for path in node.files_fn(item)) if node.files_fn else ()
                    )
                    key_components = self._key_components(
                        node,
                        upstream_shas,
                        libs_hash,
                        upstream_artifacts=inputs,
                        item=item,
                        item_files=item_files,
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
                        prepared = attempt_store.prepare(
                            target,
                            policy=node.retry,
                            declaration_digest=declaration_digest,
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
                            )
                            resumed_completed = True
                        elif action == "candidate":
                            candidate = prepared["candidate"]
                            if (
                                candidate.get("candidate_schema") != 1
                                or candidate.get("cache_key") != cache_key
                                or candidate.get("key_components") != key_components
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
                            )
                            boundary = (
                                durable_side_effect_boundary(
                                    lambda attempt_target=target: attempt_store.mark_side_effect(
                                        attempt_target
                                    )
                                )
                                if node.retry is not None
                                else nullcontext()
                            )
                            try:
                                with boundary:
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
                                    "candidate_schema": 1,
                                    "artifact": artifact,
                                    "cache_key": cache_key,
                                    "key_components": key_components,
                                    "calls": copy.deepcopy(calls),
                                    "agent_provenance": None,
                                    "seconds": time.monotonic() - started,
                                    "checkpoint_used": checkpoint_used,
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
                    }
            except Exception as error:
                # KeyboardInterrupt/SystemExit 不许伪装成单项失败被聚合吞掉:
                # 让它按原类型冲出去,调度器 drain 后原样重抛。
                return {"id": item_id, "status": "failed", "error": error}

        if len(entries) <= 1 or workers == 1:
            outcomes = [execute_item(item_id, item) for item_id, item in entries]
        else:
            with ThreadPoolExecutor(max_workers=min(workers, len(entries))) as executor:
                futures = [
                    executor.submit(execute_item, item_id, item) for item_id, item in entries
                ]
                outcomes = [future.result() for future in futures]

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
            else:
                failures.append(outcome)
        if failures:
            first = failures[0]["error"]
            if isinstance(first, DryRunError):
                raise first
            if isinstance(first, (RetryExhausted, ProviderFailure)):
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
            and all(outcome["cache"] == "hit" for outcome in outcomes),
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
        forced_all: bool,
        forced_items: set[str],
        envelope: ExecutionEnvelope,
        attempt_store: AttemptStore,
    ) -> tuple[dict[str, Any], bool, list[str], dict[str, str]]:
        """Run one carry chain serially while retaining map's item cache and sidecar contract."""
        del workers  # scan 的每项都依赖前一项 carry，串行是语义而非调度偏好。
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
            try:
                with observe() as calls:
                    item_files = (
                        tuple(Path(path) for path in node.files_fn(item)) if node.files_fn else ()
                    )
                    key_components = self._key_components(
                        node,
                        upstream_shas,
                        libs_hash,
                        upstream_artifacts=inputs,
                        item=item,
                        item_files=item_files,
                        carry=carry,
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
                        prepared = attempt_store.prepare(
                            target,
                            policy=node.retry,
                            declaration_digest=declaration_digest,
                        )
                        action = prepared["action"]
                        if action == "pending":
                            raise _MapRetryPending([target])
                        if action == "failed":
                            raise RunManifestError(
                                f"Scan target {target!r} is already terminally failed"
                            )
                        if action == "completed":
                            artifact, _metadata = self._resume_completed_artifact(
                                envelope.artifacts_path / "runs" / run_id,
                                target,
                                key_components,
                                cache_key,
                            )
                            resumed_completed = True
                        elif action == "candidate":
                            candidate = prepared["candidate"]
                            if (
                                candidate.get("candidate_schema") != 1
                                or candidate.get("cache_key") != cache_key
                                or candidate.get("key_components") != key_components
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
                            )
                            boundary = (
                                durable_side_effect_boundary(
                                    lambda attempt_target=target: attempt_store.mark_side_effect(
                                        attempt_target
                                    )
                                )
                                if node.retry is not None
                                else nullcontext()
                            )
                            try:
                                with boundary:
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
                                    "candidate_schema": 1,
                                    "artifact": artifact,
                                    "cache_key": cache_key,
                                    "key_components": key_components,
                                    "calls": copy.deepcopy(calls),
                                    "agent_provenance": None,
                                    "seconds": time.monotonic() - started,
                                    "checkpoint_used": checkpoint_used,
                                },
                            )
                        if not resumed_completed:
                            artifact = envelope.seal(
                                artifact,
                                cache_key,
                                label=(f"Scan node {node.name!r} item {item_id!r}"),
                                calls=calls,
                                cache_policy=("off" if checkpoint_used else node.cache),
                                evidence_policy=self._caller_evidence_policy(),
                            )
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
                            evidence_policy=self._caller_evidence_policy(),
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
                if isinstance(error, (RetryExhausted, ProviderFailure)):
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
            (f"prompts:{name}", sha(load_template(self._prompt_path(name))))
            for name in node.prompts
        )
        components.update(
            (f"files:{path}", _bytes_hash(self.config.resolve(path).read_bytes()))
            for path in node.files
        )
        if item is not _NO_ITEM:
            components.update(
                (f"item_files:{path}", _bytes_hash(self.config.resolve(path).read_bytes()))
                for path in item_files
            )
        if carry is not _NO_CARRY:
            # carry_fn 的源码不入键；只有本项实际收到的内容才是输入事实。
            components["carry"] = sha(carry)
        return dict(sorted(components.items()))

    def _libs_hash(self) -> str:
        contents: list[str] = []
        for source_dir in self.config.source_paths:
            if source_dir.is_dir():
                contents.extend(
                    _module_code_text(path.read_text(encoding="utf-8"))
                    for path in sorted(source_dir.rglob("*.py"))
                )
        return sha(contents)

    def _prompt_path(self, template_name: str) -> Path:
        return self.config.prompts_path / f"{template_name}.md"

    def _approval_path(self, run_id: str, name: str) -> Path:
        return store.checkpoint_path(store.runs_root(self.config.artifacts_path), run_id, name)


def _build_cli_parser() -> argparse.ArgumentParser:
    """Build the stdlib parser shared by every ``Dag.cli`` invocation."""
    parser = argparse.ArgumentParser(prog="dag")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("check")

    plan = commands.add_parser("plan")
    plan.add_argument("--targets")

    graph = commands.add_parser("graph")
    graph.add_argument("--html")
    graph.add_argument("--run-id")

    explain = commands.add_parser("explain")
    explain.add_argument("node_name")
    explain.add_argument("--run-id")

    describe = commands.add_parser("describe")
    describe.add_argument("--format", choices=("md", "json"), default="md")

    resume = commands.add_parser("resume")
    resume.add_argument("run_id")
    resume.add_argument("--workers", type=int, default=1)

    resolve = commands.add_parser("retry-resolve")
    resolve.add_argument("run_id")
    resolve.add_argument("target")
    resolve.add_argument("--attempt", type=int, required=True)
    resolve.add_argument("--action", choices=("retry", "fail"), required=True)
    resolve.add_argument("--reason", required=True)
    return parser


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


def _short_repr(value: Any, limit: int = 120) -> str:
    """限制参数展示长度，避免声明摘要被大值淹没。"""
    try:
        rendered = repr(value)
    except Exception:
        rendered = f"<{type(value).__name__}>"
    return rendered if len(rendered) <= limit else f"{rendered[: limit - 3]}..."


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
