"""Provider-neutral execution boundary for externally implemented agents.

The module deliberately owns staging, collection and publication semantics while
leaving scheduling, caching and output ownership to :mod:`kigumi.dag`.
"""

from __future__ import annotations

import copy
import json
import mimetypes
import shutil
import tempfile
import time
import tomllib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Protocol

from .artifacts import atomic_write_json, canonical_json, sha
from .blobs import BlobStore
from .evidence import EvidenceMode, EvidencePolicy, capture_evidence, scrub_evidence
from .failures import (
    AgentExecutionFailure,
    AgentRuntimeFailureCode,
    ProviderFailure,
    canonical_failure,
)

AGENT_EXECUTOR_SCHEMA = 3
AGENT_SCHEMA = 2
_DEFAULT_EVIDENCE_POLICY = EvidencePolicy()


class AgentError(RuntimeError):
    """Base error for failures at the external-agent boundary."""


class AgentCapabilityError(AgentError):
    """Raised when a task requests a capability the adapter does not provide."""


class AgentResultError(AgentError):
    """Raised when an adapter result or captured artifact violates the contract."""


@dataclass(frozen=True)
class AgentCapabilities:
    filesystem: bool = True
    terminal: bool = False


@dataclass(frozen=True)
class AgentLimits:
    timeout_seconds: float = 300.0
    max_turns: int = 24
    max_tool_calls: int = 100
    max_files: int = 100
    max_bytes: int = 10 * 1024 * 1024
    max_single_file_bytes: int = 2 * 1024 * 1024
    inline_text_max_bytes: int = 64 * 1024
    trajectory_max_events: int = 200
    trajectory_max_bytes: int = 256 * 1024
    rpc_max_bytes: int = 2 * 1024 * 1024
    stderr_max_bytes: int = 256 * 1024

    def __post_init__(self) -> None:
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, int | float)
            or self.timeout_seconds <= 0
        ):
            raise ValueError("Agent timeout_seconds must be positive")
        for name in (
            "max_turns",
            "max_tool_calls",
            "max_files",
            "max_bytes",
            "max_single_file_bytes",
            "inline_text_max_bytes",
            "trajectory_max_events",
            "trajectory_max_bytes",
            "rpc_max_bytes",
            "stderr_max_bytes",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"Agent {name} must be non-negative")

    def identity(self) -> dict[str, Any]:
        return {
            "timeout_seconds": self.timeout_seconds,
            "max_turns": self.max_turns,
            "max_tool_calls": self.max_tool_calls,
            "max_files": self.max_files,
            "max_bytes": self.max_bytes,
            "max_single_file_bytes": self.max_single_file_bytes,
            "inline_text_max_bytes": self.inline_text_max_bytes,
            "trajectory_max_events": self.trajectory_max_events,
            "trajectory_max_bytes": self.trajectory_max_bytes,
            "rpc_max_bytes": self.rpc_max_bytes,
            "stderr_max_bytes": self.stderr_max_bytes,
        }


_RESERVED_TOOLS = {"bash", "shell", "terminal"}
_THINKING_LEVELS = {"off", "minimal", "low", "medium", "high", "xhigh", "max"}
_MANIFEST_KEYS = {
    "schema_version",
    "runtime",
    "provider",
    "model",
    "thinking",
    "system_prompt",
    "skills",
    "hooks",
    "tools",
    "limits",
}
_LIMIT_KEYS = set(AgentLimits.__dataclass_fields__)
_CREDENTIAL_NAMES = {
    "api_key",
    "apikey",
    "credential",
    "credentials",
    "password",
    "secret",
    "token",
}


@dataclass(frozen=True)
class AgentSpec:
    """Validated, immutable and content-addressed Pi agent capsule."""

    root: Path = field(repr=False, compare=False)
    runtime: Literal["pi"]
    provider: str
    model: str
    thinking: str
    system_prompt: str
    skills: tuple[str, ...]
    hooks: tuple[str, ...]
    tools: tuple[str, ...]
    limits: AgentLimits
    digest: str
    _files: tuple[tuple[str, bytes], ...] = field(repr=False, compare=False)
    _directories: tuple[str, ...] = field(repr=False, compare=False)

    @classmethod
    def load(cls, path: str | Path) -> AgentSpec:
        root = Path(path)
        if root.is_symlink() or not root.is_dir():
            raise ValueError("AgentSpec path must be a regular directory, not a symlink")
        root = root.resolve()
        _validate_capsule_tree(root)
        for directory_name in ("skills", "hooks"):
            directory = root / directory_name
            if directory.is_symlink() or not directory.is_dir():
                raise ValueError(
                    f"Agent capsule must contain a regular {directory_name}/ directory"
                )
        manifest_path = root / "agent.toml"
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise ValueError("Agent capsule must contain a regular agent.toml")
        try:
            manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
            raise ValueError(f"Invalid Agent manifest: {error}") from error
        if not isinstance(manifest, dict):
            raise ValueError("Agent manifest must be a TOML table")
        unknown = set(manifest) - _MANIFEST_KEYS
        if unknown:
            names = ", ".join(sorted(unknown))
            raise ValueError(f"Unknown or credential-bearing Agent manifest keys: {names}")
        _reject_credentials(manifest)
        if manifest.get("schema_version") != 1:
            raise ValueError("Agent manifest schema_version must be 1")
        if manifest.get("runtime") != "pi":
            raise ValueError('Agent manifest runtime must be "pi"')
        provider = _required_string(manifest, "provider")
        model = _required_string(manifest, "model")
        thinking = _required_string(manifest, "thinking")
        if thinking not in _THINKING_LEVELS:
            raise ValueError(f"Unsupported Agent thinking level: {thinking!r}")
        system_prompt = _required_string(manifest, "system_prompt")
        if system_prompt != "SYSTEM.md":
            raise ValueError('Agent system_prompt must be the capsule root file "SYSTEM.md"')
        skills = _string_list(manifest, "skills")
        hooks = _string_list(manifest, "hooks")
        tools = _string_list(manifest, "tools")
        if len(set(skills)) != len(skills) or len(set(hooks)) != len(hooks):
            raise ValueError("Agent capsule contains duplicate resources")
        if len(set(tools)) != len(tools):
            raise ValueError("Agent capsule contains duplicate tools")
        forbidden = sorted({tool.lower() for tool in tools} & _RESERVED_TOOLS)
        if forbidden:
            raise ValueError(f"Pi v1 reserves disabled tools: {', '.join(forbidden)}")
        for tool in tools:
            allowed_chars = "abcdefghijklmnopqrstuvwxyz0123456789_-"
            if not tool or any(char not in allowed_chars for char in tool):
                raise ValueError(f"Invalid Agent tool name: {tool!r}")
        limits_data = manifest.get("limits")
        if not isinstance(limits_data, dict):
            raise ValueError("Agent manifest must contain a [limits] table")
        unknown_limits = set(limits_data) - _LIMIT_KEYS
        missing_limits = _LIMIT_KEYS - set(limits_data)
        if unknown_limits or missing_limits:
            raise ValueError(
                "Agent limits must explicitly declare exactly: " + ", ".join(sorted(_LIMIT_KEYS))
            )
        try:
            limits = AgentLimits(**limits_data)
        except (TypeError, ValueError) as error:
            raise ValueError(f"Invalid Agent limits: {error}") from error

        files: dict[str, bytes] = {"agent.toml": manifest_path.read_bytes()}
        directories: set[str] = set()
        owners: dict[str, str] = {"agent.toml": "manifest"}

        def add_reference(raw: str, *, kind: str, directory: bool) -> None:
            relative = _capsule_path(raw, prefix=f"{kind}/" if kind != "system" else None)
            target = root / relative
            if (
                target.is_symlink()
                or (directory and not target.is_dir())
                or (not directory and not target.is_file())
            ):
                expected = "directory" if directory else "file"
                raise ValueError(f"Agent {kind} resource must be a regular {expected}: {raw!r}")
            entries = [target, *sorted(target.rglob("*"))] if directory else [target]
            for entry in entries:
                rel = entry.relative_to(root).as_posix()
                previous = owners.get(rel)
                if previous is not None:
                    raise ValueError(
                        f"Agent resource {rel!r} is declared by both {previous!r} and {raw!r}"
                    )
                owners[rel] = raw
                if entry.is_symlink():
                    raise ValueError(f"Agent capsule may not contain symlinks: {rel}")
                if entry.is_dir():
                    directories.add(rel)
                elif entry.is_file():
                    files[rel] = entry.read_bytes()
                else:
                    raise ValueError(f"Agent capsule resource is not a regular file: {rel}")

        add_reference(system_prompt, kind="system", directory=False)
        for resource in skills:
            add_reference(resource, kind="skills", directory=True)
        for resource in hooks:
            add_reference(resource, kind="hooks", directory=False)
        resource_manifest = [
            {"path": rel, "kind": "file", "sha256": sha256(data).hexdigest()}
            for rel, data in sorted(files.items())
        ] + [{"path": rel, "kind": "directory"} for rel in sorted(directories)]
        resource_manifest.sort(key=lambda item: (item["path"], item["kind"]))
        digest = sha({"schema": 1, "resources": resource_manifest})
        return cls(
            root=root,
            runtime="pi",
            provider=provider,
            model=model,
            thinking=thinking,
            system_prompt=system_prompt,
            skills=skills,
            hooks=hooks,
            tools=tools,
            limits=limits,
            digest=digest,
            _files=tuple(sorted(files.items())),
            _directories=tuple(sorted(directories)),
        )

    def identity(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "digest": self.digest,
            "runtime": self.runtime,
            "provider": self.provider,
            "model": self.model,
            "thinking": self.thinking,
            "system_prompt": self.system_prompt,
            "skills": list(self.skills),
            "hooks": list(self.hooks),
            "tools": list(self.tools),
            "limits": self.limits.identity(),
        }

    def stage(self, destination: str | Path) -> None:
        target = Path(destination)
        if target.exists():
            raise ValueError(f"Agent capsule staging destination already exists: {target}")
        target.mkdir(parents=True)
        for relative in self._directories:
            (target / relative).mkdir(parents=True, exist_ok=True)
        for relative, data in self._files:
            path = target / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)


def _validate_capsule_tree(root: Path) -> None:
    for path in root.rglob("*"):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            raise ValueError(f"Agent capsule may not contain symlinks: {relative}")
        lowered = path.name.lower()
        sensitive_stem = Path(lowered).stem in {
            "credential",
            "credentials",
            "secret",
            "secrets",
        }
        if lowered == ".env" or lowered.startswith(".env.") or sensitive_stem:
            raise ValueError(f"Agent capsule may not contain credential files: {relative}")


def _reject_credentials(value: Any, path: tuple[str, ...] = ()) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).lower().replace("-", "_")
            sensitive_suffixes = (
                "_token",
                "_secret",
                "_password",
                "_credential",
                "_api_key",
            )
            if normalized in _CREDENTIAL_NAMES or normalized.endswith(sensitive_suffixes):
                location = ".".join((*path, str(key)))
                raise ValueError(f"Agent manifest may not contain credentials: {location}")
            _reject_credentials(child, (*path, str(key)))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_credentials(child, (*path, str(index)))


def _required_string(manifest: Mapping[str, Any], key: str) -> str:
    value = manifest.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Agent manifest {key} must be a non-empty string")
    return value


def _string_list(manifest: Mapping[str, Any], key: str) -> tuple[str, ...]:
    value = manifest.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"Agent manifest {key} must be a list of non-empty strings")
    return tuple(value)


def _capsule_path(value: str, *, prefix: str | None) -> str:
    checked = _workspace_pattern(value)
    if prefix is not None and checked != prefix.rstrip("/") and not checked.startswith(prefix):
        raise ValueError(f"Agent resource must be under {prefix!r}: {value!r}")
    return checked


def _workspace_pattern(value: str, *, glob: bool = False) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or "\\" in value:
        raise ValueError("Agent workspace paths must be non-empty POSIX paths")
    raw_parts = value.split("/")
    if any(part in {"", ".", ".."} for part in raw_parts):
        raise ValueError(f"Unsafe Agent workspace path: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"Unsafe Agent workspace path: {value!r}")
    if not glob and any(char in value for char in "*?["):
        raise ValueError(f"Agent path must be exact: {value!r}")
    return path.as_posix()


@dataclass(frozen=True)
class AgentFileSelector:
    pattern: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "pattern", _workspace_pattern(self.pattern, glob=True))


@dataclass(frozen=True)
class AgentPublish:
    source: str
    destination: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _workspace_pattern(self.source))
        object.__setattr__(self, "destination", _workspace_pattern(self.destination))


@dataclass(frozen=True)
class AgentTask:
    instruction: str
    collect: tuple[AgentFileSelector, ...] = ()
    publish: tuple[AgentPublish, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.instruction, str) or not self.instruction.strip():
            raise ValueError("AgentTask instruction must be a non-empty string")
        patterns = [selector.pattern for selector in self.collect]
        if len(set(patterns)) != len(patterns):
            raise ValueError("AgentTask contains duplicate selectors")
        destinations = [mapping.destination for mapping in self.publish]
        if len(set(destinations)) != len(destinations):
            raise ValueError("AgentTask contains duplicate publish destinations")
        canonical_json(self.canonical())

    def canonical(self) -> dict[str, Any]:
        return {
            "instruction": self.instruction,
            "collect": [selector.pattern for selector in self.collect],
            "publish": [
                {"source": mapping.source, "destination": mapping.destination}
                for mapping in self.publish
            ],
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class AgentRequest:
    task: AgentTask
    inputs: dict[str, dict[str, Any]]
    spec: AgentSpec


@dataclass(frozen=True)
class AgentCompletion:
    status: Literal["completed"]
    summary: str
    outputs: tuple[str, ...] = ()
    metrics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status != "completed":
            raise ValueError('AgentCompletion status must be "completed"')
        if not isinstance(self.summary, str) or not self.summary.strip():
            raise ValueError("AgentCompletion summary must be a non-empty string")
        checked = tuple(_workspace_pattern(path) for path in self.outputs)
        if len(set(checked)) != len(checked):
            raise ValueError("AgentCompletion outputs must be unique")
        object.__setattr__(self, "outputs", checked)
        canonical_json(dict(self.metrics))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AgentCompletion:
        if set(value) != {"status", "summary", "outputs", "metrics"}:
            raise AgentResultError(
                "Agent completion must contain exactly status, summary, outputs and metrics"
            )
        outputs = value.get("outputs")
        metrics = value.get("metrics")
        if not isinstance(outputs, list) or not all(isinstance(path, str) for path in outputs):
            raise AgentResultError("Agent completion outputs must be a list of paths")
        if not isinstance(metrics, Mapping):
            raise AgentResultError("Agent completion metrics must be an object")
        try:
            return cls(
                status=value.get("status"),  # type: ignore[arg-type]
                summary=value.get("summary"),  # type: ignore[arg-type]
                outputs=tuple(outputs),
                metrics=dict(metrics),
            )
        except (TypeError, ValueError) as error:
            raise AgentResultError(f"Invalid Agent completion: {error}") from error

    def canonical(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "summary": self.summary,
            "outputs": list(self.outputs),
            "metrics": dict(self.metrics),
        }


@dataclass(frozen=True)
class AgentRunContext:
    workspace: Path
    capsule_root: Path
    deadline: float
    emit_event: Callable[[Mapping[str, Any]], None]
    record_evidence: Callable[[str, bytes, str], None]


@dataclass(frozen=True)
class AgentRunResult:
    completion: AgentCompletion
    usage: Mapping[str, Any] | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class AgentAdapter(Protocol):
    def cache_identity(self) -> Mapping[str, Any]: ...

    def capabilities(self) -> AgentCapabilities: ...

    def run(self, request: AgentRequest, context: AgentRunContext) -> AgentRunResult: ...


class AgentBuildContext:
    """Narrow builder context containing only facts already represented in the node key."""

    def __init__(
        self,
        params: Mapping[str, Any],
        read_text: Callable[[str | Path, str], str],
        read_bytes: Callable[[str | Path], bytes],
        render: Callable[..., str],
        resolve_prompt: Callable[[str], str] | None = None,
    ) -> None:
        self._params = copy.deepcopy(dict(params))
        self._read_text = read_text
        self._read_bytes = read_bytes
        self._render = render
        self._resolve_prompt = resolve_prompt

    @property
    def params(self) -> dict[str, Any]:
        return copy.deepcopy(self._params)

    def read_text(self, path: str | Path, encoding: str = "utf-8") -> str:
        return self._read_text(path, encoding)

    def read_bytes(self, path: str | Path) -> bytes:
        return self._read_bytes(path)

    def render(self, template_name: str, **slots: str) -> str:
        return self._render(template_name, **slots)

    def resolve_prompt(self, spec_name: str) -> str:
        if self._resolve_prompt is None:
            raise ValueError("Agent builder has no declared PromptSpec resolver")
        return self._resolve_prompt(spec_name)


class AgentResultView:
    """Verified, read-only access to attachments captured by one Agent artifact."""

    def __init__(self, artifact: Mapping[str, Any], blob_store: BlobStore) -> None:
        validate_agent_artifact(artifact, blob_store)
        self._artifact = artifact
        self._blob_store = blob_store

    def list(self) -> list[str]:
        return [item["workspace_path"] for item in self._artifact["attachments"]]

    def select(self, pattern: str) -> list[str]:
        checked = _workspace_pattern(pattern, glob=True)
        return [path for path in self.list() if PurePosixPath(path).match(checked)]

    def read_bytes(self, workspace_path: str) -> bytes:
        checked = _workspace_pattern(workspace_path)
        for item in self._artifact["attachments"]:
            if item["workspace_path"] == checked:
                data = self._blob_store.read_verified(item["kigumi_attachment"])
                if len(data) != item["bytes"]:
                    raise AgentResultError(
                        f"Agent attachment {workspace_path!r} byte count does not match"
                    )
                return data
        raise KeyError(f"Unknown Agent attachment: {workspace_path}")

    def read_text(self, workspace_path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(workspace_path).decode(encoding)

    def publish(
        self,
        workspace_path: str,
        destination: str,
        *,
        inline_text_max_bytes: int = 64 * 1024,
    ) -> dict[str, Any]:
        """Build an ordinary materializable fragment from one exact attachment."""
        checked_destination = _workspace_pattern(destination)
        data = self.read_bytes(workspace_path)
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = None
        if text is not None and len(data) <= inline_text_max_bytes:
            return {"files": {checked_destination: text}}
        checked_source = _workspace_pattern(workspace_path)
        for item in self._artifact["attachments"]:
            if item["workspace_path"] == checked_source:
                return {
                    "blob": {
                        "kigumi_blob": item["kigumi_attachment"],
                        "path": checked_destination,
                        "bytes": item["bytes"],
                    }
                }
        raise KeyError(f"Unknown Agent attachment: {workspace_path}")


def agent_external_identity(adapter: AgentAdapter, spec: AgentSpec) -> dict[str, Any]:
    identity = dict(adapter.cache_identity())
    if not identity:
        raise ValueError("Agent adapter must provide a stable cache identity")
    _reject_credentials(identity)
    canonical_json(identity)
    return {
        "agent_executor_schema": AGENT_EXECUTOR_SCHEMA,
        "adapter": identity,
        "spec": spec.identity(),
        "isolation": {"level": "staged", "enforced_by": "kigumi-workspace"},
    }


@dataclass(frozen=True)
class AgentTaskExecution:
    """Canonical Agent artifact and its separately retained execution evidence."""

    artifact: dict[str, Any]
    provenance: dict[str, Any]


def execute_agent_task(
    *,
    node_name: str,
    run_id: str,
    task: AgentTask,
    inputs: dict[str, dict[str, Any]],
    declared_files: tuple[Path, ...],
    resolve: Callable[[Path], Path],
    artifacts_path: Path,
    blob_store: BlobStore,
    adapter: AgentAdapter,
    adapter_identity: Mapping[str, Any],
    spec: AgentSpec,
    evidence_policy: EvidencePolicy = _DEFAULT_EVIDENCE_POLICY,
    prompt_resolution: Mapping[str, Any] | None = None,
) -> AgentTaskExecution:
    expected_adapter = adapter_identity.get("adapter")
    unkeyed = isinstance(expected_adapter, Mapping) and expected_adapter.get("unkeyed") is True
    if not unkeyed and (
        not isinstance(expected_adapter, Mapping)
        or dict(adapter.cache_identity()) != dict(expected_adapter)
    ):
        raise AgentResultError(
            f"Agent node {node_name!r} adapter identity changed after registration"
        )
    capabilities = adapter.capabilities()
    if (task.collect or task.publish) and not capabilities.filesystem:
        raise AgentCapabilityError(f"Agent node {node_name!r} requires filesystem capability")
    limits = spec.limits
    workspace_root = artifacts_path / "_workspaces"
    workspace_root.mkdir(parents=True, exist_ok=True)
    workspace = Path(tempfile.mkdtemp(prefix=f"{node_name}-", dir=workspace_root))
    events: list[dict[str, Any]] = []
    event_bytes = 0
    truncated = False

    def emit_event(event: Mapping[str, Any]) -> None:
        nonlocal event_bytes, truncated
        normalized = json.loads(canonical_json(dict(event)))
        encoded = canonical_json(normalized).encode("utf-8")
        if len(events) >= limits.trajectory_max_events or (
            event_bytes + len(encoded) > limits.trajectory_max_bytes
        ):
            truncated = True
            return
        events.append(normalized)
        event_bytes += len(encoded)

    started = time.monotonic()
    baseline: dict[str, str] = {}
    evidence: list[dict[str, Any]] = []
    evidence_names: set[str] = set()
    evidence_bytes = 0

    def record_evidence(name: str, data: bytes, media_type: str) -> None:
        nonlocal evidence_bytes
        checked = _workspace_pattern(name)
        if not isinstance(data, bytes) or not isinstance(media_type, str) or not media_type:
            raise AgentResultError("Agent evidence must be bytes with a media type")
        if checked in evidence_names:
            raise AgentResultError(f"Agent evidence name was recorded twice: {checked!r}")
        if len(evidence) >= limits.max_files:
            raise AgentResultError("Agent evidence exceeds max_files")
        if len(data) > limits.max_single_file_bytes:
            raise AgentResultError(f"Agent evidence {checked!r} exceeds max_single_file_bytes")
        evidence_bytes += len(data)
        if evidence_bytes > limits.max_bytes:
            raise AgentResultError("Agent evidence exceeds max_bytes")
        evidence_names.add(checked)
        mode = evidence_policy.stderr if "stderr" in checked.lower() else evidence_policy.trajectory
        captured = capture_evidence(data, media_type=media_type, mode=mode)
        reference = {
            **captured.descriptor,
            "workspace_path": f".kigumi/evidence/{checked}",
        }
        if captured.data is not None:
            reference["kigumi_attachment"] = blob_store.put(captured.data)
            reference["stored_bytes"] = len(captured.data)
        evidence.append(reference)

    if not isinstance(evidence_policy, EvidencePolicy):
        raise TypeError("evidence_policy must be EvidencePolicy")

    try:
        for declared in declared_files:
            source = resolve(declared)
            if source.is_symlink() or not source.is_file():
                raise AgentResultError(f"Declared Agent input must be a regular file: {declared}")
            relative = _workspace_pattern(declared.as_posix())
            destination = workspace / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
        internal = workspace / ".kigumi"
        internal.mkdir()
        (internal / "inputs.json").write_text(canonical_json(inputs), encoding="utf-8")
        capsule_root = internal / "agent"
        spec.stage(capsule_root)
        baseline = _workspace_manifest(workspace)
        request = AgentRequest(task=task, inputs=copy.deepcopy(inputs), spec=spec)
        result = adapter.run(
            request,
            AgentRunContext(
                workspace=workspace,
                capsule_root=capsule_root,
                deadline=time.monotonic() + limits.timeout_seconds,
                emit_event=emit_event,
                record_evidence=record_evidence,
            ),
        )
        if not isinstance(result, AgentRunResult) or not isinstance(
            result.completion, AgentCompletion
        ):
            raise AgentResultError(f"Agent node {node_name!r} returned an invalid result")
        attachments = _collect(workspace, task.collect, limits, blob_store)
        files: dict[str, str] = {}
        published: list[dict[str, Any]] = []
        by_path = {item["workspace_path"]: item for item in attachments}
        completion_outputs = set(result.completion.outputs)
        unknown_outputs = completion_outputs - set(by_path)
        if unknown_outputs:
            raise AgentResultError(
                "Agent completion outputs were not collected: " + ", ".join(sorted(unknown_outputs))
            )
        missing_publications = {mapping.source for mapping in task.publish} - completion_outputs
        if missing_publications:
            raise AgentResultError(
                "Agent completion outputs do not cover publish sources: "
                + ", ".join(sorted(missing_publications))
            )
        for mapping in task.publish:
            attachment = by_path.get(mapping.source)
            if attachment is None:
                raise AgentResultError(f"Agent publish source {mapping.source!r} was not collected")
            data = blob_store.read_verified(attachment["kigumi_attachment"])
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                text = None
            if text is None or len(data) > limits.inline_text_max_bytes:
                blob = {
                    "kigumi_blob": attachment["kigumi_attachment"],
                    "path": mapping.destination,
                    "bytes": len(data),
                }
                published.append({"source": mapping.source, "blob": blob})
            else:
                files[mapping.destination] = text
                published.append(
                    {"source": mapping.source, "path": mapping.destination, "bytes": len(data)}
                )
        artifact: dict[str, Any] = {
            "agent_schema": AGENT_SCHEMA,
            "task": task.canonical(),
            "completion": result.completion.canonical(),
            "agent": {
                **copy.deepcopy(dict(adapter_identity)),
                "capabilities": {
                    "filesystem": capabilities.filesystem,
                    "terminal": capabilities.terminal,
                },
                "isolation": {"level": "staged", "enforced_by": "kigumi-workspace"},
            },
            "attachments": attachments,
            "published": published,
        }
        if files:
            artifact["files"] = files
        validate_agent_artifact(artifact, blob_store)
        duration_seconds = time.monotonic() - started
        trajectory = _capture_trajectory(
            events,
            truncated,
            blob_store,
            mode=evidence_policy.trajectory,
        )
        provenance = {
            "task_sha256": sha(task.canonical()),
            "instruction_sha256": sha(task.instruction),
            "instruction_evidence": scrub_evidence(
                str(task.instruction),
                mode=evidence_policy.request,
            ),
            "prompt_resolution": copy.deepcopy(
                dict(prompt_resolution) if prompt_resolution is not None else None
            ),
            "spec_digest": spec.digest,
            "usage": dict(result.usage) if result.usage is not None else None,
            "duration_seconds": duration_seconds,
            "workspace_manifest": _manifest_changes(baseline, _workspace_manifest(workspace)),
            "execution": dict(result.metadata),
            "trajectory": trajectory,
            "evidence": evidence,
            "exit_reason": "completed",
            "evidence_policy": evidence_policy.canonical(),
            "evidence_policy_digest": evidence_policy.digest,
        }
        return AgentTaskExecution(artifact, provenance)
    except Exception as error:
        try:
            failure_attachments = _collect(workspace, task.collect, limits, blob_store)
        except Exception:
            failure_attachments = []
        usage, stop_reason = _trajectory_summary(events)
        if isinstance(error, AgentExecutionFailure):
            typed_error = error
        elif isinstance(error, ProviderFailure):
            typed_error = AgentExecutionFailure(provider_failure=error)
        else:
            typed_error = AgentExecutionFailure(runtime_code=AgentRuntimeFailureCode.PROTOCOL)
        failure = {
            "failure_schema": 2,
            "node": node_name,
            "task_sha256": sha(task.canonical()),
            "instruction_sha256": sha(task.instruction),
            "instruction_evidence": scrub_evidence(
                str(task.instruction),
                mode=evidence_policy.request,
            ),
            "prompt_resolution": copy.deepcopy(
                dict(prompt_resolution) if prompt_resolution is not None else None
            ),
            "status": "failed",
            "failure": canonical_failure(typed_error),
            "usage": usage,
            "stop_reason": stop_reason,
            "duration_seconds": time.monotonic() - started,
            "workspace_manifest": _manifest_changes(baseline, _workspace_manifest(workspace)),
            "attachments": failure_attachments,
            "published": [],
            "trajectory": _capture_trajectory(
                events,
                truncated,
                blob_store,
                mode=evidence_policy.trajectory,
            ),
            "evidence": evidence,
            "evidence_policy": evidence_policy.canonical(),
            "evidence_policy_digest": evidence_policy.digest,
        }
        atomic_write_json(
            artifacts_path / "runs" / run_id / "failures" / f"{node_name}.json",
            failure,
        )
        if typed_error is error:
            raise
        raise typed_error from error
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def _collect(
    workspace: Path,
    selectors: tuple[AgentFileSelector, ...],
    limits: AgentLimits,
    blob_store: BlobStore,
) -> list[dict[str, Any]]:
    candidates: dict[str, Path] = {}
    for path in workspace.rglob("*"):
        if path.is_symlink():
            raise AgentResultError(f"Agent workspace contains a symlink: {path}")
    for selector in selectors:
        for path in workspace.glob(selector.pattern):
            if not path.is_file():
                continue
            relative = path.relative_to(workspace).as_posix()
            if not relative.startswith(".kigumi/"):
                candidates[relative] = path
    if len(candidates) > limits.max_files:
        raise AgentResultError("Agent collection exceeds max_files")
    total = 0
    attachments: list[dict[str, Any]] = []
    for relative, path in sorted(candidates.items()):
        size = path.stat().st_size
        if size > limits.max_single_file_bytes:
            raise AgentResultError(f"Agent file {relative!r} exceeds max_single_file_bytes")
        total += size
        if total > limits.max_bytes:
            raise AgentResultError("Agent collection exceeds max_bytes")
        digest, captured_size = blob_store.ingest(path)
        attachments.append(
            {
                "kigumi_attachment": digest,
                "workspace_path": relative,
                "bytes": captured_size,
                "media_type": mimetypes.guess_type(relative)[0] or "application/octet-stream",
            }
        )
    return attachments


def _workspace_manifest(workspace: Path) -> dict[str, str]:
    manifest: dict[str, str] = {}
    for path in workspace.rglob("*"):
        if path.is_symlink() or not path.is_file():
            continue
        relative = path.relative_to(workspace).as_posix()
        if relative.startswith(".kigumi/"):
            continue
        manifest[relative] = sha256(path.read_bytes()).hexdigest()
    return manifest


def _manifest_changes(before: Mapping[str, str], after: Mapping[str, str]) -> list[dict[str, str]]:
    changes: list[dict[str, str]] = []
    for path in sorted(set(before) | set(after)):
        status = (
            "created"
            if path not in before
            else "deleted"
            if path not in after
            else "unchanged"
            if before[path] == after[path]
            else "modified"
        )
        changes.append({"workspace_path": path, "status": status})
    return changes


def validate_agent_artifact(artifact: Mapping[str, Any], blob_store: BlobStore) -> None:
    if artifact.get("agent_schema") != AGENT_SCHEMA:
        raise AgentResultError("Agent artifact has an unsupported schema")
    allowed = {
        "agent_schema",
        "task",
        "completion",
        "agent",
        "attachments",
        "published",
        "files",
    }
    if extra := set(artifact) - allowed:
        raise AgentResultError(
            "Agent artifact contains execution evidence outside origin provenance: "
            + ", ".join(sorted(extra))
        )
    task = artifact.get("task")
    if not isinstance(task, Mapping) or not isinstance(task.get("instruction"), str):
        raise AgentResultError("Agent artifact task must be canonical task data")
    completion_value = artifact.get("completion")
    if not isinstance(completion_value, Mapping):
        raise AgentResultError("Agent artifact completion must be an object")
    completion = AgentCompletion.from_mapping(completion_value)
    attachments = artifact.get("attachments")
    if not isinstance(attachments, list):
        raise AgentResultError("Agent artifact attachments must be a list")
    for reference in attachments:
        if not isinstance(reference, dict):
            raise AgentResultError("Agent attachment must be an object")
        digest = reference.get("kigumi_attachment")
        path = reference.get("workspace_path")
        size = reference.get("bytes")
        if not isinstance(digest, str) or not isinstance(path, str) or not isinstance(size, int):
            raise AgentResultError("Agent attachment marker is malformed")
        _workspace_pattern(path)
        data = blob_store.read_verified(digest)
        if len(data) != size:
            raise AgentResultError(f"Agent attachment {path!r} byte count does not match")
    collected = {item["workspace_path"]: item for item in attachments}
    if not set(completion.outputs) <= set(collected):
        raise AgentResultError("Agent completion outputs must reference collected attachments")
    published = artifact.get("published", [])
    if not isinstance(published, list):
        raise AgentResultError("Agent artifact published must be a list")
    destinations: set[str] = set()
    expected_blobs: list[dict[str, Any]] = []
    expected_text_destinations: set[str] = set()
    files = artifact.get("files", {})
    if not isinstance(files, dict):
        raise AgentResultError("Agent artifact files must be a mapping")
    for item in published:
        if not isinstance(item, dict) or item.get("source") not in collected:
            raise AgentResultError("Agent published output must reference a collected attachment")
        blob = item.get("blob")
        if blob is not None:
            source = collected[item["source"]]
            if not isinstance(blob, dict) or blob.get("kigumi_blob") != source["kigumi_attachment"]:
                raise AgentResultError("Agent published blob must match its collected attachment")
            destination = blob.get("path")
            expected_blobs.append(blob)
        else:
            destination = item.get("path")
            if not isinstance(destination, str) or not isinstance(files.get(destination), str):
                raise AgentResultError("Agent text publication must reference top-level files")
            source_data = blob_store.read_verified(collected[item["source"]]["kigumi_attachment"])
            if files[destination].encode("utf-8") != source_data:
                raise AgentResultError("Agent text publication must match its collected attachment")
            expected_text_destinations.add(destination)
        if not isinstance(destination, str) or destination in destinations:
            raise AgentResultError("Agent published destinations must be unique paths")
        _workspace_pattern(destination)
        destinations.add(destination)
    if {item["source"] for item in published} - set(completion.outputs):
        raise AgentResultError("Agent publications must be declared by completion outputs")
    if set(files) != expected_text_destinations:
        raise AgentResultError("Agent artifact files must come only from exact publications")
    actual_blobs = list(_materializable_blob_references(artifact))
    if actual_blobs != expected_blobs:
        raise AgentResultError("Agent artifact blobs must come only from exact publications")


def validate_agent_provenance(provenance: Mapping[str, Any], blob_store: BlobStore) -> None:
    """Verify every retained Agent evidence attachment before cold/warm replay."""
    for reference in _evidence_attachment_references(provenance):
        digest = reference.get("kigumi_attachment")
        path = reference.get("workspace_path")
        stored_bytes = reference.get("stored_bytes", reference.get("bytes"))
        if (
            not isinstance(digest, str)
            or not isinstance(path, str)
            or not isinstance(stored_bytes, int)
        ):
            raise AgentResultError("Agent evidence attachment marker is malformed")
        _workspace_pattern(path)
        data = blob_store.read_verified(digest)
        if len(data) != stored_bytes:
            raise AgentResultError(f"Agent evidence attachment {path!r} byte count does not match")


def _capture_trajectory(
    events: list[dict[str, Any]],
    truncated: bool,
    blob_store: BlobStore,
    *,
    mode: EvidenceMode,
) -> dict[str, Any]:
    if not events:
        return {"events": 0, "truncated": truncated, "mode": mode}
    data = "".join(
        json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        for event in events
    ).encode("utf-8")
    captured = capture_evidence(
        data,
        media_type="application/x-ndjson",
        mode=mode,
    )
    result = {
        **captured.descriptor,
        "workspace_path": ".kigumi/normalized-events.jsonl",
        "events": len(events),
        "truncated": truncated,
    }
    if captured.data is not None:
        result["kigumi_attachment"] = blob_store.put(captured.data)
        result["stored_bytes"] = len(captured.data)
    return result


def _trajectory_summary(
    events: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str | None]:
    usage: dict[str, Any] | None = None
    stop_reason: str | None = None
    for event in events:
        if event.get("type") == "pi_execution_summary":
            if isinstance(event.get("usage"), Mapping):
                usage = copy.deepcopy(dict(event["usage"]))
            reason = event.get("stop_reason")
            if isinstance(reason, str):
                stop_reason = reason
            continue
        if event.get("type") != "message_end" or not isinstance(event.get("message"), Mapping):
            continue
        message = event["message"]
        if isinstance(message.get("usage"), Mapping):
            usage = copy.deepcopy(dict(message["usage"]))
        reason = message.get("stopReason", message.get("stop_reason"))
        if isinstance(reason, str):
            stop_reason = reason
    return usage, stop_reason


def _validate_attachment_reference(reference: Mapping[str, Any], blob_store: BlobStore) -> None:
    digest = reference.get("kigumi_attachment")
    path = reference.get("workspace_path")
    size = reference.get("bytes")
    if not isinstance(digest, str) or not isinstance(path, str) or not isinstance(size, int):
        raise AgentResultError("Agent attachment marker is malformed")
    _workspace_pattern(path)
    data = blob_store.read_verified(digest)
    if len(data) != size:
        raise AgentResultError(f"Agent attachment {path!r} byte count does not match")


def _materializable_blob_references(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        if "kigumi_blob" in value:
            yield value
        for child in value.values():
            yield from _materializable_blob_references(child)
    elif isinstance(value, list):
        for child in value:
            yield from _materializable_blob_references(child)


def _evidence_attachment_references(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        if "kigumi_attachment" in value:
            yield value
        for child in value.values():
            yield from _evidence_attachment_references(child)
    elif isinstance(value, list):
        for child in value:
            yield from _evidence_attachment_references(child)
