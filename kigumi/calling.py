"""Cached, budget-aware LLM calling built on the transport layer.

Messages may contain ``{"kigumi_file": "<path>"}`` references. Their cache
keys use content hashes, cached messages retain references plus those hashes,
and only live requests expand references into data URLs for the transport.
"""

from __future__ import annotations

import base64
import contextvars
import copy
import json
import mimetypes
import os
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from typing import Any, Protocol

from ._safe_io import (
    FileIdentity,
    digest_open_file,
    lstat_regular_file,
    open_regular_file,
    read_regular_bytes,
)
from .artifacts import atomic_write_json, sha
from .errors import CacheIntegrityError
from .evidence import EvidencePolicy, scrub_evidence
from .failures import (
    ProviderFailure,
    ProviderFailureStage,
    canonical_failure,
    provider_failure_from_exception,
)
from .prompt import (
    Attachment,
    Message,
    PreflightPolicy,
    PreflightReport,
    PreflightViolation,
    PromptResolution,
    RequestTooLarge,
    ResolvedPrompt,
    ResponseSpec,
    preflight,
    validate_prompt_resolution_record,
)
from .slots import FileSlots
from .store import CacheLookup
from .transport import EmptyResponseError, Transport

_call_observer: contextvars.ContextVar[list[dict[str, Any]] | None] = contextvars.ContextVar(
    "kigumi_call_observer", default=None
)
_durable_side_effect: contextvars.ContextVar[Callable[[dict[str, Any]], None] | None] = (
    contextvars.ContextVar("kigumi_durable_side_effect", default=None)
)
_prompt_lineage: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "kigumi_prompt_lineage", default=None
)
_file_snapshot_reader: contextvars.ContextVar[Callable[[str | Path], bytes] | None] = (
    contextvars.ContextVar("kigumi_file_snapshot_reader", default=None)
)
_response_spec: contextvars.ContextVar[ResponseSpec | None] = contextvars.ContextVar(
    "kigumi_response_spec", default=None
)
_DEFAULT_EVIDENCE_POLICY = EvidencePolicy()
_DEFAULT_PREFLIGHT_POLICY = PreflightPolicy()


def _is_managed_prompt_resolution(value: Any) -> bool:
    """Return whether lineage names an explicit managed PromptSpec.

    A file-bearing direct-chat gets a synthetic ``spec="unmanaged"`` record so
    its content hash and request lineage remain observable.  The record's
    existence is therefore not evidence of managed PromptSpec ownership.
    """
    if not isinstance(value, Mapping):
        return False
    spec = value.get("spec")
    return isinstance(spec, str) and bool(spec) and spec != "unmanaged"


@contextmanager
def observe() -> Iterator[list[dict[str, Any]]]:
    """Collect every LLMCaller call made within this context."""
    calls: list[dict[str, Any]] = []
    token = _call_observer.set(calls)
    try:
        yield calls
    finally:
        _call_observer.reset(token)


@contextmanager
def durable_side_effect_boundary(
    callback: Callable[[dict[str, Any]], None],
) -> Iterator[None]:
    """Mark the first live provider request in one durable attempt."""
    token = _durable_side_effect.set(callback)
    try:
        yield
    finally:
        _durable_side_effect.reset(token)


@contextmanager
def prompt_resolution_boundary(
    resolution: PromptResolution,
    *,
    phase: str = "primary",
    repair_round: int = 0,
) -> Iterator[None]:
    """Bind a base Prompt resolution to transformed primary/repair requests."""
    if phase not in {"primary", "repair"}:
        raise ValueError("prompt resolution phase must be primary or repair")
    if repair_round < 0:
        raise ValueError("repair_round must be non-negative")
    # This is an explicit managed-request boundary.  Validate it before the
    # context is installed so a malformed in-memory record cannot reach a
    # cache lookup or provider through a plain ``caller.call``.
    validate_prompt_resolution_record(resolution.canonical())
    lineage = {
        **resolution.canonical(),
        "base_resolution_digest": resolution.digest,
        "phase": phase,
        "repair_round": repair_round,
    }
    token = _prompt_lineage.set(lineage)
    try:
        yield
    finally:
        _prompt_lineage.reset(token)


@contextmanager
def file_snapshot_boundary(reader: Callable[[str | Path], bytes]) -> Iterator[None]:
    """Bind an immutable DAG file reader to LLM file-reference preparation."""
    if not callable(reader):
        raise TypeError("file snapshot reader must be callable")
    token = _file_snapshot_reader.set(reader)
    try:
        yield
    finally:
        _file_snapshot_reader.reset(token)


@contextmanager
def response_spec_boundary(response_spec: ResponseSpec) -> Iterator[None]:
    """Bind a response schema to L1 cache identity without sending it as a provider param."""
    if not isinstance(response_spec, ResponseSpec):
        raise TypeError("response_spec must be ResponseSpec")
    token = _response_spec.set(response_spec)
    try:
        yield
    finally:
        _response_spec.reset(token)


def _data_url(data: bytes, mime: str) -> str:
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def _file_error(message: str, path: Path) -> ValueError:
    return ValueError(f"kigumi_file {message}: {path}")


def _file_identity(info: os.stat_result) -> FileIdentity:
    """Return stable metadata used to reject obvious path/file races."""
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _regular_file_probe(path: Path) -> tuple[int, FileIdentity]:
    """Inspect a path without following a symlink and require a regular file."""
    info = lstat_regular_file(path, error=_file_error)
    return info.st_size, _file_identity(info)


class BudgetExceeded(RuntimeError):
    """Raised when a reservation or actual spend exceeds the configured ceiling."""


class DryRunError(RuntimeError):
    """Raised when dry-run mode would otherwise make a live model request."""


class _SingleFlightLock:
    """Track one in-process key lock until its last participant exits."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.waiters = 0
        self.done = False
        self.result: str | None = None
        self.error: BaseException | None = None


class Caller(Protocol):
    """Any object that can perform the normalized call used by repair helpers."""

    def call(
        self,
        messages: list[dict[str, Any]] | str,
        model: str = "default",
        **params: Any,
    ) -> str:
        """Return a completion for normalized chat messages."""


@dataclass(frozen=True)
class _FileReference:
    """A validated reference kept byte-free until a live request needs it."""

    path: Path
    mime: str
    digest: str
    size_bytes: int
    detail: Any
    has_detail: bool
    identity: FileIdentity | None = None
    snapshot_data: bytes | None = None


@dataclass(frozen=True)
class _FilePreflight:
    """Regular-file metadata collected before any attachment is hashed."""

    path: Path
    size_bytes: int
    identity: FileIdentity | None
    snapshot_data: bytes | None = None


def _open_regular_file(
    path: Path,
    *,
    expected_identity: FileIdentity | None = None,
    phase: str,
) -> Any:
    """Open one path as a non-blocking, descriptor-bound regular file.

    The lstat is only an early rejection for ordinary symlinks and special files;
    the descriptor's fstat is authoritative. O_NONBLOCK prevents a FIFO introduced
    after lstat from blocking, and O_NOFOLLOW prevents a final symlink race on
    platforms that expose that flag. All subsequent reads use this descriptor.
    """
    return open_regular_file(
        path,
        identity=_file_identity,
        expected_identity=expected_identity,
        phase=phase,
        error=_file_error,
    )


def _hash_regular_file(probe: _FilePreflight) -> str:
    """Hash a preflighted file through one stable descriptor."""
    if probe.snapshot_data is not None:
        if len(probe.snapshot_data) != probe.size_bytes:
            raise ValueError(f"kigumi_file changed during hashing: {probe.path}")
        return sha256(probe.snapshot_data).hexdigest()
    with _open_regular_file(
        probe.path,
        expected_identity=probe.identity,
        phase="before hashing",
    ) as handle:
        digest, size, _ = digest_open_file(
            handle,
            probe.path,
            identity=_file_identity,
            expected_identity=probe.identity,
            before_phase="before hashing",
            during_phase="during hashing",
            chunk_size=1024 * 1024,
            error=_file_error,
        )
    if size != probe.size_bytes:
        raise ValueError(f"kigumi_file changed during hashing: {probe.path}")
    return digest


def _read_preflighted_file(reference: _FileReference) -> bytes:
    """Read a reference only after rechecking its regular-file identity and size."""
    if reference.snapshot_data is not None:
        return reference.snapshot_data
    if reference.identity is None:
        size_bytes, identity = _regular_file_probe(reference.path)
        probe = _FilePreflight(reference.path, size_bytes, identity)
    else:
        probe = _FilePreflight(reference.path, reference.size_bytes, reference.identity)
    with _open_regular_file(
        probe.path,
        expected_identity=probe.identity,
        phase="after hashing",
    ) as handle:
        _digest, size, data = digest_open_file(
            handle,
            probe.path,
            identity=_file_identity,
            expected_identity=probe.identity,
            before_phase="after hashing",
            during_phase="after hashing",
            chunk_size=1024 * 1024,
            error=_file_error,
            collect=True,
            max_bytes=probe.size_bytes,
        )
    if size != probe.size_bytes or data is None:
        raise ValueError(f"kigumi_file changed after hashing: {probe.path}")
    return data


@dataclass(frozen=True)
class _PreparedMessages:
    """The three representations required for file-reference calling."""

    key_messages: list[dict[str, Any]]
    cache_messages: list[dict[str, Any]]
    transport_messages: list[dict[str, Any]]
    attachments: list[Attachment]


def read_call_cache(cache_path: Path, cache_key: str | None = None) -> CacheLookup:
    """Read one L1 payload while preserving missing and corrupt states."""
    if cache_key is not None:
        cache_path = Path(cache_path) / "llm" / f"{cache_key}.json"
        expected_key = cache_key
    else:
        cache_path = Path(cache_path)
        expected_key = cache_path.name.removesuffix(".json")
    try:
        raw = read_regular_bytes(
            cache_path,
            identity=_file_identity,
            phase="reading L1 cache",
            error=lambda message, path: ValueError(f"call cache file {message}: {path}"),
            snapshot=True,
            allow_atomic_replace=True,
        )
    except FileNotFoundError:
        return CacheLookup("MISSING", None, None, None, "call cache file is missing")
    except (OSError, ValueError) as error:
        return CacheLookup("CORRUPT", None, None, None, f"call cache JSON read failed: {error}")
    try:
        cached = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        return CacheLookup("CORRUPT", None, None, None, f"call cache JSON read failed: {error}")
    if not isinstance(cached, dict):
        return CacheLookup("CORRUPT", None, None, None, "call cache JSON is not an object")

    response = cached.get("response")
    actual_sha256 = sha(response) if isinstance(response, str) else None
    expected_sha256 = cached.get("response_sha256")
    if not isinstance(expected_sha256, str):
        return CacheLookup(
            "CORRUPT",
            None,
            None,
            actual_sha256,
            "call cache response_sha256 is missing or not a string",
        )
    if not isinstance(response, str) or not response:
        return CacheLookup(
            "CORRUPT",
            None,
            expected_sha256,
            actual_sha256,
            "call cache response is missing or empty",
        )
    if expected_sha256 != actual_sha256:
        return CacheLookup(
            "CORRUPT",
            None,
            expected_sha256,
            actual_sha256,
            "call cache response digest validation failed",
        )
    metadata = cached.get("meta")
    actual_key = metadata.get("key") if isinstance(metadata, dict) else None
    if actual_key != expected_key:
        return CacheLookup(
            "CORRUPT",
            None,
            expected_sha256,
            actual_sha256,
            "call cache request key validation failed",
        )
    return CacheLookup("VALID", cached, expected_sha256, actual_sha256, None)


class BudgetPermit:
    """Reservation hold on budget tokens."""

    def __init__(self, budget: Budget, estimated_tokens: int) -> None:
        self._budget = budget
        self._estimated_tokens = estimated_tokens
        self._active = True

    def commit(self, actual_usage: dict[str, Any]) -> None:
        """Convert this reservation to actual spend."""
        self._budget._commit(self, actual_usage)

    def cancel(self) -> None:
        """Release this reservation without spending it."""
        self._budget._cancel(self)


class Budget:
    """Reserve estimated tokens before calls and account their actual usage.

    Reservations close the in-process admission race between concurrent calls. The
    estimate is best effort: a provider response may use more tokens, and commit
    records that actual usage before enforcing the ceiling.
    """

    def __init__(self, max_tokens: int | None) -> None:
        self.max_tokens = max_tokens
        self._spent = 0
        self._reserved = 0
        self._lock = threading.Lock()

    @property
    def spent(self) -> int:
        """The cumulative number of reported total tokens."""
        with self._lock:
            return self._spent

    def reserve(self, estimated_tokens: int) -> BudgetPermit:
        """Reserve tokens before a call, raising when the remaining budget is insufficient."""
        estimated = self._coerce_tokens(estimated_tokens, name="estimated_tokens")
        with self._lock:
            if self.max_tokens is not None:
                available = self.max_tokens - self._spent - self._reserved
                if estimated > available:
                    raise BudgetExceeded(
                        "Token budget reservation denied: "
                        f"requested {estimated}, available {available}, "
                        f"already spent {self._spent}"
                    )
            self._reserved += estimated
        return BudgetPermit(self, estimated)

    def record(self, usage: dict[str, Any]) -> None:
        """Record usage without a prior reservation and enforce the configured cap."""
        total = self._usage_tokens(usage)
        with self._lock:
            self._spent += total
            self._raise_if_exceeded()

    @staticmethod
    def _coerce_tokens(value: int, *, name: str) -> int:
        if type(value) is not int:
            raise TypeError(f"{name} must be an integer")
        if value < 0:
            raise ValueError(f"{name} must be non-negative")
        return value

    @staticmethod
    def _usage_tokens(usage: dict[str, Any]) -> int:
        if not isinstance(usage, dict):
            raise TypeError("usage must be a mapping")
        if "total_tokens" not in usage:
            # Some transports intentionally omit usage for a successful response;
            # that remains a zero-token response, while an explicitly malformed
            # total_tokens value is rejected below.
            return 0
        total = usage["total_tokens"]
        if type(total) is not int:  # bool is an int subclass but not a token count.
            raise TypeError("usage.total_tokens must be an integer")
        if total < 0:
            raise ValueError("usage.total_tokens must be non-negative")
        return total

    def _commit(self, permit: BudgetPermit, usage: dict[str, Any]) -> None:
        total = self._usage_tokens(usage)
        with self._lock:
            if not permit._active:
                raise RuntimeError("Budget permit is no longer active")
            permit._active = False
            self._reserved -= permit._estimated_tokens
            self._spent += total
            self._raise_if_exceeded()

    def _cancel(self, permit: BudgetPermit) -> None:
        with self._lock:
            if not permit._active:
                return
            permit._active = False
            self._reserved -= permit._estimated_tokens

    def _raise_if_exceeded(self) -> None:
        if self.max_tokens is not None and self._spent > self.max_tokens:
            raise BudgetExceeded(f"Token budget exceeded: spent {self._spent} > {self.max_tokens}")


class LLMCaller:
    """Add deterministic disk caching, provenance, dry-run, and budget controls."""

    def __init__(
        self,
        transport: Transport,
        cache_dir: Path,
        seed: int = 0,
        budget: Budget | None = None,
        dry: bool = False,
        slots: FileSlots | None = None,
        evidence_policy: EvidencePolicy = _DEFAULT_EVIDENCE_POLICY,
        preflight_policy: PreflightPolicy = _DEFAULT_PREFLIGHT_POLICY,
        key_lock_timeout_seconds: float | None = None,
    ) -> None:
        self.transport = transport
        self.cache_dir = Path(cache_dir)
        self.seed = seed
        self.budget = budget
        self.dry = dry
        self.slots = slots
        self.key_lock_timeout_seconds = key_lock_timeout_seconds
        if not isinstance(evidence_policy, EvidencePolicy):
            raise TypeError("evidence_policy must be EvidencePolicy")
        if not isinstance(preflight_policy, PreflightPolicy):
            raise TypeError("preflight_policy must be PreflightPolicy")
        self.evidence_policy = evidence_policy
        self.preflight_policy = preflight_policy
        self.calls: list[dict[str, Any]] = []
        self._calls_lock = threading.Lock()
        self._key_locks: dict[str, _SingleFlightLock] = {}
        self._key_locks_lock = threading.Lock()
        # The threading lock is the fast in-process layer. When FileSlots is enabled,
        # acquire_key adds cross-process single-flight for this L1 key. Budget
        # reservation remains process-local; there is no durable cross-process budget
        # ledger or crash-recovery/refund protocol here.

    def call(
        self,
        messages: list[dict[str, Any]] | list[Message] | str,
        model: str = "default",
        **params: Any,
    ) -> str:
        """Return a cached or live completion for normalized chat messages."""
        prompt_lineage = _prompt_lineage.get()
        base_resolution = messages.resolution if isinstance(messages, ResolvedPrompt) else None
        if base_resolution is not None:
            # ``PromptResolution`` is intentionally a lightweight immutable
            # in-memory value object.  The execution boundary owns the strict
            # persisted-schema/digest check, and it must happen before even
            # preparing a cache key or resolving a transport.
            validate_prompt_resolution_record(base_resolution.canonical())
        elif isinstance(prompt_lineage, dict) and "base_resolution_digest" in prompt_lineage:
            # A repair/explicit lineage may carry the resolution separately
            # from the message object.  Direct-chat lineage is created below
            # and has no base_resolution_digest, so it remains unmanaged.
            validate_prompt_resolution_record(prompt_lineage)
        response_spec = _response_spec.get()
        if response_spec is None and base_resolution is not None:
            response_spec = base_resolution.response_spec
        if response_spec is None and isinstance(prompt_lineage, dict):
            response_spec = self._response_spec_from_lineage(prompt_lineage)
        if response_spec is None:
            response_spec = ResponseSpec()
        if base_resolution is not None and base_resolution.response_spec != response_spec:
            base_resolution = replace(base_resolution, response_spec=response_spec)
        if prompt_lineage is None and base_resolution is not None:
            prompt_lineage = {
                **base_resolution.canonical(),
                "base_resolution_digest": base_resolution.digest,
                "phase": "primary",
                "repair_round": 0,
            }
        normalized_messages = self._normalize_messages(messages)
        prepared = self._prepare_file_references(
            normalized_messages,
            preflight_policy=self.preflight_policy,
            snapshot_reader=_file_snapshot_reader.get(),
        )
        key_messages = prepared.key_messages if prepared is not None else normalized_messages
        cache_messages = prepared.cache_messages if prepared is not None else normalized_messages
        request_resolution = self._request_resolution(
            base_resolution=base_resolution,
            prompt_lineage=prompt_lineage,
            key_messages=key_messages,
            attachments=prepared.attachments if prepared is not None else [],
            response_spec=response_spec,
        )
        if prepared is not None and prepared.attachments:
            existing_lineage = prompt_lineage or {}
            prompt_lineage = {
                **request_resolution.canonical(),
                "base_resolution_digest": existing_lineage.get(
                    "base_resolution_digest", request_resolution.digest
                ),
                "phase": existing_lineage.get("phase", "primary"),
                "repair_round": existing_lineage.get("repair_round", 0),
            }
        if base_resolution is not None:
            # Rebinding messages/attachments/response schema creates the actual
            # request record used by the cache and provider.  Validate that
            # derived record as well, not only the caller-supplied base.
            validate_prompt_resolution_record(request_resolution.canonical())
        report = preflight(request_resolution, self.preflight_policy)
        if not report.is_valid():
            raise RequestTooLarge(report)
        resolved_model = self.transport.resolve(model)
        # Cache keys preserve caller intent before transport parameter normalization.
        key_inputs: dict[str, Any] = {
            "messages": key_messages,
            "model": resolved_model,
            "params": params,
            "seed": self.seed,
        }
        if response_spec != ResponseSpec():
            key_inputs["response_spec"] = response_spec.canonical()
        key = sha(key_inputs)
        cache_path = self.cache_dir / "llm" / f"{key}.json"
        cached = self._read_cached_response(cache_path)
        if cached is not None:
            return self._record_cache_hit(
                cached,
                key=key,
                model_alias=model,
                model=resolved_model,
                params=params,
                messages=key_messages,
                prompt_lineage=prompt_lineage,
            )

        with self._lock_for_key(key) as single_flight, self._file_lock_for_key(key):
            if single_flight.error is not None:
                raise single_flight.error
            cached = self._read_cached_response(cache_path)
            if cached is not None:
                result = self._record_cache_hit(
                    cached,
                    key=key,
                    model_alias=model,
                    model=resolved_model,
                    params=params,
                    messages=key_messages,
                    prompt_lineage=prompt_lineage,
                )
                single_flight.result = result
                return result

            if single_flight.done:
                if single_flight.result is None:
                    raise RuntimeError("single-flight completed without a result")
                return single_flight.result

            if self.dry:
                raise DryRunError(f"Dry run would call model {model!r} for cache key {key}")

            transport_messages = (
                self._expand_transport_messages(prepared.transport_messages)
                if prepared is not None
                else normalized_messages
            )
            permit = (
                self.budget.reserve(self._estimate_tokens(normalized_messages, params))
                if self.budget is not None
                else None
            )
            started = time.monotonic()
            # 槽位限的是远程请求本身;base64 展开是本地工作,不许占着槽做。
            try:
                slot_context = self.slots.acquire() if self.slots is not None else nullcontext()
                with slot_context:
                    durable_callback = _durable_side_effect.get()
                    if durable_callback is not None:
                        self._validate_durable_transport()
                        durable_callback(
                            {
                                "active_effect_schema": 1,
                                "kind": "call",
                                "key": key,
                                "model": resolved_model,
                                "params_digest": sha(params),
                                "prompt_sha": sha(key_messages),
                                "managed": _is_managed_prompt_resolution(prompt_lineage),
                                "prompt_resolution": copy.deepcopy(prompt_lineage),
                            }
                        )
                    response = self.transport.complete(transport_messages, model, **params)
            except Exception as error:
                if permit is not None:
                    permit.cancel()
                seconds = time.monotonic() - started
                failure = (
                    error
                    if isinstance(error, ProviderFailure)
                    else provider_failure_from_exception(
                        error,
                        provider=type(self.transport).__name__,
                        stage=ProviderFailureStage.TRANSPORT,
                    )
                )
                metadata = self._meta(
                    key=key,
                    model_alias=model,
                    model=resolved_model,
                    params=params,
                    messages=key_messages,
                    seconds=seconds,
                    usage={},
                    cache="failure",
                    failure=canonical_failure(failure),
                    request_value=cache_messages,
                    prompt_lineage=prompt_lineage,
                )
                self._append_call(metadata)
                raise failure from None
            except BaseException:
                if permit is not None:
                    permit.cancel()
                raise
            seconds = time.monotonic() - started
            if not response.text:
                if permit is not None:
                    permit.cancel()
                empty = EmptyResponseError(
                    f"Transport returned an empty response for model {resolved_model!r}."
                )
                failure = provider_failure_from_exception(
                    empty,
                    provider=type(self.transport).__name__,
                    stage=ProviderFailureStage.RESPONSE,
                )
                self._append_call(
                    self._meta(
                        key=key,
                        model_alias=model,
                        model=resolved_model,
                        params=params,
                        messages=key_messages,
                        seconds=seconds,
                        usage=response.usage,
                        cache="failure",
                        provider_response_id=response.provider_response_id,
                        provider_model=response.model,
                        provider_model_observed=response.model_observed,
                        failure=canonical_failure(failure),
                        request_value=cache_messages,
                        response_value={
                            "text": response.text,
                            "reasoning": response.reasoning,
                        },
                        prompt_lineage=prompt_lineage,
                    )
                )
                raise failure from None
            try:
                # Validate provider usage before persisting a successful response;
                # malformed accounting data must not poison the cache or budget.
                Budget._usage_tokens(response.usage)
                payload = {
                    "meta": self._meta(
                        key=key,
                        model_alias=model,
                        model=resolved_model,
                        params=params,
                        messages=key_messages,
                        seconds=seconds,
                        usage=response.usage,
                        cache="miss",
                        provider_response_id=response.provider_response_id,
                        provider_model=response.model,
                        provider_model_observed=response.model_observed,
                        request_value=cache_messages,
                        response_value={
                            "text": response.text,
                            "reasoning": response.reasoning,
                        },
                        prompt_lineage=prompt_lineage,
                    ),
                    "response": response.text,
                    "response_sha256": sha(response.text),
                    "messages": cache_messages,
                    "reasoning": response.reasoning,
                }
                atomic_write_json(cache_path, payload)
                self._append_call(payload["meta"])
                if permit is not None:
                    try:
                        permit.commit(response.usage)
                    finally:
                        permit = None
            except Exception:
                if permit is not None:
                    permit.cancel()
                raise
            except BaseException:
                if permit is not None:
                    permit.cancel()
                raise
            single_flight.result = response.text
            return response.text

    @staticmethod
    def _normalize_messages(
        messages: list[dict[str, Any]] | list[Message] | str,
    ) -> list[dict[str, Any]]:
        if isinstance(messages, str):
            # Strip a ResolvedPrompt subclass only after its lineage was captured.
            return [{"role": "user", "content": str(messages)}]
        normalized: list[dict[str, Any]] = []
        for message in messages:
            if isinstance(message, Message):
                parts = message.parts
                if len(parts) == 1:
                    content: Any = parts[0]
                else:
                    content = [
                        {"type": "text", "text": part} if isinstance(part, str) else part
                        for part in parts
                    ]
                normalized.append({"role": message.role, "content": content})
            elif isinstance(message, dict):
                normalized.append(message)
            else:
                raise TypeError("messages must contain dictionaries or Message values")
        return normalized

    @staticmethod
    def _response_spec_from_lineage(lineage: dict[str, Any]) -> ResponseSpec | None:
        value = lineage.get("response_spec")
        if not isinstance(value, dict):
            return None
        try:
            return ResponseSpec(
                schema_sha256=value.get("schema_sha256"),
                format=value.get("format", "text"),
            )
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _attachments_from_lineage(lineage: dict[str, Any] | None) -> list[Attachment]:
        if lineage is None or not isinstance(lineage.get("attachments"), list):
            return []
        attachments: list[Attachment] = []
        for value in lineage["attachments"]:
            if not isinstance(value, dict):
                continue
            try:
                attachments.append(
                    Attachment(
                        path=value["path"],
                        content_hash=value["content_hash"],
                        mime_type=value["mime_type"],
                        size_bytes=value["size_bytes"],
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return attachments

    @classmethod
    def _request_resolution(
        cls,
        *,
        base_resolution: PromptResolution | None,
        prompt_lineage: dict[str, Any] | None,
        key_messages: list[dict[str, Any]],
        attachments: list[Attachment],
        response_spec: ResponseSpec,
    ) -> PromptResolution:
        request_messages = [
            Message(
                role=message.get("role", "user"),
                parts=cls._message_parts(message.get("content")),
            )
            for message in key_messages
        ]
        if base_resolution is not None:
            return replace(
                base_resolution,
                messages=request_messages,
                attachments=[*base_resolution.attachments, *attachments],
                response_spec=response_spec,
            )
        lineage_attachments = cls._attachments_from_lineage(prompt_lineage)
        rendered_sha256 = sha(key_messages)
        rendered_bytes = len(json.dumps(key_messages, ensure_ascii=False).encode("utf-8"))
        return PromptResolution(
            spec_name="unmanaged",
            structure_digest="unmanaged",
            base={"ref": "unmanaged", "sha256": rendered_sha256, "bytes": rendered_bytes},
            layers=(),
            axes=(),
            materials=(),
            rendered_sha256=rendered_sha256,
            rendered_bytes=rendered_bytes,
            messages=request_messages,
            attachments=[*lineage_attachments, *attachments],
            response_spec=response_spec,
        )

    @staticmethod
    def _message_parts(content: Any) -> list[str | dict[str, Any]]:
        if content is None:
            return []
        if isinstance(content, (str, dict)):
            return [content]
        if isinstance(content, list):
            parts: list[str | dict[str, Any]] = []
            for part in content:
                if isinstance(part, (str, dict)):
                    parts.append(part)
                else:
                    parts.append(str(part))
            return parts
        return [str(content)]

    @classmethod
    def _estimate_tokens(cls, messages: list[dict[str, Any]], params: dict[str, Any]) -> int:
        """Estimate a reservation from prompt size plus output allowance."""
        prompt_length = sum(cls._content_length(message.get("content", "")) for message in messages)
        # Roughly two tokens per four prompt characters; actual provider usage is
        # authoritative at commit, so this intentionally remains a best-effort guard.
        prompt_estimate = max(1, prompt_length // 4 * 2)
        max_tokens = params.get("max_tokens")
        if max_tokens is None:
            return prompt_estimate
        return prompt_estimate + Budget._coerce_tokens(max_tokens, name="max_tokens")

    @classmethod
    def _content_length(cls, content: Any) -> int:
        if isinstance(content, str):
            return len(content)
        if isinstance(content, list):
            return sum(cls._content_length(part) for part in content)
        if isinstance(content, dict):
            return sum(cls._content_length(value) for value in content.values())
        return len(str(content))

    @classmethod
    def _prepare_file_references(
        cls,
        messages: list[dict[str, Any]],
        *,
        preflight_policy: PreflightPolicy = _DEFAULT_PREFLIGHT_POLICY,
        snapshot_reader: Callable[[str | Path], bytes] | None = None,
    ) -> _PreparedMessages | None:
        values = [
            value
            for message in messages
            for value in cls._content_file_references(message.get("content"))
        ]
        if not values:
            return None
        probes = cls._preflight_file_references(
            values,
            preflight_policy,
            snapshot_reader=snapshot_reader,
        )
        references = {
            id(value): cls._file_reference(value, probe=probes[id(value)]) for value in values
        }
        key_messages: list[dict[str, Any]] = []
        cache_messages: list[dict[str, Any]] = []
        transport_messages: list[dict[str, Any]] = []
        attachments: list[Attachment] = []

        for message in messages:
            prepared_content = cls._prepare_content(
                message.get("content"),
                references=references,
            )
            if prepared_content is None:
                key_messages.append(message)
                cache_messages.append(message)
                transport_messages.append(message)
                continue

            key_content, cache_content, transport_content = prepared_content
            attachments.extend(
                cls._content_attachments(message.get("content"), references=references)
            )
            key_messages.append({**message, "content": key_content})
            cache_messages.append({**message, "content": cache_content})
            transport_messages.append({**message, "content": transport_content})

        return _PreparedMessages(key_messages, cache_messages, transport_messages, attachments)

    @classmethod
    def _content_file_references(cls, content: Any) -> list[dict[str, Any]]:
        if cls._is_file_reference(content):
            return [content]
        if isinstance(content, list):
            return [value for value in content if cls._is_file_reference(value)]
        return []

    @classmethod
    def _preflight_file_references(
        cls,
        values: list[dict[str, Any]],
        policy: PreflightPolicy,
        *,
        snapshot_reader: Callable[[str | Path], bytes] | None = None,
    ) -> dict[int, _FilePreflight]:
        if len(values) > policy.max_attachments:
            report = PreflightReport(
                violations=[
                    PreflightViolation(
                        check="attachment_count",
                        limit=policy.max_attachments,
                        actual=len(values),
                        message=(
                            f"Attachment count {len(values)} exceeds limit {policy.max_attachments}"
                        ),
                    )
                ],
                estimated_tokens=0,
                total_bytes=0,
            )
            raise RequestTooLarge(report)

        probes: dict[int, _FilePreflight] = {}
        total_bytes = 0
        for value in values:
            raw_path = value["kigumi_file"]
            if not isinstance(raw_path, str):
                raise ValueError("kigumi_file must be a path string")
            path = Path(raw_path)
            if snapshot_reader is None:
                size_bytes, identity = _regular_file_probe(path)
                probe = _FilePreflight(path, size_bytes, identity)
            else:
                snapshot_data = snapshot_reader(path)
                if not isinstance(snapshot_data, bytes):
                    raise ValueError(f"kigumi_file snapshot must contain bytes: {path}")
                probe = _FilePreflight(path, len(snapshot_data), None, snapshot_data)
            probes[id(value)] = probe
            total_bytes += probe.size_bytes

        if total_bytes > policy.max_attachment_bytes:
            report = PreflightReport(
                violations=[
                    PreflightViolation(
                        check="attachment_bytes",
                        limit=policy.max_attachment_bytes,
                        actual=total_bytes,
                        message=(
                            f"Attachment bytes {total_bytes} exceed limit "
                            f"{policy.max_attachment_bytes}"
                        ),
                    )
                ],
                estimated_tokens=0,
                total_bytes=total_bytes,
            )
            raise RequestTooLarge(report)
        return probes

    @classmethod
    def _content_attachments(
        cls,
        content: Any,
        *,
        references: dict[int, _FileReference] | None = None,
    ) -> list[Attachment]:
        attachments: list[Attachment] = []
        for value in cls._content_file_references(content):
            reference = (
                references[id(value)] if references is not None else cls._file_reference(value)
            )
            attachments.append(cls._attachment(reference))
        return attachments

    @classmethod
    def _prepare_content(
        cls,
        content: Any,
        *,
        references: dict[int, _FileReference] | None = None,
    ) -> tuple[Any, Any, Any] | None:
        if cls._is_file_reference(content):
            reference = (
                references[id(content)] if references is not None else cls._file_reference(content)
            )
            return (
                cls._key_reference(reference),
                cls._cached_reference(content, reference),
                reference,
            )
        if not isinstance(content, list):
            return None

        key_content: list[Any] = []
        cache_content: list[Any] = []
        transport_content: list[Any] = []
        found_reference = False
        for part in content:
            if not cls._is_file_reference(part):
                key_content.append(part)
                cache_content.append(part)
                transport_content.append(part)
                continue
            found_reference = True
            reference = (
                references[id(part)] if references is not None else cls._file_reference(part)
            )
            key_content.append(cls._key_reference(reference))
            cache_content.append(cls._cached_reference(part, reference))
            transport_content.append(reference)

        if not found_reference:
            return None
        return key_content, cache_content, transport_content

    @staticmethod
    def _is_file_reference(value: Any) -> bool:
        return isinstance(value, dict) and "kigumi_file" in value

    @staticmethod
    def _file_reference(
        value: dict[str, Any],
        *,
        probe: _FilePreflight | None = None,
    ) -> _FileReference:
        raw_path = value["kigumi_file"]
        if not isinstance(raw_path, str):
            raise ValueError("kigumi_file must be a path string")
        path = Path(raw_path)
        mime = value.get("format")
        if mime is None:
            mime = mimetypes.guess_type(path.name)[0]
        if not isinstance(mime, str) or not mime:
            raise ValueError(f"Cannot infer MIME type for kigumi_file {path}")
        if probe is None:
            size_bytes, identity = _regular_file_probe(path)
            probe = _FilePreflight(path, size_bytes, identity)
        digest = _hash_regular_file(probe)
        return _FileReference(
            path=path,
            mime=mime,
            digest=digest,
            size_bytes=probe.size_bytes,
            detail=value.get("detail"),
            has_detail="detail" in value,
            identity=probe.identity,
            snapshot_data=probe.snapshot_data,
        )

    @staticmethod
    def _attachment(reference: _FileReference) -> Attachment:
        return Attachment(
            path=str(reference.path),
            content_hash=reference.digest,
            mime_type=reference.mime,
            size_bytes=reference.size_bytes,
        )

    @staticmethod
    def _key_reference(reference: _FileReference) -> dict[str, Any]:
        key_reference: dict[str, Any] = {
            "kigumi_file_sha256": reference.digest,
            "format": reference.mime,
        }
        if reference.has_detail:
            key_reference["detail"] = reference.detail
        return key_reference

    @staticmethod
    def _cached_reference(original: dict[str, Any], reference: _FileReference) -> dict[str, Any]:
        return {**original, "kigumi_file_sha256": reference.digest}

    @classmethod
    def _expand_transport_messages(cls, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        expanded: list[dict[str, Any]] = []
        for message in messages:
            content = message.get("content")
            if isinstance(content, _FileReference):
                expanded.append({**message, "content": [cls._expand_file_reference(content)]})
            elif isinstance(content, list):
                expanded.append(
                    {
                        **message,
                        "content": [
                            cls._expand_file_reference(part)
                            if isinstance(part, _FileReference)
                            else part
                            for part in content
                        ],
                    }
                )
            else:
                expanded.append(message)
        return expanded

    @staticmethod
    def _expand_file_reference(reference: _FileReference) -> dict[str, Any]:
        data = _read_preflighted_file(reference)
        # 缓存键在算哈希那一刻就定了;文件在发出前被换了内容,键与实际载荷
        # 就会脱钩——宁可拒发也不能让内容寻址变成谎言。
        if sha256(data).hexdigest() != reference.digest:
            raise ValueError(f"kigumi_file changed after hashing: {reference.path}")
        data_url = _data_url(data, reference.mime)
        if reference.mime.startswith("image/"):
            image_url: dict[str, Any] = {"url": data_url}
            if reference.has_detail:
                image_url["detail"] = reference.detail
            return {"type": "image_url", "image_url": image_url}
        file_part: dict[str, Any] = {"file_data": data_url, "format": reference.mime}
        if reference.has_detail:
            file_part["detail"] = reference.detail
        return {"type": "file", "file": file_part}

    @staticmethod
    def _read_cached_response(cache_path: Path) -> dict[str, Any] | None:
        lookup = read_call_cache(cache_path)
        if lookup.state == "CORRUPT":
            raise CacheIntegrityError(cache_path, lookup)
        return lookup.data if lookup.state == "VALID" else None

    def _record_cache_hit(
        self,
        cached: dict[str, Any],
        *,
        key: str,
        model_alias: str,
        model: str,
        params: dict[str, Any],
        messages: list[dict[str, Any]],
        prompt_lineage: dict[str, Any] | None,
    ) -> str:
        cached_response = cached["response"]
        cached_metadata = cached.get("meta", {})
        if not isinstance(cached_metadata, dict):
            cached_metadata = {}
        cached_usage = cached_metadata.get("usage", {})
        if not isinstance(cached_usage, dict):
            cached_usage = {}
        provider_response_id = cached_metadata.get("provider_response_id")
        if not isinstance(provider_response_id, str):
            provider_response_id = None
        provider_model = cached_metadata.get("provider_model")
        if not isinstance(provider_model, str):
            provider_model = None
        provider_model_observed = cached_metadata.get("provider_model_observed") is True
        self._append_call(
            self._meta(
                key=key,
                model_alias=model_alias,
                model=model,
                params=params,
                messages=messages,
                seconds=0.0,
                usage=cached_usage,
                cache="hit",
                provider_response_id=provider_response_id,
                provider_model=provider_model,
                provider_model_observed=provider_model_observed,
                request_value=cached.get("messages", messages),
                response_value={
                    "text": cached_response,
                    "reasoning": cached.get("reasoning"),
                },
                prompt_lineage=prompt_lineage,
            )
        )
        return cached_response

    @contextmanager
    def _lock_for_key(self, key: str) -> Iterator[_SingleFlightLock]:
        with self._key_locks_lock:
            lock = self._key_locks.get(key)
            if lock is None:
                lock = _SingleFlightLock()
                self._key_locks[key] = lock
            lock.waiters += 1
        try:
            with lock.lock:
                try:
                    yield lock
                except BaseException as error:
                    if not lock.done:
                        lock.error = error
                        lock.done = True
                    raise
                else:
                    lock.done = True
        finally:
            with self._key_locks_lock:
                lock.waiters -= 1
                if lock.done and lock.waiters == 0 and self._key_locks.get(key) is lock:
                    del self._key_locks[key]

    @contextmanager
    def _file_lock_for_key(self, key: str) -> Iterator[None]:
        if self.slots is None:
            yield
            return
        acquire_key = getattr(self.slots, "acquire_key", None)
        if not callable(acquire_key):
            yield
            return
        if self.key_lock_timeout_seconds is None:
            lock_context = acquire_key(key)
        else:
            lock_context = acquire_key(key, timeout_seconds=self.key_lock_timeout_seconds)
        with lock_context:
            yield

    def _append_call(self, metadata: dict[str, Any]) -> None:
        with self._calls_lock:
            self.calls.append(metadata)
            observer = _call_observer.get()
            if observer is not None:
                observer.append(metadata)

    def _meta(
        self,
        *,
        key: str,
        model_alias: str,
        model: str,
        params: dict[str, Any],
        messages: list[dict[str, Any]],
        seconds: float,
        usage: dict[str, Any],
        cache: str,
        provider_response_id: str | None = None,
        provider_model: str | None = None,
        provider_model_observed: bool = False,
        failure: dict[str, Any] | None = None,
        request_value: Any | None = None,
        response_value: Any | None = None,
        prompt_lineage: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        metadata = {
            "key": key,
            "model_alias": model_alias,
            "model": model,
            "params": params,
            "seed": self.seed,
            "prompt_sha": sha(messages),
            "seconds": seconds,
            "usage": usage,
            "cache": cache,
            "provider_response_id": provider_response_id,
            "provider_model": provider_model,
            "provider_model_observed": provider_model_observed,
            "evidence_policy_digest": self.evidence_policy.digest,
            "evidence_policy": self.evidence_policy.canonical(),
            "request_evidence": scrub_evidence(
                messages if request_value is None else request_value,
                mode=self.evidence_policy.request,
            ),
            "response_evidence": (
                scrub_evidence(response_value, mode=self.evidence_policy.response)
                if response_value is not None
                else None
            ),
        }
        if failure is not None:
            metadata["failure"] = copy.deepcopy(failure)
        if prompt_lineage is not None:
            metadata["prompt_resolution"] = copy.deepcopy(prompt_lineage)
        return metadata

    def _validate_durable_transport(self) -> None:
        limits = {
            "max_retries": getattr(self.transport, "max_retries", 0),
            "max_length_retries": getattr(self.transport, "max_length_retries", 0),
            "max_empty_retries": getattr(self.transport, "max_empty_retries", 0),
        }
        enabled = {name: value for name, value in limits.items() if value != 0}
        if enabled:
            details = ", ".join(f"{name}={value}" for name, value in sorted(enabled.items()))
            raise RuntimeError(
                "Durable retry requires transport, length, and empty retries to be 0 "
                f"before the provider call ({details})"
            )
