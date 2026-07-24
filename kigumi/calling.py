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
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Protocol

from .artifacts import atomic_write_json, sha, sha256_file
from .evidence import EvidencePolicy, scrub_evidence
from .failures import (
    ProviderFailure,
    ProviderFailureStage,
    canonical_failure,
    provider_failure_from_exception,
)
from .prompt import PromptResolution, ResolvedPrompt
from .slots import FileSlots
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
_DEFAULT_EVIDENCE_POLICY = EvidencePolicy()


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


def _data_url(data: bytes, mime: str) -> str:
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:{mime};base64,{encoded}"


class BudgetExceeded(RuntimeError):
    """Raised after a call pushes its token budget past the configured ceiling."""


class DryRunError(RuntimeError):
    """Raised when dry-run mode would otherwise make a live model request."""


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
    detail: Any
    has_detail: bool


@dataclass(frozen=True)
class _PreparedMessages:
    """The three representations required for file-reference calling."""

    key_messages: list[dict[str, Any]]
    cache_messages: list[dict[str, Any]]
    transport_messages: list[dict[str, Any]]


class Budget:
    """Accumulate usage and fail on the first record after the budget is exceeded.

    检查发生在调用完成之后:并发 worker 各自在途的那一批调用仍会花完,
    超支上限约为 (workers - 1) 倍单次调用用量,这是事后计费的固有限制。
    """

    def __init__(self, max_tokens: int | None) -> None:
        self.max_tokens = max_tokens
        self._spent = 0
        self._lock = threading.Lock()

    @property
    def spent(self) -> int:
        """The cumulative number of reported total tokens."""
        with self._lock:
            return self._spent

    def record(self, usage: dict[str, Any]) -> None:
        """Record a response's total-token usage and enforce the configured cap."""
        with self._lock:
            total = usage.get("total_tokens", 0)
            self._spent += int(total) if total is not None else 0
            if self.max_tokens is not None and self._spent > self.max_tokens:
                raise BudgetExceeded(
                    f"Token budget exceeded: spent {self._spent} > {self.max_tokens}"
                )


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
    ) -> None:
        self.transport = transport
        self.cache_dir = Path(cache_dir)
        self.seed = seed
        self.budget = budget
        self.dry = dry
        self.slots = slots
        if not isinstance(evidence_policy, EvidencePolicy):
            raise TypeError("evidence_policy must be EvidencePolicy")
        self.evidence_policy = evidence_policy
        self.calls: list[dict[str, Any]] = []
        self._calls_lock = threading.Lock()
        # 键锁只增不减:caller 与一次 run 同生命周期,键集有界;若改成常驻服务需先加回收。
        self._key_locks: dict[str, threading.Lock] = {}
        self._key_locks_lock = threading.Lock()

    def call(
        self,
        messages: list[dict[str, Any]] | str,
        model: str = "default",
        **params: Any,
    ) -> str:
        """Return a cached or live completion for normalized chat messages."""
        prompt_lineage = _prompt_lineage.get()
        if prompt_lineage is None and isinstance(messages, ResolvedPrompt):
            prompt_lineage = {
                **messages.resolution.canonical(),
                "base_resolution_digest": messages.resolution.digest,
                "phase": "primary",
                "repair_round": 0,
            }
        normalized_messages = self._normalize_messages(messages)
        prepared = self._prepare_file_references(normalized_messages)
        key_messages = prepared.key_messages if prepared is not None else normalized_messages
        cache_messages = prepared.cache_messages if prepared is not None else normalized_messages
        resolved_model = self.transport.resolve(model)
        # Cache keys preserve caller intent before transport parameter normalization.
        key = sha(
            {
                "messages": key_messages,
                "model": resolved_model,
                "params": params,
                "seed": self.seed,
            }
        )
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

        with self._lock_for_key(key):
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

            if self.dry:
                raise DryRunError(f"Dry run would call model {model!r} for cache key {key}")

            started = time.monotonic()
            transport_messages = (
                self._expand_transport_messages(prepared.transport_messages)
                if prepared is not None
                else normalized_messages
            )
            # 槽位限的是远程请求本身;base64 展开是本地工作,不许占着槽做。
            slot_context = self.slots.acquire() if self.slots is not None else nullcontext()
            try:
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
                                "managed": prompt_lineage is not None,
                                "prompt_resolution": copy.deepcopy(prompt_lineage),
                            }
                        )
                    response = self.transport.complete(transport_messages, model, **params)
            except Exception as error:
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
            seconds = time.monotonic() - started
            if not response.text:
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
                "messages": cache_messages,
                "reasoning": response.reasoning,
            }
            atomic_write_json(cache_path, payload)
            self._append_call(payload["meta"])
            if self.budget is not None:
                self.budget.record(response.usage)
            return response.text

    @staticmethod
    def _normalize_messages(messages: list[dict[str, Any]] | str) -> list[dict[str, Any]]:
        if isinstance(messages, str):
            # Strip a ResolvedPrompt subclass only after its lineage was captured.
            return [{"role": "user", "content": str(messages)}]
        return messages

    @classmethod
    def _prepare_file_references(cls, messages: list[dict[str, Any]]) -> _PreparedMessages | None:
        key_messages: list[dict[str, Any]] = []
        cache_messages: list[dict[str, Any]] = []
        transport_messages: list[dict[str, Any]] = []
        found_reference = False

        for message in messages:
            prepared_content = cls._prepare_content(message.get("content"))
            if prepared_content is None:
                key_messages.append(message)
                cache_messages.append(message)
                transport_messages.append(message)
                continue

            found_reference = True
            key_content, cache_content, transport_content = prepared_content
            key_messages.append({**message, "content": key_content})
            cache_messages.append({**message, "content": cache_content})
            transport_messages.append({**message, "content": transport_content})

        if not found_reference:
            return None
        return _PreparedMessages(key_messages, cache_messages, transport_messages)

    @classmethod
    def _prepare_content(cls, content: Any) -> tuple[Any, Any, Any] | None:
        if cls._is_file_reference(content):
            reference = cls._file_reference(content)
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
            reference = cls._file_reference(part)
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
    def _file_reference(value: dict[str, Any]) -> _FileReference:
        raw_path = value["kigumi_file"]
        if not isinstance(raw_path, str):
            raise ValueError("kigumi_file must be a path string")
        path = Path(raw_path)
        digest = sha256_file(path)
        mime = value.get("format")
        if mime is None:
            mime = mimetypes.guess_type(path.name)[0]
        if not isinstance(mime, str) or not mime:
            raise ValueError(f"Cannot infer MIME type for kigumi_file {path}")
        return _FileReference(
            path=path,
            mime=mime,
            digest=digest,
            detail=value.get("detail"),
            has_detail="detail" in value,
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
        data = reference.path.read_bytes()
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
        try:
            with cache_path.open(encoding="utf-8") as handle:
                cached = json.load(handle)
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return None
        response = cached.get("response") if isinstance(cached, dict) else None
        if not isinstance(response, str) or not response:
            # 空响应永远不会被合法写入;读到即按撕裂缓存处理,走 miss。
            return None
        return cached

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

    def _lock_for_key(self, key: str) -> threading.Lock:
        with self._key_locks_lock:
            return self._key_locks.setdefault(key, threading.Lock())

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
