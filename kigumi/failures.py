"""Canonical provider and external-Agent failure facts.

Classification in this module is intentionally limited to structured wire,
status, and typed SDK fields. Provider prose is hashed for correlation but is
never parsed to make retry or policy decisions.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from enum import StrEnum
from typing import Any
from urllib.error import HTTPError, URLError

from .artifacts import canonical_json


class ProviderFailureStage(StrEnum):
    """The provider boundary at which a failure became observable."""

    REQUEST = "request"
    TRANSPORT = "transport"
    PROVIDER = "provider"
    RESPONSE = "response"


class ProviderFailureKind(StrEnum):
    """Closed provider-neutral failure kinds derived from structured facts."""

    RATE_LIMIT = "rate_limit"
    SERVER_ERROR = "server_error"
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    INVALID_REQUEST = "invalid_request"
    MODEL_MISMATCH = "model_mismatch"
    UNKNOWN = "unknown"


class ProviderFailure(RuntimeError):
    """Typed provider failure with only canonical, redacted control metadata."""

    def __init__(
        self,
        *,
        provider: str,
        stage: ProviderFailureStage,
        kind: ProviderFailureKind,
        status_code: int | None,
        retry_after_ms: int | None,
        provider_request_id: str | None,
        message_digest: str,
        retryable_hint: bool | None,
    ) -> None:
        if not isinstance(provider, str) or not provider:
            raise ValueError("provider must be a non-empty string")
        if not isinstance(stage, ProviderFailureStage):
            raise TypeError("stage must be ProviderFailureStage")
        if not isinstance(kind, ProviderFailureKind):
            raise TypeError("kind must be ProviderFailureKind")
        if status_code is not None and (
            isinstance(status_code, bool)
            or not isinstance(status_code, int)
            or not 100 <= status_code <= 599
        ):
            raise ValueError("status_code must be an HTTP status code or null")
        if retry_after_ms is not None and (
            isinstance(retry_after_ms, bool)
            or not isinstance(retry_after_ms, int)
            or retry_after_ms < 0
        ):
            raise ValueError("retry_after_ms must be a non-negative integer or null")
        if provider_request_id is not None and (
            not isinstance(provider_request_id, str) or not provider_request_id
        ):
            raise ValueError("provider_request_id must be a non-empty string or null")
        if (
            not isinstance(message_digest, str)
            or len(message_digest) != 64
            or any(character not in "0123456789abcdef" for character in message_digest)
        ):
            raise ValueError("message_digest must be a lowercase SHA-256 digest")
        if retryable_hint is not None and not isinstance(retryable_hint, bool):
            raise TypeError("retryable_hint must be bool or null")
        self.provider = provider
        self.stage = stage
        self.kind = kind
        self.status_code = status_code
        self.retry_after_ms = retry_after_ms
        self.provider_request_id = provider_request_id
        self.message_digest = message_digest
        self.retryable_hint = retryable_hint
        super().__init__(f"{provider} {stage.value} failure: {kind.value}")

    def canonical(self) -> dict[str, Any]:
        """Return the stable JSON representation persisted in receipts."""
        return {
            "provider": self.provider,
            "stage": self.stage.value,
            "kind": self.kind.value,
            "status_code": self.status_code,
            "retry_after_ms": self.retry_after_ms,
            "provider_request_id": self.provider_request_id,
            "message_digest": self.message_digest,
            "retryable_hint": self.retryable_hint,
        }

    @classmethod
    def from_canonical(cls, value: Mapping[str, Any]) -> ProviderFailure:
        """Restore a validated provider failure from persisted canonical JSON."""
        return cls(
            provider=str(value["provider"]),
            stage=ProviderFailureStage(value["stage"]),
            kind=ProviderFailureKind(value["kind"]),
            status_code=value.get("status_code"),
            retry_after_ms=value.get("retry_after_ms"),
            provider_request_id=value.get("provider_request_id"),
            message_digest=str(value["message_digest"]),
            retryable_hint=value.get("retryable_hint"),
        )


class AgentRuntimeFailureCode(StrEnum):
    """External-Agent runtime failures that are not provider outcomes."""

    SPAWN_NOT_FOUND = "spawn_not_found"
    SPAWN_PERMISSION = "spawn_permission"
    SPAWN_FAILURE = "spawn_failure"
    VERSION_MISMATCH = "version_mismatch"
    PROCESS_EXIT = "process_exit"
    PROTOCOL = "protocol"
    POLICY = "policy"
    CAPACITY = "capacity"


_RUNTIME_MESSAGES = {
    AgentRuntimeFailureCode.SPAWN_NOT_FOUND: "Agent executable was not found",
    AgentRuntimeFailureCode.SPAWN_PERMISSION: "Agent executable could not be started",
    AgentRuntimeFailureCode.SPAWN_FAILURE: "Agent process could not be started",
    AgentRuntimeFailureCode.VERSION_MISMATCH: "Agent runtime version was not admitted",
    AgentRuntimeFailureCode.PROCESS_EXIT: "Agent process exited before completion",
    AgentRuntimeFailureCode.PROTOCOL: "Agent runtime protocol was violated",
    AgentRuntimeFailureCode.POLICY: "Agent runtime policy was violated",
    AgentRuntimeFailureCode.CAPACITY: "Agent execution capacity was not acquired",
}


class AgentExecutionFailure(RuntimeError):
    """A provider failure or one closed Agent runtime failure, never both."""

    def __init__(
        self,
        *,
        provider_failure: ProviderFailure | None = None,
        runtime_code: AgentRuntimeFailureCode | None = None,
    ) -> None:
        if (provider_failure is None) == (runtime_code is None):
            raise ValueError("exactly one of provider_failure or runtime_code is required")
        if provider_failure is not None and not isinstance(provider_failure, ProviderFailure):
            raise TypeError("provider_failure must be ProviderFailure")
        if runtime_code is not None and not isinstance(runtime_code, AgentRuntimeFailureCode):
            raise TypeError("runtime_code must be AgentRuntimeFailureCode")
        self.provider_failure = provider_failure
        self.runtime_code = runtime_code
        message = (
            str(provider_failure)
            if provider_failure is not None
            else _RUNTIME_MESSAGES[runtime_code]  # type: ignore[index]
        )
        super().__init__(message)

    def canonical(self) -> dict[str, Any]:
        """Return the stable failure payload used by Agent files and receipts."""
        return {
            "failure_type": "provider" if self.provider_failure is not None else "runtime",
            "provider_failure": (
                self.provider_failure.canonical() if self.provider_failure is not None else None
            ),
            "runtime_code": self.runtime_code.value if self.runtime_code is not None else None,
        }

    @classmethod
    def from_canonical(cls, value: Mapping[str, Any]) -> AgentExecutionFailure:
        """Restore a validated Agent failure from a receipt."""
        provider = value.get("provider_failure")
        runtime = value.get("runtime_code")
        return cls(
            provider_failure=(
                ProviderFailure.from_canonical(provider) if isinstance(provider, Mapping) else None
            ),
            runtime_code=AgentRuntimeFailureCode(runtime) if isinstance(runtime, str) else None,
        )


_TYPED_KIND_CODES = {
    "rate_limit": ProviderFailureKind.RATE_LIMIT,
    "rate_limited": ProviderFailureKind.RATE_LIMIT,
    "too_many_requests": ProviderFailureKind.RATE_LIMIT,
    "server_error": ProviderFailureKind.SERVER_ERROR,
    "internal_server_error": ProviderFailureKind.SERVER_ERROR,
    "timeout": ProviderFailureKind.TIMEOUT,
    "connection": ProviderFailureKind.CONNECTION,
    "connection_error": ProviderFailureKind.CONNECTION,
    "authentication": ProviderFailureKind.AUTHENTICATION,
    "authentication_error": ProviderFailureKind.AUTHENTICATION,
    "unauthorized": ProviderFailureKind.AUTHENTICATION,
    "authorization": ProviderFailureKind.AUTHORIZATION,
    "permission_denied": ProviderFailureKind.AUTHORIZATION,
    "forbidden": ProviderFailureKind.AUTHORIZATION,
    "invalid_request": ProviderFailureKind.INVALID_REQUEST,
    "invalid_request_error": ProviderFailureKind.INVALID_REQUEST,
    "bad_request": ProviderFailureKind.INVALID_REQUEST,
    "model_mismatch": ProviderFailureKind.MODEL_MISMATCH,
    "model_substitution": ProviderFailureKind.MODEL_MISMATCH,
}


def provider_failure_from_exception(
    error: BaseException,
    *,
    provider: str,
    stage: ProviderFailureStage,
) -> ProviderFailure:
    """Classify an exception without interpreting provider-authored prose."""
    if isinstance(error, ProviderFailure):
        return error
    status_code = _status_code(error)
    structured_code = _structured_code(error)
    kind = _kind_from_facts(error, status_code=status_code, structured_code=structured_code)
    headers = _headers(error)
    retry_after_ms = _retry_after_ms(error, headers)
    request_id = _request_id(error, headers)
    retryable_hint = _bool_attribute(error, "retryable", "should_retry")
    message = str(error)
    return ProviderFailure(
        provider=provider,
        stage=stage,
        kind=kind,
        status_code=status_code,
        retry_after_ms=retry_after_ms,
        provider_request_id=request_id,
        message_digest=hashlib.sha256(message.encode("utf-8", errors="replace")).hexdigest(),
        retryable_hint=retryable_hint,
    )


def canonical_failure(error: BaseException) -> dict[str, Any]:
    """Return canonical typed failure metadata for receipts and sidecars."""
    if isinstance(error, AgentExecutionFailure):
        return error.canonical()
    if isinstance(error, ProviderFailure):
        return {
            "failure_type": "provider",
            "provider_failure": error.canonical(),
            "runtime_code": None,
        }
    return {
        "failure_type": "runtime",
        "exception_type": type(error).__name__,
        "message_digest": hashlib.sha256(str(error).encode("utf-8", errors="replace")).hexdigest(),
    }


def failure_provider_kind(error: BaseException) -> ProviderFailureKind | None:
    """Return the retry-relevant provider kind, if the error has one."""
    if isinstance(error, ProviderFailure):
        return error.kind
    if isinstance(error, AgentExecutionFailure) and error.provider_failure is not None:
        return error.provider_failure.kind
    return None


def failure_retry_after_ms(error: BaseException) -> int | None:
    """Return a structured provider retry-after hint, if present."""
    if isinstance(error, ProviderFailure):
        return error.retry_after_ms
    if isinstance(error, AgentExecutionFailure) and error.provider_failure is not None:
        return error.provider_failure.retry_after_ms
    return None


def _kind_from_facts(
    error: BaseException,
    *,
    status_code: int | None,
    structured_code: str | None,
) -> ProviderFailureKind:
    if status_code == 429:
        return ProviderFailureKind.RATE_LIMIT
    if status_code is not None and 500 <= status_code <= 599:
        return ProviderFailureKind.SERVER_ERROR
    if status_code == 401:
        return ProviderFailureKind.AUTHENTICATION
    if status_code == 403:
        return ProviderFailureKind.AUTHORIZATION
    if status_code is not None and 400 <= status_code <= 499:
        return ProviderFailureKind.INVALID_REQUEST
    if isinstance(error, TimeoutError):
        return ProviderFailureKind.TIMEOUT
    if isinstance(error, (ConnectionError, URLError)):
        return ProviderFailureKind.CONNECTION
    if structured_code is not None:
        return _TYPED_KIND_CODES.get(structured_code.lower(), ProviderFailureKind.UNKNOWN)
    return ProviderFailureKind.UNKNOWN


def _status_code(error: BaseException) -> int | None:
    for name in ("status_code", "status", "http_status"):
        value = getattr(error, name, None)
        if isinstance(value, int) and not isinstance(value, bool) and 100 <= value <= 599:
            return value
    if isinstance(error, HTTPError):
        return error.code
    response = getattr(error, "response", None)
    value = getattr(response, "status_code", None)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _structured_code(error: BaseException) -> str | None:
    for source in (error, getattr(error, "error", None), getattr(error, "body", None)):
        if isinstance(source, Mapping):
            for name in ("code", "type", "kind"):
                value = source.get(name)
                if isinstance(value, str) and value:
                    return value
        elif source is not None:
            for name in ("code", "type", "kind"):
                value = getattr(source, name, None)
                if isinstance(value, str) and value:
                    return value
    return None


def _headers(error: BaseException) -> Mapping[str, Any]:
    for source in (
        getattr(error, "headers", None),
        getattr(getattr(error, "response", None), "headers", None),
    ):
        if isinstance(source, Mapping):
            return source
    return {}


def _header(headers: Mapping[str, Any], *names: str) -> str | None:
    lowered = {str(key).lower(): value for key, value in headers.items()}
    for name in names:
        value = lowered.get(name.lower())
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _retry_after_ms(error: BaseException, headers: Mapping[str, Any]) -> int | None:
    for name in ("retry_after_ms", "retryAfterMs"):
        value = getattr(error, name, None)
        if isinstance(value, int | float) and not isinstance(value, bool) and value >= 0:
            return int(value)
    value = _header(headers, "retry-after-ms")
    if value is not None:
        try:
            return max(0, int(float(value)))
        except ValueError:
            pass
    value = _header(headers, "retry-after")
    if value is not None:
        try:
            return max(0, int(float(value) * 1000))
        except ValueError:
            pass
    return None


def _request_id(error: BaseException, headers: Mapping[str, Any]) -> str | None:
    for name in ("request_id", "provider_request_id"):
        value = getattr(error, name, None)
        if isinstance(value, str) and value:
            return value
    return _header(headers, "x-request-id", "request-id", "x-amzn-requestid")


def _bool_attribute(error: BaseException, *names: str) -> bool | None:
    for name in names:
        value = getattr(error, name, None)
        if isinstance(value, bool):
            return value
    return None


def validate_canonical_failure(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate that a persisted failure is canonical JSON."""
    return __import__("json").loads(canonical_json(dict(value)))
