"""LLM request preparation and single-attempt transport implementations."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from .artifacts import sha
from .failures import (
    ProviderFailureKind,
    ProviderFailureStage,
    provider_failure_from_exception,
)
from .slots import AdaptiveCapacity


def _freeze_prepared_value(value: Any) -> Any:
    """Freeze JSON-shaped request state while preserving explicit lazy parts."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_prepared_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_prepared_value(item) for item in value)
    return value


def _canonical_prepared_value(value: Any) -> Any:
    """Project request state to stable content identity without wire-only bytes."""
    projection = getattr(value, "_prepared_canonical", None)
    if callable(projection):
        return _canonical_prepared_value(projection())
    if isinstance(value, Mapping):
        return {key: _canonical_prepared_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_prepared_value(item) for item in value]
    return value


def _wire_prepared_value(value: Any) -> Any:
    """Expand lazy request parts into the concrete value sent to a provider."""
    projection = getattr(value, "_prepared_wire", None)
    if callable(projection):
        return _wire_prepared_value(projection())
    if isinstance(value, Mapping):
        return {key: _wire_prepared_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_wire_prepared_value(item) for item in value]
    return value


@dataclass(frozen=True)
class PreparedRequest:
    """Immutable effective request shared by identity, admission, and sending."""

    messages: tuple[Mapping[str, Any], ...]
    model: str
    params: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.messages, (list, tuple)) or not all(
            isinstance(message, Mapping) for message in self.messages
        ):
            raise TypeError("PreparedRequest messages must be a list or tuple of mappings")
        if not isinstance(self.model, str) or not self.model:
            raise ValueError("PreparedRequest model must be a non-empty string")
        if not isinstance(self.params, Mapping):
            raise TypeError("PreparedRequest params must be a mapping")
        object.__setattr__(
            self,
            "messages",
            tuple(_freeze_prepared_value(message) for message in self.messages),
        )
        object.__setattr__(self, "params", _freeze_prepared_value(self.params))

    def canonical(self) -> dict[str, Any]:
        """Return stable effective-request identity without wire-only expansion."""
        return {
            "messages": _canonical_prepared_value(self.messages),
            "model": self.model,
            "params": _canonical_prepared_value(self.params),
        }

    def wire(self) -> dict[str, Any]:
        """Return a fresh concrete provider payload, expanding lazy parts."""
        return {
            "messages": _wire_prepared_value(self.messages),
            "model": self.model,
            "params": _wire_prepared_value(self.params),
        }


@dataclass
class Response:
    """Normalized result returned by every transport."""

    text: str
    usage: dict[str, Any]
    finish_reason: str | None
    reasoning: str | None = None
    model: str = ""
    provider_response_id: str | None = None
    model_observed: bool = False


class EmptyResponseError(RuntimeError):
    """Raised when a provider returns an empty response."""


class TruncatedResponseError(RuntimeError):
    """Raised when a length-limited response cannot be safely completed."""


class Transport(Protocol):
    """The minimal interface used by :class:`kigumi.calling.LLMCaller`."""

    def cache_identity(self) -> Any:
        """Return stable, credential-free adapter identity for L1 caching."""

    def prepare(
        self,
        messages: list[dict[str, Any]],
        model: str,
        params: dict[str, Any],
    ) -> PreparedRequest:
        """Resolve aliases and normalize one effective request without provider I/O."""

    def send(self, prepared: PreparedRequest) -> Response:
        """Send one prepared request exactly once."""


def _value(source: Any, name: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def _mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        return dict(dumped) if isinstance(dumped, dict) else {}
    return {}


def _is_transient_error(error: BaseException) -> bool:
    failure = provider_failure_from_exception(
        error,
        provider="transport",
        stage=ProviderFailureStage.TRANSPORT,
    )
    return failure.kind in {
        ProviderFailureKind.RATE_LIMIT,
        ProviderFailureKind.SERVER_ERROR,
        ProviderFailureKind.TIMEOUT,
        ProviderFailureKind.CONNECTION,
    }


class _BaseTransport:
    """Shared deterministic request preparation for concrete adapters."""

    def __init__(
        self,
        aliases: dict[str, str] | None = None,
        *,
        capacity: AdaptiveCapacity | None = None,
    ) -> None:
        self.aliases = self._aliases_from_environment() if aliases is None else dict(aliases)
        self.capacity = capacity

    @staticmethod
    def _aliases_from_environment() -> dict[str, str]:
        return {
            alias: configured
            for alias, configured in {
                "default": os.getenv("KIGUMI_MODEL_DEFAULT"),
                "pro": os.getenv("KIGUMI_MODEL_PRO"),
            }.items()
            if configured
        }

    def _resolve_model(self, model: str) -> str:
        """Resolve a caller-facing alias to the concrete provider model name."""
        if model in self.aliases:
            resolved = self.aliases[model]
            if resolved:
                return resolved
            raise ValueError(f"Model alias {model!r} does not resolve to a concrete model name")
        if model in {"default", "pro"}:
            raise ValueError(
                f"Model alias {model!r} is not configured; set its KIGUMI_MODEL_* variable "
                "or pass aliases."
            )
        if not model:
            raise ValueError("A concrete model name is required")
        return model

    def prepare(
        self,
        messages: list[dict[str, Any]],
        model: str,
        params: dict[str, Any],
    ) -> PreparedRequest:
        """Return the resolved, normalized request used by every later boundary."""
        normalized_messages, normalized_params = self._normalize_request(messages, params)
        return PreparedRequest(
            normalized_messages,
            self._resolve_model(model),
            normalized_params,
        )

    def send(self, prepared: PreparedRequest) -> Response:
        """Execute one provider attempt and reject incomplete responses."""
        if not isinstance(prepared, PreparedRequest):
            raise TypeError("send requires a PreparedRequest")
        wire = prepared.wire()
        try:
            response = self._send_once(
                wire["messages"],
                wire["model"],
                wire["params"],
            )
        except Exception as error:
            failure = provider_failure_from_exception(
                error,
                provider=self._provider_name(),
                stage=ProviderFailureStage.TRANSPORT,
            )
            if _is_transient_error(failure) and self.capacity is not None:
                self.capacity.on_throttle()
            raise failure from None

        if response.finish_reason == "length":
            raise TruncatedResponseError(f"Model {prepared.model!r} returned a truncated response.")
        if not response.text:
            raise EmptyResponseError(f"Model {prepared.model!r} returned an empty response.")
        if self.capacity is not None:
            self.capacity.on_success()
        return response

    @staticmethod
    def _normalize_request(
        messages: list[dict[str, Any]],
        params: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        normalized_messages = list(messages)
        normalized_params = dict(params)
        if normalized_params.pop("json_mode", False):
            if "response_format" in normalized_params:
                raise ValueError("json_mode conflicts with explicit response_format")
            normalized_params["response_format"] = {"type": "json_object"}
        system = normalized_params.pop("system", None)
        if system is not None:
            if normalized_messages and normalized_messages[0].get("role") == "system":
                raise ValueError("system parameter conflicts with an existing system message")
            normalized_messages.insert(0, {"role": "system", "content": system})
        return normalized_messages, normalized_params

    def _send_once(
        self,
        messages: list[dict[str, Any]],
        model: str,
        params: dict[str, Any],
    ) -> Response:
        raise NotImplementedError

    def _provider_name(self) -> str:
        """Return the stable provider adapter label used in typed failures."""
        return type(self).__name__


class LiteLLMTransport(_BaseTransport):
    """Transport backed by LiteLLM, imported only when a call is actually made."""

    def cache_identity(self) -> dict[str, Any]:
        return {"transport": "litellm", "schema": 1}

    def _send_once(
        self,
        messages: list[dict[str, Any]],
        model: str,
        params: dict[str, Any],
    ) -> Response:
        try:
            import litellm
        except ImportError as error:
            message = "LiteLLMTransport requires the optional 'litellm' package"
            raise RuntimeError(message) from error

        raw_response = litellm.completion(model=model, messages=messages, **params)
        return _response_from_provider(raw_response, model)

    def _provider_name(self) -> str:
        return "litellm"


class StdlibTransport(_BaseTransport):
    """OpenAI-compatible transport implemented with the Python standard library."""

    def __init__(
        self,
        api_base: str,
        api_key: str,
        aliases: dict[str, str] | None = None,
        timeout: float = 300.0,
        *,
        capacity: AdaptiveCapacity | None = None,
    ) -> None:
        super().__init__(
            aliases=aliases,
            capacity=capacity,
        )
        parsed_api_base = urlsplit(api_base)
        if not parsed_api_base.scheme:
            raise ValueError("api_base must include an http or https scheme")
        if parsed_api_base.scheme not in {"http", "https"}:
            if not parsed_api_base.netloc:
                raise ValueError(
                    f"api_base scheme {parsed_api_base.scheme!r} is not supported; "
                    "the address may be missing an http:// or https:// prefix"
                )
            raise ValueError(
                f"api_base scheme {parsed_api_base.scheme!r} is not supported; "
                "expected http or https"
            )
        if not parsed_api_base.netloc:
            raise ValueError("api_base must include a host")
        self.api_base = api_base.rstrip("/")
        self.api_key = api_key
        self._timeout = timeout

    def cache_identity(self) -> dict[str, Any]:
        return {
            "transport": "openai-compatible",
            "schema": 1,
            "api_base_sha256": sha(self.api_base),
        }

    def _send_once(
        self,
        messages: list[dict[str, Any]],
        model: str,
        params: dict[str, Any],
    ) -> Response:
        payload = {"model": model, "messages": messages, **params}
        suffix = "/chat/completions" if self.api_base.endswith("/v1") else "/v1/chat/completions"
        request = Request(
            f"{self.api_base}{suffix}",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        # Construction validates api_base as HTTP(S), so the caller cannot select local handlers.
        with urlopen(request, timeout=self._timeout) as http_response:  # nosec B310
            raw_response = json.loads(http_response.read().decode("utf-8"))
        return _response_from_provider(raw_response, model)

    def _provider_name(self) -> str:
        return "openai-compatible"


def _response_from_provider(raw_response: Any, requested_model: str) -> Response:
    choices = _value(raw_response, "choices", []) or []
    choice = choices[0] if choices else {}
    message = _value(choice, "message", {}) or {}
    content = _value(message, "content", "")
    text = content if isinstance(content, str) else ""
    reasoning = _value(message, "reasoning_content") or _value(message, "reasoning")
    if reasoning is None:
        reasoning = _value(raw_response, "reasoning_content") or _value(raw_response, "reasoning")
    provider_response_id = _value(raw_response, "id")
    provider_model = _value(raw_response, "model")
    model_observed = isinstance(provider_model, str) and bool(provider_model.strip())
    return Response(
        text=text,
        usage=_mapping(_value(raw_response, "usage")),
        finish_reason=_value(choice, "finish_reason"),
        reasoning=reasoning if isinstance(reasoning, str) else None,
        model=provider_model if model_observed else requested_model,
        provider_response_id=(
            provider_response_id
            if isinstance(provider_response_id, str) and provider_response_id.strip()
            else None
        ),
        model_observed=model_observed,
    )
