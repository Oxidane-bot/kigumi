"""LLM transport implementations with bounded recovery for transient failures."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .slots import AdaptiveCapacity


@dataclass
class Response:
    """Normalized result returned by every transport."""

    text: str
    usage: dict[str, Any]
    finish_reason: str | None
    reasoning: str | None = None
    model: str = ""
    provider_response_id: str | None = None


class EmptyResponseError(RuntimeError):
    """Raised when bounded empty-response retries are exhausted."""


class TruncatedResponseError(RuntimeError):
    """Raised when a length-limited response cannot be safely completed."""


class Transport(Protocol):
    """The minimal interface used by :class:`kigumi.calling.LLMCaller`."""

    def resolve(self, model: str) -> str:
        """Resolve a caller-facing model alias to a concrete model name."""

    def complete(self, messages: list[dict[str, Any]], model: str, **params: Any) -> Response:
        """Complete chat messages with the requested model."""


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
    if isinstance(error, HTTPError):
        return error.code == 429 or 500 <= error.code < 600
    if isinstance(error, (ConnectionError, TimeoutError, URLError)):
        return True
    status_code = getattr(error, "status_code", None)
    if status_code is None:
        status_code = getattr(error, "status", None)
    return status_code == 429 or isinstance(status_code, int) and 500 <= status_code < 600


class _RetryingTransport:
    """Shared bounded retry policy for concrete transport adapters."""

    def __init__(
        self,
        aliases: dict[str, str] | None = None,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        *,
        capacity: AdaptiveCapacity | None = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if backoff_base < 0:
            raise ValueError("backoff_base must be non-negative")
        self.aliases = self._aliases_from_environment() if aliases is None else dict(aliases)
        self.max_retries = max_retries
        self.backoff_base = backoff_base
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

    def resolve(self, model: str) -> str:
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

    def complete(self, messages: list[dict[str, Any]], model: str, **params: Any) -> Response:
        """Run one normalized completion under the shared recovery policy."""
        resolved_model = self.resolve(model)
        normalized_messages, current_params = self._normalize_request(messages, params)
        transient_retries = 0
        length_retries = 0
        empty_retries = 0

        while True:
            try:
                response = self._complete_once(normalized_messages, resolved_model, current_params)
            except BaseException as error:
                transient = _is_transient_error(error)
                if transient and self.capacity is not None:
                    self.capacity.on_throttle()
                if not transient:
                    raise
                if transient_retries >= self.max_retries:
                    endpoint = self._failure_endpoint()
                    raise RuntimeError(
                        f"Transport failed for model {resolved_model!r} after "
                        f"{self.max_retries} retries{endpoint}: {error}"
                    ) from error
                self._sleep_for_retry(transient_retries)
                transient_retries += 1
                continue

            if response.finish_reason == "length":
                if "max_tokens" not in current_params:
                    raise TruncatedResponseError(
                        f"Model {resolved_model!r} returned a truncated response; "
                        "explicitly set max_tokens then retry."
                    )
                if length_retries >= 2:
                    raise TruncatedResponseError(
                        f"Model {resolved_model!r} remained truncated after {length_retries} "
                        "max_tokens retries."
                    )
                current_params["max_tokens"] = int(current_params["max_tokens"]) * 2
                length_retries += 1
                continue
            if not response.text:
                if empty_retries >= 2:
                    raise EmptyResponseError(
                        f"Model {resolved_model!r} returned an empty response after "
                        f"{empty_retries} retries."
                    )
                self._sleep_for_retry(empty_retries)
                empty_retries += 1
                continue
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

    def _sleep_for_retry(self, retry_number: int) -> None:
        time.sleep(self.backoff_base * (2**retry_number))

    def _complete_once(
        self,
        messages: list[dict[str, Any]],
        model: str,
        params: dict[str, Any],
    ) -> Response:
        raise NotImplementedError

    def _failure_endpoint(self) -> str:
        """Return concrete-adapter context suitable for a final retry error."""
        return ""


class LiteLLMTransport(_RetryingTransport):
    """Transport backed by LiteLLM, imported only when a call is actually made."""

    def _complete_once(
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


class StdlibTransport(_RetryingTransport):
    """OpenAI-compatible transport implemented with the Python standard library."""

    def __init__(
        self,
        api_base: str,
        api_key: str,
        aliases: dict[str, str] | None = None,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        timeout: float = 300.0,
        *,
        capacity: AdaptiveCapacity | None = None,
    ) -> None:
        super().__init__(
            aliases=aliases,
            max_retries=max_retries,
            backoff_base=backoff_base,
            capacity=capacity,
        )
        self.api_base = api_base.rstrip("/")
        self.api_key = api_key
        self._timeout = timeout

    def _complete_once(
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
        with urlopen(request, timeout=self._timeout) as http_response:  # noqa: S310 -- caller supplies endpoint.
            raw_response = json.loads(http_response.read().decode("utf-8"))
        return _response_from_provider(raw_response, model)

    def _failure_endpoint(self) -> str:
        """Add the HTTP endpoint to retry exhaustion diagnostics."""
        return f" at api_base {self.api_base!r}"


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
    return Response(
        text=text,
        usage=_mapping(_value(raw_response, "usage")),
        finish_reason=_value(choice, "finish_reason"),
        reasoning=reasoning if isinstance(reasoning, str) else None,
        model=_value(raw_response, "model", requested_model) or requested_model,
        provider_response_id=(
            provider_response_id
            if isinstance(provider_response_id, str) and provider_response_id.strip()
            else None
        ),
    )
