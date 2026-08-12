from __future__ import annotations

import subprocess
import sys
from types import SimpleNamespace
from typing import Any
from urllib.error import HTTPError

import pytest

from kigumi import ProviderFailure, ProviderFailureKind
from kigumi.transport import (
    EmptyResponseError,
    LiteLLMTransport,
    PreparedRequest,
    StdlibTransport,
    TruncatedResponseError,
)


def _send(transport, messages=None, model="default", **params):
    prepared = transport.prepare(messages or [], model, params)
    return transport.send(prepared)


def test_prepared_request_is_frozen_and_canonical() -> None:
    messages = [{"role": "user", "content": "hello"}]
    params = {"temperature": 0.2}
    prepared = PreparedRequest(messages, "provider/model", params)

    messages[0]["content"] = "changed"
    params["temperature"] = 1

    assert prepared.canonical() == {
        "messages": [{"role": "user", "content": "hello"}],
        "model": "provider/model",
        "params": {"temperature": 0.2},
    }
    with pytest.raises(AttributeError):
        prepared.model = "other"  # type: ignore[misc]


def test_length_response_fails_after_single_send(monkeypatch) -> None:
    """截断由 DAG RetryPolicy 处理，transport 不改参数或隐藏重试。"""
    parameters: list[int] = []

    def completion(**kwargs):
        parameters.append(kwargs["max_tokens"])
        return {"choices": [{"message": {"content": "cut"}, "finish_reason": "length"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    with pytest.raises(TruncatedResponseError, match="provider/model"):
        _send(
            LiteLLMTransport(aliases={"default": "provider/model"}),
            [{"role": "user", "content": "hello"}],
            max_tokens=12,
        )

    assert parameters == [12]


def test_length_without_max_tokens_fails_once(monkeypatch) -> None:
    """教训 truncated_output: 未设预算的截断绝不作为完整答案返回。"""
    attempts = 0

    def completion(**kwargs):
        nonlocal attempts
        attempts += 1
        assert "max_tokens" not in kwargs
        return {"choices": [{"message": {"content": "cut"}, "finish_reason": "length"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    with pytest.raises(TruncatedResponseError, match="truncated response"):
        _send(LiteLLMTransport(aliases={"default": "provider/model"}))

    assert attempts == 1


def test_length_and_empty_responses_each_execute_once(monkeypatch) -> None:
    responses = [
        {"choices": [{"message": {"content": "cut"}, "finish_reason": "length"}]},
        {"choices": [{"message": {"content": ""}, "finish_reason": "stop"}]},
    ]
    monkeypatch.setitem(
        sys.modules,
        "litellm",
        SimpleNamespace(completion=lambda **_kwargs: responses.pop(0)),
    )
    transport = LiteLLMTransport(aliases={"default": "provider/model"})

    with pytest.raises(TruncatedResponseError):
        _send(transport, max_tokens=12)
    with pytest.raises(EmptyResponseError):
        _send(transport)
    assert responses == []


def test_empty_response_does_not_retry(monkeypatch) -> None:
    responses = [
        {"choices": [{"message": {"content": ""}, "finish_reason": "stop"}]},
        {"choices": [{"message": {"content": "ready"}, "finish_reason": "stop"}]},
    ]
    monkeypatch.setitem(
        sys.modules,
        "litellm",
        SimpleNamespace(completion=lambda **_kwargs: responses.pop(0)),
    )

    with pytest.raises(EmptyResponseError):
        _send(LiteLLMTransport(aliases={"default": "provider/model"}))

    assert len(responses) == 1


def test_empty_response_fails_after_one_attempt(monkeypatch) -> None:
    """空响应必须中断，不能 sleep、隐藏重试或进入缓存。"""
    attempts = 0

    def completion(**_kwargs: Any) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        return {"choices": [{"message": {"content": ""}, "finish_reason": "stop"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    with pytest.raises(EmptyResponseError, match="provider/model"):
        _send(LiteLLMTransport(aliases={"default": "provider/model"}))

    assert attempts == 1


def test_429_fails_after_one_attempt(monkeypatch) -> None:
    """Transient failures are facts for DAG RetryPolicy, not transport loops."""
    attempts = 0

    def completion(**_kwargs):
        nonlocal attempts
        attempts += 1
        raise HTTPError("https://example.test", 429, "rate limited", {}, None)

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    with pytest.raises(ProviderFailure) as raised:
        _send(LiteLLMTransport(aliases={"default": "provider/model"}))

    assert raised.value.kind is ProviderFailureKind.RATE_LIMIT
    assert attempts == 1


def test_transient_failure_is_typed_after_single_send(monkeypatch) -> None:
    """Retry control consumes structured facts rather than provider prose."""

    def completion(**_kwargs: Any) -> None:
        raise HTTPError("https://example.test", 429, "rate limited", {}, None)

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    with pytest.raises(ProviderFailure) as raised:
        _send(LiteLLMTransport(aliases={"default": "provider/model"}))
    assert raised.value.provider == "litellm"
    assert raised.value.kind is ProviderFailureKind.RATE_LIMIT
    assert raised.value.status_code == 429


def test_stdlib_single_send_returns_typed_failure_without_endpoint_secret(
    monkeypatch,
) -> None:

    def fake_urlopen(*_args: Any, **_kwargs: Any) -> None:
        raise HTTPError("https://example.test/v1/chat/completions", 429, "limited", {}, None)

    monkeypatch.setattr("kigumi.transport.urlopen", fake_urlopen)

    with pytest.raises(ProviderFailure) as raised:
        StdlibTransport(
            "https://example.test",
            "secret",
            aliases={"default": "provider/model"},
        ).send(PreparedRequest([], "provider/model", {}))
    assert raised.value.provider == "openai-compatible"
    assert raised.value.kind is ProviderFailureKind.RATE_LIMIT


def test_stdlib_transport_rejects_non_http_api_base() -> None:
    with pytest.raises(ValueError, match="file"):
        StdlibTransport("file:///etc/passwd", "secret")


@pytest.mark.parametrize("api_base", ["/v1", "localhost:8080"])
def test_stdlib_transport_rejects_scheme_less_api_base(api_base: str) -> None:
    with pytest.raises(ValueError, match="scheme"):
        StdlibTransport(api_base, "secret")


def test_stdlib_transport_suggests_http_scheme_for_host_port() -> None:
    with pytest.raises(ValueError, match="http://"):
        StdlibTransport("localhost:8080", "secret")


@pytest.mark.parametrize("api_base", ["http://", "https://"])
def test_stdlib_transport_rejects_http_api_base_without_host(api_base: str) -> None:
    with pytest.raises(ValueError, match="host"):
        StdlibTransport(api_base, "secret")


@pytest.mark.parametrize(
    ("api_base", "expected_api_base"),
    [
        ("http://localhost:8080/v1/", "http://localhost:8080/v1"),
        ("https://api.example.com/", "https://api.example.com"),
    ],
)
def test_stdlib_transport_accepts_http_api_base_schemes(
    api_base: str, expected_api_base: str
) -> None:
    transport = StdlibTransport(api_base, "secret")

    assert transport.api_base == expected_api_base


def test_transient_errors_adjust_adaptive_capacity_before_success(monkeypatch) -> None:
    """教训 adaptive_transport: 静态槽数在长跑生产会被 429 打死，容量必须是跨进程共享的活值。"""
    attempts = 0

    class RecordingCapacity:
        def __init__(self) -> None:
            self.events: list[str] = []

        def on_throttle(self) -> None:
            self.events.append("throttle")

        def on_success(self) -> None:
            self.events.append("success")

    def completion(**_kwargs: Any) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise HTTPError("https://example.test", 429, "rate limited", {}, None)
        return {"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))
    capacity = RecordingCapacity()
    transport = LiteLLMTransport(aliases={"default": "provider/model"}, capacity=capacity)

    with pytest.raises(ProviderFailure):
        _send(transport)
    response = _send(transport)

    assert response.text == "ok"
    assert capacity.events == ["throttle", "success"]


def test_stdlib_transport_posts_and_normalizes_response(monkeypatch) -> None:
    """教训 timeout_boundary: 标准库请求必须显式使用有限的默认超时。"""

    class FakeHTTPResponse:
        def read(self) -> bytes:
            return (
                b'{"id":"chatcmpl-test-123","model":"mock-model","usage":{"total_tokens":3},'
                b'"choices":[{"message":{"content":"ok",'
                b'"reasoning_content":"internal"},"finish_reason":"stop"}]}'
            )

        def __enter__(self) -> FakeHTTPResponse:
            return self

        def __exit__(self, *_args) -> None:
            return None

    requests: list[Any] = []

    def fake_urlopen(request: Any, *, timeout: float) -> FakeHTTPResponse:
        requests.append(request)
        assert timeout == 300.0
        return FakeHTTPResponse()

    monkeypatch.setattr("kigumi.transport.urlopen", fake_urlopen)

    response = _send(
        StdlibTransport("https://example.test", "secret", aliases={"default": "provider/model"}),
        [{"role": "user", "content": "hello"}],
    )

    assert response.text == "ok"
    assert response.reasoning == "internal"
    assert response.usage == {"total_tokens": 3}
    assert response.provider_response_id == "chatcmpl-test-123"
    assert response.model == "mock-model"
    assert response.model_observed is True
    assert requests[0].full_url == "https://example.test/v1/chat/completions"


def test_provider_model_fallback_is_marked_unobserved(monkeypatch) -> None:
    """A requested-model fallback is routing context, not provider evidence."""

    def completion(**_kwargs: Any) -> dict[str, Any]:
        return {"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    response = _send(LiteLLMTransport(aliases={"default": "provider/model"}))

    assert response.model == "provider/model"
    assert response.model_observed is False


def test_json_mode_translates_to_response_format(monkeypatch) -> None:
    """教训 params_contract: 调用语义在所有具体 transport 前统一翻译。"""
    received: dict[str, Any] = {}

    def completion(**kwargs: Any) -> dict[str, Any]:
        received.update(kwargs)
        return {"choices": [{"message": {"content": "{}"}, "finish_reason": "stop"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    _send(
        LiteLLMTransport(aliases={"default": "provider/model"}),
        json_mode=True,
        reasoning_effort="high",
    )

    assert received["response_format"] == {"type": "json_object"}
    assert "json_mode" not in received
    assert received["reasoning_effort"] == "high"


def test_system_param_prepends_system_message(monkeypatch) -> None:
    """教训 params_contract: system 参数只能形成一个显式的首条 system 消息。"""
    received: list[dict[str, Any]] = []

    def completion(**kwargs: Any) -> dict[str, Any]:
        received.extend(kwargs["messages"])
        return {"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))
    messages = [{"role": "user", "content": "hello"}]

    _send(
        LiteLLMTransport(aliases={"default": "provider/model"}),
        messages,
        system="be concise",
    )

    assert received[0] == {"role": "system", "content": "be concise"}
    assert messages == [{"role": "user", "content": "hello"}]


def test_system_param_conflicts_with_existing_system() -> None:
    """教训 params_contract: 两条 system 指令绝不静默合并。"""
    transport = LiteLLMTransport(aliases={"default": "provider/model"})

    with pytest.raises(ValueError, match="system"):
        transport.prepare(
            [{"role": "system", "content": "one"}],
            "default",
            {"system": "two"},
        )


def test_json_mode_conflicts_with_explicit_response_format() -> None:
    """教训 params_contract: JSON 输出格式的双重声明必须由调用方消歧。"""
    transport = LiteLLMTransport(aliases={"default": "provider/model"})

    with pytest.raises(ValueError, match="response_format"):
        transport.prepare(
            [],
            "default",
            {"json_mode": True, "response_format": {"type": "text"}},
        )


def test_import_without_litellm_keeps_stdlib_available() -> None:
    """教训 optional_dependency: 导入公共包不能要求未选装的 LiteLLM。"""
    program = """
import sys
sys.modules['litellm'] = None
import kigumi
from kigumi.transport import StdlibTransport
assert StdlibTransport is not None
"""

    result = subprocess.run(
        [sys.executable, "-c", program],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_unconfigured_alias_is_clear_error() -> None:
    with pytest.raises(ValueError, match="not configured"):
        LiteLLMTransport(aliases={}).prepare([], "default", {})
