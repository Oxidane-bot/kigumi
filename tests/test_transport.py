from __future__ import annotations

import subprocess
import sys
from types import SimpleNamespace
from typing import Any
from urllib.error import HTTPError

import pytest

from kigumi.transport import (
    EmptyResponseError,
    LiteLLMTransport,
    StdlibTransport,
    TruncatedResponseError,
)


def test_length_finish_doubles_max_tokens(monkeypatch) -> None:
    """教训 judge_14: length completion gets two bounded token-budget expansions."""
    parameters: list[int] = []
    responses: list[dict[str, Any]] = [
        {"choices": [{"message": {"content": "cut"}, "finish_reason": "length"}]},
        {"choices": [{"message": {"content": "still cut"}, "finish_reason": "length"}]},
        {"choices": [{"message": {"content": "done"}, "finish_reason": "stop"}]},
    ]

    def completion(**kwargs):
        parameters.append(kwargs["max_tokens"])
        return responses.pop(0)

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    response = LiteLLMTransport(aliases={"default": "provider/model"}).complete(
        [{"role": "user", "content": "hello"}], "default", max_tokens=12
    )

    assert response.text == "done"
    assert parameters == [12, 24, 48]


def test_length_without_max_tokens_returns_as_is(monkeypatch) -> None:
    """教训 truncated_output: 未设预算的截断绝不作为完整答案返回。"""
    attempts = 0

    def completion(**kwargs):
        nonlocal attempts
        attempts += 1
        assert "max_tokens" not in kwargs
        return {"choices": [{"message": {"content": "cut"}, "finish_reason": "length"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    with pytest.raises(TruncatedResponseError, match="explicitly set max_tokens"):
        LiteLLMTransport(aliases={"default": "provider/model"}).complete([], "default")

    assert attempts == 1


def test_length_retry_exhaustion_raises(monkeypatch) -> None:
    """教训 judge_14: 两次预算扩展后仍截断，不能把残文交给下游。"""
    parameters: list[int] = []

    def completion(**kwargs: Any) -> dict[str, Any]:
        parameters.append(kwargs["max_tokens"])
        return {"choices": [{"message": {"content": "cut"}, "finish_reason": "length"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    with pytest.raises(TruncatedResponseError, match="provider/model"):
        LiteLLMTransport(aliases={"default": "provider/model"}).complete(
            [], "default", max_tokens=12
        )

    assert parameters == [12, 24, 48]


def test_empty_response_retries(monkeypatch) -> None:
    responses = [
        {"choices": [{"message": {"content": ""}, "finish_reason": "stop"}]},
        {"choices": [{"message": {"content": "ready"}, "finish_reason": "stop"}]},
    ]
    monkeypatch.setitem(
        sys.modules,
        "litellm",
        SimpleNamespace(completion=lambda **_kwargs: responses.pop(0)),
    )

    response = LiteLLMTransport(aliases={"default": "provider/model"}).complete([], "default")

    assert response.text == "ready"
    assert responses == []


def test_empty_response_exhaustion_raises_with_backoff(monkeypatch) -> None:
    """教训 empty_response_poison: 空响应耗尽时必须中断，不能进入缓存。"""
    delays: list[float] = []
    attempts = 0

    def completion(**_kwargs: Any) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        return {"choices": [{"message": {"content": ""}, "finish_reason": "stop"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))
    monkeypatch.setattr("kigumi.transport.time.sleep", delays.append)

    with pytest.raises(EmptyResponseError, match="provider/model.*2 retries"):
        LiteLLMTransport(aliases={"default": "provider/model"}, backoff_base=0.25).complete(
            [], "default"
        )

    assert attempts == 3
    assert delays == [0.25, 0.5]


def test_429_backoff_retries(monkeypatch) -> None:
    """Transient 429 failures use bounded exponential delays."""
    delays: list[float] = []
    attempts = 0

    def completion(**_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise HTTPError("https://example.test", 429, "rate limited", {}, None)
        return {"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))
    monkeypatch.setattr("kigumi.transport.time.sleep", delays.append)

    response = LiteLLMTransport(
        aliases={"default": "provider/model"}, max_retries=3, backoff_base=0.25
    ).complete([], "default")

    assert response.text == "ok"
    assert attempts == 3
    assert delays == [0.25, 0.5]


def test_transient_retry_exhaustion_names_the_model(monkeypatch) -> None:
    """教训 retry_context: 多模型流水线的最终传输错误必须点名失败模型。"""

    def completion(**_kwargs: Any) -> None:
        raise HTTPError("https://example.test", 429, "rate limited", {}, None)

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))
    monkeypatch.setattr("kigumi.transport.time.sleep", lambda _delay: None)

    with pytest.raises(
        RuntimeError, match="Transport failed for model 'provider/model' after 1 retries"
    ):
        LiteLLMTransport(aliases={"default": "provider/model"}, max_retries=1).complete(
            [], "default"
        )


def test_stdlib_retry_exhaustion_names_the_endpoint(monkeypatch) -> None:
    """教训 retry_endpoint: 标准库适配器的重试错误还要标明请求端点。"""

    def fake_urlopen(*_args: Any, **_kwargs: Any) -> None:
        raise HTTPError("https://example.test/v1/chat/completions", 429, "limited", {}, None)

    monkeypatch.setattr("kigumi.transport.urlopen", fake_urlopen)

    with pytest.raises(RuntimeError, match="api_base 'https://example.test'"):
        StdlibTransport(
            "https://example.test",
            "secret",
            aliases={"default": "provider/model"},
            max_retries=0,
        ).complete([], "default")


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
    monkeypatch.setattr("kigumi.transport.time.sleep", lambda _delay: None)
    capacity = RecordingCapacity()
    response = LiteLLMTransport(aliases={"default": "provider/model"}, capacity=capacity).complete(
        [], "default"
    )

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

    response = StdlibTransport(
        "https://example.test", "secret", aliases={"default": "provider/model"}
    ).complete([{"role": "user", "content": "hello"}], "default")

    assert response.text == "ok"
    assert response.reasoning == "internal"
    assert response.usage == {"total_tokens": 3}
    assert response.provider_response_id == "chatcmpl-test-123"
    assert requests[0].full_url == "https://example.test/v1/chat/completions"


def test_json_mode_translates_to_response_format(monkeypatch) -> None:
    """教训 params_contract: 调用语义在所有具体 transport 前统一翻译。"""
    received: dict[str, Any] = {}

    def completion(**kwargs: Any) -> dict[str, Any]:
        received.update(kwargs)
        return {"choices": [{"message": {"content": "{}"}, "finish_reason": "stop"}]}

    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    LiteLLMTransport(aliases={"default": "provider/model"}).complete(
        [], "default", json_mode=True, reasoning_effort="high"
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

    LiteLLMTransport(aliases={"default": "provider/model"}).complete(
        messages, "default", system="be concise"
    )

    assert received[0] == {"role": "system", "content": "be concise"}
    assert messages == [{"role": "user", "content": "hello"}]


def test_system_param_conflicts_with_existing_system() -> None:
    """教训 params_contract: 两条 system 指令绝不静默合并。"""
    transport = LiteLLMTransport(aliases={"default": "provider/model"})

    with pytest.raises(ValueError, match="system"):
        transport.complete([{"role": "system", "content": "one"}], "default", system="two")


def test_json_mode_conflicts_with_explicit_response_format() -> None:
    """教训 params_contract: JSON 输出格式的双重声明必须由调用方消歧。"""
    transport = LiteLLMTransport(aliases={"default": "provider/model"})

    with pytest.raises(ValueError, match="response_format"):
        transport.complete(
            [],
            "default",
            json_mode=True,
            response_format={"type": "text"},
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
        LiteLLMTransport(aliases={}).complete([], "default")
