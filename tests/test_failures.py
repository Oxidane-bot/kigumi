from __future__ import annotations

from urllib.error import HTTPError

from kigumi import (
    AgentExecutionFailure,
    AgentRuntimeFailureCode,
    ProviderFailure,
    ProviderFailureKind,
    ProviderFailureStage,
)
from kigumi.failures import provider_failure_from_exception


def test_provider_failure_classifies_structured_status_and_transport_types() -> None:
    rate_limit = provider_failure_from_exception(
        HTTPError(
            "https://provider.invalid/v1/chat",
            429,
            "provider prose is not control data",
            {"Retry-After": "1.5", "x-request-id": "request-1"},
            None,
        ),
        provider="openai-compatible",
        stage=ProviderFailureStage.RESPONSE,
    )
    assert rate_limit.kind is ProviderFailureKind.RATE_LIMIT
    assert rate_limit.status_code == 429
    assert rate_limit.retry_after_ms == 1500
    assert rate_limit.provider_request_id == "request-1"

    assert (
        provider_failure_from_exception(
            TimeoutError("secret prose"),
            provider="openai-compatible",
            stage=ProviderFailureStage.TRANSPORT,
        ).kind
        is ProviderFailureKind.TIMEOUT
    )
    assert (
        provider_failure_from_exception(
            ConnectionError("secret prose"),
            provider="openai-compatible",
            stage=ProviderFailureStage.TRANSPORT,
        ).kind
        is ProviderFailureKind.CONNECTION
    )


def test_provider_failure_status_matrix_and_prose_do_not_drive_classification() -> None:
    expected = {
        401: ProviderFailureKind.AUTHENTICATION,
        403: ProviderFailureKind.AUTHORIZATION,
        400: ProviderFailureKind.INVALID_REQUEST,
        500: ProviderFailureKind.SERVER_ERROR,
        503: ProviderFailureKind.SERVER_ERROR,
    }
    for status, kind in expected.items():
        failure = provider_failure_from_exception(
            HTTPError("https://provider.invalid", status, "rate limit timeout", {}, None),
            provider="provider",
            stage=ProviderFailureStage.RESPONSE,
        )
        assert failure.kind is kind

    unknown = provider_failure_from_exception(
        RuntimeError("429 rate limit model mismatch timeout"),
        provider="provider",
        stage=ProviderFailureStage.PROVIDER,
    )
    assert unknown.kind is ProviderFailureKind.UNKNOWN


def test_provider_and_agent_failures_have_canonical_typed_metadata() -> None:
    provider = ProviderFailure(
        provider="provider",
        stage=ProviderFailureStage.PROVIDER,
        kind=ProviderFailureKind.MODEL_MISMATCH,
        status_code=None,
        retry_after_ms=None,
        provider_request_id=None,
        message_digest="a" * 64,
        retryable_hint=False,
    )
    agent = AgentExecutionFailure(provider_failure=provider)
    assert agent.canonical() == {
        "failure_type": "provider",
        "provider_failure": {
            "provider": "provider",
            "stage": "provider",
            "kind": "model_mismatch",
            "status_code": None,
            "retry_after_ms": None,
            "provider_request_id": None,
            "message_digest": "a" * 64,
            "retryable_hint": False,
        },
        "runtime_code": None,
    }

    runtime = AgentExecutionFailure(runtime_code=AgentRuntimeFailureCode.CAPACITY)
    assert runtime.canonical()["runtime_code"] == "capacity"
    assert runtime.provider_failure is None
