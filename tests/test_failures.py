from __future__ import annotations

from urllib.error import HTTPError

import pytest

from kigumi import (
    AgentExecutionFailure,
    AgentRuntimeFailureCode,
    ProviderFailure,
    ProviderFailureKind,
    ProviderFailureStage,
)
from kigumi.failures import AgentRuntimeFailureSubCode, provider_failure_from_exception


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
        "runtime_subcode": None,
    }

    runtime = AgentExecutionFailure(runtime_code=AgentRuntimeFailureCode.CAPACITY)
    assert runtime.canonical()["runtime_code"] == "capacity"
    assert runtime.canonical()["runtime_subcode"] is None
    assert runtime.provider_failure is None


def test_agent_runtime_subcodes_are_closed_canonical_and_code_compatible() -> None:
    cases = (
        (AgentRuntimeFailureCode.PROTOCOL, AgentRuntimeFailureSubCode.ENVELOPE),
        (AgentRuntimeFailureCode.POLICY, AgentRuntimeFailureSubCode.BRIDGE_POLICY),
        (AgentRuntimeFailureCode.PROTOCOL, AgentRuntimeFailureSubCode.SUBMIT_CONTRACT),
        (AgentRuntimeFailureCode.POLICY, AgentRuntimeFailureSubCode.CONFIG_POLICY),
    )
    canonical = []
    for runtime_code, runtime_subcode in cases:
        failure = AgentExecutionFailure(
            runtime_code=runtime_code,
            runtime_subcode=runtime_subcode,
        )
        value = failure.canonical()
        assert value["runtime_code"] == runtime_code.value
        assert value["runtime_subcode"] == runtime_subcode.value
        canonical.append(value)

    assert len({(item["runtime_code"], item["runtime_subcode"]) for item in canonical}) == 4
    with pytest.raises(ValueError, match="requires runtime_code"):
        AgentExecutionFailure(
            runtime_code=AgentRuntimeFailureCode.PROTOCOL,
            runtime_subcode=AgentRuntimeFailureSubCode.BRIDGE_POLICY,
        )
