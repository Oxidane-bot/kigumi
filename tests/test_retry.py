from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kigumi import ProviderFailureKind, RetryPolicy


def test_retry_policy_defaults_and_validation() -> None:
    policy = RetryPolicy()
    assert policy.max_attempts == 3
    assert policy.retry_on == frozenset(
        {
            ProviderFailureKind.RATE_LIMIT,
            ProviderFailureKind.SERVER_ERROR,
            ProviderFailureKind.TIMEOUT,
            ProviderFailureKind.CONNECTION,
        }
    )
    assert policy.digest == RetryPolicy().digest

    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError, match="jitter"):
        RetryPolicy(jitter="partial")  # type: ignore[arg-type]


def test_retry_schedule_is_deterministic_and_honors_retry_after() -> None:
    now = datetime(2026, 7, 24, 8, 0, tzinfo=UTC)
    policy = RetryPolicy(initial_delay_seconds=10, multiplier=2, max_delay_seconds=15)
    first = policy.schedule(
        run_id="run-1",
        target="node",
        attempt=1,
        now=now,
        retry_after_ms=20_000,
    )
    second = policy.schedule(
        run_id="run-1",
        target="node",
        attempt=1,
        now=now,
        retry_after_ms=20_000,
    )
    assert first == second
    assert first.delay_seconds == 20
    assert first.due_at == "2026-07-24T08:00:20+00:00"
    assert first.next_attempt == 2


def test_retry_policy_never_retries_non_retryable_provider_kinds_by_default() -> None:
    policy = RetryPolicy()
    for kind in (
        ProviderFailureKind.UNKNOWN,
        ProviderFailureKind.AUTHENTICATION,
        ProviderFailureKind.AUTHORIZATION,
        ProviderFailureKind.INVALID_REQUEST,
        ProviderFailureKind.MODEL_MISMATCH,
    ):
        assert not policy.allows(kind)
