"""Deterministic retry policy and durable attempt receipt primitives."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal

from .artifacts import sha
from .failures import ProviderFailureKind

RetryJitter = Literal["full", "none"]


@dataclass(frozen=True)
class RetrySchedule:
    """One persisted next-attempt schedule."""

    next_attempt: int
    delay_seconds: float
    due_at: str


@dataclass(frozen=True)
class RetryPolicy:
    """Explicit node retry policy; the first execution counts as attempt one."""

    max_attempts: int = 3
    initial_delay_seconds: float = 1.0
    multiplier: float = 2.0
    max_delay_seconds: float = 120.0
    jitter: RetryJitter = "full"
    retry_on: frozenset[ProviderFailureKind] = field(
        default_factory=lambda: frozenset(
            {
                ProviderFailureKind.RATE_LIMIT,
                ProviderFailureKind.SERVER_ERROR,
                ProviderFailureKind.TIMEOUT,
                ProviderFailureKind.CONNECTION,
            }
        )
    )

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts < 1
        ):
            raise ValueError("RetryPolicy max_attempts must be at least 1")
        for name in ("initial_delay_seconds", "max_delay_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
                raise ValueError(f"RetryPolicy {name} must be non-negative")
        if (
            isinstance(self.multiplier, bool)
            or not isinstance(self.multiplier, int | float)
            or self.multiplier < 1
        ):
            raise ValueError("RetryPolicy multiplier must be at least 1")
        if self.jitter not in {"full", "none"}:
            raise ValueError("RetryPolicy jitter must be 'full' or 'none'")
        normalized: set[ProviderFailureKind] = set()
        for kind in self.retry_on:
            try:
                normalized.add(ProviderFailureKind(kind))
            except ValueError as error:
                raise ValueError(f"Unsupported RetryPolicy failure kind: {kind!r}") from error
        object.__setattr__(self, "retry_on", frozenset(normalized))

    def canonical(self) -> dict[str, object]:
        """Return the stable execution-policy representation."""
        return {
            "max_attempts": self.max_attempts,
            "initial_delay_seconds": float(self.initial_delay_seconds),
            "multiplier": float(self.multiplier),
            "max_delay_seconds": float(self.max_delay_seconds),
            "jitter": self.jitter,
            "retry_on": sorted(kind.value for kind in self.retry_on),
        }

    @property
    def digest(self) -> str:
        """Return the policy digest used by manifests and attempt receipts."""
        return sha(self.canonical())

    def allows(self, kind: ProviderFailureKind | str | None) -> bool:
        """Whether this policy admits one provider failure kind."""
        if kind is None:
            return False
        try:
            normalized = ProviderFailureKind(kind)
        except ValueError:
            return False
        return normalized in self.retry_on

    def schedule(
        self,
        *,
        run_id: str,
        target: str,
        attempt: int,
        now: datetime | None = None,
        retry_after_ms: int | None = None,
    ) -> RetrySchedule:
        """Derive deterministic full jitter and honor provider retry-after."""
        if attempt < 1:
            raise ValueError("attempt must be at least 1")
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        local_cap = min(
            float(self.max_delay_seconds),
            float(self.initial_delay_seconds) * float(self.multiplier) ** (attempt - 1),
        )
        if self.jitter == "full":
            material = f"{run_id}\\0{target}\\0{attempt}\\0{self.digest}".encode()
            fraction = int.from_bytes(hashlib.sha256(material).digest()[:8], "big") / (2**64)
            local_delay = local_cap * fraction
        else:
            local_delay = local_cap
        provider_delay = max(0.0, (retry_after_ms or 0) / 1000)
        delay = max(local_delay, provider_delay)
        due = current.astimezone(UTC) + timedelta(seconds=delay)
        return RetrySchedule(
            next_attempt=attempt + 1,
            delay_seconds=delay,
            due_at=due.isoformat(),
        )


class RetryExhausted(RuntimeError):
    """Raised after a retry policy consumes its final allowed attempt."""

    def __init__(self, target: str, attempts: int, failure: dict[str, object]) -> None:
        self.target = target
        self.attempts = attempts
        self.failure = failure
        super().__init__(f"Retry exhausted for {target!r} after {attempts} attempts")


class AmbiguousAttemptError(RuntimeError):
    """Raised when a crashed attempt may already have crossed a side-effect boundary."""

    def __init__(self, run_id: str, target: str, attempt: int) -> None:
        self.run_id = run_id
        self.target = target
        self.attempt = attempt
        super().__init__(
            f"Run {run_id!r} target {target!r} attempt {attempt} is ambiguous; "
            "record an explicit retry-resolve decision"
        )
