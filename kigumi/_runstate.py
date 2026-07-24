"""Private durable run manifest and attempt receipt storage."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from .artifacts import atomic_write_json, canonical_json, sha
from .failures import canonical_failure
from .retry import AmbiguousAttemptError, RetryPolicy

RUN_MANIFEST_SCHEMA = 2
ATTEMPT_RECEIPT_SCHEMA = 2


def utc_now() -> datetime:
    """Return a timezone-aware timestamp; isolated for deterministic tests."""
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat()


class RunManifestError(RuntimeError):
    """Raised when an existing run does not match the current declaration."""


class AttemptStore:
    """Own one 0.7 run's immutable declaration and mutable attempt receipts."""

    def __init__(self, run_root: Path, manifest_identity: dict[str, Any]) -> None:
        self.run_root = run_root
        self.manifest_path = run_root / "_run.json"
        self.identity = json.loads(canonical_json(manifest_identity))

    def initialize(self) -> dict[str, Any]:
        """Create a new manifest or fail closed against an existing run."""
        self.run_root.mkdir(parents=True, exist_ok=True)
        existing = self._read_json(self.manifest_path)
        if existing is None:
            if any(self.run_root.iterdir()):
                raise RunManifestError(
                    f"Run {self.run_root.name!r} predates run manifest schema 2 and "
                    "cannot be resumed"
                )
            now = iso_now()
            manifest = {
                "run_manifest_schema": RUN_MANIFEST_SCHEMA,
                **self.identity,
                "status": "running",
                "created_at": now,
                "updated_at": now,
            }
            atomic_write_json(self.manifest_path, manifest)
            return manifest
        if existing.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA:
            raise RunManifestError(f"Run {self.run_root.name!r} has an unsupported manifest schema")
        expected = {
            key: value
            for key, value in existing.items()
            if key
            not in {
                "status",
                "created_at",
                "updated_at",
                "pending_retries",
                "ambiguous_attempts",
                "failure",
                "resume_count",
                "last_resumed_at",
                "workflow_profile",
                "workflow_profile_digest",
            }
        }
        actual = {
            "run_manifest_schema": RUN_MANIFEST_SCHEMA,
            **{
                key: value
                for key, value in self.identity.items()
                if key not in {"workflow_profile", "workflow_profile_digest"}
            },
        }
        if expected != actual:
            changed = sorted(
                key for key in set(expected) | set(actual) if expected.get(key) != actual.get(key)
            )
            raise RunManifestError(
                f"Run {self.run_root.name!r} declaration changed: {', '.join(changed)}"
            )
        return existing

    def mark_resumed(self) -> None:
        """Record an operator/runtime resume without changing immutable run identity."""
        manifest = self._required_json(self.manifest_path)
        manifest["resume_count"] = int(manifest.get("resume_count", 0)) + 1
        manifest["last_resumed_at"] = iso_now()
        manifest["updated_at"] = manifest["last_resumed_at"]
        atomic_write_json(self.manifest_path, manifest)

    def update_manifest(
        self,
        status: str,
        *,
        pending_retries: list[dict[str, Any]] | None = None,
        ambiguous_attempts: list[dict[str, Any]] | None = None,
        failure: dict[str, Any] | None = None,
    ) -> None:
        manifest = self._required_json(self.manifest_path)
        manifest["status"] = status
        manifest["updated_at"] = iso_now()
        manifest["pending_retries"] = pending_retries or []
        manifest["ambiguous_attempts"] = ambiguous_attempts or []
        if failure is not None:
            manifest["failure"] = failure
        elif status != "failed":
            manifest.pop("failure", None)
        atomic_write_json(self.manifest_path, manifest)

    def prepare(
        self,
        target: str,
        *,
        policy: RetryPolicy | None,
        declaration_digest: str,
        prompt_resolutions: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return run, pending, candidate, or completed state for one target."""
        state_path = self._state_path(target)
        state = self._read_json(state_path)
        policy_digest = policy.digest if policy is not None else None
        if state is None:
            return self._start_attempt(
                target,
                attempt=1,
                policy_digest=policy_digest,
                declaration_digest=declaration_digest,
                prompt_resolutions=prompt_resolutions or {},
            )
        self._validate_state(
            state,
            target=target,
            policy_digest=policy_digest,
            declaration_digest=declaration_digest,
            prompt_resolutions=prompt_resolutions or {},
        )
        status = state.get("status")
        if status == "running":
            attempt = int(state["attempt"])
            if state.get("side_effect_started") is True:
                state["status"] = "ambiguous"
                state["updated_at"] = iso_now()
                atomic_write_json(state_path, state)
                self._write_receipt(target, attempt, state)
                raise AmbiguousAttemptError(self.run_root.name, target, attempt)
            return self._start_attempt(
                target,
                attempt=attempt,
                policy_digest=policy_digest,
                declaration_digest=declaration_digest,
                prompt_resolutions=prompt_resolutions or {},
            )
        if status == "checkpoint_pending":
            return self._start_attempt(
                target,
                attempt=int(state["attempt"]),
                policy_digest=policy_digest,
                declaration_digest=declaration_digest,
                prompt_resolutions=prompt_resolutions or {},
            )
        if status == "retry_scheduled":
            due = datetime.fromisoformat(str(state["due_at"]))
            if utc_now() < due:
                return {"action": "pending", "state": state}
            return self._start_attempt(
                target,
                attempt=int(state["next_attempt"]),
                policy_digest=policy_digest,
                declaration_digest=declaration_digest,
                prompt_resolutions=prompt_resolutions or {},
            )
        if status == "success_candidate":
            candidate = self._required_json(
                self._target_root(target) / str(state["candidate_file"])
            )
            if state.get("candidate_sha256") != sha(candidate):
                raise RunManifestError(f"Success candidate for {target!r} failed digest validation")
            return {
                "action": "candidate",
                "state": state,
                "candidate": candidate,
            }
        if status == "completed":
            return {"action": "completed", "state": state}
        if status == "ambiguous":
            raise AmbiguousAttemptError(
                self.run_root.name,
                target,
                int(state["attempt"]),
            )
        if status == "failed":
            return {"action": "failed", "state": state}
        raise RunManifestError(f"Attempt state for {target!r} has invalid status {status!r}")

    def mark_side_effect(
        self,
        target: str,
        active_effect: dict[str, Any] | None = None,
    ) -> None:
        """Persist the provider/Agent side-effect boundary before crossing it."""
        state = self._required_json(self._state_path(target))
        if state.get("status") != "running":
            raise RunManifestError(f"Cannot mark side effect for {target!r} in non-running state")
        if active_effect is not None:
            canonical = json.loads(canonical_json(active_effect))
            if not isinstance(canonical, dict):
                raise RunManifestError("active side effect must be a canonical object")
            state["active_effect"] = canonical
        state["side_effect_started"] = True
        state.setdefault("side_effect_started_at", iso_now())
        state["updated_at"] = iso_now()
        atomic_write_json(self._state_path(target), state)
        self._write_receipt(target, int(state["attempt"]), state)

    def mark_checkpoint(self, target: str, checkpoint: str) -> None:
        state = self._required_json(self._state_path(target))
        state.update(
            {
                "status": "checkpoint_pending",
                "checkpoint": checkpoint,
                "updated_at": iso_now(),
            }
        )
        atomic_write_json(self._state_path(target), state)
        self._write_receipt(target, int(state["attempt"]), state)

    def save_candidate(self, target: str, candidate: dict[str, Any]) -> dict[str, Any]:
        """Persist canonical success before cache sealing or materialization."""
        state = self._required_json(self._state_path(target))
        if state.get("status") != "running":
            raise RunManifestError(f"Cannot save candidate for {target!r} in non-running state")
        canonical = json.loads(canonical_json(candidate))
        attempt = int(state["attempt"])
        filename = f"candidate-{attempt:04d}.json"
        atomic_write_json(self._target_root(target) / filename, canonical)
        state.update(
            {
                "status": "success_candidate",
                "candidate_file": filename,
                "candidate_sha256": sha(canonical),
                "succeeded_at": iso_now(),
                "updated_at": iso_now(),
            }
        )
        atomic_write_json(self._state_path(target), state)
        self._write_receipt(target, attempt, state)
        return canonical

    def mark_completed(self, target: str, *, artifact_sha256: str) -> None:
        state = self._required_json(self._state_path(target))
        state.update(
            {
                "status": "completed",
                "artifact_sha256": artifact_sha256,
                "completed_at": iso_now(),
                "updated_at": iso_now(),
            }
        )
        atomic_write_json(self._state_path(target), state)
        self._write_receipt(target, int(state["attempt"]), state)

    def record_failure(
        self,
        target: str,
        error: Exception,
        *,
        policy: RetryPolicy | None,
        calls: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Persist terminal or retry-scheduled failure state."""
        from .failures import failure_provider_kind, failure_retry_after_ms

        state = self._required_json(self._state_path(target))
        attempt = int(state["attempt"])
        failure = canonical_failure(error)
        retryable = (
            policy is not None
            and attempt < policy.max_attempts
            and policy.allows(failure_provider_kind(error))
        )
        state["failure"] = failure
        state["calls"] = json.loads(canonical_json(calls or []))
        state["failed_at"] = iso_now()
        if retryable:
            schedule = policy.schedule(
                run_id=self.run_root.name,
                target=target,
                attempt=attempt,
                retry_after_ms=failure_retry_after_ms(error),
            )
            state.update(
                {
                    "status": "retry_scheduled",
                    "next_attempt": schedule.next_attempt,
                    "delay_seconds": schedule.delay_seconds,
                    "due_at": schedule.due_at,
                    "updated_at": iso_now(),
                }
            )
            action = "pending"
        else:
            state.update({"status": "failed", "updated_at": iso_now()})
            action = "failed"
        atomic_write_json(self._state_path(target), state)
        self._write_receipt(target, attempt, state)
        return {"action": action, "state": state}

    def pending_retries(self) -> list[dict[str, Any]]:
        return self._states_with("retry_scheduled")

    def ambiguous_attempts(self) -> list[dict[str, Any]]:
        return self._states_with("ambiguous")

    def resolve(
        self,
        target: str,
        *,
        attempt: int,
        action: Literal["retry", "fail"],
        reason: str,
    ) -> dict[str, Any]:
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("retry resolution reason must be non-empty")
        state = self._required_json(self._state_path(target))
        if (
            state.get("status") == "running"
            and state.get("side_effect_started") is True
            and state.get("attempt") == attempt
        ):
            state["status"] = "ambiguous"
            state["updated_at"] = iso_now()
            atomic_write_json(self._state_path(target), state)
            self._write_receipt(target, attempt, state)
        if state.get("status") != "ambiguous" or state.get("attempt") != attempt:
            raise ValueError(
                f"Target {target!r} attempt {attempt} is not the active ambiguous attempt"
            )
        resolution = {
            "attempt_receipt_schema": ATTEMPT_RECEIPT_SCHEMA,
            "target": target,
            "attempt": attempt,
            "action": action,
            "reason": reason.strip(),
            "resolved_at": iso_now(),
        }
        atomic_write_json(
            self._target_root(target) / f"resolution-{attempt:04d}.json",
            resolution,
        )
        state["resolution"] = resolution
        state["updated_at"] = iso_now()
        if action == "retry":
            state.update(
                {
                    "status": "retry_scheduled",
                    "next_attempt": attempt + 1,
                    "delay_seconds": 0.0,
                    "due_at": iso_now(),
                }
            )
        else:
            state["status"] = "failed"
            state["failure"] = {
                "failure_type": "manual_resolution",
                "action": "fail",
                "reason_digest": sha(reason.strip()),
            }
        atomic_write_json(self._state_path(target), state)
        self._write_receipt(target, attempt, state)
        return state

    def state_for(self, target: str) -> dict[str, Any] | None:
        return self._read_json(self._state_path(target))

    def _start_attempt(
        self,
        target: str,
        *,
        attempt: int,
        policy_digest: str | None,
        declaration_digest: str,
        prompt_resolutions: dict[str, Any],
    ) -> dict[str, Any]:
        now = iso_now()
        state = {
            "attempt_receipt_schema": ATTEMPT_RECEIPT_SCHEMA,
            "target": target,
            "target_digest": sha(target),
            "attempt": attempt,
            "status": "running",
            "side_effect_started": False,
            "policy_digest": policy_digest,
            "declaration_digest": declaration_digest,
            "prompt_resolutions": json.loads(canonical_json(prompt_resolutions)),
            "started_at": now,
            "updated_at": now,
        }
        target_root = self._target_root(target)
        target_root.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self._state_path(target), state)
        self._write_receipt(target, attempt, state)
        return {"action": "run", "state": state}

    def _validate_state(
        self,
        state: dict[str, Any],
        *,
        target: str,
        policy_digest: str | None,
        declaration_digest: str,
        prompt_resolutions: dict[str, Any],
    ) -> None:
        if state.get("attempt_receipt_schema") != ATTEMPT_RECEIPT_SCHEMA:
            raise RunManifestError(f"Attempt state for {target!r} has unsupported schema")
        if (
            state.get("target") != target
            or state.get("target_digest") != sha(target)
            or state.get("policy_digest") != policy_digest
            or state.get("declaration_digest") != declaration_digest
            or state.get("prompt_resolutions") != prompt_resolutions
        ):
            raise RunManifestError(f"Attempt state declaration changed for {target!r}")

    def _states_with(self, status: str) -> list[dict[str, Any]]:
        attempts_root = self.run_root / "attempts"
        if not attempts_root.is_dir():
            return []
        found: list[dict[str, Any]] = []
        for path in sorted(attempts_root.glob("*/state.json")):
            state = self._read_json(path)
            if state is not None and state.get("status") == status:
                found.append(state)
        return found

    def _target_root(self, target: str) -> Path:
        return self.run_root / "attempts" / sha(target)

    def _state_path(self, target: str) -> Path:
        return self._target_root(target) / "state.json"

    def _write_receipt(self, target: str, attempt: int, state: dict[str, Any]) -> None:
        atomic_write_json(
            self._target_root(target) / f"attempt-{attempt:04d}.json",
            state,
        )

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any] | None:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def _required_json(self, path: Path) -> dict[str, Any]:
        value = self._read_json(path)
        if value is None:
            raise RunManifestError(f"Missing or invalid durable run state: {path}")
        return value
