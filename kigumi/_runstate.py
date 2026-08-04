"""Private durable run manifest and attempt receipt storage."""

from __future__ import annotations

import fcntl
import json
import os
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from .artifacts import atomic_write_json, canonical_json, sha
from .failures import canonical_failure
from .retry import AmbiguousAttemptError, RetryPolicy

RUN_MANIFEST_SCHEMA = 2
RUN_SIDECAR_SCHEMA = 2
FAILURE_SCHEMA = 2
SUCCESS_CANDIDATE_SCHEMA = 2
ATTEMPT_RECEIPT_SCHEMA = 2
_STATE_DIGEST_FIELD = "state_sha256"
_RECEIPT_SEQUENCE_FIELD = "receipt_sequence"
_PREVIOUS_RECEIPT_DIGEST_FIELD = "previous_receipt_sha256"
_RECEIPT_CHAIN_FIELD = "attempt_receipt_chains"
_TARGET_OWNER_FIELD = "target_owner_token"
_RECOVERY_DECISION_FIELD = "recovery_decision"
_RECOVERY_RECEIPT_FILE_FIELD = "recovery_receipt_file"
_RECOVERY_RECEIPT_DIGEST_FIELD = "recovery_receipt_sha256"
_PROCESS_TARGET_LEASES: dict[Path, tuple[str, Any]] = {}
_PROCESS_TARGET_LEASES_LOCK = threading.RLock()


def utc_now() -> datetime:
    """Return a timezone-aware timestamp; isolated for deterministic tests."""
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat()


class RunManifestError(RuntimeError):
    """Raised when an existing run does not match the current declaration."""


class StateIntegrityError(RunManifestError):
    """Raised when durable JSON state exists but cannot be trusted."""

    def __init__(
        self,
        path: Path,
        expected_schema: int,
        parse_error: BaseException | str,
    ) -> None:
        self.path = Path(path)
        self.expected_schema = expected_schema
        self.parse_error = parse_error
        super().__init__(
            f"Durable state integrity error at {self.path} "
            f"(expected schema {expected_schema}): {parse_error}"
        )


class AttemptStore:
    """Own one 0.7 run's immutable declaration and mutable attempt receipts."""

    def __init__(self, run_root: Path, manifest_identity: dict[str, Any]) -> None:
        self.run_root = run_root
        self.manifest_path = run_root / "_run.json"
        self.identity = json.loads(canonical_json(manifest_identity))
        self._receipt_chain_lock = threading.RLock()
        self._run_lock_local = threading.local()
        resolved_root = run_root.resolve()
        lock_name = f".kigumi-run-{sha(str(resolved_root))}.lock"
        self._run_lock_path = resolved_root.parent / lock_name
        self._target_leases: dict[str, tuple[str, Any]] = {}

    def __del__(self) -> None:
        """Release process-local target descriptors when a store is discarded."""
        for target in tuple(getattr(self, "_target_leases", {})):
            with suppress(BaseException):
                self._release_target_lease(target)

    @contextmanager
    def _run_locked(self) -> Iterator[None]:
        """Serialize one run's durable read-modify-write transactions.

        The advisory lock lives beside, rather than inside, the run directory so
        its existence cannot turn a manifest-less legacy directory into a new
        durable run.  The thread-local depth makes this lock safely re-entrant
        for helpers such as ``prepare`` -> ``_write_state``.
        """
        with self._receipt_chain_lock:
            depth = getattr(self._run_lock_local, "depth", 0)
            if depth:
                self._run_lock_local.depth = depth + 1
                try:
                    yield
                finally:
                    self._run_lock_local.depth = depth
                return

            self._run_lock_path.parent.mkdir(parents=True, exist_ok=True)
            with self._run_lock_path.open("a+", encoding="utf-8") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                self._run_lock_local.depth = 1
                try:
                    yield
                finally:
                    self._run_lock_local.depth = 0
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _target_lock_path(self, target: str) -> Path:
        return self._run_lock_path.with_name(
            f"{self._run_lock_path.name}.target-{sha(target)}.lock"
        )

    def _acquire_target_lease(
        self,
        target: str,
        expected_owner_token: str | None = None,
        *,
        allow_same_process_fence: bool = False,
    ) -> bool:
        """Claim an active target attempt until it leaves execution.

        The descriptor-backed flock is released by the operating system if the
        owning process dies.  A second live executor therefore gets an explicit
        busy error instead of restarting the same attempt and potentially
        crossing the external side-effect boundary twice.  The path is stable;
        the token is a fencing token stored in the durable state, not part of
        the lock filename.
        """
        existing = self._target_leases.get(target)
        if existing is not None:
            current_token = existing[0]
            if expected_owner_token != current_token:
                raise RunManifestError(
                    f"Target {target!r} busy or stale: local lease does not match "
                    "the durable owner token"
                )
            return False
        if expected_owner_token is not None and not self._is_owner_token(expected_owner_token):
            raise StateIntegrityError(
                self._state_path(target),
                ATTEMPT_RECEIPT_SCHEMA,
                "active state has an invalid target owner token",
            )
        token = uuid.uuid4().hex
        path = self._target_lock_path(target)
        path.parent.mkdir(parents=True, exist_ok=True)
        with _PROCESS_TARGET_LEASES_LOCK:
            process_lease = _PROCESS_TARGET_LEASES.get(path)
            if process_lease is not None and allow_same_process_fence:
                _, old_handle = process_lease
                _PROCESS_TARGET_LEASES.pop(path, None)
                if not old_handle.closed:
                    fcntl.flock(old_handle.fileno(), fcntl.LOCK_UN)
                    old_handle.close()
            handle = path.open("a+", encoding="utf-8")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                handle.close()
                raise RunManifestError(
                    f"Target {target!r} busy: another executor owns its active attempt"
                ) from error
            _PROCESS_TARGET_LEASES[path] = (token, handle)
        self._target_leases[target] = (token, handle)
        return True

    def _require_target_lease(self, target: str, state: dict[str, Any]) -> str:
        """Require this instance's lease and the state-backed fencing token."""
        expected = state.get(_TARGET_OWNER_FIELD)
        if not self._is_owner_token(expected):
            raise StateIntegrityError(
                self._state_path(target),
                ATTEMPT_RECEIPT_SCHEMA,
                "active state is missing or has an invalid target owner token",
            )
        lease = self._target_leases.get(target)
        if lease is None:
            raise RunManifestError(
                f"Target {target!r} busy or stale: this executor does not hold its lease"
            )
        current, handle = lease
        if handle.closed or current != expected:
            raise RunManifestError(
                f"Target {target!r} busy or stale: local lease does not match "
                "the durable owner token"
            )
        return current

    def _target_owner_token(self, target: str) -> str:
        lease = self._target_leases.get(target)
        if lease is None:
            raise RunManifestError(f"Target {target!r} has no active owner lease")
        return lease[0]

    def _release_target_lease(self, target: str) -> None:
        lease = self._target_leases.pop(target, None)
        if lease is None:
            return
        _, handle = lease
        path = self._target_lock_path(target)
        try:
            with _PROCESS_TARGET_LEASES_LOCK:
                process_lease = _PROCESS_TARGET_LEASES.get(path)
                if process_lease is not None and process_lease[1] is handle:
                    _PROCESS_TARGET_LEASES.pop(path, None)
                if not handle.closed:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            if not handle.closed:
                handle.close()

    def _clear_target_owner(self, target: str, state: dict[str, Any]) -> None:
        state.pop(_TARGET_OWNER_FIELD, None)
        self._release_target_lease(target)

    def initialize(self) -> dict[str, Any]:
        """Create a new manifest or fail closed against an existing run."""
        with self._run_locked():
            self.run_root.mkdir(parents=True, exist_ok=True)
            existing, corrupted = self._read_json_safe(self.manifest_path)
            if corrupted:
                raise self._integrity_error(self.manifest_path, RUN_MANIFEST_SCHEMA)
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
                    _RECEIPT_CHAIN_FIELD: {},
                }
                atomic_write_json(self.manifest_path, manifest)
                return manifest
            if existing.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA:
                raise RunManifestError(
                    f"Run {self.run_root.name!r} has an unsupported manifest schema"
                )
            self._validate_receipt_chain_map(existing)
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
                    _RECEIPT_CHAIN_FIELD,
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
                    key
                    for key in set(expected) | set(actual)
                    if expected.get(key) != actual.get(key)
                )
                raise RunManifestError(
                    f"Run {self.run_root.name!r} declaration changed: {', '.join(changed)}"
                )
            self._validate_all_attempt_receipts()
            return existing

    def mark_resumed(self) -> None:
        """Record an operator/runtime resume without changing immutable run identity."""
        with self._run_locked():
            manifest = self._required_manifest()
            self._validate_all_attempt_receipts()
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
        with self._run_locked():
            manifest = self._required_manifest()
            self._validate_all_attempt_receipts()
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
        with self._run_locked():
            self._validate_attempt_receipts(target)
            state_path = self._state_path(target)
            state, corrupted = self._read_json_safe(state_path)
            if corrupted:
                raise self._integrity_error(state_path, ATTEMPT_RECEIPT_SCHEMA)
            policy_digest = policy.digest if policy is not None else None
            resolutions = prompt_resolutions or {}
            if state is None:
                if self._receipt_chain(target):
                    raise RunManifestError(
                        f"Attempt receipt chain for {target!r} exists without a durable state"
                    )
                self._acquire_target_lease(target)
                return self._start_attempt(
                    target,
                    attempt=1,
                    policy_digest=policy_digest,
                    declaration_digest=declaration_digest,
                    prompt_resolutions=resolutions,
                )
            self._validate_state(
                state,
                target=target,
                policy_digest=policy_digest,
                declaration_digest=declaration_digest,
                prompt_resolutions=resolutions,
            )
            status = state.get("status")
            if status == "running":
                attempt = int(state["attempt"])
                if state.get("side_effect_started") is True:
                    self._acquire_target_lease(
                        target,
                        state.get(_TARGET_OWNER_FIELD),
                        allow_same_process_fence=True,
                    )
                    state[_TARGET_OWNER_FIELD] = self._target_owner_token(target)
                    state["status"] = "ambiguous"
                    state["updated_at"] = iso_now()
                    self._clear_target_owner(target, state)
                    self._write_state(target, state)
                    raise AmbiguousAttemptError(self.run_root.name, target, attempt)
                already_owned = not self._acquire_target_lease(
                    target,
                    state.get(_TARGET_OWNER_FIELD),
                )
                if already_owned:
                    return {"action": "run", "state": state}
                return self._start_attempt(
                    target,
                    attempt=attempt,
                    policy_digest=policy_digest,
                    declaration_digest=declaration_digest,
                    prompt_resolutions=resolutions,
                    inherited_nodes=state.get("inherited_nodes"),
                    recovery=state.get("recovery"),
                )
            if status == "checkpoint_pending":
                self._acquire_target_lease(target, state.get(_TARGET_OWNER_FIELD))
                return self._start_attempt(
                    target,
                    attempt=int(state["attempt"]),
                    policy_digest=policy_digest,
                    declaration_digest=declaration_digest,
                    prompt_resolutions=resolutions,
                )
            if status == "retry_scheduled":
                due = datetime.fromisoformat(str(state["due_at"]))
                if utc_now() < due:
                    return {"action": "pending", "state": state}
                self._acquire_target_lease(target, state.get(_TARGET_OWNER_FIELD))
                return self._start_attempt(
                    target,
                    attempt=int(state["next_attempt"]),
                    policy_digest=policy_digest,
                    declaration_digest=declaration_digest,
                    prompt_resolutions=resolutions,
                    inherited_nodes=state.get("inherited_nodes"),
                    recovery=state.get("recovery"),
                )
            if status == "success_candidate":
                candidate = self._required_json(
                    self._target_root(target) / str(state["candidate_file"])
                )
                if state.get("candidate_sha256") != sha(candidate):
                    raise RunManifestError(
                        f"Success candidate for {target!r} failed digest validation"
                    )
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

    def schedule_recovery(
        self,
        target: str,
        *,
        from_attempt: int,
        to_attempt: int,
        recovery: dict[str, Any],
        inherited_nodes: dict[str, Any],
        recovery_receipt: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Queue a recovery attempt and optionally bind its receipt atomically.

        ``recovery_receipt`` is written under the same run lock before the new
        state snapshot is committed.  The state records its relative filename
        and canonical digest, so a missing or modified recovery record fails
        closed on the next durable read.
        """
        with self._run_locked():
            state = self._required_state(target)
            if state.get("status") != "failed" or state.get("attempt") != from_attempt:
                raise ValueError(
                    f"Target {target!r} attempt {from_attempt} is not the active terminal failure"
                )
            if (
                state.get(_RECOVERY_DECISION_FIELD) is not None
                or state.get(_RECOVERY_RECEIPT_FILE_FIELD) is not None
            ):
                raise RunManifestError(
                    f"Target {target!r} attempt {from_attempt} already has a recovery decision"
                )
            if to_attempt != from_attempt + 1:
                raise ValueError("Recovery attempts must advance by exactly one")
            self._acquire_target_lease(target, state.get(_TARGET_OWNER_FIELD))
            next_receipt = self._target_root(target) / f"attempt-{to_attempt:04d}.json"
            if next_receipt.exists():
                raise RunManifestError(
                    f"Recovery attempt receipt already exists for {target!r}: {next_receipt}"
                )
            canonical_recovery = json.loads(canonical_json(recovery))
            canonical_inherited = json.loads(canonical_json(inherited_nodes))
            recovery_receipt_path: Path | None = None
            canonical_recovery_receipt: dict[str, Any] | None = None
            if recovery_receipt is not None:
                canonical_recovery_receipt = self._canonical_object(
                    recovery_receipt,
                    label="recovery receipt",
                )
                recovery_receipt_path = self._write_recovery_receipt_locked(
                    canonical_recovery_receipt
                )
            now = iso_now()
            state.update(
                {
                    "attempt": to_attempt,
                    "status": "retry_scheduled",
                    "next_attempt": to_attempt,
                    "delay_seconds": 0.0,
                    "due_at": now,
                    "recovery": canonical_recovery,
                    "inherited_nodes": canonical_inherited,
                    "updated_at": now,
                }
            )
            if recovery_receipt_path is not None and canonical_recovery_receipt is not None:
                state[_RECOVERY_RECEIPT_FILE_FIELD] = recovery_receipt_path.relative_to(
                    self.run_root
                ).as_posix()
                state[_RECOVERY_RECEIPT_DIGEST_FIELD] = sha(canonical_recovery_receipt)
            self._clear_target_owner(target, state)
            self._write_state(target, state)
            return state

    def record_recovery_decision(
        self,
        target: str,
        *,
        from_attempt: int,
        decision: Literal["retry_not_started", "retry_after_external_check", "fail"],
        recovery: dict[str, Any],
        inherited_nodes: dict[str, Any] | None = None,
        recovery_receipt: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Commit one mutually exclusive recovery decision for a failed attempt.

        The failed-state check, decision marker, recovery receipt, and resulting
        state transition all happen under the run lock.  Retry decisions bind the
        receipt to the newly scheduled state; fail decisions bind it to the
        terminal failed state.  Callers should use this method for operator
        recovery instead of composing ``write_recovery_receipt`` with
        ``schedule_recovery``.
        """
        valid_decisions = {
            "retry_not_started",
            "retry_after_external_check",
            "fail",
        }
        if decision not in valid_decisions:
            raise ValueError(f"Unknown recovery decision: {decision!r}")
        if isinstance(from_attempt, bool) or not isinstance(from_attempt, int) or from_attempt < 1:
            raise ValueError("from_attempt must be a positive integer")

        with self._run_locked():
            state = self._required_state(target)
            if state.get("status") != "failed" or state.get("attempt") != from_attempt:
                raise ValueError(
                    f"Target {target!r} attempt {from_attempt} is not the active terminal failure"
                )
            if (
                state.get(_RECOVERY_DECISION_FIELD) is not None
                or state.get(_RECOVERY_RECEIPT_FILE_FIELD) is not None
            ):
                raise RunManifestError(
                    f"Target {target!r} attempt {from_attempt} already has a recovery decision"
                )

            canonical_recovery = self._canonical_object(recovery, label="recovery decision")
            if canonical_recovery.get("decision") not in {None, decision}:
                raise ValueError("Recovery payload decision does not match the requested decision")
            canonical_receipt = self._canonical_object(
                recovery if recovery_receipt is None else recovery_receipt,
                label="recovery receipt",
            )
            if canonical_receipt.get("decision") not in {None, decision}:
                raise ValueError("Recovery receipt decision does not match the requested decision")

            to_attempt = from_attempt if decision == "fail" else from_attempt + 1
            if decision != "fail":
                next_receipt = self._target_root(target) / f"attempt-{to_attempt:04d}.json"
                if next_receipt.exists():
                    raise RunManifestError(
                        f"Recovery attempt receipt already exists for {target!r}: {next_receipt}"
                    )

            self._acquire_target_lease(target, state.get(_TARGET_OWNER_FIELD))
            recovery_receipt_path = self._write_recovery_receipt_locked(canonical_receipt)
            state[_RECOVERY_DECISION_FIELD] = decision
            state["recovery"] = canonical_recovery
            state[_RECOVERY_RECEIPT_FILE_FIELD] = recovery_receipt_path.relative_to(
                self.run_root
            ).as_posix()
            state[_RECOVERY_RECEIPT_DIGEST_FIELD] = sha(canonical_receipt)
            now = iso_now()
            if decision == "fail":
                state["updated_at"] = now
            else:
                canonical_inherited = json.loads(canonical_json(inherited_nodes or {}))
                state.update(
                    {
                        "attempt": to_attempt,
                        "status": "retry_scheduled",
                        "next_attempt": to_attempt,
                        "delay_seconds": 0.0,
                        "due_at": now,
                        "inherited_nodes": canonical_inherited,
                        "updated_at": now,
                    }
                )
            self._clear_target_owner(target, state)
            self._write_state(target, state)
            return state

    def write_recovery_receipt(self, payload: dict[str, Any]) -> Path:
        """Write a fail-decision recovery receipt under the run lock.

        The method uses exclusive creation and never replaces an existing
        receipt, even when two decisions share the same recovery timestamp.
        Retry scheduling should use ``schedule_recovery(...,
        recovery_receipt=payload)`` so the state also stores the file binding.
        """
        with self._run_locked():
            return self._write_recovery_receipt_locked(
                self._canonical_object(payload, label="recovery receipt")
            )

    def mark_side_effect(
        self,
        target: str,
        active_effect: dict[str, Any] | None = None,
    ) -> None:
        """Persist the provider/Agent side-effect boundary before crossing it."""
        with self._run_locked():
            state = self._required_state(target)
            if state.get("status") != "running":
                raise RunManifestError(
                    f"Cannot mark side effect for {target!r} in non-running state"
                )
            self._require_target_lease(target, state)
            if active_effect is not None:
                canonical = json.loads(canonical_json(active_effect))
                if not isinstance(canonical, dict):
                    raise RunManifestError("active side effect must be a canonical object")
                state["active_effect"] = canonical
            state["side_effect_started"] = True
            state.setdefault("side_effect_started_at", iso_now())
            state["updated_at"] = iso_now()
            self._write_state(target, state)

    def mark_checkpoint(self, target: str, checkpoint: str) -> None:
        with self._run_locked():
            state = self._required_state(target)
            if state.get("status") != "running":
                raise RunManifestError(
                    f"Cannot mark checkpoint for {target!r} in non-running state"
                )
            self._require_target_lease(target, state)
            state.update(
                {
                    "status": "checkpoint_pending",
                    "checkpoint": checkpoint,
                    "updated_at": iso_now(),
                }
            )
            self._clear_target_owner(target, state)
            self._write_state(target, state)

    def save_candidate(self, target: str, candidate: dict[str, Any]) -> dict[str, Any]:
        """Persist canonical success before cache sealing or materialization."""
        with self._run_locked():
            state = self._required_state(target)
            if state.get("status") != "running":
                raise RunManifestError(f"Cannot save candidate for {target!r} in non-running state")
            self._require_target_lease(target, state)
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
            self._clear_target_owner(target, state)
            self._write_state(target, state)
            return canonical

    def mark_completed(self, target: str, *, artifact_sha256: str) -> None:
        with self._run_locked():
            state = self._required_state(target)
            if state.get("status") == "completed":
                if state.get("artifact_sha256") != artifact_sha256:
                    raise RunManifestError(
                        f"Completed artifact for {target!r} does not match the durable state"
                    )
                return
            if state.get("status") == "running":
                self._require_target_lease(target, state)
            elif state.get("status") == "success_candidate":
                self._acquire_target_lease(
                    target,
                    state.get(_TARGET_OWNER_FIELD),
                    allow_same_process_fence=True,
                )
                state[_TARGET_OWNER_FIELD] = self._target_owner_token(target)
            else:
                raise RunManifestError(
                    f"Cannot complete {target!r} in state {state.get('status')!r}"
                )
            state.update(
                {
                    "status": "completed",
                    "artifact_sha256": artifact_sha256,
                    "completed_at": iso_now(),
                    "updated_at": iso_now(),
                }
            )
            self._clear_target_owner(target, state)
            self._write_state(target, state)

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

        with self._run_locked():
            state = self._required_state(target)
            if state.get("status") != "running":
                raise RunManifestError(f"Cannot record failure for {target!r} in non-running state")
            self._require_target_lease(target, state)
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
            self._clear_target_owner(target, state)
            self._write_state(target, state)
            return {"action": action, "state": state}

    def pending_retries(self) -> list[dict[str, Any]]:
        with self._run_locked():
            return self._states_with("retry_scheduled")

    def ambiguous_attempts(self) -> list[dict[str, Any]]:
        with self._run_locked():
            return self._states_with("ambiguous")

    def resolve(
        self,
        target: str,
        *,
        attempt: int,
        action: Literal["retry", "fail"],
        reason: str,
    ) -> dict[str, Any]:
        with self._run_locked():
            if not isinstance(reason, str) or not reason.strip():
                raise ValueError("retry resolution reason must be non-empty")
            if action not in {"retry", "fail"}:
                raise ValueError("retry resolution action must be 'retry' or 'fail'")
            state = self._required_state(target)
            if (
                state.get("status") == "running"
                and state.get("side_effect_started") is True
                and state.get("attempt") == attempt
            ):
                self._acquire_target_lease(
                    target,
                    state.get(_TARGET_OWNER_FIELD),
                    allow_same_process_fence=True,
                )
                state[_TARGET_OWNER_FIELD] = self._target_owner_token(target)
                state["status"] = "ambiguous"
                state["updated_at"] = iso_now()
                self._write_state(target, state)
            elif state.get("status") == "ambiguous" and state.get("attempt") == attempt:
                self._acquire_target_lease(
                    target,
                    state.get(_TARGET_OWNER_FIELD),
                    allow_same_process_fence=True,
                )
                current_token = self._target_owner_token(target)
                if state.get(_TARGET_OWNER_FIELD) != current_token:
                    state[_TARGET_OWNER_FIELD] = current_token
                    state["updated_at"] = iso_now()
                    self._write_state(target, state)
            if state.get("status") != "ambiguous" or state.get("attempt") != attempt:
                raise ValueError(
                    f"Target {target!r} attempt {attempt} is not the active ambiguous attempt"
                )
            self._require_target_lease(target, state)
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
            self._clear_target_owner(target, state)
            self._write_state(target, state)
            return state

    def state_for(self, target: str) -> dict[str, Any] | None:
        with self._run_locked():
            self._validate_attempt_receipts(target)
            path = self._state_path(target)
            state, corrupted = self._read_json_safe(path)
            if corrupted:
                raise self._integrity_error(path, ATTEMPT_RECEIPT_SCHEMA)
            if state is not None:
                self._validate_state_binding(target, state)
            elif self._receipt_chain(target) or any(
                self._target_root(target).glob("attempt-*.json")
            ):
                raise RunManifestError(
                    f"Attempt receipts for {target!r} exist without a durable state"
                )
            return state

    def _start_attempt(
        self,
        target: str,
        *,
        attempt: int,
        policy_digest: str | None,
        declaration_digest: str,
        prompt_resolutions: dict[str, Any],
        inherited_nodes: dict[str, Any] | None = None,
        recovery: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = iso_now()
        state = {
            "attempt_receipt_schema": ATTEMPT_RECEIPT_SCHEMA,
            "target": target,
            "target_digest": sha(target),
            "attempt": attempt,
            "status": "running",
            "side_effect_started": False,
            _TARGET_OWNER_FIELD: self._target_owner_token(target),
            "policy_digest": policy_digest,
            "declaration_digest": declaration_digest,
            "prompt_resolutions": json.loads(canonical_json(prompt_resolutions)),
            "started_at": now,
            "updated_at": now,
        }
        if inherited_nodes:
            state["inherited_nodes"] = json.loads(canonical_json(inherited_nodes))
        if recovery:
            state["recovery"] = json.loads(canonical_json(recovery))
        target_root = self._target_root(target)
        target_root.mkdir(parents=True, exist_ok=True)
        self._write_state(target, state)
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
        self._validate_state_binding(target, state)
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
            state, corrupted = self._read_json_safe(path)
            if corrupted:
                raise self._integrity_error(path, ATTEMPT_RECEIPT_SCHEMA)
            if state is None:
                continue
            target = state.get("target")
            if not isinstance(target, str) or path.parent.name != sha(target):
                raise StateIntegrityError(
                    path,
                    ATTEMPT_RECEIPT_SCHEMA,
                    "state target path binding mismatch",
                )
            self._validate_attempt_receipts(target)
            self._validate_state_binding(target, state)
            if state.get("status") == status:
                found.append(state)
        return found

    def _target_root(self, target: str) -> Path:
        return self.run_root / "attempts" / sha(target)

    def _state_path(self, target: str) -> Path:
        return self._target_root(target) / "state.json"

    @staticmethod
    def _is_sha256(value: Any) -> bool:
        return (
            isinstance(value, str)
            and len(value) == 64
            and value == value.lower()
            and all(character in "0123456789abcdef" for character in value)
        )

    @classmethod
    def _validate_receipt_chain(
        cls,
        target_digest: str,
        chain: Any,
        path: Path,
    ) -> None:
        if not isinstance(chain, list):
            raise RunManifestError(f"Receipt chain at {path} must be a JSON list")
        if not cls._is_sha256(target_digest):
            raise RunManifestError(f"Receipt chain target key at {path} is not a SHA-256 digest")
        for index, entry in enumerate(chain, start=1):
            if not isinstance(entry, dict):
                raise RunManifestError(f"Receipt chain entry {index} at {path} is not an object")
            if entry.get("target_digest") != target_digest:
                raise RunManifestError(
                    f"Receipt chain entry {index} at {path} has a target binding mismatch"
                )
            if (
                type(entry.get(_RECEIPT_SEQUENCE_FIELD)) is not int
                or entry.get(_RECEIPT_SEQUENCE_FIELD) != index
            ):
                raise RunManifestError(
                    f"Receipt chain entry {index} at {path} has an invalid sequence"
                )
            attempt = entry.get("attempt")
            if type(attempt) is not int or attempt < 1:
                raise RunManifestError(
                    f"Receipt chain entry {index} at {path} has an invalid attempt"
                )
            state_digest = entry.get(_STATE_DIGEST_FIELD)
            if not cls._is_sha256(state_digest):
                raise RunManifestError(
                    f"Receipt chain entry {index} at {path} has an invalid state digest"
                )
            previous = entry.get(_PREVIOUS_RECEIPT_DIGEST_FIELD)
            if index == 1:
                if previous is not None:
                    raise RunManifestError(
                        f"Receipt chain entry {index} at {path} has an unexpected predecessor"
                    )
            elif previous != chain[index - 2].get(_STATE_DIGEST_FIELD):
                raise RunManifestError(
                    f"Receipt chain entry {index} at {path} is not linked to its predecessor"
                )

    @classmethod
    def _validate_receipt_chain_map(cls, manifest: dict[str, Any]) -> None:
        chains = manifest.get(_RECEIPT_CHAIN_FIELD)
        if not isinstance(chains, dict):
            raise RunManifestError(f"Run manifest is missing the {_RECEIPT_CHAIN_FIELD} anchor")
        manifest_path = Path("_run.json")
        for target_digest, chain in chains.items():
            cls._validate_receipt_chain(target_digest, chain, manifest_path)

    def _required_manifest(self) -> dict[str, Any]:
        manifest, corrupted = self._read_json_safe(self.manifest_path)
        if corrupted:
            raise self._integrity_error(self.manifest_path, RUN_MANIFEST_SCHEMA)
        if manifest is None:
            raise RunManifestError(f"Missing or invalid run manifest: {self.manifest_path}")
        if manifest.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA:
            raise RunManifestError(f"Run {self.run_root.name!r} has an unsupported manifest schema")
        self._validate_receipt_chain_map(manifest)
        return manifest

    def _receipt_chain(
        self,
        target: str,
        *,
        manifest: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if manifest is None:
            manifest = self._required_manifest()
        target_digest = sha(target)
        chain = manifest[_RECEIPT_CHAIN_FIELD].get(target_digest, [])
        self._validate_receipt_chain(target_digest, chain, self.manifest_path)
        return chain

    @staticmethod
    def _state_digest(state: dict[str, Any]) -> str:
        payload = {key: value for key, value in state.items() if key != _STATE_DIGEST_FIELD}
        return sha(payload)

    def _write_state(self, target: str, state: dict[str, Any]) -> None:
        with self._run_locked():
            self._write_state_locked(target, state)

    def _write_state_locked(self, target: str, state: dict[str, Any]) -> None:
        """Append a state snapshot and bind it to the run-manifest receipt chain."""
        target_digest = sha(target)
        if state.get("target") != target or state.get("target_digest") != target_digest:
            raise RunManifestError(f"Attempt state for {target!r} has an invalid target binding")
        attempt = state.get("attempt")
        if type(attempt) is not int or attempt < 1:
            raise RunManifestError(f"Attempt state for {target!r} has invalid attempt")

        manifest = self._required_manifest()
        chains = manifest[_RECEIPT_CHAIN_FIELD]
        chain = list(self._receipt_chain(target, manifest=manifest))
        current_fields = {
            _STATE_DIGEST_FIELD: state.get(_STATE_DIGEST_FIELD),
            _RECEIPT_SEQUENCE_FIELD: state.get(_RECEIPT_SEQUENCE_FIELD),
            _PREVIOUS_RECEIPT_DIGEST_FIELD: state.get(_PREVIOUS_RECEIPT_DIGEST_FIELD),
        }
        present = [field in state for field in current_fields]
        if any(present) and not all(present):
            raise StateIntegrityError(
                self._state_path(target),
                ATTEMPT_RECEIPT_SCHEMA,
                "state receipt-chain fields are incomplete",
            )
        if chain and not any(present):
            # A fresh state is allowed here for a new retry attempt or for the
            # documented restart of an attempt that never crossed its effect
            # boundary.  A loaded state always carries the previous chain head.
            pass
        elif chain and (
            current_fields[_RECEIPT_SEQUENCE_FIELD] != len(chain)
            or current_fields[_STATE_DIGEST_FIELD] != chain[-1][_STATE_DIGEST_FIELD]
        ):
            raise StateIntegrityError(
                self._state_path(target),
                ATTEMPT_RECEIPT_SCHEMA,
                "state is not descended from the manifest receipt-chain head",
            )

        sequence = len(chain) + 1
        previous_digest = chain[-1][_STATE_DIGEST_FIELD] if chain else None
        state.pop(_STATE_DIGEST_FIELD, None)
        state[_RECEIPT_SEQUENCE_FIELD] = sequence
        state[_PREVIOUS_RECEIPT_DIGEST_FIELD] = previous_digest
        state[_STATE_DIGEST_FIELD] = self._state_digest(state)
        entry = {
            "target_digest": target_digest,
            _RECEIPT_SEQUENCE_FIELD: sequence,
            "attempt": attempt,
            _PREVIOUS_RECEIPT_DIGEST_FIELD: previous_digest,
            _STATE_DIGEST_FIELD: state[_STATE_DIGEST_FIELD],
        }
        self._validate_receipt_chain(target_digest, [*chain, entry], self.manifest_path)
        state.pop(_STATE_DIGEST_FIELD, None)
        state[_STATE_DIGEST_FIELD] = entry[_STATE_DIGEST_FIELD]
        atomic_write_json(self._state_path(target), state)
        self._write_receipt(target, attempt, state)
        chains[target_digest] = [*chain, entry]
        atomic_write_json(self.manifest_path, manifest)

    def _write_receipt(self, target: str, attempt: int, state: dict[str, Any]) -> None:
        if state.get(_STATE_DIGEST_FIELD) != self._state_digest(state):
            raise RunManifestError(f"Attempt state for {target!r} is not content-bound")
        atomic_write_json(
            self._target_root(target) / f"attempt-{attempt:04d}.json",
            state,
        )

    def _validate_state_binding(self, target: str, state: dict[str, Any]) -> None:
        """Validate state, its current receipt twin, and the manifest chain head."""
        path = self._state_path(target)
        if state.get("attempt_receipt_schema") != ATTEMPT_RECEIPT_SCHEMA:
            raise RunManifestError(f"Attempt state for {target!r} has unsupported schema")
        if state.get("target") != target or state.get("target_digest") != sha(target):
            raise StateIntegrityError(path, ATTEMPT_RECEIPT_SCHEMA, "target binding mismatch")
        attempt = state.get("attempt")
        if type(attempt) is not int or attempt < 1:
            raise StateIntegrityError(path, ATTEMPT_RECEIPT_SCHEMA, "attempt binding is invalid")
        if state.get("status") == "running" and not self._is_owner_token(
            state.get(_TARGET_OWNER_FIELD)
        ):
            raise StateIntegrityError(
                path,
                ATTEMPT_RECEIPT_SCHEMA,
                "active state is missing or has an invalid target owner token",
            )
        if state.get(_STATE_DIGEST_FIELD) != self._state_digest(state):
            raise StateIntegrityError(
                path,
                ATTEMPT_RECEIPT_SCHEMA,
                "state content digest does not match the state payload",
            )
        chain = self._receipt_chain(target)
        if not chain:
            raise StateIntegrityError(
                path,
                ATTEMPT_RECEIPT_SCHEMA,
                "state has no manifest receipt-chain anchor",
            )
        head = chain[-1]
        if (
            state.get(_RECEIPT_SEQUENCE_FIELD) != head[_RECEIPT_SEQUENCE_FIELD]
            or state.get(_PREVIOUS_RECEIPT_DIGEST_FIELD) != head[_PREVIOUS_RECEIPT_DIGEST_FIELD]
            or state.get(_STATE_DIGEST_FIELD) != head[_STATE_DIGEST_FIELD]
            or state.get("attempt") != head["attempt"]
        ):
            raise StateIntegrityError(
                path,
                ATTEMPT_RECEIPT_SCHEMA,
                "state does not match the manifest receipt-chain head",
            )
        receipt_path = self._target_root(target) / f"attempt-{attempt:04d}.json"
        receipt, corrupted = self._read_json_safe(receipt_path)
        if corrupted:
            raise self._integrity_error(receipt_path, ATTEMPT_RECEIPT_SCHEMA)
        self._validate_recovery_receipt_binding(state, path)
        if receipt is None:
            if state.get("side_effect_started") is True:
                raise AmbiguousAttemptError(
                    self.run_root.name,
                    target,
                    attempt,
                )
            return
        self._validate_receipt_record(receipt_path, receipt, target, attempt)
        if receipt != state:
            raise StateIntegrityError(
                receipt_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "current state and attempt receipt are not identical",
            )

    def _validate_receipt_record(
        self,
        path: Path,
        receipt: dict[str, Any],
        target: str,
        expected_attempt: int,
    ) -> None:
        if receipt.get("attempt_receipt_schema") != ATTEMPT_RECEIPT_SCHEMA:
            raise RunManifestError(f"Attempt receipt {path} has unsupported schema")
        if receipt.get("target") != target or receipt.get("target_digest") != sha(target):
            raise StateIntegrityError(path, ATTEMPT_RECEIPT_SCHEMA, "target binding mismatch")
        if type(receipt.get("attempt")) is not int or receipt.get("attempt") != expected_attempt:
            raise StateIntegrityError(path, ATTEMPT_RECEIPT_SCHEMA, "attempt binding mismatch")
        if (
            type(receipt.get(_RECEIPT_SEQUENCE_FIELD)) is not int
            or receipt.get(_RECEIPT_SEQUENCE_FIELD) < 1
        ):
            raise StateIntegrityError(path, ATTEMPT_RECEIPT_SCHEMA, "receipt sequence is invalid")
        if not self._is_sha256(receipt.get(_STATE_DIGEST_FIELD)):
            raise StateIntegrityError(
                path,
                ATTEMPT_RECEIPT_SCHEMA,
                "receipt state digest is invalid",
            )
        if receipt.get(_STATE_DIGEST_FIELD) != self._state_digest(receipt):
            raise StateIntegrityError(
                path,
                ATTEMPT_RECEIPT_SCHEMA,
                "receipt content digest does not match the receipt payload",
            )

    @staticmethod
    def _is_owner_token(value: Any) -> bool:
        return isinstance(value, str) and bool(value.strip())

    @staticmethod
    def _canonical_object(value: Any, *, label: str) -> dict[str, Any]:
        canonical = json.loads(canonical_json(value))
        if not isinstance(canonical, dict):
            raise RunManifestError(f"{label} must be a canonical object")
        return canonical

    def _validate_recovery_receipt_binding(
        self,
        state: dict[str, Any],
        state_path: Path,
    ) -> None:
        file_name = state.get(_RECOVERY_RECEIPT_FILE_FIELD)
        digest = state.get(_RECOVERY_RECEIPT_DIGEST_FIELD)
        if file_name is None and digest is None:
            return
        if not isinstance(file_name, str) or not file_name or not self._is_sha256(digest):
            raise StateIntegrityError(
                state_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "recovery receipt binding is incomplete or invalid",
            )
        relative = Path(file_name)
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or relative.parent != Path(".")
            or not relative.name.startswith("recovery-")
            or relative.suffix != ".json"
        ):
            raise StateIntegrityError(
                state_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "recovery receipt path escapes the run directory",
            )
        receipt_path = self.run_root / relative
        receipt, corrupted = self._read_json_safe(receipt_path)
        if corrupted:
            raise self._integrity_error(receipt_path, ATTEMPT_RECEIPT_SCHEMA)
        if receipt is None:
            raise StateIntegrityError(
                receipt_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "bound recovery receipt is missing",
            )
        if sha(receipt) != digest:
            raise StateIntegrityError(
                receipt_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "bound recovery receipt digest does not match the state",
            )

    def _write_recovery_receipt_locked(self, payload: dict[str, Any]) -> Path:
        recovery_time = payload.get("recovery_time")
        if (
            not isinstance(recovery_time, str)
            or not recovery_time
            or "/" in recovery_time
            or "\\" in recovery_time
        ):
            raise RunManifestError("recovery receipt must contain a recovery_time")
        self.run_root.mkdir(parents=True, exist_ok=True)
        stem = f"recovery-{recovery_time}"
        suffix = 0
        while True:
            suffix_text = "" if suffix == 0 else f"-{suffix}"
            path = self.run_root / f"{stem}{suffix_text}.json"
            try:
                with path.open("x", encoding="utf-8") as handle:
                    handle.write(canonical_json(payload))
                    handle.flush()
                    os.fsync(handle.fileno())
            except FileExistsError:
                suffix += 1
                continue
            return path

    def _validate_attempt_receipts(self, target: str) -> None:
        """Reject a present torn receipt while allowing an absent receipt."""
        manifest = self._required_manifest()
        chain = self._receipt_chain(target, manifest=manifest)
        last_by_attempt = {entry["attempt"]: entry for entry in chain}
        receipt_attempts: set[int] = set()
        for path in sorted(self._target_root(target).glob("attempt-*.json")):
            receipt, corrupted = self._read_json_safe(path)
            if corrupted:
                raise self._integrity_error(path, ATTEMPT_RECEIPT_SCHEMA)
            if receipt is None:
                continue
            prefix, separator, attempt_text = path.stem.partition("-")
            if prefix != "attempt" or separator != "-" or not attempt_text.isdigit():
                raise StateIntegrityError(
                    path,
                    ATTEMPT_RECEIPT_SCHEMA,
                    "attempt receipt filename is invalid",
                )
            attempt = int(attempt_text)
            self._validate_receipt_record(path, receipt, target, attempt)
            receipt_attempts.add(attempt)
            anchor = last_by_attempt.get(attempt)
            if anchor is None or (
                receipt.get(_RECEIPT_SEQUENCE_FIELD) != anchor[_RECEIPT_SEQUENCE_FIELD]
                or receipt.get(_STATE_DIGEST_FIELD) != anchor[_STATE_DIGEST_FIELD]
                or receipt.get(_PREVIOUS_RECEIPT_DIGEST_FIELD)
                != anchor[_PREVIOUS_RECEIPT_DIGEST_FIELD]
            ):
                raise StateIntegrityError(
                    path,
                    ATTEMPT_RECEIPT_SCHEMA,
                    "attempt receipt is not anchored in the manifest receipt chain",
                )
        if chain:
            current_attempt = chain[-1]["attempt"]
            for attempt in last_by_attempt:
                if attempt not in receipt_attempts and attempt != current_attempt:
                    raise StateIntegrityError(
                        self._target_root(target) / f"attempt-{attempt:04d}.json",
                        ATTEMPT_RECEIPT_SCHEMA,
                        "historical attempt receipt is missing from the receipt chain",
                    )

    def _validate_all_attempt_receipts(self) -> None:
        """Validate every target before mutating run-level durable state."""
        attempts_root = self.run_root / "attempts"
        if not attempts_root.is_dir():
            return
        for target_root in sorted(path for path in attempts_root.iterdir() if path.is_dir()):
            state_path = target_root / "state.json"
            state, corrupted = self._read_json_safe(state_path)
            if corrupted:
                raise self._integrity_error(state_path, ATTEMPT_RECEIPT_SCHEMA)
            if state is not None:
                target = state.get("target")
                if not isinstance(target, str) or target_root.name != sha(target):
                    raise StateIntegrityError(
                        state_path,
                        ATTEMPT_RECEIPT_SCHEMA,
                        "state target path binding mismatch",
                    )
                self._validate_attempt_receipts(target)
                self._validate_state_binding(target, state)
                continue

            receipt_paths = sorted(target_root.glob("attempt-*.json"))
            if not receipt_paths:
                continue
            receipt, receipt_corrupted = self._read_json_safe(receipt_paths[0])
            if receipt_corrupted or receipt is None:
                raise self._integrity_error(receipt_paths[0], ATTEMPT_RECEIPT_SCHEMA)
            target = receipt.get("target")
            if not isinstance(target, str) or target_root.name != sha(target):
                raise StateIntegrityError(
                    receipt_paths[0],
                    ATTEMPT_RECEIPT_SCHEMA,
                    "attempt receipt target path binding mismatch",
                )
            self._validate_attempt_receipts(target)
            raise RunManifestError(f"Attempt receipts for {target!r} exist without a durable state")

    @staticmethod
    def _read_json_safe(path: Path) -> tuple[dict[str, Any] | None, bool]:
        """Return ``(data, corrupted)`` while keeping missing distinct from torn JSON."""
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None, False
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None, True
        return (value, False) if isinstance(value, dict) else (None, True)

    @staticmethod
    def _parse_error(path: Path) -> BaseException | str:
        """Recover a useful parse explanation for a failed safe read."""
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            return error
        return TypeError(f"expected a JSON object, got {type(value).__name__}")

    @classmethod
    def _integrity_error(cls, path: Path, expected_schema: int) -> StateIntegrityError:
        return StateIntegrityError(path, expected_schema, cls._parse_error(path))

    def _required_json(self, path: Path) -> dict[str, Any]:
        value, corrupted = self._read_json_safe(path)
        if corrupted:
            raise self._integrity_error(path, ATTEMPT_RECEIPT_SCHEMA)
        if value is None:
            raise RunManifestError(f"Missing or invalid durable run state: {path}")
        if path.name == "state.json":
            target = value.get("target")
            if not isinstance(target, str) or path.parent.name != sha(target):
                raise StateIntegrityError(
                    path,
                    ATTEMPT_RECEIPT_SCHEMA,
                    "state target path binding mismatch",
                )
            self._validate_state_binding(target, value)
        return value

    def _required_state(self, target: str) -> dict[str, Any]:
        """Read one target only after its complete receipt history is trusted."""
        self._validate_attempt_receipts(target)
        return self._required_json(self._state_path(target))
