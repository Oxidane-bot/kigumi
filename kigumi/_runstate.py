"""Private durable run manifest and attempt receipt storage."""

from __future__ import annotations

import errno
import fcntl
import json
import os
import stat
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from ._safe_io import SecureDirectory, _open_regular_file_at
from .artifacts import atomic_write_json, canonical_json, sha
from .failures import canonical_failure
from .retry import AmbiguousAttemptError, RetryPolicy

RUN_MANIFEST_SCHEMA = 2
RUN_SIDECAR_SCHEMA = 2
FAILURE_SCHEMA = 2
SUCCESS_CANDIDATE_SCHEMA = 2
ATTEMPT_RECEIPT_SCHEMA = 2
_RUN_STATUSES = frozenset(
    {
        "running",
        "pending_retry",
        "ambiguous",
        "checkpoint_pending",
        "completed",
        "failed",
    }
)
_ATTEMPT_STATUSES = frozenset(
    {
        "running",
        "success_candidate",
        "retry_scheduled",
        "ambiguous",
        "checkpoint_pending",
        "completed",
        "failed",
    }
)
_STATE_DIGEST_FIELD = "state_sha256"
_RECEIPT_SEQUENCE_FIELD = "receipt_sequence"
_PREVIOUS_RECEIPT_DIGEST_FIELD = "previous_receipt_sha256"
_RECEIPT_CHAIN_FIELD = "attempt_receipt_chains"
_TARGET_OWNER_FIELD = "target_owner_token"
_RECOVERY_DECISION_FIELD = "recovery_decision"
_RECOVERY_RECEIPT_FILE_FIELD = "recovery_receipt_file"
_RECOVERY_RECEIPT_DIGEST_FIELD = "recovery_receipt_sha256"
_RECOVERY_DECISION_LEDGER_FIELD = "recovery_decisions"
_RECOVERY_DECISION_LEDGER_DIGEST_FIELD = "recovery_decisions_sha256"
_MANIFEST_GENERATION_FIELD = "manifest_generation"
_SIDECAR_DIGEST_FIELD = "sidecar_sha256"
_PROCESS_TARGET_LEASES: dict[Path, tuple[str, Any]] = {}
_PROCESS_TARGET_LEASES_LOCK = threading.RLock()


def _open_lock_file(path: Path, label: str) -> Any:
    """Open one lock file without following its parent or final entry."""
    path = Path(path)
    try:
        with SecureDirectory(path.parent, create=True) as directory:
            try:
                existing = directory.stat(path.name)
            except FileNotFoundError:
                existing = None
            if existing is not None and stat.S_ISLNK(existing.st_mode):
                raise RunManifestError(f"{label} must not be a symlink: {path}")
            if existing is not None and not stat.S_ISREG(existing.st_mode):
                raise RunManifestError(f"{label} must reference a regular file: {path}")

            flags = (
                os.O_RDWR
                | os.O_CREAT
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0)
            )
            try:
                descriptor = os.open(path.name, flags, 0o600, dir_fd=directory.fd)
            except OSError as error:
                if error.errno == errno.ELOOP:
                    raise RunManifestError(f"{label} must not be a symlink: {path}") from error
                raise RunManifestError(f"{label} could not be opened safely: {path}") from error
            try:
                opened = os.fstat(descriptor)
                if not stat.S_ISREG(opened.st_mode):
                    raise RunManifestError(f"{label} must reference a regular file: {path}")
                return os.fdopen(descriptor, "r+b", closefd=True)
            except BaseException:
                os.close(descriptor)
                raise
    except RunManifestError:
        raise
    except (OSError, ValueError) as error:
        raise RunManifestError(f"{label} could not be opened safely: {path}") from error


@dataclass(frozen=True)
class DurableRunSnapshot:
    """One validated read of a current run's durable evidence."""

    manifest: dict[str, Any]
    states: tuple[dict[str, Any], ...]
    candidates: dict[str, dict[str, Any]]
    materializations: dict[str, dict[str, dict[str, Any]]]
    strict: bool


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


def validate_run_path(run_root: Path) -> Path:
    """Reject symlinked path components before any durable run access.

    A run path is an ownership boundary.  Resolving it before validation would
    make a symlinked ``runs`` root or run directory look like an ordinary
    directory and would let durable writers operate in an external tree.
    Missing trailing components are allowed for a new run; every existing
    component is inspected with ``lstat`` so the check never follows a link.
    """
    path = Path(run_root)
    absolute = Path(os.path.abspath(path))
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        try:
            info = current.lstat()
        except FileNotFoundError:
            break
        except OSError as error:
            raise RunManifestError(
                f"Durable run path ownership cannot be checked at {current}: {error}"
            ) from error
        if stat.S_ISLNK(info.st_mode):
            raise RunManifestError(f"Durable run path must not contain a symlink: {current}")
    return path


class AttemptStore:
    """Own one 0.7 run's immutable declaration and mutable attempt receipts."""

    def __init__(self, run_root: Path, manifest_identity: dict[str, Any]) -> None:
        self.run_root = Path(run_root)
        validate_run_path(self.run_root)
        self.manifest_path = self.run_root / "_run.json"
        self.identity = json.loads(canonical_json(manifest_identity))
        self._receipt_chain_lock = threading.RLock()
        self._run_lock_local = threading.local()
        absolute_root = Path(os.path.abspath(self.run_root))
        lock_name = f".kigumi-run-{sha(str(absolute_root))}.lock"
        self._run_lock_path = absolute_root.parent / lock_name
        self._target_leases: dict[str, tuple[str, Any]] = {}
        self._manifest_generation: int | None = None
        self._fence_manifest_generation = False
        self._terminal_status_admission_generation: int | None = None
        self._state_mutated_since_status = False

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
        validate_run_path(self.run_root)
        with self._receipt_chain_lock:
            depth = getattr(self._run_lock_local, "depth", 0)
            if depth:
                self._run_lock_local.depth = depth + 1
                try:
                    yield
                finally:
                    self._run_lock_local.depth = depth
                return

            with _open_lock_file(self._run_lock_path, "Run lock") as handle:
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
        with _PROCESS_TARGET_LEASES_LOCK:
            process_lease = _PROCESS_TARGET_LEASES.get(path)
            if process_lease is not None and allow_same_process_fence:
                _, old_handle = process_lease
                _PROCESS_TARGET_LEASES.pop(path, None)
                if not old_handle.closed:
                    fcntl.flock(old_handle.fileno(), fcntl.LOCK_UN)
                    old_handle.close()
            handle = _open_lock_file(path, f"Target {target!r} lease")
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

    @staticmethod
    def _manifest_generation_value(manifest: dict[str, Any]) -> int:
        value = manifest.get(_MANIFEST_GENERATION_FIELD)
        if value is None:
            raise StateIntegrityError(
                Path("_run.json"),
                RUN_MANIFEST_SCHEMA,
                "manifest generation is missing",
            )
        if type(value) is not int or value < 0:
            raise StateIntegrityError(
                Path("_run.json"),
                RUN_MANIFEST_SCHEMA,
                "manifest generation is invalid",
            )
        return value

    def _commit_manifest(
        self,
        manifest: dict[str, Any],
        *,
        advance_generation: bool = True,
    ) -> None:
        """Commit one manifest mutation with optional status-generation fencing."""
        current = self._manifest_generation_value(manifest)
        if (
            advance_generation
            and self._fence_manifest_generation
            and self._manifest_generation is not None
            and current != self._manifest_generation
        ):
            raise RunManifestError(
                f"Run {self.run_root.name!r} has a stale manifest generation "
                f"(expected {self._manifest_generation}, found {current})"
            )
        next_generation = current + 1 if advance_generation else current
        manifest[_MANIFEST_GENERATION_FIELD] = next_generation
        atomic_write_json(self.manifest_path, manifest)
        self._manifest_generation = next_generation
        if advance_generation:
            self._terminal_status_admission_generation = None
            self._state_mutated_since_status = False

    def _assert_manifest_generation(self, manifest: dict[str, Any]) -> None:
        current = self._manifest_generation_value(manifest)
        if (
            self._fence_manifest_generation
            and self._manifest_generation is not None
            and current != self._manifest_generation
        ):
            raise RunManifestError(
                f"Run {self.run_root.name!r} has a stale manifest generation "
                f"(expected {self._manifest_generation}, found {current})"
            )

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
                    _MANIFEST_GENERATION_FIELD: 0,
                    "created_at": now,
                    "updated_at": now,
                    _RECEIPT_CHAIN_FIELD: {},
                    _RECOVERY_DECISION_LEDGER_FIELD: {},
                    _RECOVERY_DECISION_LEDGER_DIGEST_FIELD: sha({}),
                }
                atomic_write_json(self.manifest_path, manifest)
                self._manifest_generation = 0
                self._fence_manifest_generation = True
                return manifest
            if existing.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA:
                raise RunManifestError(
                    f"Run {self.run_root.name!r} has an unsupported manifest schema"
                )
            self._validate_manifest(existing)
            self._manifest_generation = self._manifest_generation_value(existing)
            self._fence_manifest_generation = True
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
                    _MANIFEST_GENERATION_FIELD,
                    _RECEIPT_CHAIN_FIELD,
                    _RECOVERY_DECISION_LEDGER_FIELD,
                    _RECOVERY_DECISION_LEDGER_DIGEST_FIELD,
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
            states = self._validate_all_attempt_receipts()
            self._validate_run_materializations(states, manifest=existing)
            return existing

    def mark_resumed(self) -> None:
        """Record an operator/runtime resume without changing immutable run identity."""
        with self._run_locked():
            manifest = self._required_manifest()
            self._validate_all_attempt_receipts()
            manifest["resume_count"] = int(manifest.get("resume_count", 0)) + 1
            manifest["last_resumed_at"] = iso_now()
            manifest["updated_at"] = manifest["last_resumed_at"]
            self._commit_manifest(manifest, advance_generation=False)

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
            self._assert_manifest_generation(manifest)
            states = self._validate_all_attempt_receipts()
            self._validate_manifest_status_update(manifest, status, states)
            if manifest.get("status") == "running" and status == "running":
                self._state_mutated_since_status = False
                return
            if manifest.get("status") == "completed" and status == "running":
                return
            manifest["status"] = status
            manifest["updated_at"] = iso_now()
            manifest["pending_retries"] = pending_retries or []
            manifest["ambiguous_attempts"] = ambiguous_attempts or []
            if failure is not None:
                manifest["failure"] = failure
            elif status != "failed":
                manifest.pop("failure", None)
            self._commit_manifest(manifest)

    def _validate_manifest_status_update(
        self,
        manifest: dict[str, Any],
        status: str,
        states: list[dict[str, Any]],
    ) -> None:
        if status not in _RUN_STATUSES:
            raise ValueError(f"Unknown run status: {status!r}")

        current = manifest.get("status")
        state_statuses = [state.get("status") for state in states]
        if current == "completed" and status == "running":
            # ``Dag.resume`` still enters through the historical running update
            # before discovering that every target is already materialized.  A
            # completed terminal record remains authoritative during that no-op
            # admission; it must never be reopened by a stale executor.
            self._terminal_status_admission_generation = self._manifest_generation_value(manifest)
            self._state_mutated_since_status = False
            return
        if current == "completed" and status != "completed":
            raise RunManifestError(
                f"Run {self.run_root.name!r} is terminally completed; "
                "a stale status update is fenced"
            )

        if status == "completed" and any(
            value
            in {
                "running",
                "success_candidate",
                "retry_scheduled",
                "ambiguous",
                "failed",
                "checkpoint_pending",
            }
            for value in state_statuses
        ):
            raise RunManifestError(
                f"Run {self.run_root.name!r} cannot be marked completed with active attempts"
            )
        if status == "pending_retry" and "retry_scheduled" not in state_statuses:
            raise RunManifestError(
                f"Run {self.run_root.name!r} has no durable pending retry to publish"
            )
        if (
            status == "failed"
            and state_statuses
            and all(value == "completed" for value in state_statuses)
            and not self._state_mutated_since_status
            and self._terminal_status_admission_generation
            != self._manifest_generation_value(manifest)
        ):
            raise RunManifestError(
                f"Run {self.run_root.name!r} has no durable terminal failure to publish"
            )
        if status == "failed" and "running" in state_statuses and "failed" not in state_statuses:
            raise RunManifestError(
                f"Run {self.run_root.name!r} has no durable terminal failure to publish"
            )
        if status == "ambiguous" and "ambiguous" not in state_statuses:
            raise RunManifestError(
                f"Run {self.run_root.name!r} has no durable ambiguous attempt to publish"
            )
        if status == "checkpoint_pending" and "checkpoint_pending" not in state_statuses:
            raise RunManifestError(
                f"Run {self.run_root.name!r} has no durable checkpoint to publish"
            )

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
            states = self._validate_all_attempt_receipts()
            self._validate_run_materializations(states, reject_orphan_target=target)
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
                artifact_path = self._target_artifact_path(target)
                self._validate_materialization_pair(
                    target,
                    Path(f"{artifact_path}.meta.json"),
                )
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
            manifest = self._required_failed_manifest()
            if any(
                entry.get("from_attempt") == from_attempt
                for entry in self._recovery_decisions_for(target, manifest=manifest)
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
                decision = canonical_recovery_receipt.get("decision")
                if decision not in {"retry_not_started", "retry_after_external_check"}:
                    raise RunManifestError(
                        "scheduled recovery receipt must contain a retry decision"
                    )
                if canonical_recovery.get("decision") not in {None, decision}:
                    raise ValueError(
                        "Recovery payload decision does not match the requested decision"
                    )
                recovery_receipt_path = self._write_recovery_receipt_locked(
                    canonical_recovery_receipt
                )
                self._append_recovery_decision_locked(
                    manifest,
                    target=target,
                    from_attempt=from_attempt,
                    to_attempt=to_attempt,
                    decision=decision,
                    receipt_path=recovery_receipt_path,
                    receipt_digest=sha(canonical_recovery_receipt),
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
                state[_RECOVERY_DECISION_FIELD] = decision
                state[_RECOVERY_RECEIPT_FILE_FIELD] = recovery_receipt_path.relative_to(
                    self.run_root
                ).as_posix()
                state[_RECOVERY_RECEIPT_DIGEST_FIELD] = sha(canonical_recovery_receipt)
            self._clear_target_owner(target, state)
            self._write_state(
                target,
                state,
                manifest=manifest if recovery_receipt_path is not None else None,
            )
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

            manifest = self._required_failed_manifest()
            if any(
                entry.get("from_attempt") == from_attempt
                for entry in self._recovery_decisions_for(target, manifest=manifest)
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
            recovery_receipt_digest = sha(canonical_receipt)
            self._append_recovery_decision_locked(
                manifest,
                target=target,
                from_attempt=from_attempt,
                to_attempt=to_attempt,
                decision=decision,
                receipt_path=recovery_receipt_path,
                receipt_digest=recovery_receipt_digest,
            )
            now = iso_now()
            if decision == "fail":
                self._clear_target_owner(target, state)
                manifest["updated_at"] = now
                self._commit_manifest(manifest, advance_generation=False)
                return state
            else:
                canonical_inherited = json.loads(canonical_json(inherited_nodes or {}))
                state.update(
                    {
                        _RECOVERY_DECISION_FIELD: decision,
                        "recovery": canonical_recovery,
                        _RECOVERY_RECEIPT_FILE_FIELD: recovery_receipt_path.relative_to(
                            self.run_root
                        ).as_posix(),
                        _RECOVERY_RECEIPT_DIGEST_FIELD: recovery_receipt_digest,
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
            self._write_state(target, state, manifest=manifest)
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
                self._validate_active_effect(self._state_path(target), canonical)
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
            candidate = self._validate_candidate_binding(target, state)
            if candidate is None:
                raise StateIntegrityError(
                    self._state_path(target),
                    ATTEMPT_RECEIPT_SCHEMA,
                    "completed state is missing its success candidate",
                )
            sidecar_path = Path(f"{self._target_artifact_path(target)}.meta.json")
            metadata, corrupted = self._read_json_safe(sidecar_path)
            if corrupted:
                raise self._integrity_error(sidecar_path, RUN_SIDECAR_SCHEMA)
            if metadata is None:
                raise StateIntegrityError(
                    sidecar_path,
                    RUN_SIDECAR_SCHEMA,
                    "completed state is missing its run sidecar",
                )
            artifact, metadata = self._validate_materialization_pair(target, sidecar_path)
            if sha(artifact) != artifact_sha256:
                raise RunManifestError(
                    f"Completed artifact for {target!r} does not match its run artifact"
                )
            self._validate_sidecar_candidate_binding(
                target,
                state,
                candidate,
                metadata,
            )
            state.update(
                {
                    "status": "completed",
                    "artifact_sha256": artifact_sha256,
                    "completed_at": iso_now(),
                    "updated_at": iso_now(),
                }
            )
            state[_SIDECAR_DIGEST_FIELD] = sha(metadata)
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
            states = self._validate_all_attempt_receipts()
            self._validate_run_materializations(states, reject_orphan_target=target)
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
        states = self._validate_all_attempt_receipts()
        self._validate_run_materializations(states)
        return [state for state in states if state.get("status") == status]

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

    def _validate_manifest(self, manifest: dict[str, Any]) -> None:
        if manifest.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA:
            raise RunManifestError(f"Run {self.run_root.name!r} has an unsupported manifest schema")
        if manifest.get("status") not in _RUN_STATUSES:
            raise StateIntegrityError(
                self.manifest_path,
                RUN_MANIFEST_SCHEMA,
                f"manifest status is invalid: {manifest.get('status')!r}",
            )
        self._manifest_generation_value(manifest)
        self._validate_receipt_chain_map(manifest)
        self._validate_recovery_decision_ledger(manifest)

    def _validate_manifest_state_consistency(
        self,
        manifest: dict[str, Any],
        states: list[dict[str, Any]],
    ) -> None:
        """Require the published run status to agree with durable attempts."""
        status = manifest.get("status")
        state_statuses = [state.get("status") for state in states]
        pending = manifest.get("pending_retries", [])
        ambiguous = manifest.get("ambiguous_attempts", [])
        if not isinstance(pending, list) or not isinstance(ambiguous, list):
            raise StateIntegrityError(
                self.manifest_path,
                RUN_MANIFEST_SCHEMA,
                "manifest attempt status projections are invalid",
            )
        if status == "completed":
            if any(value != "completed" for value in state_statuses):
                raise StateIntegrityError(
                    self.manifest_path,
                    RUN_MANIFEST_SCHEMA,
                    "manifest status completed does not match durable attempt state",
                )
            if pending or ambiguous:
                raise StateIntegrityError(
                    self.manifest_path,
                    RUN_MANIFEST_SCHEMA,
                    "completed manifest contains pending or ambiguous attempts",
                )
        elif status == "pending_retry" and "retry_scheduled" not in state_statuses:
            raise StateIntegrityError(
                self.manifest_path,
                RUN_MANIFEST_SCHEMA,
                "manifest status pending_retry has no durable retry-scheduled attempt",
            )
        elif status == "ambiguous" and "ambiguous" not in state_statuses:
            # ``resolve`` durably records the operator decision before its
            # caller publishes the resulting run status.  Keep that narrow
            # transition readable while still rejecting an unrelated status
            # rewrite.
            resolved = any(
                state.get("resolution") is not None
                and state.get("status") in {"retry_scheduled", "failed"}
                for state in states
            )
            if not resolved:
                raise StateIntegrityError(
                    self.manifest_path,
                    RUN_MANIFEST_SCHEMA,
                    "manifest status ambiguous has no durable ambiguous attempt",
                )
        elif status == "checkpoint_pending" and "checkpoint_pending" not in state_statuses:
            raise StateIntegrityError(
                self.manifest_path,
                RUN_MANIFEST_SCHEMA,
                "manifest status checkpoint_pending has no durable checkpoint attempt",
            )
        elif (
            status == "failed"
            and state_statuses
            and not any(
                value in {"failed", "retry_scheduled", "success_candidate"}
                for value in state_statuses
            )
        ):
            raise StateIntegrityError(
                self.manifest_path,
                RUN_MANIFEST_SCHEMA,
                "manifest status failed does not match durable attempt state",
            )

    def _validate_recovery_decision_ledger(self, manifest: dict[str, Any]) -> None:
        """Validate the append-only recovery ledger and every bound receipt."""
        ledger = manifest.get(_RECOVERY_DECISION_LEDGER_FIELD)
        ledger_digest = manifest.get(_RECOVERY_DECISION_LEDGER_DIGEST_FIELD)
        if not isinstance(ledger, dict) or not self._is_sha256(ledger_digest):
            raise StateIntegrityError(
                self.manifest_path,
                RUN_MANIFEST_SCHEMA,
                "recovery decision ledger is missing or malformed",
            )
        if sha(ledger) != ledger_digest:
            raise StateIntegrityError(
                self.manifest_path,
                RUN_MANIFEST_SCHEMA,
                "recovery decision ledger digest does not match the manifest",
            )

        valid_decisions = {
            "retry_not_started",
            "retry_after_external_check",
            "fail",
        }
        for target_digest, entries in ledger.items():
            if not self._is_sha256(target_digest):
                raise RunManifestError("Recovery decision ledger contains an invalid target digest")
            if not isinstance(entries, list):
                raise RunManifestError(
                    f"Recovery decision ledger for {target_digest!r} must be a JSON list"
                )
            seen_attempts: set[int] = set()
            for index, entry in enumerate(entries, start=1):
                if not isinstance(entry, dict):
                    raise RunManifestError(
                        f"Recovery decision ledger entry {index} for {target_digest!r} "
                        "must be a JSON object"
                    )
                if entry.get("target_digest") != target_digest:
                    raise RunManifestError(
                        f"Recovery decision ledger entry {index} has a target mismatch"
                    )
                from_attempt = entry.get("from_attempt")
                to_attempt = entry.get("to_attempt")
                decision = entry.get("decision")
                if (
                    type(from_attempt) is not int
                    or from_attempt < 1
                    or type(to_attempt) is not int
                    or to_attempt < 1
                    or decision not in valid_decisions
                ):
                    raise RunManifestError(
                        f"Recovery decision ledger entry {index} has invalid decision fields"
                    )
                expected_to_attempt = from_attempt if decision == "fail" else from_attempt + 1
                if to_attempt != expected_to_attempt:
                    raise RunManifestError(
                        f"Recovery decision ledger entry {index} has an invalid attempt range"
                    )
                if from_attempt in seen_attempts:
                    raise RunManifestError(
                        f"Recovery decision ledger has multiple decisions for attempt "
                        f"{from_attempt}"
                    )
                seen_attempts.add(from_attempt)

                file_name = entry.get(_RECOVERY_RECEIPT_FILE_FIELD)
                receipt_digest = entry.get(_RECOVERY_RECEIPT_DIGEST_FIELD)
                receipt_path = self._recovery_receipt_path(
                    file_name,
                    error_path=self.manifest_path,
                )
                if not self._is_sha256(receipt_digest):
                    raise StateIntegrityError(
                        self.manifest_path,
                        RUN_MANIFEST_SCHEMA,
                        "recovery decision ledger receipt digest is invalid",
                    )
                receipt, corrupted = self._read_json_safe(receipt_path)
                if corrupted:
                    raise self._integrity_error(receipt_path, ATTEMPT_RECEIPT_SCHEMA)
                if receipt is None:
                    raise StateIntegrityError(
                        receipt_path,
                        ATTEMPT_RECEIPT_SCHEMA,
                        "recovery receipt referenced by the recovery decision ledger is missing",
                    )
                if sha(receipt) != receipt_digest:
                    raise StateIntegrityError(
                        receipt_path,
                        ATTEMPT_RECEIPT_SCHEMA,
                        "recovery decision ledger receipt digest does not match",
                    )
                if (
                    receipt.get("from_attempt") != from_attempt
                    or receipt.get("to_attempt") != to_attempt
                    or receipt.get("decision") != decision
                ):
                    raise StateIntegrityError(
                        receipt_path,
                        ATTEMPT_RECEIPT_SCHEMA,
                        "recovery decision ledger does not match its receipt",
                    )

    def _recovery_decisions_for(
        self,
        target: str,
        *,
        manifest: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if manifest is None:
            manifest = self._required_manifest()
        ledger = manifest[_RECOVERY_DECISION_LEDGER_FIELD]
        return list(ledger.get(sha(target), []))

    def _append_recovery_decision_locked(
        self,
        manifest: dict[str, Any],
        *,
        target: str,
        from_attempt: int,
        to_attempt: int,
        decision: str,
        receipt_path: Path,
        receipt_digest: str,
    ) -> dict[str, Any]:
        entries = manifest[_RECOVERY_DECISION_LEDGER_FIELD].setdefault(sha(target), [])
        if any(entry.get("from_attempt") == from_attempt for entry in entries):
            raise RunManifestError(
                f"Target {target!r} attempt {from_attempt} already has a recovery decision"
            )
        entry = {
            "target_digest": sha(target),
            "from_attempt": from_attempt,
            "to_attempt": to_attempt,
            "decision": decision,
            _RECOVERY_RECEIPT_FILE_FIELD: receipt_path.relative_to(self.run_root).as_posix(),
            _RECOVERY_RECEIPT_DIGEST_FIELD: receipt_digest,
        }
        entries.append(entry)
        manifest[_RECOVERY_DECISION_LEDGER_DIGEST_FIELD] = sha(
            manifest[_RECOVERY_DECISION_LEDGER_FIELD]
        )
        self._validate_recovery_decision_ledger(manifest)
        return entry

    def _required_manifest(self) -> dict[str, Any]:
        validate_run_path(self.run_root)
        manifest, corrupted = self._read_json_safe(self.manifest_path)
        if corrupted:
            raise self._integrity_error(self.manifest_path, RUN_MANIFEST_SCHEMA)
        if manifest is None:
            raise RunManifestError(f"Missing or invalid run manifest: {self.manifest_path}")
        self._validate_manifest(manifest)
        if not self._fence_manifest_generation:
            self._manifest_generation = self._manifest_generation_value(manifest)
            self._fence_manifest_generation = True
        return manifest

    def _required_failed_manifest(self) -> dict[str, Any]:
        """Require the run to still be terminally failed in the current lock."""
        manifest = self._required_manifest()
        if manifest.get("status") != "failed":
            raise ValueError(f"Run {self.run_root.name!r} is not in terminal failed state")
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

    def _write_state(
        self,
        target: str,
        state: dict[str, Any],
        *,
        manifest: dict[str, Any] | None = None,
    ) -> None:
        with self._run_locked():
            self._write_state_locked(target, state, manifest=manifest)

    def _write_state_locked(
        self,
        target: str,
        state: dict[str, Any],
        *,
        manifest: dict[str, Any] | None = None,
    ) -> None:
        """Append a state snapshot and bind it to the run-manifest receipt chain."""
        target_digest = sha(target)
        if state.get("target") != target or state.get("target_digest") != target_digest:
            raise RunManifestError(f"Attempt state for {target!r} has an invalid target binding")
        attempt = state.get("attempt")
        if type(attempt) is not int or attempt < 1:
            raise RunManifestError(f"Attempt state for {target!r} has invalid attempt")
        self._validate_active_effect(self._state_path(target), state.get("active_effect"))

        if manifest is None:
            manifest = self._required_manifest()
        else:
            self._validate_manifest(manifest)
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
        self._commit_manifest(manifest, advance_generation=False)
        self._state_mutated_since_status = True

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
        status = state.get("status")
        if status not in _ATTEMPT_STATUSES:
            raise StateIntegrityError(
                path,
                ATTEMPT_RECEIPT_SCHEMA,
                f"attempt state has an invalid status {status!r}",
            )
        self._validate_active_effect(path, state.get("active_effect"))
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
        self._validate_candidate_binding(target, state)
        receipt_path = self._target_root(target) / f"attempt-{attempt:04d}.json"
        receipt, corrupted = self._read_json_safe(receipt_path)
        if corrupted:
            raise self._integrity_error(receipt_path, ATTEMPT_RECEIPT_SCHEMA)
        self._validate_recovery_receipt_binding(state, path, target)
        self._validate_recovery_decision_state(target, state)
        if receipt is None:
            if state.get("side_effect_started") is True:
                raise AmbiguousAttemptError(
                    self.run_root.name,
                    target,
                    attempt,
                )
            if state.get("status") in {
                "success_candidate",
                "checkpoint_pending",
                "retry_scheduled",
                "failed",
                "ambiguous",
                "completed",
            }:
                raise StateIntegrityError(
                    receipt_path,
                    ATTEMPT_RECEIPT_SCHEMA,
                    "current attempt receipt is missing",
                )
            return
        self._validate_receipt_record(receipt_path, receipt, target, attempt)
        if receipt != state:
            raise StateIntegrityError(
                receipt_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "current state and attempt receipt are not identical",
            )

    @staticmethod
    def _validate_active_effect(path: Path, active_effect: Any) -> None:
        """Validate the managed/unmanaged Prompt binding at the effect boundary."""
        if active_effect is None:
            return
        if not isinstance(active_effect, dict):
            raise StateIntegrityError(
                path,
                ATTEMPT_RECEIPT_SCHEMA,
                "active effect must be an object",
            )
        managed = active_effect.get("managed")
        if managed is not None and type(managed) is not bool:
            raise StateIntegrityError(
                path,
                ATTEMPT_RECEIPT_SCHEMA,
                "active effect managed flag is invalid",
            )
        resolution = active_effect.get("prompt_resolution")
        if managed is True and not isinstance(resolution, dict):
            raise StateIntegrityError(
                path,
                ATTEMPT_RECEIPT_SCHEMA,
                "managed active effect is missing its prompt resolution",
            )
        if resolution is None:
            return
        from .prompt import PromptResolutionError, validate_prompt_resolution_record

        try:
            validate_prompt_resolution_record(resolution)
        except PromptResolutionError as error:
            raise StateIntegrityError(
                path,
                ATTEMPT_RECEIPT_SCHEMA,
                f"active effect prompt resolution is invalid: {error}",
            ) from error

    def _validate_candidate_binding(
        self,
        target: str,
        state: dict[str, Any],
    ) -> dict[str, Any] | None:
        filename = state.get("candidate_file")
        if filename is None:
            if state.get("status") == "success_candidate":
                raise StateIntegrityError(
                    self._state_path(target),
                    ATTEMPT_RECEIPT_SCHEMA,
                    "success candidate binding is missing",
                )
            return None
        if (
            not isinstance(filename, str)
            or Path(filename).is_absolute()
            or Path(filename).parent != Path(".")
            or filename != f"candidate-{state.get('attempt', 0):04d}.json"
        ):
            raise StateIntegrityError(
                self._state_path(target),
                ATTEMPT_RECEIPT_SCHEMA,
                "success candidate path is invalid",
            )
        candidate_path = self._target_root(target) / filename
        candidate, corrupted = self._read_json_safe(candidate_path)
        if corrupted:
            raise self._integrity_error(candidate_path, SUCCESS_CANDIDATE_SCHEMA)
        if candidate is None:
            raise StateIntegrityError(
                candidate_path,
                SUCCESS_CANDIDATE_SCHEMA,
                "success candidate is missing",
            )
        if candidate.get("candidate_schema") != SUCCESS_CANDIDATE_SCHEMA:
            raise RunManifestError(f"Success candidate for {target!r} has unsupported schema")
        if state.get("candidate_sha256") != sha(candidate):
            raise StateIntegrityError(
                candidate_path,
                SUCCESS_CANDIDATE_SCHEMA,
                "success candidate digest does not match the state",
            )
        if not isinstance(candidate.get("artifact"), dict):
            raise StateIntegrityError(
                candidate_path,
                SUCCESS_CANDIDATE_SCHEMA,
                "success candidate artifact is invalid",
            )
        prompt_resolutions = candidate.get("prompt_resolutions")
        if not isinstance(prompt_resolutions, dict):
            raise StateIntegrityError(
                candidate_path,
                SUCCESS_CANDIDATE_SCHEMA,
                "success candidate Prompt resolutions are invalid",
            )
        state_prompt_resolutions = state.get("prompt_resolutions")
        if not isinstance(state_prompt_resolutions, dict):
            raise StateIntegrityError(
                self._state_path(target),
                ATTEMPT_RECEIPT_SCHEMA,
                "attempt Prompt resolutions are invalid",
            )
        if prompt_resolutions != state_prompt_resolutions:
            raise StateIntegrityError(
                candidate_path,
                SUCCESS_CANDIDATE_SCHEMA,
                "success candidate Prompt resolutions do not match the state",
            )
        if (
            state.get("status") == "completed"
            and state.get("artifact_sha256") is not None
            and state.get("artifact_sha256") != sha(candidate["artifact"])
        ):
            raise StateIntegrityError(
                candidate_path,
                SUCCESS_CANDIDATE_SCHEMA,
                "completed artifact does not match its success candidate",
            )
        return candidate

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

    def _recovery_receipt_path(self, file_name: Any, *, error_path: Path) -> Path:
        if not isinstance(file_name, str) or not file_name:
            raise StateIntegrityError(
                error_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "recovery receipt path is missing or invalid",
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
                error_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "recovery receipt path escapes the run directory",
            )
        return self.run_root / relative

    def _validate_recovery_receipt_binding(
        self,
        state: dict[str, Any],
        state_path: Path,
        target: str,
    ) -> None:
        decision = state.get(_RECOVERY_DECISION_FIELD)
        file_name = state.get(_RECOVERY_RECEIPT_FILE_FIELD)
        digest = state.get(_RECOVERY_RECEIPT_DIGEST_FIELD)
        if decision is None and file_name is None and digest is None:
            return
        if (
            decision not in {"retry_not_started", "retry_after_external_check", "fail"}
            or file_name is None
            or digest is None
            or not self._is_sha256(digest)
        ):
            raise StateIntegrityError(
                state_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "recovery receipt binding is incomplete or invalid",
            )
        receipt_path = self._recovery_receipt_path(file_name, error_path=state_path)
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
        if receipt.get("decision") != decision:
            raise StateIntegrityError(
                receipt_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "bound recovery receipt decision does not match the state",
            )
        if not any(
            entry.get(_RECOVERY_RECEIPT_FILE_FIELD) == file_name
            and entry.get(_RECOVERY_RECEIPT_DIGEST_FIELD) == digest
            and entry.get("decision") == decision
            for entry in self._recovery_decisions_for(target)
        ):
            raise StateIntegrityError(
                state_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "recovery receipt is not bound in the recovery decision ledger",
            )

    def _validate_recovery_decision_state(
        self,
        target: str,
        state: dict[str, Any],
    ) -> None:
        """Ensure the current state reflects every durable recovery decision."""
        state_path = self._state_path(target)
        current_attempt = state.get("attempt")
        for entry in self._recovery_decisions_for(target):
            from_attempt = entry["from_attempt"]
            decision = entry["decision"]
            to_attempt = entry["to_attempt"]
            if decision == "fail":
                valid = current_attempt == from_attempt and state.get("status") == "failed"
            else:
                valid = isinstance(current_attempt, int) and current_attempt >= to_attempt
            if not valid:
                raise StateIntegrityError(
                    state_path,
                    ATTEMPT_RECEIPT_SCHEMA,
                    "current state does not reflect the recovery decision ledger",
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
        stem = f"recovery-{recovery_time}"
        suffix = 0
        with SecureDirectory(self.run_root, create=False) as directory:
            while True:
                suffix_text = "" if suffix == 0 else f"-{suffix}"
                name = f"{stem}{suffix_text}.json"
                descriptor, temporary_name = directory.temporary(f".{name}.")
                try:
                    try:
                        with os.fdopen(
                            descriptor,
                            "w",
                            encoding="utf-8",
                            closefd=True,
                        ) as handle:
                            descriptor = -1
                            handle.write(canonical_json(payload))
                            handle.flush()
                            os.fsync(handle.fileno())
                    finally:
                        if descriptor >= 0:
                            with suppress(OSError):
                                os.close(descriptor)

                    try:
                        directory.link(temporary_name, name)
                    except FileExistsError:
                        suffix += 1
                        continue
                    return self.run_root / name
                finally:
                    with suppress(OSError, ValueError):
                        directory.unlink(temporary_name, missing_ok=True)

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

    def _validate_all_attempt_receipts(self) -> list[dict[str, Any]]:
        """Validate every target and return the one trusted state per target."""
        manifest = self._required_manifest()
        attempts_root = self.run_root / "attempts"
        try:
            attempts_info = attempts_root.lstat()
        except FileNotFoundError:
            attempts_info = None
        except OSError as error:
            raise StateIntegrityError(
                attempts_root,
                ATTEMPT_RECEIPT_SCHEMA,
                f"attempts directory stat failed: {error}",
            ) from error
        if attempts_info is not None and stat.S_ISLNK(attempts_info.st_mode):
            raise StateIntegrityError(
                attempts_root,
                ATTEMPT_RECEIPT_SCHEMA,
                "attempts directory must not be a symlink",
            )
        if attempts_info is not None and not stat.S_ISDIR(attempts_info.st_mode):
            raise StateIntegrityError(
                attempts_root,
                ATTEMPT_RECEIPT_SCHEMA,
                "attempts path must be a directory",
            )
        if not attempts_root.is_dir():
            if manifest[_RECEIPT_CHAIN_FIELD]:
                raise StateIntegrityError(
                    attempts_root,
                    ATTEMPT_RECEIPT_SCHEMA,
                    "durable state is missing for the manifest receipt chain",
                )
            self._validate_manifest_state_consistency(manifest, [])
            return []
        states: list[dict[str, Any]] = []
        target_digests: set[str] = set()
        target_roots: list[Path] = []
        for path in attempts_root.iterdir():
            try:
                info = path.lstat()
            except OSError as error:
                raise StateIntegrityError(
                    path,
                    ATTEMPT_RECEIPT_SCHEMA,
                    f"attempt target stat failed: {error}",
                ) from error
            if stat.S_ISLNK(info.st_mode):
                raise StateIntegrityError(
                    path,
                    ATTEMPT_RECEIPT_SCHEMA,
                    "attempt target directory must not be a symlink",
                )
            if stat.S_ISDIR(info.st_mode):
                target_roots.append(path)
            else:
                raise StateIntegrityError(
                    path,
                    ATTEMPT_RECEIPT_SCHEMA,
                    "attempt target entry must be a directory",
                )
        for target_root in sorted(target_roots):
            target_digests.add(target_root.name)
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
                states.append(state)
                continue

            receipt_paths = sorted(target_root.glob("attempt-*.json"))
            if not receipt_paths:
                raise StateIntegrityError(
                    state_path,
                    ATTEMPT_RECEIPT_SCHEMA,
                    "durable state is missing",
                )
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
            raise StateIntegrityError(
                state_path,
                ATTEMPT_RECEIPT_SCHEMA,
                "durable state is missing",
            )

        for target_digest in manifest[_RECEIPT_CHAIN_FIELD]:
            if target_digest not in target_digests:
                raise StateIntegrityError(
                    attempts_root / target_digest / "state.json",
                    ATTEMPT_RECEIPT_SCHEMA,
                    "durable state is missing for the manifest receipt chain",
                )
        self._validate_manifest_state_consistency(manifest, states)
        return states

    def _validate_run_materializations(
        self,
        states: list[dict[str, Any]],
        *,
        manifest: dict[str, Any] | None = None,
        reject_orphan_target: str | None = None,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, dict[str, Any]]]]:
        """Validate one run's candidates and materializations as a bound set.

        A sidecar is evidence for a durable target, not an independent success
        record.  The only read-only compatibility case is a declared warm-cache
        sidecar: cache hits historically did not create an attempt because no
        provider side effect occurred.  Unknown sidecars are deliberately
        ignored as rogue evidence and can never become runtime nodes.
        """
        if manifest is None:
            manifest = self._required_manifest()
        candidates: dict[str, dict[str, Any]] = {}
        materializations: dict[str, dict[str, dict[str, Any]]] = {}
        states_by_target = {state["target"]: state for state in states}
        paired_targets: set[str] = set()

        for state in states:
            target = state["target"]
            candidate = self._validate_candidate_binding(target, state)
            if state.get("status") == "completed" and candidate is None:
                raise StateIntegrityError(
                    self._state_path(target),
                    ATTEMPT_RECEIPT_SCHEMA,
                    "completed state is missing its success candidate",
                )
            if candidate is not None:
                candidates[target] = candidate

        ignored_targets: set[str] = set()
        for sidecar_path in sorted(self.run_root.glob("*.json.meta.json")):
            target = sidecar_path.name.removesuffix(".json.meta.json")
            if not target:
                raise StateIntegrityError(
                    sidecar_path,
                    RUN_SIDECAR_SCHEMA,
                    "run sidecar has an invalid target name",
                )
            state = states_by_target.get(target)
            declared = self._is_declared_runtime_target(target, manifest)
            if state is None and target != reject_orphan_target and not declared:
                ignored_targets.add(target)
                continue

            artifact, metadata = self._validate_materialization_pair(target, sidecar_path)
            if state is None:
                if target == reject_orphan_target or metadata.get("cache") != "hit":
                    raise StateIntegrityError(
                        sidecar_path,
                        RUN_SIDECAR_SCHEMA,
                        "run sidecar has no durable state or success candidate",
                    )
            else:
                status = state.get("status")
                if status not in {"running", "success_candidate", "completed"}:
                    raise StateIntegrityError(
                        sidecar_path,
                        RUN_SIDECAR_SCHEMA,
                        f"run sidecar is bound to non-success state {status!r}",
                    )
                candidate = candidates.get(target)
                if candidate is not None:
                    self._validate_sidecar_candidate_binding(
                        target,
                        state,
                        candidate,
                        metadata,
                    )
                if status == "completed":
                    self._validate_completed_materialization(
                        target,
                        state,
                        candidate,
                        artifact,
                        metadata,
                    )
            paired_targets.add(target)
            if state is None or state.get("status") == "completed":
                materializations[target] = {
                    "artifact": artifact,
                    "metadata": metadata,
                }

        for state in states:
            if state.get("status") != "completed":
                continue
            target = state["target"]
            if target not in materializations:
                raise StateIntegrityError(
                    self._target_artifact_path(target),
                    RUN_SIDECAR_SCHEMA,
                    "completed target run sidecar is missing",
                )

        for artifact_path in sorted(self.run_root.glob("*.json")):
            if (
                artifact_path.name == "_run.json"
                or artifact_path.name.startswith("recovery-")
                or artifact_path.name.endswith(".json.meta.json")
            ):
                continue
            target = artifact_path.stem
            if target in ignored_targets:
                continue
            if target not in paired_targets:
                raise StateIntegrityError(
                    artifact_path,
                    RUN_SIDECAR_SCHEMA,
                    "artifact has no matching run sidecar",
                )

        return candidates, materializations

    @staticmethod
    def _is_declared_runtime_target(target: str, manifest: dict[str, Any]) -> bool:
        profile = manifest.get("workflow_profile")
        graph = profile.get("graph") if isinstance(profile, dict) else None
        nodes = graph.get("nodes") if isinstance(graph, dict) else None
        if not isinstance(nodes, list):
            return False
        for node in nodes:
            if not isinstance(node, dict) or not isinstance(node.get("name"), str):
                continue
            name = node["name"]
            if target == name:
                return True
            if target.startswith(f"{name}@"):
                item_id = target.removeprefix(f"{name}@")
                ledger = manifest.get("dynamic_files_ledger")
                ledger_digest = manifest.get("dynamic_files_ledger_sha256")
                if not isinstance(ledger, dict) or ledger_digest != sha(ledger):
                    return False
                items = ledger.get(name)
                return isinstance(items, dict) and item_id in items
        return False

    def _validate_completed_materialization(
        self,
        target: str,
        state: dict[str, Any],
        candidate: dict[str, Any] | None,
        artifact: dict[str, Any],
        metadata: dict[str, Any],
    ) -> None:
        if candidate is None:
            raise StateIntegrityError(
                self._state_path(target),
                ATTEMPT_RECEIPT_SCHEMA,
                "completed state is missing its success candidate",
            )
        artifact_digest = sha(artifact)
        if state.get("artifact_sha256") != artifact_digest:
            raise StateIntegrityError(
                self._state_path(target),
                ATTEMPT_RECEIPT_SCHEMA,
                "completed state does not match its run artifact",
            )
        sidecar_digest = state.get(_SIDECAR_DIGEST_FIELD)
        if not self._is_sha256(sidecar_digest) or sidecar_digest != sha(metadata):
            raise StateIntegrityError(
                self._state_path(target),
                ATTEMPT_RECEIPT_SCHEMA,
                "completed state does not match its run sidecar digest",
            )
        self._validate_sidecar_candidate_binding(target, state, candidate, metadata)

    def _validate_sidecar_candidate_binding(
        self,
        target: str,
        state: dict[str, Any],
        candidate: dict[str, Any],
        metadata: dict[str, Any],
    ) -> None:
        if metadata.get("prompt_resolutions") != state.get("prompt_resolutions"):
            raise StateIntegrityError(
                self._target_artifact_path(target).with_suffix(".json.meta.json"),
                RUN_SIDECAR_SCHEMA,
                "run sidecar Prompt resolutions do not match the attempt",
            )
        required = ("cache_key", "key_components", "calls", "prompt_resolutions")
        if any(field not in candidate for field in required):
            raise StateIntegrityError(
                self._state_path(target),
                SUCCESS_CANDIDATE_SCHEMA,
                "success candidate is missing sidecar binding fields",
            )
        for field in ("cache_key", "key_components", "calls"):
            if metadata.get(field) != candidate.get(field):
                raise StateIntegrityError(
                    self._target_artifact_path(target).with_suffix(".json.meta.json"),
                    RUN_SIDECAR_SCHEMA,
                    f"run sidecar {field} does not match its success candidate",
                )

    def _target_artifact_path(self, target: str) -> Path:
        return self.run_root / f"{target}.json"

    def _validate_materialization_pair(
        self,
        target: str,
        sidecar_path: Path,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        artifact_path = self._target_artifact_path(target)
        artifact, corrupted = self._read_json_safe(artifact_path)
        if corrupted:
            raise self._integrity_error(artifact_path, RUN_SIDECAR_SCHEMA)
        if artifact is None:
            raise StateIntegrityError(
                artifact_path,
                RUN_SIDECAR_SCHEMA,
                "run artifact is missing",
            )
        metadata, corrupted = self._read_json_safe(sidecar_path)
        if corrupted:
            raise self._integrity_error(sidecar_path, RUN_SIDECAR_SCHEMA)
        if metadata is None:
            raise StateIntegrityError(
                sidecar_path,
                RUN_SIDECAR_SCHEMA,
                "run sidecar is missing",
            )
        artifact_digest = sha(artifact)
        origin = metadata.get("origin_provenance")
        cache_key = metadata.get("cache_key")
        key_components = metadata.get("key_components")
        outputs = metadata.get("outputs")
        calls = metadata.get("calls")
        execution_calls = metadata.get("execution_calls")
        cache_policy = metadata.get("cache_policy")
        if (
            metadata.get("run_sidecar_schema") != RUN_SIDECAR_SCHEMA
            or metadata.get("node") != target
            or not isinstance(cache_key, (str, list))
            or (isinstance(cache_key, list) and not all(isinstance(key, str) for key in cache_key))
            or (key_components is not None and not isinstance(key_components, dict))
            or not isinstance(metadata.get("cache", "unknown"), str)
            or not isinstance(cache_policy, str)
            or not isinstance(outputs, list)
            or not all(isinstance(output, str) for output in outputs)
            or not isinstance(calls, list)
            or not all(isinstance(call, dict) for call in calls)
            or not isinstance(execution_calls, list)
            or not all(isinstance(call, dict) for call in execution_calls)
            or not self._is_sha256(metadata.get("artifact_sha256"))
            or metadata.get("artifact_sha256") != artifact_digest
            or not isinstance(origin, dict)
            or origin.get("artifact_sha256") != artifact_digest
            or metadata.get("origin_provenance_digest") != sha(origin)
            or not isinstance(metadata.get("prompt_resolutions", {}), dict)
            or metadata.get("prompt_resolutions_digest")
            != sha(metadata.get("prompt_resolutions", {}))
        ):
            raise StateIntegrityError(
                sidecar_path,
                RUN_SIDECAR_SCHEMA,
                "run artifact or sidecar digest binding is invalid",
            )
        self._validate_prompt_lineage(sidecar_path, metadata)
        return artifact, metadata

    @staticmethod
    def _validate_prompt_lineage(path: Path, metadata: dict[str, Any]) -> None:
        """Validate every persisted Prompt resolution reachable from a sidecar."""
        from .prompt import PromptResolutionError, validate_prompt_resolution_record

        def resolution(value: Any, label: str) -> None:
            if value is None:
                return
            try:
                validate_prompt_resolution_record(value)
            except PromptResolutionError as error:
                raise StateIntegrityError(
                    path,
                    RUN_SIDECAR_SCHEMA,
                    f"{label} is invalid: {error}",
                ) from error

        def resolutions(value: Any, label: str) -> None:
            if not isinstance(value, dict):
                raise StateIntegrityError(path, RUN_SIDECAR_SCHEMA, f"{label} are invalid")
            for record in value.values():
                resolution(record, label)

        def calls(value: Any, label: str) -> None:
            if not isinstance(value, list) or not all(isinstance(call, dict) for call in value):
                raise StateIntegrityError(path, RUN_SIDECAR_SCHEMA, f"{label} are invalid")
            for index, call in enumerate(value):
                resolution(call.get("prompt_resolution"), f"{label} [{index}] Prompt resolution")

        resolutions(metadata.get("prompt_resolutions"), "current Prompt resolutions")
        calls(metadata.get("calls"), "current CALL lineage")
        calls(metadata.get("execution_calls"), "execution CALL lineage")

        origin = metadata.get("origin_provenance")
        if not isinstance(origin, dict):
            raise StateIntegrityError(path, RUN_SIDECAR_SCHEMA, "origin provenance is invalid")
        resolutions(origin.get("prompt_resolutions"), "origin Prompt resolutions")
        calls(origin.get("calls"), "origin CALL lineage")
        agent = origin.get("agent")
        if isinstance(agent, dict):
            resolution(agent.get("prompt_resolution"), "origin Agent Prompt resolution")

    @staticmethod
    def _read_json_bytes_safe(path: Path) -> tuple[bytes | None, BaseException | None]:
        """Read durable JSON through a bound parent descriptor.

        A lexical ``lstat`` followed by ``Path.read_text`` leaves both the
        final file and every parent directory replaceable between the check and
        the read.  Durable run paths are ownership boundaries, so bind the
        complete parent path with the existing no-follow directory primitive
        and open the final file relative to that descriptor.  The final open is
        non-blocking and rejects symlinks and special files before any bytes are
        consumed.

        ``(None, None)`` is a genuine missing file.  A non-``None`` exception is
        an integrity failure, including a symlinked parent or final entry.
        """
        try:
            with SecureDirectory(Path(path).parent, create=False) as directory:
                try:
                    with _open_regular_file_at(
                        directory,
                        Path(path).name,
                        phase="before reading durable JSON",
                    ) as handle:
                        return handle.read(), None
                except FileNotFoundError:
                    return None, None
        except FileNotFoundError:
            return None, None
        except (OSError, ValueError) as error:
            return None, error

    @classmethod
    def _read_json_safe(cls, path: Path) -> tuple[dict[str, Any] | None, bool]:
        """Return ``(data, corrupted)`` while keeping missing distinct from torn JSON."""
        raw, read_error = cls._read_json_bytes_safe(path)
        if read_error is not None:
            return None, True
        if raw is None:
            return None, False
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None, True
        return (value, False) if isinstance(value, dict) else (None, True)

    @staticmethod
    def _parse_error(path: Path) -> BaseException | str:
        """Recover a useful parse explanation for a failed safe read."""
        raw, read_error = AttemptStore._read_json_bytes_safe(path)
        if read_error is not None:
            return read_error
        if raw is None:
            return FileNotFoundError(str(path))
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
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
        self._validate_all_attempt_receipts()
        return self._required_json(self._state_path(target))


def validate_durable_run(run_root: Path) -> DurableRunSnapshot:
    """Validate one current schema-2 run for every read-only surface."""
    validate_run_path(run_root)
    manifest_path = run_root / "_run.json"
    manifest, corrupted = AttemptStore._read_json_safe(manifest_path)
    if corrupted:
        raise AttemptStore._integrity_error(manifest_path, RUN_MANIFEST_SCHEMA)
    if manifest is None:
        raise RunManifestError(f"Missing or invalid run manifest: {manifest_path}")
    if manifest.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA:
        raise RunManifestError(f"Run {run_root.name!r} has an unsupported manifest schema")
    store = AttemptStore(run_root, {})
    validated_manifest = store._required_manifest()
    states = store._validate_all_attempt_receipts()
    candidates, materializations = store._validate_run_materializations(
        states,
        manifest=validated_manifest,
    )
    return DurableRunSnapshot(
        validated_manifest,
        tuple(states),
        candidates,
        materializations,
        True,
    )
