"""Cross-process file-lock request slots for explicitly configured throttling."""

from __future__ import annotations

import fcntl
import os
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO


class SlotTimeoutError(TimeoutError):
    """Raised before protected work when no file-lock slot becomes available."""

    def __init__(self, wait_seconds: float) -> None:
        self.wait_seconds = wait_seconds
        super().__init__(f"Timed out waiting {wait_seconds:.3f}s for a shared slot")


@dataclass(frozen=True)
class SlotLease:
    """Observable identity and queue wait for one acquired advisory slot."""

    slot_identity: str | None
    wait_seconds: float


class AdaptiveCapacity:
    """Persist a shared request capacity that falls fast and recovers slowly."""

    def __init__(
        self,
        capacity_file: Path | str,
        max_slots: int,
        min_slots: int = 1,
        ramp_successes: int = 8,
    ) -> None:
        if min_slots < 1:
            raise ValueError("min_slots must be at least 1")
        if max_slots < min_slots:
            raise ValueError("max_slots must be at least min_slots")
        if ramp_successes < 1:
            raise ValueError("ramp_successes must be at least 1")
        self.capacity_file = Path(capacity_file)
        self.max_slots = max_slots
        self.min_slots = min_slots
        self.ramp_successes = ramp_successes
        self._successes_file = self.capacity_file.with_name(f"{self.capacity_file.name}.successes")
        self._lock_file = self.capacity_file.with_name(f"{self.capacity_file.name}.lock")

    def on_throttle(self) -> None:
        """Halve capacity and discard a partial recovery streak."""
        with self._locked():
            current = self._read_capacity()
            self._write_int(self.capacity_file, max(self.min_slots, current // 2))
            self._write_int(self._successes_file, 0)

    def on_success(self) -> None:
        """Raise capacity by one only after a full success streak."""
        with self._locked():
            successes = self._read_int(self._successes_file, 0) + 1
            if successes < self.ramp_successes:
                self._write_int(self._successes_file, successes)
                return
            current = self._read_capacity()
            self._write_int(self.capacity_file, min(self.max_slots, current + 1))
            self._write_int(self._successes_file, 0)

    @contextmanager
    def _locked(self) -> Iterator[None]:
        self._lock_file.parent.mkdir(parents=True, exist_ok=True)
        with self._lock_file.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _read_capacity(self) -> int:
        return max(
            self.min_slots,
            min(self.max_slots, self._read_int(self.capacity_file, self.max_slots)),
        )

    @staticmethod
    def _read_int(path: Path, default: int) -> int:
        try:
            return int(path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return default

    @staticmethod
    def _write_int(path: Path, value: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, delete=False
        ) as handle:
            handle.write(str(value))
            # 与 artifacts 的原子写同款:不 fsync 就 rename,断电后文件可能为空,
            # 容量会静默回弹到 max_slots 造成突发限流。
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        temporary.replace(path)


class FileSlots:
    """Acquire one advisory file-lock slot only when a project enables it."""

    def __init__(
        self,
        lock_dir: Path | str | None,
        slots: int,
        capacity_file: Path | str | None = None,
    ) -> None:
        self._lock_dir = Path(lock_dir) if lock_dir else None
        self._slots = slots
        self._capacity_file = Path(capacity_file) if capacity_file else None

    @classmethod
    def from_env(cls, prefix: str = "KIGUMI_REQUEST") -> FileSlots:
        """Build an optional limiter from stripped environment variables."""
        lock_dir = os.getenv(f"{prefix}_LOCK_DIR", "").strip() or None
        capacity_file = os.getenv(f"{prefix}_CAPACITY_FILE", "").strip() or None
        slots_text = os.getenv(f"{prefix}_SLOTS", "").strip()
        try:
            slots = int(slots_text)
        except ValueError:
            slots = 0
        return cls(lock_dir, slots, capacity_file)

    @property
    def enabled(self) -> bool:
        """Whether this instance may create lock files and wait for a slot."""
        return self._lock_dir is not None and self._slots >= 1

    @contextmanager
    def acquire(self, *, timeout_seconds: float | None = None) -> Iterator[SlotLease]:
        """Hold one slot, releasing it even when the protected request fails."""
        if timeout_seconds is not None and (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int | float)
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be positive or null")
        if not self.enabled:
            yield SlotLease(None, 0.0)
            return
        assert self._lock_dir is not None
        self._lock_dir.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        handle, identity = self._acquire_handle(started, timeout_seconds)
        try:
            yield SlotLease(identity, time.monotonic() - started)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def _acquire_handle(self, started: float, timeout_seconds: float | None) -> tuple[TextIO, str]:
        assert self._lock_dir is not None
        while True:
            for index in range(self._effective_slots()):
                path = self._lock_dir / f"slot_{index:03d}.lock"
                handle = path.open("a+", encoding="utf-8")
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    handle.close()
                    continue
                return handle, f"slot_{index:03d}"
            if timeout_seconds is not None and time.monotonic() - started >= timeout_seconds:
                raise SlotTimeoutError(time.monotonic() - started)
            time.sleep(0.05)

    def _effective_slots(self) -> int:
        if self._capacity_file is None:
            return self._slots
        try:
            capacity = int(self._capacity_file.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return self._slots
        return max(1, min(self._slots, capacity))
