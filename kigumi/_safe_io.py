"""Private descriptor-bound primitives for reading regular files safely."""

from __future__ import annotations

import errno
import os
import stat
from collections.abc import Callable, Iterator
from hashlib import sha256
from pathlib import Path
from typing import Any, BinaryIO, TypeAlias

FileIdentity: TypeAlias = tuple[int, ...]
FileError: TypeAlias = Callable[[str, Path], BaseException]
IdentityFactory: TypeAlias = Callable[[os.stat_result], FileIdentity]


def lstat_regular_file(
    path: Path,
    *,
    error: FileError,
) -> os.stat_result:
    """Reject symlinks and special files before any descriptor is opened."""
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise error("must reference a regular file", path)
    return info


def open_regular_file(
    path: Path,
    *,
    identity: IdentityFactory,
    expected_identity: FileIdentity | None,
    phase: str,
    error: FileError,
) -> BinaryIO:
    """Open a regular file through a non-blocking, no-follow descriptor."""
    lstat_regular_file(path, error=error)

    flags = os.O_RDONLY
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as caught:
        if getattr(caught, "errno", None) == errno.ELOOP:
            raise error("must not be a symlink", path) from caught
        raise

    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise error("must reference a regular file", path)
        if expected_identity is not None and identity(opened) != expected_identity:
            raise error(f"changed {phase}", path)
        return os.fdopen(descriptor, "rb", closefd=True)
    except BaseException:
        os.close(descriptor)
        raise


def verify_regular_descriptor(
    handle: Any,
    path: Path,
    *,
    identity: IdentityFactory,
    expected_identity: FileIdentity | None,
    phase: str,
    error: FileError,
) -> os.stat_result:
    """Validate the file currently bound to an already-open descriptor."""
    info = os.fstat(handle.fileno())
    if not stat.S_ISREG(info.st_mode):
        raise error(f"changed {phase}", path)
    if expected_identity is not None and identity(info) != expected_identity:
        raise error(f"changed {phase}", path)
    return info


def iter_file_chunks(handle: BinaryIO, chunk_size: int) -> Iterator[bytes]:
    """Read a descriptor in bounded chunks until EOF."""
    while chunk := handle.read(chunk_size):
        yield chunk


def digest_open_file(
    handle: BinaryIO,
    path: Path,
    *,
    identity: IdentityFactory,
    expected_identity: FileIdentity | None,
    before_phase: str,
    during_phase: str,
    chunk_size: int,
    error: FileError,
    collect: bool = False,
    max_bytes: int | None = None,
) -> tuple[str, int, bytes | None]:
    """Hash an open descriptor while checking its identity before and after."""
    initial = verify_regular_descriptor(
        handle,
        path,
        identity=identity,
        expected_identity=expected_identity,
        phase=before_phase,
        error=error,
    )
    initial_identity = identity(initial)
    digestor = sha256()
    size = 0
    chunks: list[bytes] = []

    if max_bytes is None:
        iterator = iter_file_chunks(handle, chunk_size)
    else:
        remaining = max_bytes + 1

        def limited_chunks() -> Iterator[bytes]:
            nonlocal remaining
            while remaining:
                chunk = handle.read(min(chunk_size, remaining))
                if not chunk:
                    return
                remaining -= len(chunk)
                yield chunk

        iterator = limited_chunks()

    for chunk in iterator:
        digestor.update(chunk)
        if collect:
            chunks.append(chunk)
        size += len(chunk)

    verify_regular_descriptor(
        handle,
        path,
        identity=identity,
        expected_identity=initial_identity,
        phase=during_phase,
        error=error,
    )
    return digestor.hexdigest(), size, b"".join(chunks) if collect else None
