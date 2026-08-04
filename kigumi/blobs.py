"""Content-addressed binary storage with one descriptor-relative I/O boundary.

Blob bytes are immutable and addressed by SHA-256.  The store owns the root
directory descriptor for every publish, read, materialize, and GC operation;
no path-based lifecycle operation is allowed to follow a project symlink.
"""

from __future__ import annotations

import os
import re
import stat
from contextlib import suppress
from hashlib import sha256
from pathlib import Path

from ._safe_io import (
    FileError,
    FileIdentity,
    SecureDirectory,
    digest_open_file,
    iter_file_chunks,
    open_regular_file,
    rename_at,
    verify_regular_descriptor,
)
from ._safe_io import (
    _open_regular_file_at as _safe_open_regular_file_at,
)

_CHUNK_SIZE = 1024 * 1024
_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


def _validate_digest(digest: str) -> str:
    """Validate a blob name before constructing or opening its path."""
    if not isinstance(digest, str) or _DIGEST_PATTERN.fullmatch(digest) is None:
        raise ValueError("Blob digest must be exactly 64 lowercase hexadecimal characters")
    candidate = Path(digest)
    if (
        candidate.is_absolute()
        or candidate.name != digest
        or any(part in {".", ".."} for part in candidate.parts)
    ):
        raise ValueError("Blob digest must be a single path component")
    return digest


def _blob_source_error(message: str, path: Path) -> ValueError:
    return ValueError(f"Blob source {message}: {path}")


def _blob_file_error(message: str, path: Path) -> ValueError:
    return ValueError(f"File {message}: {path}")


def _file_identity(info: os.stat_result) -> FileIdentity:
    """Return stable metadata for replacement and in-place mutation checks."""
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
    )


def _open_regular_file(
    path: Path,
    *,
    expected_identity: FileIdentity | None = None,
    phase: str,
    error: FileError = _blob_source_error,
):
    """Open a path through the shared no-follow regular-file boundary."""
    return open_regular_file(
        Path(path),
        identity=_file_identity,
        expected_identity=expected_identity,
        phase=phase,
        error=error,
    )


def _open_regular_file_at(
    directory: SecureDirectory,
    name: str,
    *,
    expected_identity: FileIdentity | None = None,
    phase: str,
):
    """Open a final entry relative to an already-bound directory."""
    return _safe_open_regular_file_at(
        directory,
        name,
        identity=_file_identity,
        expected_identity=expected_identity,
        phase=phase,
        error=_blob_file_error,
    )


def _file_digest_and_size(
    path: Path,
    *,
    expected_identity: FileIdentity | None = None,
) -> tuple[str, int]:
    """Hash one regular file through one stable descriptor."""
    with _open_regular_file(
        path,
        expected_identity=expected_identity,
        phase="before hashing",
    ) as handle:
        digest, size, _ = digest_open_file(
            handle,
            path,
            identity=_file_identity,
            expected_identity=expected_identity,
            before_phase="before hashing",
            during_phase="during hashing",
            chunk_size=_CHUNK_SIZE,
            error=_blob_file_error,
        )
    return digest, size


def _file_digest_and_size_at(
    directory: SecureDirectory,
    name: str,
    *,
    expected_identity: FileIdentity | None = None,
) -> tuple[str, int]:
    """Hash a final entry without reopening its parent path by name."""
    path = directory.path / name
    with _open_regular_file_at(
        directory,
        name,
        expected_identity=expected_identity,
        phase="before hashing",
    ) as handle:
        digest, size, _ = digest_open_file(
            handle,
            path,
            identity=_file_identity,
            expected_identity=expected_identity,
            before_phase="before hashing",
            during_phase="during hashing",
            chunk_size=_CHUNK_SIZE,
            error=_blob_file_error,
        )
    return digest, size


def _read_file_verified(
    path: Path,
    *,
    expected_identity: FileIdentity | None = None,
) -> tuple[str, int, bytes]:
    """Hash and return one regular file through the same stable descriptor."""
    with _open_regular_file(
        path,
        expected_identity=expected_identity,
        phase="before reading",
    ) as handle:
        digest, size, data = digest_open_file(
            handle,
            path,
            identity=_file_identity,
            expected_identity=expected_identity,
            before_phase="before reading",
            during_phase="during reading",
            chunk_size=_CHUNK_SIZE,
            error=_blob_source_error,
            collect=True,
        )
    assert data is not None
    return digest, size, data


def _copy_open_file_verified(
    input_handle,
    output_handle,
    path: Path,
    *,
    expected_identity: FileIdentity,
    error: FileError,
) -> tuple[str, int]:
    """Copy an already-open source while hashing exactly the bytes written."""
    initial = verify_regular_descriptor(
        input_handle,
        path,
        identity=_file_identity,
        expected_identity=expected_identity,
        phase="before materialization",
        error=error,
    )
    initial_identity = _file_identity(initial)
    digestor = sha256()
    size = 0
    for chunk in iter_file_chunks(input_handle, _CHUNK_SIZE):
        digestor.update(chunk)
        output_handle.write(chunk)
        size += len(chunk)
    verify_regular_descriptor(
        input_handle,
        path,
        identity=_file_identity,
        expected_identity=initial_identity,
        phase="during materialization",
        error=error,
    )
    return digestor.hexdigest(), size


def _validated_blob_source(root: Path, digest: str) -> tuple[Path, FileIdentity]:
    """Validate a digest in a bound root before returning its path identity."""
    _validate_digest(digest)
    root = Path(root)
    with SecureDirectory(root, create=False) as directory:
        info = directory.stat(digest)
        if stat.S_ISLNK(info.st_mode):
            raise _blob_source_error("must not be a symlink", root / digest)
        if not stat.S_ISREG(info.st_mode):
            raise _blob_source_error("must reference a regular file", root / digest)
        identity = _file_identity(info)
    return root / digest, identity


def _verify_existing_blob_at(
    directory: SecureDirectory,
    digest: str,
    info: os.stat_result | None = None,
) -> None:
    """Verify an existing descriptor-bound destination before treating it as a hit."""
    if info is None:
        info = directory.stat(digest)
    if stat.S_ISLNK(info.st_mode):
        raise _blob_file_error("destination must not be a symlink", directory.path / digest)
    if not stat.S_ISREG(info.st_mode):
        raise _blob_file_error("destination must be a regular file", directory.path / digest)
    actual_digest, _size = _file_digest_and_size_at(
        directory,
        digest,
        expected_identity=_file_identity(info),
    )
    if actual_digest != digest:
        raise ValueError(f"Blob digest mismatch for {digest}: store content is {actual_digest}")


def _publish_if_absent(
    directory: SecureDirectory,
    temporary_name: str,
    digest: str,
) -> None:
    """Publish a completed blob without replacing a concurrent destination."""
    try:
        directory.link(temporary_name, digest)
    except FileExistsError:
        _verify_existing_blob_at(directory, digest)


class BlobStore:
    """Store immutable bytes under their SHA-256 digest."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def put(self, data: bytes) -> str:
        """Store bytes once and return their SHA-256 digest."""
        if not isinstance(data, bytes):
            raise TypeError("Blob data must be bytes")
        digest = sha256(data).hexdigest()
        with SecureDirectory(self.root, create=True) as directory:
            try:
                existing = directory.stat(digest)
            except FileNotFoundError:
                existing = None
            if existing is not None:
                _verify_existing_blob_at(directory, digest, existing)
                return digest

            descriptor, temporary_name = directory.temporary(".blob-")
            try:
                try:
                    with os.fdopen(descriptor, "wb", closefd=True) as handle:
                        descriptor = -1
                        handle.write(data)
                        handle.flush()
                        os.fsync(handle.fileno())
                finally:
                    if descriptor >= 0:
                        with suppress(OSError):
                            os.close(descriptor)

                directory.verify_bound()
                try:
                    existing = directory.stat(digest)
                except FileNotFoundError:
                    existing = None
                if existing is None:
                    _publish_if_absent(directory, temporary_name, digest)
                else:
                    _verify_existing_blob_at(directory, digest, existing)
            finally:
                if descriptor >= 0:
                    with suppress(OSError):
                        os.close(descriptor)
                if temporary_name:
                    directory.unlink(temporary_name, missing_ok=True)
        return digest

    def ingest(self, path: Path) -> tuple[str, int]:
        """Copy a source file into the store while hashing it in bounded memory."""
        source = Path(path)
        digestor = sha256()
        size = 0
        with (
            SecureDirectory(self.root, create=True) as directory,
            _open_regular_file(source, phase="before ingest") as input_handle,
        ):
            initial = verify_regular_descriptor(
                input_handle,
                source,
                identity=_file_identity,
                expected_identity=None,
                phase="before ingest",
                error=_blob_source_error,
            )
            initial_identity = _file_identity(initial)
            descriptor, temporary_name = directory.temporary(".blob-")
            try:
                try:
                    with os.fdopen(descriptor, "wb", closefd=True) as output_handle:
                        descriptor = -1
                        for chunk in iter_file_chunks(input_handle, _CHUNK_SIZE):
                            digestor.update(chunk)
                            output_handle.write(chunk)
                            size += len(chunk)
                        output_handle.flush()
                        os.fsync(output_handle.fileno())
                finally:
                    if descriptor >= 0:
                        with suppress(OSError):
                            os.close(descriptor)

                verify_regular_descriptor(
                    input_handle,
                    source,
                    identity=_file_identity,
                    expected_identity=initial_identity,
                    phase="during ingest",
                    error=_blob_source_error,
                )
                digest = digestor.hexdigest()
                directory.verify_bound()
                try:
                    existing = directory.stat(digest)
                except FileNotFoundError:
                    existing = None
                if existing is None:
                    _publish_if_absent(directory, temporary_name, digest)
                else:
                    _verify_existing_blob_at(directory, digest, existing)
            finally:
                if descriptor >= 0:
                    with suppress(OSError):
                        os.close(descriptor)
                if temporary_name:
                    directory.unlink(temporary_name, missing_ok=True)
        return digestor.hexdigest(), size

    def materialize(self, digest: str, destination: Path) -> None:
        """Copy and verify stored content through one descriptor, then publish atomically."""
        source, identity = _validated_blob_source(self.root, digest)
        target = Path(destination)
        with SecureDirectory(target.parent, create=True) as parent:
            parent.verify_bound()
            try:
                target_info = parent.stat(target.name)
            except FileNotFoundError:
                target_info = None
            if target_info is not None and stat.S_ISLNK(target_info.st_mode):
                raise ValueError(f"Materialization target must not be a symlink: {target}")
            if target_info is not None and stat.S_ISDIR(target_info.st_mode):
                raise IsADirectoryError(f"Cannot replace output directory: {target}")

            descriptor, temporary_name = parent.temporary(f".{target.name}.")
            try:
                with (
                    _open_regular_file(
                        source,
                        expected_identity=identity,
                        phase="during materialization",
                    ) as input_handle,
                    os.fdopen(descriptor, "wb", closefd=True) as output_handle,
                ):
                    descriptor = -1
                    actual_digest, size = _copy_open_file_verified(
                        input_handle,
                        output_handle,
                        source,
                        expected_identity=identity,
                        error=_blob_source_error,
                    )
                    output_handle.flush()
                    os.fsync(output_handle.fileno())
                if actual_digest != digest:
                    raise ValueError(
                        f"Blob digest mismatch for {digest}: materialized source is {actual_digest}"
                    )
                parent.verify_bound()
                try:
                    target_info = parent.stat(target.name)
                except FileNotFoundError:
                    target_info = None
                if target_info is not None and stat.S_ISLNK(target_info.st_mode):
                    raise ValueError(f"Materialization target must not be a symlink: {target}")
                if target_info is not None and stat.S_ISDIR(target_info.st_mode):
                    raise IsADirectoryError(f"Cannot replace output directory: {target}")
                if target_info is not None and stat.S_ISREG(target_info.st_mode):
                    target_digest, target_size = _file_digest_and_size_at(
                        parent,
                        target.name,
                        expected_identity=_file_identity(target_info),
                    )
                    if target_size == size and target_digest == digest:
                        same_inode = (target_info.st_dev, target_info.st_ino) == identity[:2]
                        if not same_inode:
                            return
                rename_at(
                    temporary_name,
                    target.name,
                    source_directory=parent,
                    destination_directory=parent,
                )
                temporary_name = ""
            finally:
                if descriptor >= 0:
                    with suppress(OSError):
                        os.close(descriptor)
                if temporary_name:
                    parent.unlink(temporary_name, missing_ok=True)

    def read_verified(self, digest: str) -> bytes:
        """Read immutable bytes only after verifying their content address."""
        source, identity = _validated_blob_source(self.root, digest)
        actual_digest, _size, data = _read_file_verified(source, expected_identity=identity)
        if actual_digest != digest:
            raise ValueError(f"Blob digest mismatch for {digest}: store content is {actual_digest}")
        return data

    def gc(self, referenced: set[str]) -> int:
        """Delete unreferenced regular blobs without following root or child symlinks."""
        try:
            directory_context = SecureDirectory(self.root, create=False)
            directory = directory_context.__enter__()
        except FileNotFoundError:
            return 0
        try:
            removed = 0
            for name in directory.names():
                if name in referenced:
                    continue
                try:
                    info = directory.stat(name)
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(info.st_mode):
                    continue
                directory.unlink(name)
                removed += 1
            return removed
        finally:
            directory.close()


# Kept as a narrow compatibility alias for the materialization transaction in store.py and
# focused tests; the implementation itself lives only in _safe_io.py.
_rename_at = rename_at
