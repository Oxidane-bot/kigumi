"""二进制交付物的内容寻址仓。

节点函数自己写二进制文件是缓存看不见的副作用：缓存命中时函数不会执行，
交付物却会凭空消失而 run 仍显示成功。blob 仓把字节与 artifact 引用分离，
使命中路径也能重新物化同一份已校验内容。
"""

from __future__ import annotations

import errno
import os
import re
import stat
import tempfile
from contextlib import suppress
from hashlib import sha256
from pathlib import Path
from secrets import token_hex

from ._safe_io import (
    FileError,
    FileIdentity,
    _secure_directory_absolute,
    digest_open_file,
    iter_file_chunks,
    lstat_regular_file,
    open_regular_file,
    verify_regular_descriptor,
)

_CHUNK_SIZE = 1024 * 1024
_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


def _secure_directory_supported() -> bool:
    """Return whether this interpreter exposes the directory-relative primitives we need."""
    supported = getattr(os, "supports_dir_fd", ())
    return (
        all(
            operation in supported
            for operation in (os.open, os.mkdir, os.rename, os.stat, os.unlink, os.rmdir)
        )
        and hasattr(os, "O_DIRECTORY")
        and hasattr(os, "O_NOFOLLOW")
    )


def _secure_materialization_error(path: Path) -> OSError:
    """Build the explicit fail-closed error for platforms without safe directory I/O."""
    return OSError(
        errno.ENOTSUP,
        "Secure materialization requires descriptor-relative directory I/O: " + str(path),
    )


class _SecureDirectory:
    """Keep a destination directory bound to no-follow descriptors while mutating it.

    The descriptor-relative backend is deliberately fail-closed.  A path check followed by
    ``mkdir``/``rename`` is not an equivalent fallback: another process can replace a checked
    component with a symlink in between those operations.
    """

    def __init__(self, path: Path, *, create: bool) -> None:
        self.path = Path(path)
        self.create = create
        self.fd = -1
        self._fds: list[int] = []
        self._created: list[tuple[int, str]] = []
        self._absolute = _secure_directory_absolute(self.path)

    def __enter__(self) -> _SecureDirectory:
        if not _secure_directory_supported():
            raise _secure_materialization_error(self.path)

        absolute = self._absolute
        if any(part == ".." for part in absolute.parts):
            raise ValueError(f"Materialization destination must not contain '..': {self.path}")

        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        try:
            current_fd = os.open(absolute.anchor, flags)
            self._fds.append(current_fd)
            for component in absolute.parts[1:]:
                if component in {"", "."}:
                    continue
                try:
                    next_fd = os.open(component, flags, dir_fd=current_fd)
                except FileNotFoundError:
                    if not self.create:
                        raise
                    os.mkdir(component, 0o777, dir_fd=current_fd)
                    self._created.append((current_fd, component))
                    try:
                        next_fd = os.open(component, flags, dir_fd=current_fd)
                    except OSError as caught:
                        if self._is_symlink(current_fd, component):
                            raise ValueError(
                                f"Materialization destination must not contain a symlink: "
                                f"{absolute.parent / component}"
                            ) from caught
                        raise
                except OSError as caught:
                    if caught.errno == errno.ELOOP or self._is_symlink(current_fd, component):
                        raise ValueError(
                            f"Materialization destination must not contain a symlink: "
                            f"{absolute.parent / component}"
                        ) from caught
                    raise
                self._fds.append(next_fd)
                current_fd = next_fd
            self.fd = current_fd
            self.verify_bound()
            return self
        except BaseException:
            self.remove_created()
            self.close()
            raise

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()

    def close(self) -> None:
        """Close every descriptor retained while walking the directory path."""
        for descriptor in reversed(self._fds):
            with suppress(OSError):
                os.close(descriptor)
        self._fds.clear()
        self.fd = -1

    def verify_bound(self) -> None:
        """Reject a directory path that no longer names the opened directory."""
        if self.fd < 0:
            raise RuntimeError("Secure directory is not open")
        try:
            named = self._absolute.lstat()
        except FileNotFoundError as caught:
            raise ValueError(
                f"Materialization destination directory changed: {self.path}"
            ) from caught
        opened = os.fstat(self.fd)
        if (
            not stat.S_ISDIR(named.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise ValueError(f"Materialization destination directory changed: {self.path}")

    def stat(self, name: str) -> os.stat_result:
        """Inspect one final entry without following a symlink."""
        self._verify_name(name)
        return os.stat(name, dir_fd=self.fd, follow_symlinks=False)

    def temporary(self, prefix: str) -> tuple[int, str]:
        """Create a private temporary file relative to the bound directory."""
        self.verify_bound()
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        for _ in range(128):
            name = f"{prefix}{token_hex(12)}"
            try:
                return os.open(name, flags, 0o600, dir_fd=self.fd), name
            except FileExistsError:
                continue
        raise FileExistsError(f"Could not allocate a temporary file in {self.path}")

    def unlink(self, name: str, *, missing_ok: bool = False) -> None:
        """Remove one final entry without resolving any parent path string."""
        self._verify_name(name)
        try:
            os.unlink(name, dir_fd=self.fd)
        except FileNotFoundError:
            if not missing_ok:
                raise

    def remove_created(self) -> None:
        """Remove only directories this handle created, deepest first."""
        for parent_fd, name in reversed(self._created):
            with suppress(OSError):
                os.rmdir(name, dir_fd=parent_fd)
        self._created.clear()

    @staticmethod
    def _verify_name(name: str) -> None:
        if not name or Path(name).name != name or name in {".", ".."}:
            raise ValueError(f"Expected one output path component, got {name!r}")

    @staticmethod
    def _is_symlink(parent_fd: int, name: str) -> bool:
        """Check one entry without following it when an open operation failed."""
        try:
            info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError:
            return False
        return stat.S_ISLNK(info.st_mode)


def _rename_at(
    source: str | Path,
    destination: str,
    *,
    source_directory: _SecureDirectory | None = None,
    destination_directory: _SecureDirectory | None = None,
) -> None:
    """Atomically rename using directory descriptors for every project-side path."""
    os.rename(
        source,
        destination,
        src_dir_fd=None if source_directory is None else source_directory.fd,
        dst_dir_fd=None if destination_directory is None else destination_directory.fd,
    )


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
    """Return identity fields unaffected by temporary hard-link cleanup.

    ``st_ctime_ns`` changes when a publisher adds and removes the temporary
    hard link, even though the descriptor still points at the same immutable
    bytes. Device/inode, size, and mtime remain descriptor-bound checks for
    replacement and ordinary in-place mutation; callers also hash the
    descriptor contents before accepting a blob.
    """
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
    )


def _validated_blob_source(root: Path, digest: str) -> tuple[Path, FileIdentity]:
    """Resolve a validated digest to a non-symlink regular file without opening it."""
    _validate_digest(digest)
    source = root / digest
    info = lstat_regular_file(source, error=_blob_source_error)
    return source, _file_identity(info)


def _reject_symlink_components(path: Path) -> None:
    """Reject a destination containing a symlink before creating anything."""
    path = _secure_directory_absolute(Path(path))
    if path.is_absolute():
        current = Path(path.anchor)
        components = path.parts[1:]
    else:
        current = Path.cwd()
        components = path.parts

    for component in components:
        if component in {"", "."}:
            continue
        current /= component
        try:
            info = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f"Materialization destination must not contain a symlink: {current}")


def _open_regular_file(
    path: Path,
    *,
    expected_identity: FileIdentity | None = None,
    phase: str,
    error: FileError = _blob_source_error,
):
    """Open a path through a non-blocking descriptor bound to a regular file."""
    return open_regular_file(
        path,
        identity=_file_identity,
        expected_identity=expected_identity,
        phase=phase,
        error=error,
    )


def _verify_existing_blob(
    destination: Path,
    digest: str,
    info: os.stat_result | None = None,
) -> None:
    """Verify an existing destination before treating it as an immutable hit."""
    if info is None:
        info = destination.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"Blob destination must be a regular file: {destination}")
    actual_digest, _size = _file_digest_and_size(
        destination,
        expected_identity=_file_identity(info),
    )
    if actual_digest != digest:
        raise ValueError(f"Blob digest mismatch for {digest}: store content is {actual_digest}")


class BlobStore:
    """Store immutable bytes under their SHA-256 digest."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def put(self, data: bytes) -> str:
        """Store bytes once and return their SHA-256 digest."""
        if not isinstance(data, bytes):
            raise TypeError("Blob data must be bytes")
        digest = sha256(data).hexdigest()
        destination = self.root / digest
        try:
            existing = destination.lstat()
        except FileNotFoundError:
            existing = None
        if existing is not None:
            _verify_existing_blob(destination, digest, existing)
            return digest
        self.root.mkdir(parents=True, exist_ok=True)
        temporary = self._temporary_file()
        try:
            with temporary.open("wb") as handle:
                handle.write(data)
            try:
                existing = destination.lstat()
            except FileNotFoundError:
                existing = None
            if existing is None:
                self._publish_if_absent(temporary, destination, digest)
            else:
                _verify_existing_blob(destination, digest, existing)
        finally:
            temporary.unlink(missing_ok=True)
        return digest

    def ingest(self, path: Path) -> tuple[str, int]:
        """Copy a source file into the store while hashing it in bounded memory."""
        source = Path(path)
        self.root.mkdir(parents=True, exist_ok=True)
        temporary = self._temporary_file()
        digestor = sha256()
        size = 0
        try:
            with (
                _open_regular_file(source, phase="before ingest") as input_handle,
                temporary.open("wb") as output_handle,
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
                for chunk in iter_file_chunks(input_handle, _CHUNK_SIZE):
                    digestor.update(chunk)
                    output_handle.write(chunk)
                    size += len(chunk)
                verify_regular_descriptor(
                    input_handle,
                    source,
                    identity=_file_identity,
                    expected_identity=initial_identity,
                    phase="during ingest",
                    error=_blob_source_error,
                )
            digest = digestor.hexdigest()
            destination = self.root / digest
            try:
                existing = destination.lstat()
            except FileNotFoundError:
                existing = None
            if existing is None:
                self._publish_if_absent(temporary, destination, digest)
            else:
                _verify_existing_blob(destination, digest, existing)
        finally:
            temporary.unlink(missing_ok=True)
        return digestor.hexdigest(), size

    @staticmethod
    def _publish_if_absent(temporary: Path, destination: Path, digest: str) -> None:
        """Publish a completed blob without replacing a concurrent destination."""
        try:
            os.link(temporary, destination)
        except FileExistsError:
            _verify_existing_blob(destination, digest)

    def materialize(self, digest: str, destination: Path) -> None:
        """Copy and verify stored content through one descriptor, then publish atomically."""
        source, identity = _validated_blob_source(self.root, digest)
        target = Path(destination)
        _reject_symlink_components(target)
        with _SecureDirectory(target.parent, create=True) as parent:
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
                _rename_at(
                    temporary_name,
                    target.name,
                    source_directory=parent,
                    destination_directory=parent,
                )
                temporary_name = ""
            finally:
                if descriptor >= 0:
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
        """Delete stored blobs that no retained artifact references."""
        if not self.root.is_dir():
            return 0
        removed = 0
        for path in self.root.iterdir():
            if path.is_file() and path.name not in referenced:
                path.unlink()
                removed += 1
        return removed

    def _temporary_file(self) -> Path:
        descriptor, name = tempfile.mkstemp(prefix=".blob-", dir=self.root)
        os.close(descriptor)
        return Path(name)


def _file_digest_and_size(
    path: Path,
    *,
    expected_identity: FileIdentity | None = None,
) -> tuple[str, int]:
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


def _open_regular_file_at(
    directory: _SecureDirectory,
    name: str,
    *,
    expected_identity: FileIdentity | None = None,
    phase: str,
):
    """Open a final entry relative to an already-bound destination directory."""
    directory._verify_name(name)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    path = directory.path / name
    try:
        descriptor = os.open(name, flags, dir_fd=directory.fd)
    except OSError as caught:
        if caught.errno == errno.ELOOP:
            raise _blob_file_error("must not be a symlink", path) from caught
        raise
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise _blob_file_error("must reference a regular file", path)
        if expected_identity is not None and _file_identity(opened) != expected_identity:
            raise _blob_file_error(f"changed {phase}", path)
        return os.fdopen(descriptor, "rb", closefd=True)
    except BaseException:
        os.close(descriptor)
        raise


def _file_digest_and_size_at(
    directory: _SecureDirectory,
    name: str,
    *,
    expected_identity: FileIdentity | None = None,
) -> tuple[str, int]:
    """Hash a destination entry without reopening its parent path by name."""
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
