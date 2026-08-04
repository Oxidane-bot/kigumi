"""Private descriptor-bound primitives for safe file reads and atomic writes."""

from __future__ import annotations

import errno
import os
import stat
from collections.abc import Callable, Iterator
from contextlib import suppress
from hashlib import sha256
from pathlib import Path
from secrets import token_hex
from typing import Any, BinaryIO, TypeAlias

FileIdentity: TypeAlias = tuple[int, ...]
FileError: TypeAlias = Callable[[str, Path], BaseException]
IdentityFactory: TypeAlias = Callable[[os.stat_result], FileIdentity]
_ORIGINAL_OS_LINK = os.link
_ORIGINAL_OS_OPEN = os.open
_ORIGINAL_OS_MKDIR = os.mkdir
_ORIGINAL_OS_RENAME = os.rename
_ORIGINAL_OS_STAT = os.stat
_ORIGINAL_OS_UNLINK = os.unlink
_ORIGINAL_OS_RMDIR = os.rmdir
_ORIGINAL_OS_FSTAT = os.fstat

# macOS exposes these directories as root-level symlinks.  They are stable
# system aliases, unlike a symlink introduced anywhere inside a project path.
# Keep the allowlist explicit: resolving arbitrary path components here would
# undo the no-follow boundary this module is intended to provide.
_SYSTEM_DIRECTORY_ALIASES = tuple(
    (Path(os.sep, name), Path(os.sep, "private", name)) for name in ("etc", "tmp", "var")
)


def _secure_directory_absolute(path: Path) -> Path:
    """Return a lexical absolute path with only trusted macOS aliases normalized."""
    absolute = Path(path).absolute()
    if os.name != "posix":
        return absolute

    for alias, canonical in _SYSTEM_DIRECTORY_ALIASES:
        try:
            suffix = absolute.relative_to(alias)
        except ValueError:
            continue

        try:
            alias_info = alias.lstat()
        except FileNotFoundError:
            return absolute
        if not stat.S_ISLNK(alias_info.st_mode):
            return absolute

        try:
            resolved = alias.resolve(strict=True)
        except OSError as caught:
            raise ValueError(
                f"Secure directory must not contain an untrusted system alias: {alias}"
            ) from caught
        if resolved != canonical:
            raise ValueError(
                f"Secure directory must not contain an untrusted system alias: {alias}"
            )
        return canonical.joinpath(*suffix.parts)

    return absolute


def _secure_directory_supported() -> bool:
    """Return whether descriptor-relative no-follow directory I/O is available."""
    supported = getattr(os, "supports_dir_fd", ())
    return (
        all(
            operation in supported
            for operation in (
                _ORIGINAL_OS_OPEN,
                _ORIGINAL_OS_MKDIR,
                _ORIGINAL_OS_RENAME,
                _ORIGINAL_OS_STAT,
                _ORIGINAL_OS_UNLINK,
                _ORIGINAL_OS_RMDIR,
            )
        )
        and hasattr(os, "O_DIRECTORY")
        and hasattr(os, "O_NOFOLLOW")
    )


def _secure_directory_error(path: Path) -> OSError:
    """Build the explicit fail-closed error for unsupported secure directory I/O."""
    return OSError(
        errno.ENOTSUP,
        "Secure directory I/O requires descriptor-relative directory I/O and no-follow operations: "
        + str(path),
    )


def _default_file_identity(info: os.stat_result) -> FileIdentity:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def _default_file_error(message: str, path: Path) -> ValueError:
    return ValueError(f"File {message}: {path}")


class SecureDirectory:
    """Bind a path to descriptor-relative, no-follow directory operations.

    A lexical ``lstat`` followed by a path-based write is not a security boundary:
    another process can replace any checked parent with a symlink.  This class opens
    every component with ``O_NOFOLLOW`` and keeps the resulting descriptors for all
    subsequent operations.  Platforms without those primitives fail closed with
    ``ENOTSUP`` instead of falling back to a racy path check.
    """

    def __init__(self, path: Path, *, create: bool) -> None:
        self.path = Path(path)
        self.create = create
        self.fd = -1
        self._fds: list[int] = []
        self._created: list[tuple[int, str]] = []
        self._absolute = _secure_directory_absolute(self.path)

    def __enter__(self) -> SecureDirectory:
        if not _secure_directory_supported():
            raise _secure_directory_error(self.path)
        if any(part == ".." for part in self._absolute.parts):
            raise ValueError(f"Secure directory path must not contain '..': {self.path}")

        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        try:
            current_fd = os.open(self._absolute.anchor, flags)
            self._fds.append(current_fd)
            for component in self._absolute.parts[1:]:
                if component in {"", "."}:
                    continue
                try:
                    next_fd = os.open(component, flags, dir_fd=current_fd)
                except FileNotFoundError:
                    if not self.create:
                        raise
                    created = False
                    try:
                        os.mkdir(component, 0o777, dir_fd=current_fd)
                    except FileExistsError:
                        pass
                    else:
                        created = True
                    if created:
                        self._created.append((current_fd, component))
                    try:
                        next_fd = os.open(component, flags, dir_fd=current_fd)
                    except OSError as caught:
                        if self._is_symlink(current_fd, component):
                            raise ValueError(
                                "Secure directory must not contain a symlink: "
                                f"{self._absolute.parent / component}"
                            ) from caught
                        raise
                except OSError as caught:
                    if caught.errno == errno.ELOOP or self._is_symlink(current_fd, component):
                        raise ValueError(
                            "Secure directory must not contain a symlink: "
                            f"{self._absolute.parent / component}"
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
            raise ValueError(f"Secure directory changed: {self.path}") from caught
        opened = _ORIGINAL_OS_FSTAT(self.fd)
        if (
            not stat.S_ISDIR(named.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise ValueError(f"Secure directory changed: {self.path}")

    def stat(self, name: str) -> os.stat_result:
        """Inspect one final entry without following a symlink."""
        self._verify_name(name)
        self.verify_bound()
        return os.stat(name, dir_fd=self.fd, follow_symlinks=False)

    def names(self) -> list[str]:
        """List direct child names from the already-bound directory descriptor."""
        self.verify_bound()
        return os.listdir(self.fd)

    def mkdir(self, name: str, *, mode: int = 0o777) -> None:
        """Create one child directory without following a replacement symlink."""
        self._verify_name(name)
        self.verify_bound()
        os.mkdir(name, mode, dir_fd=self.fd)

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

    def rename(self, source: str, destination: str) -> None:
        """Atomically rename two entries in this bound directory."""
        self._verify_name(source)
        self._verify_name(destination)
        self.verify_bound()
        os.rename(source, destination, src_dir_fd=self.fd, dst_dir_fd=self.fd)

    def link(self, source: str, destination: str) -> None:
        """Create a hard link between two entries in this bound directory."""
        supported = getattr(os, "supports_dir_fd", ())
        follow_symlinks = getattr(os, "supports_follow_symlinks", ())
        if _ORIGINAL_OS_LINK not in supported or _ORIGINAL_OS_LINK not in follow_symlinks:
            raise _secure_directory_error(self.path)
        self._verify_name(source)
        self._verify_name(destination)
        self.verify_bound()
        os.link(
            source,
            destination,
            src_dir_fd=self.fd,
            dst_dir_fd=self.fd,
            follow_symlinks=False,
        )

    def unlink(self, name: str, *, missing_ok: bool = False) -> None:
        """Remove one final entry without resolving a parent path string."""
        self._verify_name(name)
        self.verify_bound()
        try:
            os.unlink(name, dir_fd=self.fd)
        except FileNotFoundError:
            if not missing_ok:
                raise

    def remove_created(self) -> None:
        """Remove only directories created by this handle, deepest first."""
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
        try:
            info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError:
            return False
        return stat.S_ISLNK(info.st_mode)


def rename_at(
    source: str | Path,
    destination: str | Path,
    *,
    source_directory: SecureDirectory | None = None,
    destination_directory: SecureDirectory | None = None,
) -> None:
    """Rename using bound descriptors for any managed directory side."""
    source_name = str(source)
    destination_name = str(destination)
    if source_directory is not None:
        source_directory._verify_name(source_name)
        source_directory.verify_bound()
    if destination_directory is not None:
        destination_directory._verify_name(destination_name)
        destination_directory.verify_bound()
    os.rename(
        source_name,
        destination_name,
        src_dir_fd=None if source_directory is None else source_directory.fd,
        dst_dir_fd=None if destination_directory is None else destination_directory.fd,
    )


def _write_target_error(directory: SecureDirectory, name: str) -> BaseException | None:
    """Return an error for a final write target that is not a regular file."""
    try:
        info = directory.stat(name)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode):
        return ValueError(f"Write target must not be a symlink: {directory.path / name}")
    if stat.S_ISDIR(info.st_mode):
        return IsADirectoryError(f"Cannot replace write target directory: {directory.path / name}")
    if not stat.S_ISREG(info.st_mode):
        return ValueError(f"Write target must be a regular file: {directory.path / name}")
    return None


def secure_atomic_write_text(path: str | Path, text: str) -> None:
    """Atomically write UTF-8 text through a descriptor-bound parent directory."""
    if not isinstance(text, str):
        raise TypeError("atomic text writes require str data")
    destination = Path(path)
    with SecureDirectory(destination.parent, create=True) as parent:
        target_error = _write_target_error(parent, destination.name)
        if target_error is not None:
            raise target_error

        descriptor, temporary_name = parent.temporary(f".{destination.name}.")
        try:
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8", closefd=True) as handle:
                    descriptor = -1
                    handle.write(text)
                    handle.flush()
                    os.fsync(handle.fileno())
            finally:
                if descriptor >= 0:
                    with suppress(OSError):
                        os.close(descriptor)

            parent.verify_bound()
            target_error = _write_target_error(parent, destination.name)
            if target_error is not None:
                raise target_error
            parent.rename(temporary_name, destination.name)
            temporary_name = ""
        finally:
            if descriptor >= 0:
                with suppress(OSError):
                    os.close(descriptor)
            if temporary_name:
                parent.unlink(temporary_name, missing_ok=True)


def secure_atomic_write_json(path: str | Path, obj: Any) -> None:
    """Atomically write canonical JSON through :func:`secure_atomic_write_text`."""
    # Import lazily so the safe primitives remain usable while artifacts.py is importing.
    from .artifacts import canonical_json

    secure_atomic_write_text(path, canonical_json(obj))


def _open_regular_file_at(
    directory: SecureDirectory,
    name: str,
    *,
    identity: IdentityFactory = _default_file_identity,
    expected_identity: FileIdentity | None = None,
    phase: str,
    error: FileError = _default_file_error,
) -> BinaryIO:
    """Open one regular entry relative to a bound directory without following a symlink."""
    directory._verify_name(name)
    directory.verify_bound()
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


# Keep the low-level names discoverable for storage code that wants one shared write boundary.
atomic_write_text = secure_atomic_write_text
atomic_write_json = secure_atomic_write_json


def lstat_regular_file(
    path: Path,
    *,
    error: FileError,
) -> os.stat_result:
    """Reject symlinks and special files before any descriptor is opened."""
    path = Path(path)
    # Keep this lexical probe as an early error and race hook.  The
    # descriptor-relative stat below is authoritative for the parent path.
    try:
        lexical = path.lstat()
    except FileNotFoundError:
        lexical = None
    if lexical is not None and stat.S_ISLNK(lexical.st_mode):
        raise error("must not be a symlink", path)
    with SecureDirectory(path.parent, create=False) as directory:
        info = directory.stat(path.name)
    if stat.S_ISLNK(info.st_mode):
        raise error("must not be a symlink", path)
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
    initial = lstat_regular_file(path, error=error)
    with SecureDirectory(Path(path).parent, create=False) as directory:
        return _open_regular_file_at(
            directory,
            Path(path).name,
            identity=identity,
            expected_identity=expected_identity or identity(initial),
            phase=phase,
            error=error,
        )


def read_regular_bytes(
    path: str | Path,
    *,
    identity: IdentityFactory = _default_file_identity,
    expected_identity: FileIdentity | None = None,
    phase: str,
    error: FileError = _default_file_error,
) -> bytes:
    """Read one regular file through the shared descriptor-relative boundary."""
    with open_regular_file(
        Path(path),
        identity=identity,
        expected_identity=expected_identity,
        phase=phase,
        error=error,
    ) as handle:
        return handle.read()


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
