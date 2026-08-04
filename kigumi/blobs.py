"""二进制交付物的内容寻址仓。

节点函数自己写二进制文件是缓存看不见的副作用：缓存命中时函数不会执行，
交付物却会凭空消失而 run 仍显示成功。blob 仓把字节与 artifact 引用分离，
使命中路径也能重新物化同一份已校验内容。
"""

from __future__ import annotations

import os
import re
import stat
import tempfile
from hashlib import sha256
from pathlib import Path

from ._safe_io import (
    FileError,
    FileIdentity,
    digest_open_file,
    iter_file_chunks,
    lstat_regular_file,
    open_regular_file,
    verify_regular_descriptor,
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
    path = Path(path)
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
        """Verify stored content, then atomically copy it to its project destination."""
        source, identity = _validated_blob_source(self.root, digest)
        actual_digest, size = _file_digest_and_size(source, expected_identity=identity)
        if actual_digest != digest:
            raise ValueError(f"Blob digest mismatch for {digest}: store content is {actual_digest}")
        target = Path(destination)
        _reject_symlink_components(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        _reject_symlink_components(target)
        try:
            target_info = target.lstat()
        except FileNotFoundError:
            target_info = None
        if target_info is not None and stat.S_ISLNK(target_info.st_mode):
            raise ValueError(f"Materialization target must not be a symlink: {target}")
        if target_info is not None and stat.S_ISREG(target_info.st_mode):
            target_digest, target_size = _file_digest_and_size(target)
            if target_size == size and target_digest == digest:
                same_inode = (target_info.st_dev, target_info.st_ino) == identity[:2]
                if not same_inode:
                    return
        temporary = self._temporary_destination(target)
        try:
            with (
                _open_regular_file(
                    source,
                    expected_identity=identity,
                    phase="during materialization",
                ) as input_handle,
                temporary.open("wb") as output_handle,
            ):
                verify_regular_descriptor(
                    input_handle,
                    source,
                    identity=_file_identity,
                    expected_identity=identity,
                    phase="during materialization",
                    error=_blob_source_error,
                )
                for chunk in iter_file_chunks(input_handle, _CHUNK_SIZE):
                    output_handle.write(chunk)
                verify_regular_descriptor(
                    input_handle,
                    source,
                    identity=_file_identity,
                    expected_identity=identity,
                    phase="during materialization",
                    error=_blob_source_error,
                )
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

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

    @staticmethod
    def _temporary_destination(destination: Path) -> Path:
        descriptor, name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
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
