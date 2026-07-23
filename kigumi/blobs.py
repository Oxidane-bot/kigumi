"""二进制交付物的内容寻址仓。

节点函数自己写二进制文件是缓存看不见的副作用：缓存命中时函数不会执行，
交付物却会凭空消失而 run 仍显示成功。blob 仓把字节与 artifact 引用分离，
使命中路径也能重新物化同一份已校验内容。
"""

from __future__ import annotations

import os
import shutil
import tempfile
from hashlib import sha256
from pathlib import Path

_CHUNK_SIZE = 1024 * 1024


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
        if destination.is_file():
            return digest
        self.root.mkdir(parents=True, exist_ok=True)
        temporary = self._temporary_file()
        try:
            with temporary.open("wb") as handle:
                handle.write(data)
            if not destination.exists():
                os.replace(temporary, destination)
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
            with source.open("rb") as input_handle, temporary.open("wb") as output_handle:
                while chunk := input_handle.read(_CHUNK_SIZE):
                    digestor.update(chunk)
                    output_handle.write(chunk)
                    size += len(chunk)
            digest = digestor.hexdigest()
            destination = self.root / digest
            if not destination.exists():
                os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return digestor.hexdigest(), size

    def materialize(self, digest: str, destination: Path) -> None:
        """Verify stored content, then atomically copy it to its project destination."""
        source = self.root / digest
        actual_digest, size = _file_digest_and_size(source)
        if actual_digest != digest:
            raise ValueError(f"Blob digest mismatch for {digest}: store content is {actual_digest}")
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_file():
            target_digest, target_size = _file_digest_and_size(target)
            if target_size == size and target_digest == digest:
                return
        temporary = self._temporary_destination(target)
        try:
            with source.open("rb") as input_handle, temporary.open("wb") as output_handle:
                shutil.copyfileobj(input_handle, output_handle, _CHUNK_SIZE)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    def read_verified(self, digest: str) -> bytes:
        """Read immutable bytes only after verifying their content address."""
        source = self.root / digest
        actual_digest, _size = _file_digest_and_size(source)
        if actual_digest != digest:
            raise ValueError(f"Blob digest mismatch for {digest}: store content is {actual_digest}")
        return source.read_bytes()

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


def _file_digest_and_size(path: Path) -> tuple[str, int]:
    digestor = sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK_SIZE):
            digestor.update(chunk)
            size += len(chunk)
    return digestor.hexdigest(), size
