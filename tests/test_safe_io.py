from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

import kigumi._safe_io as safe_io
from kigumi import BlobStore


def test_secure_atomic_write_accepts_a_normal_tempfile_path() -> None:
    with tempfile.TemporaryDirectory(prefix="kigumi-safe-io-") as directory:
        destination = Path(directory) / "nested" / "entry.txt"

        safe_io.secure_atomic_write_text(destination, "safe")

        assert destination.read_text(encoding="utf-8") == "safe"


def test_secure_atomic_write_rejects_a_user_symlink_parent(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    linked_parent = tmp_path / "linked-parent"
    try:
        linked_parent.symlink_to(outside, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support directory symlinks: {error}")

    with pytest.raises(ValueError, match="symlink"):
        safe_io.secure_atomic_write_text(linked_parent / "escape.txt", "must stay inside")

    assert not (outside / "escape.txt").exists()


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS root aliases are not present")
@pytest.mark.parametrize(
    ("alias", "canonical"),
    (("/tmp", "/private/tmp"), ("/var", "/private/var")),
)
def test_macos_system_temp_aliases_are_supported_by_cache_and_blob_paths(
    alias: str, canonical: str
) -> None:
    alias_path = Path(alias)
    if not alias_path.is_symlink() or alias_path.resolve(strict=True) != Path(canonical):
        pytest.skip(f"{alias} is not the expected macOS system alias")

    # /var itself is not writable on macOS; tempfile uses the writable
    # /var/folders subtree.  /tmp is a writable root-level alias.
    temp_directory = tempfile.TemporaryDirectory(
        dir=alias if alias == "/tmp" else None,
        prefix="kigumi-safe-io-",
    )
    with temp_directory as directory:
        root = Path(directory)
        if alias_path not in root.parents:
            pytest.skip(f"temporary directory did not retain the {alias} spelling")

        safe_io.secure_atomic_write_text(root / "cache" / "entry.txt", "safe")

        store = BlobStore(root / "blobs")
        digest = store.put(b"safe blob")
        store.materialize(digest, root / "output" / "result.bin")

        assert (root / "cache" / "entry.txt").read_text(encoding="utf-8") == "safe"
        assert (root / "output" / "result.bin").read_bytes() == b"safe blob"
