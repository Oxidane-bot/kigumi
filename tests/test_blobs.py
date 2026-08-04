from __future__ import annotations

import os
import stat
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from pathlib import Path
from threading import Barrier, Event
from types import SimpleNamespace

import pytest

import kigumi.blobs as blobs
from kigumi import BlobStore


def test_put_and_ingest_deduplicate_identical_content(tmp_path: Path) -> None:
    """教训 blob_dedup: 同一字节重复收编不能膨胀仓容量。"""
    store = BlobStore(tmp_path / "blobs")
    source = tmp_path / "source.bin"
    source.write_bytes(b"same binary payload")

    first = store.put(b"same binary payload")
    second = store.put(b"same binary payload")
    ingested_first, size_first = store.ingest(source)
    ingested_second, size_second = store.ingest(source)

    assert first == second == ingested_first == ingested_second
    assert size_first == size_second == len(b"same binary payload")
    assert [path.name for path in (tmp_path / "blobs").iterdir()] == [first]


def _run_concurrently(call, count: int = 64) -> list:
    barrier = Barrier(count)

    def invoke():
        barrier.wait(timeout=10)
        return call()

    with ThreadPoolExecutor(max_workers=count) as executor:
        return list(executor.map(lambda _index: invoke(), range(count)))


def _hold_blob_temp_links_until_a_verifier_opens(monkeypatch, root: Path, digest: str) -> None:
    """Force a real publish/unlink overlap without relying on scheduler luck."""
    destination = root / digest
    published = Event()
    verifier_opened = Event()
    original_link = blobs.os.link
    original_open = blobs._open_regular_file
    original_unlink = Path.unlink

    def controlled_link(source, target, *args, **kwargs):
        result = original_link(source, target, *args, **kwargs)
        if Path(target) == destination:
            published.set()
        return result

    def controlled_open(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if Path(path) == destination:
            verifier_opened.set()
        return handle

    def controlled_unlink(path, *args, **kwargs):
        if (
            published.is_set()
            and Path(path).parent == root
            and Path(path).name.startswith(".blob-")
        ):
            assert verifier_opened.wait(timeout=10)
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(blobs.os, "link", controlled_link)
    monkeypatch.setattr(blobs, "_open_regular_file", controlled_open)
    monkeypatch.setattr(Path, "unlink", controlled_unlink)


def test_put_64_concurrent_publishers_do_not_report_source_changed(
    tmp_path: Path, monkeypatch
) -> None:
    data = b"concurrent put payload\0" * 65536
    root = tmp_path / "blobs"
    digest = sha256(data).hexdigest()
    root.mkdir()
    _hold_blob_temp_links_until_a_verifier_opens(monkeypatch, root, digest)

    results = _run_concurrently(lambda: BlobStore(root).put(data))

    assert results == [digest] * 64
    assert BlobStore(root).read_verified(digest) == data


def test_ingest_64_concurrent_publishers_do_not_report_source_changed(
    tmp_path: Path, monkeypatch
) -> None:
    data = b"concurrent ingest payload\0" * 65536
    source = tmp_path / "source.bin"
    source.write_bytes(data)
    root = tmp_path / "blobs"
    digest = sha256(data).hexdigest()
    root.mkdir()
    _hold_blob_temp_links_until_a_verifier_opens(monkeypatch, root, digest)

    results = _run_concurrently(lambda: BlobStore(root).ingest(source))

    assert results == [(digest, len(data))] * 64
    assert BlobStore(root).read_verified(digest) == data


def test_put_fails_closed_on_an_existing_wrong_regular_blob(tmp_path: Path) -> None:
    """不可变仓不覆盖错误 regular blob，也不能把它误报成命中。"""
    data = b"correct payload"
    wrong = b"wrong payload"
    root = tmp_path / "blobs"
    root.mkdir()
    digest = sha256(data).hexdigest()
    destination = root / digest
    destination.write_bytes(wrong)

    with pytest.raises(ValueError, match=rf"{digest}.*{sha256(wrong).hexdigest()}"):
        BlobStore(root).put(data)

    assert destination.read_bytes() == wrong


def test_ingest_fails_closed_on_an_existing_wrong_regular_blob(tmp_path: Path) -> None:
    """收编遇到错误 regular blob 时 fail closed，并保留原仓内容。"""
    data = b"correct ingest payload"
    wrong = b"wrong ingest payload"
    source = tmp_path / "source.bin"
    source.write_bytes(data)
    root = tmp_path / "blobs"
    root.mkdir()
    digest = sha256(data).hexdigest()
    destination = root / digest
    destination.write_bytes(wrong)

    with pytest.raises(ValueError, match=rf"{digest}.*{sha256(wrong).hexdigest()}"):
        BlobStore(root).ingest(source)

    assert destination.read_bytes() == wrong


def test_materialize_rejects_a_tampered_store_file(tmp_path: Path) -> None:
    """教训 blob_integrity: 仓被篡改宁可拒绝物化，也不能让寻址变成谎言。"""
    store = BlobStore(tmp_path / "blobs")
    digest = store.put(b"original")
    (tmp_path / "blobs" / digest).write_bytes(b"tampered")

    with pytest.raises(ValueError, match=digest):
        store.materialize(digest, tmp_path / "output.bin")


@pytest.mark.parametrize("target_kind", ["parent", "target"])
def test_materialize_rejects_symlink_destination_components(
    tmp_path: Path, target_kind: str
) -> None:
    store = BlobStore(tmp_path / "blobs")
    digest = store.put(b"symlink destination")
    outside = tmp_path / "outside"
    outside.mkdir()
    project = tmp_path / "project"
    project.mkdir()

    if target_kind == "parent":
        parent = project / "output"
        parent.symlink_to(outside, target_is_directory=True)
        destination = parent / "result.bin"
    else:
        destination = project / "result.bin"
        outside_target = outside / "result.bin"
        outside_target.write_bytes(b"keep me")
        destination.symlink_to(outside_target)

    with pytest.raises(ValueError, match="symlink"):
        store.materialize(digest, destination)

    assert (
        not (outside / "result.bin").exists() or (outside / "result.bin").read_bytes() == b"keep me"
    )


def test_read_verified_accepts_a_hardlink_blob(tmp_path: Path) -> None:
    data = b"hardlink blob payload"
    source = tmp_path / "source.bin"
    source.write_bytes(data)
    root = tmp_path / "blobs"
    root.mkdir()
    digest = sha256(data).hexdigest()
    try:
        (root / digest).hardlink_to(source)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support hardlinks: {error}")

    assert BlobStore(root).read_verified(digest) == data


def test_materialize_same_digest_breaks_a_hardlink_alias(tmp_path: Path) -> None:
    data = b"hardlink materialization payload"
    store = BlobStore(tmp_path / "blobs")
    digest = store.put(data)
    destination = tmp_path / "output.bin"
    try:
        destination.hardlink_to(store.root / digest)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support hardlinks: {error}")

    store.materialize(digest, destination)
    assert os.stat(destination).st_ino != os.stat(store.root / digest).st_ino
    destination.write_bytes(b"modified output")

    assert store.read_verified(digest) == data


def test_put_rejects_a_symlink_destination(tmp_path: Path) -> None:
    data = b"symlink destination payload"
    target = tmp_path / "target.bin"
    target.write_bytes(data)
    root = tmp_path / "blobs"
    root.mkdir()
    digest = sha256(data).hexdigest()
    destination = root / digest
    try:
        destination.symlink_to(target)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support symlinks: {error}")

    with pytest.raises(ValueError, match="regular file|symlink"):
        BlobStore(root).put(data)


def test_ingest_rejects_a_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.bin"
    target.write_bytes(b"symlink source")
    alias = tmp_path / "alias.bin"
    try:
        alias.symlink_to(target)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support symlinks: {error}")

    with pytest.raises(ValueError, match="regular file|symlink"):
        BlobStore(tmp_path / "blobs").ingest(alias)


@pytest.mark.parametrize(
    "digest",
    ["not-a-digest", "f" * 63, "g" * 64, "../outside", "/etc/hosts"],
)
def test_read_verified_rejects_invalid_digest_before_opening_any_path(
    tmp_path: Path, digest: str, monkeypatch
) -> None:
    """blob digest 不是严格 hex 名称时，校验必须发生在路径访问之前。"""
    root = tmp_path / "blobs"
    root.mkdir()
    opened: list[Path] = []
    original_open = Path.open

    def tracking_open(path: Path, *args, **kwargs):
        opened.append(path)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", tracking_open)

    with pytest.raises(ValueError, match="digest"):
        BlobStore(root).read_verified(digest)

    assert opened == []


def test_read_verified_rejects_non_regular_blob_without_opening_it(tmp_path: Path) -> None:
    """合法 digest 名称指向目录/FIFO 时，也必须在 open 前 fail closed。"""
    root = tmp_path / "blobs"
    root.mkdir()
    digest = "a" * 64
    (root / digest).mkdir()

    with pytest.raises(ValueError, match="regular file"):
        BlobStore(root).read_verified(digest)


@pytest.mark.skipif(
    not hasattr(os, "mknod") or not hasattr(os, "makedev"), reason="device files are unavailable"
)
def test_read_verified_rejects_character_device_before_opening_it(tmp_path: Path) -> None:
    root = tmp_path / "blobs"
    root.mkdir()
    digest = "d" * 64
    device = root / digest
    try:
        os.mknod(device, stat.S_IFCHR | 0o600, os.makedev(0, 0))
    except (PermissionError, OSError) as error:
        pytest.skip(f"character devices are unavailable: {error}")

    with pytest.raises(ValueError, match="regular file"):
        BlobStore(root).read_verified(digest)


def test_open_regular_file_closes_descriptor_after_descriptor_validation_failure(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"descriptor cleanup")
    closed: list[int] = []
    original_close = blobs.os.close

    def fake_fstat(_descriptor: int):
        return SimpleNamespace(st_mode=stat.S_IFIFO)

    def tracking_close(descriptor: int) -> None:
        closed.append(descriptor)
        original_close(descriptor)

    monkeypatch.setattr(blobs.os, "fstat", fake_fstat)
    monkeypatch.setattr(blobs.os, "close", tracking_close)

    with pytest.raises(ValueError, match="regular file"):
        blobs._open_regular_file(source, phase="descriptor validation")

    assert closed


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="mkfifo is unavailable")
def test_read_verified_rejects_fifo_without_blocking_or_hashing(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "blobs"
    root.mkdir()
    digest = "b" * 64
    fifo = root / digest
    os.mkfifo(fifo)

    original_open = Path.open

    def fail_if_opened(path: Path, *args, **kwargs):
        if path == fifo:
            raise AssertionError("FIFO must be rejected before open")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_if_opened)

    with pytest.raises(ValueError, match="regular file"):
        BlobStore(root).read_verified(digest)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="mkfifo is unavailable")
def test_read_verified_race_to_fifo_does_not_block(tmp_path: Path) -> None:
    """blob 校验后的路径若被替换成 FIFO，读取必须无阻塞地 fail closed。"""
    root = tmp_path / "blobs"
    root.mkdir()
    digest = "c" * 64
    source = root / digest
    source.write_bytes(b"payload")
    script = """
import os
import sys
from pathlib import Path

import kigumi.blobs as blobs

root = Path(sys.argv[1])
digest = sys.argv[2]
source_path = root / digest
original_open = blobs.os.open
replaced = False

def open_race(path, flags, mode=0o777, *, dir_fd=None):
    global replaced
    if Path(path) == source_path and not replaced:
        replaced = True
        source_path.unlink()
        os.mkfifo(source_path)
    if dir_fd is None:
        return original_open(path, flags, mode)
    return original_open(path, flags, mode, dir_fd=dir_fd)

blobs.os.open = open_race
blobs.BlobStore(root).read_verified(digest)
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", script, str(root), digest],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except subprocess.TimeoutExpired as error:
        pytest.fail(f"blob 读取在 FIFO 竞态中阻塞: {error}")

    assert result.returncode != 0
    assert "regular" in result.stderr.lower()


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="mkfifo is unavailable")
def test_ingest_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    """BlobStore.ingest 不能直接阻塞读取调用方提供的 FIFO。"""
    source = tmp_path / "source.bin"
    os.mkfifo(source)
    root = tmp_path / "blobs"
    script = """
import sys
from pathlib import Path

from kigumi import BlobStore

BlobStore(Path(sys.argv[2])).ingest(Path(sys.argv[1]))
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", script, str(source), str(root)],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except subprocess.TimeoutExpired as error:
        pytest.fail(f"BlobStore.ingest 在 FIFO 上阻塞: {error}")

    assert result.returncode != 0
    assert "regular" in result.stderr.lower()
