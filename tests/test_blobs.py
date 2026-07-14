from __future__ import annotations

from pathlib import Path

import pytest

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


def test_materialize_rejects_a_tampered_store_file(tmp_path: Path) -> None:
    """教训 blob_integrity: 仓被篡改宁可拒绝物化，也不能让寻址变成谎言。"""
    store = BlobStore(tmp_path / "blobs")
    digest = store.put(b"original")
    (tmp_path / "blobs" / digest).write_bytes(b"tampered")

    with pytest.raises(ValueError, match=digest):
        store.materialize(digest, tmp_path / "output.bin")
