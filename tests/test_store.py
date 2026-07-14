"""存储层边界与兼容入口的回归检查。"""

from __future__ import annotations

import ast
from pathlib import Path

from kigumi import approve_checkpoint, diff_runs, gc_artifacts, gc_cache, store


def test_store_keeps_one_way_dependency_and_top_level_exports() -> None:
    """教训 store_boundary: 存储层不能反向依赖调度层，公开入口直接归属存储层。"""
    module = ast.parse(Path(store.__file__).read_text(encoding="utf-8"))
    imports = [
        alias.name
        for node in ast.walk(module)
        if isinstance(node, ast.Import | ast.ImportFrom)
        for alias in node.names
    ]

    assert "dag" not in imports
    assert "kigumi.dag" not in imports
    assert approve_checkpoint is store.approve_checkpoint
    assert diff_runs is store.diff_runs
    assert gc_cache is store.gc_cache
    assert gc_artifacts is store.gc_artifacts


def test_gc_retains_numeric_latest_run_and_its_blob_references(tmp_path: Path) -> None:
    """教训 run_sort: run-10000 不能被字典序排到 run-9999 前而误删引用。"""
    artifacts = tmp_path / "artifacts"
    runs = artifacts / "runs"
    cache = artifacts / "_cache" / "nodes"
    blobs = artifacts / "_cache" / "blobs"
    for run_id, cache_key, digest in [
        ("run-9999", "old", "old-blob"),
        ("run-10000", "latest", "latest-blob"),
    ]:
        run = runs / run_id
        run.mkdir(parents=True)
        (run / "node.json.meta.json").write_text(
            '{"cache_key": "' + cache_key + '"}', encoding="utf-8"
        )
        (run / "node.json").write_text(
            '{"file": {"kigumi_blob": "' + digest + '"}}', encoding="utf-8"
        )
        cache.mkdir(parents=True, exist_ok=True)
        (cache / f"{cache_key}.json").write_text("{}", encoding="utf-8")
        blobs.mkdir(parents=True, exist_ok=True)
        (blobs / digest).write_bytes(b"blob")

    assert store.gc_artifacts(artifacts, keep_last=1) == 2
    assert (cache / "latest.json").is_file()
    assert not (cache / "old.json").exists()
    assert (blobs / "latest-blob").is_file()
    assert not (blobs / "old-blob").exists()
