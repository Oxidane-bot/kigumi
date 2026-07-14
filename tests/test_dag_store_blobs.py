from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from kigumi.dag import Dag
from tests._dag_helpers import _make_dag


def test_diff_and_gc_keep_recent_run_cache_entries(tmp_path: Path) -> None:
    """教训 run_history: 差异与缓存回收必须基于可读 runs 溯源。"""
    for value in range(3):
        dag = _make_dag(tmp_path)

        @dag.node("result", params={"value": value})
        def result(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": ctx.params["value"]}

        dag.run()

    inspector = _make_dag(tmp_path)
    assert inspector.diff("run-0001", "run-0002") == {
        "changed": ["result"],
        "only_a": [],
        "only_b": [],
    }
    assert inspector.gc(keep_last=1) == 2
    assert len(list((tmp_path / "artifacts" / "_cache" / "nodes").glob("*.json"))) == 1


def test_run_sidecar_contains_cache_and_new_caller_provenance(tmp_path: Path) -> None:
    """教训 provenance_slice: 节点 sidecar 必须保留本节点新增调用溯源。"""
    dag = _make_dag(tmp_path)

    @dag.node("ask")
    def ask(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"answer": ctx.llm("say something")}

    result = dag.run(run_id="provenance")
    sidecar = tmp_path / "artifacts" / "runs" / "provenance" / "ask.json.meta.json"

    assert result.artifacts["ask"] == {"answer": "model output"}
    metadata = json.loads(sidecar.read_text(encoding="utf-8"))
    assert metadata["cache"] == "miss"
    assert metadata["cache_key"]
    assert len(metadata["calls"]) == 1
    assert metadata["calls"][0]["cache"] == "miss"


def test_miss_and_hit_paths_feed_downstream_identical_shape(tmp_path: Path) -> None:
    """教训 hit_miss_parity: miss 路径喂下游的键序必须与命中路径读盘一致(bf06)。"""

    def run_leaf(marker: int) -> str:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"zeta": "z", "alpha": "a"}

        @dag.node("leaf", deps=("source",), params={"marker": marker})
        def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"prompt_text": json.dumps(inputs["source"], ensure_ascii=False)}

        return dag.run().artifacts["leaf"]["prompt_text"]

    first = run_leaf(1)
    second = run_leaf(2)

    assert first == second


def test_map_gc_retains_item_cache_keys_referenced_by_aggregate(tmp_path: Path) -> None:
    """教训 map_gc: gc 不认识聚合里的 item 键会静默删掉可复用缓存。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return item

    run = dag.run()
    meta = json.loads(
        (tmp_path / "artifacts" / "runs" / run.run_id / "m.json.meta.json").read_text(
            encoding="utf-8"
        )
    )
    assert isinstance(meta["cache_key"], list)
    # 逐项 sidecar 只是冗余引用;删掉它们,聚合 sidecar 必须独自保住 item 缓存。
    for sidecar in (tmp_path / "artifacts" / "runs" / run.run_id).glob("m@*.json.meta.json"):
        sidecar.unlink()
    assert dag.gc(keep_last=1) == 0


def test_map_item_sidecars_retain_item_cache_without_aggregate_sidecar(tmp_path: Path) -> None:
    """教训 map_gc_redundancy: 聚合 sidecar 丢失时逐项引用仍必须保住缓存。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return item

    run = dag.run()
    run_root = tmp_path / "artifacts" / "runs" / run.run_id
    item_keys = [
        json.loads(sidecar.read_text(encoding="utf-8"))["cache_key"]
        for sidecar in sorted(run_root.glob("m@*.json.meta.json"))
    ]
    item_caches = [tmp_path / "artifacts" / "_cache" / "nodes" / f"{key}.json" for key in item_keys]
    assert all(path.is_file() for path in item_caches)

    (run_root / "m.json.meta.json").unlink()

    assert dag.gc(keep_last=1) == 0
    assert all(path.is_file() for path in item_caches)


def test_gc_skips_invalid_sidecars_without_losing_normal_references(tmp_path: Path) -> None:
    """教训 gc_sidecar_tolerance: 坏 sidecar 不能阻断或削弱其他引用。"""
    dag = _make_dag(tmp_path)

    @dag.node("normal")
    def normal(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "kept"}

    run = dag.run()
    run_root = tmp_path / "artifacts" / "runs" / run.run_id
    normal_metadata = json.loads((run_root / "normal.json.meta.json").read_text(encoding="utf-8"))
    normal_cache = (
        tmp_path / "artifacts" / "_cache" / "nodes" / f"{normal_metadata['cache_key']}.json"
    )
    stale_cache = tmp_path / "artifacts" / "_cache" / "nodes" / "unreferenced.json"
    stale_cache.write_text("{}", encoding="utf-8")
    (run_root / "broken.json.meta.json").write_text("{", encoding="utf-8")
    (run_root / "scalar.json.meta.json").write_text("[]", encoding="utf-8")

    assert dag.gc(keep_last=1) == 1
    assert normal_cache.is_file()
    assert not stale_cache.exists()


def test_blob_cache_hit_rematerializes_deleted_binary_output(tmp_path: Path) -> None:
    """教训 blob_cache_hit: 命中缓存也必须恢复曾被清理的二进制交付物。"""
    dag = _make_dag(tmp_path)
    executions = 0

    @dag.node("build")
    def build(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        nonlocal executions
        executions += 1
        return {"file": ctx.emit_file("generated/result.bin", b"binary result")}

    first = dag.run()
    output = tmp_path / "generated" / "result.bin"
    output.unlink()
    second = dag.run()

    assert first.cache_hits == []
    assert second.cache_hits == ["build"]
    assert executions == 1
    assert output.read_bytes() == b"binary result"


def test_missing_blob_names_its_digest_and_node(tmp_path: Path) -> None:
    """教训 blob_missing: 换机器或误删仓文件不能让节点假装成功。"""
    dag = _make_dag(tmp_path)

    @dag.node("package")
    def package(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"file": ctx.emit_file("generated/package.bin", b"package")}

    digest = dag.run().artifacts["package"]["file"]["kigumi_blob"]
    (tmp_path / "artifacts" / "_cache" / "blobs" / digest).unlink()

    with pytest.raises(FileNotFoundError, match=rf"{digest}.*package"):
        dag.run()


@pytest.mark.parametrize("relative_path", ["/tmp/escape.bin", "../escape.bin"])
def test_emit_file_rejects_paths_outside_the_project(tmp_path: Path, relative_path: str) -> None:
    """教训 blob_path_guard: artifact 物化路径不能借 blob 逃出项目目录。"""
    dag = _make_dag(tmp_path)

    @dag.node("emit")
    def emit(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"file": ctx.emit_file(relative_path, b"unsafe")}

    with pytest.raises(ValueError, match="project-relative"):
        dag.run()


def test_ingest_file_copies_an_external_source_without_moving_it(tmp_path: Path) -> None:
    """教训 blob_ingest_copy: 工具临时产物属于调用方，收编不能偷偷 move。"""
    external = tmp_path.parent / f"{tmp_path.name}-external.bin"
    external.write_bytes(b"tool output")
    dag = _make_dag(tmp_path)

    @dag.node("ingest")
    def ingest(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"file": ctx.ingest_file(external, "generated/ingested.bin")}

    try:
        result = dag.run()
        reference = result.artifacts["ingest"]["file"]

        assert external.read_bytes() == b"tool output"
        assert reference["bytes"] == len(b"tool output")
        assert (tmp_path / "generated" / "ingested.bin").read_bytes() == b"tool output"
        assert (tmp_path / "artifacts" / "_cache" / "blobs" / reference["kigumi_blob"]).is_file()
    finally:
        external.unlink(missing_ok=True)


def test_gc_keeps_blobs_referenced_by_the_latest_run(tmp_path: Path) -> None:
    """教训 blob_gc: 回收旧缓存时不能误删保留 run 仍可物化的交付物。"""

    def build(data: bytes) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("build", params={"data": data.decode("ascii")})
        def output(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"file": ctx.emit_file("generated/current.bin", data)}

        return dag

    old = build(b"old").run().artifacts["build"]["file"]["kigumi_blob"]
    current_dag = build(b"new")
    new = current_dag.run().artifacts["build"]["file"]["kigumi_blob"]

    assert current_dag.gc(keep_last=1) == 2
    blobs = tmp_path / "artifacts" / "_cache" / "blobs"
    assert not (blobs / old).exists()
    assert (blobs / new).is_file()
    (tmp_path / "generated" / "current.bin").unlink()
    assert current_dag.run().cache_hits == ["build"]
    assert (tmp_path / "generated" / "current.bin").read_bytes() == b"new"


def test_blob_reference_invalidates_a_downstream_node_by_content(tmp_path: Path) -> None:
    """教训 blob_upstream_key: 二进制变更必须让依赖其引用的下游失效。"""

    def build(data: bytes) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source", params={"data": data.decode("ascii")})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"file": ctx.emit_file("generated/source.bin", data)}

        @dag.node("consumer", deps=("source",))
        def consumer(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"digest": inputs["source"]["file"]["kigumi_blob"]}

        return dag

    assert build(b"stable").run().cache_hits == []
    assert build(b"stable").run().cache_hits == ["source", "consumer"]
    assert build(b"changed").run().cache_hits == []


def test_map_blob_items_recompute_only_changed_items_and_rematerialize_hits(tmp_path: Path) -> None:
    """教训 map_blob_item_cache: 单项内容变化不能拖累命中项的缓存或物化。"""
    executed: list[str] = []

    def build(items: list[dict[str, str]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": ctx.params["items"]}

        @dag.map("render", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def render(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            executed.append(item["id"])
            return {"file": ctx.emit_file(f"generated/{item['id']}.bin", item["data"].encode())}

        return dag

    first_items = [{"id": "a", "data": "one"}, {"id": "b", "data": "two"}]
    build(first_items).run()
    executed.clear()
    (tmp_path / "generated" / "b.bin").unlink()
    second_items = [{"id": "a", "data": "changed"}, {"id": "b", "data": "two"}]
    result = build(second_items).run()

    assert result.cache_hits == []
    assert executed == ["a"]
    assert (tmp_path / "generated" / "b.bin").read_bytes() == b"two"
