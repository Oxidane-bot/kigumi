from __future__ import annotations

import json
import threading
import unicodedata
from itertools import repeat
from pathlib import Path
from typing import Any

import pytest

from kigumi import Dag, OutputOwnershipError
from kigumi.calling import LLMCaller
from kigumi.config import KigumiConfig
from kigumi.testing import FakeTransport
from kigumi.transport import Response


def _make_dag(tmp_path: Path) -> Dag:
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    transport = FakeTransport(repeat(Response("model output", {"total_tokens": 1}, "stop")))
    return Dag(config, LLMCaller(transport, tmp_path / "llm"))


def test_serial_output_collision_preserves_winner_and_claims_atomically(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("winner")
    def winner(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"shared.txt": "winner"}}

    @dag.node("loser")
    def loser(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"free.txt": "must-not-write", "shared.txt": "loser"}}

    with pytest.raises(OutputOwnershipError, match="shared.txt.*winner.*loser"):
        dag.run(workers=1)

    assert (tmp_path / "shared.txt").read_text(encoding="utf-8") == "winner"
    assert not (tmp_path / "free.txt").exists()


def test_parallel_output_collision_is_thread_safe(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    barrier = threading.Barrier(2)

    @dag.node("left")
    def left(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        barrier.wait()
        return {"files": {"shared.txt": "left"}}

    @dag.node("right")
    def right(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        barrier.wait()
        return {"files": {"shared.txt": "right"}}

    with pytest.raises(OutputOwnershipError):
        dag.run(workers=2)

    assert (tmp_path / "shared.txt").read_text(encoding="utf-8") in {"left", "right"}


def test_symlink_aliases_cannot_bypass_output_ownership(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "alias").symlink_to(real, target_is_directory=True)
    dag = _make_dag(tmp_path)

    @dag.node("winner")
    def winner(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"real/shared.txt": "winner"}}

    @dag.node("loser")
    def loser(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"alias/shared.txt": "loser"}}

    with pytest.raises(OutputOwnershipError, match="real/shared.txt.*winner.*loser"):
        dag.run()

    assert (real / "shared.txt").read_text(encoding="utf-8") == "winner"


def test_symlink_aliases_are_duplicate_paths_within_one_artifact(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "alias").symlink_to(real, target_is_directory=True)
    dag = _make_dag(tmp_path)

    @dag.node("duplicate")
    def duplicate(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"real/shared.txt": "one", "alias/shared.txt": "two"}}

    with pytest.raises(OutputOwnershipError, match="duplicate.*real/shared.txt"):
        dag.run()

    assert not (real / "shared.txt").exists()


def test_case_aliases_follow_target_filesystem_output_identity(tmp_path: Path) -> None:
    stored = tmp_path / "CaseDir"
    stored.mkdir()
    alias = tmp_path / "casedir"
    if not alias.exists() or not alias.samefile(stored):
        pytest.skip("target filesystem is case-sensitive")
    dag = _make_dag(tmp_path)

    @dag.node("winner")
    def winner(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"CaseDir/shared.txt": "winner"}}

    @dag.node("loser")
    def loser(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"casedir/shared.txt": "loser"}}

    with pytest.raises(OutputOwnershipError):
        dag.run()

    assert (stored / "shared.txt").read_text(encoding="utf-8") == "winner"


def test_unicode_aliases_are_duplicate_paths_within_one_artifact(tmp_path: Path) -> None:
    composed_name = "café"
    decomposed_name = unicodedata.normalize("NFD", composed_name)
    stored = tmp_path / composed_name
    stored.mkdir()
    alias = tmp_path / decomposed_name
    if not alias.exists() or not alias.samefile(stored):
        pytest.skip("target filesystem preserves Unicode normalization distinctions")
    dag = _make_dag(tmp_path)

    @dag.node("duplicate")
    def duplicate(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {
            "files": {
                f"{composed_name}/shared.txt": "one",
                f"{decomposed_name}/shared.txt": "two",
            }
        }

    with pytest.raises(OutputOwnershipError, match="duplicate"):
        dag.run()

    assert not (stored / "shared.txt").exists()


def test_unicode_casefold_expansion_remains_distinct_when_filesystem_does(tmp_path: Path) -> None:
    sharp_s = tmp_path / "straße"
    expanded = tmp_path / "strasse"
    sharp_s.mkdir()
    if expanded.exists():
        pytest.skip("target filesystem aliases the Unicode casefold expansion")
    expanded.mkdir()
    dag = _make_dag(tmp_path)

    @dag.node("both")
    def both(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {
            "files": {
                "straße/shared.txt": "sharp-s",
                "strasse/shared.txt": "expanded",
            }
        }

    dag.run()

    assert (sharp_s / "shared.txt").read_text(encoding="utf-8") == "sharp-s"
    assert (expanded / "shared.txt").read_text(encoding="utf-8") == "expanded"


def test_distinct_hardlink_names_can_be_materialized_independently(tmp_path: Path) -> None:
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text("old", encoding="utf-8")
    try:
        second.hardlink_to(first)
    except OSError as error:
        pytest.skip(f"target filesystem does not support hardlinks: {error}")
    dag = _make_dag(tmp_path)

    @dag.node("replace")
    def replace(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"first.txt": "first", "second.txt": "second"}}

    dag.run()

    assert first.read_text(encoding="utf-8") == "first"
    assert second.read_text(encoding="utf-8") == "second"
    assert first.stat().st_ino != second.stat().st_ino


def test_symlink_output_cannot_escape_project_root(tmp_path: Path) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    (tmp_path / "escape").symlink_to(outside, target_is_directory=True)
    dag = _make_dag(tmp_path)

    @dag.node("escape")
    def escape(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"escape/result.txt": "forbidden"}}

    with pytest.raises(ValueError, match="resolve inside the project root"):
        dag.run()

    assert not (outside / "result.txt").exists()


def test_sibling_map_items_cannot_claim_same_output(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.map("render", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def render(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"shared.txt": item["id"]}}

    with pytest.raises(OutputOwnershipError, match="render@a.*render@b"):
        dag.run()

    assert (tmp_path / "shared.txt").read_text(encoding="utf-8") == "a"


def test_sibling_scan_items_raise_public_ownership_error(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.scan("render", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def render(
        item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any
    ) -> dict[str, Any]:
        return {"files": {"shared.txt": item["id"]}}

    with pytest.raises(OutputOwnershipError, match="render@a.*render@b"):
        dag.run()

    assert (tmp_path / "shared.txt").read_text(encoding="utf-8") == "a"


def test_text_and_nested_blob_duplicate_is_rejected_before_writing(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("package")
    def package(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {
            "files": {"result.bin": "text"},
            "nested": {"blob": ctx.emit_file("result.bin", b"binary")},
        }

    with pytest.raises(OutputOwnershipError, match="duplicate.*result.bin"):
        dag.run()

    assert not (tmp_path / "result.bin").exists()


def test_cache_hit_can_rematerialize_output_for_same_producer(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"result.txt": "stable"}}

    dag.run()
    (tmp_path / "result.txt").unlink()
    second = dag.run()

    assert second.cache_hits == ["work"]
    assert (tmp_path / "result.txt").read_text(encoding="utf-8") == "stable"


def test_dynamic_aggregate_may_rematerialize_its_own_item_blob_paths(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.map("render", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def render(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"blob": ctx.emit_file(f"out/{item['id']}.bin", item["id"].encode())}

    result = dag.run()
    run_root = tmp_path / "artifacts" / "runs" / result.run_id
    aggregate = json.loads((run_root / "render.json.meta.json").read_text(encoding="utf-8"))

    assert (tmp_path / "out" / "a.bin").read_bytes() == b"a"
    assert (tmp_path / "out" / "b.bin").read_bytes() == b"b"
    assert aggregate["outputs"] == ["out/a.bin", "out/b.bin"]


def test_ingest_file_source_remains_caller_owned_after_gc(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    external = tmp_path / "external-source.bin"
    external.write_bytes(b"source")

    @dag.node("ingest")
    def ingest(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"file": ctx.ingest_file(external, "managed/copy.bin")}

    dag.run()
    dag.gc(keep_last=0)

    assert external.read_bytes() == b"source"
    assert (tmp_path / "managed" / "copy.bin").read_bytes() == b"source"
