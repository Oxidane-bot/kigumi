from __future__ import annotations

import json
import os
import shutil
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from kigumi._runstate import RunManifestError
from tests._dag_helpers import _make_dag


@pytest.mark.parametrize("kind", ["node", "map", "scan"])
def test_dag_file_snapshot_rejects_symlinks_at_the_regular_file_boundary(
    tmp_path: Path, kind: str
) -> None:
    root = tmp_path / kind
    root.mkdir()
    outside = tmp_path / f"{kind}-outside.txt"
    outside.write_text("must not be followed", encoding="utf-8")
    linked = root / "input.txt"
    try:
        linked.symlink_to(outside)
    except OSError as error:
        pytest.skip(f"symlink setup is unavailable: {error}")

    dag = _make_dag(root)

    if kind == "node":

        @dag.node("reader", files=(linked,))
        def reader(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            del inputs, ctx
            return {"value": "unexpected"}

    else:

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            del inputs, ctx
            return {"items": [{"id": "one", "file": "input.txt"}]}

        register = dag.map if kind == "map" else dag.scan

        @register(
            "reader",
            items_from=("source", "items"),
            key_fn=lambda item: item["id"],
            files_fn=lambda item: (item["file"],),
        )
        def reader(*args: Any) -> dict[str, str]:
            del args
            return {"value": "unexpected"}

    with pytest.raises(ValueError, match="symlink|regular file"):
        dag.run()


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="mkfifo is unavailable")
def test_dag_file_snapshot_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    fifo = tmp_path / "input.fifo"
    os.mkfifo(fifo)
    dag = _make_dag(tmp_path)

    @dag.node("reader", files=(fifo,))
    def reader(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "unexpected"}

    with ThreadPoolExecutor(max_workers=1) as executor:
        future: Future[Any] = executor.submit(dag.run)
        try:
            with pytest.raises(ValueError, match="regular file"):
                future.result(timeout=1)
        except FutureTimeoutError:
            # Release an old implementation that opened the FIFO before checking
            # its type, then fail the test instead of leaking a blocked thread.
            with fifo.open("wb") as handle:
                handle.write(b"unblock")
            future.result(timeout=1)
            pytest.fail("DAG file snapshot blocked while reading a FIFO")


def test_scan_resume_rejects_changed_unexecuted_files_fn_suffix_from_manifest_ledger(
    tmp_path: Path,
) -> None:
    items = [
        {"id": "a", "file": "a.txt"},
        {"id": "b", "file": "b.txt"},
        {"id": "c", "file": "c.txt"},
    ]
    for item in items:
        (tmp_path / item["file"]).write_text(item["id"], encoding="utf-8")
    paused = {"value": True}
    executed: list[str] = []
    dag = _make_dag(tmp_path)

    @dag.node("source", params={"items": items})
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        return {"items": ctx.params["items"]}

    @dag.scan(
        "chain",
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
        files_fn=lambda item: (item["file"],),
    )
    def chain(item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del carry, inputs
        executed.append(item["id"])
        if item["id"] == "b" and paused["value"]:
            return {"approval": ctx.checkpoint("editor", {"item": item["id"]})}
        return {"id": item["id"], "text": ctx.read_text(item["file"])}

    first = dag.run(run_id="dynamic-snapshot")
    assert first.pending_checkpoints == ["editor@b"]
    manifest_path = tmp_path / "artifacts" / "runs" / first.run_id / "_run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    ledger = manifest["dynamic_files_ledger"]["chain"]
    assert ledger["c"] == [{"path": "c.txt", "sha256": sha256(b"c").hexdigest()}]
    assert manifest["dynamic_files_ledger_sha256"]

    (tmp_path / "c.txt").write_text("changed-after-snapshot", encoding="utf-8")
    dag.approve(first.run_id, "editor@b", {"ok": True})
    paused["value"] = False

    with pytest.raises(RunManifestError, match="dynamic file|snapshot|ledger"):
        dag.resume(first.run_id)
    assert "c" not in executed


def test_resume_can_bind_dynamic_files_after_upstream_checkpoint(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("a", encoding="utf-8")
    paused = {"value": True}
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        if paused["value"]:
            return {"approval": ctx.checkpoint("editor", {})}
        return {"items": [{"id": "a", "file": "a.txt"}]}

    @dag.scan(
        "chain",
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
        files_fn=lambda item: (item["file"],),
    )
    def chain(item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del carry, inputs
        return {"value": ctx.read_text(item["file"])}

    first = dag.run(run_id="deferred-dynamic")
    assert first.pending_checkpoints == ["editor"]
    manifest = json.loads(
        (tmp_path / "artifacts" / "runs" / first.run_id / "_run.json").read_text(encoding="utf-8")
    )
    assert manifest["dynamic_files_ledger"] == {}

    dag.approve(first.run_id, "editor", {"ok": True})
    paused["value"] = False
    resumed = dag.resume(first.run_id)

    assert resumed.artifacts["chain"]["items"] == {"a": {"value": "a"}}


def test_resume_rejects_a_rogue_artifact_pair_without_durable_target_ownership(
    tmp_path: Path,
) -> None:
    paused = {"value": False}
    work_executions = 0
    dag = _make_dag(tmp_path)

    @dag.node("source", cache="off")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        if paused["value"]:
            return {"value": ctx.checkpoint("review", {"ready": True})}
        return {"value": "source"}

    @dag.node("work", deps=("source",))
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        nonlocal work_executions
        del inputs, ctx
        work_executions += 1
        return {"value": "work"}

    completed = dag.run(run_id="source-run")
    paused["value"] = True
    pending = dag.run(run_id="rogue-run")
    assert pending.pending_checkpoints == ["review"]
    assert not (tmp_path / "artifacts" / "runs" / "rogue-run" / "work.json").exists()

    source_root = tmp_path / "artifacts" / "runs" / completed.run_id
    rogue_root = tmp_path / "artifacts" / "runs" / pending.run_id
    for filename in ("work.json", "work.json.meta.json"):
        shutil.copyfile(source_root / filename, rogue_root / filename)

    dag.approve(pending.run_id, "review", {"ok": True})
    paused["value"] = False
    with pytest.raises(RunManifestError, match="state|candidate|ownership"):
        dag.resume(pending.run_id)
    assert work_executions == 1


def test_completed_resume_rejects_tampered_materialized_output_without_overwriting_it(
    tmp_path: Path,
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("build")
    def build(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"files": {"generated/result.txt": "expected"}}

    first = dag.run(run_id="tampered-output")
    output = tmp_path / "generated" / "result.txt"
    output.write_text("tampered", encoding="utf-8")

    with pytest.raises(RunManifestError, match="materialized output|output|digest"):
        dag.resume(first.run_id)
    assert output.read_text(encoding="utf-8") == "tampered"
