from __future__ import annotations

import json
import subprocess
import threading
from pathlib import Path
from typing import Any

import pytest

from kigumi._runstate import RunManifestError
from kigumi.calling import DryRunError, LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import CheckpointPending, Dag
from kigumi.testing import FakeTransport

_CALLER = """
import sys
from pathlib import Path

from kigumi.calling import LLMCaller
from kigumi.transport import Response

class Transport:
    def resolve(self, model):
        return model

    def complete(self, messages, model, **params):
        return Response('stable response', {'total_tokens': 1}, 'stop')

print(LLMCaller(Transport(), Path(sys.argv[1])).call('same request'))
"""


def test_multiple_processes_leave_a_valid_shared_cache(tmp_path: Path) -> None:
    """教训 shared_cache_atomicity: 并发写同一键后缓存仍必须是完整 JSON。"""
    script = tmp_path / "caller_worker.py"
    script.write_text(_CALLER, encoding="utf-8")
    cache_dir = tmp_path / "cache"
    root = Path(__file__).resolve().parents[1]
    processes = [
        subprocess.Popen(
            ["uv", "run", "python", str(script), str(cache_dir)],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]

    results = [process.communicate(timeout=30) for process in processes]

    assert all(process.returncode == 0 for process in processes), results
    assert [stdout.strip() for stdout, _ in results] == ["stable response"] * 4
    cache_files = list((cache_dir / "llm").glob("*.json"))
    assert len(cache_files) == 1
    assert json.loads(cache_files[0].read_text(encoding="utf-8"))["response"] == "stable response"


def _make_dag(tmp_path: Path) -> Dag:
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    return Dag(config, LLMCaller(FakeTransport(), tmp_path))


def test_parallel_changed_run_declaration_does_not_archive(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """0.6 manifests reject changed declarations before archive allocation."""
    import kigumi.dag as dag_module

    barrier = threading.Barrier(2)

    def build(revision: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("alpha", params={"revision": revision})
        def alpha(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            del inputs
            barrier.wait(timeout=5)
            return {"revision": ctx.params["revision"]}

        @dag.node("beta", params={"revision": revision})
        def beta(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            del inputs
            barrier.wait(timeout=5)
            return {"revision": ctx.params["revision"]}

        return dag

    build(1).run(run_id="shared", workers=2)
    calls = 0
    original = dag_module.store.next_history_id

    def count_history_id(path: Path) -> str:
        nonlocal calls
        calls += 1
        return original(path)

    monkeypatch.setattr(dag_module.store, "next_history_id", count_history_id)
    with pytest.raises(RunManifestError, match="declaration changed"):
        build(2).run(run_id="shared", workers=2)

    assert calls == 0
    assert not (tmp_path / "artifacts" / "runs" / "shared" / "history").exists()


def test_map_parallel_failures_preserve_all_details_and_dry_run(tmp_path: Path) -> None:
    def build(error: type[Exception]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
            del inputs, ctx
            return {"items": [{"id": "first"}, {"id": "second"}]}

        @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
        def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            del inputs, ctx
            raise error(item["id"])

        return dag

    with pytest.raises(RuntimeError) as failures:
        build(ValueError).run(workers=2)

    assert "first (ValueError: first)" in str(failures.value)
    assert "second (ValueError: second)" in str(failures.value)
    assert isinstance(failures.value.__cause__, ValueError)
    assert str(failures.value.__cause__) == "first"
    with pytest.raises(DryRunError, match="first"):
        build(DryRunError).run(workers=2)


def test_map_pending_and_success_write_success_sidecar(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        del inputs, ctx
        return {"items": [{"id": "pending"}, {"id": "success"}]}

    @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        if item["id"] == "pending":
            raise CheckpointPending("approve-pending", {"id": item["id"]})
        return {"id": item["id"]}

    result = dag.run(run_id="mixed", workers=2)

    assert result.pending_checkpoints == ["approve-pending"]
    assert (tmp_path / "artifacts" / "runs" / "mixed" / "work@success.json.meta.json").is_file()
