from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

import pytest

from kigumi.artifacts import sha
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from tests._dag_helpers import _make_dag


def test_cache_hit_materializes_files_and_runs_post_node(tmp_path: Path) -> None:
    """教训 materialize_cache_hit: 缓存不能跳过下游依赖的磁盘物化。"""
    events: list[tuple[str, bool]] = []
    dag = _make_dag(tmp_path, lambda name, artifact, hit: events.append((name, hit)))

    @dag.node("build")
    def build(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"value": "ready", "files": {"generated/result.txt": "materialized"}}

    assert dag.run().cache_hits == []
    materialized = tmp_path / "generated" / "result.txt"
    materialized.unlink()

    assert dag.run().cache_hits == ["build"]
    assert materialized.read_text(encoding="utf-8") == "materialized"
    assert events == [("build", False), ("build", True)]


def test_run_id_must_be_a_safe_single_path_component(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "safe"}

    for run_id in ("", "../escape", "nested/run", "nested\\run", ".", ".."):
        with pytest.raises(ValueError, match="Run ID.*single non-empty relative path component"):
            dag.run(run_id=run_id)

    assert not (tmp_path / "escape").exists()


def test_force_recomputes_a_cache_hit_and_replaces_cached_artifact(tmp_path: Path) -> None:
    """教训 force_rerun: 指定节点必须越过 L3 缓存并覆盖同一内容键。"""

    class FakeCaller:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def call(self, prompt: str, model: str = "default", **params: Any) -> str:
            self.calls.append({"prompt": prompt, "model": model})
            return str(len(self.calls))

    caller = FakeCaller()
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    dag = Dag(config, caller)  # type: ignore[arg-type]

    @dag.node("ask")
    def ask(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"answer": ctx.llm("same prompt")}

    assert dag.run().artifacts["ask"] == {"answer": "1"}
    assert dag.run().cache_hits == ["ask"]
    assert dag.run(force=("ask",)).artifacts["ask"] == {"answer": "2"}
    assert dag.run().artifacts["ask"] == {"answer": "2"}
    assert len(caller.calls) == 2


def test_changed_same_run_artifact_is_archived_once(tmp_path: Path) -> None:
    """教训 evidence_archive: 覆盖同 run 产物前必须保留旧数据与 sidecar。"""

    def make(value: str) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("work", params={"value": value})
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"value": ctx.params["value"]}

        return dag

    assert make("first").run(run_id="evidence").artifacts["work"] == {"value": "first"}
    assert make("second").run(run_id="evidence").artifacts["work"] == {"value": "second"}
    history = tmp_path / "artifacts" / "runs" / "evidence" / "history" / "0001"
    assert (history / "work.json").exists()
    assert (history / "work.json.meta.json").exists()

    assert make("second").run(run_id="evidence").cache_hits == ["work"]
    assert [path.name for path in (history.parent).iterdir() if path.is_dir()] == ["0001"]


def test_force_rejects_unknown_node_names(tmp_path: Path) -> None:
    """教训 force_typo: force 名字打错必须报错,静默全量命中看起来像成功。"""
    dag = _make_dag(tmp_path)

    @dag.node("ask")
    def ask(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "x"}

    with pytest.raises(ValueError, match="Unknown forced nodes: aks"):
        dag.run(force=("aks",))


def test_run_allocations_reserve_directories_atomically(tmp_path: Path) -> None:
    """教训 run_id_race: 分配号即建目录占号,两次运行不得撞同一 run 目录。"""
    dag = _make_dag(tmp_path)
    runs_root = tmp_path / "artifacts" / "runs"
    (runs_root / "run-0001").mkdir(parents=True)

    first = dag.run().run_id
    second = dag.run().run_id

    assert first == "run-0002"
    assert second == "run-0003"
    assert (runs_root / first).is_dir()
    assert (runs_root / second).is_dir()


def test_parallel_ready_nodes_overlap_when_workers_allow_it(tmp_path: Path) -> None:
    """教训 parallel_overlap: 就绪兄弟若仍串行，会把互等协作误判为超时。"""
    dag = _make_dag(tmp_path)
    first_ready = threading.Event()
    second_ready = threading.Event()

    @dag.node("first")
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        first_ready.set()
        assert second_ready.wait(5), "second node did not overlap execution"
        return {"done": True}

    @dag.node("second")
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        second_ready.set()
        assert first_ready.wait(5), "first node did not overlap execution"
        return {"done": True}

    assert dag.run(workers=2).artifacts == {
        "first": {"done": True},
        "second": {"done": True},
    }


def test_parallel_node_calls_are_observed_by_their_own_sidecars(tmp_path: Path) -> None:
    """教训 call_observer: 并行节点不能用全局调用顺序切片归属溯源。"""
    dag = _make_dag(tmp_path)

    @dag.node("alpha")
    def alpha(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"answer": ctx.call("alpha prompt")}

    @dag.node("beta")
    def beta(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        first = ctx.call("beta first")
        second = ctx.call("beta second")
        return {"answer": first + second}

    dag.run(run_id="parallel-provenance", workers=2)
    run_root = tmp_path / "artifacts" / "runs" / "parallel-provenance"
    alpha_calls = json.loads((run_root / "alpha.json.meta.json").read_text(encoding="utf-8"))[
        "calls"
    ]
    beta_calls = json.loads((run_root / "beta.json.meta.json").read_text(encoding="utf-8"))["calls"]

    assert [call["prompt_sha"] for call in alpha_calls] == [
        sha([{"role": "user", "content": "alpha prompt"}])
    ]
    assert [call["prompt_sha"] for call in beta_calls] == [
        sha([{"role": "user", "content": "beta first"}]),
        sha([{"role": "user", "content": "beta second"}]),
    ]


def test_pending_branch_does_not_block_independent_parallel_branch(tmp_path: Path) -> None:
    """教训 pending_branch: 检查点只阻断下游，不该饿死已就绪旁支。"""
    dag = _make_dag(tmp_path)

    @dag.node("a")
    def pending(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"approval": ctx.checkpoint("review", {"need": "human"})}

    @dag.node("b", deps=("a",))
    def blocked(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        return {"ran": True}

    @dag.node("c")
    def independent(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        return {"ran": True}

    result = dag.run(workers=2)
    assert result.artifacts == {"c": {"ran": True}}
    assert result.pending_checkpoints == ["review"]
    assert result.skipped == ["b"]


def test_parallel_failures_raise_the_first_topological_error(tmp_path: Path) -> None:
    """教训 deterministic_failure: 并行完成顺序不能决定对外暴露的失败。"""
    dag = _make_dag(tmp_path)
    barrier = threading.Barrier(2)

    @dag.node("first")
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        barrier.wait(5)
        raise ValueError("first failure")

    @dag.node("second")
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        barrier.wait(5)
        raise RuntimeError("second failure")

    with pytest.raises(ValueError, match="first failure"):
        dag.run(workers=2)


def test_workers_must_be_positive(tmp_path: Path) -> None:
    """教训 workers_guard: 无效线程数必须在调度前明确失败。"""
    dag = _make_dag(tmp_path)
    with pytest.raises(ValueError, match="workers"):
        dag.run(workers=0)


def test_concurrent_archives_share_one_history_directory(tmp_path: Path) -> None:
    """教训 archive_race: 并发节点归档必须共用同一个 history 目录,一次 run 一份历史。"""
    data = tmp_path / "data.txt"

    def build(tag: str) -> Dag:
        dag = _make_dag(tmp_path)
        barrier = threading.Barrier(2, timeout=10)

        @dag.node("left", params={"tag": tag})
        def left(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            barrier.wait()
            return {"value": ctx.params["tag"]}

        @dag.node("scan")
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"id": "a"}]}

        @dag.map(
            "m",
            items_from=("scan", "items"),
            key_fn=lambda item: item["id"],
            files_fn=lambda item: ("data.txt",),
        )
        def render(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            barrier.wait()
            return {"text": ctx.read_text("data.txt")}

        return dag

    data.write_text("one", encoding="utf-8")
    build("one").run(run_id="race", workers=2)
    data.write_text("two", encoding="utf-8")
    build("two").run(run_id="race", workers=2)

    history = tmp_path / "artifacts" / "runs" / "race" / "history"
    assert [path.name for path in sorted(history.iterdir()) if path.is_dir()] == ["0001"]


def test_parallel_failures_keep_topological_first_and_note_the_rest(tmp_path: Path) -> None:
    """教训 concurrent_failure: 并发旁支失败不能因首个异常而无声丢失。"""
    dag = _make_dag(tmp_path)
    first_ready = threading.Event()
    second_ready = threading.Event()

    @dag.node("first")
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        first_ready.set()
        assert second_ready.wait(5)
        raise ValueError("first failure")

    @dag.node("second")
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        second_ready.set()
        assert first_ready.wait(5)
        raise RuntimeError("second failure")

    with pytest.raises(ValueError, match="first failure") as raised:
        dag.run(workers=2)

    assert raised.value.__notes__ == [
        "additional concurrent failure: second: RuntimeError: second failure"
    ]
