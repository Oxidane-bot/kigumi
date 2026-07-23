from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from kigumi.agents import (
    AgentCapabilities,
    AgentCompletion,
    AgentFileSelector,
    AgentRunResult,
    AgentTask,
)
from kigumi.bench import AgentSubject, DagSubject, FunctionSubject, TrialObservation, Variant, bench
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.evals import Judgment
from tests._agent_helpers import make_agent_spec
from tests._dag_helpers import _make_dag


def test_multi_seed_dag_subject_rejects_auto_before_trials(tmp_path: Path) -> None:
    def factory(context: Any) -> Dag:
        dag = _make_dag(context.project_root)

        @dag.node("target")
        def target(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": 1}

        return dag

    subject = DagSubject(factory, "target", {"kind": "dag"})
    with pytest.raises(ValueError, match="refresh.*off"):
        bench(
            [Variant("current", "baseline", subject, True)],
            [{"id": 1}],
            lambda example, output: Judgment(1.0, "ok"),
            seeds=(0, 1),
            experiment_dir=tmp_path,
        )
    assert not (tmp_path / "trials").exists()


def test_function_and_refresh_dag_share_isolated_grid(tmp_path: Path) -> None:
    def factory(context: Any) -> Dag:
        config = KigumiConfig(
            project_root=context.project_root,
            artifacts_dir=str(context.evidence_root),
            source_dirs=[],
        )
        dag = _make_dag(context.project_root)
        dag.config = config

        @dag.node("target", params={"seed": context.seed}, cache="refresh")
        def target(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": ctx.params["seed"]}

        return dag

    function = FunctionSubject(
        lambda example, context: TrialObservation(example["value"], seed_applied=False),
        identity={"kind": "function"},
    )
    dag_subject = DagSubject(
        factory,
        "target",
        {"kind": "dag"},
        output=lambda artifact: artifact["value"],
        seed_mode="applied",
        seed_keyed=True,
    )
    report = bench(
        [
            Variant("current", "baseline", function, True),
            Variant("dag", "DAG execution", dag_subject),
        ],
        [{"value": 0}],
        lambda example, output: Judgment(1.0 if output in {0, 1} else 0.0, "ok"),
        seeds=(0, 1),
        experiment_dir=tmp_path,
    )
    assert len(report["trials"]) == 4
    assert len({trial["project_root"] for trial in report["trials"]}) == 4
    assert "winner" not in report


def test_agent_subject_trials_are_isolated_and_aggregate_usage_evidence(tmp_path: Path) -> None:
    class Adapter:
        def __init__(self) -> None:
            self.runs = 0

        def cache_identity(self) -> dict[str, Any]:
            return {"adapter": "bench-fake", "version": 1}

        def capabilities(self) -> AgentCapabilities:
            return AgentCapabilities()

        def run(self, request, context) -> AgentRunResult:
            self.runs += 1
            text = (context.workspace / "input.txt").read_text(encoding="utf-8")
            (context.workspace / "result.txt").write_text(text, encoding="utf-8")
            context.record_evidence("rpc.jsonl", b"{}\n", "application/x-ndjson")
            return AgentRunResult(
                AgentCompletion("completed", "done", ("result.txt",), {}),
                usage={"total_tokens": 7, "cost": 0.1},
            )

    adapter = Adapter()
    spec = make_agent_spec(tmp_path / "agent")

    def task(example: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask("echo", collect=(AgentFileSelector("result.txt"),))

    def files(example: dict[str, Any]) -> dict[str, str]:
        return {"input.txt": example["text"]}

    subject = AgentSubject(
        adapter,
        spec,
        task,
        files=files,
        output=lambda artifact: artifact["completion"]["summary"],
    )
    report = bench(
        [Variant("agent", "agent baseline", subject, True)],
        [{"text": "hello"}],
        lambda example, output: Judgment(1.0 if output == "done" else 0.0, "ok"),
        seeds=(0, 1),
        experiment_dir=tmp_path / "experiment",
    )
    assert adapter.runs == 2
    assert len({trial["project_root"] for trial in report["trials"]}) == 2
    assert {trial["evidence"]["cache"] for trial in report["trials"]} == {"off"}
    assert {trial["usage"]["total_tokens"] for trial in report["trials"]} == {7}
    assert all(trial["evidence"]["raw_evidence"] for trial in report["trials"])


def test_agent_subject_failure_keeps_run_evidence_and_scores_zero(tmp_path: Path) -> None:
    class Adapter:
        def cache_identity(self) -> dict[str, Any]:
            return {"adapter": "broken", "version": 1}

        def capabilities(self) -> AgentCapabilities:
            return AgentCapabilities()

        def run(self, request, context):
            context.record_evidence("rpc.jsonl", b"bad\n", "application/x-ndjson")
            raise RuntimeError("broken agent")

    def task(example: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask("fail")

    subject = AgentSubject(Adapter(), make_agent_spec(tmp_path / "agent"), task)
    report = bench(
        [Variant("agent", "failure evidence", subject, True)],
        [{"text": "hello"}],
        lambda example, output: Judgment(1.0, "should not run"),
        seeds=(0,),
        experiment_dir=tmp_path / "experiment",
    )
    trial = report["trials"][0]
    assert trial["judgment"]["score"] == 0
    assert trial["judgment"]["tags"] == ["task_error"]
    assert trial["evidence"]["failure"]["evidence"][0]["workspace_path"].endswith("rpc.jsonl")
