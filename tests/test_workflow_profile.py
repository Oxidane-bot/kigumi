from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from kigumi import (
    AgentCapabilities,
    AgentCompletion,
    AgentExecutionFailure,
    AgentRunResult,
    AgentTask,
    EvidencePolicy,
    InputRef,
    ParamRef,
    PromptAxis,
    PromptLayer,
    PromptRef,
    PromptSpec,
)
from kigumi.profile import WorkflowProfileError, render_profile_markdown
from tests._agent_helpers import make_agent_spec
from tests._dag_helpers import _make_dag


def _write(root: Path, name: str, text: str) -> None:
    path = root / "prompts" / f"{name}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_static_profile_reads_declarations_without_executing_nodes(tmp_path: Path) -> None:
    _write(tmp_path, "base", "{{method}}")
    _write(tmp_path, "concise", "concise")
    dag = _make_dag(tmp_path)
    spec = PromptSpec(
        "write",
        PromptRef("base"),
        layers=(
            PromptLayer(
                "method",
                PromptAxis(
                    "mode",
                    InputRef("config", ("mode",)),
                    {"concise": PromptRef("concise")},
                ),
            ),
        ),
    )

    @dag.node("config")
    def config(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        raise AssertionError("static profile must not execute nodes")

    @dag.node("write", deps=("config",), prompt_specs=(spec,))
    def write(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        raise AssertionError("static profile must not execute nodes")

    profile = dag.profile()

    assert profile["workflow_profile_schema"] == 1
    assert profile["mode"] == "static"
    assert profile["prompts"]["specs"][0]["resolution_status"] == "unresolved"
    assert any(edge["role"] == "selector" for edge in profile["graph"]["edges"])
    markdown = render_profile_markdown(profile)
    assert "```mermaid" in markdown
    assert "| write | write | base | method | mode |" in markdown


def test_profile_and_manifest_digest_params_instead_of_persisting_raw_values(
    tmp_path: Path,
) -> None:
    dag = _make_dag(tmp_path)
    secret = "credential-value-that-must-not-enter-profile"

    @dag.node("work", params={"credential": secret})
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "ok"}

    static_text = json.dumps(dag.profile())
    result = dag.run()
    manifest_text = (tmp_path / "artifacts" / "runs" / result.run_id / "_run.json").read_text()

    assert secret not in static_text
    assert secret not in manifest_text


def test_runtime_profile_uses_persisted_current_and_origin_prompt_lineage(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "base", "{{method}}")
    _write(tmp_path, "concise", "concise")

    def build() -> Any:
        dag = _make_dag(tmp_path)
        spec = PromptSpec(
            "managed",
            PromptRef("base"),
            layers=(
                PromptLayer(
                    "method",
                    PromptAxis(
                        "mode",
                        ParamRef("mode"),
                        {"concise": PromptRef("concise")},
                    ),
                ),
            ),
        )

        @dag.node("work", params={"mode": "concise"}, prompt_specs=(spec,))
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            ctx.call(ctx.resolve_prompt("managed"))
            return {"value": "ok"}

        return dag

    cold_dag = build()
    cold = cold_dag.run(run_id="cold")
    cold_profile = cold_dag.profile(cold.run_id)
    assert cold_profile["run"]["attempts"][0]["calls"][0]["managed"] is True
    warm_dag = build()
    warm = warm_dag.run(run_id="warm")
    profile = warm_dag.profile(warm.run_id)
    node = profile["run"]["nodes"][0]

    assert cold.run_id == "cold"
    assert warm.cache_hits == ["work"]
    assert node["cache"] == "hit"
    assert node["calls"] == []
    assert node["current_prompt_resolutions"]["managed"]["resolution_digest"]
    assert node["origin_prompt_resolutions"]["managed"]["resolution_digest"]
    assert node["origin_calls"][0]["prompt_resolution"]["phase"] == "primary"
    assert "request_evidence" not in node["origin_calls"][0]
    expanded_node = warm_dag.profile(warm.run_id, include_content=True)["run"]["nodes"][0]
    assert expanded_node["origin_calls"][0]["request_evidence"] is not None
    prompt = profile["prompts"]["specs"][0]
    assert prompt["resolution_status"] == "resolved"
    assert prompt["runtime"][0]["target"] == "work"
    assert prompt["runtime"][0]["current"]["axes"][0]["selected"] == "concise"
    assert prompt["runtime"][0]["origin"]["axes"][0]["selected"] == "concise"


def test_runtime_profile_fails_closed_for_corrupt_resolution_digest(tmp_path: Path) -> None:
    _write(tmp_path, "base", "managed")
    dag = _make_dag(tmp_path)
    spec = PromptSpec("managed", PromptRef("base"))

    @dag.node("work", prompt_specs=(spec,))
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "ok"}

    result = dag.run()
    sidecar = tmp_path / "artifacts" / "runs" / result.run_id / "work.json.meta.json"
    value = json.loads(sidecar.read_text())
    value["prompt_resolutions"]["managed"]["resolution_digest"] = "corrupt"
    sidecar.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(WorkflowProfileError, match="digest"):
        dag.profile(result.run_id)


def test_runtime_profile_validates_origin_call_resolution_even_when_origin_rehashed(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "base", "managed")
    dag = _make_dag(tmp_path)
    spec = PromptSpec("managed", PromptRef("base"))

    @dag.node("work", prompt_specs=(spec,))
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        ctx.call(ctx.resolve_prompt("managed"))
        return {"value": "ok"}

    result = dag.run()
    sidecar = tmp_path / "artifacts" / "runs" / result.run_id / "work.json.meta.json"
    value = json.loads(sidecar.read_text())
    value["origin_provenance"]["calls"][0]["prompt_resolution"]["resolution_digest"] = "corrupt"
    from kigumi.artifacts import sha

    value["origin_provenance_digest"] = sha(value["origin_provenance"])
    sidecar.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(WorkflowProfileError, match="digest"):
        dag.profile(result.run_id)


def test_runtime_profile_validates_candidate_prompt_resolutions(tmp_path: Path) -> None:
    _write(tmp_path, "base", "managed")
    dag = _make_dag(tmp_path)
    spec = PromptSpec("managed", PromptRef("base"))

    @dag.node("work", prompt_specs=(spec,))
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": ctx.resolve_prompt("managed")}

    result = dag.run()
    attempt_root = next((tmp_path / "artifacts" / "runs" / result.run_id / "attempts").iterdir())
    state_path = attempt_root / "state.json"
    state = json.loads(state_path.read_text())
    candidate_path = attempt_root / state["candidate_file"]
    candidate = json.loads(candidate_path.read_text())
    candidate["prompt_resolutions"]["managed"]["resolution_digest"] = "corrupt"
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")
    from kigumi.artifacts import sha

    state["candidate_sha256"] = sha(candidate)
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(WorkflowProfileError, match="digest"):
        dag.profile(result.run_id)


def test_legacy_profile_is_read_only_and_marks_resolution_unavailable(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    run = tmp_path / "artifacts" / "runs" / "legacy"
    run.mkdir(parents=True)
    (run / "_run.json").write_text(
        json.dumps({"run_manifest_schema": 1, "status": "completed"}),
        encoding="utf-8",
    )

    profile = dag.profile("legacy")

    assert profile["mode"] == "legacy"
    assert profile["resolution_status"] == "unavailable_legacy"


def test_runtime_profile_reports_resume_count_without_reexecuting_provider(
    tmp_path: Path,
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("work", cache="off")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "once"}

    dag.run(run_id="resumed")
    dag.resume("resumed")
    profile = dag.profile("resumed")

    assert profile["run"]["resume_count"] == 1
    assert profile["run"]["last_resumed_at"]


def test_profile_only_doc_edit_does_not_change_resumable_graph_identity(
    tmp_path: Path,
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("work", cache="off")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        """Original description."""
        return {"value": "once"}

    dag.run(run_id="doc-edit")
    dag._nodes["work"].function.__doc__ = "Edited description."

    resumed = dag.resume("doc-edit")

    assert resumed.artifacts["work"] == {"value": "once"}
    assert dag.profile("doc-edit")["graph"]["nodes"][0]["declaration"]["doc"] == (
        "Original description."
    )


def test_agent_instruction_accepts_resolved_prompt_and_persists_lineage(tmp_path: Path) -> None:
    class Adapter:
        def cache_identity(self) -> dict[str, str]:
            return {"adapter": "profile-test"}

        def capabilities(self) -> AgentCapabilities:
            return AgentCapabilities()

        def run(self, request: Any, context: Any) -> AgentRunResult:
            return AgentRunResult(AgentCompletion("completed", "done"))

    _write(tmp_path, "agent", "managed agent")
    dag = _make_dag(tmp_path)
    spec = PromptSpec("agent_prompt", PromptRef("agent"))

    @dag.agent(
        "agent",
        adapter=Adapter(),
        spec=make_agent_spec(tmp_path / "capsule"),
        prompt_specs=(spec,),
    )
    def agent(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask(ctx.resolve_prompt("agent_prompt"))

    result = dag.run()
    profile = dag.profile(result.run_id)
    agent_profile = profile["run"]["nodes"][0]["agent"]

    assert agent_profile["prompt_resolution"]["spec"] == "agent_prompt"
    assert agent_profile["instruction_sha256"]


def test_agent_failure_profile_keeps_managed_lineage_without_expanding_instruction(
    tmp_path: Path,
) -> None:
    class Adapter:
        def cache_identity(self) -> dict[str, str]:
            return {"adapter": "profile-failure"}

        def capabilities(self) -> AgentCapabilities:
            return AgentCapabilities()

        def run(self, request: Any, context: Any) -> AgentRunResult:
            raise RuntimeError("agent failed")

    _write(tmp_path, "agent", "managed secret instruction")
    dag = _make_dag(tmp_path)
    spec = PromptSpec("agent_prompt", PromptRef("agent"))

    @dag.agent(
        "agent",
        adapter=Adapter(),
        spec=make_agent_spec(tmp_path / "capsule"),
        prompt_specs=(spec,),
        evidence_policy=EvidencePolicy(request="hash_only"),
    )
    def agent(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask(ctx.resolve_prompt("agent_prompt"))

    with pytest.raises(AgentExecutionFailure):
        dag.run(run_id="failed-agent")

    profile = dag.profile("failed-agent")
    failure = profile["run"]["failures"][0]

    assert failure["managed"] is True
    assert failure["prompt_resolution"]["spec"] == "agent_prompt"
    assert "instruction_evidence" not in failure

    expanded = dag.profile("failed-agent", include_content=True)
    evidence = expanded["run"]["failures"][0]["instruction_evidence"]
    assert evidence["mode"] == "hash_only"
    assert "managed secret instruction" not in json.dumps(evidence)
