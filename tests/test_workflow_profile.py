from __future__ import annotations

import json
import os
import shutil
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

import kigumi.profile as profile_module
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
from kigumi.profile import (
    WorkflowProfileError,
    _validate_run_integrity,
    load_run_profile,
    render_profile_markdown,
)
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

    assert profile["workflow_profile_schema"] == 2
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


def test_runtime_profile_keeps_file_direct_chat_unmanaged_with_lineage(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.png"
    contents = b"profile direct-chat attachment"
    source.write_bytes(contents)
    dag = _make_dag(tmp_path)

    @dag.node("work", files=(source,))
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs
        return {"answer": ctx.call([{"role": "user", "content": {"kigumi_file": str(source)}}])}

    result = dag.run(run_id="unmanaged-file-chat")
    profile = dag.profile(result.run_id)
    attempt = profile["run"]["attempts"][0]
    call = attempt["calls"][0]
    resolution = call["prompt_resolution"]

    assert attempt["active_effect"]["managed"] is False
    assert call["managed"] is False
    assert call["resolution_status"] == "unmanaged"
    assert resolution["spec"] == "unmanaged"
    assert resolution["attachments"][0]["content_hash"] == sha256(contents).hexdigest()
    assert attempt["active_effect"]["key"]


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


def test_schema_one_profile_is_rejected(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    run = tmp_path / "artifacts" / "runs" / "legacy"
    run.mkdir(parents=True)
    (run / "_run.json").write_text(
        json.dumps({"run_manifest_schema": 1, "status": "completed"}),
        encoding="utf-8",
    )

    with pytest.raises(WorkflowProfileError, match="supported manifest"):
        dag.profile("legacy")


@pytest.mark.parametrize(
    "receipt",
    ["sidecar", "attempt", "candidate", "failure"],
)
def test_runtime_profile_rejects_unsupported_receipt_schema(
    tmp_path: Path,
    receipt: str,
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "ok"}

    result = dag.run()
    run = tmp_path / "artifacts" / "runs" / result.run_id
    attempt_root = next((run / "attempts").iterdir())
    state_path = attempt_root / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    if receipt == "sidecar":
        path = run / "work.json.meta.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["run_sidecar_schema"] = 999
    elif receipt == "attempt":
        path = state_path
        value = state
        value["attempt_receipt_schema"] = 999
    elif receipt == "candidate":
        path = attempt_root / state["candidate_file"]
        value = json.loads(path.read_text(encoding="utf-8"))
        value["candidate_schema"] = 999
    else:
        path = run / "failures" / "work.json"
        value = {"failure_schema": 999}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(WorkflowProfileError, match="schema|receipt|corrupt"):
        dag.profile(result.run_id)


@pytest.mark.parametrize("kind", ["symlink_dir", "fifo", "invalid_json"])
def test_runtime_profile_failure_receipts_are_owned_nonblocking_and_fail_closed(
    tmp_path: Path,
    kind: str,
) -> None:
    """Failure profiles must not follow external trees or block on special files."""
    if kind == "fifo" and not hasattr(os, "mkfifo"):
        pytest.skip("FIFO is not supported on this platform")

    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    result = dag.run(run_id=f"profile-failures-{kind}")
    run = tmp_path / "artifacts" / "runs" / result.run_id
    failures = run / "failures"
    failures.mkdir()
    if kind == "symlink_dir":
        external = tmp_path / "external-failures"
        external.mkdir()
        failures.rmdir()
        failures.symlink_to(external, target_is_directory=True)
    elif kind == "fifo":
        os.mkfifo(failures / "work.json")
    else:
        (failures / "work.json").write_text("{", encoding="utf-8")

    with pytest.raises(WorkflowProfileError, match="failure|owned|invalid"):
        dag.profile(result.run_id)


@pytest.mark.parametrize("use_snapshot", [False, True])
def test_runtime_profile_rejects_ordinary_run_directory_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    use_snapshot: bool,
) -> None:
    """A replacement run tree must not contribute failures to a profile."""
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "original"}

    result = dag.run(run_id="profile-ownership")
    run_path = tmp_path / "artifacts" / "runs" / result.run_id
    original_path = tmp_path / "original-run"
    replacement_path = tmp_path / "replacement-run"
    run_path.rename(original_path)
    shutil.copytree(original_path, replacement_path)
    failures = replacement_path / "failures"
    failures.mkdir()
    (failures / "external.json").write_text(
        json.dumps(
            {
                "failure_schema": 2,
                "node": "external-only",
                "status": "failed",
                "failure": {"failure_type": "runtime", "message": "external"},
            }
        ),
        encoding="utf-8",
    )
    original_path.rename(run_path)

    snapshot = _validate_run_integrity(run_path) if use_snapshot else None
    swapped = False
    original_validate = profile_module.validate_run_path

    def validate_then_replace(path: Path) -> Path:
        nonlocal swapped
        validated = original_validate(path)
        if not swapped:
            swapped = True
            path.rename(original_path)
            replacement_path.rename(path)
        return validated

    monkeypatch.setattr(profile_module, "validate_run_path", validate_then_replace)

    with pytest.raises(WorkflowProfileError, match="owned|changed|integrity"):
        if use_snapshot:
            assert snapshot is not None
            load_run_profile(run_path, _snapshot=snapshot)
        else:
            load_run_profile(run_path)

    assert swapped is True


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
