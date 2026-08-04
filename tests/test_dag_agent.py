from __future__ import annotations

import json
from itertools import repeat
from pathlib import Path
from typing import Any

import pytest

from kigumi.agents import (
    AgentCapabilities,
    AgentCompletion,
    AgentFileSelector,
    AgentPublish,
    AgentRunResult,
    AgentTask,
)
from kigumi.artifacts import sha
from kigumi.calling import LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.evidence import EvidencePolicy
from kigumi.pi import PiRpcAdapter
from kigumi.testing import FakeTransport
from kigumi.transport import Response
from tests._agent_helpers import make_agent_spec
from tests._dag_helpers import _make_dag


class WritingAdapter:
    def __init__(self) -> None:
        self.runs = 0

    def cache_identity(self) -> dict[str, Any]:
        return {"adapter": "fake", "build": "1"}

    def capabilities(self) -> AgentCapabilities:
        return AgentCapabilities(filesystem=True)

    def run(self, request: Any, context: Any) -> AgentRunResult:
        self.runs += 1
        assert request.inputs == {"brief": {"text": "hello"}}
        assert (context.workspace / "skill.md").read_text(encoding="utf-8") == "be concise"
        (context.workspace / "notes").mkdir()
        (context.workspace / "draft.md").write_text("draft", encoding="utf-8")
        (context.workspace / "notes" / "reasoning.md").write_text("why", encoding="utf-8")
        return AgentRunResult(AgentCompletion("completed", "done", ("draft.md",), {}))


def _make_profile_dag(tmp_path: Path) -> tuple[Dag, Path]:
    capsule = tmp_path / "agents" / "writer"
    capsule.parent.mkdir(parents=True)
    make_agent_spec(capsule)
    config = KigumiConfig(
        project_root=tmp_path,
        source_dirs=[],
        agent_profiles={
            "writer": {
                "capsule": "agents/writer",
                "runtime": "pi",
                "expected_version": "0.83.0",
            }
        },
    )
    transport = FakeTransport(repeat(Response("model output", {"total_tokens": 1}, "stop")))
    return Dag(config, LLMCaller(transport, tmp_path / "llm")), capsule


def test_agent_profile_registers_call_and_scan_once_and_reuses_binding(tmp_path: Path) -> None:
    dag, capsule = _make_profile_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "a"}]}

    @dag.agent("draft", profile="writer")
    def draft(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        del inputs, ctx
        return AgentTask("draft")

    @dag.agent_scan(
        "revise",
        profile="writer",
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
    )
    def revise(item: dict[str, Any], carry: Any, inputs: dict[str, Any], ctx: Any) -> AgentTask:
        del item, carry, inputs, ctx
        return AgentTask("revise")

    draft_node = dag._nodes["draft"]
    revise_node = dag._nodes["revise"]
    assert isinstance(draft_node.agent_adapter, PiRpcAdapter)
    assert draft_node.agent_spec is not None
    assert draft_node.agent_spec.root == capsule.resolve()
    assert draft_node.agent_adapter is revise_node.agent_adapter
    assert draft_node.agent_spec is revise_node.agent_spec
    assert revise_node.scan is True


def test_agent_profile_and_explicit_binding_have_the_same_cache_identity(
    tmp_path: Path,
) -> None:
    profile_dag, capsule = _make_profile_dag(tmp_path / "profile")

    @profile_dag.agent("work", profile="writer")
    def profile_work(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        del inputs, ctx
        return AgentTask("work")

    explicit_root = tmp_path / "explicit"
    explicit_root.mkdir()
    explicit_spec = make_agent_spec(explicit_root / "agent")
    explicit_dag = _make_dag(explicit_root)

    @explicit_dag.agent(
        "work",
        adapter=PiRpcAdapter(("pi",), expected_version="0.83.0"),
        spec=explicit_spec,
    )
    def explicit_work(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        del inputs, ctx
        return AgentTask("work")

    profile_node = profile_dag._nodes["work"]
    explicit_node = explicit_dag._nodes["work"]
    assert profile_node.agent_identity is not None
    assert explicit_node.agent_identity is not None
    assert profile_node.agent_identity == explicit_node.agent_identity
    assert profile_node.external_fingerprint_digest == explicit_node.external_fingerprint_digest
    assert profile_node.agent_spec is not None
    assert profile_node.agent_spec.root == capsule.resolve()


def test_agent_profile_binding_errors_are_explicit(tmp_path: Path) -> None:
    dag, _ = _make_profile_dag(tmp_path)
    spec = make_agent_spec(tmp_path / "explicit-agent")
    adapter = PiRpcAdapter(("pi",), expected_version="0.83.0")

    with pytest.raises(ValueError, match="Unknown Agent profile 'missing'"):
        dag.agent("missing", profile="missing")

    with pytest.raises(ValueError, match="cannot be combined"):
        dag.agent("mixed", profile="writer", adapter=adapter, spec=spec)

    with pytest.raises(TypeError, match="profile= or both adapter= and spec="):
        dag.agent("unbound")

    with pytest.raises(ValueError, match="cannot be combined"):
        dag.agent_scan(
            "mixed-scan",
            profile="writer",
            adapter=adapter,
            spec=spec,
            items_from=("source", "items"),
        )


def test_dag_agent_uses_normal_cache_and_publishes_exact_attachments(tmp_path: Path) -> None:
    (tmp_path / "skill.md").write_text("be concise", encoding="utf-8")
    adapter = WritingAdapter()
    spec = make_agent_spec(tmp_path / "agent")

    def build() -> Any:
        dag = _make_dag(tmp_path)

        @dag.node("brief")
        def brief(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"text": "hello"}

        @dag.agent(
            "draft",
            adapter=adapter,
            spec=spec,
            deps=("brief",),
            files=("skill.md",),
        )
        def draft(inputs: dict[str, Any], ctx: Any) -> AgentTask:
            assert ctx.params == {}
            assert not hasattr(ctx, "project_root")
            return AgentTask(
                "write a draft",
                collect=(AgentFileSelector("draft.md"), AgentFileSelector("notes/*.md")),
                publish=(AgentPublish("draft.md", "generated/draft.md"),),
            )

        return dag

    first = build().run()
    artifact = first.artifacts["draft"]
    assert artifact["agent_schema"] == 3
    assert artifact["task"]["instruction"] == "write a draft"
    assert artifact["completion"] == {
        "status": "completed",
        "summary": "done",
        "outputs": ["draft.md"],
        "metrics": {},
    }
    assert [item["workspace_path"] for item in artifact["attachments"]] == [
        "draft.md",
        "notes/reasoning.md",
    ]
    assert artifact["files"] == {"generated/draft.md": "draft"}
    assert (
        not {
            "usage",
            "duration_seconds",
            "manifest",
            "trajectory",
            "evidence",
        }
        & artifact.keys()
    )
    assert adapter.runs == 1

    cold_sidecar = json.loads(
        (tmp_path / "artifacts" / "runs" / first.run_id / "draft.json.meta.json").read_text(
            encoding="utf-8"
        )
    )
    assert cold_sidecar["origin_provenance"]["agent"]["workspace_manifest"] == [
        {"workspace_path": "draft.md", "status": "created"},
        {"workspace_path": "notes/reasoning.md", "status": "created"},
        {"workspace_path": "skill.md", "status": "unchanged"},
    ]

    output = tmp_path / "generated" / "draft.md"
    output.unlink()
    second = build().run()
    assert second.cache_hits == ["brief", "draft"]
    assert second.artifacts["draft"] == artifact
    assert output.read_text(encoding="utf-8") == "draft"
    assert adapter.runs == 1
    warm_sidecar = json.loads(
        (tmp_path / "artifacts" / "runs" / "run-0002" / "draft.json.meta.json").read_text(
            encoding="utf-8"
        )
    )
    expected_origin = cold_sidecar["origin_provenance"]
    assert warm_sidecar["execution_calls"] == []
    assert warm_sidecar["artifact_sha256"] == sha(artifact)
    assert warm_sidecar["origin_provenance"] == expected_origin
    assert warm_sidecar["prompt_sha256"] == sha("write a draft")
    assert warm_sidecar["model"] == spec.model
    assert warm_sidecar["params"] == {
        "provider": spec.provider,
        "thinking": spec.thinking,
        "tools": list(spec.tools),
        "limits": spec.limits.identity(),
    }
    assert expected_origin["kind"] == "agent"
    assert expected_origin["agent"]["instruction_sha256"] == sha("write a draft")
    assert expected_origin["agent"]["spec_digest"] == spec.digest
    assert expected_origin["evidence_policy_digest"]

    description = build().describe()["draft"]
    assert description["kind"] == "node"
    assert description["executor"] == "agent"
    assert description["agent"]["adapter"] == {"adapter": "fake", "build": "1"}
    assert description["agent"]["spec"]["digest"] == spec.digest


def test_agent_stages_the_same_file_snapshot_used_for_its_cache_key(tmp_path: Path) -> None:
    input_path = tmp_path / "input.txt"
    input_path.write_text("before", encoding="utf-8")
    staged: list[str] = []

    class SnapshotAdapter:
        def __init__(self, path: Path) -> None:
            self.path = path

        def cache_identity(self) -> dict[str, str]:
            return {"adapter": "snapshot", "version": "1"}

        def capabilities(self) -> AgentCapabilities:
            self.path.write_text("after", encoding="utf-8")
            return AgentCapabilities(filesystem=True)

        def run(self, request: Any, context: Any) -> AgentRunResult:
            del request
            staged.append((context.workspace / "input.txt").read_text(encoding="utf-8"))
            return AgentRunResult(AgentCompletion("completed", staged[-1]))

    dag = _make_dag(tmp_path)
    adapter = SnapshotAdapter(input_path)

    @dag.agent(
        "agent",
        adapter=adapter,
        spec=make_agent_spec(tmp_path / "agent"),
        files=("input.txt",),
        cache="off",
    )
    def agent(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        del inputs, ctx
        return AgentTask("read the input")

    result = dag.run(run_id="agent-file-snapshot")

    assert staged == ["before"]
    assert result.artifacts["agent"]["completion"]["summary"] == "before"


def test_agent_capsule_change_misses_cache_while_hit_skips_builder(tmp_path: Path) -> None:
    builders = 0
    capsule = tmp_path / "agent"
    first_spec = make_agent_spec(capsule)

    def build(spec):
        dag = _make_dag(tmp_path)

        @dag.agent("agent", adapter=adapter, spec=spec)
        def agent(inputs: dict[str, Any], ctx: Any) -> AgentTask:
            nonlocal builders
            builders += 1
            return AgentTask("work")

        return dag

    class NoFileAdapter(WritingAdapter):
        def run(self, request: Any, context: Any) -> AgentRunResult:
            self.runs += 1
            return AgentRunResult(AgentCompletion("completed", "done"))

    adapter = NoFileAdapter()
    build(first_spec).run()
    second = build(first_spec).run()
    assert second.cache_hits == ["agent"]
    assert builders == 1
    assert adapter.runs == 1

    (capsule / "skills" / "new.md").write_text("new skill", encoding="utf-8")
    build(type(first_spec).load(capsule)).run()
    assert builders == 2
    assert adapter.runs == 2


def test_agent_evidence_policy_miss_preserves_content_key_and_canonical_artifact(
    tmp_path: Path,
) -> None:
    class EvidenceAdapter(WritingAdapter):
        def run(self, request: Any, context: Any) -> AgentRunResult:
            self.runs += 1
            context.emit_event(
                {
                    "type": "message_end",
                    "message": {"content": "sensitive", "model": "fake-model"},
                }
            )
            context.record_evidence(
                "rpc.jsonl",
                b'{"type":"message","content":"sensitive"}\n',
                "application/x-ndjson",
            )
            return AgentRunResult(
                AgentCompletion("completed", "done"),
                usage={"total_tokens": 2},
            )

    adapter = EvidenceAdapter()
    spec = make_agent_spec(tmp_path / "agent")

    def build(policy: EvidencePolicy):
        dag = _make_dag(tmp_path)

        @dag.agent("work", adapter=adapter, spec=spec, evidence_policy=policy)
        def work(inputs: dict[str, Any], ctx: Any) -> AgentTask:
            return AgentTask("work")

        return dag

    full = build(EvidencePolicy()).run()
    redacted = build(EvidencePolicy(stderr="redacted", trajectory="redacted")).run()
    warm = build(EvidencePolicy(stderr="redacted", trajectory="redacted")).run()

    assert full.artifacts["work"] == redacted.artifacts["work"]
    assert adapter.runs == 2
    assert warm.cache_hits == ["work"]
    cache_keys = []
    for result in (full, redacted, warm):
        metadata = json.loads(
            (tmp_path / "artifacts" / "runs" / result.run_id / "work.json.meta.json").read_text(
                encoding="utf-8"
            )
        )
        cache_keys.append(metadata["cache_key"])
    assert len(set(cache_keys)) == 1
    redacted_origin = json.loads(
        (tmp_path / "artifacts" / "runs" / redacted.run_id / "work.json.meta.json").read_text(
            encoding="utf-8"
        )
    )["origin_provenance"]
    evidence = redacted_origin["agent"]["evidence"][0]
    data = build(EvidencePolicy()).blob_store.read_verified(evidence["kigumi_attachment"])
    assert b"sensitive" not in data

    (tmp_path / "artifacts" / "_cache" / "blobs" / evidence["kigumi_attachment"]).unlink()
    with pytest.raises(FileNotFoundError, match=evidence["kigumi_attachment"]):
        build(EvidencePolicy(stderr="redacted", trajectory="redacted")).run()


def test_agent_hash_only_evidence_writes_no_raw_evidence_blob(tmp_path: Path) -> None:
    class Adapter(WritingAdapter):
        def run(self, request: Any, context: Any) -> AgentRunResult:
            context.emit_event({"type": "message", "content": "sensitive"})
            context.record_evidence(
                "rpc.jsonl",
                b'{"type":"message","content":"sensitive"}\n',
                "application/x-ndjson",
            )
            return AgentRunResult(AgentCompletion("completed", "done"))

    dag = _make_dag(tmp_path)

    @dag.agent(
        "work",
        adapter=Adapter(),
        spec=make_agent_spec(tmp_path / "agent"),
        evidence_policy=EvidencePolicy(stderr="hash_only", trajectory="hash_only"),
    )
    def work(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask("work")

    run = dag.run()
    origin = json.loads(
        (tmp_path / "artifacts" / "runs" / run.run_id / "work.json.meta.json").read_text(
            encoding="utf-8"
        )
    )["origin_provenance"]["agent"]
    assert origin["trajectory"]["mode"] == "hash_only"
    assert "kigumi_attachment" not in origin["trajectory"]
    assert origin["evidence"][0]["mode"] == "hash_only"
    assert "kigumi_attachment" not in origin["evidence"][0]
    assert not list((tmp_path / "artifacts" / "_cache" / "blobs").glob("*"))
