from __future__ import annotations

from pathlib import Path
from typing import Any

from kigumi.agents import (
    AgentCapabilities,
    AgentCompletion,
    AgentFileSelector,
    AgentPublish,
    AgentRunResult,
    AgentTask,
)
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
    assert artifact["agent_schema"] == 1
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
    assert artifact["manifest"] == [
        {"workspace_path": "draft.md", "status": "created"},
        {"workspace_path": "notes/reasoning.md", "status": "created"},
        {"workspace_path": "skill.md", "status": "unchanged"},
    ]
    assert adapter.runs == 1

    output = tmp_path / "generated" / "draft.md"
    output.unlink()
    second = build().run()
    assert second.cache_hits == ["brief", "draft"]
    assert second.artifacts["draft"] == artifact
    assert output.read_text(encoding="utf-8") == "draft"
    assert adapter.runs == 1

    description = build().describe()["draft"]
    assert description["kind"] == "node"
    assert description["executor"] == "agent"
    assert description["agent"]["adapter"] == {"adapter": "fake", "build": "1"}
    assert description["agent"]["spec"]["digest"] == spec.digest


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
