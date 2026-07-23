from __future__ import annotations

from kigumi.agents import AgentFileSelector, AgentPublish, AgentTask


def test_agent_task_rejects_unsafe_or_duplicate_paths() -> None:
    for source in ("", "/absolute", "../escape", "a/../b"):
        try:
            AgentTask("write", collect=(AgentFileSelector(source),))
        except ValueError:
            pass
        else:
            raise AssertionError(f"unsafe selector was accepted: {source!r}")

    try:
        AgentTask(
            "write",
            collect=(AgentFileSelector("draft.md"),),
            publish=(
                AgentPublish("draft.md", "out.md"),
                AgentPublish("draft.md", "out.md"),
            ),
        )
    except ValueError as error:
        assert "duplicate" in str(error).lower()
    else:
        raise AssertionError("duplicate publish destination was accepted")
