from __future__ import annotations

import pytest

from kigumi.agents import (
    AgentFileSelector,
    AgentPublish,
    AgentResultError,
    AgentTask,
    execute_agent_task,
)


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


def test_agent_task_rejects_invalid_prompt_resolution_before_execution() -> None:
    with pytest.raises(AgentResultError, match="prompt resolution"):
        execute_agent_task(
            node_name="agent",
            run_id="run",
            task=AgentTask("work"),
            inputs={},
            declared_files=(),
            resolve=lambda path: path,
            artifacts_path=None,  # type: ignore[arg-type]
            blob_store=None,  # type: ignore[arg-type]
            adapter=None,  # type: ignore[arg-type]
            adapter_identity={},
            spec=None,  # type: ignore[arg-type]
            prompt_resolution={"prompt_resolution_schema": 1},
        )
