from __future__ import annotations

from pathlib import Path

from kigumi.agents import AgentSpec


def make_agent_spec(root: Path, *, tools: tuple[str, ...] = ("read", "write")) -> AgentSpec:
    root.mkdir()
    (root / "SYSTEM.md").write_text("Work deterministically.\n", encoding="utf-8")
    (root / "skills").mkdir()
    (root / "hooks").mkdir()
    tool_list = ", ".join(f'"{tool}"' for tool in tools)
    (root / "agent.toml").write_text(
        f"""schema_version = 1
runtime = "pi"
provider = "fake"
model = "fake-model"
thinking = "off"
system_prompt = "SYSTEM.md"
skills = ["skills"]
hooks = []
tools = [{tool_list}]

[limits]
timeout_seconds = 30
max_turns = 10
max_tool_calls = 20
max_files = 100
max_bytes = 10485760
max_single_file_bytes = 2097152
inline_text_max_bytes = 65536
trajectory_max_events = 200
trajectory_max_bytes = 262144
rpc_max_bytes = 2097152
stderr_max_bytes = 262144
""",
        encoding="utf-8",
    )
    return AgentSpec.load(root)
