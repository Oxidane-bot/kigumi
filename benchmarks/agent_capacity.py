"""Opt-in fake-Agent capacity stress benchmark for 4/8/16 ready nodes."""

from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from kigumi import AgentSpec, Dag, LLMCaller
from kigumi.agents import (
    AgentCapabilities,
    AgentCompletion,
    AgentRunResult,
    AgentTask,
)
from kigumi.config import KigumiConfig


class _UnusedTransport:
    def resolve(self, model: str) -> str:
        return model

    def complete(self, messages: object, model: str, **params: object) -> Any:
        raise AssertionError("capacity benchmark must not make provider calls")


class _Adapter:
    def __init__(self) -> None:
        self.current = 0
        self.peak = 0
        self.lock = threading.Lock()

    def cache_identity(self) -> dict[str, object]:
        return {"adapter": "capacity-benchmark", "version": 1}

    def capabilities(self) -> AgentCapabilities:
        return AgentCapabilities()

    def run(self, request: object, context: object) -> AgentRunResult:
        del request, context
        with self.lock:
            self.current += 1
            self.peak = max(self.peak, self.current)
        time.sleep(0.05)
        with self.lock:
            self.current -= 1
        return AgentRunResult(AgentCompletion("completed", "done"))


def _spec(root: Path) -> AgentSpec:
    root.mkdir()
    (root / "SYSTEM.md").write_text("Work deterministically.\n", encoding="utf-8")
    (root / "skills").mkdir()
    (root / "hooks").mkdir()
    (root / "agent.toml").write_text(
        """schema_version = 1
runtime = "pi"
provider = "fake"
model = "fake"
thinking = "off"
system_prompt = "SYSTEM.md"
skills = ["skills"]
hooks = []
tools = []

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


def run_case(workers: int, slots: int) -> tuple[float, int]:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        adapter = _Adapter()
        dag = Dag(
            KigumiConfig(
                project_root=root,
                source_dirs=[],
                agent_slots=slots,
            ),
            LLMCaller(_UnusedTransport(), root / "llm"),
        )
        spec = _spec(root / "agent")
        for index in range(workers):

            @dag.agent(
                f"agent-{index}",
                adapter=adapter,
                spec=spec,
                cache="off",
            )
            def agent(inputs: dict[str, Any], ctx: Any) -> AgentTask:
                del inputs, ctx
                return AgentTask("capacity benchmark")

        started = time.monotonic()
        dag.run(workers=workers)
        return time.monotonic() - started, adapter.peak


def main() -> None:
    for workers in (4, 8, 16):
        for slots in (1, workers):
            seconds, peak = run_case(workers, slots)
            print(f"workers={workers:2d} slots={slots:2d} peak={peak:2d} seconds={seconds:.3f}")


if __name__ == "__main__":
    main()
