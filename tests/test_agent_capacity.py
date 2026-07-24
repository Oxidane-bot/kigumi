from __future__ import annotations

import threading
import time
from itertools import repeat
from pathlib import Path
from typing import Any

import pytest

from kigumi import AgentExecutionFailure, AgentRuntimeFailureCode
from kigumi.agents import AgentCapabilities, AgentCompletion, AgentRunResult, AgentTask
from kigumi.calling import LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.slots import FileSlots
from kigumi.testing import FakeTransport
from kigumi.transport import Response
from tests._agent_helpers import make_agent_spec


class _SleepingAdapter:
    def __init__(self) -> None:
        self.runs = 0
        self.current = 0
        self.peak = 0
        self.lock = threading.Lock()

    def cache_identity(self) -> dict[str, Any]:
        return {"adapter": "sleeping", "version": 1}

    def capabilities(self) -> AgentCapabilities:
        return AgentCapabilities()

    def run(self, request: Any, context: Any) -> AgentRunResult:
        del request, context
        with self.lock:
            self.runs += 1
            self.current += 1
            self.peak = max(self.peak, self.current)
        time.sleep(0.05)
        with self.lock:
            self.current -= 1
        return AgentRunResult(AgentCompletion("completed", "done"))


def _capacity_dag(tmp_path: Path, adapter: _SleepingAdapter, *, slots: int) -> Dag:
    tmp_path.mkdir(parents=True)
    config = KigumiConfig(project_root=tmp_path, source_dirs=[], agent_slots=slots)
    caller = LLMCaller(
        FakeTransport(repeat(Response("unused", {}, "stop"))),
        tmp_path / "llm",
    )
    dag = Dag(config, caller)
    spec = make_agent_spec(tmp_path / "agent")

    @dag.agent("left", adapter=adapter, spec=spec, cache="off")
    def left(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask("left")

    @dag.agent("right", adapter=adapter, spec=spec, cache="off")
    def right(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask("right")

    return dag


def test_agent_capacity_defaults_to_one_and_explicit_slots_allow_parallelism(
    tmp_path: Path,
) -> None:
    serial_adapter = _SleepingAdapter()
    serial = _capacity_dag(tmp_path / "serial", serial_adapter, slots=1).run(workers=2)
    assert serial_adapter.peak == 1
    for name in ("left", "right"):
        origin = (
            tmp_path / "serial" / "artifacts" / "runs" / serial.run_id / f"{name}.json.meta.json"
        )
        metadata = __import__("json").loads(origin.read_text(encoding="utf-8"))
        assert metadata["origin_provenance"]["agent"]["slot_identity"] == "slot_000"
        assert metadata["origin_provenance"]["agent"]["queue_wait_seconds"] >= 0

    parallel_adapter = _SleepingAdapter()
    _capacity_dag(tmp_path / "parallel", parallel_adapter, slots=2).run(workers=2)
    assert parallel_adapter.peak == 2


def test_agent_slot_timeout_is_typed_and_happens_before_builder_or_adapter(
    tmp_path: Path,
) -> None:
    adapter = _SleepingAdapter()
    config = KigumiConfig(
        project_root=tmp_path,
        source_dirs=[],
        agent_slot_timeout_seconds=0.01,
    )
    dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
    spec = make_agent_spec(tmp_path / "agent")
    builders = 0

    @dag.agent("blocked", adapter=adapter, spec=spec, cache="off")
    def blocked(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        nonlocal builders
        builders += 1
        return AgentTask("blocked")

    with (
        FileSlots(config.agent_lock_path, 1).acquire(),
        pytest.raises(AgentExecutionFailure) as raised,
    ):
        dag.run()
    assert raised.value.runtime_code is AgentRuntimeFailureCode.CAPACITY
    assert builders == 0
    assert adapter.runs == 0


def test_agent_cache_hit_bypasses_an_occupied_global_slot(tmp_path: Path) -> None:
    adapter = _SleepingAdapter()
    config = KigumiConfig(
        project_root=tmp_path,
        source_dirs=[],
        agent_slot_timeout_seconds=0.01,
    )
    dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))

    @dag.agent(
        "cached",
        adapter=adapter,
        spec=make_agent_spec(tmp_path / "agent"),
    )
    def cached(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask("cached")

    assert dag.run().cache_hits == []
    with FileSlots(config.agent_lock_path, 1).acquire():
        replay = dag.run()
    assert replay.cache_hits == ["cached"]
    assert adapter.runs == 1


def test_agent_slot_is_released_when_adapter_fails(tmp_path: Path) -> None:
    class FailingAdapter(_SleepingAdapter):
        def run(self, request: Any, context: Any) -> AgentRunResult:
            del request, context
            raise RuntimeError("adapter failed")

    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))

    @dag.agent(
        "failing",
        adapter=FailingAdapter(),
        spec=make_agent_spec(tmp_path / "agent"),
        cache="off",
    )
    def failing(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask("fail")

    with pytest.raises(AgentExecutionFailure):
        dag.run()
    with FileSlots(config.agent_lock_path, 1).acquire(timeout_seconds=0.1) as lease:
        assert lease.slot_identity == "slot_000"
