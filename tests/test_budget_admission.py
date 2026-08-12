from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from kigumi.calling import Budget, BudgetExceeded, LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.testing import FakeTransport
from kigumi.transport import Response


def _build_budget_map(tmp_path: Path) -> tuple[Dag, list[str], FakeTransport, Budget]:
    budget = Budget(max_tokens=1)
    transport = FakeTransport([Response("answer", {"total_tokens": 1}, "stop") for _ in range(3)])
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(transport, tmp_path / "llm", budget=budget),
    )
    attempted: list[str] = []

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "first"}, {"id": "second"}, {"id": "third"}]}

    @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs
        attempted.append(item["id"])
        return {"answer": ctx.call(item["id"], max_tokens=1)}

    return dag, attempted, transport, budget


def test_budget_permit_commit_and_cancel() -> None:
    budget = Budget(max_tokens=10)

    permit = budget.reserve(6)
    assert budget.spent == 0
    permit.commit({"total_tokens": 4})
    assert budget.spent == 4

    cancelled = budget.reserve(6)
    cancelled.cancel()
    assert budget.spent == 4
    budget.reserve(6).cancel()


def test_budget_reserve_reports_requested_available_and_spent() -> None:
    budget = Budget(max_tokens=10)
    budget.record({"total_tokens": 4})

    with pytest.raises(BudgetExceeded) as raised:
        budget.reserve(7)

    message = str(raised.value)
    assert "requested 7" in message
    assert "available 6" in message
    assert "already spent 4" in message


def test_budget_exceeded_preserves_through_map_and_stops_following_items(
    tmp_path: Path,
) -> None:
    budget = Budget(max_tokens=1)
    transport = FakeTransport([Response("answer", {"total_tokens": 1}, "stop")])
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(transport, tmp_path / "llm", budget=budget),
    )
    attempted: list[str] = []

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "first"}, {"id": "second"}, {"id": "third"}]}

    @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs
        attempted.append(item["id"])
        return {"answer": ctx.call(item["id"], max_tokens=1)}

    with pytest.raises(BudgetExceeded) as raised:
        dag.run()

    assert type(raised.value) is BudgetExceeded
    # Prompt-plus-output admission is conservative enough to reject the first
    # call under a one-token ceiling before the provider is reached.
    assert attempted == ["first"]
    assert len(transport.requests) == 0
    assert budget.spent == 0


def test_budget_exceeded_parallel_map_does_not_start_later_items(tmp_path: Path) -> None:
    dag, attempted, transport, budget = _build_budget_map(tmp_path)

    with pytest.raises(BudgetExceeded) as raised:
        dag.run(workers=3)

    assert type(raised.value) is BudgetExceeded
    assert "third" not in attempted
    assert len(transport.requests) <= 1
    assert budget.spent <= 1


def test_budget_exceeded_map_respects_caller_workers_with_resource_limits(
    tmp_path: Path,
) -> None:
    dag, attempted, transport, budget = _build_budget_map(tmp_path)

    with pytest.raises(BudgetExceeded) as raised:
        dag.run(workers=1, resource_limits={None: 4})

    assert type(raised.value) is BudgetExceeded
    assert "third" not in attempted
    assert len(transport.requests) <= 1
    assert budget.spent <= 1


def test_budget_exceeded_propagates_from_scan_and_stops_later_items(tmp_path: Path) -> None:
    budget = Budget(max_tokens=1)
    transport = FakeTransport([Response("answer", {"total_tokens": 1}, "stop") for _ in range(3)])
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(transport, tmp_path / "llm", budget=budget),
    )
    attempted: list[str] = []

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "first"}, {"id": "second"}, {"id": "third"}]}

    @dag.scan("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del carry, inputs
        attempted.append(item["id"])
        return {"answer": ctx.call(item["id"], max_tokens=1)}

    with pytest.raises(BudgetExceeded) as raised:
        dag.run(workers=3)

    assert type(raised.value) is BudgetExceeded
    assert attempted == ["first"]
    assert len(transport.requests) <= 1


def test_budget_exceeded_propagates_from_foreach_and_stops_later_items(tmp_path: Path) -> None:
    budget = Budget(max_tokens=1)
    transport = FakeTransport([Response("answer", {"total_tokens": 1}, "stop") for _ in range(3)])
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(transport, tmp_path / "llm", budget=budget),
    )
    attempted: list[str] = []

    @dag.foreach(
        "work-{i}",
        [{"id": "first"}, {"id": "second"}, {"id": "third"}],
        params_fn=lambda item: {"id": item["id"]},
    )
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs
        attempted.append(ctx.params["id"])
        return {"answer": ctx.call(ctx.params["id"], max_tokens=1)}

    with pytest.raises(BudgetExceeded) as raised:
        dag.run(workers=3)

    assert type(raised.value) is BudgetExceeded
    assert "third" not in attempted


def test_estimate_tokens_includes_prompt_when_max_tokens_is_supplied() -> None:
    messages = [{"role": "user", "content": "x" * 100}]

    assert LLMCaller._estimate_tokens(messages, {"max_tokens": 1}) == 51


def test_non_budget_map_failures_still_aggregate_all_failed_items(tmp_path: Path) -> None:
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(FakeTransport(), tmp_path / "llm"),
    )
    attempted: list[str] = []

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "first"}, {"id": "second"}, {"id": "third"}]}

    @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        attempted.append(item["id"])
        if item["id"] in {"first", "second"}:
            raise ValueError(item["id"])
        return {"id": item["id"]}

    with pytest.raises(RuntimeError) as raised:
        dag.run(workers=3)

    assert set(attempted) == {"first", "second", "third"}
    assert "first (ValueError: first)" in str(raised.value)
    assert "second (ValueError: second)" in str(raised.value)


def test_cache_hit_skips_reserve_and_cache_miss_reserves_before_transport(
    tmp_path: Path,
) -> None:
    class RecordingBudget(Budget):
        def __init__(self) -> None:
            super().__init__(max_tokens=10)
            self.reservations: list[int] = []

        def reserve(self, estimated_tokens: int):
            self.reservations.append(estimated_tokens)
            return super().reserve(estimated_tokens)

    budget = RecordingBudget()

    class RecordingTransport(FakeTransport):
        def send(self, prepared):
            assert budget.reservations == [5]
            return super().send(prepared)

    transport = RecordingTransport([Response("answer", {"total_tokens": 2}, "stop")])
    caller = LLMCaller(transport, tmp_path, budget=budget)

    assert caller.call("hello", max_tokens=3) == "answer"
    assert budget.reservations == [5]
    assert budget.spent == 2

    assert caller.call("hello", max_tokens=3) == "answer"
    assert budget.reservations == [5]
    assert len(transport.requests) == 1
