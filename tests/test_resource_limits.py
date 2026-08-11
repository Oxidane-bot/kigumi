from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

import pytest

from kigumi import ResourceRequest
from tests._dag_helpers import _make_dag


def test_nodes_share_a_resource_limit(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    first_started = threading.Event()
    allow_first_finish = threading.Event()
    second_started = threading.Event()
    state_lock = threading.Lock()
    active = 0
    peak = 0

    @dag.node("first", resources=(ResourceRequest("gpu"),))
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        nonlocal active, peak
        del inputs, ctx
        with state_lock:
            active += 1
            peak = max(peak, active)
        first_started.set()
        assert allow_first_finish.wait(5)
        with state_lock:
            active -= 1
        return {"done": True}

    @dag.node("second", resources=(ResourceRequest("gpu"),))
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        nonlocal active, peak
        del inputs, ctx
        with state_lock:
            active += 1
            peak = max(peak, active)
        second_started.set()
        with state_lock:
            active -= 1
        return {"done": True}

    outcome: list[Any] = []

    def run() -> None:
        try:
            outcome.append(dag.run(workers=2, resource_limits={"gpu": 1}))
        except BaseException as error:  # pragma: no cover - surfaced below
            outcome.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    assert first_started.wait(5)
    allow_first_finish.set()
    thread.join(5)

    assert not thread.is_alive()
    assert len(outcome) == 1
    if isinstance(outcome[0], BaseException):
        raise outcome[0]
    assert outcome[0].artifacts == {
        "first": {"done": True},
        "second": {"done": True},
    }
    assert second_started.is_set()
    assert peak == 1


def test_map_items_share_a_resource_pool_with_a_concurrency_ceiling(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    state_lock = threading.Lock()
    active = 0
    peak = 0

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": str(index)} for index in range(5)]}

    @dag.map(
        "work",
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
        resources=(ResourceRequest("api"),),
    )
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        nonlocal active, peak
        del inputs, ctx
        with state_lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.03)
        with state_lock:
            active -= 1
        return item

    result = dag.run(workers=5, resource_limits={"api": 2})

    assert result.artifacts["work"]["count"] == 5
    assert peak == 2


def test_mixed_resource_types_can_overlap_and_multi_resource_nodes_share_each_pool(
    tmp_path: Path,
) -> None:
    dag = _make_dag(tmp_path)
    intervals: dict[str, tuple[float, float]] = {}
    api_lock = threading.Lock()
    api_active = 0
    api_peak = 0
    gpu_started = threading.Event()
    cpu_started = threading.Event()

    def record(
        name: str,
        *,
        started_event: threading.Event | None = None,
        wait_for: threading.Event | None = None,
    ) -> None:
        nonlocal api_active, api_peak
        started = time.monotonic()
        if name in {"gpu", "api"}:
            with api_lock:
                api_active += 1
                api_peak = max(api_peak, api_active)
        if started_event is not None:
            started_event.set()
            assert wait_for is not None
            assert wait_for.wait(5)
        if name in {"gpu", "api"}:
            with api_lock:
                api_active -= 1
        intervals[name] = (started, time.monotonic())

    @dag.node(
        "gpu",
        resources=(ResourceRequest("gpu"), ResourceRequest("api")),
    )
    def gpu(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        del inputs, ctx
        record("gpu", started_event=gpu_started, wait_for=cpu_started)
        return {"done": True}

    @dag.node("cpu", resources=(ResourceRequest("cpu"),))
    def cpu(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        del inputs, ctx
        record("cpu", started_event=cpu_started, wait_for=gpu_started)
        return {"done": True}

    @dag.node("api", resources=(ResourceRequest("api"),))
    def api(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        del inputs, ctx
        record("api")
        return {"done": True}

    dag.run(
        workers=3,
        resource_limits={"gpu": 1, "api": 1, "cpu": 1},
    )

    gpu_start, gpu_end = intervals["gpu"]
    cpu_start, cpu_end = intervals["cpu"]
    assert max(gpu_start, cpu_start) < min(gpu_end, cpu_end)
    assert api_peak == 1


def test_nodes_without_resources_use_the_default_pool(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    state_lock = threading.Lock()
    active = 0
    peak = 0

    for index in range(5):

        @dag.node(f"work-{index}")
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
            nonlocal active, peak
            del inputs, ctx
            with state_lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.03)
            with state_lock:
                active -= 1
            return {"done": True}

    result = dag.run(workers=5, resource_limits={None: 3})

    assert len(result.artifacts) == 5
    assert peak == 3


def test_resource_timeout_is_exposed_and_names_the_waited_resource(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    started = threading.Event()
    release = threading.Event()
    ran_nodes: list[str] = []
    outcome: list[BaseException | object] = []

    @dag.node("first", resources=(ResourceRequest("gpu"),))
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        del inputs, ctx
        ran_nodes.append("first")
        started.set()
        assert release.wait(5)
        return {"done": True}

    @dag.node("second", resources=(ResourceRequest("gpu"),))
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        del inputs, ctx
        ran_nodes.append("second")
        started.set()
        assert release.wait(5)
        return {"done": True}

    def execute() -> None:
        try:
            outcome.append(
                dag.run(
                    workers=2,
                    resource_limits={"gpu": 1},
                    resource_timeout_seconds=0.05,
                )
            )
        except BaseException as error:  # pragma: no cover - asserted below
            outcome.append(error)

    thread = threading.Thread(target=execute)
    thread.start()
    assert started.wait(5)
    time.sleep(0.5)
    release.set()
    thread.join(5)

    assert not thread.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], TimeoutError)
    assert "gpu" in str(outcome[0])
    assert "s for resource 'gpu'" in str(outcome[0])
    assert len(ran_nodes) == 1


def test_zero_resource_limit_fails_before_a_resource_node_runs(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    ran = False

    @dag.node("gpu", resources=(ResourceRequest("gpu"),))
    def gpu(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        nonlocal ran
        del inputs, ctx
        ran = True
        return {"done": True}

    with pytest.raises(ValueError, match=r"resource 'gpu'.*limit is 0"):
        dag.run(resource_limits={"gpu": 0})
    assert not ran
