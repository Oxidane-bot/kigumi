from __future__ import annotations

import json
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

from kigumi.calling import LLMCaller
from kigumi.slots import AdaptiveCapacity, FileSlots
from kigumi.testing import FakeTransport
from kigumi.transport import Response

_WORKER = """
import fcntl
import json
import sys
import time
from pathlib import Path

from kigumi.slots import FileSlots

lock_dir, counter_path, slots, capacity_file = sys.argv[1:]
counter = Path(counter_path)
guard = counter.with_suffix('.guard')

def update(delta):
    with guard.open('a+', encoding='utf-8') as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        if counter.exists():
            value = json.loads(counter.read_text(encoding='utf-8'))
        else:
            value = {'current': 0, 'peak': 0}
        value['current'] += delta
        value['peak'] = max(value['peak'], value['current'])
        counter.write_text(json.dumps(value), encoding='utf-8')
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

with FileSlots(lock_dir, int(slots), capacity_file or None).acquire():
    update(1)
    time.sleep(0.2)
    update(-1)
"""


def _run_workers(tmp_path: Path, slots: int, capacity_file: Path | None = None) -> int:
    tmp_path.mkdir(parents=True, exist_ok=True)
    script = tmp_path / "slot_worker.py"
    script.write_text(_WORKER, encoding="utf-8")
    counter = tmp_path / "counter.json"
    root = Path(__file__).resolve().parents[1]
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                str(script),
                str(tmp_path / "locks"),
                str(counter),
                str(slots),
                str(capacity_file) if capacity_file is not None else "",
            ],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    results = [process.communicate(timeout=10) for process in processes]
    assert all(process.returncode == 0 for process in processes), results
    return json.loads(counter.read_text(encoding="utf-8"))["peak"]


def test_disabled_slots_pass_through_without_filesystem_side_effects(
    tmp_path: Path, monkeypatch
) -> None:
    """教训 opt_in_throttling: 未配置限流时绝不创建锁目录。"""
    disabled_path = tmp_path / "disabled"
    with FileSlots(disabled_path, 0).acquire():
        pass
    assert not disabled_path.exists()

    for key in ("KIGUMI_REQUEST_LOCK_DIR", "KIGUMI_REQUEST_SLOTS", "KIGUMI_REQUEST_CAPACITY_FILE"):
        monkeypatch.delenv(key, raising=False)
    from_env = FileSlots.from_env()
    with from_env.acquire():
        pass
    assert not from_env.enabled


def test_file_slots_limit_parallel_processes(tmp_path: Path) -> None:
    """教训 cross_process_limit: 多进程真实请求不能超过配置槽位。"""
    assert _run_workers(tmp_path, slots=2) <= 2


def test_capacity_file_clamps_and_invalid_values_fall_back(tmp_path: Path) -> None:
    capacity = tmp_path / "capacity.txt"
    capacity.write_text("1", encoding="utf-8")
    assert _run_workers(tmp_path / "one", slots=3, capacity_file=capacity) <= 1
    capacity.write_text("not-a-number", encoding="utf-8")
    assert _run_workers(tmp_path / "fallback", slots=3, capacity_file=capacity) <= 3
    capacity.write_text("0", encoding="utf-8")
    assert _run_workers(tmp_path / "zero", slots=3, capacity_file=capacity) <= 1


def test_adaptive_capacity_halves_to_its_minimum(tmp_path: Path) -> None:
    """教训 adaptive_throttle: 静态槽数在长跑生产会被 429 打死，容量必须是跨进程共享的活值。"""
    capacity_file = tmp_path / "capacity"
    capacity = AdaptiveCapacity(capacity_file, max_slots=8)

    for expected in (4, 2, 1, 1):
        capacity.on_throttle()
        assert capacity_file.read_text(encoding="utf-8") == str(expected)


def test_adaptive_capacity_ramps_and_throttle_resets_streak(tmp_path: Path) -> None:
    """教训 adaptive_recovery: 静态槽数在长跑生产会被 429 打死，容量必须是跨进程共享的活值。"""
    capacity_file = tmp_path / "capacity"
    capacity = AdaptiveCapacity(capacity_file, max_slots=4, min_slots=1, ramp_successes=2)
    capacity.on_throttle()
    assert capacity_file.read_text(encoding="utf-8") == "2"
    capacity.on_success()
    capacity.on_success()
    assert capacity_file.read_text(encoding="utf-8") == "3"
    capacity.on_success()
    capacity.on_throttle()
    assert capacity_file.read_text(encoding="utf-8") == "1"
    capacity.on_success()
    capacity.on_success()
    assert capacity_file.read_text(encoding="utf-8") == "2"


def test_adaptive_capacity_file_is_consumed_by_file_slots(tmp_path: Path) -> None:
    """教训 adaptive_fileslots: 动态容量必须仍是 FileSlots 能读取的纯整数文件。"""
    capacity_file = tmp_path / "capacity"
    capacity = AdaptiveCapacity(capacity_file, max_slots=8)
    capacity.on_throttle()

    slots = FileSlots(tmp_path / "locks", slots=8, capacity_file=capacity_file)
    assert capacity_file.read_text(encoding="utf-8") == "4"
    assert slots._effective_slots() == 4


def test_cache_hit_does_not_acquire_request_slot(tmp_path: Path) -> None:
    """教训 cache_hit_slot: 命中缓存不应排队等待真实请求槽。"""

    class ExplodingSlots:
        def acquire(self):
            raise AssertionError("cache hit must not acquire a slot")

    transport = FakeTransport([Response("answer", {}, "stop")])
    assert LLMCaller(transport, tmp_path).call("hello") == "answer"
    assert LLMCaller(transport, tmp_path, slots=ExplodingSlots()).call("hello") == "answer"


def test_cache_miss_holds_slot_around_live_request(tmp_path: Path) -> None:
    # 只测命中不占槽还不够:集成本身悄悄消失时那条测试照样绿。
    # 这里断言 miss 的真实请求确实发生在持槽区间内。
    events: list[str] = []

    class RecordingTransport:
        def resolve(self, model: str) -> str:
            return model

        def complete(self, messages, model: str, **params) -> Response:
            events.append("complete")
            return Response("answer", {}, "stop")

    class RecordingSlots:
        @contextmanager
        def acquire(self):
            events.append("acquire")
            yield
            events.append("release")

    caller = LLMCaller(RecordingTransport(), tmp_path, slots=RecordingSlots())
    assert caller.call("hello") == "answer"
    assert events == ["acquire", "complete", "release"]
