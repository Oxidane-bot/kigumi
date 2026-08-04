from __future__ import annotations

import base64
import json
import os
import stat
import subprocess
import sys
import threading
import time
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

import kigumi.calling as calling_module
from kigumi import EvidencePolicy, ProviderFailure, ProviderFailureKind, observe
from kigumi.artifacts import sha
from kigumi.calling import (
    Budget,
    BudgetExceeded,
    CacheIntegrityError,
    DryRunError,
    LLMCaller,
    durable_side_effect_boundary,
)
from kigumi.prompt import PreflightPolicy, RequestTooLarge
from kigumi.testing import FakeTransport
from kigumi.transport import Response

_CROSS_PROCESS_CALLER = """
from __future__ import annotations

import fcntl
import json
import os
import sys
import time
from pathlib import Path

from kigumi.calling import LLMCaller
from kigumi.slots import FileSlots
from kigumi.testing import FakeTransport

cache_dir = Path(sys.argv[1])
lock_dir = Path(sys.argv[2])
state_dir = Path(sys.argv[3])
mode = sys.argv[4]
state_dir.mkdir(parents=True, exist_ok=True)
counter = state_dir / "provider-count.json"
guard = state_dir / "provider-count.guard"


def mark(name: str) -> None:
    (state_dir / f"{name}-{os.getpid()}").touch()


def wait_for_release() -> None:
    while not (state_dir / "release").exists():
        time.sleep(0.01)


def increment_provider_count() -> None:
    with guard.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        if counter.exists():
            value = json.loads(counter.read_text(encoding="utf-8"))
        else:
            value = {"calls": 0}
        value["calls"] += 1
        counter.write_text(json.dumps(value), encoding="utf-8")
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class CountingTransport(FakeTransport):
    def complete(self, messages, model, **params):
        increment_provider_count()
        mark("provider")
        if mode in {"flight", "disabled"}:
            wait_for_release()
        elif mode == "hold":
            while True:
                time.sleep(1)
        return super().complete(messages, model, **params)


if mode in {"flight", "disabled"}:
    mark("ready")
    while len(list(state_dir.glob("ready-*"))) < 2:
        time.sleep(0.01)

slots = None if mode == "disabled" else FileSlots(lock_dir, slots=2)
caller = LLMCaller(CountingTransport(), cache_dir, slots=slots)
print(caller.call("same request"), flush=True)
"""


def _wait_for_markers(state_dir: Path, prefix: str, count: int = 1) -> None:
    def marker_count() -> int:
        marker_prefix = f"{prefix}-"
        return sum(
            1
            for path in state_dir.glob(f"{prefix}-*")
            if path.name.removeprefix(marker_prefix).isdigit()
        )

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if marker_count() >= count:
            return
        time.sleep(0.01)
    assert marker_count() >= count


def _start_caller_worker(
    script: Path,
    cache_dir: Path,
    lock_dir: Path,
    state_dir: Path,
    mode: str,
) -> subprocess.Popen[str]:
    root = Path(__file__).resolve().parents[1]
    return subprocess.Popen(
        [sys.executable, str(script), str(cache_dir), str(lock_dir), str(state_dir), mode],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_file_reference_contract_is_documented() -> None:
    """教训 file_reference_contract: 文件内容件的缓存语义必须在实现前固定。"""
    assert "kigumi_file" in calling_module.__doc__
    assert "content hashes" in calling_module.__doc__


def test_cache_key_ignores_param_order(tmp_path: Path) -> None:
    """教训 bf06: parameter key order must not create a new cache family."""
    transport = FakeTransport()
    caller = LLMCaller(transport, tmp_path)

    first = caller.call("hello", temperature=0.2, max_tokens=12)
    second = caller.call("hello", max_tokens=12, temperature=0.2)

    assert first == second == "answer"
    assert len(transport.requests) == 1
    assert [call["cache"] for call in caller.calls] == ["miss", "hit"]


def test_seed_changes_cache_key(tmp_path: Path) -> None:
    """seed 是缓存命名空间，同请求不同 seed 必须各自未命中。"""
    transport = FakeTransport()
    first = LLMCaller(transport, tmp_path, seed=0)
    second = LLMCaller(transport, tmp_path, seed=1)

    assert first.call("hello") == "answer"
    assert second.call("hello") == "answer"
    assert len(transport.requests) == 2
    assert len(list((tmp_path / "llm").glob("*.json"))) == 2

    assert first.call("hello") == "answer"
    assert len(transport.requests) == 2
    assert first.calls[-1]["cache"] == "hit"


def test_observe_collects_call_metadata_and_resets(tmp_path: Path) -> None:
    caller = LLMCaller(FakeTransport(), tmp_path)

    with observe() as calls:
        caller.call("inside")

    assert len(calls) == 1
    caller.call("outside")
    assert len(calls) == 1


def test_call_evidence_policy_is_rebuilt_from_l1_without_provider_request(
    tmp_path: Path,
) -> None:
    transport = FakeTransport([Response("sensitive answer", {}, "stop")])
    full = LLMCaller(
        transport,
        tmp_path,
        evidence_policy=EvidencePolicy(request="full", response="full"),
    )
    assert full.call("sensitive prompt") == "sensitive answer"
    redacted = LLMCaller(
        transport,
        tmp_path,
        evidence_policy=EvidencePolicy(request="redacted", response="hash_only"),
    )
    assert redacted.call("sensitive prompt") == "sensitive answer"
    assert len(transport.requests) == 1
    assert redacted.calls[0]["request_evidence"][0]["content"]["redacted"] is True
    assert redacted.calls[0]["response_evidence"]["mode"] == "hash_only"


def test_failed_call_observation_contains_canonical_typed_failure(tmp_path: Path) -> None:
    class FailingTransport:
        def resolve(self, model: str) -> str:
            return model

        def complete(self, messages, model: str, **params):
            del messages, model, params
            error = type("TypedError", (RuntimeError,), {"status_code": 429})
            raise error("untrusted prose")

    caller = LLMCaller(FailingTransport(), tmp_path)
    with observe() as calls, pytest.raises(ProviderFailure) as raised:
        caller.call("hello", model="provider/model")
    assert raised.value.kind is ProviderFailureKind.RATE_LIMIT
    assert calls[0]["cache"] == "failure"
    assert calls[0]["failure"]["failure_type"] == "provider"
    assert calls[0]["failure"]["provider_failure"] == raised.value.canonical()


def test_empty_custom_transport_response_has_typed_failed_call_metadata(
    tmp_path: Path,
) -> None:
    caller = LLMCaller(FakeTransport([Response("", {}, "stop")]), tmp_path)
    with observe() as calls, pytest.raises(ProviderFailure) as raised:
        caller.call("hello")
    assert raised.value.kind is ProviderFailureKind.UNKNOWN
    assert calls[0]["failure"]["provider_failure"]["stage"] == "response"
    assert calls[0]["cache"] == "failure"


def test_torn_cache_raises_integrity_error(tmp_path: Path) -> None:
    """A torn historical cache cannot silently trigger a new provider call."""
    transport = FakeTransport()
    caller = LLMCaller(transport, tmp_path)
    key = sha(
        {
            "messages": [{"role": "user", "content": "hello"}],
            "model": "default",
            "params": {},
            "seed": 0,
        }
    )
    path = tmp_path / "llm" / f"{key}.json"
    path.parent.mkdir(parents=True)
    path.write_text('{"response": ', encoding="utf-8")

    with pytest.raises(CacheIntegrityError):
        caller.call("hello")
    assert transport.requests == []
    assert path.read_text(encoding="utf-8") == '{"response": '


def test_poisoned_empty_cache_raises_integrity_error(tmp_path: Path) -> None:
    """An invalid historical response cannot silently trigger a new provider call."""
    transport = FakeTransport()
    caller = LLMCaller(transport, tmp_path)
    key = sha(
        {
            "messages": [{"role": "user", "content": "hello"}],
            "model": "default",
            "params": {},
            "seed": 0,
        }
    )
    path = tmp_path / "llm" / f"{key}.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps({"meta": {"usage": {"total_tokens": 4}}, "response": ""}),
        encoding="utf-8",
    )

    with pytest.raises(CacheIntegrityError):
        caller.call("hello")
    assert transport.requests == []


def test_dry_run_raises_before_live_call(tmp_path: Path) -> None:
    transport = FakeTransport()

    with pytest.raises(DryRunError):
        LLMCaller(transport, tmp_path, dry=True).call("hello")

    assert transport.requests == []


def test_budget_exceeded_aborts(tmp_path: Path) -> None:
    """教训 provenance_before_budget: 超限的昂贵调用仍必须留下调用记录。"""
    budget = Budget(max_tokens=3)
    caller = LLMCaller(FakeTransport(), tmp_path, budget=budget)

    with pytest.raises(BudgetExceeded):
        caller.call("hello")

    assert budget.spent == 4
    assert caller.calls[0]["cache"] == "miss"


def test_budget_records_concurrently() -> None:
    """教训 concurrent_budget: 并行调用的 token 记账不能丢失增量。"""
    workers = 8
    records_per_worker = 100
    start = threading.Barrier(workers + 1)
    budget = Budget(max_tokens=None)

    def record_many() -> None:
        start.wait()
        for _ in range(records_per_worker):
            budget.record({"total_tokens": 1})

    threads = [threading.Thread(target=record_many) for _ in range(workers)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert budget.spent == workers * records_per_worker


@pytest.mark.parametrize(
    ("usage", "error_type"),
    [
        ({"total_tokens": -1}, ValueError),
        ({"total_tokens": 1.5}, TypeError),
        ({"total_tokens": True}, TypeError),
        ({"total_tokens": "3"}, TypeError),
        ({"total_tokens": None}, TypeError),
        (None, TypeError),
        ([], TypeError),
    ],
)
def test_budget_rejects_malformed_usage_without_mutating_spend(
    usage: object, error_type: type[Exception]
) -> None:
    """损坏的 provider 用量必须 fail closed，不能污染预算总账。"""
    budget = Budget(max_tokens=10)

    with pytest.raises(error_type):
        budget.record(usage)  # type: ignore[arg-type]
    assert budget.spent == 0

    permit = budget.reserve(1)
    with pytest.raises(error_type):
        permit.commit(usage)  # type: ignore[arg-type]
    assert budget.spent == 0
    permit.cancel()


@pytest.mark.parametrize(
    ("estimate", "error_type"),
    [
        (True, TypeError),
        (1.5, TypeError),
        ("3", TypeError),
        (None, TypeError),
        (-1, ValueError),
    ],
)
def test_budget_rejects_implicitly_coerced_estimates(
    estimate: object, error_type: type[Exception]
) -> None:
    """预算 estimate 必须是严格的非负 int，不能接受隐式数值转换。"""
    budget = Budget(max_tokens=10)

    with pytest.raises(error_type):
        budget.reserve(estimate)  # type: ignore[arg-type]

    assert budget.spent == 0
    assert budget._reserved == 0


def test_budget_treats_transport_empty_usage_as_zero_tokens() -> None:
    """transport 将缺失 usage 规范化为 {}, 其成功调用应记为零 token。"""
    budget = Budget(max_tokens=10)

    budget.record({})
    permit = budget.reserve(1)
    permit.commit({})

    assert budget.spent == 0


def test_provider_malformed_usage_is_rejected_before_cache_write(tmp_path: Path) -> None:
    """非法 usage 不能先写成功缓存再让预算记账失败。"""
    caller = LLMCaller(
        FakeTransport([Response("answer", {"total_tokens": -1}, "stop")]),
        tmp_path,
        budget=Budget(max_tokens=10),
    )

    with pytest.raises(ValueError):
        caller.call("hello")

    assert list((tmp_path / "llm").glob("*.json")) == []
    assert caller.budget is not None
    assert caller.budget.spent == 0


def test_cache_hit_skips_transport_and_budget(tmp_path: Path) -> None:
    budget = Budget(max_tokens=4)
    transport = FakeTransport()
    caller = LLMCaller(transport, tmp_path, budget=budget)

    assert caller.call("hello") == "answer"
    assert caller.call("hello") == "answer"

    assert len(transport.requests) == 1
    assert budget.spent == 4
    assert caller.calls[-1]["cache"] == "hit"


def test_reasoning_is_cached_but_not_in_call_metadata(tmp_path: Path) -> None:
    caller = LLMCaller(FakeTransport(), tmp_path)

    caller.call("hello")
    cache_file = next((tmp_path / "llm").glob("*.json"))
    payload = json.loads(cache_file.read_text(encoding="utf-8"))

    assert payload["reasoning"] == "private"
    assert "reasoning" not in caller.calls[0]


def test_provider_response_id_is_preserved_in_cache_and_call_provenance(tmp_path: Path) -> None:
    """A portable model artifact can trace a cached call back to its provider response."""
    transport = FakeTransport(
        [
            Response(
                "answer",
                {"total_tokens": 4},
                "stop",
                model="provider/model",
                provider_response_id="resp-123",
                model_observed=True,
            )
        ]
    )
    caller = LLMCaller(transport, tmp_path)

    assert caller.call("hello") == "answer"
    assert caller.call("hello") == "answer"

    payload = json.loads(next((tmp_path / "llm").glob("*.json")).read_text(encoding="utf-8"))
    assert payload["meta"]["provider_response_id"] == "resp-123"
    assert [call["provider_response_id"] for call in caller.calls] == ["resp-123", "resp-123"]
    assert payload["meta"]["provider_model"] == "provider/model"
    assert payload["meta"]["provider_model_observed"] is True
    assert [call["provider_model"] for call in caller.calls] == [
        "provider/model",
        "provider/model",
    ]
    assert [call["provider_model_observed"] for call in caller.calls] == [True, True]


def test_empty_transport_response_is_rejected_without_cache(tmp_path: Path) -> None:
    """教训 empty_response_poison: 非法空响应不能被第三方 transport 写进缓存。"""
    caller = LLMCaller(FakeTransport([Response("", {}, "stop")]), tmp_path)

    with pytest.raises(ProviderFailure) as raised:
        caller.call("hello")

    assert raised.value.kind is ProviderFailureKind.UNKNOWN
    assert raised.value.stage.value == "response"
    assert not (tmp_path / "llm").exists()


def test_resolved_model_changes_cache_key_and_provenance(tmp_path: Path) -> None:
    """教训 model_alias_drift: 别名的不同解析结果必须属于不同缓存族。"""
    transport = FakeTransport(resolved_models={"default": "provider/model-v1"})
    caller = LLMCaller(transport, tmp_path)

    caller.call("hello")
    transport.resolved_models["default"] = "provider/model-v2"
    caller.call("hello")

    payloads = [
        json.loads(path.read_text(encoding="utf-8")) for path in (tmp_path / "llm").glob("*.json")
    ]
    assert len(transport.requests) == 2
    assert len(payloads) == 2
    assert {payload["meta"]["model"] for payload in payloads} == {
        "provider/model-v1",
        "provider/model-v2",
    }
    assert {payload["meta"]["model_alias"] for payload in payloads} == {"default"}


def test_inflight_same_key_calls_transport_once(tmp_path: Path) -> None:
    """教训 inflight_dedup: 同键并发调用只允许一个真实请求穿透缓存。"""
    start = threading.Barrier(3)
    entered = threading.Event()
    release = threading.Event()

    class BlockingTransport(FakeTransport):
        def complete(
            self,
            messages: list[dict[str, Any]],
            model: str,
            **params: Any,
        ) -> Response:
            response = super().complete(messages, model, **params)
            entered.set()
            assert release.wait(timeout=2)
            return response

    caller = LLMCaller(BlockingTransport(), tmp_path)
    results: list[str] = []

    def invoke() -> None:
        start.wait()
        results.append(caller.call("hello"))

    first = threading.Thread(target=invoke)
    second = threading.Thread(target=invoke)
    first.start()
    second.start()
    start.wait()
    assert entered.wait(timeout=2)
    release.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert results == ["answer", "answer"]
    assert len(caller.transport.requests) == 1


def test_cross_process_same_key_calls_provider_once(tmp_path: Path) -> None:
    """教训 cross_process_single_flight: 同一 L1 键跨进程只允许一次穿透。"""
    script = tmp_path / "caller_worker.py"
    script.write_text(_CROSS_PROCESS_CALLER, encoding="utf-8")
    cache_dir = tmp_path / "cache"
    lock_dir = tmp_path / "locks"
    state_dir = tmp_path / "state"
    processes = [
        _start_caller_worker(script, cache_dir, lock_dir, state_dir, "flight") for _ in range(2)
    ]

    try:
        _wait_for_markers(state_dir, "ready", 2)
        _wait_for_markers(state_dir, "provider")
        (state_dir / "release").touch()
        results = [process.communicate(timeout=20) for process in processes]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()

    assert all(process.returncode == 0 for process in processes), results
    assert [stdout.strip() for stdout, _ in results] == ["answer", "answer"]
    assert json.loads((state_dir / "provider-count.json").read_text(encoding="utf-8")) == {
        "calls": 1
    }
    key_lock_files = list(lock_dir.glob("key_*.lock"))
    assert len(key_lock_files) == 1
    assert len(key_lock_files[0].stem.removeprefix("key_")) == 64
    assert key_lock_files[0].parent == lock_dir


def test_cross_process_key_lock_is_default_off(tmp_path: Path) -> None:
    """教训 opt_in_key_lock: 没有 FileSlots 配置时不创建键锁文件。"""
    script = tmp_path / "caller_worker.py"
    script.write_text(_CROSS_PROCESS_CALLER, encoding="utf-8")
    cache_dir = tmp_path / "cache"
    lock_dir = tmp_path / "locks"
    state_dir = tmp_path / "state"
    processes = [
        _start_caller_worker(script, cache_dir, lock_dir, state_dir, "disabled") for _ in range(2)
    ]

    try:
        _wait_for_markers(state_dir, "ready", 2)
        # Both no-lock workers must enter complete() before release, or the first
        # worker can write the L1 cache before the second performs its lookup.
        _wait_for_markers(state_dir, "provider", 2)
        (state_dir / "release").touch()
        results = [process.communicate(timeout=20) for process in processes]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()

    assert all(process.returncode == 0 for process in processes), results
    assert [stdout.strip() for stdout, _ in results] == ["answer", "answer"]
    assert json.loads((state_dir / "provider-count.json").read_text(encoding="utf-8")) == {
        "calls": 2
    }
    assert not lock_dir.exists()


def test_cross_process_key_lock_releases_after_holder_dies(tmp_path: Path) -> None:
    """教训 flock_crash_release: 持锁进程死亡后下一个 miss 仍可继续。"""
    script = tmp_path / "caller_worker.py"
    script.write_text(_CROSS_PROCESS_CALLER, encoding="utf-8")
    cache_dir = tmp_path / "cache"
    lock_dir = tmp_path / "locks"
    state_dir = tmp_path / "state"
    holder = _start_caller_worker(script, cache_dir, lock_dir, state_dir, "hold")

    try:
        _wait_for_markers(state_dir, "provider")
        holder.kill()
        holder_stdout, holder_stderr = holder.communicate(timeout=10)
        assert holder.returncode != 0, (holder_stdout, holder_stderr)

        follower = _start_caller_worker(script, cache_dir, lock_dir, state_dir, "follow")
        try:
            follower_stdout, follower_stderr = follower.communicate(timeout=10)
        finally:
            if follower.poll() is None:
                follower.kill()
                follower.wait()
        assert follower.returncode == 0, (follower_stdout, follower_stderr)
        assert follower_stdout.strip() == "answer"
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.wait()


def test_kigumi_file_cache_key_uses_content_hash(tmp_path: Path) -> None:
    """教训 file_content_addressing: 挪动同字节文件不能换缓存族，改字节必须换。"""
    original = tmp_path / "original.png"
    copied = tmp_path / "copied.png"
    original.write_bytes(b"first image")
    copied.write_bytes(b"first image")
    transport = FakeTransport()
    caller = LLMCaller(transport, tmp_path / "cache")

    assert caller.call([{"role": "user", "content": {"kigumi_file": str(original)}}]) == "answer"
    assert caller.call([{"role": "user", "content": {"kigumi_file": str(copied)}}]) == "answer"
    original.write_bytes(b"changed image")
    assert caller.call([{"role": "user", "content": {"kigumi_file": str(original)}}]) == "answer"

    assert len(transport.requests) == 2
    assert [call["cache"] for call in caller.calls] == ["miss", "hit", "miss"]


def test_direct_file_chat_is_unmanaged_but_keeps_secure_lineage(tmp_path: Path) -> None:
    """附件不应把无 PromptSpec 的 direct-chat 伪装成 managed request。"""
    source = tmp_path / "source.png"
    contents = b"direct-chat attachment"
    source.write_bytes(contents)
    transport = FakeTransport()
    caller = LLMCaller(transport, tmp_path / "cache")
    active_effect: dict[str, Any] = {}

    with durable_side_effect_boundary(active_effect.update):
        assert caller.call([{"role": "user", "content": {"kigumi_file": str(source)}}]) == "answer"

    resolution = active_effect["prompt_resolution"]
    assert active_effect["managed"] is False
    assert resolution["spec"] == "unmanaged"
    assert resolution["attachments"][0]["content_hash"] == sha256(contents).hexdigest()
    assert active_effect["key"] == caller.calls[0]["key"]
    assert caller.calls[0]["prompt_resolution"]["attachments"] == resolution["attachments"]


def test_kigumi_file_cache_keeps_reference_without_bytes(tmp_path: Path) -> None:
    """教训 file_cache_bloat: 旧实现 base64 内联会膨胀缓存并让挪文件失效。"""
    source = tmp_path / "source.pdf"
    contents = b"private document contents"
    source.write_bytes(contents)
    caller = LLMCaller(FakeTransport(), tmp_path / "cache")

    caller.call([{"role": "user", "content": {"kigumi_file": str(source)}}])

    cache_text = next((tmp_path / "cache" / "llm").glob("*.json")).read_text(encoding="utf-8")
    assert '"kigumi_file"' in cache_text
    assert '"kigumi_file_sha256"' in cache_text
    assert "file_data" not in cache_text
    assert base64.b64encode(contents).decode("ascii") not in cache_text


def test_kigumi_file_expands_only_for_live_transport(tmp_path: Path) -> None:
    image = tmp_path / "image.png"
    video = tmp_path / "clip.mp4"
    image.write_bytes(b"image bytes")
    video.write_bytes(b"video bytes")
    transport = FakeTransport()
    caller = LLMCaller(transport, tmp_path / "cache")
    image_messages = [
        {
            "role": "user",
            "content": {"kigumi_file": str(image), "detail": "high"},
        }
    ]
    video_messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "inspect this"},
                {"kigumi_file": str(video), "detail": "low"},
            ],
        }
    ]

    caller.call(image_messages)
    caller.call(video_messages)
    caller.call(image_messages)

    image_part = transport.requests[0][0][0]["content"][0]
    video_part = transport.requests[1][0][0]["content"][1]
    assert image_part == {
        "type": "image_url",
        "image_url": {
            "url": "data:image/png;base64,aW1hZ2UgYnl0ZXM=",
            "detail": "high",
        },
    }
    assert video_part["type"] == "file"
    assert video_part["file"]["format"] == "video/mp4"
    assert video_part["file"]["detail"] == "low"
    assert video_part["file"]["file_data"].startswith("data:video/mp4;base64,")
    assert len(transport.requests) == 2


def test_kigumi_file_missing_or_unknown_format_fails_before_call(tmp_path: Path) -> None:
    transport = FakeTransport()
    caller = LLMCaller(transport, tmp_path / "cache")

    with pytest.raises(FileNotFoundError, match="missing.png"):
        caller.call([{"role": "user", "content": {"kigumi_file": str(tmp_path / "missing.png")}}])
    unknown = tmp_path / "payload.unknown"
    unknown.write_bytes(b"contents")
    with pytest.raises(ValueError, match="payload.unknown"):
        caller.call([{"role": "user", "content": {"kigumi_file": str(unknown)}}])

    assert transport.requests == []


def test_kigumi_file_size_preflight_happens_before_hashing(tmp_path: Path, monkeypatch) -> None:
    """超限附件应先被 stat 拒绝，不能先完整读取/哈希文件。"""
    source = tmp_path / "payload.bin"
    source.write_bytes(b"payload")
    opened: list[Path] = []
    original_open = Path.open

    def tracking_open(path: Path, *args, **kwargs):
        if path == source:
            opened.append(path)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", tracking_open)
    transport = FakeTransport()
    caller = LLMCaller(
        transport,
        tmp_path / "cache",
        preflight_policy=PreflightPolicy(max_attachment_bytes=0),
    )

    with pytest.raises(RequestTooLarge):
        caller.call([{"role": "user", "content": {"kigumi_file": str(source)}}])

    assert opened == []
    assert transport.requests == []


def test_kigumi_file_rejects_non_regular_file_before_hashing(tmp_path: Path) -> None:
    """目录等非 regular 文件不能进入附件哈希路径。"""
    directory = tmp_path / "payload.bin"
    directory.mkdir()
    caller = LLMCaller(FakeTransport(), tmp_path / "cache")

    with pytest.raises(ValueError, match="regular file"):
        caller.call([{"role": "user", "content": {"kigumi_file": str(directory)}}])


@pytest.mark.skipif(
    not hasattr(os, "mknod") or not hasattr(os, "makedev"), reason="device files are unavailable"
)
def test_kigumi_file_rejects_character_device_before_hashing(tmp_path: Path) -> None:
    device = tmp_path / "device.bin"
    try:
        os.mknod(device, stat.S_IFCHR | 0o600, os.makedev(0, 0))
    except (PermissionError, OSError) as error:
        pytest.skip(f"character devices are unavailable: {error}")

    with pytest.raises(ValueError, match="regular file"):
        LLMCaller._file_reference({"kigumi_file": str(device)})


def test_kigumi_file_accepts_a_hardlink_to_a_regular_file(tmp_path: Path) -> None:
    source = tmp_path / "source.png"
    source.write_bytes(b"hardlink payload")
    alias = tmp_path / "alias.png"
    try:
        alias.hardlink_to(source)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support hardlinks: {error}")

    transport = FakeTransport()
    assert (
        LLMCaller(transport, tmp_path / "cache").call(
            [{"role": "user", "content": {"kigumi_file": str(alias)}}]
        )
        == "answer"
    )


def test_kigumi_file_rejects_a_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.png"
    target.write_bytes(b"symlink payload")
    alias = tmp_path / "alias.png"
    try:
        alias.symlink_to(target)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support symlinks: {error}")

    with pytest.raises(ValueError, match="regular file|symlink"):
        LLMCaller(FakeTransport(), tmp_path / "cache").call(
            [{"role": "user", "content": {"kigumi_file": str(alias)}}]
        )


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="mkfifo is unavailable")
def test_kigumi_file_race_to_fifo_does_not_block(tmp_path: Path) -> None:
    """stat 后路径变成 FIFO 时，附件哈希必须无阻塞地 fail closed。"""
    source = tmp_path / "payload.bin"
    source.write_bytes(b"payload")
    script = """
import os
import sys
from pathlib import Path

import kigumi.calling as calling

source = Path(sys.argv[1])
original_open = calling.os.open
replaced = False

def open_race(path, flags, mode=0o777, *, dir_fd=None):
    global replaced
    # File reads now bind the parent directory and open the final name
    # relative to its descriptor; race the descriptor-relative final open.
    if dir_fd is not None and str(path) == source.name and not replaced:
        replaced = True
        source.unlink()
        os.mkfifo(source)
    if dir_fd is None:
        return original_open(path, flags, mode)
    return original_open(path, flags, mode, dir_fd=dir_fd)

calling.os.open = open_race
calling.LLMCaller._file_reference({"kigumi_file": str(source)})
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", script, str(source)],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except subprocess.TimeoutExpired as error:
        pytest.fail(f"附件在 FIFO 竞态中阻塞: {error}")

    assert result.returncode != 0
    assert "regular" in result.stderr.lower()


def test_plain_messages_keep_existing_cache_key(tmp_path: Path) -> None:
    """教训 cache_compatibility: 没有文件引用的旧缓存必须逐字节继续命中。"""
    messages = [{"role": "user", "content": "hello"}]
    caller = LLMCaller(FakeTransport(), tmp_path)

    caller.call(messages, temperature=0.2)

    expected = sha(
        {
            "messages": messages,
            "model": "default",
            "params": {"temperature": 0.2},
            "seed": 0,
        }
    )
    assert caller.calls[0]["key"] == expected


def test_kigumi_file_refuses_to_send_content_changed_after_hashing(tmp_path: Path) -> None:
    """教训 hash_payload_binding: 算键后文件被换内容,发出即让内容寻址变成谎言,必须拒发。"""
    from kigumi.calling import LLMCaller as Caller

    source = tmp_path / "image.png"
    source.write_bytes(b"original bytes")
    reference = Caller._file_reference({"kigumi_file": str(source)})
    source.write_bytes(b"tampered bytes")

    with pytest.raises(ValueError, match="changed after hashing"):
        Caller._expand_file_reference(reference)
