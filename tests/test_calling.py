from __future__ import annotations

import base64
import json
import threading
from pathlib import Path
from typing import Any

import pytest

import kigumi.calling as calling_module
from kigumi.artifacts import sha
from kigumi.calling import Budget, BudgetExceeded, DryRunError, EmptyResponseError, LLMCaller
from kigumi.testing import FakeTransport
from kigumi.transport import Response


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
    from kigumi import observe

    caller = LLMCaller(FakeTransport(), tmp_path)

    with observe() as calls:
        caller.call("inside")

    assert len(calls) == 1
    caller.call("outside")
    assert len(calls) == 1


def test_torn_cache_treated_as_miss(tmp_path: Path) -> None:
    """A torn historical cache is discarded and atomically repaired by the live call."""
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

    assert caller.call("hello") == "answer"
    assert len(transport.requests) == 1
    assert json.loads(path.read_text(encoding="utf-8"))["response"] == "answer"


def test_poisoned_empty_cache_treated_as_miss(tmp_path: Path) -> None:
    """A historical cache entry with an empty response is invalid by definition: miss and repair."""
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

    assert caller.call("hello") == "answer"
    assert len(transport.requests) == 1
    assert json.loads(path.read_text(encoding="utf-8"))["response"] == "answer"


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
            )
        ]
    )
    caller = LLMCaller(transport, tmp_path)

    assert caller.call("hello") == "answer"
    assert caller.call("hello") == "answer"

    payload = json.loads(next((tmp_path / "llm").glob("*.json")).read_text(encoding="utf-8"))
    assert payload["meta"]["provider_response_id"] == "resp-123"
    assert [call["provider_response_id"] for call in caller.calls] == ["resp-123", "resp-123"]


def test_empty_transport_response_is_rejected_without_cache(tmp_path: Path) -> None:
    """教训 empty_response_poison: 非法空响应不能被第三方 transport 写进缓存。"""
    caller = LLMCaller(FakeTransport([Response("", {}, "stop")]), tmp_path)

    with pytest.raises(EmptyResponseError):
        caller.call("hello")

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
