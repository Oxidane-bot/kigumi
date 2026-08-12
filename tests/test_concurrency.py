from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from kigumi._runstate import RunManifestError
from kigumi.artifacts import atomic_write_json, sha
from kigumi.calling import DryRunError, LLMCaller, read_call_cache
from kigumi.config import KigumiConfig
from kigumi.dag import CheckpointPending, Dag
from kigumi.testing import FakeTransport

_CALLER = """
import sys
from pathlib import Path

from kigumi.calling import LLMCaller
from kigumi.transport import PreparedRequest, Response

class Transport:
    def cache_identity(self):
        return {'transport': 'subprocess-concurrency', 'schema': 1}

    def prepare(self, messages, model, params):
        return PreparedRequest(messages, model, params)

    def send(self, prepared):
        return Response('stable response', {'total_tokens': 1}, 'stop')

print(LLMCaller(Transport(), Path(sys.argv[1])).call('same request'))
"""


def test_multiple_processes_leave_a_valid_shared_cache(tmp_path: Path) -> None:
    """教训 shared_cache_atomicity: 并发写同一键后缓存仍必须是完整 JSON。"""
    script = tmp_path / "caller_worker.py"
    script.write_text(_CALLER, encoding="utf-8")
    cache_dir = tmp_path / "cache"
    root = Path(__file__).resolve().parents[1]
    processes = [
        subprocess.Popen(
            [sys.executable, str(script), str(cache_dir)],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]

    results = [process.communicate(timeout=30) for process in processes]

    assert all(process.returncode == 0 for process in processes), results
    assert [stdout.strip() for stdout, _ in results] == ["stable response"] * 4
    cache_files = list((cache_dir / "llm").glob("*.json"))
    assert len(cache_files) == 1
    assert json.loads(cache_files[0].read_text(encoding="utf-8"))["response"] == "stable response"


def test_l1_reader_accepts_a_legitimate_atomic_replacement(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A cache inode swap between parent bind and open must not become CORRUPT."""
    import kigumi._safe_io as safe_io

    caller = LLMCaller(FakeTransport(), tmp_path)
    prepared = caller.transport.prepare(
        [{"role": "user", "content": "hello"}],
        "default",
        {},
    )
    key = sha(
        {
            "transport": caller.transport.cache_identity(),
            "prepared": prepared.canonical(),
            "seed": 0,
        }
    )
    path = tmp_path / "llm" / f"{key}.json"
    old_payload = {
        "meta": {"key": key},
        "response": "old complete response",
        "response_sha256": sha("old complete response"),
    }
    new_payload = {
        "meta": {"key": key},
        "response": "new complete response",
        "response_sha256": sha("new complete response"),
    }
    atomic_write_json(path, old_payload)

    open_attempt = threading.Event()
    replacement_done = threading.Event()
    original_open = safe_io._open_regular_file_at

    def delayed_open(directory, name, **kwargs):
        if name == path.name:
            open_attempt.set()
            if not replacement_done.wait(timeout=5):
                raise AssertionError("timed out waiting for atomic cache replacement")
        return original_open(directory, name, **kwargs)

    monkeypatch.setattr(safe_io, "_open_regular_file_at", delayed_open)
    result: list[str] = []
    failures: list[BaseException] = []

    def read_cache() -> None:
        try:
            result.append(caller.call("hello"))
        except BaseException as error:
            failures.append(error)

    reader = threading.Thread(target=read_cache)
    reader.start()
    assert open_attempt.wait(timeout=5)
    atomic_write_json(path, new_payload)
    replacement_done.set()
    reader.join(timeout=5)

    assert not reader.is_alive()
    assert failures == []
    assert result in ([old_payload["response"]], [new_payload["response"]])
    assert caller.transport.requests == []


def test_l1_reader_accepts_replacement_after_old_descriptor_is_open(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A replaced old inode may change ctime after its complete read snapshot."""
    import kigumi._safe_io as safe_io

    caller = LLMCaller(FakeTransport(), tmp_path)
    key = "cache-key"
    path = tmp_path / "llm" / f"{key}.json"
    old_payload = {
        "meta": {"key": key},
        "response": "old complete response",
        "response_sha256": sha("old complete response"),
    }
    new_payload = {
        "meta": {"key": key},
        "response": "new complete response",
        "response_sha256": sha("new complete response"),
    }
    atomic_write_json(path, old_payload)

    original_verify = safe_io.verify_regular_descriptor
    verify_calls = 0

    def replace_before_final_verify(handle, target, **kwargs):
        nonlocal verify_calls
        verify_calls += 1
        if verify_calls == 2:
            atomic_write_json(path, new_payload)
        return original_verify(handle, target, **kwargs)

    monkeypatch.setattr(safe_io, "verify_regular_descriptor", replace_before_final_verify)

    lookup = read_call_cache(path)

    assert lookup.state == "VALID"
    assert lookup.data == old_payload
    assert caller.transport.requests == []


@pytest.mark.parametrize("mutation_phase", ("before_open", "after_open"))
def test_l1_reader_rejects_complete_in_place_mutation_around_open(
    tmp_path: Path, monkeypatch: Any, mutation_phase: str
) -> None:
    """A self-consistent rewrite of the same inode is never a cache hit."""
    import kigumi._safe_io as safe_io

    key = "cache-key"
    path = tmp_path / "llm" / f"{key}.json"
    original_payload = {
        "meta": {"key": key},
        "response": "original response",
        "response_sha256": sha("original response"),
    }
    tampered_payload = {
        "meta": {"key": key},
        "response": "tampered but self-consistent response",
        "response_sha256": sha("tampered but self-consistent response"),
    }
    atomic_write_json(path, original_payload)
    original_inode = path.stat().st_ino
    original_open = safe_io._open_regular_file_at

    def mutate_around_open(directory, name, **kwargs):
        if name == path.name and mutation_phase == "before_open":
            path.write_text(json.dumps(tampered_payload), encoding="utf-8")
        handle = original_open(directory, name, **kwargs)
        if name == path.name and mutation_phase == "after_open":
            path.write_text(json.dumps(tampered_payload), encoding="utf-8")
        return handle

    monkeypatch.setattr(safe_io, "_open_regular_file_at", mutate_around_open)

    lookup = read_call_cache(path)

    assert lookup.state == "CORRUPT"
    assert "changed" in (lookup.reason or "")
    assert path.stat().st_ino == original_inode


def test_l1_reader_rejects_in_place_truncation_after_read(tmp_path: Path, monkeypatch: Any) -> None:
    """An in-place truncation is not an atomic replacement and stays corrupt."""
    import kigumi._safe_io as safe_io

    key = "cache-key"
    path = tmp_path / "llm" / f"{key}.json"
    payload = {
        "meta": {"key": key},
        "response": "complete response",
        "response_sha256": sha("complete response"),
    }
    atomic_write_json(path, payload)

    original_verify = safe_io.verify_regular_descriptor
    verify_calls = 0

    def truncate_before_final_verify(handle, target, **kwargs):
        nonlocal verify_calls
        verify_calls += 1
        if verify_calls == 2:
            path.write_bytes(b"")
        return original_verify(handle, target, **kwargs)

    monkeypatch.setattr(safe_io, "verify_regular_descriptor", truncate_before_final_verify)

    lookup = read_call_cache(path)

    assert lookup.state == "CORRUPT"
    assert "read failed" in (lookup.reason or "")


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="mkfifo is unavailable")
def test_l1_reader_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    """The cache boundary must reject a FIFO without waiting for a writer."""
    path = tmp_path / "llm" / "cache-key.json"
    path.parent.mkdir()
    os.mkfifo(path)

    started = time.monotonic()
    lookup = read_call_cache(path)

    assert lookup.state == "CORRUPT"
    assert time.monotonic() - started < 1


def test_l1_reader_stays_valid_under_atomic_replacement_pressure(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Repeated descriptor reads accept either complete side of atomic swaps."""
    import kigumi._safe_io as safe_io

    key = "cache-key"
    path = tmp_path / "llm" / f"{key}.json"
    payloads = [
        {
            "meta": {"key": key},
            "response": "old complete response",
            "response_sha256": sha("old complete response"),
        },
        {
            "meta": {"key": key},
            "response": "new complete response",
            "response_sha256": sha("new complete response"),
        },
    ]
    atomic_write_json(path, payloads[0])

    original_open = safe_io._open_regular_file_at

    def yield_after_open(directory, name, **kwargs):
        handle = original_open(directory, name, **kwargs)
        if name == path.name:
            time.sleep(0.0005)
        return handle

    monkeypatch.setattr(safe_io, "_open_regular_file_at", yield_after_open)
    writes = 160
    writer_done = threading.Event()
    failures: list[BaseException] = []
    lookups: list[Any] = []

    def write_cache() -> None:
        try:
            for index in range(writes):
                atomic_write_json(path, payloads[index % len(payloads)])
        except BaseException as error:
            failures.append(error)
        finally:
            writer_done.set()

    writer = threading.Thread(target=write_cache)
    writer.start()
    for _ in range(writes):
        lookups.append(read_call_cache(path))
    writer.join(timeout=10)

    assert not writer.is_alive()
    assert failures == []
    assert writer_done.is_set()
    corrupt_reasons = [lookup.reason for lookup in lookups if lookup.state != "VALID"]
    assert lookups and not corrupt_reasons, corrupt_reasons


def test_l1_reader_outlasts_more_than_four_consecutive_atomic_open_races(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A finite burst of legitimate publishers must not exhaust the snapshot reader."""
    import kigumi._safe_io as safe_io

    key = "cache-key"
    path = tmp_path / "llm" / f"{key}.json"
    payloads = [
        {
            "meta": {"key": key},
            "response": response,
            "response_sha256": sha(response),
        }
        for response in ("first complete response", "second complete response")
    ]
    atomic_write_json(path, payloads[0])

    original_open = safe_io._open_regular_file_at
    replacements = 8
    calls = 0

    def replace_before_open(directory, name, **kwargs):
        nonlocal calls
        calls += 1
        if name == path.name and calls <= replacements:
            atomic_write_json(path, payloads[calls % len(payloads)])
        return original_open(directory, name, **kwargs)

    monkeypatch.setattr(safe_io, "_open_regular_file_at", replace_before_open)

    lookup = read_call_cache(path)

    assert lookup.state == "VALID"
    assert calls == replacements + 1


def _make_dag(tmp_path: Path) -> Dag:
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    return Dag(config, LLMCaller(FakeTransport(), tmp_path))


def test_parallel_changed_run_declaration_does_not_archive(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """0.6 manifests reject changed declarations before archive allocation."""
    import kigumi.dag as dag_module

    barrier = threading.Barrier(2)

    def build(revision: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("alpha", params={"revision": revision})
        def alpha(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            del inputs
            barrier.wait(timeout=5)
            return {"revision": ctx.params["revision"]}

        @dag.node("beta", params={"revision": revision})
        def beta(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            del inputs
            barrier.wait(timeout=5)
            return {"revision": ctx.params["revision"]}

        return dag

    build(1).run(run_id="shared", workers=2)
    calls = 0
    original = dag_module.store.next_history_id

    def count_history_id(path: Path) -> str:
        nonlocal calls
        calls += 1
        return original(path)

    monkeypatch.setattr(dag_module.store, "next_history_id", count_history_id)
    with pytest.raises(RunManifestError, match="declaration changed"):
        build(2).run(run_id="shared", workers=2)

    assert calls == 0
    assert not (tmp_path / "artifacts" / "runs" / "shared" / "history").exists()


def test_map_parallel_failures_preserve_all_details_and_dry_run(tmp_path: Path) -> None:
    def build(error: type[Exception]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
            del inputs, ctx
            return {"items": [{"id": "first"}, {"id": "second"}]}

        @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
        def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            del inputs, ctx
            raise error(item["id"])

        return dag

    with pytest.raises(RuntimeError) as failures:
        build(ValueError).run(workers=2)

    assert "first (ValueError: first)" in str(failures.value)
    assert "second (ValueError: second)" in str(failures.value)
    assert isinstance(failures.value.__cause__, ValueError)
    assert str(failures.value.__cause__) == "first"
    with pytest.raises(DryRunError, match="first"):
        build(DryRunError).run(workers=2)


def test_map_pending_and_success_write_success_sidecar(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        del inputs, ctx
        return {"items": [{"id": "pending"}, {"id": "success"}]}

    @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        if item["id"] == "pending":
            raise CheckpointPending("approve-pending", {"id": item["id"]})
        return {"id": item["id"]}

    result = dag.run(run_id="mixed", workers=2)

    assert result.pending_checkpoints == ["approve-pending"]
    assert (tmp_path / "artifacts" / "runs" / "mixed" / "work@success.json.meta.json").is_file()
