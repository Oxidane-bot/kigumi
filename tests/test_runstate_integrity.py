from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

from kigumi._runstate import AttemptStore, RunManifestError, StateIntegrityError
from kigumi.artifacts import sha


def _store(tmp_path: Path) -> AttemptStore:
    store = AttemptStore(tmp_path / "run", {})
    store.initialize()
    return store


def _receipt_path(tmp_path: Path) -> Path:
    return tmp_path / "run" / "attempts" / sha("work") / "attempt-0001.json"


def _state_path(tmp_path: Path) -> Path:
    return tmp_path / "run" / "attempts" / sha("work") / "state.json"


def test_corrupt_attempt_receipt_fails_prepare_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    receipt = _receipt_path(tmp_path)
    receipt.write_text("{not-json", encoding="utf-8")

    with pytest.raises(StateIntegrityError):
        store.prepare("work", policy=None, declaration_digest="decl")


def test_missing_attempt_receipt_is_not_started_case(tmp_path: Path) -> None:
    store = _store(tmp_path)

    prepared = store.prepare("work", policy=None, declaration_digest="decl")
    receipt = _receipt_path(tmp_path)
    receipt.unlink()

    resumed = store.prepare("work", policy=None, declaration_digest="decl")

    assert prepared["action"] == "run"
    assert resumed["action"] == "run"


def test_tampered_running_state_fails_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    store.mark_side_effect("work", {"kind": "provider"})

    state_path = _state_path(tmp_path)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["side_effect_started"] = False
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(StateIntegrityError):
        store.prepare("work", policy=None, declaration_digest="decl")


def test_tampered_attempt_receipt_fails_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")

    receipt = _receipt_path(tmp_path)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["status"] = "completed"
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(StateIntegrityError):
        store.prepare("work", policy=None, declaration_digest="decl")


def test_coordinated_state_and_receipt_rewrite_fails_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    store.mark_side_effect("work", {"kind": "provider"})

    state_path = _state_path(tmp_path)
    receipt_path = _receipt_path(tmp_path)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["side_effect_started"] = False
    state["state_sha256"] = sha(
        {key: value for key, value in state.items() if key != "state_sha256"}
    )
    payload = json.dumps(state)
    state_path.write_text(payload, encoding="utf-8")
    receipt_path.write_text(payload, encoding="utf-8")

    with pytest.raises(StateIntegrityError):
        store.prepare("work", policy=None, declaration_digest="decl")


def test_state_integrity_error_includes_path_and_parse_error(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    receipt = _receipt_path(tmp_path)
    receipt.write_text("{", encoding="utf-8")

    with pytest.raises(StateIntegrityError) as raised:
        store.prepare("work", policy=None, declaration_digest="decl")

    assert str(receipt) in str(raised.value)
    assert "Expecting property name" in str(raised.value)


def test_mark_resumed_is_atomic_across_processes(tmp_path: Path) -> None:
    _store(tmp_path)
    worker = tmp_path / "mark_resumed_worker.py"
    worker.write_text(
        """
import sys
import time
from pathlib import Path

import kigumi._runstate as runstate
from kigumi._runstate import AttemptStore

original_write = runstate.atomic_write_json

def slow_write(path, payload):
    if Path(path).name == "_run.json":
        time.sleep(0.01)
    original_write(path, payload)

runstate.atomic_write_json = slow_write
attempts = AttemptStore(Path(sys.argv[1]), {})
for _ in range(int(sys.argv[2])):
    attempts.mark_resumed()
""",
        encoding="utf-8",
    )
    processes = [
        subprocess.Popen(
            [sys.executable, str(worker), str(tmp_path / "run"), "12"],
            cwd=Path(__file__).resolve().parents[1],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    results = [process.communicate(timeout=30) for process in processes]
    assert all(process.returncode == 0 for process in processes), results

    manifest = json.loads((tmp_path / "run" / "_run.json").read_text(encoding="utf-8"))
    assert manifest["resume_count"] == 48


def test_live_target_lease_blocks_second_attempt_owner(tmp_path: Path) -> None:
    ready = tmp_path / "ready"
    release = tmp_path / "release"
    worker = tmp_path / "lease_worker.py"
    worker.write_text(
        """
import sys
import time
from pathlib import Path

from kigumi._runstate import AttemptStore

run_root = Path(sys.argv[1])
ready = Path(sys.argv[2])
release = Path(sys.argv[3])
attempts = AttemptStore(run_root, {})
attempts.initialize()
attempts.prepare("work", policy=None, declaration_digest="decl")
ready.touch()
while not release.exists():
    time.sleep(0.01)
""",
        encoding="utf-8",
    )
    process = subprocess.Popen(
        [sys.executable, str(worker), str(tmp_path / "run"), str(ready), str(release)],
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists(), process.communicate(timeout=5)

        second = AttemptStore(tmp_path / "run", {})
        second.initialize()
        with pytest.raises(RunManifestError, match="Target .* busy"):
            second.prepare("work", policy=None, declaration_digest="decl")
        with pytest.raises(RunManifestError, match="Target .* busy"):
            second.mark_side_effect("work", {"kind": "provider"})
        assert not list((tmp_path / "run").glob("*.lock"))
    finally:
        release.touch()
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, (stdout, stderr)


def test_receipt_chain_is_monotonic_and_manifest_anchored(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    store.mark_side_effect("work", {"kind": "provider"})

    state = json.loads(_state_path(tmp_path).read_text(encoding="utf-8"))
    manifest = json.loads((tmp_path / "run" / "_run.json").read_text(encoding="utf-8"))
    chain = manifest["attempt_receipt_chains"][sha("work")]

    assert [entry["receipt_sequence"] for entry in chain] == [1, 2]
    assert chain[0]["previous_receipt_sha256"] is None
    assert chain[1]["previous_receipt_sha256"] == chain[0]["state_sha256"]
    assert state["receipt_sequence"] == chain[-1]["receipt_sequence"]
    assert state["state_sha256"] == chain[-1]["state_sha256"]
