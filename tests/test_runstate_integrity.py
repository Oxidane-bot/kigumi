from __future__ import annotations

from pathlib import Path

import pytest

from kigumi._runstate import AttemptStore, StateIntegrityError
from kigumi.artifacts import sha


def _store(tmp_path: Path) -> AttemptStore:
    store = AttemptStore(tmp_path / "run", {})
    store.initialize()
    return store


def _receipt_path(tmp_path: Path) -> Path:
    return tmp_path / "run" / "attempts" / sha("work") / "attempt-0001.json"


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


def test_state_integrity_error_includes_path_and_parse_error(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.prepare("work", policy=None, declaration_digest="decl")
    receipt = _receipt_path(tmp_path)
    receipt.write_text("{", encoding="utf-8")

    with pytest.raises(StateIntegrityError) as raised:
        store.prepare("work", policy=None, declaration_digest="decl")

    assert str(receipt) in str(raised.value)
    assert "Expecting property name" in str(raised.value)
