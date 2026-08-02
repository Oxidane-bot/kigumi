from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

import kigumi.dag as dag_module
from kigumi import (
    Attachment,
    FileRef,
    LLMCaller,
    Message,
    ParamRef,
    PreflightPolicy,
    PromptRef,
    PromptResolution,
    PromptSpec,
    RequestTooLarge,
    ResolvedPrompt,
    ResponseSpec,
)
from kigumi.calling import LLMCaller as CallingLLMCaller
from kigumi.prompt import PromptCatalogSnapshot, PromptMaterial
from kigumi.testing import FakeTransport


def _resolution(
    *,
    messages: list[Message],
    attachments: list[Attachment] | None = None,
    response_spec: ResponseSpec | None = None,
) -> PromptResolution:
    return PromptResolution(
        spec_name="managed",
        structure_digest="structure",
        base={},
        layers=(),
        axes=(),
        materials=(),
        rendered_sha256="rendered",
        rendered_bytes=0,
        messages=messages,
        attachments=attachments or [],
        response_spec=response_spec or ResponseSpec(),
    )


def _file_spec() -> PromptSpec:
    return PromptSpec(
        "file_prompt",
        PromptRef("base"),
        materials=(PromptMaterial("file", FileRef(ParamRef("path"))),),
    )


def _resolve_file(tmp_path: Path, path: str, contents: bytes):
    prompts = tmp_path / "prompts"
    prompts.mkdir(exist_ok=True)
    (prompts / "base.md").write_text("before{{file}}after", encoding="utf-8")
    snapshot = PromptCatalogSnapshot.capture(prompts, prompt_specs=(_file_spec(),))
    return snapshot.resolve(
        _file_spec(),
        inputs={},
        params={"path": path},
        file_contents={path: contents},
    )


def test_resolution_digest_is_content_addressed_for_attachments(tmp_path: Path) -> None:
    first = _resolve_file(tmp_path, "one.txt", b"same content")
    second = _resolve_file(tmp_path, "two.txt", b"same content")
    changed = _resolve_file(tmp_path, "one.txt", b"changed content")

    assert first.resolution.attachments == [
        Attachment(
            path="one.txt",
            content_hash=sha256(b"same content").hexdigest(),
            mime_type="text/plain",
            size_bytes=len(b"same content"),
        )
    ]
    assert first.resolution.digest == second.resolution.digest
    assert first.resolution.digest != changed.resolution.digest


def test_preflight_rejects_oversized_request_before_cache_or_provider(tmp_path: Path) -> None:
    text = "x" * 1_000_000
    resolution = _resolution(messages=[Message("user", [text])])
    transport = FakeTransport()
    caller = LLMCaller(transport, tmp_path / "cache")

    with pytest.raises(RequestTooLarge) as raised:
        caller.call(ResolvedPrompt(text, resolution))

    report = raised.value.report
    assert report.is_valid() is False
    violation = next(item for item in report.violations if item.check == "token_count")
    assert violation.actual == report.estimated_tokens
    assert violation.limit == PreflightPolicy().max_tokens
    assert report.estimated_tokens > 200_000
    assert transport.requests == []
    assert not (tmp_path / "cache").exists()


def test_FileRef_is_end_to_end_manifest_and_digest_input(tmp_path: Path) -> None:
    resolved = _resolve_file(tmp_path, "test.txt", b"first")
    changed = _resolve_file(tmp_path, "test.txt", b"second")

    assert resolved.resolution.attachments[0].content_hash == sha256(b"first").hexdigest()
    assert resolved.resolution.attachments[0].size_bytes == 5
    assert resolved.resolution.attachments[0].mime_type == "text/plain"
    assert resolved.resolution.attachments[0].content_hash in json.dumps(
        resolved.resolution.canonical(), ensure_ascii=False
    )
    assert resolved.resolution.digest != changed.resolution.digest


def test_response_schema_identity_changes_managed_cache_key(tmp_path: Path) -> None:
    text = "same prompt"
    first = _resolution(
        messages=[Message("user", [text])],
        response_spec=ResponseSpec("a" * 64, "structured"),
    )
    second = _resolution(
        messages=[Message("user", [text])],
        response_spec=ResponseSpec("b" * 64, "structured"),
    )
    transport = FakeTransport()
    caller = CallingLLMCaller(transport, tmp_path / "cache")

    assert caller.call(ResolvedPrompt(text, first)) == "answer"
    assert caller.call(ResolvedPrompt(text, second)) == "answer"

    assert len(transport.requests) == 2
    assert caller.calls[0]["key"] != caller.calls[1]["key"]
    assert caller.calls[0]["prompt_resolution"]["response_spec"]["schema_sha256"] == "a" * 64


def test_cache_schema_seven_does_not_read_schema_six_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert dag_module.CACHE_SCHEMA == 7

    def build() -> Any:
        from kigumi.config import KigumiConfig
        from kigumi.dag import Dag

        return Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=[]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )

    monkeypatch.setattr(dag_module, "CACHE_SCHEMA", 6)
    old = build()

    @old.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "old"}

    old_result = old.run(run_id="old")

    monkeypatch.setattr(dag_module, "CACHE_SCHEMA", 7)
    new = build()

    @new.node("work")
    def work_again(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "new"}

    new_result = new.run(run_id="new")

    assert old_result.cache_hits == []
    assert new_result.cache_hits == []
    old_meta = json.loads(
        (tmp_path / "artifacts" / "runs" / "old" / "work.json.meta.json").read_text()
    )
    new_meta = json.loads(
        (tmp_path / "artifacts" / "runs" / "new" / "work.json.meta.json").read_text()
    )
    assert old_meta["key_components"]["kigumi"] != new_meta["key_components"]["kigumi"]
