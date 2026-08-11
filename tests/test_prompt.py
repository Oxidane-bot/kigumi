from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, Field

import kigumi.prompt as prompt_module
from kigumi.artifacts import sha
from kigumi.prompt import (
    Attachment,
    KigumiPromptWarning,
    Message,
    PromptResolution,
    PromptResolutionError,
    ResponseSpec,
    TemplateSlotError,
    clip,
    inject,
    load_template,
    preflight,
    render_items,
    render_template,
    schema_format_section,
    section,
    validate_prompt_resolution_record,
)

GOLDEN_FAILURE = (
    "公共 prompt 成分变更 = 全项目缓存换族,确认有意变更后更新 golden 并在 CHANGELOG 标注缓存失效"
)
GOLDENS = Path(__file__).parent / "goldens"


def _managed_record(
    messages: list[Message],
    attachments: list[Attachment],
    response_spec: ResponseSpec,
) -> dict[str, Any]:
    return PromptResolution(
        spec_name="managed",
        structure_digest="structure",
        base={"ref": "base", "sha256": "base-digest", "bytes": 0},
        layers=(),
        axes=(),
        materials=(),
        rendered_sha256="rendered",
        rendered_bytes=8,
        messages=messages,
        attachments=attachments,
        response_spec=response_spec,
    ).canonical()


class SnapshotLocation(BaseModel):
    city: str = Field(description="城市")


class SnapshotModel(BaseModel):
    title: str = Field(description="标题")
    enabled: bool = Field(description="是否启用")
    location: SnapshotLocation = Field(description="地点")
    tags: list[str] = Field(description="标签")


def test_managed_prompt_record_is_accepted_when_complete() -> None:
    messages = [Message(role="user", parts=["hello"])]
    attachments = [
        Attachment(
            path="input.txt",
            content_hash="a" * 64,
            mime_type="text/plain",
            size_bytes=5,
        )
    ]
    response_spec = ResponseSpec(schema_sha256="b" * 64, format="structured")
    record = _managed_record(messages, attachments, response_spec)

    validate_prompt_resolution_record(record)


@pytest.mark.parametrize(
    ("schema", "guidance"),
    (
        (0, "older than supported schema 1; no migration available — rebuild required"),
        (2, "newer than supported schema 1; upgrade kigumi"),
    ),
)
def test_persisted_prompt_resolution_schema_mismatch_reports_versions_and_guidance(
    schema: int, guidance: str
) -> None:
    record = _managed_record([], [], ResponseSpec())
    record["prompt_resolution_schema"] = schema

    with pytest.raises(PromptResolutionError) as error:
        validate_prompt_resolution_record(record)

    assert str(error.value) == f"persisted Prompt resolution schema {schema} is {guidance}"


def test_prompt_resolution_migration_registry_dispatches_and_preserves_record_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = _managed_record(
        [Message(role="user", parts=["hello"])],
        [],
        ResponseSpec(),
    )
    legacy_record = {**record, "prompt_resolution_schema": 0}
    seen: list[dict[str, Any]] = []

    def migrate(value: dict[str, Any]) -> dict[str, Any]:
        seen.append(dict(value))
        return {"prompt_resolution_schema": prompt_module.PROMPT_RESOLUTION_SCHEMA}

    monkeypatch.setitem(prompt_module.PROMPT_RESOLUTION_MIGRATIONS, 0, migrate)

    validate_prompt_resolution_record(legacy_record)

    assert seen == [legacy_record]
    assert legacy_record == {**record, "prompt_resolution_schema": 0}


def test_prompt_resolution_lineage_inputs_are_deeply_immutable() -> None:
    nested_part = {"type": "text", "metadata": {"labels": ["source"]}}
    message = Message(role="user", parts=[nested_part])
    resolution = PromptResolution(
        spec_name="managed",
        structure_digest="structure",
        base={},
        layers=(),
        axes=(),
        materials=(),
        rendered_sha256="rendered",
        rendered_bytes=8,
        messages=[message],
        attachments=[
            Attachment(
                path="input.txt",
                content_hash="a" * 64,
                mime_type="text/plain",
                size_bytes=5,
            )
        ],
    )
    digest = resolution.digest

    with pytest.raises((AttributeError, TypeError)):
        resolution.messages.append(message)
    with pytest.raises((AttributeError, TypeError)):
        resolution.attachments[0].path = "changed.txt"
    with pytest.raises((AttributeError, TypeError)):
        message.parts[0]["metadata"]["labels"].append("changed")

    canonical = resolution.canonical()
    canonical["messages"][0]["parts"][0]["metadata"]["labels"].append("changed")
    assert resolution.digest == digest
    assert nested_part["metadata"]["labels"] == ["source"]


def test_frozen_message_parts_still_support_preflight_serialization() -> None:
    resolution = PromptResolution(
        spec_name="managed",
        structure_digest="structure",
        base={},
        layers=(),
        axes=(),
        materials=(),
        rendered_sha256="rendered",
        rendered_bytes=8,
        messages=[Message("user", [{"kigumi_file_sha256": "a" * 64}])],
    )

    assert preflight(resolution).is_valid()


@pytest.mark.parametrize("schema", [True, 1.0])
def test_prompt_resolution_constructor_requires_native_schema_one(schema: object) -> None:
    with pytest.raises(PromptResolutionError, match="unsupported prompt resolution schema"):
        PromptResolution(
            spec_name="managed",
            structure_digest="structure",
            base={},
            layers=(),
            axes=(),
            materials=(),
            rendered_sha256="rendered",
            rendered_bytes=8,
            schema=schema,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("schema", "guidance"),
    (
        (0, "older than supported schema 1; no migration available — rebuild required"),
        (2, "newer than supported schema 1; upgrade kigumi"),
    ),
)
def test_prompt_resolution_constructor_schema_mismatch_reports_versions_and_guidance(
    schema: int, guidance: str
) -> None:
    with pytest.raises(PromptResolutionError) as error:
        PromptResolution(
            spec_name="managed",
            structure_digest="structure",
            base={},
            layers=(),
            axes=(),
            materials=(),
            rendered_sha256="rendered",
            rendered_bytes=8,
            schema=schema,
        )

    assert str(error.value) == f"persisted Prompt resolution schema {schema} is {guidance}"


def test_managed_prompt_record_requires_all_request_fields_before_digest_validation() -> None:
    messages = [Message(role="user", parts=["hello"])]
    attachments = [
        Attachment(
            path="input.txt",
            content_hash="a" * 64,
            mime_type="text/plain",
            size_bytes=5,
        )
    ]
    response_spec = ResponseSpec(schema_sha256="b" * 64, format="structured")
    record = _managed_record(messages, attachments, response_spec)

    for missing, replacement in (
        ("messages", []),
        ("attachments", []),
        ("response_spec", ResponseSpec()),
    ):
        candidate = dict(record)
        candidate.pop(missing)
        values: dict[str, Any] = {
            "messages": messages,
            "attachments": attachments,
            "response_spec": response_spec,
        }
        values[missing] = replacement
        candidate["resolution_digest"] = _managed_record(**values)["resolution_digest"]

        with pytest.raises(PromptResolutionError, match="managed request fields"):
            validate_prompt_resolution_record(candidate)


def test_legacy_prompt_resolution_record_is_rejected() -> None:
    body = {
        "prompt_resolution_schema": 1,
        "spec": "legacy",
        "structure_digest": "structure",
        "base": {},
        "layers": [],
        "axes": [],
        "materials": [],
        "rendered": {"sha256": "rendered", "bytes": 8},
    }
    record = {**body, "resolution_digest": sha(body)}

    with pytest.raises(PromptResolutionError, match="managed request fields"):
        validate_prompt_resolution_record(record)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("messages", ()),
        ("attachments", ()),
        ("response_spec", {"schema_sha256": None}),
        ("base", []),
    ),
)
def test_managed_prompt_record_rejects_required_field_type_or_shape(
    field: str,
    value: Any,
) -> None:
    record = _managed_record(
        [Message(role="user", parts=["hello"])],
        [],
        ResponseSpec(),
    )
    record[field] = value

    with pytest.raises(PromptResolutionError, match="managed request fields|digest"):
        validate_prompt_resolution_record(record)


def test_managed_prompt_record_rejects_digest_mismatch() -> None:
    record = _managed_record(
        [Message(role="user", parts=["hello"])],
        [],
        ResponseSpec(),
    )
    record["messages"] = [{"role": "user", "parts": ["changed"]}]

    with pytest.raises(PromptResolutionError, match="digest"):
        validate_prompt_resolution_record(record)


@pytest.mark.parametrize(
    ("section", "record"),
    (
        (
            "layers",
            {"slot": "layer", "ref": "fragment", "sha256": "digest"},
        ),
        (
            "axes",
            {
                "name": "mode",
                "selector": {},
                "selected": "concise",
                "ref": "concise",
                "sha256": "digest",
            },
        ),
        (
            "materials",
            {
                "slot": "material",
                "source": {},
                "title": None,
                "sha256": "digest",
                "bytes": True,
            },
        ),
    ),
)
def test_managed_prompt_record_validates_provenance_record_fields(
    section: str, record: dict[str, Any]
) -> None:
    """重新计算 digest 也不能把缺字段/错类型的 provenance 伪装成合法记录。"""
    candidate = _managed_record(
        [Message(role="user", parts=["hello"])],
        [],
        ResponseSpec(),
    )
    candidate[section] = [record]
    candidate["resolution_digest"] = sha(
        {key: value for key, value in candidate.items() if key != "resolution_digest"}
    )

    with pytest.raises(PromptResolutionError, match="managed request fields"):
        validate_prompt_resolution_record(candidate)


def test_managed_prompt_record_requires_strict_call_lineage_fields() -> None:
    """CALL lineage 的 phase/round/base digest 必须成组且类型和值严格。"""
    record = _managed_record(
        [Message(role="user", parts=["hello"])],
        [],
        ResponseSpec(),
    )
    digest = record["resolution_digest"]
    valid_lineage = {
        "base_resolution_digest": digest,
        "phase": "primary",
        "repair_round": 0,
    }

    validate_prompt_resolution_record({**record, **valid_lineage})

    for invalid_lineage in (
        {"phase": "primary", "repair_round": 0},
        {"base_resolution_digest": digest, "phase": 1, "repair_round": 0},
        {"base_resolution_digest": digest, "phase": "primary", "repair_round": True},
        {"base_resolution_digest": 3, "phase": "primary", "repair_round": 0},
        {"base_resolution_digest": digest, "phase": "repair", "repair_round": 0},
    ):
        with pytest.raises(PromptResolutionError, match="lineage"):
            validate_prompt_resolution_record({**record, **invalid_lineage})

    for missing in valid_lineage:
        incomplete = {**record, **valid_lineage}
        incomplete.pop(missing)
        with pytest.raises(PromptResolutionError, match="lineage"):
            validate_prompt_resolution_record(incomplete)


def test_attachment_manifest_metadata_is_not_prompt_lineage_identity() -> None:
    """Prompt lineage keys attachment bytes, not path/MIME/preflight metadata."""
    messages = [Message(role="user", parts=["hello"])]
    response_spec = ResponseSpec()
    content_hash = "a" * 64

    baseline = _managed_record(
        messages,
        [
            Attachment(
                path="input.txt",
                content_hash=content_hash,
                mime_type="text/plain",
                size_bytes=5,
            )
        ],
        response_spec,
    )
    metadata_changed = _managed_record(
        messages,
        [
            Attachment(
                path="renamed.bin",
                content_hash=content_hash,
                mime_type="application/octet-stream",
                size_bytes=999,
            )
        ],
        response_spec,
    )

    assert baseline["attachments"][0]["mime_type"] == "text/plain"
    assert baseline["attachments"][0]["size_bytes"] == 5
    assert baseline["resolution_digest"] == metadata_changed["resolution_digest"]


def test_inject_is_byte_stable_and_warns_for_numeric_dict_keys() -> None:
    """教训 bf06: 键序变化不能让同一材料进入不同的缓存族。"""
    first = {"b": "木", "a": {"z": 2, "y": 1}}
    second = {"a": {"y": 1, "z": 2}, "b": "木"}

    assert inject(first).encode("utf-8") == inject(second).encode("utf-8")
    assert inject("木") == "```\n木\n```\n"
    with pytest.warns(KigumiPromptWarning, match="有序数据必须用 list"):
        inject({"items": {"1": "a", "10": "b", "2": "c"}})


def test_render_template_is_strict_and_loads_utf8(tmp_path: Path) -> None:
    """教训 declarative_templates: 槽位缺失或漂移必须在渲染前失败。"""
    path = tmp_path / "template.md"
    path.write_text("你好，{{name}}：{{body}}", encoding="utf-8")
    template = load_template(path)

    assert render_template(template, {"name": "木组", "body": "稳定"}) == "你好，木组：稳定"
    with pytest.raises(TemplateSlotError, match="missing: body; extra: extra"):
        render_template(template, {"name": "木组", "extra": "x"})


def test_section_omits_empty_values_and_composes_by_concatenation() -> None:
    """教训 optional_section: 空材料不靠自然语言条件指令来跳过;
    教训 section_composition: 输出保证换行收尾,两个 section 直接拼接不得粘行。"""
    assert section("上下文", None) == ""
    assert section("上下文", "") == ""
    assert section("上下文", "材料") == "## 上下文\n\n材料\n"
    assert section("上下文", "材料\n") == "## 上下文\n\n材料\n"

    combined = section("甲", "一") + section("乙", "二")
    assert "一\n## 乙" in combined


def test_schema_format_section_tracks_field_descriptions() -> None:
    """教训 schema_single_source: 描述变更必须从校验模型自动进入 prompt。"""

    class Before(BaseModel):
        title: str = Field(description="旧描述")

    class After(BaseModel):
        title: str = Field(description="新描述")

    before = schema_format_section(Before, with_example=False)
    after = schema_format_section(After, with_example=False)

    assert "旧描述" in before
    assert "旧描述" not in after
    assert "新描述" in after
    assert "必填" in after


def test_clip_emits_visible_annotation_and_event() -> None:
    """教训 clip_visibility: 截断必须同时留下用户可见标注和 sidecar 事件。"""
    result = clip("第一行\n第二行\n第三行", 5, boundary="line")

    assert result.clipped is True
    assert result.text.startswith("第一行\n")
    assert "已截断" in result.text
    assert result.event == {"from": 11, "to": 4, "boundary": "line"}
    assert result.original_chars == 11
    assert result.kept_chars == 4


def test_clip_sentence_boundary_and_noop() -> None:
    """教训 clip_boundary: 句子截断保留完整句，未截断时不注入任何标记。"""
    clipped = clip("First. Second sentence.", 10, boundary="sentence")
    unchanged = clip("完整", 10, boundary="sentence")

    assert clipped.text.startswith("First.")
    assert clipped.event == {"from": 23, "to": 6, "boundary": "sentence"}
    assert unchanged.text == "完整"
    assert unchanged.event is None


def test_inject_escalates_fence_for_backtick_material() -> None:
    """教训 fence_collision: 材料自带代码围栏时,注入围栏必须比它更长。"""
    material = "前文\n```python\nprint(1)\n```\n后文"

    assert inject(material) == f"````\n{material}\n````\n"
    assert inject("木") == "```\n木\n```\n"


def test_clip_hard_cuts_when_no_boundary_within_limit() -> None:
    """教训 clip_no_boundary: 上限内找不到安全边界时硬切,不得把材料清空。"""
    result = clip("无换行也无句号的一整段材料", 5, boundary="line")

    assert result.clipped is True
    assert result.text.startswith("无换行也无")
    assert result.kept_chars == 5
    assert "已截断" in result.text


def test_render_items_supports_json_and_bullets() -> None:
    """教训 list_rendering: 列表材料的两种展示形式都必须保持确定性。"""
    assert render_items(["一", "二"], format="json") == inject(["一", "二"])
    assert render_items(["一\n续", "二"], format="bullets") == "- 一\n  续\n- 二"


def test_prompt_component_golden_snapshot() -> None:
    """教训 prompt_cache_family: 公共措辞与围栏是内容寻址缓存的组成部分。"""
    wording_names = [
        "TITLE_DELIMITER",
        "WORDING_CLIPPED",
        "WORDING_JSON_ONLY",
        "WORDING_REPAIR_ROUND",
        "WORDING_REPAIR_STUCK",
        "WORDING_REPAIR_PREAMBLE",
        "WORDING_REPAIR_ECHO",
    ]
    wording_snapshot = "\n".join(
        f"{name} = {getattr(prompt_module, name)!r}" for name in wording_names
    )
    material = {"b": "木", "a": 1}
    actual = f"{wording_snapshot}\n\n--- inject ---\n{inject(material, title='示例材料')}"

    expected = (GOLDENS / "prompt_components.txt").read_bytes()
    assert actual.encode("utf-8") == expected, GOLDEN_FAILURE


def test_schema_format_golden_snapshot() -> None:
    """教训 schema_snapshot: 模型字段顺序、递归示例和固定收尾需要字节级冻结。"""
    actual = schema_format_section(SnapshotModel)

    assert actual.encode("utf-8") == (GOLDENS / "schema_format.txt").read_bytes(), GOLDEN_FAILURE
