from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import BaseModel, Field

import kigumi.prompt as prompt_module
from kigumi.prompt import (
    KigumiPromptWarning,
    TemplateSlotError,
    clip,
    inject,
    load_template,
    render_items,
    render_template,
    schema_format_section,
    section,
)

GOLDEN_FAILURE = (
    "公共 prompt 成分变更 = 全项目缓存换族,确认有意变更后更新 golden 并在 CHANGELOG 标注缓存失效"
)
GOLDENS = Path(__file__).parent / "goldens"


class SnapshotLocation(BaseModel):
    city: str = Field(description="城市")


class SnapshotModel(BaseModel):
    title: str = Field(description="标题")
    enabled: bool = Field(description="是否启用")
    location: SnapshotLocation = Field(description="地点")
    tags: list[str] = Field(description="标签")


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
