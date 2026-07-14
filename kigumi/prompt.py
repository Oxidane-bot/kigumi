"""Deterministic prompt assembly primitives."""

from __future__ import annotations

import json
import re
import warnings
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import UnionType
from typing import Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel

from .artifacts import canonical_json

TITLE_DELIMITER = "## {title}\n\n"
WORDING_CLIPPED = "(已截断：原文 {original_chars} 字，保留 {kept_chars} 字)"
WORDING_JSON_ONLY = "只输出一个 JSON 对象；不要输出解释、前后缀或代码围栏。"
WORDING_REPAIR_ROUND = "这是第 {round} 轮修复。"
WORDING_REPAIR_STUCK = "检测到输出与上次完全相同；请逐项修正错误，不得原样重交。"
WORDING_REPAIR_PREAMBLE = "上次输出未通过校验。下面是错误："
WORDING_REPAIR_ECHO = "你上一轮的输出如下："

_SLOT_PATTERN = re.compile(r"{{([a-z_][a-z0-9_]*)}}")
_SENTENCE_BOUNDARY = re.compile(r"[。！？.!?]")


class KigumiPromptWarning(UserWarning):
    """Warning emitted when JSON object key order looks like ordered data."""


class TemplateSlotError(ValueError):
    """Raised when a declarative template's slots do not match supplied values."""


@dataclass(frozen=True)
class Clipped:
    """A clip result with the sidecar event needed to disclose truncation."""

    text: str
    clipped: bool
    original_chars: int
    kept_chars: int
    event: dict[str, int | str] | None


def inject(obj: Any, *, title: str | None = None) -> str:
    """Render text or JSON-serializable material in a deterministic fenced block."""
    _warn_numeric_dict_keys(obj)
    if isinstance(obj, str):
        body, lang = obj, ""
    else:
        body, lang = canonical_json(obj), "json"
    fence = _fence_for(body)
    fenced = f"{fence}{lang}\n{body}\n{fence}\n"
    return f"{TITLE_DELIMITER.format(title=title)}{fenced}" if title is not None else fenced


def load_template(path: Path) -> str:
    """Load an explicitly supplied UTF-8 template file."""
    return path.read_text(encoding="utf-8")


def slot_names(text: str) -> list[str]:
    """Return a template's ``{{slot}}`` names in first-appearance order, deduplicated."""
    return list(dict.fromkeys(_SLOT_PATTERN.findall(text)))


def render_template(text: str, slots: dict[str, str]) -> str:
    """Render a declarative ``{{slot}}`` template with an exact slot contract."""
    required = set(slot_names(text))
    supplied = set(slots)
    missing = sorted(required - supplied)
    extra = sorted(supplied - required)
    if missing or extra:
        parts: list[str] = []
        if missing:
            parts.append(f"missing: {', '.join(missing)}")
        if extra:
            parts.append(f"extra: {', '.join(extra)}")
        raise TemplateSlotError(f"Template slots mismatch: {'; '.join(parts)}")
    return _SLOT_PATTERN.sub(lambda match: slots[match.group(1)], text)


def section(title: str, value: str | None) -> str:
    """Render a titled section only when its body has content.

    Output always ends with a newline so sections compose by plain concatenation.
    """
    if not value:
        return ""
    rendered = f"{TITLE_DELIMITER.format(title=title)}{value}"
    return rendered if rendered.endswith("\n") else f"{rendered}\n"


def schema_format_section(model_cls: type[BaseModel], *, with_example: bool = True) -> str:
    """Describe a Pydantic model and optionally include a recursive JSON skeleton."""
    field_lines = ["字段："]
    for name, field in model_cls.model_fields.items():
        required = "必填" if field.is_required() else "可选"
        description = field.description or "无描述"
        field_lines.append(
            f"- `{name}`：`{_type_label(field.annotation)}`；{required}；{description}"
        )

    body = "\n".join(field_lines)
    if with_example:
        example = {
            name: _example_value(field.annotation) for name, field in model_cls.model_fields.items()
        }
        example_json = json.dumps(example, ensure_ascii=False, indent=2)
        body = f"{body}\n\n示例：\n```json\n{example_json}\n```\n"
    return section("输出格式", body + "\n" + WORDING_JSON_ONLY)


def clip(text: str, limit: int, *, boundary: Literal["line", "sentence"] = "line") -> Clipped:
    """Clip only at an explicit safe boundary and disclose every truncation."""
    if limit < 0:
        raise ValueError("limit must be non-negative")
    if boundary not in {"line", "sentence"}:
        raise ValueError("boundary must be 'line' or 'sentence'")
    original_chars = len(text)
    if original_chars <= limit:
        return Clipped(text, False, original_chars, original_chars, None)

    prefix = text[:limit]
    # 找不到安全边界时硬切到 limit:宁可切破一行,不可把材料整段清空;标注照常披露。
    if boundary == "line":
        last_newline = prefix.rfind("\n")
        kept = prefix[: last_newline + 1] if last_newline >= 0 else prefix
    else:
        matches = list(_SENTENCE_BOUNDARY.finditer(prefix))
        kept = prefix[: matches[-1].end()] if matches else prefix
    kept_chars = len(kept)
    annotation = WORDING_CLIPPED.format(
        original_chars=original_chars,
        kept_chars=kept_chars,
    )
    separator = "" if not kept or kept.endswith("\n") else "\n"
    event: dict[str, int | str] = {
        "from": original_chars,
        "to": kept_chars,
        "boundary": boundary,
    }
    return Clipped(f"{kept}{separator}{annotation}", True, original_chars, kept_chars, event)


def render_items(items: list[Any], *, format: Literal["json", "bullets"] = "json") -> str:
    """Render a list deterministically as JSON material or indented bullet points."""
    if format == "json":
        return inject(items)
    if format == "bullets":
        return "\n".join(_bullet_item(item) for item in items)
    raise ValueError("format must be 'json' or 'bullets'")


def _fence_for(body: str) -> str:
    # 围栏必须长于材料内最长的反引号连串,否则材料自带 ``` 时边界破裂。
    longest = max((len(run.group(0)) for run in re.finditer(r"`+", body)), default=0)
    return "`" * max(3, longest + 1)


def _warn_numeric_dict_keys(obj: Any) -> None:
    if isinstance(obj, dict):
        keys = list(obj)
        if keys and all(isinstance(key, str) and key.isdigit() for key in keys):
            warnings.warn(
                "有序数据必须用 list——sort_keys 按字典序会把 1,10,11,2 排乱",
                KigumiPromptWarning,
                stacklevel=3,
            )
        for value in obj.values():
            _warn_numeric_dict_keys(value)
    elif isinstance(obj, (list, tuple)):
        for value in obj:
            _warn_numeric_dict_keys(value)


def _bullet_item(item: Any) -> str:
    return "- " + str(item).replace("\n", "\n  ")


def _type_label(annotation: Any) -> str:
    origin = get_origin(annotation)
    if annotation is Any:
        return "Any"
    if annotation is type(None):
        return "None"
    if origin is list:
        arguments = get_args(annotation)
        return f"list[{_type_label(arguments[0]) if arguments else 'Any'}]"
    if origin is dict:
        arguments = get_args(annotation)
        key = _type_label(arguments[0]) if arguments else "Any"
        value = _type_label(arguments[1]) if len(arguments) > 1 else "Any"
        return f"dict[{key}, {value}]"
    if origin in {Union, UnionType}:
        return " | ".join(_type_label(argument) for argument in get_args(annotation))
    if origin is Literal:
        return " | ".join(repr(argument) for argument in get_args(annotation))
    if isinstance(annotation, type):
        return annotation.__name__
    return str(annotation).replace("typing.", "")


def _example_value(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if annotation is Any:
        return "<value>"
    if origin is list:
        arguments = get_args(annotation)
        return [_example_value(arguments[0] if arguments else Any)]
    if origin is dict:
        return {}
    if origin in {Union, UnionType}:
        non_none = [argument for argument in get_args(annotation) if argument is not type(None)]
        return _example_value(non_none[0] if non_none else type(None))
    if origin is Literal:
        arguments = get_args(annotation)
        return arguments[0] if arguments else "<literal>"
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return {
            name: _example_value(field.annotation)
            for name, field in annotation.model_fields.items()
        }
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return next(iter(annotation)).value
    if annotation is str:
        return "<string>"
    if annotation is int:
        return 0
    if annotation is float:
        return 0.0
    if annotation is bool:
        return False
    if annotation is type(None):
        return None
    return "<value>"
