"""DAG 声明与运行态的只读渲染，消费纯数据，不依赖调度器。"""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from typing import Any

from .artifacts import sha


def render_summary(description: Mapping[str, Any]) -> str:
    """渲染每节点一行的 Markdown 声明表，不读取运行状态。"""
    rows = [
        (
            "| 节点 | 子图 | cache | 说明 | 类型 | 依赖 | items_from | carry_from | prompts | "
            "files | params | 校验模型 | 检查点 |"
        ),
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    models = description["models"]
    for name, entry in _nodes(description).items():
        node_models = ", ".join(model["model"] for model in entry["validated_models"])
        rows.append(
            "| "
            + " | ".join(
                _markdown_cell(value)
                for value in (
                    name,
                    entry["subgraph"] or "-",
                    entry["cache"],
                    _short_text(entry["doc"], 60),
                    entry["kind"],
                    ", ".join(entry["deps"]),
                    _format_locator(entry["items_from"]),
                    _format_locator(entry["carry_from"]),
                    ", ".join(entry["prompts"]),
                    ", ".join(entry["files"]),
                    ", ".join(f"{key}={value}" for key, value in entry["params"].items()),
                    node_models,
                    ", ".join(entry["checkpoints"]),
                )
            )
            + " |"
        )
    if models:
        rows.append("\n### 校验模型")
        for model_name, fields in models.items():
            rows.extend(
                [
                    f"\n#### {model_name}",
                    "| 字段 | 类型 | 含义 |",
                    "| --- | --- | --- |",
                ]
            )
            rows.extend(
                "| "
                + " | ".join(
                    _markdown_cell(field[value] if field[value] is not None else "")
                    for value in ("name", "type", "description")
                )
                + " |"
                for field in fields
            )
    return "\n".join(rows)


def render_mermaid(description: Mapping[str, Any], runtime: Mapping[str, Any] | None = None) -> str:
    """渲染 Mermaid 图源；运行态已经由调用方从 sidecar 收集。"""
    lines = ["flowchart TD"]
    node_ids = {name: f"n_{sha(name)[:12]}" for name in _nodes(description)}
    for name, entry in _nodes(description).items():
        kind = entry["kind"]
        label = f"{name}<br/>[{kind}]"
        if (doc := entry["doc"]) is not None:
            label = f"{name}<br/>{_short_text(doc, 40)}<br/>[{kind}]"
        if entry["checkpoints"]:
            label += "<br/>[checkpoint]"
        if runtime is not None and entry["kind"] in {"map", "scan"}:
            hit_count, miss_count = _item_cache_counts(name, runtime["metadata"])
            label += f"<br/>{hit_count} hit / {miss_count} miss"
        lines.append(f'{node_ids[name]}["{_mermaid_label(label)}"]')

    for name, entry in _nodes(description).items():
        for dependency in entry["deps"]:
            lines.append(f"{node_ids[dependency]} --> {node_ids[name]}")
        if (locator := entry["items_from"]) is not None:
            lines.append(
                f'{node_ids[locator["node"]]} -. "items_from: '
                f'{_mermaid_label(locator["path"])}" .-> {node_ids[name]}'
            )
        if (locator := entry["carry_from"]) is not None:
            lines.append(
                f'{node_ids[locator["node"]]} -. "carry_from: '
                f'{_mermaid_label(locator["path"])}" .-> {node_ids[name]}'
            )

    lines.extend(
        [
            "classDef hit fill:#dcfce7,stroke:#16a34a,color:#14532d",
            "classDef miss fill:#fee2e2,stroke:#dc2626,color:#7f1d1d",
            "classDef skipped fill:#e5e7eb,stroke:#6b7280,color:#374151",
            "classDef checkpoint_pending fill:#fef3c7,stroke:#d97706,color:#78350f",
        ]
    )
    if runtime is not None:
        for name, entry in _nodes(description).items():
            if (state := _render_state(name, entry, runtime)) is not None:
                lines.append(f"class {node_ids[name]} {state}")
    return "\n".join(lines)


def render_pipeline(
    description: Mapping[str, Any], title: str, runtime: Mapping[str, Any] | None = None
) -> str:
    """渲染自包含 HTML 工位架；运行态已经由调用方从 sidecar 收集。"""
    waves = _pipeline_waves(description)
    rows: list[str] = []
    for wave_number, names in enumerate(waves):
        cards: list[str] = []
        for name in names:
            entry = description[name]
            status = _pipeline_status(name, entry, runtime) if runtime is not None else None
            node_doc = _pipeline_escape(_short_text(entry["doc"], 26))
            node_type = _pipeline_escape(_pipeline_type_label(entry))
            detail_lines = [
                f'<div class="node-doc">{node_doc}</div>',
                f'<div class="node-type">{node_type}</div>',
            ]
            if status is not None:
                detail_lines.append(f'<div class="node-status status-{status}">{status}</div>')
            if runtime is not None and entry["kind"] in {"map", "scan"}:
                hit_count, miss_count = _item_cache_counts(name, runtime["metadata"])
                detail_lines.append(
                    f'<div class="node-counts">{hit_count} hit / {miss_count} miss</div>'
                )
            cards.append(
                f'<div class="node" style="{_pipeline_node_style(entry)}">'
                f'<div class="node-name">{_pipeline_escape(name)}</div>'
                f"{''.join(detail_lines)}"
                "</div>"
            )
        rows.append(
            '<div class="wave-row">'
            f'<div class="wave-label">W{wave_number}<span class="parallel">'
            f"&times;{len(names)}</span></div>"
            f'<div class="wave-nodes">{"".join(cards)}</div>'
            "</div>"
        )
        if wave_number < len(waves) - 1:
            rows.append(
                '<div class="connector"><svg width="2" height="20" '
                'aria-hidden="true"><line x1="1" y1="0" x2="1" y2="20" '
                'stroke="#aaa" stroke-width="1.5"/></svg></div>'
            )

    escaped_title = _pipeline_escape(title)
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{escaped_title}</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  background: #fafafa; color: #222; font-family: "SF Mono", "JetBrains Mono", Menlo, monospace;
  padding: 32px;
}}
h1 {{ color: #444; font-size: 14px; font-weight: 600; margin-bottom: 8px; text-align: center; }}
.legend {{ color: #666; font-size: 11px; margin-bottom: 24px; text-align: center; }}
.legend span {{ border-radius: 3px; display: inline-block; margin: 0 4px; padding: 2px 8px; }}
.pipeline {{ align-items: center; display: flex; flex-direction: column; }}
.wave-row {{ align-items: center; display: flex; gap: 16px; padding: 6px 0; }}
.wave-label {{ color: #888; flex-shrink: 0; font-size: 11px; text-align: right; width: 52px; }}
.parallel {{ color: #aaa; display: block; font-size: 10px; }}
.wave-nodes {{ display: flex; flex-wrap: wrap; gap: 8px; justify-content: center; }}
.node {{
  background: #fff; border-radius: 5px; max-width: 180px; min-width: 120px;
  padding: 8px 14px; text-align: center;
}}
.node-name {{
  color: #222; font-size: 11px; font-weight: 600; overflow: hidden;
  text-overflow: ellipsis; white-space: nowrap;
}}
.node-doc {{
  -webkit-box-orient: vertical; -webkit-line-clamp: 2; color: #666; display: -webkit-box;
  font-size: 9.5px; line-height: 1.3; margin-top: 3px; overflow: hidden;
}}
.node-type {{ color: #999; font-size: 9px; font-style: italic; margin-top: 3px; }}
.node-status, .node-counts {{ font-size: 9px; margin-top: 3px; }}
.status-hit {{ color: #2e7d32; }}
.status-miss {{ color: #c62828; }}
.status-pending {{ color: #b8860b; }}
.status-skipped {{ color: #757575; }}
.node-counts {{ color: #777; }}
.connector {{ display: flex; height: 20px; justify-content: center; }}
</style>
</head>
<body>
<h1>{escaped_title}</h1>
<div class="legend">
  <span style="border: 1.5px solid #1565c0">node</span>
  <span style="border: 1.5px dashed #e65100">map</span>
  <span style="border: 1.5px dashed #7b1fa2">scan</span>
  <span style="border: 2.5px double #555">checkpoint</span>
</div>
<div class="pipeline">{"".join(rows)}</div>
</body>
</html>"""


def render_pipeline_text(
    description: Mapping[str, Any], title: str, runtime: Mapping[str, Any] | None = None
) -> str:
    """渲染可直接打印到终端的 Unicode 工位架。"""
    waves = _pipeline_waves(description)
    label_width = max(
        (_pipeline_text_width(f"W{number} x{len(names)}") for number, names in enumerate(waves)),
        default=0,
    )
    rendered_waves: list[tuple[str, list[str]]] = []
    rack_width = 0
    for wave_number, names in enumerate(waves):
        cards: list[tuple[str, list[str], int, int]] = []
        for name in names:
            entry = description[name]
            status = _pipeline_status(name, entry, runtime) if runtime is not None else None
            if runtime is not None:
                status = status or "pending"
            counts = (
                _item_cache_counts(name, runtime["metadata"])
                if runtime is not None and entry["kind"] in {"map", "scan"}
                else None
            )
            cards.append(_pipeline_text_card(name, entry, status, counts))

        content_height = max((height for _, _, height, _ in cards), default=0)
        card_lines = [
            _pipeline_text_box(type_label, contents, content_height, width)
            for type_label, contents, _, width in cards
        ]
        wave_lines = ["  ".join(parts) for parts in zip(*card_lines, strict=True)]
        rack_width = max(
            rack_width, max((_pipeline_text_width(line) for line in wave_lines), default=0)
        )
        rendered_waves.append((f"W{wave_number} x{len(names)}", wave_lines))

    total_width = max(_pipeline_text_width(title), label_width + 2 + rack_width)
    lines = [title, "─" * total_width]
    for wave_number, (label, wave_lines) in enumerate(rendered_waves):
        lines.extend(
            f"{_pipeline_text_pad(label, label_width, left=False)}  "
            f"{_pipeline_text_center(wave_line, rack_width)}"
            for wave_line in wave_lines
        )
        if wave_number < len(rendered_waves) - 1:
            lines.append(" " * (label_width + 2) + _pipeline_text_center("│", rack_width))
    lines.append("Legend: node=solid map/scan=dashed checkpoint=double")
    return "\n".join(lines)


def _nodes(description: Mapping[str, Any]) -> dict[str, Any]:
    """排除声明元数据，避免它们被当成节点渲染。"""
    return {
        name: entry for name, entry in description.items() if name not in {"models", "subgraphs"}
    }


def _format_locator(locator: Mapping[str, str] | None) -> str:
    """把结构化 locator 压成摘要表中的一格。"""
    return "" if locator is None else f"{locator['node']}.{locator['path']}"


def _short_text(value: str | None, limit: int) -> str:
    """限制人类说明长度，沿用声明摘要的省略号风格。"""
    if value is None:
        return ""
    return value if len(value) <= limit else f"{value[: limit - 3]}..."


def _markdown_cell(value: Any) -> str:
    """转义表格分隔符，保证任意声明仍是一行 Markdown。"""
    return str(value).replace("|", "\\|").replace("\n", "<br>")


def _mermaid_label(value: str) -> str:
    """转义 Mermaid 标签中的引号，防止声明文本破坏图语法。"""
    return value.replace("&", "&amp;").replace('"', "&quot;").replace("\n", "<br/>")


def _pipeline_waves(description: Mapping[str, Any]) -> list[list[str]]:
    """按最长依赖路径分波次，保留同一波次的注册顺序。"""
    nodes = _nodes(description)
    depths: dict[str, int] = {}
    visiting: set[str] = set()

    def get_depth(name: str) -> int:
        if name in depths:
            return depths[name]
        if name in visiting:
            raise ValueError("Cycle detected while rendering pipeline")
        entry = nodes.get(name)
        if entry is None:
            raise ValueError(f"Unknown dependency {name!r} while rendering pipeline")
        visiting.add(name)
        dependencies = entry["deps"]
        depths[name] = 0 if not dependencies else max(get_depth(dep) for dep in dependencies) + 1
        visiting.remove(name)
        return depths[name]

    waves: dict[int, list[str]] = {}
    for name in nodes:
        waves.setdefault(get_depth(name), []).append(name)
    return [waves[depth] for depth in sorted(waves)]


def _pipeline_escape(value: Any) -> str:
    """转义任意声明文本，避免节点内容破坏自包含 HTML。"""
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _pipeline_node_style(entry: Mapping[str, Any]) -> str:
    """根据静态节点类型返回工位架边框。"""
    if entry["checkpoints"]:
        return "border: 2.5px double #555"
    if entry["kind"] == "scan":
        return "border: 1.5px dashed #7b1fa2"
    if entry["kind"] == "map":
        return "border: 1.5px dashed #e65100"
    return "border: 1.5px solid #1565c0"


def _pipeline_type_label(entry: Mapping[str, Any]) -> str:
    """检查点优先于其底层 node 类型显示。"""
    return "checkpoint" if entry["checkpoints"] else str(entry["kind"])


def _pipeline_text_card(
    name: str,
    entry: Mapping[str, Any],
    status: str | None,
    counts: tuple[int, int] | None,
) -> tuple[str, list[str], int, int]:
    """准备一个文本工位框的内容，并计算同波次对齐所需高度。"""
    type_label = _pipeline_text_type_label(entry)
    contents = [
        _pipeline_text_clip(name, 20),
        _pipeline_text_clip(_short_text(entry["doc"], 18), 20),
    ]
    if status is not None:
        contents.append(status)
    if counts is not None:
        hit_count, miss_count = counts
        contents.append(f"{hit_count} hit / {miss_count} miss")
    width = min(
        22,
        max(12, max(_pipeline_text_width(line) for line in (*contents, type_label)) + 2),
    )
    contents = [_pipeline_text_clip(line, width - 2) for line in contents]
    return type_label, contents, len(contents), width


def _pipeline_text_box(
    type_label: str, contents: list[str], content_height: int, width: int
) -> list[str]:
    """以节点类型对应的 box-drawing 字符绘制一个已对齐的工位框。"""
    left, horizontal, right, vertical, bottom_left, bottom_right = _pipeline_text_border(type_label)
    padded_contents = [*contents, *("" for _ in range(content_height - len(contents)))]
    return [
        f"{left}{horizontal * (width - 2)}{right}",
        *(
            f"{vertical}{_pipeline_text_center(line, width - 2)}{vertical}"
            for line in padded_contents
        ),
        f"{bottom_left}{horizontal * (width - 2)}{bottom_right}",
        _pipeline_text_center(type_label, width),
    ]


def _pipeline_text_type_label(entry: Mapping[str, Any]) -> str:
    """返回文本工位框底部的紧凑类型标签。"""
    if entry["checkpoints"]:
        return "[ckpt]"
    return f"[{entry['kind']}]"


def _pipeline_text_border(type_label: str) -> tuple[str, str, str, str, str, str]:
    """按文本标签选择普通、虚线或双线框字符。"""
    if type_label == "[ckpt]":
        return "╔", "═", "╗", "║", "╚", "╝"
    if type_label in {"[map]", "[scan]"}:
        return "┌", "╌", "┐", "╎", "└", "┘"
    return "┌", "─", "┐", "│", "└", "┘"


def _pipeline_text_width(value: str) -> int:
    """返回字符串在常见 CJK 终端中的显示列宽。"""
    return sum(
        0
        if unicodedata.combining(character)
        else 2
        if unicodedata.east_asian_width(character) in {"F", "W"}
        else 1
        for character in value
    )


def _pipeline_text_clip(value: str, width: int) -> str:
    """按终端显示宽度截断；超长文本保留省略号。"""
    if _pipeline_text_width(value) <= width:
        return value
    clipped = ""
    for character in value:
        if _pipeline_text_width(clipped + character + "...") > width:
            break
        clipped += character
    return f"{clipped}..."


def _pipeline_text_pad(value: str, width: int, *, left: bool) -> str:
    """将文本填充到指定显示宽度。"""
    padding = max(0, width - _pipeline_text_width(value))
    return f"{' ' * padding}{value}" if left else f"{value}{' ' * padding}"


def _pipeline_text_center(value: str, width: int) -> str:
    """按终端显示列宽居中文本。"""
    padding = max(0, width - _pipeline_text_width(value))
    return f"{' ' * (padding // 2)}{value}{' ' * (padding - padding // 2)}"


def _pipeline_status(name: str, entry: Mapping[str, Any], runtime: Mapping[str, Any]) -> str | None:
    """将 Mermaid 运行状态映射为工位架的紧凑标签。"""
    state = _render_state(name, entry, runtime)
    return "pending" if state == "checkpoint_pending" else state


def _item_cache_counts(name: str, metadata: Mapping[str, Mapping[str, Any]]) -> tuple[int, int]:
    """仅从 item sidecar 汇总 map 或 scan 的 hit/miss，不重新计算键。"""
    prefix = f"{name}@"
    states = [
        entry.get("cache")
        for entry_name, entry in metadata.items()
        if entry_name.startswith(prefix) and entry.get("cache") in {"hit", "miss"}
    ]
    return states.count("hit"), states.count("miss")


def _render_state(name: str, entry: Mapping[str, Any], runtime: Mapping[str, Any]) -> str | None:
    """映射 sidecar 与已知检查点到 Mermaid class，缺失记录保持未着色。"""
    metadata = runtime["metadata"]
    if (metadata_entry := metadata.get(name)) is not None and metadata_entry.get("cache") in {
        "hit",
        "miss",
    }:
        return metadata_entry["cache"]
    if name in runtime["pending_nodes"]:
        return "checkpoint_pending"
    if name in runtime["skipped_nodes"]:
        return "skipped"
    return None
