from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, Field

from tests._dag_helpers import _make_dag


class _DescribeReview(BaseModel):
    title: str
    score: int


class _DescribedReview(BaseModel):
    title: str = Field(description="标题|含义")
    score: float = Field(description="置信分数")
    tags: list[str] = Field(description="标签列表")
    notes: str


def test_describe_and_summary_expose_registered_declarations(tmp_path: Path) -> None:
    """教训 graph_contract: 图审阅必须在首跑前看见所有静态声明。"""
    dag = _make_dag(tmp_path)

    @dag.node("source", params={"large": "x" * 200})
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "a"}], "carry": {"seed": 1}}

    @dag.node("review", deps=("source",))
    def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        ctx.call_validated("review", _DescribeReview)
        ctx.checkpoint("editor", {"ready": True})
        checkpoint_name = "runtime-name"
        ctx.checkpoint(checkpoint_name, {"ready": True})
        return {}

    @dag.map(
        "fanout",
        items_from=("source", "items"),
        files_fn=lambda item: (f"{item['id']}.txt",),
    )
    def fanout(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return item

    @dag.scan(
        "chain",
        items_from=("source", "items"),
        carry_from=("source", "carry"),
        carry_fn=lambda artifact: artifact,
    )
    def chain(item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del carry, inputs, ctx
        return item

    description = dag.describe()

    assert description["source"]["kind"] == "node"
    assert description["source"]["params"]["large"].endswith("...")
    assert description["review"]["validated_models"] == [
        {"model": "_DescribeReview", "fields": {"title": "str", "score": "int"}}
    ]
    assert description["review"]["checkpoints"] == ["editor", "<动态>"]
    assert description["fanout"]["kind"] == "map"
    assert description["fanout"]["items_from"] == {"node": "source", "path": "items"}
    assert description["fanout"]["has_files_fn"] is True
    assert description["chain"]["kind"] == "scan"
    assert description["chain"]["carry_from"] == {"node": "source", "path": "carry"}
    assert description["chain"]["has_carry_fn"] is True
    assert "| review | - | auto |  | node | source |" in dag.render_summary()


def test_describe_adds_doc_key_for_documented_and_undocumented_nodes(tmp_path: Path) -> None:
    """教训 graph_docs: 声明摘要必须展示注册函数已有的人类说明。"""
    dag = _make_dag(tmp_path)

    @dag.node("documented")
    def documented(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        """读取输入首行。

        第二行不应进入渲染说明。
        """
        del inputs, ctx
        return {"items": [{"id": "a"}], "carry": {}}

    @dag.node("undocumented", deps=("documented",))
    def undocumented(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {}

    @dag.map("fanout", items_from=("documented", "items"))
    def fanout(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        """逐项扩展输入记录。"""
        del inputs, ctx
        return item

    @dag.scan("chain", items_from=("documented", "items"), carry_from=("documented", "carry"))
    def chain(item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        """按顺序累积记录。"""
        del item, inputs, ctx
        return carry

    @dag.foreach("scene-{i}", [{"id": "one"}, {"id": "two"}])
    def scene(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        """生成固定场景。"""
        del inputs, ctx
        return {}

    description = dag.describe()

    assert description["documented"]["doc"] == "读取输入首行。"
    assert description["undocumented"]["doc"] is None
    assert description["fanout"]["doc"] == "逐项扩展输入记录。"
    assert description["chain"]["doc"] == "按顺序累积记录。"
    assert description["scene-0"]["doc"] == "生成固定场景。"
    assert description["scene-1"]["doc"] == "生成固定场景。"


def test_describe_adds_models_key_with_field_descriptions(tmp_path: Path) -> None:
    """教训 graph_schema_docs: 全图模型字段含义必须随声明摘要可见。"""
    dag = _make_dag(tmp_path)

    @dag.node("review")
    def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        ctx.call_validated("review", _DescribedReview)
        return {}

    assert dag.describe()["models"] == {
        "_DescribedReview": [
            {"name": "title", "type": "str", "description": "标题|含义"},
            {"name": "score", "type": "float", "description": "置信分数"},
            {"name": "tags", "type": "list[str]", "description": "标签列表"},
            {"name": "notes", "type": "str", "description": None},
        ]
    }


def test_render_summary_adds_doc_column_after_node_column(tmp_path: Path) -> None:
    """教训 summary_docs: Markdown 表必须把节点说明放在节点名之后。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        """生成 A|B 输入。"""
        del inputs, ctx
        return {}

    rendered = dag.render_summary()

    assert (
        "| 节点 | 子图 | cache | 说明 | 类型 | 依赖 | items_from | carry_from | prompts | "
        "files | params | 校验模型 | 检查点 |"
    ) in rendered
    assert "| source | - | auto | 生成 A\\|B 输入。 | node |" in rendered


def test_render_summary_appends_validated_model_section(tmp_path: Path) -> None:
    """教训 summary_schema_docs: Markdown 摘要必须展示模型字段含义。"""
    dag = _make_dag(tmp_path)

    @dag.node("review")
    def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        ctx.call_validated("review", _DescribedReview)
        return {}

    rendered = dag.render_summary()

    assert "### 校验模型" in rendered
    assert "#### _DescribedReview" in rendered
    assert "| 字段 | 类型 | 含义 |" in rendered
    assert "| title | str | 标题\\|含义 |" in rendered
    assert "| notes | str |  |" in rendered


def test_render_mermaid_adds_escaped_doc_line_to_node_label(tmp_path: Path) -> None:
    """教训 mermaid_docs: Mermaid 标签必须展示并转义节点说明。"""
    dag = _make_dag(tmp_path)

    @dag.node("quote")
    def quote(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        """生成 "A&B" 标签。"""
        del inputs, ctx
        return {}

    rendered = dag.render_mermaid()

    assert "quote<br/>生成 &quot;A&amp;B&quot; 标签。<br/>[node]" in rendered


def test_render_mermaid_uses_run_sidecars_for_item_counts(tmp_path: Path) -> None:
    """教训 graph_runtime: 运行图状态必须来自 sidecar，不能为了渲染重算。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "a"}, {"id": "b"}], "carry": {"value": 0}}

    @dag.map("fanout", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def fanout(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return item

    @dag.scan(
        "chain",
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
        carry_from=("source", "carry"),
    )
    def chain(item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del item, inputs, ctx
        return carry

    result = dag.run(run_id="graph-run")
    rendered = dag.render_mermaid(result.run_id)

    assert "flowchart TD" in rendered
    assert "[map]" in rendered
    assert "[scan]" in rendered
    assert "items_from: items" in rendered
    assert "carry_from: carry" in rendered
    assert "0 hit / 2 miss" in rendered
    assert "classDef hit" in rendered
    assert "classDef miss" in rendered
    assert "classDef skipped" in rendered
    assert "classDef checkpoint_pending" in rendered
    with pytest.raises(ValueError, match="does not exist"):
        dag.render_mermaid("missing")


def test_render_mermaid_marks_pending_checkpoint_and_skipped_descendants(tmp_path: Path) -> None:
    """教训 graph_pending: 挂起与被跳过节点也必须在运行图中可见。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        del inputs, ctx
        return {"value": 1}

    @dag.node("review", deps=("source",))
    def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        return {"approval": ctx.checkpoint("editor", {"value": 1})}

    @dag.node("publish", deps=("review",))
    def publish(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        del inputs, ctx
        return {"published": 1}

    result = dag.run(run_id="pending-graph")
    rendered = dag.render_mermaid(result.run_id)

    assert result.pending_checkpoints == ["editor"]
    assert " checkpoint_pending" in rendered
    assert " skipped" in rendered


def test_render_pipeline_produces_valid_html(tmp_path: Path) -> None:
    """工位架视图应包含每个已注册节点与四类图例。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "a"}], "carry": {}}

    @dag.node("review", deps=("source",))
    def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        return {"approval": ctx.checkpoint("editor", {"ready": True})}

    @dag.map("fanout", items_from=("source", "items"))
    def fanout(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return item

    @dag.scan("chain", items_from=("source", "items"), carry_from=("source", "carry"))
    def chain(item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del item, inputs, ctx
        return carry

    rendered = dag.render_pipeline()

    assert "<html>" in rendered
    assert all(name in rendered for name in ("source", "review", "fanout", "chain"))
    assert all(kind in rendered for kind in ("node", "map", "scan", "checkpoint"))


def test_render_pipeline_shows_doc_in_node(tmp_path: Path) -> None:
    """节点首行 docstring 应直接出现在工位框内。"""
    dag = _make_dag(tmp_path)

    @dag.node("documented")
    def documented(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        """生成可审阅的输入。"""
        del inputs, ctx
        return {}

    assert "生成可审阅的输入。" in dag.render_pipeline()


def test_render_pipeline_wave_labels(tmp_path: Path) -> None:
    """最长路径分波次，同波次节点显示并行度。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {}

    @dag.node("left", deps=("source",))
    def left(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {}

    @dag.node("right", deps=("source",))
    def right(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {}

    @dag.node("finish", deps=("left", "right"))
    def finish(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {}

    rendered = dag.render_pipeline()

    assert "W0" in rendered and "&times;1" in rendered
    assert "W1" in rendered and "&times;2" in rendered
    assert "W2" in rendered


def test_render_pipeline_run_overlay(tmp_path: Path) -> None:
    """已落盘 sidecar 的命中与失配状态应显示在工位框内。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        del inputs, ctx
        return {"value": 1}

    @dag.node("leaf", deps=("source",))
    def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        del inputs, ctx
        return {"value": 2}

    dag.run(run_id="pipeline-prime")
    result = dag.run(run_id="pipeline-overlay", force=("leaf",))
    rendered = dag.render_pipeline(result.run_id)

    assert 'class="node-status status-hit">hit' in rendered
    assert 'class="node-status status-miss">miss' in rendered


def test_render_pipeline_text_contains_all_nodes(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        """准备输入。"""
        del inputs, ctx
        return {"items": [{"id": "a"}], "carry": {}}

    @dag.node("review", deps=("source",))
    def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        """审阅输入。"""
        del inputs
        return {"approval": ctx.checkpoint("editor", {"ready": True})}

    @dag.map("fanout", items_from=("source", "items"))
    def fanout(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        """逐项处理。"""
        del inputs, ctx
        return item

    @dag.scan("chain", items_from=("source", "items"), carry_from=("source", "carry"))
    def chain(item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        """顺序处理。"""
        del item, inputs, ctx
        return carry

    rendered = dag.render_pipeline_text()

    assert all(name in rendered for name in ("source", "review", "fanout", "chain"))


def test_render_pipeline_text_wave_labels(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {}

    @dag.node("left", deps=("source",))
    def left(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {}

    @dag.node("right", deps=("source",))
    def right(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {}

    @dag.node("finish", deps=("left", "right"))
    def finish(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {}

    rendered = dag.render_pipeline_text()

    assert "W0 x1" in rendered
    assert "W1 x2" in rendered
    assert "W2 x1" in rendered


def test_render_pipeline_text_box_styles(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": []}

    @dag.node("review", deps=("source",))
    def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        return {"approval": ctx.checkpoint("editor", {"ready": True})}

    @dag.map("fanout", items_from=("source", "items"))
    def fanout(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return item

    rendered = dag.render_pipeline_text()

    assert "╔" in rendered
    assert "╎" in rendered or "╌" in rendered


def test_render_pipeline_text_run_overlay(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        del inputs, ctx
        return {"value": 1}

    @dag.node("leaf", deps=("source",))
    def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        del inputs, ctx
        return {"value": 2}

    dag.run(run_id="pipeline-text-prime")
    result = dag.run(run_id="pipeline-text-overlay", force=("leaf",))
    rendered = dag.render_pipeline_text(result.run_id)

    assert "hit" in rendered
    assert "miss" in rendered
