from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from kigumi.calling import LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import Dag, UndeclaredInputError
from kigumi.testing import FakeTransport
from tests._dag_helpers import _make_dag


def test_context_no_longer_exposes_imperative_render(tmp_path: Path) -> None:
    """Prompt 输入只能通过注册期 PromptSpec 声明。"""
    prompts = tmp_path / "prompts"
    prompts.mkdir()
    (prompts / "hidden.md").write_text("{{value}}", encoding="utf-8")
    dag = _make_dag(tmp_path)

    @dag.node("bad")
    def bad(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": ctx.render("hidden", value="x")}

    with pytest.raises(AttributeError, match="render"):
        dag.run()


def test_context_reads_only_declared_files_with_relative_and_absolute_paths(tmp_path: Path) -> None:
    """教训 declared_read_boundary: 受控读取必须和缓存声明使用同一解析规则。"""
    text_path = tmp_path / "input.txt"
    bytes_path = tmp_path / "input.bin"
    text_path.write_text("受控文本", encoding="utf-8")
    bytes_path.write_bytes(b"\x00\x01")
    dag = _make_dag(tmp_path)

    @dag.node("reader", files=("input.txt", bytes_path))
    def reader(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {
            "text": ctx.read_text("input.txt"),
            "bytes": list(ctx.read_bytes(bytes_path)),
        }

    assert dag.run().artifacts == {"reader": {"text": "受控文本", "bytes": [0, 1]}}


def test_run_file_snapshot_binds_key_and_context_to_one_immutable_value(
    tmp_path: Path,
) -> None:
    data = tmp_path / "input.txt"
    data.write_text("stable", encoding="utf-8")

    def mutate_after_first(name: str, artifact: dict[str, Any], hit: bool) -> None:
        del artifact, hit
        if name == "first":
            data.write_text("changed", encoding="utf-8")

    dag = _make_dag(tmp_path, mutate_after_first)

    @dag.node("first", files=("input.txt",))
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs
        return {"value": ctx.read_text("input.txt")}

    @dag.node("second", deps=("first",), files=("input.txt",))
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs
        return {"value": ctx.read_text("input.txt")}

    result = dag.run(run_id="file-snapshot")
    run_root = tmp_path / "artifacts" / "runs" / result.run_id
    second_sidecar = json.loads((run_root / "second.json.meta.json").read_text(encoding="utf-8"))

    assert result.artifacts == {
        "first": {"value": "stable"},
        "second": {"value": "stable"},
    }
    assert second_sidecar["key_components"]["files:input.txt"] == sha256(b"stable").hexdigest()


def test_context_rejects_an_undeclared_file_with_a_declaration_hint(tmp_path: Path) -> None:
    """教训 undeclared_read: 缺失 files 声明必须在读取点显式失败，不能复用陈旧键。"""
    (tmp_path / "declared.txt").write_text("ok", encoding="utf-8")
    (tmp_path / "hidden.txt").write_text("no", encoding="utf-8")
    dag = _make_dag(tmp_path)

    @dag.node("reader", files=("declared.txt",))
    def reader(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": ctx.read_text("hidden.txt")}

    with pytest.raises(UndeclaredInputError, match="reader.*files= 或 files_fn"):
        dag.run()


def test_map_context_reads_only_its_own_files_fn_declaration(tmp_path: Path) -> None:
    """教训 item_file_isolation: map 项不能借用别项 files_fn 的缓存输入。"""
    (tmp_path / "one.txt").write_text("one", encoding="utf-8")
    (tmp_path / "two.txt").write_text("two", encoding="utf-8")
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "one", "file": "one.txt"}, {"id": "two", "file": "two.txt"}]}

    @dag.map(
        "read",
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
        files_fn=lambda item: (item["file"],),
    )
    def read(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": ctx.read_text(item["file"])}

    assert dag.run().artifacts["read"]["items"] == {
        "one": {"text": "one"},
        "two": {"text": "two"},
    }

    forbidden = _make_dag(tmp_path / "forbidden")
    (tmp_path / "forbidden").mkdir(exist_ok=True)
    (tmp_path / "forbidden" / "one.txt").write_text("one", encoding="utf-8")
    (tmp_path / "forbidden" / "two.txt").write_text("two", encoding="utf-8")

    @forbidden.node("source")
    def forbidden_source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "one", "file": "one.txt"}, {"id": "two", "file": "two.txt"}]}

    @forbidden.map(
        "read",
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
        files_fn=lambda item: (item["file"],),
    )
    def forbidden_read(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": ctx.read_text("one.txt")}

    with pytest.raises(RuntimeError, match="UndeclaredInputError.*files= 或 files_fn"):
        forbidden.run()


def test_context_call_validated_repairs_without_an_adapter(tmp_path: Path) -> None:
    """教训 structured_context_gate: 结构化调用是库的门，不该逼用户写适配皮。"""

    class Answer(BaseModel):
        value: str

    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    transport = FakeTransport(['{"missing": true}', '{"value": "fixed"}'])
    dag = Dag(config, LLMCaller(transport, tmp_path / "llm"))

    @dag.node("structured")
    def structured(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        answer = ctx.call_validated("return an answer", Answer, max_repairs=1)
        return answer.model_dump()

    assert dag.run().artifacts["structured"] == {"value": "fixed"}


def test_node_context_exposes_resolved_project_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """教训 context_root: 节点不应靠建图闭包捕获项目根。"""
    monkeypatch.chdir(tmp_path.parent)
    dag = _make_dag(tmp_path / "project")
    seen: list[Path] = []

    @dag.node("node")
    def node(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        seen.append(ctx.project_root)
        return {"root": str(ctx.project_root)}

    result = dag.run()

    assert seen == [(tmp_path / "project").resolve()]
    assert result.artifacts["node"]["root"] == str((tmp_path / "project").resolve())
