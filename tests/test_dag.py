from __future__ import annotations

import importlib.util
import json
import threading
from collections.abc import Callable
from itertools import repeat
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, Field

import kigumi.dag as dag_module
import kigumi.prompt as prompt_module
import kigumi.repair as repair_module
from kigumi import __version__
from kigumi.artifacts import sha
from kigumi.calling import DryRunError, LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import CheckpointPending, Dag, UndeclaredInputError
from kigumi.testing import FakeTransport
from kigumi.transport import Response


class _DescribeReview(BaseModel):
    title: str
    score: int


class _DescribedReview(BaseModel):
    title: str = Field(description="标题|含义")
    score: float = Field(description="置信分数")
    tags: list[str] = Field(description="标签列表")
    notes: str


def _make_dag(
    tmp_path: Path,
    post_node: Callable[[str, dict[str, Any], bool], None] | None = None,
) -> Dag:
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    transport = FakeTransport(repeat(Response("model output", {"total_tokens": 1}, "stop")))
    return Dag(config, LLMCaller(transport, tmp_path / "llm"), post_node=post_node)


def _load_work(
    path: Path,
    docstring: str,
    value: int,
) -> Callable[[dict[str, Any], Any], dict[str, int]]:
    path.write_text(
        f'def work(inputs, ctx):\n    """{docstring}"""\n    return {{\'value\': {value}}}\n',
        encoding="utf-8",
    )
    spec = importlib.util.spec_from_file_location(f"dag_version_{path.stem}", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.work


def test_registration_duplicate_unknown_dependency_and_cycle_are_rejected(tmp_path: Path) -> None:
    """教训 topology_guard: 无效拓扑必须在执行模型调用前失败。"""
    dag = _make_dag(tmp_path)

    @dag.node("root")
    def root(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "root"}

    with pytest.raises(ValueError, match="already registered"):

        @dag.node("root")
        def duplicate(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"value": "duplicate"}

    unknown = _make_dag(tmp_path / "unknown")

    @unknown.node("broken", deps=("missing",))
    def broken(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "broken"}

    with pytest.raises(ValueError, match="Unknown dependency"):
        unknown.run()

    cyclic = _make_dag(tmp_path / "cycle")

    @cyclic.node("first", deps=("second",))
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "first"}

    @cyclic.node("second", deps=("first",))
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "second"}

    with pytest.raises(ValueError, match="Cycle"):
        cyclic.run()


def test_docstring_does_not_change_cache_but_code_does(tmp_path: Path) -> None:
    """教训 code_version: 注释文档不换缓存族，逻辑变更必须换。"""
    first = _load_work(tmp_path / "first.py", "first documentation", 1)
    second = _load_work(tmp_path / "second.py", "rewritten documentation", 1)
    changed = _load_work(tmp_path / "changed.py", "rewritten documentation", 2)
    events: list[tuple[str, bool]] = []

    first_dag = _make_dag(tmp_path, lambda name, artifact, hit: events.append((name, hit)))
    first_dag.node("work")(first)
    assert first_dag.run().artifacts["work"] == {"value": 1}

    second_dag = _make_dag(tmp_path, lambda name, artifact, hit: events.append((name, hit)))
    second_dag.node("work")(second)
    assert second_dag.run().cache_hits == ["work"]

    changed_dag = _make_dag(tmp_path, lambda name, artifact, hit: events.append((name, hit)))
    changed_dag.node("work")(changed)
    assert changed_dag.run().artifacts["work"] == {"value": 2}
    assert events == [("work", False), ("work", True), ("work", False)]


def test_kigumi_component_tracks_repair_bytes_and_uses_schema(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """repair.py 参与生成 prompt 字节，改它必须换 L3 的 kigumi 成分。"""
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    node = dag._nodes["work"]
    baseline = dag._key_components(node, {}, dag._libs_hash())["kigumi"]
    original_read_bytes = Path.read_bytes
    repair_path = Path(repair_module.__file__).resolve()

    def changed_read_bytes(path: Path) -> bytes:
        contents = original_read_bytes(path)
        if path.resolve() == repair_path:
            return contents + b"\n# cache-key probe\n"
        return contents

    monkeypatch.setattr(Path, "read_bytes", changed_read_bytes)
    changed = dag._key_components(node, {}, dag._libs_hash())["kigumi"]

    assert changed != baseline
    inputs = dag_module._kigumi_key_inputs()
    assert inputs["schema"] == dag_module.CACHE_SCHEMA
    assert inputs["schema"] == 3
    assert "version" not in inputs
    assert __version__ not in inputs.values()


def test_key_components_lock_exact_label_set(tmp_path: Path) -> None:
    """最小普通节点的标签集合变化必须显式更新缓存键契约。"""
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    components = dag._key_components(dag._nodes["work"], {}, dag._libs_hash())

    assert set(components) == {"source", "libs", "params", "kigumi"}


def test_kigumi_component_tracks_prompt_bytes_and_pydantic_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """prompt.py 字节和 Pydantic 版本都必须换 L3 的 kigumi 成分。"""
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    node = dag._nodes["work"]
    baseline = dag._key_components(node, {}, dag._libs_hash())["kigumi"]
    original_read_bytes = Path.read_bytes
    prompt_path = Path(prompt_module.__file__).resolve()

    def changed_read_bytes(path: Path) -> bytes:
        contents = original_read_bytes(path)
        if path.resolve() == prompt_path:
            return contents + b"\n# cache-key probe\n"
        return contents

    monkeypatch.setattr(Path, "read_bytes", changed_read_bytes)
    prompt_changed = dag._key_components(node, {}, dag._libs_hash())["kigumi"]
    monkeypatch.setattr(dag_module.pydantic, "__version__", "cache-key-probe")
    pydantic_changed = dag._key_components(node, {}, dag._libs_hash())["kigumi"]

    assert prompt_changed != baseline
    assert pydantic_changed != prompt_changed


def test_explain_records_key_components_and_reports_one_changed_input(tmp_path: Path) -> None:
    """教训 opaque_key: 重算理由必须能从同源成分摘要直接看见。"""

    def build(value: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("work", params={"value": value})
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": ctx.params["value"]}

        return dag

    first = build(1)
    run = first.run()
    assert first.explain("work", run.run_id).status == "hit"

    changed = build(2).explain("work", run.run_id)
    assert changed.status == "miss"
    assert changed.changed == ["params"]
    assert set(changed.details["params"]) == {"old", "new"}


def test_explain_map_item_uses_its_own_sidecar(tmp_path: Path) -> None:
    """教训 map_explain: 单项缓存键不能被聚合节点摘要掩盖。"""

    def build(tone: str) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
            return {"items": [{"id": "a"}, {"id": "b"}]}

        @dag.map(
            "work",
            items_from=("source", "items"),
            key_fn=lambda item: item["id"],
            params={"tone": tone},
        )
        def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"id": item["id"], "tone": ctx.params["tone"]}

        return dag

    first = build("brief")
    run = first.run()
    assert first.explain("work@a", run.run_id).status == "hit"

    changed = build("full").explain("work@a", run.run_id)
    assert changed.status == "miss"
    assert changed.changed == ["params"]


def test_explain_reports_unknown_no_entry_and_legacy_without_guessing(tmp_path: Path) -> None:
    """教训 explain_honesty: 无法取得上游或旧记录时不能伪造变化原因。"""

    def build(value: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source", params={"value": value})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": ctx.params["value"]}

        @dag.node("leaf", deps=("source",))
        def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return dict(inputs["source"])

        return dag

    first = build(1)
    assert first.explain("source").status == "no_entry"
    run = first.run()

    unknown = build(2).explain("leaf", run.run_id)
    assert unknown.status == "unknown"
    assert unknown.pending_on == ("source",)

    sidecar = tmp_path / "artifacts" / "runs" / run.run_id / "source.json.meta.json"
    metadata = json.loads(sidecar.read_text(encoding="utf-8"))
    metadata.pop("key_components")
    sidecar.write_text(json.dumps(metadata), encoding="utf-8")
    assert first.explain("source", run.run_id).status == "legacy"


def test_prompt_upstream_and_params_changes_invalidate_caches(tmp_path: Path) -> None:
    """教训 cache_inputs: 声明输入任一变化都必须级联换节点键。"""
    prompts = tmp_path / "prompts"
    prompts.mkdir()
    prompt = prompts / "draft.md"
    prompt.write_text("version one", encoding="utf-8")
    events: list[tuple[str, bool]] = []

    def run_with(value: int) -> tuple[dict[str, Any], list[str]]:
        dag = _make_dag(tmp_path, lambda name, artifact, hit: events.append((name, hit)))

        @dag.node("source", params={"value": value})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": ctx.params["value"]}

        @dag.node("leaf", deps=("source",), prompts=("draft",))
        def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": inputs["source"]["value"]}

        result = dag.run()
        return result.artifacts, result.cache_hits

    first, first_hits = run_with(1)
    prompt.write_text("version two", encoding="utf-8")
    second, second_hits = run_with(1)
    third, third_hits = run_with(2)

    assert first == {"source": {"value": 1}, "leaf": {"value": 1}}
    assert first_hits == []
    assert second_hits == ["source"]
    assert second == first
    assert third == {"source": {"value": 2}, "leaf": {"value": 2}}
    assert third_hits == []
    assert events.count(("leaf", False)) == 3


def test_map_hashes_shared_upstream_once_per_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """教训 upstream_sha_once: map 的共享上游摘要必须跨 item 复用。"""
    dag = _make_dag(tmp_path)
    shared_artifact = {"payload": "shared input"}
    original_sha = sha
    shared_sha_calls = 0

    def counting_sha(value: Any) -> str:
        nonlocal shared_sha_calls
        if value == shared_artifact:
            shared_sha_calls += 1
        return original_sha(value)

    monkeypatch.setattr("kigumi.dag.sha", counting_sha)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "one"}, {"id": "two"}, {"id": "three"}]}

    @dag.node("shared")
    def shared(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return shared_artifact

    @dag.map("work", items_from=("scan", "items"), deps=("shared",))
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"id": item["id"], "payload": inputs["shared"]["payload"]}

    dag.run()

    assert shared_sha_calls == 1


def test_declared_files_and_library_sources_invalidate_caches(tmp_path: Path) -> None:
    """教训 declared_inputs: File 与 helper 源码都属于节点内容寻址输入。"""
    source_dir = tmp_path / "lib"
    source_dir.mkdir()
    helper = source_dir / "helper.py"
    helper.write_text("VALUE = 1\n", encoding="utf-8")
    source_file = tmp_path / "source.txt"
    source_file.write_text("first", encoding="utf-8")
    config = KigumiConfig(project_root=tmp_path, source_dirs=["lib"])
    events: list[bool] = []

    def run_once() -> list[str]:
        dag = Dag(
            config,
            LLMCaller(FakeTransport(), tmp_path / "llm"),
            post_node=lambda name, artifact, hit: events.append(hit),
        )

        @dag.node("work", files=("source.txt",))
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"value": "stable"}

        return dag.run().cache_hits

    assert run_once() == []
    source_file.write_text("second", encoding="utf-8")
    assert run_once() == []
    helper.write_text("VALUE = 2\n", encoding="utf-8")
    assert run_once() == []
    assert events == [False, False, False]


def test_foreach_binds_each_item_and_supports_chained_dependencies(tmp_path: Path) -> None:
    """教训 late_binding: fan-out 的闭包、参数和依赖都必须逐项固定。"""
    dag = _make_dag(tmp_path)
    items = [
        {"value": "first", "deps": ()},
        {"value": "second", "deps": ("scene-0",)},
    ]

    @dag.foreach(
        "scene-{i}",
        items,
        deps=lambda item: item["deps"],
        params_fn=lambda item: {"value": item["value"]},
    )
    def scene(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"value": ctx.params["value"], "upstream": dict(inputs)}

    items[0]["value"] = "mutated"
    items[1]["value"] = "mutated"
    result = dag.run()

    assert result.artifacts["scene-0"]["value"] == "first"
    assert result.artifacts["scene-1"]["value"] == "second"
    assert result.artifacts["scene-1"]["upstream"] == {
        "scene-0": {"value": "first", "upstream": {}}
    }


def test_foreach_requires_keyword_deps_and_merges_params(tmp_path: Path) -> None:
    """教训 foreach_signature: 依赖必须具名，逐项参数才能安全覆盖共享基底。"""
    dag = _make_dag(tmp_path)

    with pytest.raises(TypeError):
        dag.foreach("scene-{i}", [0], ("root",))

    @dag.foreach(
        "scene-{i}",
        [{"name": "first"}],
        params={"shared": "base", "override": "base"},
        params_fn=lambda item: {"name": item["name"], "override": "item"},
    )
    def scene(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return dict(ctx.params)

    assert dag.run().artifacts["scene-0"] == {
        "shared": "base",
        "override": "item",
        "name": "first",
    }


def test_cache_hit_materializes_files_and_runs_post_node(tmp_path: Path) -> None:
    """教训 materialize_cache_hit: 缓存不能跳过下游依赖的磁盘物化。"""
    events: list[tuple[str, bool]] = []
    dag = _make_dag(tmp_path, lambda name, artifact, hit: events.append((name, hit)))

    @dag.node("build")
    def build(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"value": "ready", "files": {"generated/result.txt": "materialized"}}

    assert dag.run().cache_hits == []
    materialized = tmp_path / "generated" / "result.txt"
    materialized.unlink()

    assert dag.run().cache_hits == ["build"]
    assert materialized.read_text(encoding="utf-8") == "materialized"
    assert events == [("build", False), ("build", True)]


def test_run_id_must_be_a_safe_single_path_component(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "safe"}

    for run_id in ("", "../escape", "nested/run", "nested\\run", ".", ".."):
        with pytest.raises(ValueError, match="Run ID.*single non-empty relative path component"):
            dag.run(run_id=run_id)

    assert not (tmp_path / "escape").exists()


def test_checkpoint_names_must_be_safe_single_path_components(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("review")
    def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"approval": ctx.checkpoint("../escape", {"ready": True})}

    with pytest.raises(
        ValueError,
        match="Checkpoint name.*single non-empty relative path component",
    ):
        dag.run(run_id="safe-run")

    assert not (tmp_path / "artifacts" / "runs" / "safe-run" / "escape.pending.json").exists()


def test_checkpoint_pending_approval_and_resume(tmp_path: Path) -> None:
    """教训 interrupt_resume: 待审批分支停止，批准后以同一 run 续过。"""
    dag = _make_dag(tmp_path)

    @dag.node("independent")
    def independent(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        return {"completed": True}

    @dag.node("review")
    def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"approval": ctx.checkpoint("editor", {"question": "approve?"})}

    @dag.node("publish", deps=("review",))
    def publish(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"published": True}

    pending = dag.run(run_id="review-run")
    pending_path = (
        tmp_path / "artifacts" / "runs" / "review-run" / "approvals" / "editor.pending.json"
    )
    assert pending.artifacts == {"independent": {"completed": True}}
    assert pending.pending_checkpoints == ["editor"]
    # 教训 visible_skip: 挂起的下游不执行可以,静默消失不可以。
    assert pending.skipped == ["publish"]
    assert pending_path.exists()

    dag.approve("review-run", "editor", {"accepted": True})
    resumed = dag.run(run_id="review-run")

    assert resumed.pending_checkpoints == []
    assert resumed.skipped == []
    assert resumed.artifacts == {
        "independent": {"completed": True},
        "review": {"approval": {"accepted": True}},
        "publish": {"published": True},
    }

    fresh = dag.run(run_id="fresh-review")
    assert fresh.pending_checkpoints == ["editor"]
    assert "review" not in fresh.cache_hits
    assert fresh.skipped == ["publish"]


def test_diff_and_gc_keep_recent_run_cache_entries(tmp_path: Path) -> None:
    """教训 run_history: 差异与缓存回收必须基于可读 runs 溯源。"""
    for value in range(3):
        dag = _make_dag(tmp_path)

        @dag.node("result", params={"value": value})
        def result(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": ctx.params["value"]}

        dag.run()

    inspector = _make_dag(tmp_path)
    assert inspector.diff("run-0001", "run-0002") == {
        "changed": ["result"],
        "only_a": [],
        "only_b": [],
    }
    assert inspector.gc(keep_last=1) == 2
    assert len(list((tmp_path / "artifacts" / "_cache" / "nodes").glob("*.json"))) == 1


def test_context_render_requires_declared_template(tmp_path: Path) -> None:
    """教训 prompt_declaration: 未声明模板不能绕过节点缓存键。"""
    prompts = tmp_path / "prompts"
    prompts.mkdir()
    (prompts / "hidden.md").write_text("{{value}}", encoding="utf-8")
    dag = _make_dag(tmp_path)

    @dag.node("bad")
    def bad(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": ctx.render("hidden", value="x")}

    with pytest.raises(ValueError, match="not declared"):
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


def test_registration_rejects_raw_io_and_allows_a_reasoned_waiver(tmp_path: Path) -> None:
    """教训 raw_io_guard: 注册期必须阻止节点体直接打开未声明输入。"""
    dag = _make_dag(tmp_path)

    with pytest.raises(ValueError, match="ctx.read_text or ctx.read_bytes"):

        @dag.node("unsafe")
        def unsafe(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            with open(tmp_path / "input.txt", encoding="utf-8") as handle:
                return {"text": handle.read()}

    @dag.node("waived")
    def waived(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": (tmp_path / "fixture.txt").read_text()}  # kigumi: raw-io-ok fixture


def test_registration_rejects_raw_io_waiver_without_a_reason(tmp_path: Path) -> None:
    """教训 raw_io_waiver_reason: 空理由不能把 raw I/O 守卫变成静默后门。"""
    dag = _make_dag(tmp_path)

    with pytest.raises(ValueError, match="ctx.read_text or ctx.read_bytes"):

        @dag.node("unsafe")
        def unsafe(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"text": (tmp_path / "input.txt").read_text()}  # kigumi: raw-io-ok


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


def test_run_sidecar_contains_cache_and_new_caller_provenance(tmp_path: Path) -> None:
    """教训 provenance_slice: 节点 sidecar 必须保留本节点新增调用溯源。"""
    dag = _make_dag(tmp_path)

    @dag.node("ask")
    def ask(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"answer": ctx.llm("say something")}

    result = dag.run(run_id="provenance")
    sidecar = tmp_path / "artifacts" / "runs" / "provenance" / "ask.json.meta.json"

    assert result.artifacts["ask"] == {"answer": "model output"}
    metadata = json.loads(sidecar.read_text(encoding="utf-8"))
    assert metadata["cache"] == "miss"
    assert metadata["cache_key"]
    assert len(metadata["calls"]) == 1
    assert metadata["calls"][0]["cache"] == "miss"


def test_checkpoint_exception_exposes_name_and_payload() -> None:
    """教训 checkpoint_contract: runner 只能通过结构化 pending 信息落盘。"""
    pending = CheckpointPending("editor", {"question": "approve?"})

    assert pending.name == "editor"
    assert pending.payload == {"question": "approve?"}


def test_miss_and_hit_paths_feed_downstream_identical_shape(tmp_path: Path) -> None:
    """教训 hit_miss_parity: miss 路径喂下游的键序必须与命中路径读盘一致(bf06)。"""

    def run_leaf(marker: int) -> str:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"zeta": "z", "alpha": "a"}

        @dag.node("leaf", deps=("source",), params={"marker": marker})
        def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"prompt_text": json.dumps(inputs["source"], ensure_ascii=False)}

        return dag.run().artifacts["leaf"]["prompt_text"]

    first = run_leaf(1)
    second = run_leaf(2)

    assert first == second


def test_approval_binds_to_payload_content(tmp_path: Path) -> None:
    """教训 checkpoint_binding: 审批绑定 payload 内容哈希,内容变更必须重批。"""

    def make(value: str) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("review", params={"value": value})
        def review(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"approval": ctx.checkpoint("editor", {"content": ctx.params["value"]})}

        return dag

    first = make("v1")
    with pytest.raises(ValueError, match="No pending checkpoint"):
        first.approve("bind-run", "editor", {"ok": True})

    assert first.run(run_id="bind-run").pending_checkpoints == ["editor"]
    first.approve("bind-run", "editor", {"ok": True})
    assert first.run(run_id="bind-run").artifacts["review"] == {"approval": {"ok": True}}

    changed = make("v2")
    assert changed.run(run_id="bind-run").pending_checkpoints == ["editor"]
    assert (
        tmp_path / "artifacts" / "runs" / "bind-run" / "approvals" / "editor.pending.json"
    ).exists()
    changed.approve("bind-run", "editor", {"ok": "second"})
    resumed = changed.run(run_id="bind-run")
    assert resumed.artifacts["review"] == {"approval": {"ok": "second"}}


def test_torn_node_cache_is_recomputed(tmp_path: Path) -> None:
    """教训 torn_cache: 半截缓存文件按 miss 重算重写,不崩 run。"""

    def run_once() -> Any:
        dag = _make_dag(tmp_path)

        @dag.node("work")
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"value": "stable"}

        return dag.run()

    assert run_once().cache_hits == []
    cache_files = list((tmp_path / "artifacts" / "_cache" / "nodes").glob("*.json"))
    assert len(cache_files) == 1
    cache_files[0].write_text('{"artifact": {"val', encoding="utf-8")

    torn = run_once()
    assert torn.cache_hits == []
    assert torn.artifacts["work"] == {"value": "stable"}
    assert run_once().cache_hits == ["work"]


def test_foreach_fixes_generator_declarations_for_every_item(tmp_path: Path) -> None:
    """教训 generator_exhaustion: 生成器声明只固定一次,第二项不允许静默变空。"""
    shared = tmp_path / "shared.txt"
    shared.write_text("first", encoding="utf-8")

    def run_once() -> list[str]:
        dag = _make_dag(tmp_path)

        @dag.foreach(
            "scene-{i}",
            [{"n": 0}, {"n": 1}],
            files=(name for name in ("shared.txt",)),
            params_fn=lambda item: {"n": item["n"]},
        )
        def scene(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"value": "x"}

        return dag.run().cache_hits

    assert run_once() == []
    shared.write_text("second", encoding="utf-8")
    # 两个节点都必须失效;若 scene-1 命中,说明它的 files 声明被生成器吃掉了。
    assert run_once() == []


def test_node_registration_blocks_loop_calls_and_allows_reasoned_waivers(tmp_path: Path) -> None:
    """教训 fake_registry: 注册环必须拒绝循环裸调与推导式绕行。"""
    dag = _make_dag(tmp_path)

    with pytest.raises(ValueError, match="ctx.llm"):

        @dag.node("loop")
        def loop(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            for prompt in ["one"]:
                ctx.llm(prompt)
            return {"value": "never"}

    with pytest.raises(ValueError, match="ctx.llm"):

        @dag.node("comprehension")
        def comprehension(inputs: dict[str, Any], ctx: Any) -> dict[str, list[str]]:
            return {"values": [ctx.llm(prompt) for prompt in ["one"]]}

    @dag.node("waived")
    def waived(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        for prompt in ["one"]:
            ctx.llm(prompt)  # kigumi: raw-llm-ok bounded fixture replay
        return {"value": "registered"}

    assert waived.__name__ == "waived"


def test_force_recomputes_a_cache_hit_and_replaces_cached_artifact(tmp_path: Path) -> None:
    """教训 force_rerun: 指定节点必须越过 L3 缓存并覆盖同一内容键。"""

    class FakeCaller:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def call(self, prompt: str, model: str = "default", **params: Any) -> str:
            self.calls.append({"prompt": prompt, "model": model})
            return str(len(self.calls))

    caller = FakeCaller()
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    dag = Dag(config, caller)  # type: ignore[arg-type]

    @dag.node("ask")
    def ask(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"answer": ctx.llm("same prompt")}

    assert dag.run().artifacts["ask"] == {"answer": "1"}
    assert dag.run().cache_hits == ["ask"]
    assert dag.run(force=("ask",)).artifacts["ask"] == {"answer": "2"}
    assert dag.run().artifacts["ask"] == {"answer": "2"}
    assert len(caller.calls) == 2


def test_changed_same_run_artifact_is_archived_once(tmp_path: Path) -> None:
    """教训 evidence_archive: 覆盖同 run 产物前必须保留旧数据与 sidecar。"""

    def make(value: str) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("work", params={"value": value})
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"value": ctx.params["value"]}

        return dag

    assert make("first").run(run_id="evidence").artifacts["work"] == {"value": "first"}
    assert make("second").run(run_id="evidence").artifacts["work"] == {"value": "second"}
    history = tmp_path / "artifacts" / "runs" / "evidence" / "history" / "0001"
    assert (history / "work.json").exists()
    assert (history / "work.json.meta.json").exists()

    assert make("second").run(run_id="evidence").cache_hits == ["work"]
    assert [path.name for path in (history.parent).iterdir() if path.is_dir()] == ["0001"]


def test_force_rejects_unknown_node_names(tmp_path: Path) -> None:
    """教训 force_typo: force 名字打错必须报错,静默全量命中看起来像成功。"""
    dag = _make_dag(tmp_path)

    @dag.node("ask")
    def ask(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "x"}

    with pytest.raises(ValueError, match="Unknown forced nodes: aks"):
        dag.run(force=("aks",))


def test_foreach_validates_registration_once(tmp_path: Path, monkeypatch: Any) -> None:
    """教训 foreach_validation: 同一函数对象 fan-out N 项只做一次 AST 校验。"""
    import kigumi.dag as dag_module

    counter = {"count": 0}
    original = dag_module._validate_registration

    def counting(function: Any) -> None:
        counter["count"] += 1
        original(function)

    monkeypatch.setattr(dag_module, "_validate_registration", counting)
    dag = _make_dag(tmp_path)

    @dag.foreach("scene-{i}", [{"n": 0}, {"n": 1}, {"n": 2}])
    def scene(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "x"}

    assert counter["count"] == 1
    assert dag.run().artifacts.keys() == {"scene-0", "scene-1", "scene-2"}


def test_run_allocations_reserve_directories_atomically(tmp_path: Path) -> None:
    """教训 run_id_race: 分配号即建目录占号,两次运行不得撞同一 run 目录。"""
    dag = _make_dag(tmp_path)
    runs_root = tmp_path / "artifacts" / "runs"
    (runs_root / "run-0001").mkdir(parents=True)

    first = dag.run().run_id
    second = dag.run().run_id

    assert first == "run-0002"
    assert second == "run-0003"
    assert (runs_root / first).is_dir()
    assert (runs_root / second).is_dir()


def test_parallel_ready_nodes_overlap_when_workers_allow_it(tmp_path: Path) -> None:
    """教训 parallel_overlap: 就绪兄弟若仍串行，会把互等协作误判为超时。"""
    dag = _make_dag(tmp_path)
    first_ready = threading.Event()
    second_ready = threading.Event()

    @dag.node("first")
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        first_ready.set()
        assert second_ready.wait(5), "second node did not overlap execution"
        return {"done": True}

    @dag.node("second")
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        second_ready.set()
        assert first_ready.wait(5), "first node did not overlap execution"
        return {"done": True}

    assert dag.run(workers=2).artifacts == {
        "first": {"done": True},
        "second": {"done": True},
    }


def test_parallel_node_calls_are_observed_by_their_own_sidecars(tmp_path: Path) -> None:
    """教训 call_observer: 并行节点不能用全局调用顺序切片归属溯源。"""
    dag = _make_dag(tmp_path)

    @dag.node("alpha")
    def alpha(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"answer": ctx.call("alpha prompt")}

    @dag.node("beta")
    def beta(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        first = ctx.call("beta first")
        second = ctx.call("beta second")
        return {"answer": first + second}

    dag.run(run_id="parallel-provenance", workers=2)
    run_root = tmp_path / "artifacts" / "runs" / "parallel-provenance"
    alpha_calls = json.loads((run_root / "alpha.json.meta.json").read_text(encoding="utf-8"))[
        "calls"
    ]
    beta_calls = json.loads((run_root / "beta.json.meta.json").read_text(encoding="utf-8"))["calls"]

    assert [call["prompt_sha"] for call in alpha_calls] == [
        sha([{"role": "user", "content": "alpha prompt"}])
    ]
    assert [call["prompt_sha"] for call in beta_calls] == [
        sha([{"role": "user", "content": "beta first"}]),
        sha([{"role": "user", "content": "beta second"}]),
    ]


def test_pending_branch_does_not_block_independent_parallel_branch(tmp_path: Path) -> None:
    """教训 pending_branch: 检查点只阻断下游，不该饿死已就绪旁支。"""
    dag = _make_dag(tmp_path)

    @dag.node("a")
    def pending(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"approval": ctx.checkpoint("review", {"need": "human"})}

    @dag.node("b", deps=("a",))
    def blocked(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        return {"ran": True}

    @dag.node("c")
    def independent(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        return {"ran": True}

    result = dag.run(workers=2)
    assert result.artifacts == {"c": {"ran": True}}
    assert result.pending_checkpoints == ["review"]
    assert result.skipped == ["b"]


def test_parallel_failures_raise_the_first_topological_error(tmp_path: Path) -> None:
    """教训 deterministic_failure: 并行完成顺序不能决定对外暴露的失败。"""
    dag = _make_dag(tmp_path)
    barrier = threading.Barrier(2)

    @dag.node("first")
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        barrier.wait(5)
        raise ValueError("first failure")

    @dag.node("second")
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        barrier.wait(5)
        raise RuntimeError("second failure")

    with pytest.raises(ValueError, match="first failure"):
        dag.run(workers=2)


def test_workers_must_be_positive(tmp_path: Path) -> None:
    """教训 workers_guard: 无效线程数必须在调度前明确失败。"""
    dag = _make_dag(tmp_path)
    with pytest.raises(ValueError, match="workers"):
        dag.run(workers=0)


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


def test_foreach_files_fn_invalidates_only_the_changed_item(tmp_path: Path) -> None:
    """教训 item_file_cache: 逐项文件依赖不进缓存键，改 clip 就不会失效。"""
    first_file = tmp_path / "first.mp4"
    second_file = tmp_path / "second.mp4"
    first_file.write_bytes(b"first")
    second_file.write_bytes(b"second")
    executed: list[str] = []

    def run_once() -> Any:
        dag = _make_dag(tmp_path)

        @dag.foreach(
            "analyze_{name}",
            [{"name": "first", "clip": first_file}, {"name": "second", "clip": second_file}],
            files_fn=lambda item: (item["clip"],),
            params_fn=lambda item: {"name": item["name"]},
        )
        def analyze(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            executed.append(ctx.params["name"])
            return {"name": ctx.params["name"]}

        return dag.run()

    assert run_once().cache_hits == []
    assert executed == ["first", "second"]
    executed.clear()
    first_file.write_bytes(b"changed")
    result = run_once()
    assert result.cache_hits == ["analyze_second"]
    assert executed == ["first"]


def test_map_fans_out_runtime_items_and_aggregates_for_downstream(tmp_path: Path) -> None:
    """教训 map_aggregate: 运行期清单也必须以一个稳定聚合产物喂给下游。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan", params={"items": [{"id": "a", "value": 1}, {"id": "b", "value": 2}]})
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": ctx.params["items"]}

    @dag.map("process", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        return {"value": item["value"] * 2}

    @dag.node("gather", deps=("process",))
    def gather(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"received": inputs["process"]}

    result = dag.run()

    assert result.artifacts["process"] == {
        "items": {"a": {"value": 2}, "b": {"value": 4}},
        "order": ["a", "b"],
        "count": 2,
    }
    assert result.artifacts["gather"]["received"] == result.artifacts["process"]
    assert (tmp_path / "artifacts" / "runs" / result.run_id / "process@a.json").is_file()


def test_sidecar_shapes_distinguish_node_aggregate_and_item(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        del inputs, ctx
        return {"items": [{"id": "one"}]}

    @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"id": item["id"]}

    result = dag.run()
    run_root = tmp_path / "artifacts" / "runs" / result.run_id
    source_metadata = json.loads((run_root / "source.json.meta.json").read_text(encoding="utf-8"))
    aggregate = json.loads((run_root / "work.json.meta.json").read_text(encoding="utf-8"))
    item = json.loads((run_root / "work@one.json.meta.json").read_text(encoding="utf-8"))

    assert isinstance(source_metadata["cache_key"], str)
    assert set(source_metadata) == {
        "node",
        "cache_key",
        "key_components",
        "cache",
        "cache_policy",
        "outputs",
        "seconds",
        "calls",
        "created_at",
    }
    assert isinstance(aggregate["cache_key"], list)
    assert set(aggregate) == {
        "node",
        "cache_key",
        "cache",
        "cache_policy",
        "outputs",
        "seconds",
        "calls",
        "created_at",
    }
    assert "key_components" not in aggregate
    assert item["node"] == "work@one"
    assert set(item) == {
        "node",
        "cache_key",
        "key_components",
        "cache",
        "cache_policy",
        "outputs",
        "seconds",
        "calls",
        "created_at",
    }
    assert "key_components" in item


def test_map_aggregate_fn_can_narrow_downstream_artifact(tmp_path: Path) -> None:
    """教训 map_aggregate_fn: 大 item 不必复制进只消费摘要的下游聚合。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "a", "value": 2}, {"id": "b", "value": 4}]}

    @dag.map(
        "process",
        items_from=("scan", "items"),
        key_fn=lambda item: item["id"],
        aggregate_fn=lambda items, order: {
            "ids": order,
            "total": sum(items[item_id]["value"] for item_id in order),
        },
    )
    def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        return {"value": item["value"]}

    @dag.node("downstream", deps=("process",))
    def downstream(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"received": inputs["process"]}

    result = dag.run()

    assert result.artifacts["process"] == {"ids": ["a", "b"], "total": 6}
    assert result.artifacts["downstream"] == {"received": {"ids": ["a", "b"], "total": 6}}


def test_map_aggregate_fn_change_reuses_items_and_invalidates_downstream(tmp_path: Path) -> None:
    """教训 map_aggregate_fn_cache: 聚合变更重建下游，但绝不进入 item 缓存键。"""
    executed: list[str] = []

    def summary_v1(items: dict[str, dict[str, Any]], order: list[str]) -> dict[str, Any]:
        return {"count": len(order)}

    def summary_v2(items: dict[str, dict[str, Any]], order: list[str]) -> dict[str, Any]:
        return {"ids": order, "count": len(order)}

    def build(
        aggregate_fn: Callable[[dict[str, dict[str, Any]], list[str]], dict[str, Any]],
    ) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan")
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"id": "a"}, {"id": "b"}]}

        @dag.map(
            "m",
            items_from=("scan", "items"),
            key_fn=lambda item: item["id"],
            aggregate_fn=aggregate_fn,
        )
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            executed.append(item["id"])
            return {"id": item["id"]}

        @dag.node("downstream", deps=("m",))
        def downstream(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"aggregate": inputs["m"]}

        return dag

    build(summary_v1).run()
    executed.clear()
    changed = build(summary_v2)

    plan = changed.plan()
    assert plan.nodes == {
        "scan": "hit",
        "m@a": "hit",
        "m@b": "hit",
        "m": "hit",
        "downstream": "miss",
    }
    result = changed.run()

    assert executed == []
    assert result.cache_hits == ["scan", "m"]
    assert result.artifacts["m"] == {"ids": ["a", "b"], "count": 2}
    assert result.artifacts["downstream"] == {"aggregate": {"ids": ["a", "b"], "count": 2}}
    assert changed.plan().nodes == {
        "scan": "hit",
        "m@a": "hit",
        "m@b": "hit",
        "m": "hit",
        "downstream": "hit",
    }


def test_map_aggregate_fn_must_return_dict(tmp_path: Path) -> None:
    """教训 map_aggregate_fn_type: 聚合契约错误必须标出所属 map 节点。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": ["one"]}

    @dag.map("process", items_from=("scan", "items"), aggregate_fn=lambda items, order: [])
    def process(item: str, inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"item": item}

    with pytest.raises(
        TypeError, match="Map node 'process' aggregate_fn must return a dict artifact"
    ):
        dag.run()


def test_map_incrementally_recomputes_only_changed_and_added_items(tmp_path: Path) -> None:
    """教训 map_incremental: 清单变动不能把未牵连 item 的缓存一起冲掉。"""
    executed: list[str] = []

    def build(items: list[dict[str, Any]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": ctx.params["items"]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            executed.append(item["id"])
            return {"value": item["value"]}

        @dag.node("gather", deps=("m",))
        def gather(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"count": inputs["m"]["count"]}

        return dag

    assert build([{"id": "a", "value": 1}, {"id": "b", "value": 2}]).run().cache_hits == []
    assert executed == ["a", "b"]
    executed.clear()
    result = build(
        [{"id": "a", "value": 1}, {"id": "b", "value": 20}, {"id": "c", "value": 3}]
    ).run()

    assert executed == ["b", "c"]
    assert result.cache_hits == []
    assert result.artifacts["gather"] == {"count": 3}


def test_map_reorder_rebuilds_aggregate_without_reexecuting_items(tmp_path: Path) -> None:
    """教训 map_order: 顺序属于聚合语义，却不是 item 身份输入。"""
    executed: list[str] = []

    def build(items: list[dict[str, str]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": ctx.params["items"]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            executed.append(item["id"])
            return {"id": item["id"]}

        @dag.node("downstream", deps=("m",))
        def downstream(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"order": inputs["m"]["order"]}

        return dag

    build([{"id": "a"}, {"id": "b"}]).run()
    executed.clear()
    result = build([{"id": "b"}, {"id": "a"}]).run()

    assert executed == []
    assert result.cache_hits == ["m"]
    assert result.artifacts["m"]["order"] == ["b", "a"]
    assert result.artifacts["downstream"] == {"order": ["b", "a"]}


def test_node_names_must_be_safe_single_path_components(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    for name in (
        "",
        "../escape",
        "nested/node",
        "nested\\node",
        ".",
        "..",
        "models",
        "subgraphs",
    ):
        with pytest.raises(ValueError, match="single relative path component|reserved"):
            dag.node(name)


def test_map_rejects_duplicate_item_ids_without_deduplicating(tmp_path: Path) -> None:
    """教训 map_duplicate_id: 寻址冲突必须立即显形，不能静默吞 item。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "same"}, {"id": "same"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return item

    with pytest.raises(ValueError, match="duplicate item_id values: same"):
        dag.run()


def test_map_rejects_item_ids_that_are_not_single_path_components(tmp_path: Path) -> None:
    for item_id in ("../escape", "nested/item", "nested\\item", ".", ".."):
        dag = _make_dag(tmp_path / item_id.replace("/", "-"))

        @dag.node("source", params={"item_id": item_id})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"id": ctx.params["item_id"]}]}

        @dag.map("m", items_from=("source", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return item

        with pytest.raises(ValueError, match="single relative path component"):
            dag.run()


def test_map_preserves_successful_items_when_one_item_fails(tmp_path: Path) -> None:
    """教训 map_failure_isolation: 一个 item 崩溃不能浪费已完成 item 的缓存。"""
    executed: list[str] = []

    def build(items: list[dict[str, Any]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": ctx.params["items"]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            executed.append(item["id"])
            if item["bad"]:
                raise ValueError("broken")
            return {"id": item["id"]}

        return dag

    initial = [{"id": "a", "bad": False}, {"id": "b", "bad": True}, {"id": "c", "bad": False}]
    with pytest.raises(RuntimeError, match=r"b \(ValueError: broken\)"):
        build(initial).run()
    assert executed == ["a", "b", "c"]
    executed.clear()
    result = build(
        [{"id": "a", "bad": False}, {"id": "b", "bad": False}, {"id": "c", "bad": False}]
    ).run()

    assert executed == ["b"]
    assert result.artifacts["m"]["count"] == 3


def test_map_checkpoint_is_namespaced_and_resumes_one_item(tmp_path: Path) -> None:
    """教训 map_checkpoint: item 审批要隔离命名，恢复时只重跑挂起项。"""
    dag = _make_dag(tmp_path)
    executed: list[str] = []

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "ready"}, {"id": "review"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        executed.append(item["id"])
        if item["id"] == "review":
            return {"approval": ctx.checkpoint("editor", {"id": item["id"]})}
        return {"id": item["id"]}

    @dag.node("after", deps=("m",))
    def after(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        return {"ran": True}

    first = dag.run(run_id="map-approval")
    assert first.pending_checkpoints == ["editor@review"]
    assert first.skipped == ["after"]
    assert executed == ["ready", "review"]
    dag.approve("map-approval", "editor@review", {"ok": True})
    executed.clear()
    resumed = dag.run(run_id="map-approval")

    assert executed == ["review"]
    assert resumed.artifacts["m"]["items"]["review"] == {"approval": {"ok": True}}
    assert resumed.skipped == []

    executed.clear()
    fresh = dag.run(run_id="map-approval-fresh")
    assert fresh.pending_checkpoints == ["editor@review"]
    assert fresh.skipped == ["after"]
    assert executed == ["review"]


def test_scan_checkpoint_approval_does_not_leak_through_item_cache(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    executed: list[str] = []

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "ready"}, {"id": "review"}]}

    @dag.scan("review", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def review(
        item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any
    ) -> dict[str, Any]:
        executed.append(item["id"])
        if item["id"] == "review":
            return {"approval": ctx.checkpoint("editor", {"id": item["id"]})}
        return {"id": item["id"]}

    first = dag.run(run_id="scan-approval")
    assert first.pending_checkpoints == ["editor@review"]
    assert executed == ["ready", "review"]

    dag.approve(first.run_id, "editor@review", {"ok": True})
    executed.clear()
    resumed = dag.run(run_id=first.run_id)
    assert resumed.pending_checkpoints == []
    assert resumed.artifacts["review"]["items"]["review"] == {"approval": {"ok": True}}
    assert executed == ["review"]

    executed.clear()
    fresh = dag.run(run_id="scan-approval-fresh")
    assert fresh.pending_checkpoints == ["editor@review"]
    assert executed == ["review"]


def test_map_force_can_target_one_runtime_item(tmp_path: Path) -> None:
    """教训 map_force_item: 运行时 id 的 force 必须精确且拼错立即报错。"""
    dag = _make_dag(tmp_path)
    executed: list[str] = []

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "one"}, {"id": "two"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        executed.append(item["id"])
        return item

    dag.run()
    executed.clear()
    dag.run(force=["m@two"])
    assert executed == ["two"]
    with pytest.raises(ValueError, match="m@nope"):
        dag.run(force=["m@nope"])


def test_map_files_fn_invalidates_only_the_affected_item(tmp_path: Path) -> None:
    """教训 map_item_file: 逐项文件内容只应换掉对应 item 的缓存键。"""
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text("first", encoding="utf-8")
    second.write_text("second", encoding="utf-8")
    executed: list[str] = []
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "first"}, {"id": "second"}]}

    @dag.map(
        "m",
        items_from=("scan", "items"),
        key_fn=lambda item: item["id"],
        files_fn=lambda item: (first if item["id"] == "first" else second,),
    )
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        executed.append(item["id"])
        return item

    dag.run()
    executed.clear()
    first.write_text("changed", encoding="utf-8")
    dag.run()
    assert executed == ["first"]


def test_map_gc_retains_item_cache_keys_referenced_by_aggregate(tmp_path: Path) -> None:
    """教训 map_gc: gc 不认识聚合里的 item 键会静默删掉可复用缓存。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return item

    run = dag.run()
    meta = json.loads(
        (tmp_path / "artifacts" / "runs" / run.run_id / "m.json.meta.json").read_text(
            encoding="utf-8"
        )
    )
    assert isinstance(meta["cache_key"], list)
    # 逐项 sidecar 只是冗余引用;删掉它们,聚合 sidecar 必须独自保住 item 缓存。
    for sidecar in (tmp_path / "artifacts" / "runs" / run.run_id).glob("m@*.json.meta.json"):
        sidecar.unlink()
    assert dag.gc(keep_last=1) == 0


def test_map_item_sidecars_retain_item_cache_without_aggregate_sidecar(tmp_path: Path) -> None:
    """教训 map_gc_redundancy: 聚合 sidecar 丢失时逐项引用仍必须保住缓存。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return item

    run = dag.run()
    run_root = tmp_path / "artifacts" / "runs" / run.run_id
    item_keys = [
        json.loads(sidecar.read_text(encoding="utf-8"))["cache_key"]
        for sidecar in sorted(run_root.glob("m@*.json.meta.json"))
    ]
    item_caches = [tmp_path / "artifacts" / "_cache" / "nodes" / f"{key}.json" for key in item_keys]
    assert all(path.is_file() for path in item_caches)

    (run_root / "m.json.meta.json").unlink()

    assert dag.gc(keep_last=1) == 0
    assert all(path.is_file() for path in item_caches)


def test_gc_skips_invalid_sidecars_without_losing_normal_references(tmp_path: Path) -> None:
    """教训 gc_sidecar_tolerance: 坏 sidecar 不能阻断或削弱其他引用。"""
    dag = _make_dag(tmp_path)

    @dag.node("normal")
    def normal(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "kept"}

    run = dag.run()
    run_root = tmp_path / "artifacts" / "runs" / run.run_id
    normal_metadata = json.loads((run_root / "normal.json.meta.json").read_text(encoding="utf-8"))
    normal_cache = (
        tmp_path / "artifacts" / "_cache" / "nodes" / f"{normal_metadata['cache_key']}.json"
    )
    stale_cache = tmp_path / "artifacts" / "_cache" / "nodes" / "unreferenced.json"
    stale_cache.write_text("{}", encoding="utf-8")
    (run_root / "broken.json.meta.json").write_text("{", encoding="utf-8")
    (run_root / "scalar.json.meta.json").write_text("[]", encoding="utf-8")

    assert dag.gc(keep_last=1) == 1
    assert normal_cache.is_file()
    assert not stale_cache.exists()


def test_map_item_sidecars_capture_only_their_own_calls(tmp_path: Path) -> None:
    """教训 map_call_observer: 并行 item 的调用溯源不能串到同一 sidecar。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"answer": ctx.call(f"prompt {item['id']}")}

    result = dag.run(run_id="map-calls", workers=2)
    run_root = tmp_path / "artifacts" / "runs" / result.run_id
    for item_id in ("a", "b"):
        calls = json.loads((run_root / f"m@{item_id}.json.meta.json").read_text(encoding="utf-8"))[
            "calls"
        ]
        assert [call["prompt_sha"] for call in calls] == [
            sha([{"role": "user", "content": f"prompt {item_id}"}])
        ]


def test_map_key_fn_renaming_does_not_change_item_identity(tmp_path: Path) -> None:
    """教训 map_id_not_identity: 改寻址名称不能让内容相同的 item 重算。"""
    executed: list[str] = []

    def build(key_fn: Callable[[dict[str, str]], str]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan")
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"name": "first"}, {"name": "second"}]}

        @dag.map("m", items_from=("scan", "items"), key_fn=key_fn)
        def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            executed.append(item["name"])
            return item

        return dag

    build(lambda item: f"old-{item['name']}").run()
    executed.clear()
    result = build(lambda item: f"new-{item['name']}").run()

    assert executed == []
    assert result.artifacts["m"]["order"] == ["new-first", "new-second"]
    assert result.cache_hits == ["scan", "m"]


def test_map_dry_run_only_errors_for_uncached_item_calls(tmp_path: Path) -> None:
    """教训 map_dry_run: 探测本地清单后，未命中 item 仍必须阻止真实调用。"""

    def build(dry: bool) -> Dag:
        config = KigumiConfig(project_root=tmp_path, source_dirs=[])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm", dry=dry))

        @dag.node("scan")
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"id": "a"}]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"answer": ctx.call("would call")}

        return dag

    with pytest.raises(DryRunError):
        build(dry=True).run()
    assert build(dry=False).run().cache_hits == ["scan"]
    assert build(dry=True).run().cache_hits == ["scan", "m"]


def test_blob_cache_hit_rematerializes_deleted_binary_output(tmp_path: Path) -> None:
    """教训 blob_cache_hit: 命中缓存也必须恢复曾被清理的二进制交付物。"""
    dag = _make_dag(tmp_path)
    executions = 0

    @dag.node("build")
    def build(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        nonlocal executions
        executions += 1
        return {"file": ctx.emit_file("generated/result.bin", b"binary result")}

    first = dag.run()
    output = tmp_path / "generated" / "result.bin"
    output.unlink()
    second = dag.run()

    assert first.cache_hits == []
    assert second.cache_hits == ["build"]
    assert executions == 1
    assert output.read_bytes() == b"binary result"


def test_missing_blob_names_its_digest_and_node(tmp_path: Path) -> None:
    """教训 blob_missing: 换机器或误删仓文件不能让节点假装成功。"""
    dag = _make_dag(tmp_path)

    @dag.node("package")
    def package(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"file": ctx.emit_file("generated/package.bin", b"package")}

    digest = dag.run().artifacts["package"]["file"]["kigumi_blob"]
    (tmp_path / "artifacts" / "_cache" / "blobs" / digest).unlink()

    with pytest.raises(FileNotFoundError, match=rf"{digest}.*package"):
        dag.run()


@pytest.mark.parametrize("relative_path", ["/tmp/escape.bin", "../escape.bin"])
def test_emit_file_rejects_paths_outside_the_project(tmp_path: Path, relative_path: str) -> None:
    """教训 blob_path_guard: artifact 物化路径不能借 blob 逃出项目目录。"""
    dag = _make_dag(tmp_path)

    @dag.node("emit")
    def emit(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"file": ctx.emit_file(relative_path, b"unsafe")}

    with pytest.raises(ValueError, match="project-relative"):
        dag.run()


def test_ingest_file_copies_an_external_source_without_moving_it(tmp_path: Path) -> None:
    """教训 blob_ingest_copy: 工具临时产物属于调用方，收编不能偷偷 move。"""
    external = tmp_path.parent / f"{tmp_path.name}-external.bin"
    external.write_bytes(b"tool output")
    dag = _make_dag(tmp_path)

    @dag.node("ingest")
    def ingest(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"file": ctx.ingest_file(external, "generated/ingested.bin")}

    try:
        result = dag.run()
        reference = result.artifacts["ingest"]["file"]

        assert external.read_bytes() == b"tool output"
        assert reference["bytes"] == len(b"tool output")
        assert (tmp_path / "generated" / "ingested.bin").read_bytes() == b"tool output"
        assert (tmp_path / "artifacts" / "_cache" / "blobs" / reference["kigumi_blob"]).is_file()
    finally:
        external.unlink(missing_ok=True)


def test_gc_keeps_blobs_referenced_by_the_latest_run(tmp_path: Path) -> None:
    """教训 blob_gc: 回收旧缓存时不能误删保留 run 仍可物化的交付物。"""

    def build(data: bytes) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("build", params={"data": data.decode("ascii")})
        def output(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"file": ctx.emit_file("generated/current.bin", data)}

        return dag

    old = build(b"old").run().artifacts["build"]["file"]["kigumi_blob"]
    current_dag = build(b"new")
    new = current_dag.run().artifacts["build"]["file"]["kigumi_blob"]

    assert current_dag.gc(keep_last=1) == 2
    blobs = tmp_path / "artifacts" / "_cache" / "blobs"
    assert not (blobs / old).exists()
    assert (blobs / new).is_file()
    (tmp_path / "generated" / "current.bin").unlink()
    assert current_dag.run().cache_hits == ["build"]
    assert (tmp_path / "generated" / "current.bin").read_bytes() == b"new"


def test_blob_reference_invalidates_a_downstream_node_by_content(tmp_path: Path) -> None:
    """教训 blob_upstream_key: 二进制变更必须让依赖其引用的下游失效。"""

    def build(data: bytes) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source", params={"data": data.decode("ascii")})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"file": ctx.emit_file("generated/source.bin", data)}

        @dag.node("consumer", deps=("source",))
        def consumer(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"digest": inputs["source"]["file"]["kigumi_blob"]}

        return dag

    assert build(b"stable").run().cache_hits == []
    assert build(b"stable").run().cache_hits == ["source", "consumer"]
    assert build(b"changed").run().cache_hits == []


def test_map_blob_items_recompute_only_changed_items_and_rematerialize_hits(tmp_path: Path) -> None:
    """教训 map_blob_item_cache: 单项内容变化不能拖累命中项的缓存或物化。"""
    executed: list[str] = []

    def build(items: list[dict[str, str]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": ctx.params["items"]}

        @dag.map("render", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def render(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            executed.append(item["id"])
            return {"file": ctx.emit_file(f"generated/{item['id']}.bin", item["data"].encode())}

        return dag

    first_items = [{"id": "a", "data": "one"}, {"id": "b", "data": "two"}]
    build(first_items).run()
    executed.clear()
    (tmp_path / "generated" / "b.bin").unlink()
    second_items = [{"id": "a", "data": "changed"}, {"id": "b", "data": "two"}]
    result = build(second_items).run()

    assert result.cache_hits == []
    assert executed == ["a"]
    assert (tmp_path / "generated" / "b.bin").read_bytes() == b"two"


def test_concurrent_archives_share_one_history_directory(tmp_path: Path) -> None:
    """教训 archive_race: 并发节点归档必须共用同一个 history 目录,一次 run 一份历史。"""
    data = tmp_path / "data.txt"

    def build(tag: str) -> Dag:
        dag = _make_dag(tmp_path)
        barrier = threading.Barrier(2, timeout=10)

        @dag.node("left", params={"tag": tag})
        def left(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            barrier.wait()
            return {"value": ctx.params["tag"]}

        @dag.node("scan")
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"id": "a"}]}

        @dag.map(
            "m",
            items_from=("scan", "items"),
            key_fn=lambda item: item["id"],
            files_fn=lambda item: ("data.txt",),
        )
        def render(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            barrier.wait()
            return {"text": ctx.read_text("data.txt")}

        return dag

    data.write_text("one", encoding="utf-8")
    build("one").run(run_id="race", workers=2)
    data.write_text("two", encoding="utf-8")
    build("two").run(run_id="race", workers=2)

    history = tmp_path / "artifacts" / "runs" / "race" / "history"
    assert [path.name for path in sorted(history.iterdir()) if path.is_dir()] == ["0001"]


def test_map_items_from_resolves_nested_paths_even_when_dotted_top_level_exists(
    tmp_path: Path,
) -> None:
    """教训 nested_items_from: 点分路径必须一律逐段下钻，不能保留旧 artifact 歧义。"""
    received: list[str] = []
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {
            "structure": {"segments": [{"id": "nested"}]},
            "structure.segments": [{"id": "exact"}],
        }

    @dag.map("m", items_from=("scan", "structure.segments"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        received.append(item["id"])
        return item

    result = dag.run()

    assert received == ["nested"]
    assert result.artifacts["m"]["order"] == ["nested"]


def test_parallel_failures_keep_topological_first_and_note_the_rest(tmp_path: Path) -> None:
    """教训 concurrent_failure: 并发旁支失败不能因首个异常而无声丢失。"""
    dag = _make_dag(tmp_path)
    first_ready = threading.Event()
    second_ready = threading.Event()

    @dag.node("first")
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        first_ready.set()
        assert second_ready.wait(5)
        raise ValueError("first failure")

    @dag.node("second")
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        second_ready.set()
        assert first_ready.wait(5)
        raise RuntimeError("second failure")

    with pytest.raises(ValueError, match="first failure") as raised:
        dag.run(workers=2)

    assert raised.value.__notes__ == [
        "additional concurrent failure: second: RuntimeError: second failure"
    ]


def test_explain_without_run_id_uses_numeric_latest_run(tmp_path: Path) -> None:
    """教训 explain_run_sort: 最新 sidecar 的选择必须按 run 序号而非字典序。"""
    dag = _make_dag(tmp_path)
    root = tmp_path / "artifacts" / "runs"
    for run_id, status in [("run-9999", "old"), ("run-10000", "latest")]:
        run = root / run_id
        run.mkdir(parents=True)
        (run / "node.json.meta.json").write_text('{"status": "' + status + '"}', encoding="utf-8")

    assert dag._read_explain_sidecar("node", None) == {"status": "latest"}


@pytest.mark.parametrize(
    ("artifact", "expected"),
    [
        ({"structure": {}}, "broke at 'segments': key is missing"),
        ({"structure": []}, "broke at 'segments': 'structure' is not a Mapping"),
    ],
)
def test_map_items_from_path_errors_name_the_full_path_and_break(
    tmp_path: Path, artifact: dict[str, Any], expected: str
) -> None:
    """教训 nested_path_diagnostics: 路径错位必须指出完整路径与断点。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan", params={"artifact": artifact})
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return ctx.params["artifact"]

    @dag.map("m", items_from=("scan", "structure.segments"))
    def process(item: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"item": item}

    with pytest.raises(ValueError, match="Map node 'm'.*structure\\.segments") as error:
        dag.run()
    assert expected in str(error.value)


def test_nested_items_from_keeps_item_cache_granularity(tmp_path: Path) -> None:
    """教训 nested_item_cache: 取列表路径不应改变 item 内容寻址语义。"""
    executed: list[str] = []

    def build(items: list[dict[str, Any]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"structure": {"segments": ctx.params["items"]}}

        @dag.map("m", items_from=("scan", "structure.segments"), key_fn=lambda item: item["id"])
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            executed.append(item["id"])
            return {"value": item["value"]}

        return dag

    build([{"id": "a", "value": 1}, {"id": "b", "value": 2}]).run()
    assert executed == ["a", "b"]
    executed.clear()
    result = build([{"id": "a", "value": 1}, {"id": "b", "value": 20}]).run()

    assert executed == ["b"]
    assert result.map_items == {"m": {"a": "hit", "b": "miss"}}


def test_run_result_exposes_map_item_hit_miss_statuses(tmp_path: Path) -> None:
    """教训 map_result_status: 验收不该读取 sidecar 私有布局判断逐项缓存。"""
    executed: list[str] = []

    def build(items: list[dict[str, Any]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": ctx.params["items"]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            executed.append(item["id"])
            return {"value": item["value"]}

        return dag

    first = build([{"id": "a", "value": 1}, {"id": "b", "value": 2}]).run()
    second = build([{"id": "a", "value": 1}, {"id": "b", "value": 2}]).run()
    third = build([{"id": "a", "value": 1}, {"id": "b", "value": 3}]).run()

    assert first.map_items == {"m": {"a": "miss", "b": "miss"}}
    assert second.map_items == {"m": {"a": "hit", "b": "hit"}}
    assert third.map_items == {"m": {"a": "hit", "b": "miss"}}
    assert executed == ["a", "b", "b"]


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


def test_plan_forecasts_cache_without_calling_nodes_and_matches_run(tmp_path: Path) -> None:
    """教训 plan_truthfulness: 预览只能复用真实键路径，不能猜上游 miss 的产物。"""
    calls: list[str] = []

    def build(value: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source", params={"value": value})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            calls.append("source")
            return {"value": ctx.params["value"]}

        @dag.node("leaf", deps=("source",))
        def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            calls.append("leaf")
            return {"value": inputs["source"]["value"] + 1}

        return dag

    fresh = build(1).plan()
    assert fresh.nodes == {"source": "miss", "leaf": "unknown"}
    assert fresh.misses == ["source", "leaf"]
    assert calls == []
    assert not (tmp_path / "artifacts" / "runs").exists()
    build(1).run()
    assert build(1).plan().nodes == {"source": "hit", "leaf": "hit"}

    changed = build(2)
    forecast = changed.plan()
    result = changed.run()
    actual_misses = [name for name in ("source", "leaf") if name not in result.cache_hits]

    assert forecast.misses == actual_misses == ["source", "leaf"]


def test_plan_reports_unknown_reason_chains_and_cost_bounds(tmp_path: Path) -> None:
    """教训 plan_intervals: unknown 必须说明直接等待谁，而非被误判为确定重算。"""

    def build(value: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("a", params={"value": value})
        def a(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            del inputs
            return {"value": ctx.params["value"]}

        @dag.node("b", deps=("a",))
        def b(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": inputs["a"]["value"] + 1}

        @dag.node("c", deps=("b",))
        def c(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": inputs["b"]["value"] + 1}

        return dag

    build(1).run()
    forecast = build(2).plan()

    assert forecast.nodes == {"a": "miss", "b": "unknown", "c": "unknown"}
    assert forecast.pending_on == {"b": ("a",), "c": ("b",)}
    assert forecast.certain == ["a"]
    assert forecast.at_risk == ["b", "c"]


def test_plan_map_aggregate_miss_is_the_downstream_unknown_reason(tmp_path: Path) -> None:
    """教训 plan_map_reason: map 聚合未命中时下游只直接等待 map 节点。"""

    def build(tag: str) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, int | str]]]:
            del inputs, ctx
            return {"items": [{"id": "a", "value": 1}]}

        @dag.map(
            "mapped",
            items_from=("source", "items"),
            key_fn=lambda item: item["id"],
            params={"tag": tag},
        )
        def mapped(item: dict[str, int | str], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            del inputs, ctx
            return {"value": item["value"]}

        @dag.node("after", deps=("mapped",))
        def after(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": int(inputs["mapped"]["items"]["a"]["value"])}

        return dag

    build("v1").run()
    forecast = build("v2").plan()

    assert forecast.nodes == {
        "source": "hit",
        "mapped@a": "miss",
        "mapped": "miss",
        "after": "unknown",
    }
    assert forecast.pending_on == {"after": ("mapped",)}


def test_plan_map_items_expose_their_direct_unknown_dependency(tmp_path: Path) -> None:
    """教训 plan_map_item_reason: 已知清单的展开项也必须带直接等待边。"""

    def build(style: str) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
            del inputs, ctx
            return {"items": [{"id": "a"}, {"id": "b"}]}

        @dag.node("style", params={"style": style})
        def style_node(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            del inputs
            return {"style": ctx.params["style"]}

        @dag.map(
            "mapped",
            items_from=("source", "items"),
            key_fn=lambda item: item["id"],
            deps=("style",),
        )
        def mapped(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            del inputs, ctx
            return item

        return dag

    build("v1").run()
    forecast = build("v2").plan()

    assert forecast.nodes == {
        "source": "hit",
        "style": "miss",
        "mapped@a": "unknown",
        "mapped@b": "unknown",
        "mapped": "unknown",
    }
    assert forecast.pending_on == {
        "mapped@a": ("style",),
        "mapped@b": ("style",),
        "mapped": ("style",),
    }


def test_plan_expands_map_items_and_propagates_unknown(tmp_path: Path) -> None:
    """教训 plan_map_cost: map 只有在上游 artifact 已知时才能诚实展开 item 成本。"""

    def build(value: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"value": value})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"id": "a", "value": ctx.params["value"]}, {"id": "b", "value": 2}]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"value": item["value"]}

        @dag.node("after", deps=("m",))
        def after(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"count": inputs["m"]["count"]}

        return dag

    assert build(1).plan().nodes == {"scan": "miss", "m": "unknown", "after": "unknown"}
    build(1).run()
    forecast = build(2).plan()

    assert forecast.nodes == {
        "scan": "miss",
        "m": "unknown",
        "after": "unknown",
    }
    forced_item = build(1).plan(force=("m@a",))
    assert forced_item.nodes == {
        "scan": "hit",
        "m@a": "miss",
        "m@b": "hit",
        "m": "miss",
        "after": "unknown",
    }
    assert build(1).plan().nodes == {
        "scan": "hit",
        "m@a": "hit",
        "m@b": "hit",
        "m": "hit",
        "after": "hit",
    }


def _build_scan_dag(
    tmp_path: Path,
    items: list[dict[str, Any]],
    *,
    initial: int = 0,
    carry_fn: Callable[[dict[str, Any]], Any] | None = None,
    fail: Callable[[str], bool] | None = None,
    attempts: dict[str, int] | None = None,
    item_files: bool = False,
) -> tuple[Dag, list[str]]:
    """Build a small carry chain whose source list may change between DAG instances."""
    dag = _make_dag(tmp_path)
    executed: list[str] = []
    effective_carry_fn = carry_fn or (lambda artifact: artifact["carry"])

    @dag.node("source", params={"items": items})
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        return {"items": ctx.params["items"]}

    @dag.node("initial", params={"initial": initial})
    def initial_node(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        return {"carry": {"total": ctx.params["initial"]}}

    @dag.scan(
        "chain",
        items_from=("source", "items"),
        carry_from=("initial", "carry"),
        key_fn=lambda item: item["id"],
        carry_fn=effective_carry_fn,
        files_fn=(lambda item: (item["file"],)) if item_files else None,
    )
    def chain(
        item: dict[str, Any], carry: dict[str, int], inputs: dict[str, Any], ctx: Any
    ) -> dict[str, Any]:
        del inputs, ctx
        executed.append(item["id"])
        if fail is not None and fail(item["id"]):
            raise ValueError(f"broken {item['id']}")
        number = attempts.get(item["id"], 0) + 1 if attempts is not None else 1
        if attempts is not None:
            attempts[item["id"]] = number
        total = carry["total"] + int(item["value"])
        return {"carry": {"total": total, "attempt": number}, "id": item["id"]}

    @dag.node("after", deps=("chain",))
    def after(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        return {"total": inputs["chain"]["items"]["c"]["carry"]["total"]}

    return dag, executed


def test_scan_reuses_prefix_and_invalidates_suffix(tmp_path: Path) -> None:
    """教训 scan_prefix: 改第 K 项时，先前 carry 链必须继续命中。"""
    original = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    original_dag, _ = _build_scan_dag(tmp_path, original)
    original_dag.run()
    changed, executed = _build_scan_dag(
        tmp_path, [{"id": "a", "value": 1}, {"id": "b", "value": 20}, {"id": "c", "value": 3}]
    )

    result = changed.run()

    assert result.map_items["chain"] == {"a": "hit", "b": "miss", "c": "miss"}
    assert executed == ["b", "c"]


def test_scan_carry_fn_code_is_irrelevant_when_extracted_content_is_equal(tmp_path: Path) -> None:
    """教训 scan_carry_content: carry_fn 的实现不是输入，实际 carry 内容才是。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    original_dag, _ = _build_scan_dag(tmp_path, items, carry_fn=lambda artifact: artifact["carry"])
    original_dag.run()
    equivalent, _ = _build_scan_dag(
        tmp_path, items, carry_fn=lambda artifact: dict(artifact["carry"])
    )

    equal_result = equivalent.run()
    changed, _ = _build_scan_dag(
        tmp_path,
        items,
        carry_fn=lambda artifact: {
            "total": artifact["carry"]["total"] + 1,
            "attempt": artifact["carry"]["attempt"],
        },
    )
    changed_result = changed.run()

    assert equal_result.map_items["chain"] == {"a": "hit", "b": "hit", "c": "hit"}
    assert changed_result.map_items["chain"] == {"a": "hit", "b": "miss", "c": "miss"}


def test_scan_carry_from_content_invalidates_the_whole_chain(tmp_path: Path) -> None:
    """教训 scan_initial_carry: 初始 carry 内容变化必须从第一项自然传导。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    original_dag, _ = _build_scan_dag(tmp_path, items, initial=0)
    original_dag.run()
    changed, _ = _build_scan_dag(tmp_path, items, initial=10)

    result = changed.run()

    assert result.map_items["chain"] == {"a": "miss", "b": "miss", "c": "miss"}


def test_scan_items_stay_serial_while_a_ready_side_branch_runs(tmp_path: Path) -> None:
    """教训 scan_serial: carry 链串行不应占掉无关旁支的并行调度机会。"""
    dag = _make_dag(tmp_path)
    scan_started = threading.Event()
    side_finished = threading.Event()
    completed: list[str] = []

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        del inputs, ctx
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.scan("chain", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def chain(item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del carry, inputs, ctx
        if item["id"] == "a":
            scan_started.set()
            assert side_finished.wait(timeout=1)
        else:
            assert completed == ["a"]
        completed.append(item["id"])
        return item

    @dag.node("side", deps=("source",))
    def side(inputs: dict[str, Any], ctx: Any) -> dict[str, bool]:
        del inputs, ctx
        assert scan_started.wait(timeout=1)
        side_finished.set()
        return {"ran": True}

    result = dag.run(workers=2)

    assert completed == ["a", "b"]
    assert result.artifacts["side"] == {"ran": True}


def test_scan_force_item_propagates_changed_carry_to_suffix(tmp_path: Path) -> None:
    """教训 scan_force_carry: 强算项产物变化后，后缀不能错误复用旧 carry。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    attempts: dict[str, int] = {}
    dag, _ = _build_scan_dag(tmp_path, items, attempts=attempts)
    dag.run()

    result = dag.run(force=("chain@b",))

    assert result.map_items["chain"] == {"a": "hit", "b": "miss", "c": "miss"}


def test_scan_preserves_completed_prefix_after_item_failure(tmp_path: Path) -> None:
    """教训 scan_resume: 第 K 项失败不能丢弃已落盘的前缀缓存。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    failed_once = {"active": True}
    dag, _ = _build_scan_dag(
        tmp_path, items, fail=lambda item_id: failed_once["active"] and item_id == "b"
    )

    with pytest.raises(RuntimeError, match="failed item 'b'"):
        dag.run()
    failed_once["active"] = False
    result = dag.run()

    assert result.map_items["chain"] == {"a": "hit", "b": "miss", "c": "miss"}


def test_scan_plan_forecasts_prefix_and_matches_execution(tmp_path: Path) -> None:
    """教训 scan_plan: 已知前缀可精确预告，首个 miss 后只能诚实标 unknown。"""
    original = [
        {"id": "a", "value": 1, "file": "a.txt"},
        {"id": "b", "value": 2, "file": "b.txt"},
        {"id": "c", "value": 3, "file": "c.txt"},
    ]
    for item in original:
        (tmp_path / item["file"]).write_text(item["id"], encoding="utf-8")
    original_dag, _ = _build_scan_dag(tmp_path, original, item_files=True)
    original_dag.run()
    (tmp_path / "b.txt").write_text("changed", encoding="utf-8")
    changed, _ = _build_scan_dag(tmp_path, original, item_files=True)

    forecast = changed.plan()
    result = changed.run()

    assert forecast.nodes == {
        "source": "hit",
        "initial": "hit",
        "chain@a": "hit",
        "chain@b": "miss",
        "chain@c": "unknown",
        "chain": "miss",
        "after": "unknown",
    }
    assert forecast.pending_on == {"chain@c": ("chain@b",), "after": ("chain",)}
    assert result.map_items["chain"] == {"a": "hit", "b": "miss", "c": "hit"}


def test_plan_rejects_unknown_forced_map_items_like_run(tmp_path: Path) -> None:
    """教训 plan_force: force 项名打错时预告必须与 run 同样报错,
    静默忽略会让成本闸门看似通过、实跑才失败。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    dag, _ = _build_scan_dag(tmp_path, items)
    dag.run()

    with pytest.raises(ValueError, match="Unknown forced map items: chain@nope"):
        dag.plan(force=["chain@nope"])


def test_plan_wraps_carry_fn_failure_with_scan_item_context_like_run(tmp_path: Path) -> None:
    """教训 plan_carry_error: 同一个 carry_fn 故障在 plan 与 run 必须
    给出同形态的节点+项上下文,不许裸抛原始异常。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    dag, _ = _build_scan_dag(tmp_path, items)
    dag.run()

    def broken_carry(artifact: dict[str, Any]) -> Any:
        raise ValueError(f"bad ledger {artifact['id']}")

    rebuilt, _ = _build_scan_dag(tmp_path, items, carry_fn=broken_carry)
    with pytest.raises(RuntimeError, match="Scan node 'chain' failed item 'a'"):
        rebuilt.plan()
    with pytest.raises(RuntimeError, match="Scan node 'chain' failed item 'a'"):
        rebuilt.run()


def test_scan_rebuilds_aggregate_records_items_and_preserves_gc_references(tmp_path: Path) -> None:
    """教训 scan_sidecar: 聚合不缓存，但逐项键必须留在 sidecar 防止 gc 误删。"""
    items = [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "c", "value": 3}]
    dag, _ = _build_scan_dag(tmp_path, items)

    result = dag.run()
    metadata = json.loads(
        (tmp_path / "artifacts" / "runs" / result.run_id / "chain.json.meta.json").read_text(
            encoding="utf-8"
        )
    )

    assert result.artifacts["chain"]["order"] == ["a", "b", "c"]
    assert result.map_items["chain"] == {"a": "miss", "b": "miss", "c": "miss"}
    assert len(metadata["cache_key"]) == 3
    assert dag.gc(keep_last=1) == 0


def test_scan_registration_keeps_the_loop_guard_active(tmp_path: Path) -> None:
    """教训 scan_guard: 新原语不能成为节点内裸循环调用的守卫绕行。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        del inputs, ctx
        return {"items": [{"id": "a"}]}

    with pytest.raises(ValueError, match="Raw LLM calls inside loops"):

        @dag.scan("chain", items_from=("source", "items"), key_fn=lambda item: item["id"])
        def chain(
            item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any
        ) -> dict[str, Any]:
            del item, carry, inputs
            for _ in range(1):
                ctx.call("not allowed")
            return {}


def test_scan_resolves_a_dotted_runtime_list_path(tmp_path: Path) -> None:
    """教训 scan_dot_path: scan 与 map 必须共用点路径清单解析。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"nested": {"items": [{"id": "a", "value": 1}, {"id": "b", "value": 2}]}}

    @dag.scan("chain", items_from=("source", "nested.items"), key_fn=lambda item: item["id"])
    def chain(item: dict[str, Any], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del carry, inputs, ctx
        return {"value": item["value"]}

    result = dag.run()

    assert result.map_items["chain"] == {"a": "miss", "b": "miss"}


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


def test_libs_hash_ignores_comment_and_docstring_edits(tmp_path: Path) -> None:
    """教训 libs_granularity: lib 注释/docstring 修订不得让全流水线换族重算。"""
    lib = tmp_path / "lib"
    lib.mkdir()
    module = lib / "util.py"
    module.write_text(
        '"""旧模块说明。"""\n\n\ndef helper():\n    # 旧注释\n    return 1\n', encoding="utf-8"
    )
    config = KigumiConfig(project_root=tmp_path, source_dirs=["lib"])
    transport = FakeTransport(repeat(Response("out", {"total_tokens": 1}, "stop")))
    dag = Dag(config, LLMCaller(transport, tmp_path / "llm"))

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"value": 1}

    assert dag.run().cache_hits == []

    module.write_text(
        '"""新模块说明。"""\n\n\ndef helper():\n    # 新注释\n    return 1\n', encoding="utf-8"
    )
    assert dag.run().cache_hits == ["work"]

    module.write_text(
        '"""新模块说明。"""\n\n\ndef helper():\n    # 新注释\n    return 2\n', encoding="utf-8"
    )
    assert dag.run().cache_hits == []


def test_libs_hash_tolerates_broken_syntax_by_hashing_raw_text(tmp_path: Path) -> None:
    """教训 libs_broken_file: 中途编辑的残破文件不该让只读 plan 崩溃。"""
    lib = tmp_path / "lib"
    lib.mkdir()
    module = lib / "util.py"
    module.write_text("def helper():\n    return 1\n", encoding="utf-8")
    config = KigumiConfig(project_root=tmp_path, source_dirs=["lib"])
    transport = FakeTransport(repeat(Response("out", {"total_tokens": 1}, "stop")))
    dag = Dag(config, LLMCaller(transport, tmp_path / "llm"))

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"value": 1}

    dag.run()
    module.write_text("def helper(:\n", encoding="utf-8")

    assert dag.plan().nodes["work"] == "miss"

    module.write_text("def helper():\n    return 1\n", encoding="utf-8")
    assert dag.plan().nodes["work"] == "hit"
