from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests._dag_helpers import _make_dag


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


def test_registration_hard_fails_dynamic_callables_and_their_aliases(tmp_path: Path) -> None:
    """注册入口必须执行动态解析硬切，而不是只依赖独立 AST 测试。"""
    dag = _make_dag(tmp_path)

    with pytest.raises(ValueError) as caught:

        @dag.node("dynamic-callables")
        def dynamic_callables(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            del inputs, ctx
            method_name = "read_text"
            globals_alias = globals
            locals_alias = locals
            getattr_alias = getattr
            eval_alias = eval
            exec_alias = exec
            import_alias = __import__
            return {
                "globals": globals(),
                "globals_alias": globals_alias(),
                "locals": locals(),
                "locals_alias": locals_alias(),
                "getattr": getattr(Path, method_name),
                "getattr_alias": getattr_alias(Path, "read_text"),
                "eval": eval("1"),
                "eval_alias": eval_alias("1"),
                "exec": exec("pass"),
                "exec_alias": exec_alias("pass"),
                "import": __import__("pathlib"),
                "import_alias": import_alias("pathlib"),
            }

    message = str(caught.value)
    assert "Raw file reads are not allowed" in message
    for snippet in (
        "globals()",
        "globals_alias()",
        "locals()",
        "locals_alias()",
        "getattr(Path, method_name)",
        'getattr_alias(Path, "read_text")',
        'eval("1")',
        'eval_alias("1")',
        'exec("pass")',
        'exec_alias("pass")',
        '__import__("pathlib")',
        'import_alias("pathlib")',
    ):
        assert snippet in message


def test_registration_hard_fails_raw_io_fact_propagation_escapes(tmp_path: Path) -> None:
    """注册环必须与 enforce 一样收口容器、星号、重绑定和定义期表达式。"""
    dag = _make_dag(tmp_path)

    with pytest.raises(ValueError, match="Raw file reads are not allowed"):

        @dag.node("indexed-container")
        def indexed_container(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            readers = [open]
            return {"text": readers[0]("secret.txt")}

    with pytest.raises(ValueError, match="Raw file reads are not allowed"):

        @dag.node("builtin-import-chain")
        def builtin_import_chain(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            import builtins

            importer = builtins.__dict__["__import__"]
            builtins_module = importer("builtins")
            reader = builtins_module.__dict__["open"]
            return {"text": reader("secret.txt")}

    with pytest.raises(ValueError, match="Raw file reads are not allowed"):

        @dag.node("helper-star")
        def helper_star(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            def helper(reader: Any) -> str:
                return reader("secret.txt")

            readers = [open]
            return {"text": helper(*readers)}

    with pytest.raises(ValueError, match="Raw file reads are not allowed"):

        @dag.node("map-star")
        def map_star(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"values": list(map(*[open], inputs))}

    with pytest.raises(ValueError, match="Raw file reads are not allowed"):

        @dag.node("rebound-context")
        def rebound_context(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            ctx = Path("secret.txt")
            return {"text": ctx.read_text()}

    with pytest.raises(ValueError, match="Raw file reads are not allowed"):

        @dag.node("definition-expressions")
        def definition_expressions(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            @Path("decorator-secret.txt").read_text()
            def helper(value: Path("annotation-secret.txt").read_text()) -> str:
                return value

            return {"value": ctx.read_text("declared.txt")}


def test_registration_hard_fails_nested_classes_and_reachable_instance_method_alias(
    tmp_path: Path,
) -> None:
    """nested class 与其可达的实例方法别名都必须在注册期被拒绝。"""
    dag = _make_dag(tmp_path)

    with pytest.raises(ValueError) as class_error:

        @dag.node("nested-class")
        def nested_class(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            del inputs, ctx

            class Reader:
                def read(self) -> str:
                    return "controlled"

            return {"reader": Reader}

    assert "class Reader:" in str(class_error.value)

    with pytest.raises(ValueError) as alias_error:

        @dag.node("nested-method-alias")
        def nested_method_alias(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            del inputs, ctx

            class Reader:
                def read(self) -> str:
                    return Path("secret.txt").read_text()

            reader = Reader()
            method = reader.read
            return {"text": method()}

    alias_message = str(alias_error.value)
    assert "class Reader:" in alias_message
    assert 'return Path("secret.txt").read_text()' in alias_message


def test_registration_allows_context_bound_reads(tmp_path: Path) -> None:
    """ctx.read_text/read_bytes remain the approved declared-input boundary."""
    dag = _make_dag(tmp_path)

    @dag.node("context-reads")
    def context_reads(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        return {
            "text": ctx.read_text("input.txt"),
            "bytes": ctx.read_bytes("input.bin"),
        }

    assert context_reads.__name__ == "context_reads"


def test_registration_rejects_every_direct_context_getattr_form(tmp_path: Path) -> None:
    """getattr 的结果即使不立即调用，也不能绕过节点边界。"""
    dag = _make_dag(tmp_path)

    with pytest.raises(ValueError, match="Raw file reads are not allowed"):

        @dag.node("dynamic-context-reader-assignment")
        def dynamic_context_reader_assignment(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            del inputs
            reader = getattr(ctx, "read_text")  # noqa: B009 -- hard-cut regression probe.
            return {"reader": reader}

    with pytest.raises(ValueError, match="Raw file reads are not allowed"):

        @dag.node("dynamic-context-reader-return")
        def dynamic_context_reader_return(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            del inputs
            return {"reader": getattr(ctx, "read_text")}  # noqa: B009

    with pytest.raises(ValueError, match="Raw file reads are not allowed"):

        @dag.node("dynamic-context-params")
        def dynamic_context_params(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            del inputs
            return {"params": getattr(ctx, "params")}  # noqa: B009

    with pytest.raises(ValueError, match="Raw file reads are not allowed"):

        @dag.node("dynamic-context-reader-call")
        def dynamic_context_reader_call(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            del inputs
            return {"text": getattr(ctx, "read_text")("input.txt")}  # noqa: B009


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
