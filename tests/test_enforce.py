from __future__ import annotations

from pathlib import Path

from kigumi.enforce import (
    RawIOFinding,
    check_paths,
    check_raw_io_node_paths,
    check_raw_io_source,
    check_source,
)


def test_loop_calls_are_findings_but_non_loop_calls_are_not() -> None:
    """教训 raw_llm_loop: 循环内裸调用会绕过缓存、修复与预算边界。"""
    source = """
client.call([])
for item in items:
    client.call([])
    client.llm("prompt")
"""

    findings = check_source(source, Path("sample.py"))

    assert [(finding.lineno, finding.snippet) for finding in findings] == [
        (4, "client.call([])"),
        (5, 'client.llm("prompt")'),
    ]


def test_waiver_reason_is_visible_and_empty_waiver_remains_violation() -> None:
    """教训 waiver_visibility: 例外必须有理由，空豁免不能成为静默后门。"""
    source = (
        "\n"
        "for item in items:\n"
        "    client.call([])  # kigumi: raw-llm-ok fixture replay\n"
        '    client.llm("x")  # kigumi: raw-llm-ok' + "   \n"
    )

    findings = check_source(source, Path("sample.py"))

    assert findings[0].waived is True
    assert findings[0].waiver_reason == "fixture replay"
    assert findings[1].waived is False
    assert findings[1].waiver_reason == "豁免必须写理由"


def test_helpers_and_async_loops_are_scanned_recursively(tmp_path: Path) -> None:
    """教训 helper_evasion: 把循环装进 helper 不能让裸调用从扫描中消失。"""
    source = """
def helper(items, client):
    for item in items:
        client.call([])

async def run(items, client):
    async for item in items:
        client.llm("x")
"""
    path = tmp_path / "nodes" / "helper.py"
    path.parent.mkdir()
    path.write_text(source, encoding="utf-8")

    findings = check_paths([tmp_path / "nodes", tmp_path / "missing"])

    assert [finding.lineno for finding in findings] == [4, 8]


def test_comprehensions_count_as_loops() -> None:
    """教训 comprehension_evasion: 推导式是循环——最典型的守卫绕行写法必须被扫到。"""
    source = """
def batch(prompts, ctx):
    drafts = [ctx.llm(p) for p in prompts]
    lookup = {p: ctx.call(p) for p in prompts}
    single = ctx.llm(prompts[0])
    return drafts, lookup, single
"""
    findings = check_source(source, Path("nodes/batch.py"))

    assert [finding.lineno for finding in findings] == [3, 4]


def test_raw_io_finds_only_direct_node_body_reads_and_honors_its_own_waiver() -> None:
    """教训 stale_file_cache: 节点体绕过 ctx 会让未声明输入复用陈旧缓存。"""
    source = """
def node(inputs, ctx):
    def helper(path):
        return path.read_text(encoding="utf-8")
    with open("input.txt", encoding="utf-8") as handle:
        return handle.read()
    with (root / "raw.txt").open(encoding="utf-8") as handle:
        return handle.read()
    return Path("fixture.txt").read_bytes()  # kigumi: raw-io-ok fixture fixture
    return ctx.read_text("input.txt")
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    assert [(finding.lineno, finding.waived) for finding in findings] == [
        (5, False),
        (7, False),
        (9, True),
    ]
    assert findings[2].waiver_reason == "fixture fixture"


def test_raw_io_context_exempts_only_controlled_read_methods() -> None:
    """教训 context_method_gap: 上下文不存在的 open 不能成为 raw-I/O 后门。"""
    source = """
def node(inputs, context):
    context.open("secret.txt").read()
    context.read_text("declared.txt")
    context.read_bytes("declared.bin")
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"), context_name="context")

    assert [(finding.lineno, finding.waived) for finding in findings] == [(3, False)]


def test_raw_io_snapshot_preserves_type_location_order_and_separate_waivers() -> None:
    """快照锁定 finding 类型、源码位置、排序和 raw-io/raw-llm 豁免边界。"""
    source = """
def node(inputs, ctx):
    def helper():
        return Path("helper.txt").read_text()  # kigumi: raw-io-ok helper input

    def unreachable():
        return Path("unreachable.txt").read_text()  # kigumi: raw-llm-ok not raw io

    return (
        open("direct.txt").read(),  # kigumi: raw-io-ok direct input
        helper(),
        ctx.read_text("declared.txt"),
    )
"""

    findings = check_raw_io_source(source, Path("nodes/snapshot.py"))

    assert all(isinstance(finding, RawIOFinding) for finding in findings)
    assert [
        (finding.lineno, finding.snippet, finding.waived, finding.waiver_reason)
        for finding in findings
    ] == [
        (
            4,
            'return Path("helper.txt").read_text()  # kigumi: raw-io-ok helper input',
            True,
            "helper input",
        ),
        (
            10,
            'open("direct.txt").read(),  # kigumi: raw-io-ok direct input',
            True,
            "direct input",
        ),
    ]


def test_raw_io_snapshot_keeps_multiple_calls_on_one_line() -> None:
    """同一行的多个调用仍分别报告，不能被 finding 去重吞掉。"""
    source = """
def node(inputs, ctx):
    return Path("first.txt").read_text(), Path("second.txt").read_bytes()
"""

    findings = check_raw_io_source(source, Path("nodes/same-line.py"))

    assert len(findings) == 2
    assert [(finding.lineno, finding.snippet) for finding in findings] == [
        (3, 'return Path("first.txt").read_text(), Path("second.txt").read_bytes()'),
        (3, 'return Path("first.txt").read_text(), Path("second.txt").read_bytes()'),
    ]


def test_raw_io_snapshot_keeps_raw_llm_waiver_separate() -> None:
    """raw-llm-ok 不能改变 raw-I/O finding 的未豁免状态。"""
    source = """
def node(inputs, ctx):
    return Path("secret.txt").read_text()  # kigumi: raw-llm-ok model fixture
"""

    findings = check_raw_io_source(source, Path("nodes/dual-waiver.py"))

    assert [(finding.lineno, finding.waived, finding.waiver_reason) for finding in findings] == [
        (3, False, None)
    ]


def test_raw_io_recurses_into_reachable_nested_helpers_and_lambdas() -> None:
    """可达的局部 helper/lambda 不能把未声明读取藏在节点边界之后。"""
    source = """
def node(inputs, ctx):
    def helper():
        return Path("nested.txt").read_text()

    reader = lambda: Path("lambda.txt").read_bytes()

    def unused():
        return Path("unreachable.txt").read_text()

    return {"helper": helper(), "lambda": reader()}
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    assert [(finding.lineno, finding.snippet) for finding in findings] == [
        (4, 'return Path("nested.txt").read_text()'),
        (6, 'reader = lambda: Path("lambda.txt").read_bytes()'),
    ]


def test_raw_io_scans_reachable_nested_class_methods_but_skips_unreachable_helpers() -> None:
    """可达 class method 必须 fail-closed，但未调用的 helper 不得误报。"""
    source = """
def node(inputs, ctx):
    class Reader:
        def read(self):
            return Path("class-secret.txt").read_text()

    def unused():
        return Path("unused-secret.txt").read_text()

    return Reader().read()
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    assert [(finding.lineno, finding.snippet) for finding in findings] == [
        (3, "class Reader:"),
        (5, 'return Path("class-secret.txt").read_text()'),
    ]
    assert "unused-secret.txt" not in "\n".join(finding.snippet for finding in findings)


def test_raw_io_rejects_raw_callable_aliases_and_callback_use() -> None:
    """open/read aliases and callbacks must not create an AST blind spot."""
    source = """
def node(inputs, ctx):
    open_alias = open
    text_alias = Path("text.txt").read_text
    return (
        open_alias("secret.txt").read(),
        text_alias(),
        list(map(open, inputs)),
    )
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    assert [finding.lineno for finding in findings] == [6, 7, 8]


def test_raw_io_propagates_callable_facts_through_reachable_helper_arguments() -> None:
    """helper 的实际 callable 参数必须保留 raw、dynamic 与 opaque 事实。"""
    source = """
def node(inputs, ctx):
    def raw_helper(reader):
        return reader("raw-secret.txt").read()

    def dynamic_helper(reader):
        return reader("dynamic-secret.txt")

    def opaque_helper(reader):
        return reader("opaque-secret.txt")

    raw_result = raw_helper(open)
    dynamic_result = dynamic_helper(globals)
    opaque_result = opaque_helper(globals()["open"])
    return raw_result, dynamic_result, opaque_result
"""

    findings = check_raw_io_source(source, Path("nodes/parameter-facts.py"))

    assert [(finding.lineno, finding.snippet) for finding in findings] == [
        (4, 'return reader("raw-secret.txt").read()'),
        (7, 'return reader("dynamic-secret.txt")'),
        (10, 'return reader("opaque-secret.txt")'),
        (13, "dynamic_result = dynamic_helper(globals)"),
        (14, 'opaque_result = opaque_helper(globals()["open"])'),
    ]


def test_raw_io_propagates_tuple_for_and_named_open_aliases() -> None:
    """解构、for 绑定和 named expression 都不能隐藏 open alias。"""
    source = """
def node(inputs, ctx):
    tuple_opener, tuple_reader = (open, Path("tuple-reader.txt").read_text)
    tuple_result = tuple_opener("tuple-secret.txt").read(), tuple_reader()
    for loop_opener in (open,):
        loop_result = loop_opener("loop-secret.txt").read()
    named_result = (named_opener := open)("named-secret.txt").read()
    return tuple_result, loop_result, named_result
"""

    findings = check_raw_io_source(source, Path("nodes/compound-aliases.py"))

    assert [finding.lineno for finding in findings] == [4, 4, 6, 7]


def test_raw_io_orders_same_line_findings_by_source_column() -> None:
    """同一行中后产生的 helper finding 也必须按源码列号稳定排序。"""
    source = """
def node(inputs, ctx):
    return (lambda: Path("early-secret.txt").read_text())(), globals()  # kigumi: raw-io-ok fixture
"""

    findings = check_raw_io_source(source, Path("nodes/column-order.py"))

    assert [finding.waived for finding in findings] == [True, False]
    assert [finding._col_offset for finding in findings] == sorted(
        finding._col_offset for finding in findings
    )


def test_raw_io_rejects_opaque_dynamic_callables_and_scans_executed_defaults() -> None:
    """动态 callable 采用硬切；helper 默认值在定义时执行，必须被扫描。"""
    source = """
def node(inputs, ctx):
    dynamic = globals()["open"]

    def helper(value=Path("default-secret.txt").read_text()):
        return value

    def unused():
        return Path("unused-secret.txt").read_text()

    return dynamic("secret.txt").read()
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    assert [finding.lineno for finding in findings] == [3, 5, 11]
    assert findings[2].snippet == 'return dynamic("secret.txt").read()'
    assert "unused-secret.txt" not in "\n".join(finding.snippet for finding in findings)


def test_raw_io_rejects_direct_globals_callable_execution() -> None:
    """globals() 的直接调用和其结果被执行都必须进入硬切边界。"""
    source = """
def node(inputs, ctx):
    return globals()["open"]("secret.txt").read()
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    assert [(finding.lineno, finding.snippet, finding.waived) for finding in findings] == [
        (3, 'return globals()["open"]("secret.txt").read()', False)
    ]


def test_raw_io_rejects_imported_and_indirect_raw_callable_aliases() -> None:
    """危险 callable 的导入、赋值和动态取值都不能绕过 raw-I/O 守卫。"""
    source = """
def node(inputs, ctx):
    from builtins import open as opener
    open_alias = open
    getter = getattr
    first = opener("one.txt").read()
    second = open_alias("two.txt").read()
    third = locals()["opener"]("three.txt").read()
    fourth = getter(Path, "read_text")("four.txt")
    return first, second, third, fourth
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    assert [finding.lineno for finding in findings] == [5, 6, 7, 8, 9]
    assert all(not finding.waived for finding in findings)


def test_raw_io_rejects_direct_dynamic_execution_and_builtin_lookup() -> None:
    """直接动态执行或 builtin lookup 都必须硬失败，而非只拦截最终读文件。"""
    source = """
def node(inputs, ctx):
    exec("value = open('secret.txt').read()", globals())
    value = getattr(builtins, name)("secret.txt")
    globals()
    locals()
    exec(code)
    eval(code)
    __import__("pathlib")
    return value
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    snippets = {finding.snippet for finding in findings}
    assert {
        "exec(\"value = open('secret.txt').read()\", globals())",
        'value = getattr(builtins, name)("secret.txt")',
        "globals()",
        "locals()",
        "exec(code)",
        "eval(code)",
        '__import__("pathlib")',
    } <= snippets
    assert all(not finding.waived for finding in findings)


def test_raw_io_rejects_bare_dynamic_references_in_saved_and_returned_values() -> None:
    """动态原语裸引用的保存/返回路径也必须在使用点硬失败。"""
    source = """
import builtins as bi
from builtins import globals as namespace, getattr as lookup

def node(inputs, ctx):
    saved_name = globals
    saved_attribute = bi.globals
    saved_import = namespace
    return lookup
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    assert [(finding.lineno, finding.snippet, finding.waived) for finding in findings] == [
        (6, "saved_name = globals", False),
        (7, "saved_attribute = bi.globals", False),
        (8, "saved_import = namespace", False),
        (9, "return lookup", False),
    ]


def test_raw_io_reports_dynamic_call_targets_once_and_keeps_ordinary_attributes() -> None:
    """调用目标只产生一个 finding；普通对象属性与受控 ctx 入口不受影响。"""
    source = """
def node(inputs, ctx):
    import builtins as bi
    from builtins import locals as local_namespace
    direct = globals()
    imported = local_namespace()
    qualified = bi.locals()
    ordinary = settings.globals
    return ordinary, ctx.read_text
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    assert [(finding.lineno, finding.snippet, finding.waived) for finding in findings] == [
        (5, "direct = globals()", False),
        (6, "imported = local_namespace()", False),
        (7, "qualified = bi.locals()", False),
    ]


def test_raw_io_dynamic_call_target_wins_over_raw_getattr_shape() -> None:
    """直接执行动态 getattr 结果仍不可豁免；绑定后才沿用 raw alias 语义。"""
    source = """
def node(inputs, ctx):
    direct = getattr(Path, "read_text")("secret.txt")  # kigumi: raw-io-ok not enough
    reader = getattr(Path, "read_text")
    bound = reader("declared.txt")  # kigumi: raw-io-ok fixture input
    return direct, bound
"""

    findings = check_raw_io_source(source, Path("nodes/classifier.py"))

    assert [(finding.lineno, finding.waived, finding.waiver_reason) for finding in findings] == [
        (3, False, None),
        (4, False, None),
        (5, True, "fixture input"),
    ]


def test_raw_io_rejects_instance_method_aliases_and_nested_classes() -> None:
    """实例方法别名必须可达解析；nested class 即使无 raw read 也硬切。"""
    source = """
def node(inputs, ctx):
    class Reader:
        def read(self):
            return Path("class-secret.txt").read_text()

    reader = Reader()
    method = reader.read
    return method()
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    assert {
        "class Reader:",
        'return Path("class-secret.txt").read_text()',
    } <= {finding.snippet for finding in findings}
    assert all(not finding.waived for finding in findings)


def test_raw_io_rejects_nested_class_without_raw_read() -> None:
    """nested class 本身就是结构违规，不依赖其方法是否读取文件。"""
    source = """
def node(inputs, ctx):
    class Reader:
        def read(self):
            return "controlled"
    return Reader
"""

    findings = check_raw_io_source(source, Path("nodes/sample.py"))

    assert [(finding.lineno, finding.snippet, finding.waived) for finding in findings] == [
        (3, "class Reader:", False)
    ]


def test_raw_io_path_guard_checks_only_decorated_top_level_node_bodies(tmp_path: Path) -> None:
    """教训 raw_io_scope: 项目级守卫不能把合法 helper 读取误判成节点违规。"""
    source = tmp_path / "nodes"
    source.mkdir()
    path = source / "pipeline.py"
    path.write_text(
        """
def helper():
    return Path("fixture.txt").read_text()

@pipeline.foreach("items", [])
def mapped(item, inputs, context):
    def nested():
        return Path("nested.txt").read_text()
    return Path("input.txt").read_text()

@dag.node("waived")
def waived(inputs, ctx):
    return open("fixture.txt").read()  # kigumi: raw-io-ok fixture input

@decorator
def ordinary():
    return Path("ordinary.txt").read_text()
""",
        encoding="utf-8",
    )

    findings = check_raw_io_node_paths([source])

    assert [(finding.lineno, finding.waived, finding.waiver_reason) for finding in findings] == [
        (9, False, None),
        (13, True, "fixture input"),
    ]


def test_raw_io_path_guard_treats_agent_builder_as_a_node_body(tmp_path: Path) -> None:
    source = tmp_path / "nodes" / "agent.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        """
@dag.agent("writer", adapter=adapter, config=config)
def writer(inputs, ctx):
    return Path("secret.txt").read_text()
""",
        encoding="utf-8",
    )

    findings = check_raw_io_node_paths([source.parent])

    assert [(finding.lineno, finding.waived) for finding in findings] == [(4, False)]
