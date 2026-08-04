from __future__ import annotations

from pathlib import Path

from kigumi.enforce import (
    RawIOFinding,
    check_paths,
    check_raw_io_node_paths,
    check_raw_io_node_source,
    check_raw_io_source,
    check_source,
    raw_io_waiver_reasons,
    waiver_reasons,
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


def test_waivers_require_source_comments_and_exact_tokens() -> None:
    """字符串中的伪注释和 token 前缀不能制造跨类或隐式豁免。"""
    source = """
for item in items:
    fake = "# kigumi: raw-llm-ok string content"
    client.call([])  # kigumi: raw-llm-okhidden not a token
    client.llm("x")  # kigumi: raw-llm-ok loop fixture

def node(inputs, ctx):
    fake = "# kigumi: raw-io-ok string content"
    first = Path("first.txt").read_text() if "# kigumi: raw-io-ok string content" else ""
    second = Path("second.txt").read_text()  # kigumi: raw-io-ok file fixture
    return first, second
"""

    llm_findings = check_source(source, Path("nodes/waiver-boundary.py"))
    raw_io_findings = check_raw_io_source(source, Path("nodes/waiver-boundary.py"))

    assert [(finding.waived, finding.waiver_reason) for finding in llm_findings] == [
        (False, None),
        (True, "loop fixture"),
    ]
    assert [(finding.waived, finding.waiver_reason) for finding in raw_io_findings] == [
        (False, None),
        (True, "file fixture"),
    ]
    assert waiver_reasons(source) == ["loop fixture"]
    assert raw_io_waiver_reasons(source) == ["file fixture"]


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


def test_raw_io_reaches_inline_callable_in_a_reachable_helper_default() -> None:
    """inline lambda 与可达 helper 的默认 callable 都必须进入扫描边界。"""
    source = """
def node(inputs, ctx):
    reader = lambda: Path("inline-secret.txt").read_text()

    def helper(reader=lambda: Path("default-inline-secret.txt").read_text()):
        return reader()

    return reader(), helper()
"""

    findings = check_raw_io_source(source, Path("nodes/inline-callables.py"))

    assert [(finding.lineno, finding.snippet) for finding in findings] == [
        (3, 'reader = lambda: Path("inline-secret.txt").read_text()'),
        (5, 'def helper(reader=lambda: Path("default-inline-secret.txt").read_text()):'),
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


def test_raw_io_propagates_callable_facts_through_helper_defaults() -> None:
    """helper 的默认 callable 也必须传播 raw、dynamic 与 opaque 事实。"""
    source = """
def node(inputs, ctx):
    def raw_helper(reader=open):
        return reader("raw-default.txt").read()

    def dynamic_helper(reader=globals):
        return reader("dynamic-default.txt")

    def opaque_helper(reader=globals()["open"]):
        return reader("opaque-default.txt")

    return raw_helper(), dynamic_helper(), opaque_helper()
"""

    findings = check_raw_io_source(source, Path("nodes/default-facts.py"))

    snippets = [finding.snippet for finding in findings]
    assert any('return reader("raw-default.txt").read()' in snippet for snippet in snippets)
    assert any('return reader("dynamic-default.txt")' in snippet for snippet in snippets)
    assert any('return reader("opaque-default.txt")' in snippet for snippet in snippets)


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


def test_raw_io_propagates_indirect_sequence_aliases_through_destructuring() -> None:
    """先绑定到 tuple/list 的 alias 也不能绕过解构后的 raw-I/O 检查。"""
    source = """
def node(inputs, ctx):
    tuple_pair = (open,)
    tuple_reader, = tuple_pair
    list_pair = [open]
    list_reader, = list_pair
    return (
        tuple_reader("tuple-alias-secret.txt").read(),
        list_reader("list-alias-secret.txt").read(),
    )
"""

    findings = check_raw_io_source(source, Path("nodes/indirect-aliases.py"))

    assert [finding.lineno for finding in findings] == [8, 9]


def test_raw_io_propagates_indexed_container_aliases_and_starred_helper_arguments() -> None:
    """容器下标和静态星号参数都不能隐藏 raw callable。"""
    source = """
def node(inputs, ctx):
    readers = [open]
    reader = readers[0]

    def helper(reader):
        return reader("helper-secret.txt").read()

    return reader("alias-secret.txt").read(), helper(*[open])
"""

    findings = check_raw_io_source(source, Path("nodes/container-callables.py"))

    assert [finding.lineno for finding in findings] == [7, 9]


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


def test_raw_io_rejects_builtins_dict_callable_lookup_but_not_arbitrary_subscripts() -> None:
    """builtins.__dict__ 的 callable lookup 是 opaque；普通动态下标仍保持未知。"""
    source = """
import builtins

def node(inputs, ctx):
    opaque = builtins.__dict__["open"]
    return opaque("secret.txt").read(), readers[index]("not-proven-raw")
"""

    findings = check_raw_io_source(source, Path("nodes/builtin-dict.py"))

    assert [(finding.lineno, finding.snippet, finding.waived) for finding in findings] == [
        (5, 'opaque = builtins.__dict__["open"]', False),
        (6, 'return opaque("secret.txt").read(), readers[index]("not-proven-raw")', False),
    ]


def test_raw_io_rejects_builtin_import_lookup_chained_to_open() -> None:
    """__import__ 的 builtins 字典链不能绕过动态 import 与 raw-I/O 硬切。"""
    source = """
import builtins

def node(inputs, ctx):
    importer = builtins.__dict__["__import__"]
    builtins_module = importer("builtins")
    reader = builtins_module.__dict__["open"]
    return reader("secret.txt").read()
"""

    findings = check_raw_io_source(source, Path("nodes/builtin-import-chain.py"))

    assert [finding.lineno for finding in findings] == [7, 8]
    snippets = {finding.snippet for finding in findings}
    assert {
        'reader = builtins_module.__dict__["open"]',
        'return reader("secret.txt").read()',
    } <= snippets
    assert all(not finding.waived for finding in findings)


def test_raw_io_rejects_direct_static_container_callable_index() -> None:
    """静态 callable 容器的直接下标调用必须传播 raw-I/O 事实。"""
    source = """
def node(inputs, ctx):
    readers = [open]
    return readers[0]("secret.txt")
"""

    findings = check_raw_io_source(source, Path("nodes/static-container-call.py"))

    assert [finding.snippet for finding in findings] == [
        'return readers[0]("secret.txt")',
    ]
    assert findings[0].waived is False


def test_raw_io_hard_dynamic_and_opaque_findings_ignore_raw_io_waivers() -> None:
    """动态/opaque callable 的结构 finding 不能被旧缓存理由放行。"""
    source = """
import builtins

def node(inputs, ctx):
    opaque = builtins.__dict__["open"]  # kigumi: raw-io-ok old cache
    return opaque("secret.txt")  # kigumi: raw-io-ok old cache
"""

    findings = check_raw_io_source(source, Path("nodes/opaque-waiver.py"))

    assert [(finding.lineno, finding.waived, finding.waiver_reason) for finding in findings] == [
        (5, False, None),
        (6, False, None),
    ]


def test_raw_io_keeps_unknown_data_subscripts_out_of_scope() -> None:
    """未知数据下标没有可证明 callable 事实时不能扩大误报边界。"""
    source = """
def node(inputs, ctx):
    readers = inputs["readers"]
    return readers[index]
"""

    assert check_raw_io_source(source, Path("nodes/unknown-data-subscript.py")) == []


def test_raw_io_propagates_static_container_through_helper_star_args() -> None:
    """helper(*readers) 展开的 raw callable 不能丢失参数事实。"""
    source = """
def node(inputs, ctx):
    def helper(reader):
        return reader("secret.txt")

    readers = [open]
    return helper(*readers)
"""

    findings = check_raw_io_source(source, Path("nodes/helper-star-container.py"))

    assert [finding.snippet for finding in findings] == [
        'return reader("secret.txt")',
    ]
    assert findings[0].waived is False


def test_raw_io_rejects_starred_static_map_callback() -> None:
    """map(*[open], ...) 的静态 callback 展开不能绕过 raw-I/O 检查。"""
    source = """
def node(inputs, ctx):
    return list(map(*[open], inputs))
"""

    findings = check_raw_io_source(source, Path("nodes/starred-map-callback.py"))

    assert [finding.snippet for finding in findings] == [
        "return list(map(*[open], inputs))",
    ]
    assert findings[0].waived is False


def test_raw_io_reaches_lambda_from_static_container_index_call() -> None:
    """静态容器下标取得的 lambda 函数体必须进入可达扫描。"""
    source = """
def node(inputs, ctx):
    readers = [lambda: Path("lambda-secret.txt").read_text()]
    return readers[0]()
"""

    findings = check_raw_io_source(source, Path("nodes/container-lambda.py"))

    assert [finding.snippet for finding in findings] == [
        'readers = [lambda: Path("lambda-secret.txt").read_text()]',
    ]
    assert findings[0].waived is False


def test_raw_io_rejects_context_read_after_context_rebinding() -> None:
    """ctx 被重新绑定为 Path 后，read_text 不再是受控上下文读取。"""
    source = """
def node(inputs, ctx):
    ctx = Path("secret.txt")
    return ctx.read_text()
"""

    findings = check_raw_io_source(source, Path("nodes/rebound-context.py"))

    assert [finding.snippet for finding in findings] == [
        "return ctx.read_text()",
    ]
    assert findings[0].waived is False


def test_raw_io_scans_nested_helper_decorators_and_annotations() -> None:
    """nested helper 的 decorator/annotation 在定义时执行，不能藏 raw read。"""
    source = """
def node(inputs, ctx):
    @Path("decorator-secret.txt").read_text()
    def helper(value: Path("annotation-secret.txt").read_text()):
        return value

    return ctx.read_text("declared.txt")
"""

    findings = check_raw_io_source(source, Path("nodes/helper-definition-expressions.py"))

    assert {
        '@Path("decorator-secret.txt").read_text()',
        'def helper(value: Path("annotation-secret.txt").read_text()):',
    } <= {finding.snippet for finding in findings}
    assert all(not finding.waived for finding in findings)


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

    assert [finding.lineno for finding in findings] == [5, 6, 7, 8, 9, 10]
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
    """getattr 及其派生 callable 都是动态硬切，raw-io-ok 不能豁免。"""
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
        (5, False, None),
        (6, False, None),
        (6, False, None),
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


def test_raw_io_path_guard_propagates_static_container_callable_facts(tmp_path: Path) -> None:
    """项目级装饰器筛选也必须沿用静态容器 callable 事实。"""
    source = tmp_path / "nodes" / "indexed.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        """
@dag.node("indexed")
def indexed(inputs, ctx):
    readers = [open]
    return readers[0]("secret.txt")
""",
        encoding="utf-8",
    )

    findings = check_raw_io_node_paths([source.parent])

    assert [(finding.lineno, finding.waived) for finding in findings] == [(5, False)]


def test_raw_io_path_guard_scans_top_level_definition_expressions() -> None:
    """顶层节点的 decorator/default/annotation 也在函数定义时执行。"""
    source = """
@dag.node("top")
@open("decorator-open-secret.txt")
@Path("decorator-secret.txt").read_text()
def top(
    reader=open,
    opened=open("default-open-secret.txt"),
    value=Path("default-secret.txt").read_text(),
    typed: Path("annotation-secret.txt").read_text() = None,
    opened_type: open("annotation-open-secret.txt") = None,
) -> Path("return-secret.txt").read_text():
    return reader("default-open-secret.txt").read()
"""

    findings = check_raw_io_node_source(source, Path("nodes/top-level-definition.py"))

    assert {
        '@open("decorator-open-secret.txt")',
        '@Path("decorator-secret.txt").read_text()',
        'opened=open("default-open-secret.txt"),',
        'value=Path("default-secret.txt").read_text(),',
        'typed: Path("annotation-secret.txt").read_text() = None,',
        'opened_type: open("annotation-open-secret.txt") = None,',
        ') -> Path("return-secret.txt").read_text():',
        'return reader("default-open-secret.txt").read()',
    } <= {finding.snippet for finding in findings}


def test_raw_io_propagates_negative_subscripts_and_static_dict_containers() -> None:
    """负下标与静态 dict callable 不能成为 raw open 的事实传播旁路。"""
    source = """
def node(inputs, ctx):
    readers = [open]
    dict_readers = {"reader": readers[-1]}
    reader = dict_readers["reader"]
    return reader("negative-dict-secret.txt").read()
"""

    findings = check_raw_io_source(source, Path("nodes/static-containers.py"))

    assert [finding.snippet for finding in findings] == [
        'return reader("negative-dict-secret.txt").read()',
    ]
    assert findings[0].waived is False


def test_raw_io_closes_builtin_alias_importlib_and_sys_modules_open_chains() -> None:
    """builtin/module 字典链在落到 open 时必须仍是可审计的 raw-I/O 事实。"""
    source = """
import builtins
import importlib
import sys

def node(inputs, ctx):
    builtin_dict = builtins.__dict__
    builtin_reader = builtin_dict["open"]
    first = builtin_reader("builtin-alias-secret.txt").read()

    imported_module = importlib.import_module("builtins")
    second = imported_module.__dict__["open"]("importlib-secret.txt").read()
    fourth = importlib.import_module("builtins").__dict__["open"](
        "inline-importlib-secret.txt"
    ).read()

    module_table = sys.modules
    sys_builtin = module_table["builtins"]
    third = sys_builtin.__dict__["open"]("sys-modules-secret.txt").read()

    return first, second, third
"""

    findings = check_raw_io_source(source, Path("nodes/module-open-chains.py"))

    snippets = {finding.snippet for finding in findings}
    assert {
        'builtin_reader = builtin_dict["open"]',
        'first = builtin_reader("builtin-alias-secret.txt").read()',
        'second = imported_module.__dict__["open"]("importlib-secret.txt").read()',
        'fourth = importlib.import_module("builtins").__dict__["open"](',
        'third = sys_builtin.__dict__["open"]("sys-modules-secret.txt").read()',
    } <= snippets
    assert 'imported_module = importlib.import_module("builtins")' not in snippets
    assert all(not finding.waived for finding in findings)


def test_raw_io_hard_cuts_dict_method_and_module_open_lookup_chains() -> None:
    """动态 dict lookup 的 get/__getitem__ 变体不能丢掉 hard-cut。"""
    source = """
import builtins
import importlib
import sys

def node(inputs, ctx):
    builtin_get = builtins.__dict__.get("open")
    first = builtin_get("builtin-get-secret.txt").read()
    builtin_getitem = builtins.__dict__.__getitem__("open")
    second = builtin_getitem("builtin-getitem-secret.txt").read()
    sys_get = sys.modules.get("builtins").__dict__.get("open")
    third = sys_get("sys-get-secret.txt").read()
    sys_getitem = sys.modules.__getitem__("builtins").__dict__.__getitem__("open")
    fourth = sys_getitem("sys-getitem-secret.txt").read()
    importlib_get = importlib.import_module("builtins").__dict__.get("open")
    fifth = importlib_get("importlib-get-secret.txt").read()
    importlib_getitem = importlib.import_module("builtins").__dict__.__getitem__("open")
    sixth = importlib_getitem("importlib-getitem-secret.txt").read()
    return first, second, third, fourth, fifth, sixth
"""

    findings = check_raw_io_source(source, Path("nodes/dynamic-open-lookups.py"))

    snippets = {finding.snippet for finding in findings}
    assert {
        'builtin_get = builtins.__dict__.get("open")',
        'first = builtin_get("builtin-get-secret.txt").read()',
        'builtin_getitem = builtins.__dict__.__getitem__("open")',
        'second = builtin_getitem("builtin-getitem-secret.txt").read()',
        'sys_get = sys.modules.get("builtins").__dict__.get("open")',
        'third = sys_get("sys-get-secret.txt").read()',
        'sys_getitem = sys.modules.__getitem__("builtins").__dict__.__getitem__("open")',
        'fourth = sys_getitem("sys-getitem-secret.txt").read()',
        'importlib_get = importlib.import_module("builtins").__dict__.get("open")',
        'fifth = importlib_get("importlib-get-secret.txt").read()',
        'importlib_getitem = importlib.import_module("builtins").__dict__.__getitem__("open")',
        'sixth = importlib_getitem("importlib-getitem-secret.txt").read()',
    } <= snippets
    assert all(not finding.waived for finding in findings)


def test_raw_io_does_not_expand_import_module_names_into_ordinary_attributes() -> None:
    """importlib/sys 的普通属性保持 unknown，不把模块名本身变成无界误报。"""
    source = """
import importlib
import sys

def node(inputs, ctx):
    return importlib.util, sys.version
"""

    assert check_raw_io_source(source, Path("nodes/ordinary-module-attributes.py")) == []


def test_raw_io_propagates_static_lambda_star_and_keyword_arguments() -> None:
    """静态 star/**kwargs 展开必须传播 raw callable，并跟进 lambda 体。"""
    source = """
def node(inputs, ctx):
    def invoke(reader):
        return reader("keyword-secret.txt").read()

    invoke(**{"reader": open})
    def run(reader):
        return reader()

    callbacks = [lambda: Path("lambda-secret.txt").read_text()]
    return invoke(**{"reader": open}), run(*callbacks)
"""

    findings = check_raw_io_source(source, Path("nodes/static-callable-arguments.py"))

    snippets = {finding.snippet for finding in findings}
    assert {
        'return reader("keyword-secret.txt").read()',
        'callbacks = [lambda: Path("lambda-secret.txt").read_text()]',
    } <= snippets
    assert all(not finding.waived for finding in findings)


def test_raw_io_propagates_nested_static_dict_unpack_callable_arguments() -> None:
    """嵌套静态 dict unpack 仍必须把 callable fact 传入 helper。"""
    source = """
def node(inputs, ctx):
    def invoke(reader, **ignored):
        return reader("nested-keyword-secret.txt").read()

    base = {"reader": open}
    nested = {"unused": "value", **base}
    deeply_nested = {**nested}
    unknown = inputs["kwargs"]
    invoke(**deeply_nested)
    invoke(**unknown)
"""

    findings = check_raw_io_source(source, Path("nodes/nested-static-kwargs.py"))

    assert [finding.snippet for finding in findings] == [
        'return reader("nested-keyword-secret.txt").read()',
    ]
    assert findings[0].waived is False


def test_raw_io_invalidates_context_for_comprehension_and_match_bindings() -> None:
    """推导式和 match pattern 的 ctx 重绑定不能继续伪装成受控读取。"""
    source = """
def node(inputs, ctx):
    values = [ctx.read_text("comprehension-secret.txt") for ctx in [Path("rebound.txt")]]
    candidate = {"ctx": Path("match-rebound.txt")}
    match candidate:
        case {"ctx": ctx}:
            matched = ctx.read_text()
    after = ctx.read_text("after-match-secret.txt")
    return values, matched, after
"""

    findings = check_raw_io_source(source, Path("nodes/context-rebindings.py"))

    assert [finding.snippet for finding in findings] == [
        'values = [ctx.read_text("comprehension-secret.txt") for ctx in [Path("rebound.txt")]]',
        "matched = ctx.read_text()",
        'after = ctx.read_text("after-match-secret.txt")',
    ]
    assert all(not finding.waived for finding in findings)
