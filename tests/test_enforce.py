from __future__ import annotations

from pathlib import Path

from kigumi.enforce import check_paths, check_raw_io_node_paths, check_raw_io_source, check_source


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
