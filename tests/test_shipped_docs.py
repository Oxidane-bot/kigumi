from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

from kigumi.cli import main
from kigumi.docs import SHIPPED_DOCS, read_doc, resolve_doc

ROOT = Path(__file__).resolve().parents[1]
MARKDOWN_LINK_PATTERN = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")

# Links that cannot resolve in both the repository and the packaged layout. Keep this
# empty: a target that is wrong to ship (examples/, package source) should be linked
# absolutely instead, so the link works in both layouts rather than one.
UNRESOLVABLE_WHEN_PACKAGED: set[tuple[str, str]] = set()


def _force_include() -> dict[str, str]:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        document = tomllib.load(handle)
    targets = document["tool"]["hatch"]["build"]["targets"]
    return targets["wheel"]["force-include"]


def test_every_shipped_doc_is_force_included_into_the_wheel() -> None:
    """教训 unshipped_page: 声明为可交付的页在 wheel 里必须真的存在。"""
    mapping = _force_include()
    for doc in SHIPPED_DOCS:
        target = f"kigumi/{doc.packaged}"
        # contracts/ ships as one directory entry, so its index is covered by the parent.
        parent = Path(doc.source).parent.as_posix()
        included = mapping.get(doc.source) == target or mapping.get(parent) is not None
        assert included, f"{doc.source} is not force-included into the wheel as {target}"
        assert (ROOT / doc.source).is_file(), f"{doc.source} does not exist in the repository"


def test_force_include_sources_all_exist_and_are_not_hand_copied() -> None:
    """教训 copy_drift: 打包只做映射,仓库里不得再留一份手抄副本。"""
    for source in _force_include():
        assert (ROOT / source).exists(), f"force-include source {source} is missing"
    assert not (ROOT / "kigumi" / "docs").exists(), (
        "kigumi/docs must not be committed: docs/ is the single source of truth "
        "and hatch force-include maps it into the wheel"
    )


def test_resolve_doc_reads_every_page_from_a_source_checkout() -> None:
    """教训 checkout_fallback: 源码树里没有 kigumi/docs,回退路径仍须可读。"""
    for doc in SHIPPED_DOCS:
        assert resolve_doc(doc.name).is_file()
        assert read_doc(doc.name).strip()


def test_resolve_doc_rejects_an_unknown_page() -> None:
    """教训 silent_miss: 未知页名要报错并列出可用页,不能静默返回空。"""
    with pytest.raises(KeyError, match="unknown doc"):
        resolve_doc("nonexistent")


def test_brief_and_docs_run_outside_a_configured_project(tmp_path: Path, monkeypatch) -> None:
    """教训 preconfig_access: 未 init 的目录里也要能读文档,否则 agent 无从下手。"""
    monkeypatch.chdir(tmp_path)
    assert main(["brief"]) == 0
    assert main(["docs"]) == 0
    assert main(["docs", "capabilities"]) == 0


def test_docs_listing_names_every_shipped_page(tmp_path: Path, monkeypatch, capsys) -> None:
    """教训 hidden_page: 清单必须列全,漏掉的页等于没有交付。"""
    monkeypatch.chdir(tmp_path)
    assert main(["docs"]) == 0
    listing = capsys.readouterr().out
    for doc in SHIPPED_DOCS:
        assert doc.name in listing
        assert doc.summary in listing


def test_brief_prints_the_brief_page(tmp_path: Path, monkeypatch, capsys) -> None:
    """教训 wrong_page: brief 必须打印 brief.md 本身,不是别的页。"""
    monkeypatch.chdir(tmp_path)
    assert main(["brief"]) == 0
    assert capsys.readouterr().out == read_doc("brief")


def test_docs_rejects_an_unknown_page_name(tmp_path: Path, monkeypatch) -> None:
    """教训 typo_exit: 拼错页名由 argparse 判 2,不能当成成功。"""
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as exited:
        main(["docs", "nonexistent"])
    assert exited.value.code == 2


def test_brief_points_only_at_real_symbols_and_shipped_pages() -> None:
    """教训 dead_pointer: brief 是入口,指向不存在的符号或页比不写更糟。"""
    import importlib

    import kigumi
    from kigumi.dag import Dag, NodeContext

    text = read_doc("brief")
    # Config keys, keyword arguments and CLI words are not importable symbols.
    ignored = {
        "kigumi",
        "pyproject.toml",
        "CHANGELOG.md",
        "consumes",
        "files",
        "cache_dir",
        "seed",
        "open",
    }
    unresolved: list[str] = []
    for token in sorted(set(re.findall(r"`@?([A-Za-z_][A-Za-z0-9_.]*)`", text))):
        if token in ignored or token.isupper() or token.startswith("KIGUMI_"):
            continue
        if token.startswith(("kigumi ", "dag ")):
            continue
        if token.startswith("ctx."):
            holder, attribute = NodeContext, token[4:]
        elif token.startswith("dag."):
            holder, attribute = Dag, token[4:]
        elif "." in token:
            module_path, attribute = token.rsplit(".", 1)
            holder = importlib.import_module(module_path)
        else:
            holder, attribute = kigumi, token
        if not hasattr(holder, attribute):
            unresolved.append(token)
    assert not unresolved, "Brief points at missing symbols:\n" + "\n".join(unresolved)

    referenced = set(re.findall(r"kigumi docs (\w+)", text))
    known = {doc.name for doc in SHIPPED_DOCS}
    assert referenced <= known, f"brief references unshipped pages: {sorted(referenced - known)}"


def test_brief_documents_every_kigumi_subcommand() -> None:
    """教训 command_gap: 新增子命令必须出现在 brief,否则 agent 不会用它。"""
    text = read_doc("brief")
    for name in _subcommand_names(_parser_of_main()):
        assert f"kigumi {name}" in text, f"brief does not document `kigumi {name}`"


def test_brief_documents_the_graph_command_entry_points() -> None:
    """教训 phantom_command: brief 不能把不存在的可执行文件写成能直接敲的命令。

    修好之前 `dag` 只是 argparse 的 prog 名,没有任何 `[project.scripts]` 提供它,
    但 brief 把 8 个图命令都写成 `dag <command>`。现在图命令走 `kigumi`,而 `dag`
    只在项目自己注册 entry point 时才存在——这两点都必须说清楚。
    """
    from kigumi.dag import GRAPH_COMMAND_HELP, _build_cli_parser

    text = read_doc("brief")
    assert set(_subcommand_names(_build_cli_parser())) == set(GRAPH_COMMAND_HELP)
    for name in GRAPH_COMMAND_HELP:
        assert f"kigumi {name}" in text, f"brief does not document `kigumi {name}`"

    # Any `dag <command>` spelling must be accompanied by how that command comes to
    # exist, otherwise the reader is told to run something they do not have.
    if re.search(r"`dag \w", text):
        assert "[project.scripts]" in text, "brief shows `dag ...` without saying how to get it"
    assert "dag_entry" in text, "brief must name the key that makes graph commands reachable"


def test_shipped_relative_links_resolve_inside_the_wheel_layout(tmp_path: Path) -> None:
    """教训 link_rot: 装到 site-packages 后,页间相对链接必须仍然可达。"""
    mapping = _force_include()
    for source, target_path in mapping.items():
        target = tmp_path / target_path
        origin = ROOT / source
        target.parent.mkdir(parents=True, exist_ok=True)
        if origin.is_dir():
            for page in origin.rglob("*.md"):
                destination = target / page.relative_to(origin)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(page.read_bytes())
        else:
            target.write_bytes(origin.read_bytes())
    # The package's own source files are link targets from the contracts index.
    (tmp_path / "kigumi" / "__init__.py").write_bytes(b"")

    packaged = tmp_path / "kigumi"
    failures: list[str] = []
    unused = set(UNRESOLVABLE_WHEN_PACKAGED)
    for page in sorted(packaged.rglob("*.md")):
        location = page.relative_to(packaged).as_posix()
        for raw_target in MARKDOWN_LINK_PATTERN.findall(page.read_text(encoding="utf-8")):
            relative = _link_path(raw_target)
            if relative is None:
                continue
            if (page.parent / relative).resolve().is_file():
                continue
            entry = (location, raw_target.strip())
            if entry in UNRESOLVABLE_WHEN_PACKAGED:
                unused.discard(entry)
                continue
            failures.append(f"{location}: {raw_target}")
    assert not failures, "Broken links in the shipped documentation layout:\n" + "\n".join(failures)
    assert not unused, f"Remove stale link allowlist entries: {sorted(unused)}"


def _parser_of_main():
    from kigumi.cli import _parser

    return _parser()


def _subcommand_names(parser) -> list[str]:
    from argparse import _SubParsersAction

    for action in parser._actions:
        if isinstance(action, _SubParsersAction):
            return sorted(action.choices)
    return []


def _link_path(raw_target: str) -> str | None:
    from urllib.parse import unquote, urlsplit

    stripped = raw_target.strip()
    if stripped.startswith("<") and ">" in stripped:
        stripped = stripped[1 : stripped.index(">")]
    else:
        stripped = stripped.split(maxsplit=1)[0]
    parsed = urlsplit(stripped)
    if parsed.scheme or parsed.netloc or stripped.startswith("/"):
        return None
    path = unquote(parsed.path)
    return path or None
