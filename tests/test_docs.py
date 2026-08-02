from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

import kigumi

ROOT = Path(__file__).resolve().parents[1]
STATUS_LINE_PATTERN = r"Status: (?:Active \(\d+\.\d+\.\d+\)|Draft \(Unreleased\))"
STATUS_PATTERN = re.compile(rf"^{STATUS_LINE_PATTERN}$", re.MULTILINE)
MARKDOWN_LINK_PATTERN = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")

# Keep exclusions explicit and justified. There are currently none: every public export is
# useful to callers and must remain discoverable in user-facing documentation.
UNDOCUMENTED_EXPORT_ALLOWLIST: set[str] = set()


def test_readme_status_versions_match_package_version() -> None:
    """教训 version_drift:两份入口文档的状态版本必须跟随唯一包版本源。"""
    version_source = (ROOT / "kigumi" / "_version.py").read_text(encoding="utf-8")
    source_match = re.search(r'^__version__ = "(\d+\.\d+\.\d+)"$', version_source, re.MULTILINE)
    assert source_match is not None
    assert source_match.group(1) == kigumi.__version__

    for filename, heading in (("README.md", "Status"), ("README.zh-CN.md", "状态")):
        text = (ROOT / filename).read_text(encoding="utf-8")
        status_match = re.search(
            rf"^## {heading}\s*\n+(\d+\.\d+\.\d+)\b",
            text,
            re.MULTILINE,
        )
        assert status_match is not None, f"{filename} has no versioned {heading} section"
        assert status_match.group(1) == kigumi.__version__


def test_every_contract_has_a_recognized_status_and_index_entry() -> None:
    """教训 contract_index:契约状态必须严格可识别，也不能成为索引外的孤岛。"""
    contracts_root = ROOT / "docs" / "contracts"
    index = (contracts_root / "README.md").read_text(encoding="utf-8")
    contract_files = sorted(
        path for path in contracts_root.glob("*.md") if path.name != "README.md"
    )
    assert contract_files

    for path in contract_files:
        text = path.read_text(encoding="utf-8")
        assert re.match(
            rf"^# .+\n\n{STATUS_LINE_PATTERN}\n",
            text,
        ), f"{path.relative_to(ROOT)} must put one recognized Status directly below its H1"
        assert len(STATUS_PATTERN.findall(text)) == 1, (
            f"{path.relative_to(ROOT)} must have exactly one recognized Status"
        )
        assert f"]({path.name})" in index, f"{path.name} is missing from the contracts index"


def test_all_relative_markdown_links_resolve_to_existing_files() -> None:
    """教训 link_rot:中文入口与权威文档中的相对链接必须随文件移动同步更新。"""
    documents = [
        ROOT / "README.zh-CN.md",
        ROOT / "DESIGN.md",
        ROOT / "AGENTS.md",
        *sorted((ROOT / "docs").rglob("*.md")),
    ]
    failures: list[str] = []
    for document in documents:
        text = document.read_text(encoding="utf-8")
        for raw_target in MARKDOWN_LINK_PATTERN.findall(text):
            target = _markdown_link_target(raw_target)
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc or target.startswith("/"):
                continue
            relative = unquote(parsed.path)
            if not relative:
                continue
            resolved = (document.parent / relative).resolve()
            if not resolved.is_file():
                failures.append(
                    f"{document.relative_to(ROOT)}: {raw_target!r} -> "
                    f"{resolved.relative_to(ROOT) if resolved.is_relative_to(ROOT) else resolved}"
                )
    assert not failures, "Broken relative Markdown links:\n" + "\n".join(failures)


def test_every_public_export_appears_in_user_facing_docs() -> None:
    """教训 api_discovery:顶层公开名不能只存在于源码和自动补全里。"""
    documents = [
        ROOT / "README.md",
        ROOT / "README.zh-CN.md",
        ROOT / "DESIGN.md",
        *sorted((ROOT / "docs").rglob("*.md")),
    ]
    corpus = "\n".join(path.read_text(encoding="utf-8") for path in documents)
    missing = {
        name
        for name in kigumi.__all__
        if re.search(rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])", corpus) is None
    }
    assert missing <= UNDOCUMENTED_EXPORT_ALLOWLIST
    assert set(kigumi.__all__) >= UNDOCUMENTED_EXPORT_ALLOWLIST
    assert not (UNDOCUMENTED_EXPORT_ALLOWLIST - missing), "Remove stale documentation allowlist"


def test_capability_index_points_only_at_real_symbols() -> None:
    """教训 dead_pointer:能力索引是入口,指向不存在的符号比不写更糟。"""
    import importlib

    from kigumi.dag import Dag, NodeContext

    text = (ROOT / "docs" / "capabilities.md").read_text(encoding="utf-8")
    # Names that are not importable symbols: env vars, CLI words, pytest item
    # names, config keys, decorators and message-dict keys.
    ignored = {
        "kigumi",
        "kigumi_file",
        "kigumi_guard",
        "kigumi_dry_render",
        "session_carry",
        "consumes",
        "files",
        "files_fn",
        "external_fingerprint",
        "cache_dir",
        "seed",
        "dry",
        "DryRunError",
        "pytest.mark.live",
    }
    unresolved: list[str] = []
    for token in sorted(set(re.findall(r"`([A-Za-z_][A-Za-z0-9_.]*)`", text))):
        if token in ignored or token.isupper() or token.startswith("KIGUMI_"):
            continue
        if token.startswith("ctx."):
            holder, attribute = NodeContext, token[4:]
        elif token.startswith("dag."):
            holder, attribute = Dag, token[4:]
        elif "." in token:
            module_path, attribute = token.rsplit(".", 1)
            try:
                holder = importlib.import_module(module_path)
            except ImportError:
                unresolved.append(token)
                continue
        else:
            holder, attribute = kigumi, token
        if not hasattr(holder, attribute):
            unresolved.append(token)
    assert not unresolved, "Capability index points at missing symbols:\n" + "\n".join(unresolved)


def _markdown_link_target(raw_target: str) -> str:
    stripped = raw_target.strip()
    if stripped.startswith("<") and ">" in stripped:
        return stripped[1 : stripped.index(">")]
    return stripped.split(maxsplit=1)[0]
