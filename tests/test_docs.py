from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

import kigumi

ROOT = Path(__file__).resolve().parents[1]
STATUS_PATTERN = re.compile(r"^Status: Active \((\d+\.\d+\.\d+)\)$", re.MULTILINE)
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


def test_every_contract_has_a_versioned_status_and_index_entry() -> None:
    """教训 contract_index:新增契约不能缺状态，也不能成为索引外的孤岛。"""
    contracts_root = ROOT / "docs" / "contracts"
    index = (contracts_root / "README.md").read_text(encoding="utf-8")
    contract_files = sorted(
        path for path in contracts_root.glob("*.md") if path.name != "README.md"
    )
    assert contract_files

    for path in contract_files:
        text = path.read_text(encoding="utf-8")
        assert re.match(
            r"^# .+\n\nStatus: Active \(\d+\.\d+\.\d+\)\n",
            text,
        ), f"{path.relative_to(ROOT)} must put a versioned Status directly below its H1"
        assert STATUS_PATTERN.search(text) is not None
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


def _markdown_link_target(raw_target: str) -> str:
    stripped = raw_target.strip()
    if stripped.startswith("<") and ">" in stripped:
        return stripped[1 : stripped.index(">")]
    return stripped.split(maxsplit=1)[0]
