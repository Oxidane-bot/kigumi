from __future__ import annotations

import importlib.util
import re
import subprocess
import textwrap
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest

import kigumi

ROOT = Path(__file__).resolve().parents[1]
STATUS_LINE_PATTERN = r"Status: (?:Active \(\d+\.\d+\.\d+\)|Draft \(Unreleased\))"
STATUS_PATTERN = re.compile(rf"^{STATUS_LINE_PATTERN}$", re.MULTILINE)
MARKDOWN_LINK_PATTERN = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")

# Keep exclusions explicit and justified. There are currently none: every public export is
# useful to callers and must remain discoverable in user-facing documentation.
UNDOCUMENTED_EXPORT_ALLOWLIST: set[str] = set()


def test_readme_status_versions_match_package_version() -> None:
    """两份入口文档的状态版本必须跟随唯一包版本源，不能静默落后。"""
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


def test_release_workflows_gate_security_and_both_distribution_formats() -> None:
    """PRs and releases must exercise the same security and artifact contracts."""
    security = (ROOT / ".github" / "workflows" / "security.yml").read_text(encoding="utf-8")
    assert re.search(r"^  pull_request:\s*$", security, re.MULTILINE)
    assert re.search(r"^  workflow_call:\s*$", security, re.MULTILINE)

    release = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert re.search(
        r"^  security:\n    uses: \.\/\.github\/workflows\/security\.yml\s*$",
        release,
        re.MULTILINE,
    )
    assert re.search(r"^  build:\n    needs: \[quality, test, security\]", release, re.MULTILINE)

    for workflow_name in ("ci.yml", "release.yml"):
        workflow = (ROOT / ".github" / "workflows" / workflow_name).read_text(encoding="utf-8")
        assert "uv sync --locked --extra dev" in workflow
        assert "uv sync --extra dev" not in workflow
        assert "KIGUMI_DIST_DIR=dist" in workflow
        assert "dist/*.whl" in workflow
        assert "dist/*.tar.gz" in workflow
        assert "scripts/smoke_installed.py" in workflow
        assert "uv pip check --python" in workflow

    security = (ROOT / ".github" / "workflows" / "security.yml").read_text(encoding="utf-8")
    assert "uv sync --locked --extra dev" in security
    assert "uv sync --extra dev" not in security

    for workflow_name in ("ci.yml", "release.yml"):
        workflow = (ROOT / ".github" / "workflows" / workflow_name).read_text(encoding="utf-8")
        assert "distribution-smoke:" in workflow
        assert 'python-version: ["3.11", "3.12", "3.13"]' in workflow
        assert 'format: ["wheel", "sdist"]' in workflow

    release = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert 'wheel_paths = sorted(Path("dist").glob("kigumi-*.whl"))' in release
    assert 'sdist_paths = sorted(Path("dist").glob("kigumi-*.tar.gz"))' in release
    assert "len(wheel_paths) != 1 or len(sdist_paths) != 1" in release
    assert "len(published) != 2" in release
    assert "local_paths = wheel_paths + sdist_paths" in release


def test_locked_uv_workflows_require_a_tracked_lockfile() -> None:
    """干净 git archive 也必须能执行所有 ``--locked`` workflow 步骤。"""
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert "uv.lock" not in {line.strip() for line in gitignore if line.strip()}
    assert (ROOT / "uv.lock").is_file()

    result = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "uv.lock"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, "uv.lock must be tracked for clean checkout --locked runs"


def test_013_docs_describe_current_guard_and_historical_boundaries() -> None:
    """0.13 文档必须与递归 guard、schema-2 hard cut 和历史版本归属一致。"""
    contracts = (ROOT / "docs" / "contracts" / "README.md").read_text(encoding="utf-8")
    design = (ROOT / "DESIGN.md").read_text(encoding="utf-8")
    api = (ROOT / "docs" / "api.md").read_text(encoding="utf-8")
    adoption = (ROOT / "docs" / "adoption.md").read_text(encoding="utf-8")
    design_flat = re.sub(r"\s+", " ", design)

    assert "[分层 Prompt 解析契约](prompt-resolution.md) | Active (0.13.0)" in contracts
    assert "递归跟随执行路径中可达的局部 helper/lambda" in design_flat
    assert "递归跟随可达 helper/lambda" in api
    assert "只扫匹配函数的最外层函数体" not in design
    assert "顶层 `node`/`map`/`scan`/`foreach`/`agent` 装饰器函数的最外层函数体。" not in api
    assert "schema-2 run manifest 禁止覆盖" in adoption
    assert "0.6 manifest 禁止覆盖" not in adoption

    history = design.split("## 修订记录", 1)[1]
    assert (
        "2026-08-03 0.11.0：managed request"
        "（附件、响应 schema）、输入预检、`CACHE_SCHEMA=7`" in history
    )
    release_013 = history.split("2026-08-04 0.13.0：", 1)[1].split("\n", 1)[0]
    assert "managed request" not in release_013
    assert "输入预检" not in release_013
    assert "CACHE_SCHEMA=7" not in release_013


def test_verify_dist_covers_every_shipped_doc() -> None:
    """发行物校验必须覆盖 ``kigumi docs`` 实际声明的全部页面。"""
    from kigumi.docs import SHIPPED_DOCS

    spec = importlib.util.spec_from_file_location(
        "verify_dist_for_test", ROOT / "scripts/verify_dist.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    wheel_docs = set(module.REQUIRED_WHEEL_DOCS)
    sdist_docs = set(module.REQUIRED_SDIST_DOCS)
    for doc in SHIPPED_DOCS:
        assert f"kigumi/{doc.packaged}" in wheel_docs
        assert doc.source in sdist_docs


def test_verify_dist_rejects_wrong_unique_artifact_names() -> None:
    """A single stale artifact must not pass the standalone release verifier."""
    spec = importlib.util.spec_from_file_location(
        "verify_dist_names_for_test", ROOT / "scripts/verify_dist.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    with pytest.raises(RuntimeError, match="unexpected sdist filename"):
        module._verify_artifact_names(
            ROOT / "kigumi-0.13.0-py3-none-any.whl",
            ROOT / "kigumi-0.12.0.tar.gz",
            "0.13.0",
        )


def test_installed_smoke_covers_cli_positive_and_negative_paths() -> None:
    """干净安装 smoke 必须检查所有 shipped docs 和关键 CLI 拒绝路径。"""
    smoke = (ROOT / "scripts" / "smoke_installed.py").read_text(encoding="utf-8")
    assert "sysconfig.get_paths()" in smoke
    assert "for doc in SHIPPED_DOCS" in smoke
    assert 'run_cli(root, "docs", doc.name)' in smoke
    assert "def run_cli_failure" in smoke
    assert 'run_cli_failure(root, "init")' in smoke
    assert 'run_cli_failure(root, "docs", "not-a-shipped-doc")' in smoke
    assert 'run_cli_failure(root, "init")' in smoke
    assert "pyproject_before_repeat" in smoke


def test_release_hash_check_is_valid_python() -> None:
    """release workflow 内嵌的 PyPI hash 校验脚本必须能被标准库编译。"""
    release = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    match = re.search(r"if python - <<'PY'\n(?P<script>.*?)\n          PY", release, re.DOTALL)
    assert match is not None, "release workflow must contain one inline hash-check script"
    compile(textwrap.dedent(match.group("script")), "<release hash check>", "exec")


def _markdown_link_target(raw_target: str) -> str:
    stripped = raw_target.strip()
    if stripped.startswith("<") and ">" in stripped:
        return stripped[1 : stripped.index(">")]
    return stripped.split(maxsplit=1)[0]
