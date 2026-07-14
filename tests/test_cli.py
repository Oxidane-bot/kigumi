from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from kigumi.artifacts import atomic_write_json, canonical_json, write_artifact
from kigumi.cli import main
from kigumi.config import KigumiConfig
from kigumi.dag import Dag


def _project(tmp_path: Path, *, source_dirs: str = '["nodes"]') -> Path:
    (tmp_path / "pyproject.toml").write_text(
        f"[project]\nname = 'sample'\n\n[tool.kigumi]\nsource_dirs = {source_dirs}\n",
        encoding="utf-8",
    )
    return tmp_path


def _git(root: Path, *arguments: str) -> None:
    subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )


def _cli_dag(tmp_path: Path, *, source_dirs: list[str] | None = None) -> Dag:
    return Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=source_dirs or []),
        object(),  # type: ignore[arg-type] -- these CLI fixtures never call the model.
    )


def _run_dag_cli(dag: Dag, argv: list[str]) -> int:
    with pytest.raises(SystemExit) as exited:
        dag.cli(argv)
    return int(exited.value.code)


def test_init_creates_default_layout_and_refuses_repeat(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """教训 explicit_init: 脚手架只能显式激活一次，不能静默改已有配置。"""
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'sample'\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert main(["init"]) == 0
    assert "[tool.kigumi]" in (tmp_path / "pyproject.toml").read_text(encoding="utf-8")
    assert (tmp_path / "prompts" / ".gitkeep").exists()
    assert (tmp_path / "artifacts" / ".gitkeep").exists()
    assert (tmp_path / "artifacts" / "_llm" / ".gitkeep").exists()
    assert (tmp_path / "nodes" / ".gitkeep").exists()
    assert (tmp_path / "lib" / ".gitkeep").exists()
    assert "artifacts/" in (tmp_path / ".gitignore").read_text(encoding="utf-8")
    assert main(["init"]) == 1
    assert "already exists" in capsys.readouterr().err


def test_init_hooks_refuses_existing_hook_and_missing_pyproject(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """教训 hook_ownership: init 绝不猜项目形态或覆盖用户 hook。"""
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 1
    assert "pyproject.toml" in capsys.readouterr().err

    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'sample'\n", encoding="utf-8")
    _git(tmp_path, "init")
    assert main(["init", "--hooks"]) == 0
    hook = tmp_path / ".git" / "hooks" / "pre-commit"
    assert "uv run kigumi guard --changed" in hook.read_text(encoding="utf-8")
    assert hook.stat().st_mode & 0o111

    second = tmp_path / "second"
    second.mkdir()
    (second / "pyproject.toml").write_text("[project]\nname = 'second'\n", encoding="utf-8")
    _git(second, "init")
    existing = second / ".git" / "hooks" / "pre-commit"
    existing.write_text("custom hook\n", encoding="utf-8")
    monkeypatch.chdir(second)
    assert main(["init", "--hooks"]) == 1
    assert existing.read_text(encoding="utf-8") == "custom hook\n"


def test_guard_reports_violations_waivers_and_new_changed_waivers(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """教训 visible_waiver: 合法豁免可通过，但新增与原因都必须被点名。"""
    root = _project(tmp_path)
    nodes = root / "nodes"
    nodes.mkdir()
    bad = nodes / "bad.py"
    bad.write_text("for item in items:\n    client.call([])\n", encoding="utf-8")
    monkeypatch.chdir(root)

    assert main(["guard"]) == 1
    assert "nodes/bad.py:2" in capsys.readouterr().out
    bad.write_text(
        "for item in items:\n    client.call([])  # kigumi: raw-llm-ok fixture tape\n",
        encoding="utf-8",
    )
    assert main(["guard"]) == 0
    assert "waiver nodes/bad.py:2 fixture tape" in capsys.readouterr().out
    assert main(["guard", "--changed"]) == 2
    assert "git repository" in capsys.readouterr().err

    _git(root, "init")
    _git(root, "config", "user.email", "test@example.com")
    _git(root, "config", "user.name", "Test")
    bad.write_text("value = 'clean'\n", encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "clean")
    bad.write_text(
        "for item in items:\n    client.call([])  # kigumi: raw-llm-ok fixture tape\n",
        encoding="utf-8",
    )

    assert main(["guard", "--changed"]) == 0
    assert "new waiver: nodes/bad.py:2 fixture tape" in capsys.readouterr().out

    untracked = nodes / "untracked.py"
    untracked.write_text("for item in items:\n    client.call([])\n", encoding="utf-8")
    # git diff 看不见未跟踪文件;guard --changed 必须照样抓到。
    assert main(["guard", "--changed"]) == 1
    assert "nodes/untracked.py:2" in capsys.readouterr().out

    untracked.unlink()
    _git(root, "add", ".")
    _git(root, "commit", "-m", "waiver committed")
    bad.write_text(
        "# shifted\nfor item in items:\n    client.call([])  # kigumi: raw-llm-ok fixture tape\n",
        encoding="utf-8",
    )
    # 行号漂移不是新增豁免:比对按理由文本,不按行号。
    assert main(["guard", "--changed"]) == 0
    assert "new waiver" not in capsys.readouterr().out


def test_guard_checks_decorated_raw_io_but_not_helpers_and_tracks_its_waivers(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """教训 raw_io_guard_cli: 提交环只扫节点体，raw-io 豁免独立留痕。"""
    root = _project(tmp_path)
    nodes = root / "nodes"
    nodes.mkdir()
    source = nodes / "pipeline.py"
    source.write_text(
        """
def helper():
    return open("fixture.txt").read()

@dag.node("unsafe")
def unsafe(inputs, ctx):
    return open("input.txt").read()
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(root)

    assert main(["guard"]) == 1
    assert "nodes/pipeline.py:7" in capsys.readouterr().out

    source.write_text(
        """
@dag.node("waived")
def waived(inputs, ctx):
    return open("fixture.txt").read()  # kigumi: raw-io-ok fixture setup
""",
        encoding="utf-8",
    )
    assert main(["guard"]) == 0
    assert "waiver nodes/pipeline.py:4 fixture setup" in capsys.readouterr().out

    _git(root, "init")
    _git(root, "config", "user.email", "test@example.com")
    _git(root, "config", "user.name", "Test")
    source.write_text(
        "for item in items:\n    client.call([])  # kigumi: raw-llm-ok fixture setup\n",
        encoding="utf-8",
    )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "clean")
    source.write_text(
        """
@dag.map("items", items_from=("source", "items"))
def mapped(item, inputs, ctx):
    return Path("fixture.txt").read_text()  # kigumi: raw-io-ok fixture setup
""",
        encoding="utf-8",
    )

    assert main(["guard", "--changed"]) == 0
    # 两类同名理由不能互相吞掉：HEAD 的 raw-llm 豁免不抵本次 raw-io 豁免。
    assert "new waiver: nodes/pipeline.py:4 fixture setup" in capsys.readouterr().out


def test_render_fills_missing_slots_and_rejects_residual_syntax(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """教训 dry_render_cli: CLI 渲染沿用严格模板契约与占位值。"""
    root = _project(tmp_path)
    prompts = root / "prompts"
    prompts.mkdir()
    (prompts / "hello.md").write_text("Hello {{name}}", encoding="utf-8")
    (prompts / "broken.md").write_text("{{BadSlot}}", encoding="utf-8")
    monkeypatch.chdir(root)

    assert main(["render", "hello"]) == 0
    assert "Hello <name>" in capsys.readouterr().out
    assert main(["render", "hello", "--slot", "name=Kigumi"]) == 0
    assert "Hello Kigumi" in capsys.readouterr().out
    assert main(["render", "broken"]) == 1
    assert "unrendered template slots" in capsys.readouterr().err


def test_doctor_reports_keys_without_env_values(tmp_path: Path, monkeypatch, capsys) -> None:
    """教训 secret_hygiene: doctor 可诊断装载键，绝不能回显密钥值。"""
    root = _project(tmp_path)
    (root / ".env").write_text("SECRET_TOKEN=do-not-print\n", encoding="utf-8")
    monkeypatch.chdir(root)
    monkeypatch.delenv("SECRET_TOKEN", raising=False)

    assert main(["doctor"]) == 0
    output = capsys.readouterr().out
    assert "SECRET_TOKEN" in output
    assert "do-not-print" not in output
    assert "llm cache:" in output


def test_trace_call_diff_and_json_run_views_use_persisted_evidence(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Agent observability follows sidecars to L1 payloads without a DAG import."""
    root = _project(tmp_path)
    artifacts = root / "artifacts"
    run_a = artifacts / "runs" / "run-2"
    run_b = artifacts / "runs" / "run-10"
    call = {
        "key": "call-key-123",
        "model_alias": "fast",
        "model": "provider/model",
        "cache": "miss",
        "prompt_sha": "prompt-sha",
        "seconds": 0.5,
        "usage": {"total_tokens": 3},
    }
    write_artifact(
        run_a / "node.json",
        canonical_json({"value": "a"}),
        {
            "cache_key": "node-key",
            "cache": "miss",
            "seconds": 1.5,
            "calls": [call],
            "key_components": {"prompt": "a"},
        },
    )
    write_artifact(
        run_b / "node.json",
        canonical_json({"value": "b"}),
        {
            "cache_key": "node-key",
            "cache": "hit",
            "seconds": 0,
            "calls": [],
            "key_components": {"prompt": "b"},
        },
    )
    atomic_write_json(
        root / "artifacts" / "_llm" / "llm" / "call-key-123.json",
        {
            "meta": call,
            "messages": [{"role": "user", "content": "hello"}],
            "response": "world",
            "reasoning": "why",
        },
    )
    monkeypatch.chdir(root)

    assert main(["trace", "run-2", "--node", "node", "--json"]) == 0
    traced = json.loads(capsys.readouterr().out)
    assert traced["nodes"][0]["calls"][0]["payload_path"].endswith("call-key-123.json")
    assert main(["call", "call-key", "--field", "response"]) == 0
    assert capsys.readouterr().out == "world\n"
    assert main(["call", "call-key", "--field", "messages"]) == 0
    assert json.loads(capsys.readouterr().out) == [{"role": "user", "content": "hello"}]
    assert main(["diff", "run-2", "run-10", "--json"]) == 0
    difference = json.loads(capsys.readouterr().out)
    assert difference["components"]["node"]["changed"] == ["prompt"]
    assert main(["runs", "list", "--json"]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert [entry["run_id"] for entry in listed["runs"]] == ["run-2", "run-10"]
    assert main(["runs", "show", "run-2", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["nodes"][0]["name"] == "node"

    assert main(["call", "missing"]) == 1
    assert "No LLM payload" in capsys.readouterr().err
    # 打错 run 或节点名必须报错,不许静默给空结果——空结果会被误读成"没有差异"。
    assert main(["diff", "run-2", "run-typo"]) == 1
    assert "run not found: run-typo" in capsys.readouterr().err
    assert main(["trace", "run-2", "--node", "typo"]) == 1
    assert "node not found in run-2: typo" in capsys.readouterr().err


def test_runs_approve_diff_and_gc_commands_use_persisted_artifacts(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """教训 cli_without_dag: 历史操作必须直接复用 runs 文件而非构建 caller。"""
    root = _project(tmp_path)
    artifacts = root / "artifacts"
    run_a = artifacts / "runs" / "run-a"
    run_b = artifacts / "runs" / "run-b"
    write_artifact(
        run_a / "node.json",
        canonical_json({"value": "one"}),
        {"cache_key": "keep", "cache": "miss", "seconds": 1.5, "calls": [{}]},
    )
    write_artifact(
        run_b / "node.json",
        canonical_json({"value": "two"}),
        {"cache_key": "keep", "cache": "hit", "seconds": 0.0, "calls": []},
    )
    atomic_write_json(run_a / "approvals" / "editor.pending.json", {"question": "approve"})
    cache_root = artifacts / "_cache" / "nodes"
    atomic_write_json(cache_root / "keep.json", {"artifact": {}})
    atomic_write_json(cache_root / "old.json", {"artifact": {}})
    blobs_root = artifacts / "_cache" / "blobs"
    retained_blob = "retained"
    stale_blob = "stale"
    blobs_root.mkdir(parents=True)
    (blobs_root / retained_blob).write_bytes(b"keep")
    (blobs_root / stale_blob).write_bytes(b"remove")
    atomic_write_json(run_b / "blob.json", {"file": {"kigumi_blob": retained_blob}})
    monkeypatch.chdir(root)

    assert main(["runs", "list"]) == 0
    assert "run-a nodes=1" in capsys.readouterr().out
    assert main(["runs", "show", "run-a"]) == 0
    shown = capsys.readouterr().out
    assert "node cache=miss" in shown
    assert "pending: editor" in shown
    assert main(["runs", "show", "missing"]) == 1
    assert "run not found" in capsys.readouterr().err

    assert main(["approve", "run-a", "missing"]) == 1
    assert "No pending checkpoint" in capsys.readouterr().err
    assert main(["approve", "run-a", "editor", "--data", '{"ok": true}']) == 0
    approval = json.loads((run_a / "approvals" / "editor.json").read_text(encoding="utf-8"))
    assert approval["data"] == {"ok": True}
    assert not (run_a / "approvals" / "editor.pending.json").exists()

    assert main(["runs", "show", "run-a"]) == 0
    approved_show = capsys.readouterr().out
    assert "pending: editor" not in approved_show
    assert "approved: editor" in approved_show

    assert main(["diff", "run-a", "run-b"]) == 0
    assert "changed: node" in capsys.readouterr().out
    assert main(["gc", "--keep", "1"]) == 0
    assert "deleted cache and blob entries: 2" in capsys.readouterr().out
    assert (cache_root / "keep.json").exists()
    assert not (cache_root / "old.json").exists()
    assert (blobs_root / retained_blob).exists()
    assert not (blobs_root / stale_blob).exists()


def test_cli_check_reports_clean_dag(tmp_path: Path, capsys) -> None:
    dag = _cli_dag(tmp_path)
    (tmp_path / "input.txt").write_text("fixture", encoding="utf-8")

    @dag.node("clean", files=("input.txt",))
    def clean(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        """Read an explicitly declared input."""
        del inputs, ctx
        return {"status": "clean"}

    assert _run_dag_cli(dag, ["check"]) == 0
    assert "0 errors" in capsys.readouterr().out


def test_cli_check_reports_missing_docstring(tmp_path: Path, capsys) -> None:
    dag = _cli_dag(tmp_path)

    @dag.node("undocumented")
    def undocumented(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"status": "missing docs"}

    assert _run_dag_cli(dag, ["check"]) == 0
    assert "undocumented: missing docstring" in capsys.readouterr().out


def test_cli_check_reports_guard_violation(tmp_path: Path, capsys) -> None:
    nodes = tmp_path / "nodes"
    nodes.mkdir()
    (nodes / "bad.py").write_text(
        "for item in items:\n    client.call([])\n",
        encoding="utf-8",
    )
    dag = _cli_dag(tmp_path, source_dirs=["nodes"])

    @dag.node("documented")
    def documented(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        """Provide a valid static declaration alongside the guarded source."""
        del inputs, ctx
        return {"status": "ok"}

    assert _run_dag_cli(dag, ["check"]) == 1
    assert "violation" in capsys.readouterr().out


def test_cli_check_raw_io_filters_to_decorated_node_bodies(tmp_path: Path, capsys) -> None:
    """教训 raw_io_cli_check: 图检查不得因 source_dirs 的 helper 产生误报。"""
    nodes = tmp_path / "nodes"
    nodes.mkdir()
    (nodes / "guards.py").write_text(
        """
def helper():
    return open("fixture.txt").read()

@pipeline.node("unsafe")
def unsafe(inputs, context):
    return open("input.txt").read()
""",
        encoding="utf-8",
    )
    dag = _cli_dag(tmp_path, source_dirs=["nodes"])

    assert _run_dag_cli(dag, ["check"]) == 1
    output = capsys.readouterr().out
    assert "guards.py:7" in output
    assert "guards.py:3" not in output


def test_cli_plan_shows_counts(tmp_path: Path, capsys) -> None:
    dag = _cli_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        """Provide a cacheable source value."""
        del inputs, ctx
        return {"value": 1}

    dag.run()

    assert _run_dag_cli(dag, ["plan"]) == 0
    output = capsys.readouterr().out
    assert "certain" in output
    assert "hit" in output


def test_cli_graph_text(tmp_path: Path, capsys) -> None:
    dag = _cli_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        """Provide a graph node."""
        del inputs, ctx
        return {"value": 1}

    assert _run_dag_cli(dag, ["graph"]) == 0
    assert "W0 x1" in capsys.readouterr().out


def test_cli_graph_html(tmp_path: Path, capsys) -> None:
    dag = _cli_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        """Provide a graph node."""
        del inputs, ctx
        return {"value": 1}

    output = tmp_path / "pipeline.html"
    assert _run_dag_cli(dag, ["graph", "--html", str(output)]) == 0
    assert output.exists()
    assert "<html>" in output.read_text(encoding="utf-8")
    assert str(output) in capsys.readouterr().out


def test_cli_explain(tmp_path: Path, capsys) -> None:
    dag = _cli_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        """Provide an artifact for cache explanation."""
        del inputs, ctx
        return {"value": 1}

    result = dag.run()

    assert _run_dag_cli(dag, ["explain", "source", "--run-id", result.run_id]) == 0
    output = capsys.readouterr().out
    assert "hit" in output or "miss" in output


def test_cli_describe_md(tmp_path: Path, capsys) -> None:
    dag = _cli_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        """Provide a declared source."""
        del inputs, ctx
        return {"value": 1}

    assert _run_dag_cli(dag, ["describe"]) == 0
    assert "| 节点 |" in capsys.readouterr().out


def test_cli_describe_json(tmp_path: Path, capsys) -> None:
    dag = _cli_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        """Provide a declared source."""
        del inputs, ctx
        return {"value": 1}

    assert _run_dag_cli(dag, ["describe", "--format", "json"]) == 0
    assert "source" in json.loads(capsys.readouterr().out)
