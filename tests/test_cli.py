from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import kigumi.cli as cli_module
from kigumi import (
    PromptRef,
    PromptSpec,
    ProviderFailure,
    ProviderFailureKind,
    ProviderFailureStage,
    RetryPolicy,
)
from kigumi._runstate import RUN_SIDECAR_SCHEMA, SUCCESS_CANDIDATE_SCHEMA, AttemptStore
from kigumi.artifacts import atomic_write_json, canonical_json, sha, write_artifact
from kigumi.cli import _demote_brief_headings, _parser, _recovery_advice, main
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.docs import read_doc


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


def _cli_profile(nodes: list[str]) -> dict[str, Any]:
    return {
        "workflow_profile_schema": 2,
        "mode": "static",
        "resolution_status": "unresolved",
        "graph": {
            "nodes": [{"name": name} for name in nodes],
            "edges": [],
            "mounts": [],
            "models": {},
        },
        "prompts": {"specs": []},
        "run": None,
    }


def _write_completed_cli_run(
    run_path: Path,
    *,
    target: str,
    artifact: dict[str, Any],
    cache: str,
    cache_key: str,
    key_components: dict[str, str],
    seconds: float,
    calls: list[dict[str, Any]],
) -> None:
    """Create a complete schema-2 run fixture owned by durable runstate."""
    profile = _cli_profile([target])
    store = AttemptStore(
        run_path,
        {
            "workflow_profile": profile,
            "workflow_profile_digest": sha(profile),
        },
    )
    store.initialize()
    store.prepare(
        target,
        policy=None,
        declaration_digest=sha({"target": target}),
    )
    prompt_resolutions: dict[str, Any] = {}
    store.save_candidate(
        target,
        {
            "candidate_schema": SUCCESS_CANDIDATE_SCHEMA,
            "artifact": artifact,
            "cache_key": cache_key,
            "key_components": key_components,
            "prompt_resolutions": prompt_resolutions,
            "calls": calls,
        },
    )

    artifact_digest = sha(artifact)
    primary_call = calls[0] if len(calls) == 1 else {}
    origin = {
        "kind": "call" if calls else "code",
        "artifact_sha256": artifact_digest,
        "calls": calls,
        "agent": None,
        "prompt_resolutions": prompt_resolutions,
        "prompt_sha256": primary_call.get("prompt_sha"),
        "model": primary_call.get("model"),
        "params": primary_call.get("params") or {},
        "provider_response_id": primary_call.get("provider_response_id"),
        "usage": primary_call.get("usage"),
        "evidence_policy": {},
        "evidence_policy_digest": sha({}),
    }
    metadata = {
        "run_sidecar_schema": RUN_SIDECAR_SCHEMA,
        "node": target,
        "cache_key": cache_key,
        "cache": cache,
        "cache_policy": "auto",
        "key_components": key_components,
        "outputs": [],
        "seconds": seconds,
        "calls": calls,
        "execution_calls": calls,
        "prompt_resolutions": prompt_resolutions,
        "prompt_resolutions_digest": sha(prompt_resolutions),
        "origin_provenance": origin,
        "origin_provenance_digest": sha(origin),
        "artifact_sha256": artifact_digest,
        "prompt_sha256": origin["prompt_sha256"],
        "model": origin["model"],
        "params": origin["params"],
        "provider_response_id": origin["provider_response_id"],
        "usage": origin["usage"],
    }
    write_artifact(run_path / f"{target}.json", canonical_json(artifact), metadata)
    store.mark_completed(target, artifact_sha256=artifact_digest)
    store.update_manifest("completed")


def _write_pending_retry_cli_run(run_path: Path) -> str:
    """Create a valid pending retry with its state and attempt receipt bound."""
    profile = _cli_profile([])
    policy = RetryPolicy(
        max_attempts=2,
        initial_delay_seconds=3600,
        max_delay_seconds=3600,
        jitter="none",
    )
    evidence_digest = sha({"fixture": "evidence-policy"})
    store = AttemptStore(
        run_path,
        {
            "workflow_profile": profile,
            "workflow_profile_digest": sha(profile),
            "evidence_policy_digests": {"ask": evidence_digest},
            "retry_policy_digests": {"ask": policy.digest},
        },
    )
    store.initialize()
    store.prepare(
        "ask",
        policy=policy,
        declaration_digest=sha({"target": "ask"}),
    )
    failure = ProviderFailure(
        provider="fixture-provider",
        stage=ProviderFailureStage.PROVIDER,
        kind=ProviderFailureKind.RATE_LIMIT,
        status_code=429,
        retry_after_ms=0,
        provider_request_id=None,
        message_digest="a" * 64,
        retryable_hint=None,
    )
    state = store.record_failure("ask", failure, policy=policy)["state"]
    store.update_manifest(
        "pending_retry",
        pending_retries=[{"target": "ask", "due_at": state["due_at"]}],
        ambiguous_attempts=[],
    )
    return str(state["due_at"])


def _markdown_headings(text: str) -> list[tuple[int, str]]:
    headings: list[tuple[int, str]] = []
    in_fence = False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        match = re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if match:
            headings.append((len(match.group(1)), match.group(2)))
    return headings


def test_demote_brief_headings_preserves_leading_comments_in_fences() -> None:
    """Fenced comment lines remain exact while headings outside fences are demoted."""
    brief = (
        "# kigumi brief (read this first)\n\n"
        "## Section one\n\n"
        "```bash\n"
        "# this comment must not be demoted\n"
        "kigumi plan\n"
        "```\n\n"
        "```toml\n"
        "# neither must this one\n"
        'dag_entry = "nodes.graph:build_dag"\n'
        "```\n\n"
        "## Section two\n"
    )

    output = _demote_brief_headings(brief)
    output_lines = output.encode().splitlines()

    assert output_lines[0] == b"## kigumi"
    assert b"### Section one" in output_lines
    assert b"### Section two" in output_lines
    assert b"# this comment must not be demoted" in output_lines
    assert b"# neither must this one" in output_lines
    assert b"## this comment must not be demoted" not in output_lines
    assert b"## neither must this one" not in output_lines


@pytest.mark.parametrize(
    ("run_id", "target"),
    [
        ("run with spaces", "target with spaces"),
        ("run 'with' \"quotes\"", "target 'with' \"quotes\""),
        ("run;$(touch should-not-run)", "target && echo unsafe"),
    ],
)
def test_recovery_advice_shell_quotes_dynamic_arguments(run_id: str, target: str) -> None:
    """The copy/paste recovery and resume commands preserve argv boundaries."""
    advice = _recovery_advice(run_id, target, 7)
    recovery_line = next(
        line.strip() for line in advice.splitlines() if line.strip().startswith("kigumi recover ")
    )
    resume_line = next(
        line.strip().removeprefix("Then explicitly run: ")
        for line in advice.splitlines()
        if line.strip().startswith("Then explicitly run: kigumi resume ")
    )

    assert shlex.split(recovery_line) == [
        "kigumi",
        "recover",
        "--attempt",
        "7",
        "--decision",
        "retry_after_external_check",
        "--reason",
        "<explanation>",
        "--",
        run_id,
        target,
    ]
    assert shlex.split(resume_line) == ["kigumi", "resume", "--", run_id]

    parser = _parser()
    recovery_args = parser.parse_args(shlex.split(recovery_line)[1:])
    resume_args = parser.parse_args(shlex.split(resume_line)[1:])
    assert recovery_args.run_id == run_id
    assert recovery_args.target == target
    assert resume_args.run_id == run_id


def test_recovery_advice_warns_to_reuse_graph_args() -> None:
    """Advice must not pretend it can reconstruct graph factory arguments."""
    lines = _recovery_advice("run-0042", "transcode", 3).splitlines()
    command_index = next(
        index for index, line in enumerate(lines) if line.strip().startswith("kigumi recover ")
    )

    assert "--" in lines[command_index]
    assert "--" in lines[command_index + 2]
    assert lines[command_index + 1] == (
        "Before the `--` separator on both commands, add the same actual repeated "
        "--graph-arg KEY=VALUE options used to construct this run; run state cannot "
        "reconstruct them, and placeholder values are invalid."
    )


def test_recovery_advice_graph_args_can_be_added_before_separator() -> None:
    """The documented graph-argument insertion point must remain parser-valid."""
    advice = _recovery_advice("--historical-run", "--work", 3)
    lines = advice.splitlines()
    recovery_line = next(
        line.strip() for line in lines if line.strip().startswith("kigumi recover ")
    )
    resume_line = next(
        line.strip().removeprefix("Then explicitly run: ")
        for line in lines
        if line.strip().startswith("Then explicitly run: kigumi resume ")
    )
    parser = _parser()

    recovery_argv = shlex.split(recovery_line)
    recovery_separator = recovery_argv.index("--")
    recovery_argv[recovery_separator:recovery_separator] = [
        "--graph-arg",
        "episode=E2S4",
        "--graph-arg",
        "profile=production",
    ]
    recovery_args = parser.parse_args(recovery_argv[1:])

    resume_argv = shlex.split(resume_line)
    resume_separator = resume_argv.index("--")
    resume_argv[resume_separator:resume_separator] = [
        "--graph-arg",
        "episode=E2S4",
        "--graph-arg",
        "profile=production",
    ]
    resume_args = parser.parse_args(resume_argv[1:])

    assert recovery_args.graph_arg == ["episode=E2S4", "profile=production"]
    assert resume_args.graph_arg == recovery_args.graph_arg
    assert recovery_args.run_id == "--historical-run"
    assert recovery_args.target == "--work"
    assert resume_args.run_id == "--historical-run"


def test_init_creates_default_layout_and_is_idempotent(tmp_path: Path, monkeypatch, capsys) -> None:
    """Init scaffolds once; the agent-docs sentinel makes repeat init idempotent."""
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
    config_text = (tmp_path / "pyproject.toml").read_text(encoding="utf-8")
    assert "agent_slots = 1" in config_text
    assert 'agent_lock_dir = "artifacts/_locks/agents"' in config_text
    assert main(["init"]) == 0
    assert (tmp_path / "pyproject.toml").read_text(encoding="utf-8") == config_text
    assert "synchronized kigumi agent docs" in capsys.readouterr().out


def test_installed_console_script_init_check_and_generated_graph_run(tmp_path: Path) -> None:
    """The installed command must scaffold and execute a real downstream graph."""
    executable = Path(sys.executable).with_name("kigumi")
    assert executable.is_file(), f"expected installed console script at {executable}"
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'sample'\n", encoding="utf-8")

    def run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(executable), *arguments],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=False,
        )

    initialized = run_cli("init")
    assert initialized.returncode == 0, initialized.stderr
    checked = run_cli("check")
    assert checked.returncode == 0, checked.stderr

    executed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from nodes.graph import build_dag; "
                "result = build_dag().run(run_id='installed-init'); "
                "assert result.artifacts['example'] == {'ok': 'replace me'}"
            ),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert executed.returncode == 0, executed.stderr


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


def test_init_existing_project_does_not_scaffold_and_keeps_hooks_explicit(
    tmp_path: Path, monkeypatch
) -> None:
    """An existing config gets docs only; hooks remain an explicit opt-in."""
    root = tmp_path / "existing"
    root.mkdir()
    pyproject = root / "pyproject.toml"
    pyproject.write_text(
        "[project]\nname = 'existing'\n\n[tool.kigumi]\nsource_dirs = ['src']\n",
        encoding="utf-8",
    )
    _git(root, "init")
    original_pyproject = pyproject.read_text(encoding="utf-8")
    monkeypatch.chdir(root)

    assert main(["init"]) == 0
    assert pyproject.read_text(encoding="utf-8") == original_pyproject
    assert not (root / "prompts").exists()
    assert not (root / "artifacts").exists()
    assert not (root / "nodes").exists()
    assert not (root / "lib").exists()
    hook = root / ".git" / "hooks" / "pre-commit"
    assert not hook.exists()

    assert main(["init", "--hooks"]) == 0
    assert "uv run kigumi guard --changed" in hook.read_text(encoding="utf-8")
    assert main(["init", "--hooks"]) == 1


def test_init_preflights_conflicting_destinations_before_mutating(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """A destination conflict leaves the pyproject and project tree untouched."""
    pyproject = tmp_path / "pyproject.toml"
    original = "[project]\nname = 'sample'\n"
    pyproject.write_text(original, encoding="utf-8")
    (tmp_path / "nodes").write_text("project code\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert main(["init"]) == 1
    assert pyproject.read_text(encoding="utf-8") == original
    assert not (tmp_path / "prompts").exists()
    assert not (tmp_path / "artifacts").exists()
    assert "must be a directory" in capsys.readouterr().err


def test_init_rejects_directory_symlink_before_mutating(tmp_path: Path, monkeypatch) -> None:
    """Init never scaffolds through a user-controlled directory symlink."""
    pyproject = tmp_path / "pyproject.toml"
    original = "[project]\nname = 'sample'\n"
    pyproject.write_text(original, encoding="utf-8")
    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "artifacts"
    try:
        linked.symlink_to(outside, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"target filesystem does not support directory symlinks: {error}")
    monkeypatch.chdir(tmp_path)

    assert main(["init"]) == 1
    assert pyproject.read_text(encoding="utf-8") == original
    assert not (outside / ".gitkeep").exists()
    assert not (tmp_path / "prompts").exists()


def test_init_rolls_back_after_a_late_write_failure(tmp_path: Path, monkeypatch) -> None:
    """A failure after pyproject and directory writes restores every prior mutation."""
    pyproject = tmp_path / "pyproject.toml"
    original = "[project]\nname = 'sample'\n"
    pyproject.write_text(original, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    real_write = cli_module.atomic_write_text

    def fail_on_agent_rules(path: Path, text: str) -> None:
        if Path(path).name == "AGENTS.md":
            raise OSError("injected init write failure")
        real_write(path, text)

    monkeypatch.setattr(cli_module, "atomic_write_text", fail_on_agent_rules)

    assert main(["init"]) == 1
    assert pyproject.read_text(encoding="utf-8") == original
    for path in (
        tmp_path / "prompts",
        tmp_path / "artifacts",
        tmp_path / "nodes",
        tmp_path / "lib",
        tmp_path / "CLAUDE.md",
        tmp_path / "AGENTS.md",
    ):
        assert not path.exists(), path


def test_init_writes_agent_docs_for_claude_and_codex(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """教训 agent_docs_auto: init always writes kigumi guidance into CLAUDE.md and AGENTS.md."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'sample'\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 0

    for filename in ("CLAUDE.md", "AGENTS.md"):
        text = (tmp_path / filename).read_text(encoding="utf-8")
        assert "kigumi" in text
        assert "kigumi brief" in text
        assert "kigumi plan" in text
        assert "ctx.call" in text


def test_init_appends_to_existing_agent_docs_without_duplication(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Existing agent docs get one appended section, guarded by the sentinel on repeat init."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'sample'\n", encoding="utf-8")
    existing = "# My Project\n\nCustom rules here.\n"
    (tmp_path / "CLAUDE.md").write_text(existing, encoding="utf-8")
    (tmp_path / "AGENTS.md").write_text(existing, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 0
    first = {
        filename: (tmp_path / filename).read_text(encoding="utf-8")
        for filename in ("CLAUDE.md", "AGENTS.md")
    }

    for filename in ("CLAUDE.md", "AGENTS.md"):
        text = first[filename]
        assert text.startswith(existing)
        assert "kigumi" in text

    assert main(["init"]) == 0
    for filename, text in first.items():
        repeated = (tmp_path / filename).read_text(encoding="utf-8")
        assert repeated == text
        assert repeated.count("<!-- kigumi-agent-docs -->") == 1


def test_init_existing_kigumi_syncs_docs_without_scaffolding_or_changing_pyproject(
    tmp_path: Path, monkeypatch
) -> None:
    """Existing projects sync missing guidance while preserving config and custom text.

    The sentinel keeps repeated synchronization idempotent.
    """
    pyproject = tmp_path / "pyproject.toml"
    original_pyproject = (
        "[project]\nname = 'sample'\n\n"
        "[tool.kigumi]\nsource_dirs = ['src']\ncustom_setting = 'keep'\n"
    )
    pyproject.write_text(original_pyproject, encoding="utf-8")
    custom = "# Existing project\n\nKeep this custom rule.  \n"
    (tmp_path / "CLAUDE.md").write_text(custom, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert main(["init"]) == 0
    assert pyproject.read_text(encoding="utf-8") == original_pyproject
    claude = (tmp_path / "CLAUDE.md").read_text(encoding="utf-8")
    agents = (tmp_path / "AGENTS.md").read_text(encoding="utf-8")
    assert claude.startswith(custom)
    assert "<!-- kigumi-agent-docs -->" in claude
    assert "<!-- kigumi-agent-docs -->" in agents
    for path in (
        tmp_path / "prompts",
        tmp_path / "artifacts",
        tmp_path / "nodes",
        tmp_path / "lib",
        tmp_path / "nodes" / "graph.py",
        tmp_path / ".gitignore",
    ):
        assert not path.exists(), path

    first_docs = {
        filename: (tmp_path / filename).read_text(encoding="utf-8")
        for filename in ("CLAUDE.md", "AGENTS.md")
    }
    assert main(["init"]) == 0
    assert pyproject.read_text(encoding="utf-8") == original_pyproject
    for filename, text in first_docs.items():
        repeated = (tmp_path / filename).read_text(encoding="utf-8")
        assert repeated == text
        assert repeated.count("<!-- kigumi-agent-docs -->") == 1


def test_init_demotes_injected_headings_into_kigumi_section(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Injected brief sections are nested below the kigumi root heading."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'sample'\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 0

    headings = _markdown_headings((tmp_path / "CLAUDE.md").read_text(encoding="utf-8"))

    assert [level for level, _ in headings] == [2, 3, 3, 3, 3, 3, 3]
    assert headings[0] == (2, "kigumi")
    assert [title for _, title in headings[1:]] == [
        "Code the framework cannot see",
        "Do not reimplement",
        "Run these before and after every change",
        "Project commands and graph commands",
        "Working rules",
        "Where to read more",
    ]


def test_init_preserves_fenced_code_comments_when_demoting_headings(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Heading demotion leaves fenced code, including comments, byte-identical."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'sample'\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 0

    text = (tmp_path / "CLAUDE.md").read_text(encoding="utf-8")

    assert "### Project commands and graph commands" in text
    brief = read_doc("brief")
    for comment in (
        "# kigumi init scaffolds this",
        "# current state: nodes, map items, every LLM call",
    ):
        original_line = next(line for line in brief.splitlines() if comment in line)
        injected_line = next(line for line in text.splitlines() if comment in line)
        assert injected_line.encode() == original_line.encode()
    assert "## kigumi init scaffolds this" not in text
    assert "## current state: nodes, map items, every LLM call" not in text


def test_init_injects_framework_boundaries_guidance(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Injected guidance names both source scanning and graph registration boundaries."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'sample'\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 0

    text = (tmp_path / "CLAUDE.md").read_text(encoding="utf-8")

    assert "source_dirs" in text
    assert "dag_entry" in text


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
    _write_completed_cli_run(
        run_a,
        target="node",
        artifact={"value": "a"},
        cache="miss",
        cache_key="node-key",
        key_components={"prompt": "a"},
        seconds=1.5,
        calls=[call],
    )
    _write_completed_cli_run(
        run_b,
        target="node",
        artifact={"value": "b"},
        cache="hit",
        cache_key="node-key",
        key_components={"prompt": "b"},
        seconds=0.0,
        calls=[],
    )
    atomic_write_json(
        root / "artifacts" / "_llm" / "llm" / "call-key-123.json",
        {
            "meta": call,
            "messages": [{"role": "user", "content": "hello"}],
            "response": "world",
            "response_sha256": sha("world"),
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


def test_cli_call_reports_corrupt_l1_payload_without_replaying_it(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = _project(tmp_path)
    atomic_write_json(
        root / "artifacts" / "_llm" / "llm" / "abc123.json",
        {"meta": {"key": "abc123"}, "response": "world"},
    )
    monkeypatch.chdir(root)

    assert main(["call", "abc123", "--field", "response"]) == 1

    assert "Corrupt cache" in capsys.readouterr().err


def test_runs_show_and_trace_include_durable_attempt_state(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = _project(tmp_path)
    run = root / "artifacts" / "runs" / "durable"
    due_at = _write_pending_retry_cli_run(run)
    monkeypatch.chdir(root)

    assert main(["runs", "show", "durable", "--json"]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["status"] == "pending_retry"
    assert shown["attempts"][0]["failure"]["provider_failure"]["kind"] == "rate_limit"
    assert shown["attempts"][0]["due_at"] == due_at
    assert shown["evidence_policy_digests"]["ask"] == sha({"fixture": "evidence-policy"})
    assert shown["workflow_profile"]["resolution_status"] == "available"

    assert main(["trace", "durable", "--json"]) == 0
    traced = json.loads(capsys.readouterr().out)
    assert traced["attempts"][0]["due_at"] == due_at
    assert traced["run_status"] == "pending_retry"


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
    retained_blob = "retained"
    stale_blob = "stale"
    _write_completed_cli_run(
        run_a,
        target="node",
        artifact={"value": "one"},
        cache="miss",
        cache_key="keep",
        key_components={},
        seconds=1.5,
        calls=[{}],
    )
    _write_completed_cli_run(
        run_b,
        target="node",
        artifact={"value": "two", "file": {"kigumi_blob": retained_blob}},
        cache="hit",
        cache_key="keep",
        key_components={},
        seconds=0.0,
        calls=[],
    )
    atomic_write_json(run_a / "approvals" / "editor.pending.json", {"question": "approve"})
    cache_root = artifacts / "_cache" / "nodes"
    atomic_write_json(cache_root / "keep.json", {"artifact": {}})
    atomic_write_json(cache_root / "old.json", {"artifact": {}})
    blobs_root = artifacts / "_cache" / "blobs"
    blobs_root.mkdir(parents=True)
    (blobs_root / retained_blob).write_bytes(b"keep")
    (blobs_root / stale_blob).write_bytes(b"remove")
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


def test_runs_list_rejects_external_sidecar_symlinks(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _project(tmp_path)
    run_path = root / "artifacts" / "runs" / "owned"
    _write_completed_cli_run(
        run_path,
        target="node",
        artifact={"value": "safe"},
        cache="miss",
        cache_key="safe-cache",
        key_components={},
        seconds=0.0,
        calls=[],
    )
    external = tmp_path / "external-sidecar.json"
    external.write_text('{"cache": "hit", "secret": "must-not-leak"}', encoding="utf-8")
    try:
        (run_path / "escape.json.meta.json").symlink_to(external)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"filesystem does not support file symlinks: {error}")

    monkeypatch.chdir(root)
    assert main(["runs", "list", "--json"]) == 1
    captured = capsys.readouterr()
    assert "symlink" in captured.err
    assert "must-not-leak" not in captured.out + captured.err


def test_runs_list_and_show_reject_symlinked_run_directories(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = _project(tmp_path)
    runs = root / "artifacts" / "runs"
    runs.mkdir(parents=True)
    external = tmp_path / "external-run"
    external.mkdir()
    (external / "_run.json").write_text(
        '{"secret": "must-not-leak"}',
        encoding="utf-8",
    )
    try:
        (runs / "escape").symlink_to(external, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"filesystem does not support directory symlinks: {error}")

    monkeypatch.chdir(root)
    assert main(["runs", "list"]) == 1
    listed = capsys.readouterr()
    assert "symlink" in listed.err
    assert "must-not-leak" not in listed.out + listed.err
    assert main(["runs", "show", "escape"]) == 1
    shown = capsys.readouterr()
    assert "symlink" in shown.err
    assert "must-not-leak" not in shown.out + shown.err


def test_runs_show_rejects_external_approval_directory_symlink(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = _project(tmp_path)
    run_path = root / "artifacts" / "runs" / "owned"
    _write_completed_cli_run(
        run_path,
        target="node",
        artifact={"value": "safe"},
        cache="miss",
        cache_key="safe-cache",
        key_components={},
        seconds=0.0,
        calls=[],
    )
    external = tmp_path / "external-approvals"
    external.mkdir()
    (external / "leaked.json").write_text(
        '{"secret": "must-not-leak"}',
        encoding="utf-8",
    )
    approvals = run_path / "approvals"
    try:
        approvals.symlink_to(external, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"filesystem does not support directory symlinks: {error}")

    monkeypatch.chdir(root)
    assert main(["runs", "show", "owned", "--json"]) == 1
    captured = capsys.readouterr()
    assert "Unable to inspect Pending approval directory" in captured.err
    assert "must-not-leak" not in captured.out + captured.err


def test_runs_show_rejects_fifo_sidecar_without_blocking(tmp_path: Path) -> None:
    root = _project(tmp_path)
    run_path = root / "artifacts" / "runs" / "fifo"
    _write_completed_cli_run(
        run_path,
        target="node",
        artifact={"value": "safe"},
        cache="miss",
        cache_key="safe-cache",
        key_components={},
        seconds=0.0,
        calls=[],
    )
    fifo = run_path / "blocked.json.meta.json"
    if not hasattr(os, "mkfifo"):
        pytest.skip("filesystem does not support FIFOs")
    try:
        os.mkfifo(fifo)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"filesystem does not support FIFOs: {error}")

    project_root = str(Path(__file__).resolve().parents[1])
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (project_root, env.get("PYTHONPATH")) if part
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from kigumi.cli import main; raise SystemExit(main(['runs', 'show', 'fifo']))",
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=2,
        check=False,
    )
    assert result.returncode == 1
    assert "regular file" in result.stderr


def test_runs_list_fails_closed_after_directory_replacement(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = _project(tmp_path)
    run_path = root / "artifacts" / "runs" / "run-1"
    _write_pending_retry_cli_run(run_path)
    replacement = tmp_path / "replacement" / "run-1"
    _write_completed_cli_run(
        replacement,
        target="external",
        artifact={"value": "external"},
        cache="hit",
        cache_key="external-cache",
        key_components={},
        seconds=0.0,
        calls=[],
    )
    moved = tmp_path / "original-run"
    original_durable = cli_module.durable_run_state
    swapped = False

    def replace_before_durable(path: Path, *, _store=None, **kwargs: Any) -> dict[str, Any]:
        nonlocal swapped
        if not swapped:
            run_path.rename(moved)
            replacement.rename(run_path)
            swapped = True
        assert _store is not None
        return original_durable(path, _store=_store, **kwargs)

    monkeypatch.setattr(cli_module, "durable_run_state", replace_before_durable)
    monkeypatch.chdir(root)

    assert main(["runs", "list", "--json"]) == 1
    captured = capsys.readouterr()
    assert "no longer owned" in captured.err
    assert "external" not in captured.out + captured.err


def test_runs_show_fails_closed_after_directory_replacement(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = _project(tmp_path)
    run_path = root / "artifacts" / "runs" / "run-1"
    _write_completed_cli_run(
        run_path,
        target="original",
        artifact={"value": "original"},
        cache="miss",
        cache_key="original-cache",
        key_components={"source": "original"},
        seconds=1.0,
        calls=[],
    )
    replacement = tmp_path / "replacement" / "run-1"
    _write_completed_cli_run(
        replacement,
        target="external",
        artifact={"value": "external"},
        cache="hit",
        cache_key="external-cache",
        key_components={"source": "external"},
        seconds=0.0,
        calls=[],
    )
    moved = tmp_path / "original-run"
    original_profile = cli_module._load_run_profile_owned
    swapped = False

    def replace_before_profile(path: Path, store: AttemptStore, **kwargs: Any) -> dict[str, Any]:
        nonlocal swapped
        if not swapped:
            run_path.rename(moved)
            replacement.rename(run_path)
            swapped = True
        return original_profile(path, store, **kwargs)

    monkeypatch.setattr(cli_module, "_load_run_profile_owned", replace_before_profile)
    monkeypatch.chdir(root)

    assert main(["runs", "show", "run-1", "--json"]) == 1
    captured = capsys.readouterr()
    assert "no longer owned" in captured.err
    assert "external" not in captured.out + captured.err


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


def test_cli_check_reports_source_syntax_error_as_nonzero_diagnostic(
    tmp_path: Path, capsys
) -> None:
    nodes = tmp_path / "nodes"
    nodes.mkdir()
    (nodes / "broken.py").write_text("def broken(:\n    pass\n", encoding="utf-8")
    dag = _cli_dag(tmp_path, source_dirs=["nodes"])

    assert _run_dag_cli(dag, ["check"]) == 1
    output = capsys.readouterr().out
    assert "source:" in output
    assert "invalid syntax" in output or "SyntaxError" in output


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


def test_cli_profile_and_prompt_graph_share_canonical_ir(tmp_path: Path, capsys) -> None:
    prompts = tmp_path / "prompts"
    prompts.mkdir()
    (prompts / "base.md").write_text("managed", encoding="utf-8")
    dag = _cli_dag(tmp_path)

    @dag.node("source", prompt_specs=(PromptSpec("managed", PromptRef("base")),))
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"prompt": ctx.resolve_prompt("managed")}

    assert _run_dag_cli(dag, ["profile", "--format", "json"]) == 0
    profile = json.loads(capsys.readouterr().out)
    assert profile["workflow_profile_schema"] == 2
    assert profile["prompts"]["specs"][0]["name"] == "managed"

    assert _run_dag_cli(dag, ["graph", "--prompts"]) == 0
    assert "flowchart TD" in capsys.readouterr().out

    result = dag.run(run_id="profile-cli")
    assert (
        _run_dag_cli(
            dag,
            ["profile", "--run-id", result.run_id, "--format", "md"],
        )
        == 0
    )
    rendered = capsys.readouterr().out
    assert "Workflow Profile (run)" in rendered
    assert "| source | managed | base |" in rendered


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


def test_cli_describe_md_shows_prompt_spec_name(tmp_path: Path, capsys) -> None:
    prompts = tmp_path / "prompts"
    prompts.mkdir()
    (prompts / "base.md").write_text("source", encoding="utf-8")
    dag = _cli_dag(tmp_path)

    @dag.node("source", prompt_specs=(PromptSpec("source_prompt", PromptRef("base")),))
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        del inputs, ctx
        return {"value": 1}

    assert _run_dag_cli(dag, ["describe"]) == 0
    assert "| source | - | auto |  | node |  |  |  | source_prompt |" in capsys.readouterr().out


def test_cli_describe_json(tmp_path: Path, capsys) -> None:
    dag = _cli_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        """Provide a declared source."""
        del inputs, ctx
        return {"value": 1}

    assert _run_dag_cli(dag, ["describe", "--format", "json"]) == 0
    assert "source" in json.loads(capsys.readouterr().out)
