"""Command-line operations for configured kigumi projects."""

from __future__ import annotations

import argparse
import importlib
import json
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

from .artifacts import atomic_write_text, canonical_json
from .config import KigumiConfig, find_project_root, load_config, load_env
from .enforce import (
    Finding,
    RawIOFinding,
    check_paths,
    check_raw_io_node_paths,
    check_raw_io_node_source,
    check_source,
    raw_io_waiver_reasons,
    waiver_reasons,
)
from .inspect import diff_components, durable_run_state, load_call, trace_run
from .profile import WorkflowProfileError, load_run_profile
from .prompt import TemplateSlotError, load_template, render_template, slot_names
from .store import approve_checkpoint, diff_runs, gc_artifacts, run_directory, run_sort_key


def main(argv: list[str] | None = None) -> int:
    """Run the stdlib-only kigumi command-line interface."""
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command == "init":
        return _init(Path.cwd(), hooks=args.hooks)

    root = find_project_root(Path.cwd())
    try:
        config = load_config(root) if root is not None else None
    except ValueError as error:
        _error(str(error))
        return 2
    if config is None:
        _error("not a kigumi project (run: kigumi init)")
        return 2

    if args.command == "guard":
        return _guard(config, changed=args.changed)
    if args.command == "doctor":
        return _doctor(config)
    if args.command == "render":
        return _render(config, args.template, args.slot)
    if args.command == "runs":
        return _runs(
            config,
            args.runs_command,
            getattr(args, "run_id", None),
            json_output=args.json,
        )
    if args.command == "approve":
        return _approve(config, args.run_id, args.name, args.data)
    if args.command == "diff":
        return _diff(config, args.run_a, args.run_b, json_output=args.json)
    if args.command == "trace":
        return _trace(config, args.run_id, args.node, json_output=args.json)
    if args.command == "call":
        return _call(config, args.key_prefix, args.field)
    if args.command == "gc":
        return _gc(config, args.keep)
    parser.error("unknown command")
    return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kigumi")
    subcommands = parser.add_subparsers(dest="command", required=True)

    init = subcommands.add_parser("init")
    init.add_argument("--hooks", action="store_true")

    guard = subcommands.add_parser("guard")
    guard.add_argument("--changed", action="store_true")

    subcommands.add_parser("doctor")

    render = subcommands.add_parser("render")
    render.add_argument("template")
    render.add_argument("--slot", action="append", default=[])

    runs = subcommands.add_parser("runs")
    run_commands = runs.add_subparsers(dest="runs_command", required=True)
    run_list = run_commands.add_parser("list")
    run_list.add_argument("--json", action="store_true")
    show = run_commands.add_parser("show")
    show.add_argument("run_id")
    show.add_argument("--json", action="store_true")

    approve = subcommands.add_parser("approve")
    approve.add_argument("run_id")
    approve.add_argument("name")
    approve.add_argument("--data", default="{}")

    diff = subcommands.add_parser("diff")
    diff.add_argument("run_a")
    diff.add_argument("run_b")
    diff.add_argument("--json", action="store_true")

    trace = subcommands.add_parser("trace")
    trace.add_argument("run_id")
    trace.add_argument("--node")
    trace.add_argument("--json", action="store_true")

    call = subcommands.add_parser("call")
    call.add_argument("key_prefix")
    call.add_argument("--field", choices=("messages", "response", "reasoning", "meta"))

    gc = subcommands.add_parser("gc")
    gc.add_argument("--keep", type=int, required=True)
    return parser


def _init(root: Path, *, hooks: bool) -> int:
    pyproject = root / "pyproject.toml"
    if not pyproject.is_file():
        _error("no pyproject.toml found; run uv init first")
        return 1
    try:
        with pyproject.open("rb") as handle:
            document = tomllib.load(handle)
    except tomllib.TOMLDecodeError as error:
        _error(f"invalid pyproject.toml: {error}")
        return 1
    if isinstance(document.get("tool"), dict) and "kigumi" in document["tool"]:
        _error("[tool.kigumi] already exists")
        return 1
    hook_path = root / ".git" / "hooks" / "pre-commit"
    if hooks and not (root / ".git").is_dir():
        _error("cannot install hooks outside a git repository")
        return 1
    if hooks and hook_path.exists():
        _error("refusing to overwrite existing pre-commit hook")
        return 1

    block = (
        "\n\n[tool.kigumi]\n"
        'prompts_dir = "prompts"\n'
        'artifacts_dir = "artifacts"\n'
        'llm_cache_dir = "artifacts/_llm"\n'
        'source_dirs = ["nodes", "lib"]\n'
        'env_file = ".env"\n'
        "agent_slots = 1\n"
        'agent_lock_dir = "artifacts/_locks/agents"\n'
        "agent_slot_timeout_seconds = 300\n"
    )
    existing = pyproject.read_text(encoding="utf-8")
    atomic_write_text(pyproject, existing.rstrip() + block)
    config = KigumiConfig(project_root=root)
    for directory in [
        config.prompts_path,
        config.artifacts_path,
        config.llm_cache_path,
        *config.source_paths,
    ]:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / ".gitkeep").touch(exist_ok=True)
    _append_gitignore(root / ".gitignore", f"{config.artifacts_dir.rstrip('/')}/")
    if hooks:
        hook_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(hook_path, "#!/bin/sh\nuv run kigumi guard --changed\n")
        hook_path.chmod(0o755)
    print("initialized kigumi project")
    return 0


def _append_gitignore(path: Path, entry: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines() if path.is_file() else []
    if entry not in lines:
        lines.append(entry)
        atomic_write_text(path, "\n".join(lines) + "\n")


def _guard(config: KigumiConfig, *, changed: bool) -> int:
    if changed:
        paths = _changed_source_paths(config)
        if paths is None:
            return 2
        findings = [finding for path in paths for finding in _check_file(path)]
    else:
        findings = [
            *check_paths(config.source_paths),
            *check_raw_io_node_paths(config.source_paths),
        ]
    violations = [finding for finding in findings if not finding.waived]
    for finding in violations:
        location = _display_path(config.project_root, finding.path)
        print(f"{location}:{finding.lineno}: {finding.snippet}")
    for finding in findings:
        if finding.waived:
            print(
                "waiver "
                f"{_display_path(config.project_root, finding.path)}:{finding.lineno} "
                f"{finding.waiver_reason}"
            )
    if changed:
        _print_new_waivers(config.project_root, findings)
    return 1 if violations else 0


def _changed_source_paths(config: KigumiConfig) -> list[Path] | None:
    root = config.project_root
    probe = _git(root, "rev-parse", "--is-inside-work-tree")
    if probe.returncode != 0 or probe.stdout.strip() != "true":
        _error("--changed requires a git repository")
        return None
    changed: set[str] = set()
    for arguments in (
        ("diff", "--name-only", "HEAD"),
        ("diff", "--cached", "--name-only"),
        # git diff 看不见未跟踪文件;漏掉它们是静默的覆盖缺口。
        ("ls-files", "--others", "--exclude-standard"),
    ):
        result = _git(root, *arguments)
        if result.returncode != 0:
            _error("could not determine changed files")
            return None
        changed.update(line for line in result.stdout.splitlines() if line)
    paths: list[Path] = []
    for relative in sorted(changed):
        path = root / relative
        if path.suffix != ".py" or not path.is_file():
            continue
        if any(_is_within(path, source_dir) for source_dir in config.source_paths):
            paths.append(path)
    return paths


def _print_new_waivers(root: Path, findings: list[Finding | RawIOFinding]) -> None:
    # 按理由文本而非行号比对:上方任意编辑都会移动行号,行号比对既误报也漏报。
    head_reasons_by_path: dict[tuple[Path, bool], list[str]] = {}
    for finding in findings:
        if not finding.waived:
            continue
        is_raw_io = isinstance(finding, RawIOFinding)
        key = (finding.path, is_raw_io)
        if key not in head_reasons_by_path:
            relative = _display_path(root, finding.path)
            result = _git(root, "show", f"HEAD:{relative}")
            head_text = result.stdout if result.returncode == 0 else ""
            reasons = raw_io_waiver_reasons if is_raw_io else waiver_reasons
            head_reasons_by_path[key] = reasons(head_text)
        head_reasons = head_reasons_by_path[key]
        if finding.waiver_reason in head_reasons:
            head_reasons.remove(finding.waiver_reason)
        else:
            print(
                "new waiver: "
                f"{_display_path(root, finding.path)}:{finding.lineno} {finding.waiver_reason}"
            )


def _check_file(path: Path) -> list[Finding | RawIOFinding]:
    text = path.read_text(encoding="utf-8")
    return [*check_source(text, path), *check_raw_io_node_source(text, path)]


def _git(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=False,
        capture_output=True,
        text=True,
    )


def _is_within(path: Path, directory: Path) -> bool:
    try:
        path.resolve().relative_to(directory.resolve())
    except ValueError:
        return False
    return True


def _doctor(config: KigumiConfig) -> int:
    loaded = load_env(config.env_path)
    print(f"project root: {config.project_root}")
    print(f"prompts: {config.prompts_path} ({_existence(config.prompts_path)})")
    print(f"artifacts: {config.artifacts_path} ({_existence(config.artifacts_path)})")
    print(f"llm cache: {config.llm_cache_path} ({_existence(config.llm_cache_path)})")
    for source_path in config.source_paths:
        print(f"source: {source_path} ({_existence(source_path)})")
    print(f"env: {config.env_path} ({_existence(config.env_path)})")
    print(f"loaded env keys: {', '.join(loaded) if loaded else 'none'}")
    try:
        importlib.import_module("litellm")
    except ImportError:
        print("litellm: unavailable")
    else:
        print("litellm: available")
    template_count = (
        len(list(config.prompts_path.rglob("*.md"))) if config.prompts_path.is_dir() else 0
    )
    print(f"templates: {template_count}")
    return 0


def _existence(path: Path) -> str:
    return "present" if path.exists() else "missing"


def _render(config: KigumiConfig, template_name: str, specifications: list[str]) -> int:
    try:
        text = load_template(config.prompts_path / f"{template_name}.md")
        slots = {name: f"<{name}>" for name in slot_names(text)}
        for specification in specifications:
            if "=" not in specification:
                raise ValueError(f"invalid slot: {specification}")
            name, value = specification.split("=", 1)
            slots[name] = value
        rendered = render_template(text, slots)
        if "{{" in rendered:
            raise ValueError("unrendered template slots remain")
    except (FileNotFoundError, TemplateSlotError, ValueError) as error:
        _error(str(error))
        return 1
    print(rendered)
    return 0


def _runs(config: KigumiConfig, command: str, run_id: str | None, *, json_output: bool) -> int:
    if command == "list":
        runs: list[dict[str, Any]] = []
        for run_path in _run_directories(config.artifacts_path / "runs"):
            sidecars = list(run_path.glob("*.json.meta.json"))
            metadata = [_read_json(path) for path in sidecars]
            hits = sum(1 for item in metadata if item.get("cache") == "hit")
            misses = sum(1 for item in metadata if item.get("cache") == "miss")
            pending = _pending_names(run_path)
            durable = durable_run_state(run_path)
            runs.append(
                {
                    "run_id": run_path.name,
                    "nodes": len(sidecars),
                    "hits": hits,
                    "misses": misses,
                    "pending": len(pending),
                    "status": durable.get("run_status", "legacy"),
                    "pending_retries": len(durable.get("pending_retries", [])),
                    "ambiguous_attempts": len(durable.get("ambiguous_attempts", [])),
                }
            )
        if json_output:
            _print_json({"runs": runs})
        else:
            for run in runs:
                print(
                    f"{run['run_id']} nodes={run['nodes']} hits={run['hits']} "
                    f"misses={run['misses']} pending={run['pending']} "
                    f"status={run['status']} retries={run['pending_retries']} "
                    f"ambiguous={run['ambiguous_attempts']}"
                )
        return 0
    assert run_id is not None
    try:
        run_path = run_directory(config.artifacts_path, run_id)
    except ValueError as error:
        _error(str(error))
        return 1
    if not run_path.is_dir():
        _error(f"run not found: {run_id}")
        return 1
    workflow: dict[str, Any] | None = None
    if _read_json(run_path / "_run.json").get("run_manifest_schema") in {1, 2}:
        try:
            workflow = load_run_profile(run_path)
        except WorkflowProfileError as error:
            _error(str(error))
            return 1
    nodes: list[dict[str, Any]] = []
    runtime_nodes = (
        workflow["run"]["nodes"]
        if workflow is not None and isinstance(workflow.get("run"), dict)
        else None
    )
    if isinstance(runtime_nodes, list):
        node_sources = runtime_nodes
    else:
        node_sources = [
            {
                "target": sidecar.name.removesuffix(".json.meta.json"),
                **_read_json(sidecar),
            }
            for sidecar in sorted(run_path.glob("*.json.meta.json"))
        ]
    for metadata in node_sources:
        name = metadata.get("target", metadata.get("name"))
        calls = metadata.get("calls", [])
        call_count = len(calls) if isinstance(calls, list) else 0
        nodes.append(
            {
                "name": name,
                "cache": metadata.get("cache", "unknown"),
                "seconds": metadata.get("seconds", 0),
                "calls": call_count,
            }
        )
    pending = _pending_names(run_path)
    durable = durable_run_state(run_path)
    approvals = run_path / "approvals"
    approved: list[str] = []
    if approvals.is_dir():
        for approval in sorted(approvals.glob("*.json")):
            if not approval.name.endswith(".pending.json"):
                approved.append(approval.stem)
    if json_output:
        _print_json(
            {
                "run_id": run_id,
                "nodes": nodes,
                "pending": pending,
                "approved": approved,
                "status": durable.get("run_status", "legacy"),
                "attempts": durable.get("attempts", []),
                "retry_policy_digests": durable.get("retry_policy_digests", {}),
                "evidence_policy_digests": durable.get("evidence_policy_digests", {}),
                "pending_retries": durable.get("pending_retries", []),
                "ambiguous_attempts": durable.get("ambiguous_attempts", []),
                "workflow_profile": workflow,
            }
        )
    else:
        print(f"status: {durable.get('run_status', 'legacy')}")
        for entry in nodes:
            print(
                f"{entry['name']} cache={entry['cache']} seconds={entry['seconds']} "
                f"calls={entry['calls']}"
            )
        for name in pending:
            print(f"pending: {name}")
        for name in approved:
            print(f"approved: {name}")
        for attempt in durable.get("attempts", []):
            details = [
                f"attempt={attempt.get('attempt')}",
                f"status={attempt.get('status')}",
            ]
            if attempt.get("due_at") is not None:
                details.append(f"due_at={attempt['due_at']}")
            failure = attempt.get("failure")
            if isinstance(failure, dict):
                details.append(f"failure={canonical_json(failure)}")
            print(f"attempt: {attempt.get('target')} {' '.join(details)}")
        evidence = durable.get("evidence_policy_digests", {})
        if evidence:
            print(f"evidence policies: {canonical_json(evidence)}")
    return 0


def _run_directories(runs_root: Path) -> list[Path]:
    if not runs_root.is_dir():
        return []
    return sorted((path for path in runs_root.iterdir() if path.is_dir()), key=run_sort_key)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _pending_names(run_path: Path) -> list[str]:
    approvals = run_path / "approvals"
    if not approvals.is_dir():
        return []
    return sorted(
        path.name.removesuffix(".pending.json") for path in approvals.glob("*.pending.json")
    )


def _approve(config: KigumiConfig, run_id: str, name: str, data_text: str) -> int:
    try:
        data = json.loads(data_text)
        approve_checkpoint(config.artifacts_path / "runs", run_id, name, data)
    except (ValueError, json.JSONDecodeError) as error:
        _error(str(error))
        return 1
    print(f"approved {name} in {run_id}")
    return 0


def _diff(config: KigumiConfig, run_a: str, run_b: str, *, json_output: bool) -> int:
    for run_id in (run_a, run_b):
        try:
            run_path = run_directory(config.artifacts_path, run_id)
        except ValueError as error:
            _error(str(error))
            return 1
        if not run_path.is_dir():
            _error(f"run not found: {run_id}")
            return 1
    result = diff_runs(config.artifacts_path / "runs", run_a, run_b)
    components = diff_components(config.artifacts_path, run_a, run_b)
    if json_output:
        _print_json({**result, "components": components})
        return 0
    for name in ("changed", "only_a", "only_b"):
        print(f"{name}: {', '.join(result[name])}")
    print("components:")
    for name in sorted(key for key in components if key not in {"only_in_a", "only_in_b"}):
        change = components[name]
        if change == "unavailable":
            print(f"  {name}: unavailable")
        else:
            print(
                f"  {name}: changed={', '.join(change['changed'])} "
                f"unchanged={', '.join(change['unchanged'])}"
            )
    for name in ("only_in_a", "only_in_b"):
        print(f"  {name}: {', '.join(components[name])}")
    return 0


def _trace(config: KigumiConfig, run_id: str, node: str | None, *, json_output: bool) -> int:
    try:
        result = trace_run(config.artifacts_path, config.llm_cache_path, run_id, node)
    except (FileNotFoundError, ValueError) as error:
        _error(str(error))
        return 1
    if json_output:
        _print_json(result)
        return 0
    print(f"run: {result['run_id']}")
    if "run_status" in result:
        print(f"status: {result['run_status']}")
    for entry in result["nodes"]:
        _print_trace_node(entry, indent="")
    for attempt in result.get("attempts", []):
        line = (
            f"attempt {attempt.get('target')} #{attempt.get('attempt')} "
            f"status={attempt.get('status')}"
        )
        if attempt.get("due_at") is not None:
            line += f" due_at={attempt['due_at']}"
        if isinstance(attempt.get("failure"), dict):
            line += f" failure={canonical_json(attempt['failure'])}"
        print(line)
    if result.get("evidence_policy_digests"):
        print(f"evidence policies: {canonical_json(result['evidence_policy_digests'])}")
    for warning in result.get("warnings", []):
        print(f"warning: {warning}")
    return 0


def _print_trace_node(entry: dict[str, Any], *, indent: str) -> None:
    print(
        f"{indent}{entry['name']} cache={entry['cache']} seconds={entry['seconds']} "
        f"cache_key={entry['cache_key']}"
    )
    components = entry["key_components"]
    if components is not None:
        print(f"{indent}  key_components: {canonical_json(components)}")
    for call in entry["calls"]:
        print(
            f"{indent}  call {call['key']} model={call['model']} cache={call['cache']} "
            f"payload={call['payload_path']}"
        )
    for item in entry.get("items", []):
        _print_trace_node(item, indent=f"{indent}  ")


def _call(config: KigumiConfig, key_prefix: str, field: str | None) -> int:
    try:
        _key, payload = load_call(config.llm_cache_path, key_prefix)
    except (FileNotFoundError, ValueError) as error:
        _error(str(error))
        return 1
    if field == "response":
        response = payload.get("response")
        if not isinstance(response, str):
            _error("LLM payload response is not text")
            return 1
        print(response)
    else:
        _print_json(payload if field is None else payload.get(field))
    return 0


def _print_json(value: Any) -> None:
    print(canonical_json(value))


def _gc(config: KigumiConfig, keep_last: int) -> int:
    try:
        removed = gc_artifacts(config.artifacts_path, keep_last)
    except ValueError as error:
        _error(str(error))
        return 1
    print(f"deleted cache and blob entries: {removed}")
    return 0


def _display_path(root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


def _error(message: str) -> None:
    print(message, file=sys.stderr)
