"""Command-line operations for configured kigumi projects."""

from __future__ import annotations

import argparse
import importlib
import inspect
import json
import re
import shlex
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

from ._runstate import RUN_MANIFEST_SCHEMA
from .artifacts import atomic_write_text, canonical_json
from .config import KigumiConfig, find_project_root, load_config, load_env
from .dag import GRAPH_COMMAND_HELP, Dag, register_graph_commands
from .docs import SHIPPED_DOCS, read_doc
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

DAG_ENTRY_MODULE = "nodes.graph"
"""Module `kigumi init` scaffolds, matching the default ``source_dirs`` entry."""

DAG_ENTRY_TEMPLATE = '''"""Build this project's DAG.

`build_dag()` is the single place that constructs the graph. Both entry points call
it, so they always inspect the same topology:

    kigumi describe          # via [tool.kigumi] dag_entry
    dag describe             # via [project.scripts], if you register main below

Importing this module must register every node and stay free of side effects: the
graph commands import it to read the topology without running anything.

If the graph's shape or params depend on runtime input, give `build_dag` keyword
parameters and pass them per invocation, so the graph commands inspect the same
graph a real run builds:

    def build_dag(episode: str) -> Dag: ...
    kigumi plan --graph-arg episode=E2S4

Do not default them to placeholder values to keep the commands quiet: params are
cache-key components, so a placeholder makes `plan` forecast a key space nothing
will ever run in and `explain` report every node as changed.
"""

from __future__ import annotations

from pathlib import Path

from kigumi import Dag, KigumiConfig, LLMCaller, LiteLLMTransport, find_project_root


def build_dag() -> Dag:
    """Return the fully registered DAG."""
    root = find_project_root(Path(__file__)) or Path.cwd()
    config = KigumiConfig(project_root=root)
    caller = LLMCaller(
        LiteLLMTransport(),
        cache_dir=config.llm_cache_path,
        seed=0,
    )
    dag = Dag(config, caller)

    @dag.node("example", prompt_specs=())
    def example(inputs, ctx) -> dict[str, str]:
        """Replace this with a real node; the docstring is required by `check`."""
        del inputs, ctx
        return {"ok": "replace me"}

    return dag


def main(argv: list[str] | None = None) -> None:
    """Entry point for a standalone `dag` command."""
    build_dag().cli(argv)


if __name__ == "__main__":
    main()
'''


def main(argv: list[str] | None = None) -> int:
    """Run the stdlib-only kigumi command-line interface."""
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command == "init":
        return _init(Path.cwd(), hooks=args.hooks)
    # Documentation is readable without a configured project: an agent exploring an
    # unfamiliar repository needs the capability surface before anything is set up.
    if args.command == "brief":
        return _print_doc("brief")
    if args.command == "docs":
        return _docs(args.name)

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
    if args.command in GRAPH_COMMAND_HELP:
        return _graph_command(config, args)
    parser.error("unknown command")
    return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kigumi")
    subcommands = parser.add_subparsers(dest="command", required=True)

    init = subcommands.add_parser("init")
    init.add_argument("--hooks", action="store_true")

    subcommands.add_parser("brief", help="print the agent brief: what kigumi already owns")

    docs = subcommands.add_parser("docs", help="list or print shipped documentation")
    docs.add_argument("name", nargs="?", choices=[doc.name for doc in SHIPPED_DOCS])

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

    # The graph commands need the in-memory Dag, so they are only reachable when
    # [tool.kigumi] declares dag_entry. They are always listed: an agent must be able
    # to discover that `kigumi plan` exists from --help alone, and get a message that
    # names the missing key rather than "unknown command".
    # graph_args only makes sense here: this entry point builds the graph, so it is the
    # one that can forward runtime arguments to the factory.
    register_graph_commands(subcommands, graph_args=True)

    gc = subcommands.add_parser("gc")
    gc.add_argument("--keep", type=int, required=True)
    return parser


def _graph_command(config: KigumiConfig, args: argparse.Namespace) -> int:
    """Import the project's graph and dispatch one graph command onto it."""
    try:
        graph_args = _parse_graph_args(getattr(args, "graph_arg", []))
    except ValueError as error:
        _error(str(error))
        return 2
    dag = _load_dag(config, graph_args)
    if dag is None:
        return 2
    return dag.run_command(args)


class GraphEntryParameters:
    """What a ``dag_entry`` factory accepts, and how to bind ``--graph-arg`` to it."""

    def __init__(
        self,
        required: tuple[str, ...],
        optional: tuple[str, ...],
        variadic: bool,
    ) -> None:
        self.required = required
        self.optional = optional
        self.variadic = variadic

    @property
    def accepted(self) -> tuple[str, ...]:
        return self.required + self.optional

    def bind(self, provided: dict[str, str]) -> dict[str, str]:
        """Return the keyword arguments to call the factory with, or explain the gap."""
        # A factory taking **kwargs decides its own parameter names; second-guessing it
        # here would reject arguments it is willing to accept.
        if not self.variadic:
            unknown = sorted(set(provided) - set(self.accepted))
            if unknown:
                raise ValueError(
                    f"does not accept --graph-arg {', '.join(repr(name) for name in unknown)}; "
                    f"it accepts {_render_names(self.accepted)}"
                )
        missing = [name for name in self.required if name not in provided]
        if missing:
            raise ValueError(
                f"needs {_render_names(tuple(missing))} to build the graph; pass "
                + " ".join(f"--graph-arg {name}=<value>" for name in missing)
                + ". Pass the same values a real run uses: params are cache-key "
                "components, so a placeholder makes plan and explain describe a graph "
                "that will never run"
            )
        return {name: provided[name] for name in provided}


def _render_names(names: tuple[str, ...]) -> str:
    return ", ".join(repr(name) for name in names) if names else "no arguments"


def graph_entry_parameters(factory: Any) -> GraphEntryParameters:
    """Describe a graph factory's keyword-passable parameters, refusing shapes CLI cannot fill."""
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError) as error:
        raise ValueError(f"signature cannot be inspected: {error}") from error
    required: list[str] = []
    optional: list[str] = []
    variadic = False
    for parameter in signature.parameters.values():
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            variadic = True
            continue
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            continue
        if parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
            # There is no key=value spelling for these, so failing here names the real
            # problem instead of reporting the parameter as merely missing.
            raise ValueError(
                f"has positional-only parameter {parameter.name!r}, which --graph-arg "
                "cannot fill; make it keyword-passable"
            )
        if parameter.default is inspect.Parameter.empty:
            required.append(parameter.name)
        else:
            optional.append(parameter.name)
    return GraphEntryParameters(tuple(required), tuple(optional), variadic)


def _parse_graph_args(specifications: list[str]) -> dict[str, str]:
    """Parse ``key=value`` graph arguments, rejecting shapes that hide intent."""
    parsed: dict[str, str] = {}
    for specification in specifications:
        key, separator, value = specification.partition("=")
        if not separator or not key.strip():
            raise ValueError(
                f"invalid --graph-arg {specification!r}; expected key=value "
                "(the first '=' separates them, later ones belong to the value)"
            )
        name = key.strip()
        if name in parsed:
            raise ValueError(
                f"--graph-arg {name!r} was given more than once; "
                "silently keeping one of them would build a graph you cannot reason about"
            )
        parsed[name] = value
    return parsed


def _load_dag(config: KigumiConfig, graph_args: dict[str, str] | None = None) -> Any | None:
    """Build the project's ``Dag`` from ``dag_entry``, reporting setup errors."""
    if config.dag_entry is None:
        _error(
            "[tool.kigumi] dag_entry is not set, so the graph is not reachable from "
            'this CLI; add dag_entry = "module:callable" pointing at a function that '
            "returns your Dag (see: kigumi docs cli)"
        )
        return None
    module_name, _, attribute = config.dag_entry.partition(":")
    # The project's own modules are importable relative to its root, which is not
    # guaranteed to be on sys.path when kigumi is invoked as an installed script.
    root = str(config.project_root)
    if root not in sys.path:
        sys.path.insert(0, root)
    try:
        module = importlib.import_module(module_name)
    except ImportError as error:
        _error(f"dag_entry module {module_name!r} is not importable: {error}")
        return None
    factory = getattr(module, attribute, None)
    if factory is None:
        _error(f"dag_entry {config.dag_entry!r} not found: {module_name} has no {attribute!r}")
        return None
    if not callable(factory):
        _error(f"dag_entry {config.dag_entry!r} is not callable; it must return a Dag")
        return None
    # Bind against the real signature rather than calling and catching TypeError: a
    # factory that raises TypeError from its own body must not be reported as a CLI
    # usage error, and a missing parameter must be named before anything is built.
    try:
        accepted = graph_entry_parameters(factory)
    except ValueError as error:
        _error(f"dag_entry {config.dag_entry!r} {error}")
        return None
    try:
        call_arguments = accepted.bind(graph_args or {})
    except ValueError as error:
        _error(f"dag_entry {config.dag_entry!r} {error}")
        return None
    dag = factory(**call_arguments)
    if not isinstance(dag, Dag):
        _error(f"dag_entry {config.dag_entry!r} returned {type(dag).__name__}, expected a Dag")
        return None
    return dag


def _docs(name: str | None) -> int:
    """List the shipped documentation pages, or print one of them."""
    if name is not None:
        return _print_doc(name)
    width = max(len(doc.name) for doc in SHIPPED_DOCS)
    print("shipped documentation (print one with: kigumi docs <name>)\n")
    for doc in SHIPPED_DOCS:
        print(f"  {doc.name.ljust(width)}  {doc.summary}")
    return 0


def _print_doc(name: str) -> int:
    """Print one shipped page, reporting a broken installation as an error."""
    try:
        print(read_doc(name), end="")
    except (FileNotFoundError, KeyError) as error:
        _error(str(error).strip("'"))
        return 1
    return 0


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
        f'dag_entry = "{DAG_ENTRY_MODULE}:build_dag"\n'
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
    entry_path = _write_dag_entry(root)
    _write_agent_docs(root)
    if hooks:
        hook_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(hook_path, "#!/bin/sh\nuv run kigumi guard --changed\n")
        hook_path.chmod(0o755)
    print("initialized kigumi project")
    if entry_path is not None:
        relative = entry_path.relative_to(root)
        print(f"  wrote {relative} (fill in build_dag, then: kigumi describe)")
        print(f'  optional standalone command: [project.scripts] dag = "{DAG_ENTRY_MODULE}:main"')
    return 0


def _write_dag_entry(root: Path) -> Path | None:
    """Write the graph entry-point skeleton, never overwriting existing project code."""
    package = root / DAG_ENTRY_MODULE.split(".")[0]
    entry_path = root / (DAG_ENTRY_MODULE.replace(".", "/") + ".py")
    if entry_path.exists():
        return None
    package.mkdir(parents=True, exist_ok=True)
    (package / "__init__.py").touch(exist_ok=True)
    atomic_write_text(entry_path, DAG_ENTRY_TEMPLATE)
    return entry_path


_AGENT_DOCS_SENTINEL = "<!-- kigumi-agent-docs -->"
_BRIEF_ROOT_HEADING = "# kigumi brief (read this first)"
_ATX_HEADING_PATTERN = re.compile(r"^( {0,3})(#{1,6})(?=[ \t]|$)")


def _demote_brief_headings(brief: str) -> str:
    """Nest brief headings below the host document without changing fenced code."""
    lines: list[str] = []
    in_fence = False
    for line in brief.splitlines(keepends=True):
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            lines.append(line)
            continue
        if in_fence:
            lines.append(line)
            continue

        if line.startswith(_BRIEF_ROOT_HEADING):
            line = "## kigumi" + line[len(_BRIEF_ROOT_HEADING) :]
        else:
            match = _ATX_HEADING_PATTERN.match(line)
            if match:
                line = f"{match.group(1)}#{match.group(2)}{line[match.end() :]}"
        lines.append(line)
    return "".join(lines)


def _write_agent_docs(root: Path) -> None:
    """Append kigumi framework guidance to CLAUDE.md and AGENTS.md (idempotent).

    Called unconditionally by ``kigumi init``.  The HTML sentinel prevents
    double-injection if the pyproject ``[tool.kigumi]`` block is ever manually
    removed and ``init`` is re-run.
    """
    body = _demote_brief_headings(read_doc("brief")).strip()
    block = f"\n{_AGENT_DOCS_SENTINEL}\n{body}\n"
    for filename in ("CLAUDE.md", "AGENTS.md"):
        path = root / filename
        if path.is_file():
            existing = path.read_text(encoding="utf-8")
            if _AGENT_DOCS_SENTINEL in existing:
                continue
            atomic_write_text(path, existing.rstrip() + "\n" + block)
        else:
            atomic_write_text(path, block.lstrip("\n"))


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
    # Reported as configured text, not resolved: doctor stays disk-only, and importing
    # the project to introspect the factory is the graph commands' cost, not this one's.
    print(f"dag entry: {config.dag_entry or 'not set (graph commands unavailable)'}")
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
            try:
                durable = durable_run_state(run_path)
            except WorkflowProfileError as error:
                _error(str(error))
                return 1
            runs.append(
                {
                    "run_id": run_path.name,
                    "nodes": len(sidecars),
                    "hits": hits,
                    "misses": misses,
                    "pending": len(pending),
                    "status": durable.get("run_status", "unknown"),
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
    manifest_path = run_path / "_run.json"
    manifest = _read_json(manifest_path)
    if manifest_path.is_file():
        try:
            if manifest.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA:
                raise WorkflowProfileError(f"Run {run_id!r} has an unsupported manifest schema")
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
    try:
        durable = durable_run_state(run_path)
    except WorkflowProfileError as error:
        _error(str(error))
        return 1
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
                "status": durable.get("run_status", "unknown"),
                "attempts": durable.get("attempts", []),
                "retry_policy_digests": durable.get("retry_policy_digests", {}),
                "evidence_policy_digests": durable.get("evidence_policy_digests", {}),
                "pending_retries": durable.get("pending_retries", []),
                "ambiguous_attempts": durable.get("ambiguous_attempts", []),
                "workflow_profile": workflow,
            }
        )
    else:
        print(f"status: {durable.get('run_status', 'unknown')}")
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
        if durable.get("run_status") == "failed":
            failed_attempts = [
                attempt
                for attempt in durable.get("attempts", [])
                if attempt.get("status") == "failed"
            ]
            latest = (
                max(
                    failed_attempts,
                    key=lambda attempt: (
                        attempt.get("attempt") if isinstance(attempt.get("attempt"), int) else -1
                    ),
                )
                if failed_attempts
                else {}
            )
            print(
                _recovery_advice(
                    run_id,
                    str(latest.get("target", "<target>")),
                    latest.get("attempt", "<N>"),
                )
            )
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


def _recovery_advice(run_id: str, target: str, attempt: int | str) -> str:
    """Explain the non-destructive terminal-failure recovery path."""
    recovery_argv = [
        "kigumi",
        "recover",
        "--attempt",
        str(attempt),
        "--decision",
        "retry_after_external_check",
        "--reason",
        "<explanation>",
        "--",
        run_id,
        target,
    ]
    resume_argv = ["kigumi", "resume", "--", run_id]
    return (
        "To retry with explicit decision:\n"
        f"  {shlex.join(recovery_argv)}\n"
        "Before the `--` separator on both commands, add the same actual repeated "
        "--graph-arg KEY=VALUE options used to construct this run; run state cannot "
        "reconstruct them, and placeholder values are invalid.\n"
        f"Then explicitly run: {shlex.join(resume_argv)}\n"
        "Recovery does not resume automatically; see docs/cli.md and docs/recovery.md"
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
    except (FileNotFoundError, ValueError, WorkflowProfileError) as error:
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
