"""Command-line operations for configured kigumi projects."""

from __future__ import annotations

import argparse
import importlib
import inspect
import json
import os
import re
import shlex
import stat
import subprocess
import sys
import tomllib
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._runstate import RUN_MANIFEST_SCHEMA, AttemptStore, RunManifestError
from ._safe_io import SecureDirectory, _open_regular_file_at
from ._safe_io import secure_atomic_write_text as atomic_write_text
from .artifacts import canonical_json
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
from .errors import CacheIntegrityError
from .inspect import (
    _load_run_profile_owned,
    diff_run_views,
    durable_run_state,
    load_call,
    trace_run,
)
from .profile import WorkflowProfileError
from .prompt import TemplateSlotError, load_template, render_template, slot_names
from .store import approve_checkpoint, gc_artifacts, run_directory, run_sort_key

DAG_ENTRY_MODULE = "nodes.graph"
"""Module `kigumi init` scaffolds, matching the default ``source_dirs`` entry."""

_HOOK_MODE = (
    stat.S_IRUSR
    | stat.S_IWUSR
    | stat.S_IXUSR
    | stat.S_IRGRP
    | stat.S_IXGRP
    | stat.S_IROTH
    | stat.S_IXOTH
)

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


class _InitValidationError(ValueError):
    """A preflight error that must not leave a partial scaffold behind."""


@dataclass(frozen=True)
class _InitPlan:
    """All init writes computed before the first filesystem mutation."""

    root: Path
    directories: tuple[Path, ...]
    empty_files: tuple[Path, ...]
    writes: tuple[tuple[Path, str], ...]
    entry_path: Path | None
    hook_path: Path | None


def _init_lstat(path: Path) -> os.stat_result | None:
    """Inspect a path without following its final component."""
    try:
        return path.lstat()
    except FileNotFoundError:
        return None
    except OSError as error:
        raise _InitValidationError(f"cannot inspect {path}: {error}") from error


def _init_identity(info: os.stat_result) -> tuple[int, int]:
    return info.st_dev, info.st_ino


def _validate_init_destination(
    root: Path,
    path: Path,
    *,
    kind: str,
    label: str,
    required: bool = False,
) -> None:
    """Validate every existing component of an init destination.

    ``Path.mkdir`` and ordinary text reads follow directory symlinks.  Init is a
    project-layout operation, so an existing symlink anywhere below the project
    root is ambiguous and is rejected before any write is attempted.
    """
    root = root.absolute()
    path = path.absolute()
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise _InitValidationError(f"{label} must stay inside the project root") from error
    if any(part == ".." for part in relative.parts):
        raise _InitValidationError(f"{label} must not contain '..'")

    current = root
    parts = relative.parts
    for index, part in enumerate(parts):
        current = current / part
        info = _init_lstat(current)
        if info is None:
            if required and index == len(parts) - 1:
                raise _InitValidationError(f"{label} does not exist: {current}")
            return
        if stat.S_ISLNK(info.st_mode):
            raise _InitValidationError(f"{label} must not contain a symlink: {current}")
        if index != len(parts) - 1:
            if not stat.S_ISDIR(info.st_mode):
                raise _InitValidationError(f"{label} parent is not a directory: {current}")
            continue
        if kind == "directory" and not stat.S_ISDIR(info.st_mode):
            raise _InitValidationError(f"{label} must be a directory: {current}")
        if kind == "file" and not stat.S_ISREG(info.st_mode):
            raise _InitValidationError(f"{label} must be a regular file: {current}")


def _read_init_text(path: Path, label: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise _InitValidationError(f"cannot read {label} {path}: {error}") from error


class _InitTransaction:
    """Journal init mutations so a late write failure restores the project."""

    def __init__(self, root: Path) -> None:
        self.root = root.absolute()
        self.created_directories: list[tuple[Path, tuple[int, int]]] = []
        self.created_files: list[tuple[Path, tuple[int, int]]] = []
        self.original_files: dict[Path, tuple[str, int]] = {}

    def ensure_directory(self, path: Path) -> None:
        _validate_init_destination(
            self.root,
            path,
            kind="directory",
            label="init directory",
        )
        relative = path.absolute().relative_to(self.root)
        current = self.root
        for part in relative.parts:
            current = current / part
            info = _init_lstat(current)
            if info is not None:
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                    raise _InitValidationError(
                        f"init directory must be a non-symlink directory: {current}"
                    )
                continue
            try:
                current.mkdir()
            except FileExistsError as error:
                info = _init_lstat(current)
                if info is None or stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                    raise _InitValidationError(
                        f"init directory appeared as a non-directory: {current}"
                    ) from error
                continue
            info = _init_lstat(current)
            if info is None or stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise _InitValidationError(f"init directory was not created safely: {current}")
            self.created_directories.append((current, _init_identity(info)))

    def write_text(self, path: Path, text: str) -> None:
        _validate_init_destination(
            self.root,
            path,
            kind="file",
            label="init file",
        )
        info = _init_lstat(path)
        is_new = info is None
        if not is_new:
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                raise _InitValidationError(f"init file must be a regular file: {path}")
            if path not in self.original_files:
                self.original_files[path] = (
                    _read_init_text(path, "init file"),
                    stat.S_IMODE(info.st_mode),
                )
        atomic_write_text(path, text)
        if is_new:
            created = _init_lstat(path)
            if (
                created is None
                or stat.S_ISLNK(created.st_mode)
                or not stat.S_ISREG(created.st_mode)
            ):
                raise _InitValidationError(f"init file was not created safely: {path}")
            self.created_files.append((path, _init_identity(created)))

    def chmod(self, path: Path, mode: int) -> None:
        info = _init_lstat(path)
        if info is None or stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise _InitValidationError(f"cannot set mode on non-regular init file: {path}")
        os.chmod(path, mode, follow_symlinks=False)

    def rollback(self) -> list[str]:
        """Undo this transaction, refusing to remove paths whose identity changed."""
        errors: list[str] = []
        for path, identity in reversed(self.created_files):
            try:
                info = _init_lstat(path)
                if info is None:
                    continue
                if stat.S_ISREG(info.st_mode) and _init_identity(info) == identity:
                    path.unlink()
                else:
                    errors.append(f"did not remove changed file {path}")
            except OSError as error:
                errors.append(f"could not remove {path}: {error}")

        for path, (text, mode) in self.original_files.items():
            try:
                info = _init_lstat(path)
                if info is not None and (
                    stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode)
                ):
                    errors.append(f"did not restore changed file {path}")
                    continue
                # Use the descriptor-relative writer directly during rollback so a
                # fault-injected forward write cannot also disable restoration.
                _restore_atomic_write_text(path, text)
                os.chmod(path, mode, follow_symlinks=False)
            except (OSError, ValueError) as error:
                errors.append(f"could not restore {path}: {error}")

        for path, identity in reversed(self.created_directories):
            try:
                info = _init_lstat(path)
                if info is None:
                    continue
                if stat.S_ISDIR(info.st_mode) and _init_identity(info) == identity:
                    path.rmdir()
                else:
                    errors.append(f"did not remove changed directory {path}")
            except OSError as error:
                errors.append(f"could not remove {path}: {error}")
        return errors


def _plan_init(root: Path, *, hooks: bool) -> _InitPlan:
    """Validate and materialize the complete init plan without mutating disk."""
    root = Path(root).absolute()
    root_info = _init_lstat(root)
    if root_info is None or stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
        raise _InitValidationError(f"project root must be a non-symlink directory: {root}")

    pyproject = root / "pyproject.toml"
    _validate_init_destination(
        root,
        pyproject,
        kind="file",
        label="pyproject.toml",
        required=True,
    )
    existing = _read_init_text(pyproject, "pyproject.toml")
    try:
        document = tomllib.loads(existing)
    except tomllib.TOMLDecodeError as error:
        raise _InitValidationError(f"invalid pyproject.toml: {error}") from error
    tool = document.get("tool")
    if tool is not None and not isinstance(tool, dict):
        raise _InitValidationError("pyproject.toml [tool] must be a table")
    if isinstance(tool, dict) and "kigumi" in tool:
        raise _InitValidationError("[tool.kigumi] already exists")

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
    updated_pyproject = existing.rstrip() + block
    try:
        prospective = tomllib.loads(updated_pyproject)
    except tomllib.TOMLDecodeError as error:
        raise _InitValidationError(f"generated pyproject.toml is invalid: {error}") from error
    prospective_tool = prospective.get("tool")
    if not isinstance(prospective_tool, dict) or not isinstance(
        prospective_tool.get("kigumi"), dict
    ):
        raise _InitValidationError("generated [tool.kigumi] configuration is invalid")

    directories = tuple(
        root / relative for relative in ("prompts", "artifacts", "artifacts/_llm", "nodes", "lib")
    )
    for directory in directories:
        _validate_init_destination(
            root,
            directory,
            kind="directory",
            label="init directory",
        )

    empty_files: list[Path] = []
    for directory in (*directories[:2], directories[3], directories[4]):
        gitkeep = directory / ".gitkeep"
        _validate_init_destination(root, gitkeep, kind="file", label=".gitkeep")
        if _init_lstat(gitkeep) is None:
            empty_files.append(gitkeep)
    llm_gitkeep = directories[2] / ".gitkeep"
    _validate_init_destination(root, llm_gitkeep, kind="file", label=".gitkeep")
    if _init_lstat(llm_gitkeep) is None:
        empty_files.append(llm_gitkeep)

    entry_path = root / (DAG_ENTRY_MODULE.replace(".", "/") + ".py")
    _validate_init_destination(root, entry_path, kind="file", label="DAG entry")
    entry_exists = _init_lstat(entry_path) is not None
    if not entry_exists:
        package_init = entry_path.parent / "__init__.py"
        _validate_init_destination(root, package_init, kind="file", label="nodes package")
        if _init_lstat(package_init) is None:
            empty_files.append(package_init)
    planned_entry = entry_path if not entry_exists else None

    writes: list[tuple[Path, str]] = [(pyproject, updated_pyproject)]

    gitignore = root / ".gitignore"
    _validate_init_destination(root, gitignore, kind="file", label=".gitignore")
    artifact_ignore = "artifacts/"
    gitignore_info = _init_lstat(gitignore)
    if gitignore_info is None:
        writes.append((gitignore, artifact_ignore + "\n"))
    else:
        gitignore_text = _read_init_text(gitignore, ".gitignore")
        lines = gitignore_text.splitlines()
        if artifact_ignore not in lines:
            writes.append((gitignore, "\n".join([*lines, artifact_ignore]) + "\n"))

    if planned_entry is not None:
        writes.append((planned_entry, DAG_ENTRY_TEMPLATE))

    try:
        brief = _demote_brief_headings(read_doc("brief")).strip()
    except (FileNotFoundError, KeyError, OSError) as error:
        raise _InitValidationError(f"cannot load shipped brief for init: {error}") from error
    agent_block = f"\n{_AGENT_DOCS_SENTINEL}\n{brief}\n"
    for filename in ("CLAUDE.md", "AGENTS.md"):
        path = root / filename
        _validate_init_destination(root, path, kind="file", label=filename)
        info = _init_lstat(path)
        if info is None:
            writes.append((path, agent_block.lstrip("\n")))
            continue
        text = _read_init_text(path, filename)
        if _AGENT_DOCS_SENTINEL not in text:
            writes.append((path, text.rstrip() + "\n" + agent_block))

    hook_path: Path | None = None
    if hooks:
        git_root = root / ".git"
        _validate_init_destination(root, git_root, kind="directory", label=".git", required=True)
        hooks_dir = git_root / "hooks"
        _validate_init_destination(root, hooks_dir, kind="directory", label="git hooks")
        hook_path = hooks_dir / "pre-commit"
        _validate_init_destination(root, hook_path, kind="file", label="pre-commit hook")
        if _init_lstat(hook_path) is not None:
            raise _InitValidationError("refusing to overwrite existing pre-commit hook")
        writes.append((hook_path, "#!/bin/sh\nuv run kigumi guard --changed\n"))

    return _InitPlan(
        root=root,
        directories=directories,
        empty_files=tuple(empty_files),
        writes=tuple(writes),
        entry_path=planned_entry,
        hook_path=hook_path,
    )


_restore_atomic_write_text = atomic_write_text


def _init(root: Path, *, hooks: bool) -> int:
    try:
        plan = _plan_init(root, hooks=hooks)
    except _InitValidationError as error:
        _error(str(error))
        return 1

    transaction = _InitTransaction(plan.root)
    try:
        for directory in plan.directories:
            transaction.ensure_directory(directory)
        for empty_file in plan.empty_files:
            transaction.write_text(empty_file, "")
        for path, text in plan.writes:
            transaction.write_text(path, text)
        if plan.hook_path is not None:
            transaction.chmod(plan.hook_path, _HOOK_MODE)
    except Exception as error:
        rollback_errors = transaction.rollback()
        detail = f"; rollback incomplete: {', '.join(rollback_errors)}" if rollback_errors else ""
        _error(f"init failed: {error}{detail}")
        return 1

    print("initialized kigumi project")
    if plan.entry_path is not None:
        relative = plan.entry_path.relative_to(plan.root)
        print(f"  wrote {relative} (fill in build_dag, then: kigumi describe)")
        print(f'  optional standalone command: [project.scripts] dag = "{DAG_ENTRY_MODULE}:main"')
    return 0


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
        try:
            run_paths = _run_directories(config.artifacts_path / "runs")
        except WorkflowProfileError as error:
            _error(str(error))
            return 1
        runs: list[dict[str, Any]] = []
        for run_path in run_paths:
            try:
                with _owned_run(run_path) as store:
                    sidecars = _sidecar_paths(run_path, store=store)
                    metadata = [_read_owned_json(store, path) for path in sidecars]
                    hits = sum(1 for item in metadata if item.get("cache") == "hit")
                    misses = sum(1 for item in metadata if item.get("cache") == "miss")
                    pending = _pending_names(run_path, store=store)
                    durable = durable_run_state(run_path, _store=store)
            except (FileNotFoundError, WorkflowProfileError) as error:
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
    workflow: dict[str, Any] | None = None
    manifest_path = run_path / "_run.json"
    try:
        with _owned_run(run_path) as store:
            manifest_info = _owned_entry_stat(store, manifest_path, label="Run manifest")
            manifest = _read_owned_json(store, manifest_path)
            sidecar_paths = _sidecar_paths(run_path, store=store)
            sidecar_metadata = [_read_owned_json(store, path) for path in sidecar_paths]
            pending = _pending_names(run_path, store=store)
            approved_paths = _approval_paths(run_path, store=store)
            if manifest_info is not None:
                if manifest.get("run_manifest_schema") != RUN_MANIFEST_SCHEMA:
                    raise WorkflowProfileError(f"Run {run_id!r} has an unsupported manifest schema")
                workflow = _load_run_profile_owned(run_path, store)
            durable = durable_run_state(run_path, _store=store)
    except FileNotFoundError:
        _error(f"run not found: {run_id}")
        return 1
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
                **metadata,
            }
            for sidecar, metadata in zip(sidecar_paths, sidecar_metadata, strict=True)
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
    approved = [
        approval.name.removesuffix(".json")
        for approval in approved_paths
        if not approval.name.endswith(".pending.json")
    ]
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
    try:
        with SecureDirectory(runs_root, create=False) as directory:
            paths: list[Path] = []
            for name in directory.names():
                try:
                    info = directory.stat(name)
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(info.st_mode):
                    raise WorkflowProfileError(
                        f"Run directory must not be a symlink: {runs_root / name}"
                    )
                if stat.S_ISDIR(info.st_mode):
                    paths.append(runs_root / name)
            return sorted(paths, key=run_sort_key)
    except FileNotFoundError:
        return []
    except WorkflowProfileError:
        raise
    except (OSError, ValueError) as error:
        raise WorkflowProfileError(
            f"Unable to inspect runs directory {runs_root}: {error}"
        ) from error


@contextmanager
def _owned_run(run_path: Path):
    """Keep one run's descriptor-bound ownership boundary for CLI reads."""
    try:
        store = AttemptStore(run_path, {})
    except (OSError, RunManifestError, ValueError) as error:
        raise WorkflowProfileError(
            f"Unable to inspect run {run_path.name!r} safely: {error}"
        ) from error
    if store._run_directory is None:  # noqa: SLF001
        if store._runs_directory is not None:  # noqa: SLF001
            store._runs_directory.close()  # noqa: SLF001
        raise FileNotFoundError(run_path)
    try:
        yield store
    finally:
        for directory in (
            store._run_directory,  # noqa: SLF001
            store._runs_directory,  # noqa: SLF001
        ):
            if directory is not None:
                directory.close()


def _owned_entry_stat(
    store: AttemptStore,
    path: Path,
    *,
    label: str,
) -> os.stat_result | None:
    """Stat one run entry through the bound AttemptStore directory."""
    try:
        return store._owned_stat(path)  # noqa: SLF001
    except FileNotFoundError:
        return None
    except (OSError, RunManifestError, ValueError) as error:
        raise WorkflowProfileError(f"Unable to inspect {label} path {path}: {error}") from error


def _owned_json_paths(
    store: AttemptStore,
    directory_path: Path,
    *,
    suffix: str,
    label: str,
) -> list[Path]:
    """List regular JSON entries from a descriptor-bound run directory."""
    try:
        names = store._owned_names(directory_path)  # noqa: SLF001
    except FileNotFoundError:
        return []
    except (OSError, RunManifestError, ValueError) as error:
        raise WorkflowProfileError(
            f"Unable to inspect {label} directory {directory_path}: {error}"
        ) from error
    paths: list[Path] = []
    for name in sorted(name for name in names if name.endswith(suffix)):
        path = directory_path / name
        info = _owned_entry_stat(store, path, label=label)
        if info is None:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise WorkflowProfileError(f"{label} must not be a symlink: {path}")
        if not stat.S_ISREG(info.st_mode):
            raise WorkflowProfileError(f"{label} must reference a regular file: {path}")
        paths.append(path)
    return paths


def _sidecar_paths(run_path: Path, *, store: AttemptStore) -> list[Path]:
    return _owned_json_paths(
        store,
        run_path,
        suffix=".json.meta.json",
        label="Run sidecar",
    )


def _approval_paths(run_path: Path, *, store: AttemptStore) -> list[Path]:
    return _owned_json_paths(
        store,
        run_path / "approvals",
        suffix=".json",
        label="Approval file",
    )


def _read_owned_json(store: AttemptStore, path: Path) -> dict[str, Any]:
    """Read one run JSON object through the bound durable reader."""
    data, corrupted = store._read_owned_json(path)  # noqa: SLF001
    if corrupted or not isinstance(data, dict):
        return {}
    return data


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with (
            SecureDirectory(path.parent, create=False) as directory,
            _open_regular_file_at(
                directory,
                path.name,
                phase="reading CLI JSON",
            ) as handle,
        ):
            data = json.loads(handle.read().decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _pending_names(run_path: Path, *, store: AttemptStore) -> list[str]:
    return sorted(
        path.name.removesuffix(".pending.json")
        for path in _owned_json_paths(
            store,
            run_path / "approvals",
            suffix=".pending.json",
            label="Pending approval",
        )
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
    try:
        result, components = diff_run_views(config.artifacts_path, run_a, run_b)
    except (FileNotFoundError, ValueError, WorkflowProfileError) as error:
        _error(str(error))
        return 1
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
    except (CacheIntegrityError, FileNotFoundError, ValueError, WorkflowProfileError) as error:
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
    except (CacheIntegrityError, FileNotFoundError, ValueError) as error:
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
