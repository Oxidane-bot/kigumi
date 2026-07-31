"""锁定图命令的两条入口:`kigumi <command>` 与 `dag <command>` 走同一套 handler。

这些命令此前只挂在 `Dag.cli` 上,而仓库里没有任何地方绑定它,下游项目装完之后
`plan` / `describe` 没有入口可敲。这里的测试钉住修好后的形状:两个入口共享同一份
子命令定义与 dispatch 表,`kigumi init` 生成的骨架真的可运行,且缺 `dag_entry` 时
报出可执行的修复动作而不是 "unknown command"。
"""

from __future__ import annotations

import argparse
import inspect
import re
from pathlib import Path

import pytest

import kigumi
from kigumi.cli import DAG_ENTRY_MODULE, _parser, main
from kigumi.config import KigumiConfig
from kigumi.dag import GRAPH_COMMAND_HELP, Dag, _build_cli_parser


def _graph_commands(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    """Return the subparsers of a built parser, keyed by command name."""
    actions = [
        action
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)  # noqa: SLF001
    ]
    assert len(actions) == 1, "expected exactly one subparser action"
    return dict(actions[0].choices)


def _option_strings(parser: argparse.ArgumentParser) -> set[str]:
    return {string for action in parser._actions for string in action.option_strings}  # noqa: SLF001


KIGUMI_ONLY_OPTIONS = {"--graph-arg"}
"""Flags that belong to `kigumi <command>` alone, with the reason they cannot drift.

`kigumi` constructs the graph by importing `dag_entry`, so only it can forward
runtime arguments to the factory. `Dag.cli` is called on an already-built graph,
where `--graph-arg` could not do anything. Every other flag must stay identical on
both entry points; see `test_both_entry_points_expose_identical_graph_commands`.
"""


EXPECTED_GRAPH_ARGUMENTS: dict[str, tuple[list[str], set[str]]] = {
    "check": ([], set()),
    "plan": ([], {"--targets"}),
    "graph": ([], {"--html", "--run-id", "--prompts"}),
    "profile": ([], {"--run-id", "--format", "--include-content"}),
    "explain": (["node_name"], {"--run-id"}),
    "describe": ([], {"--format"}),
    "resume": (["run_id"], {"--workers"}),
    "retry-resolve": (["run_id", "target"], {"--attempt", "--action", "--reason"}),
}
"""The documented surface of each graph command, spelled out independently.

Comparing the two parsers to each other proves nothing: both are built by
``register_graph_commands``, so they cannot disagree. This table is the second
opinion — a flag silently dropped from the shared builder fails here.
"""


def test_graph_commands_keep_their_documented_arguments() -> None:
    """教训 silent_flag_loss: 共享 builder 会让两侧同时丢参数,对比彼此发现不了。"""
    commands = _graph_commands(_build_cli_parser())
    assert set(commands) == set(EXPECTED_GRAPH_ARGUMENTS)
    for name, (positionals, options) in EXPECTED_GRAPH_ARGUMENTS.items():
        assert _positionals(commands[name]) == positionals, f"{name} positionals changed"
        assert _option_strings(commands[name]) - {"-h", "--help"} == options, (
            f"{name} options changed"
        )


def test_both_entry_points_expose_identical_graph_commands() -> None:
    """教训 entry_drift: 两条入口必须共用一份定义,不能各自 add_parser 一份。"""
    cli_source = inspect.getsource(_parser)
    for name in GRAPH_COMMAND_HELP:
        assert f'add_parser("{name}"' not in cli_source, (
            f"kigumi's parser hand-rolls {name}; it must use register_graph_commands"
        )

    dag_commands = _graph_commands(_build_cli_parser())
    kigumi_commands = _graph_commands(_parser())

    for name in GRAPH_COMMAND_HELP:
        assert name in dag_commands, f"dag CLI is missing {name}"
        assert name in kigumi_commands, f"kigumi CLI is missing {name}"
        dag_options = _option_strings(dag_commands[name])
        kigumi_options = _option_strings(kigumi_commands[name])
        assert kigumi_options - dag_options == KIGUMI_ONLY_OPTIONS, (
            f"{name}: kigumi's extra flags are not exactly the ones it alone can honour"
        )
        assert not dag_options - kigumi_options, f"{name} accepts flags on dag that kigumi does not"
        assert _positionals(dag_commands[name]) == _positionals(kigumi_commands[name]), (
            f"{name} takes different arguments depending on which CLI you use"
        )


def _positionals(parser: argparse.ArgumentParser) -> list[str]:
    return [
        action.dest
        for action in parser._actions  # noqa: SLF001
        if not action.option_strings
    ]


def test_registered_commands_and_dispatch_table_agree(tmp_path: Path) -> None:
    """教训 unroutable_command: 注册了子命令但没有 handler,等于给出一个会崩的入口。

    只断言"每个命令都能跑"会漏掉反向缺口:dispatch 表里多出的名字永远无法从 CLI
    到达。两侧都对齐,才能保证注册面与实现面是同一个集合。
    """
    dag = Dag(KigumiConfig(project_root=tmp_path, source_dirs=[]), object())  # type: ignore[arg-type]
    registered = set(_graph_commands(_build_cli_parser()))
    assert registered == set(GRAPH_COMMAND_HELP), (
        "GRAPH_COMMAND_HELP and the registered subcommands disagree"
    )

    dispatched = set()
    for name in registered:
        try:
            dag.run_command(argparse.Namespace(command=name))
        except KeyError as error:
            # An unrouted command fails looking up its own name; anything else means
            # the handler ran and failed later on the deliberately empty fixture.
            if error.args[:1] == (name,):
                continue
            dispatched.add(name)
        except Exception:
            dispatched.add(name)
        else:
            dispatched.add(name)
    assert dispatched == registered, f"registered but unroutable: {sorted(registered - dispatched)}"

    table = inspect.getsource(Dag.run_command)
    for name in re.findall(r'"([a-z-]+)": self\._cli_', table):
        assert name in registered, f"{name} is in the dispatch table but no CLI registers it"


def test_help_lists_every_graph_command() -> None:
    """教训 undiscoverable: agent 只有 --help,命令必须能在那里被发现。"""
    assert set(GRAPH_COMMAND_HELP) <= set(_graph_commands(_parser()))
    for name, help_text in GRAPH_COMMAND_HELP.items():
        assert help_text and help_text[0].islower(), f"{name} help should read as a phrase"


def test_init_scaffolds_a_runnable_graph_entry(tmp_path: Path, monkeypatch, capsys) -> None:
    """教训 dead_capability: init 必须留下能真正跑起来的图入口,而不是只写配置键。"""
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'sample'\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert main(["init"]) == 0
    entry = tmp_path / (DAG_ENTRY_MODULE.replace(".", "/") + ".py")
    assert entry.is_file(), "init did not scaffold the graph entry point"
    assert 'dag_entry = "nodes.graph:build_dag"' in (tmp_path / "pyproject.toml").read_text(
        encoding="utf-8"
    )
    output = capsys.readouterr().out
    assert str(entry.relative_to(tmp_path)) in output
    assert "[project.scripts]" in output, "init should say how to get a standalone dag command"

    source = entry.read_text(encoding="utf-8")
    compile(source, str(entry), "exec")
    assert "def build_dag()" in source
    assert "def main(" in source
    # The skeleton must not import names kigumi does not export.
    imported = source.split("from kigumi import ", 1)[1].split("\n")[0]
    for name in imported.replace("(", "").replace("\n", " ").split(","):
        symbol = name.strip()
        if symbol:
            assert hasattr(kigumi, symbol), f"skeleton imports kigumi.{symbol}, which is missing"


def test_graph_command_without_dag_entry_names_the_fix(tmp_path: Path, monkeypatch, capsys) -> None:
    """教训 dead_end_error: 缺配置时要说清补什么,不能只说命令不可用。"""
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname = 'sample'\n\n[tool.kigumi]\nsource_dirs = []\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    assert main(["plan"]) == 2
    message = capsys.readouterr().err
    assert "dag_entry" in message
    assert "module:callable" in message


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        ("nodes.missing:build_dag", "not importable"),
        ("nodes.graph:absent", "has no"),
        ("nodes.graph:NOT_CALLABLE", "not callable"),
        ("nodes.graph:wrong_type", "expected a Dag"),
    ],
)
def test_broken_dag_entry_reports_which_part_is_wrong(
    tmp_path: Path, monkeypatch, capsys, entry: str, expected: str
) -> None:
    """教训 opaque_entry: 入口配错时要指出错在哪一段,而不是抛裸 traceback。"""
    (tmp_path / "pyproject.toml").write_text(
        f"[project]\nname = 'sample'\n\n[tool.kigumi]\nsource_dirs = []\ndag_entry = '{entry}'\n",
        encoding="utf-8",
    )
    package = tmp_path / "nodes"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "graph.py").write_text(
        "NOT_CALLABLE = 1\n\n\ndef wrong_type():\n    return 'not a dag'\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))

    assert main(["describe"]) == 2
    assert expected in capsys.readouterr().err


def test_malformed_dag_entry_is_rejected_by_config(tmp_path: Path, monkeypatch, capsys) -> None:
    """教训 late_failure: 形状不对的 dag_entry 是配置错误,应在加载时就报。"""
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname = 'sample'\n\n[tool.kigumi]\ndag_entry = 'nodes.graph'\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    assert main(["describe"]) == 2
    assert "module:callable" in capsys.readouterr().err
