"""锁定 `dag_entry` 工厂可以接收运行时参数,而不必伪造一个占位图。

`dag_entry` 此前只能是零参 callable。图形状或 `params` 依赖运行时输入的项目
(按 episode 展开 `foreach`、按输入文件声明 `files`)因此只有两条路:不声明
`dag_entry`,让 8 条图命令全部没有入口;或者用占位参数构图,让 `plan` /
`explain` / `check` 报出与任何真实运行都不对应的结论——`params` 是 L3 键成分,
占位值下 `plan` 恒定全 miss、`explain` 每个节点都报 `params` 变化,而 `resume`
会带着错误的 `graph_identity` 真的执行。两条路都让这组命令失去价值。

这里钉住修好后的形状:`--graph-arg key=value` 把参数传给工厂;工厂需要而没给、
给了工厂不认识的、形状不对的,各自报出可执行的修复动作而不是裸 traceback 或
静默忽略。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest

from kigumi.cli import _parser, main
from kigumi.dag import GRAPH_COMMAND_HELP, _build_cli_parser

PYPROJECT = """[project]
name = 'sample'

[tool.kigumi]
source_dirs = []
dag_entry = 'nodes.graph:{factory}'
"""

GRAPH_MODULE = '''"""Fixture graph whose topology depends on runtime arguments."""

from __future__ import annotations

from pathlib import Path

from kigumi import Dag, KigumiConfig


def _dag(episode: str) -> Dag:
    root = Path(__file__).resolve().parent.parent
    dag = Dag(KigumiConfig(project_root=root, source_dirs=[]), object())

    @dag.node(f"scene_{episode}", params={"episode": episode})
    def scene(inputs, ctx):
        """Node whose name and params carry the runtime episode."""
        return {"episode": ctx.params["episode"]}

    return dag


def needs_episode(episode):
    """Factory with one required parameter."""
    return _dag(episode)


def defaulted_episode(episode="fallback"):
    """Factory whose parameter is optional."""
    return _dag(episode)


def zero_arg():
    """Factory that takes nothing, as before."""
    return _dag("static")


def variadic(**overrides):
    """Factory accepting any keyword."""
    return _dag(overrides.get("episode", "variadic"))


def positional_only(episode, /):
    """Factory whose parameter cannot be passed by name."""
    return _dag(episode)
'''


@pytest.fixture
def project(tmp_path: Path, monkeypatch) -> Path:
    """Write a project whose graph factory needs a runtime episode."""
    package = tmp_path / "nodes"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "graph.py").write_text(GRAPH_MODULE, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    # `_load_dag` imports the project module, and `sys.modules` outlives the test. A
    # later test writing its own nodes/graph.py would silently get this one instead.
    for name in ("nodes", "nodes.graph"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    yield tmp_path
    for name in ("nodes", "nodes.graph"):
        sys.modules.pop(name, None)


def _configure(project: Path, factory: str) -> None:
    (project / "pyproject.toml").write_text(PYPROJECT.format(factory=factory), encoding="utf-8")


def test_graph_arg_reaches_the_factory(project: Path, capsys) -> None:
    """教训 placeholder_graph: 参数必须真的传进工厂,否则只能靠伪造参数构图。"""
    _configure(project, "needs_episode")

    assert main(["describe", "--graph-arg", "episode=E2S4"]) == 0
    assert "scene_E2S4" in capsys.readouterr().out


def test_missing_graph_arg_names_the_parameter(project: Path, capsys) -> None:
    """教训 opaque_construction: 缺参数要指名补什么,不能抛 TypeError traceback。"""
    _configure(project, "needs_episode")

    assert main(["describe"]) == 2
    message = capsys.readouterr().err
    assert "episode" in message
    assert "--graph-arg" in message
    assert "Traceback" not in message


def test_unknown_graph_arg_is_rejected(project: Path, capsys) -> None:
    """教训 silent_drop: 工厂不认识的参数必须报错,静默丢弃会构出另一个图。"""
    _configure(project, "needs_episode")

    assert main(["describe", "--graph-arg", "episode=E2S4", "--graph-arg", "nope=1"]) == 2
    message = capsys.readouterr().err
    assert "nope" in message
    assert "episode" in message, "the message should list what the factory does accept"


def test_malformed_graph_arg_is_rejected(project: Path, capsys) -> None:
    """教训 shape_error: 没有 '=' 的参数是用法错误,要说清期望形状。"""
    _configure(project, "needs_episode")

    assert main(["describe", "--graph-arg", "episode"]) == 2
    assert "key=value" in capsys.readouterr().err


def test_repeated_graph_arg_is_rejected(project: Path, capsys) -> None:
    """教训 ambiguous_override: 同名参数给两次,静默取一个会构出说不清的图。"""
    _configure(project, "needs_episode")

    assert main(["describe", "--graph-arg", "episode=A", "--graph-arg", "episode=B"]) == 2
    assert "episode" in capsys.readouterr().err


def test_graph_arg_value_keeps_later_equals_signs(project: Path, capsys) -> None:
    """教训 overzealous_split: 只按首个 '=' 切分,值里的 '=' 属于值。"""
    _configure(project, "needs_episode")

    assert main(["describe", "--graph-arg", "episode=a=b"]) == 0
    assert "scene_a=b" in capsys.readouterr().out


def test_zero_arg_factory_needs_no_graph_arg(project: Path, capsys) -> None:
    """教训 regression: 既有零参工厂必须原样继续工作。"""
    _configure(project, "zero_arg")

    assert main(["describe"]) == 0
    assert "scene_static" in capsys.readouterr().out


def test_defaulted_parameter_is_optional(project: Path, capsys) -> None:
    """有默认值的参数不是必需项;给了就覆盖默认值。"""
    _configure(project, "defaulted_episode")

    assert main(["describe"]) == 0
    assert "scene_fallback" in capsys.readouterr().out

    assert main(["describe", "--graph-arg", "episode=E1S1"]) == 0
    assert "scene_E1S1" in capsys.readouterr().out


def test_variadic_factory_accepts_any_graph_arg(project: Path, capsys) -> None:
    """`**kwargs` 工厂自行裁决参数名,CLI 不替它拒绝。"""
    _configure(project, "variadic")

    assert main(["describe", "--graph-arg", "episode=E9S9"]) == 0
    assert "scene_E9S9" in capsys.readouterr().out


def test_positional_only_parameter_is_reported(project: Path, capsys) -> None:
    """教训 unsupported_shape: 无法按名传的参数要明说,不要报成"缺参数"。"""
    _configure(project, "positional_only")

    assert main(["describe", "--graph-arg", "episode=E2S4"]) == 2
    message = capsys.readouterr().err
    assert "positional-only" in message
    assert "episode" in message


def _subcommand_options(parser: argparse.ArgumentParser, name: str) -> set[str]:
    """Return one subcommand's option strings."""
    actions = [
        action
        for action in parser._actions  # noqa: SLF001
        if isinstance(action, argparse._SubParsersAction)  # noqa: SLF001
    ]
    assert len(actions) == 1, "expected exactly one subparser action"
    subparser = actions[0].choices[name]
    return {
        string
        for action in subparser._actions  # noqa: SLF001
        for string in action.option_strings
    }


def test_every_graph_command_accepts_graph_args() -> None:
    """教训 partial_surface: 只有一部分命令能传参,等于另一部分仍然不可用。"""
    parser = _parser()
    for name in GRAPH_COMMAND_HELP:
        assert "--graph-arg" in _subcommand_options(parser, name), (
            f"kigumi {name} cannot parameterize its graph"
        )


def test_standalone_dag_cli_does_not_offer_graph_args() -> None:
    """教训 inert_flag: `dag` 收到的是已构好的图,一个不起作用的旗标比没有更糟。"""
    parser = _build_cli_parser()
    for name in GRAPH_COMMAND_HELP:
        assert "--graph-arg" not in _subcommand_options(parser, name), (
            f"dag {name} offers --graph-arg, but it never constructs the graph"
        )


def test_doctor_reports_the_configured_entry_without_importing(
    project: Path, monkeypatch, capsys
) -> None:
    """`doctor` 报出配置的入口,但绝不 import 项目代码——那是图命令的代价,不是它的。"""
    _configure(project, "needs_episode")
    del monkeypatch

    assert main(["doctor"]) == 0
    output = capsys.readouterr().out
    assert "nodes.graph:needs_episode" in output
    assert "nodes.graph" not in sys.modules, (
        "doctor imported project code; project commands must stay disk-only"
    )


def test_missing_graph_arg_is_the_discovery_path(project: Path, capsys) -> None:
    """教训 undiscoverable: 参数名从图命令的报错里得到,不必靠试错或读源码。"""
    _configure(project, "needs_episode")

    assert main(["check"]) == 2
    message = capsys.readouterr().err
    assert "episode" in message
    assert "--graph-arg" in message
