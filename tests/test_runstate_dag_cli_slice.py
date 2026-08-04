from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from kigumi import PromptRef, PromptSpec
from kigumi.config import KigumiConfig
from kigumi.dag import Dag


def _cli_dag(tmp_path: Path) -> Dag:
    return Dag(KigumiConfig(project_root=tmp_path, source_dirs=[]), object())  # type: ignore[arg-type]


def _run_dag_cli(dag: Dag, argv: list[str]) -> int:
    with pytest.raises(SystemExit) as exited:
        dag.cli(argv)
    return int(exited.value.code)


def test_cli_check_and_plan_fail_cleanly_on_invalid_topology(tmp_path: Path, capsys) -> None:
    dag = _cli_dag(tmp_path)

    @dag.node("broken", deps=("missing",))
    def broken(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"status": "broken"}

    assert _run_dag_cli(dag, ["check"]) == 1
    output = capsys.readouterr().out
    assert "topology:" in output
    assert "Unknown dependency" in output

    assert _run_dag_cli(dag, ["plan"]) == 1
    error = capsys.readouterr().err
    assert "Unknown dependency" in error
    assert "Traceback" not in error


def test_cli_check_and_plan_fail_cleanly_on_missing_prompt_file(tmp_path: Path, capsys) -> None:
    dag = _cli_dag(tmp_path)

    @dag.node("prompted", prompt_specs=(PromptSpec("missing", PromptRef("missing")),))
    def prompted(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"status": "prompted"}

    assert _run_dag_cli(dag, ["check"]) == 1
    output = capsys.readouterr().out
    assert "prompts:" in output
    assert "missing" in output

    assert _run_dag_cli(dag, ["plan"]) == 1
    error = capsys.readouterr().err
    assert "missing" in error
    assert "Traceback" not in error
