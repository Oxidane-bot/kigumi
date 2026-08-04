#!/usr/bin/env python3
"""Smoke the public API and minimal CALL/DAG replay from an installed wheel."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import sysconfig
import tempfile
from importlib.resources import files
from pathlib import Path

import kigumi
from kigumi import (
    AgentSpec,
    Dag,
    EvidencePolicy,
    InputRef,
    LLMCaller,
    PiRpcAdapter,
    PromptAxis,
    PromptLayer,
    PromptMaterial,
    PromptRef,
    PromptResolution,
    PromptSpec,
    ProviderFailure,
    ResolvedPrompt,
    RetryPolicy,
)
from kigumi.config import KigumiConfig
from kigumi.docs import SHIPPED_DOCS, read_doc
from kigumi.transport import Response


class _Transport:
    def __init__(self) -> None:
        self.requests = 0

    def resolve(self, model: str) -> str:
        return model

    def complete(self, messages: object, model: str, **params: object) -> Response:
        del messages, model, params
        self.requests += 1
        return Response("smoke", {"total_tokens": 1}, "stop")


def main() -> int:
    expected = os.environ["KIGUMI_EXPECTED_VERSION"]
    assert kigumi.__version__ == expected
    package_path = Path(kigumi.__file__).resolve()
    site_packages = Path(sysconfig.get_paths()["purelib"]).resolve()
    assert package_path.is_relative_to(site_packages), (
        f"installed smoke imported kigumi outside site-packages: {package_path}"
    )
    assert callable(getattr(Dag, "agent_scan", None))
    assert all(
        symbol is not None
        for symbol in (
            AgentSpec,
            PiRpcAdapter,
            EvidencePolicy,
            RetryPolicy,
            ProviderFailure,
            PromptRef,
            InputRef,
            PromptAxis,
            PromptLayer,
            PromptMaterial,
            PromptSpec,
            PromptResolution,
            ResolvedPrompt,
        )
    )
    package = files("kigumi")
    assert package.joinpath("_pi_bridge.ts").read_bytes()
    assert package.joinpath("_pi_bridge_policy.mjs").read_bytes()

    # Every documented page must be readable from the wheel with no checkout present,
    # and both doc commands must run without a configured project.
    for doc in SHIPPED_DOCS:
        assert read_doc(doc.name).strip(), f"shipped doc {doc.name} is empty"

    executable = Path(sys.executable).with_name("kigumi")
    if not executable.is_file():
        located = shutil.which("kigumi")
        assert located is not None, "installed smoke requires the real kigumi console script"
        executable = Path(located)
    executable = executable.resolve()
    environment_bin = Path(sys.executable).parent.resolve()
    assert executable.parent == environment_bin, (
        f"installed smoke found kigumi outside the active environment: {executable}"
    )

    def run_cli(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
        completed = subprocess.run(
            [str(executable), *arguments],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, (
            f"kigumi {' '.join(arguments)} failed with {completed.returncode}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
        return completed

    def run_cli_failure(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
        completed = subprocess.run(
            [str(executable), *arguments],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode != 0, (
            f"kigumi {' '.join(arguments)} unexpectedly succeeded\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
        return completed

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        run_cli(root, "--help")
        run_cli(root, "brief")
        run_cli(root, "docs")
        for doc in SHIPPED_DOCS:
            run_cli(root, "docs", doc.name)
        run_cli_failure(root, "docs", "not-a-shipped-doc")
        run_cli_failure(root, "init")

        (root / "pyproject.toml").write_text(
            "[project]\nname = 'installed-smoke'\n", encoding="utf-8"
        )
        run_cli(root, "init")
        pyproject_before_repeat = (root / "pyproject.toml").read_bytes()
        run_cli(root, "init")
        assert (root / "pyproject.toml").read_bytes() == pyproject_before_repeat
        run_cli(root, "check")

        generated_run = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from nodes.graph import build_dag; "
                    "result = build_dag().run(run_id='installed-init'); "
                    "assert result.artifacts['example'] == {'ok': 'replace me'}"
                ),
            ],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        assert generated_run.returncode == 0, (
            "generated init graph failed to run\n"
            f"stdout:\n{generated_run.stdout}\nstderr:\n{generated_run.stderr}"
        )

        invalid_node = root / "nodes" / "invalid.py"
        invalid_node.write_text("for item in items:\n    client.call([])\n", encoding="utf-8")
        run_cli_failure(root, "check")
        invalid_node.unlink()

        hooks_root = root / "hooks-negative"
        hooks_root.mkdir()
        (hooks_root / "pyproject.toml").write_text(
            "[project]\nname = 'hooks-smoke'\n", encoding="utf-8"
        )
        run_cli_failure(hooks_root, "init", "--hooks")

        existing_hook_root = root / "existing-hook-negative"
        (existing_hook_root / ".git" / "hooks").mkdir(parents=True)
        (existing_hook_root / ".git" / "hooks" / "pre-commit").write_text(
            "#!/bin/sh\n", encoding="utf-8"
        )
        (existing_hook_root / "pyproject.toml").write_text(
            "[project]\nname = 'existing-hook-smoke'\n", encoding="utf-8"
        )
        run_cli_failure(existing_hook_root, "init", "--hooks")

        transport = _Transport()
        caller = LLMCaller(transport, root / "artifacts" / "_llm")
        dag = Dag(KigumiConfig(project_root=root, source_dirs=[]), caller)

        @dag.node("call")
        def call(inputs: dict[str, object], ctx: object) -> dict[str, str]:
            del inputs
            return {"response": ctx.call("smoke")}  # type: ignore[attr-defined]

        assert dag.run().artifacts["call"] == {"response": "smoke"}
        assert dag.run().cache_hits == ["call"]
        assert transport.requests == 1
    artifact = os.environ.get("KIGUMI_SMOKE_ARTIFACT", "installed distribution")
    print(f"installed smoke passed for kigumi {expected} ({artifact})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
