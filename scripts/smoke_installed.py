#!/usr/bin/env python3
"""Smoke the public API and minimal CALL/DAG replay from an installed wheel."""

from __future__ import annotations

import os
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

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
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
    print(f"installed smoke passed for kigumi {expected}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
