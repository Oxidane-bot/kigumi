from __future__ import annotations

import json
import os
import shlex
from pathlib import Path
from typing import Any

import pytest

from kigumi.agents import AgentFileSelector, AgentPublish, AgentTask
from kigumi.pi import PiRpcAdapter
from tests._agent_helpers import make_agent_spec
from tests._dag_helpers import _make_dag


@pytest.mark.live
def test_real_pi_rpc_conformance(tmp_path: Path) -> None:
    """Opt-in smoke test for an installed, credentialed Pi runtime."""
    if os.environ.get("KIGUMI_PI_LIVE") != "1":
        pytest.skip("set KIGUMI_PI_LIVE=1 to run the real Pi conformance test")
    command = os.environ.get("KIGUMI_PI_COMMAND", "pi")
    version = os.environ.get("KIGUMI_PI_VERSION")
    if not version:
        pytest.skip("KIGUMI_PI_VERSION must pin the exact installed Pi version")
    provider = os.environ.get("KIGUMI_PI_PROVIDER")
    model = os.environ.get("KIGUMI_PI_MODEL")
    if not provider or not model:
        pytest.skip("KIGUMI_PI_PROVIDER and KIGUMI_PI_MODEL are required")

    capsule = tmp_path / "agent"
    spec = make_agent_spec(capsule, tools=("write",))
    manifest = (capsule / "agent.toml").read_text(encoding="utf-8")
    manifest = manifest.replace('provider = "fake"', f'provider = "{provider}"')
    manifest = manifest.replace('model = "fake-model"', f'model = "{model}"')
    (capsule / "agent.toml").write_text(manifest, encoding="utf-8")
    spec = type(spec).load(capsule)
    dag = _make_dag(tmp_path)
    sentinel = "kigumi-live-secret-must-not-persist"
    adapter = PiRpcAdapter(
        tuple(shlex.split(command)),
        version,
        env_resolver=lambda: {"KIGUMI_LIVE_SENTINEL": sentinel},
    )

    @dag.agent("pi", adapter=adapter, spec=spec, cache="off")
    def pi_node(inputs: dict[str, Any], ctx: Any) -> AgentTask:
        return AgentTask(
            "Write exactly 'pi live one' to one.txt and exactly 'pi live two' to two.txt, "
            "then submit_result with both files.",
            collect=(AgentFileSelector("one.txt"), AgentFileSelector("two.txt")),
            publish=(
                AgentPublish("one.txt", "published/one.txt"),
                AgentPublish("two.txt", "published/two.txt"),
            ),
        )

    result = dag.run()
    artifact = result.artifacts["pi"]
    assert artifact["completion"]["status"] == "completed"
    assert artifact["completion"]["outputs"] == ["one.txt", "two.txt"]
    assert [item["workspace_path"] for item in artifact["attachments"]] == [
        "one.txt",
        "two.txt",
    ]
    assert (tmp_path / "published" / "one.txt").read_text(encoding="utf-8") == "pi live one"
    assert (tmp_path / "published" / "two.txt").read_text(encoding="utf-8") == "pi live two"

    sidecar = json.loads(
        (tmp_path / "artifacts" / "runs" / result.run_id / "pi.json.meta.json").read_text(
            encoding="utf-8"
        )
    )
    origin = sidecar["origin_provenance"]["agent"]
    assert isinstance(origin["usage"], dict)
    assert origin["trajectory"]["events"] > 0
    assert origin["evidence"]
    assert origin["slot_identity"] == "slot_000"
    assert origin["queue_wait_seconds"] >= 0
    assert origin["exit_reason"] == "completed"
    for path in tmp_path.rglob("*"):
        if path.is_file():
            assert sentinel.encode() not in path.read_bytes()
