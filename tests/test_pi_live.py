from __future__ import annotations

import json
import os
import shlex
from pathlib import Path
from typing import Any

import pytest

from kigumi.agents import (
    AgentFileSelector,
    AgentPublish,
    AgentRequest,
    AgentRunContext,
    AgentTask,
)
from kigumi.pi import PiModelConfig, PiProviderConfig, PiRpcAdapter
from tests._agent_helpers import make_agent_spec
from tests._dag_helpers import _make_dag
from tests.test_pi_first import _fake_pi

# Pi's pi-ai compat registry (0.83.0) registers these text-model APIs.
_PI_SUPPORTED_APIS = (
    "anthropic-messages",
    "openai-completions",
    "openai-responses",
    "openai-codex-responses",
    "azure-openai-responses",
    "google-generative-ai",
    "google-vertex",
    "mistral-conversations",
    "bedrock-converse-stream",
    "pi-messages",
)


def _live_providers(provider: str, model: str) -> tuple[PiProviderConfig, ...]:
    """Build optional typed live-fixture config without changing manifest selection."""
    api = os.environ.get("KIGUMI_PI_API", "openai-responses")
    if api not in _PI_SUPPORTED_APIS:
        accepted = ", ".join(_PI_SUPPORTED_APIS)
        raise ValueError(f"KIGUMI_PI_API must be one of: {accepted}; got {api!r}")
    base_url = os.environ.get("KIGUMI_PI_BASE_URL")
    api_key_env = os.environ.get("KIGUMI_PI_API_KEY_ENV")
    if base_url is None and api_key_env is None:
        return ()
    if not base_url or not api_key_env:
        pytest.skip("KIGUMI_PI_BASE_URL and KIGUMI_PI_API_KEY_ENV are required together")
    if not api_key_env.isidentifier():
        pytest.skip("KIGUMI_PI_API_KEY_ENV must be a valid environment variable name")
    if api_key_env not in os.environ:
        pytest.skip(f"{api_key_env} must be set for the configured Pi provider")
    return (
        PiProviderConfig(
            id=provider,
            api=api,
            base_url=base_url,
            api_key_env=api_key_env,
            models=(PiModelConfig(id=model),),
        ),
    )


@pytest.mark.parametrize(
    ("configured_api", "expected_api"),
    ((None, "openai-responses"), ("openai-completions", "openai-completions")),
    ids=("default-responses", "explicit-completions"),
)
def test_live_typed_provider_is_injected_into_pi_temp_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    configured_api: str | None,
    expected_api: str,
) -> None:
    provider = "custom-gateway"
    model = "custom-model"
    base_url = "https://gateway.example/v1"
    api_key_env = "KIGUMI_PI_TEST_API_KEY"
    monkeypatch.setenv("KIGUMI_PI_BASE_URL", base_url)
    monkeypatch.setenv("KIGUMI_PI_API_KEY_ENV", api_key_env)
    monkeypatch.setenv(api_key_env, "fake-live-key-not-real")
    if configured_api is None:
        monkeypatch.delenv("KIGUMI_PI_API", raising=False)
    else:
        monkeypatch.setenv("KIGUMI_PI_API", configured_api)
    config_dump = tmp_path / "pi-config.json"
    monkeypatch.setenv("CONFIG_DUMP_FILE", str(config_dump))

    capsule = tmp_path / "agent"
    spec = make_agent_spec(capsule, tools=("write",))
    manifest = (capsule / "agent.toml").read_text(encoding="utf-8")
    manifest = manifest.replace('provider = "fake"', f'provider = "{provider}"')
    manifest = manifest.replace('model = "fake-model"', f'model = "{model}"')
    (capsule / "agent.toml").write_text(manifest, encoding="utf-8")
    spec = type(spec).load(capsule)

    fake = _fake_pi(tmp_path / "fake-pi")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    capsule_root = workspace / ".kigumi" / "agent"
    spec.stage(capsule_root)
    adapter = PiRpcAdapter(
        (str(fake),),
        "1.2.3",
        env_resolver=lambda: {"KIGUMI_LIVE_SENTINEL": "sentinel"},
        providers=_live_providers(provider, model),
    )

    adapter.run(
        AgentRequest(
            AgentTask("write", collect=(AgentFileSelector("draft.md"),)),
            {},
            spec,
        ),
        AgentRunContext(
            workspace,
            capsule_root,
            10**9,
            lambda event: None,
            lambda name, data, media: None,
        ),
    )

    captured = json.loads(config_dump.read_text(encoding="utf-8"))
    assert json.loads(captured["models.json"]) == {
        "providers": {
            provider: {
                "api": expected_api,
                "apiKey": f"${api_key_env}",
                "baseUrl": base_url,
                "models": [{"id": model}],
            }
        }
    }
    assert "fake-live-key-not-real" not in captured["models.json"]


def test_live_typed_provider_rejects_unknown_api(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIGUMI_PI_BASE_URL", "https://gateway.example/v1")
    monkeypatch.setenv("KIGUMI_PI_API_KEY_ENV", "KIGUMI_PI_TEST_API_KEY")
    monkeypatch.setenv("KIGUMI_PI_TEST_API_KEY", "fake-live-key-not-real")
    monkeypatch.setenv("KIGUMI_PI_API", "not-a-pi-api")

    with pytest.raises(ValueError, match="^KIGUMI_PI_API must be one of:") as raised:
        _live_providers("custom-gateway", "custom-model")
    assert "openai-responses" in str(raised.value)
    assert "got 'not-a-pi-api'" in str(raised.value)


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
    providers = _live_providers(provider, model)

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
        providers=providers,
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

    session_dag = _make_dag(tmp_path)
    session_adapter = PiRpcAdapter(
        tuple(shlex.split(command)),
        version,
        env_resolver=lambda: {"KIGUMI_LIVE_SENTINEL": sentinel},
        session_carry=True,
        providers=providers,
    )

    @session_dag.node("session-source")
    def session_source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @session_dag.agent_scan(
        "session-revise",
        adapter=session_adapter,
        spec=spec,
        items_from=("session-source", "items"),
        key_fn=lambda item: item["id"],
        carry_fn=lambda artifact: artifact["session"],
    )
    def session_revise(
        item: dict[str, str],
        carry: dict[str, Any] | None,
        inputs: dict[str, Any],
        ctx: Any,
    ) -> AgentTask:
        del carry, inputs, ctx
        output = f"{item['id']}.txt"
        return AgentTask(
            f"Write exactly '{item['id']}' to {output}, then submit_result with that file.",
            collect=(AgentFileSelector(output),),
        )

    cold = session_dag.run()
    warm = session_dag.run()
    cold_items = cold.artifacts["session-revise"]["items"]

    assert cold_items["a"]["session"]["bytes"] > 0
    assert cold_items["b"]["session"]["bytes"] > cold_items["a"]["session"]["bytes"]
    assert warm.map_items["session-revise"] == {"a": "hit", "b": "hit"}
    assert warm.artifacts["session-revise"] == cold.artifacts["session-revise"]
