from __future__ import annotations

import hashlib
import json
import shutil
import stat
import subprocess
import textwrap
import time
from pathlib import Path

import pytest

import kigumi.pi as pi_module
from kigumi import (
    AgentExecutionFailure,
    AgentRuntimeFailureCode,
    ProviderFailure,
    ProviderFailureKind,
    ProviderFailureStage,
)
from kigumi.agents import (
    AgentFileSelector,
    AgentLimits,
    AgentPublish,
    AgentRequest,
    AgentRunContext,
    AgentSpec,
    AgentTask,
)
from kigumi.bench import AgentSubject, TrialContext
from kigumi.pi import PiRpcAdapter, normalize_pi_trajectory_event
from kigumi.store import gc_artifacts
from tests._dag_helpers import _make_dag


def test_agent_execution_failure_contract_is_closed_typed_and_redacted() -> None:
    failure = AgentExecutionFailure(
        provider_failure=ProviderFailure(
            provider="provider",
            stage=ProviderFailureStage.PROVIDER,
            kind=ProviderFailureKind.RATE_LIMIT,
            status_code=429,
            retry_after_ms=None,
            provider_request_id=None,
            message_digest="a" * 64,
            retryable_hint=None,
        )
    )

    assert failure.provider_failure is not None
    assert failure.provider_failure.kind is ProviderFailureKind.RATE_LIMIT
    assert failure.provider_failure.status_code == 429
    assert "provider" in str(failure)
    with pytest.raises(ValueError, match="exactly one"):
        AgentExecutionFailure()
    with pytest.raises(ValueError, match="exactly one"):
        AgentExecutionFailure(
            provider_failure=failure.provider_failure,
            runtime_code=AgentRuntimeFailureCode.PROTOCOL,
        )


def test_pi_workspace_secret_scan_fails_before_candidate_collection(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "episode.json"
    output.parent.mkdir()
    secret = "provider-secret-sentinel"
    output.write_bytes(b"x" * (64 * 1024 - 4) + secret.encode("utf-8"))

    with pytest.raises(Exception, match="provider credential bytes"):
        pi_module._assert_workspace_secrets_absent(tmp_path, (secret,))

    output.write_text('{"safe":true}', encoding="utf-8")
    private = tmp_path / ".kigumi" / "pi-home"
    private.mkdir(parents=True)
    (private / "runtime.txt").write_text(secret, encoding="utf-8")
    pi_module._assert_workspace_secrets_absent(tmp_path, (secret,))


def test_normalize_pi_trajectory_event_compacts_cumulative_message_updates() -> None:
    event = {
        "type": "message_update",
        "message": {
            "role": "assistant",
            "content": [{"type": "text", "text": "draft" * 10_000}],
        },
    }
    canonical = json.dumps(
        event,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    normalized = normalize_pi_trajectory_event(event)

    assert normalized == {
        "type": "message_update",
        "event_sha256": hashlib.sha256(canonical).hexdigest(),
        "event_bytes": len(canonical),
        "thinking_content": False,
    }
    assert len(json.dumps(normalized).encode("utf-8")) < len(canonical)
    changed = {
        **event,
        "message": {
            "role": "assistant",
            "content": [{"type": "thinking", "thinking": "hidden"}],
        },
    }
    changed_normalized = normalize_pi_trajectory_event(changed)
    assert changed_normalized["event_sha256"] != normalized["event_sha256"]
    assert changed_normalized["thinking_content"] is True


def test_normalize_pi_trajectory_event_preserves_non_update_content() -> None:
    event = {
        "type": "message_end",
        "message": {"role": "assistant", "content": [{"type": "text", "text": "done"}]},
    }

    normalized = normalize_pi_trajectory_event(event)

    assert normalized == event
    assert normalized is not event


def test_pi_bridge_rejects_runtime_owned_workspace_paths() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required to execute the Pi bridge path-policy contract")
    policy = Path(pi_module.__file__).with_name("_pi_bridge_policy.mjs")
    script = """
const { checkedPath } = await import(process.argv[1]);
for (const candidate of [
  ".kigumi",
  ".kigumi/hidden.txt",
  "./.kigumi/hidden.txt",
  ".KIGUMI/hidden.txt",
  ".KiGuMi/hidden.txt",
  "./.KiGuMi/hidden.txt",
]) {
  let rejected = false;
  try {
    checkedPath(candidate);
  } catch {
    rejected = true;
  }
  if (!rejected) throw new Error(`runtime-owned path was admitted: ${candidate}`);
}
for (const candidate of [
  "outputs/episode.json",
  ".kigumi-not-reserved/file.txt",
  "nested/.kigumi/file.txt",
]) {
  if (checkedPath(candidate) !== candidate) {
    throw new Error(`ordinary workspace path changed: ${candidate}`);
  }
}
"""
    completed = subprocess.run(
        (node, "--input-type=module", "-e", script, policy.as_uri()),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def _capsule(
    root: Path,
    *,
    model: str = "test-model",
    tools: tuple[str, ...] = ("write",),
    thinking: str = "low",
) -> Path:
    root.mkdir()
    (root / "SYSTEM.md").write_text("Be exact.\n", encoding="utf-8")
    (root / "skills").mkdir()
    (root / "skills" / "writer.md").write_text("Write.\n", encoding="utf-8")
    (root / "hooks").mkdir()
    (root / "hooks" / "policy.ts").write_text("export default () => {};\n", encoding="utf-8")
    tool_list = ", ".join(f'"{tool}"' for tool in tools)
    (root / "agent.toml").write_text(
        textwrap.dedent(
            f"""
            schema_version = 1
            runtime = "pi"
            provider = "test"
            model = "{model}"
            thinking = "{thinking}"
            system_prompt = "SYSTEM.md"
            skills = ["skills"]
            hooks = ["hooks/policy.ts"]
            tools = [{tool_list}]

            [limits]
            timeout_seconds = 3
            max_turns = 4
            max_tool_calls = 8
            max_files = 10
            max_bytes = 100000
            max_single_file_bytes = 50000
            inline_text_max_bytes = 10000
            trajectory_max_events = 100
            trajectory_max_bytes = 100000
            rpc_max_bytes = 100000
            stderr_max_bytes = 10000
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return root


def test_agent_spec_hashes_only_manifest_referenced_resources(tmp_path: Path) -> None:
    capsule = _capsule(tmp_path / "agent")
    first = AgentSpec.load(capsule)
    (capsule / "notes.txt").write_text("not referenced", encoding="utf-8")
    assert AgentSpec.load(capsule).digest == first.digest

    (capsule / "skills" / "writer.md").write_text("Changed.\n", encoding="utf-8")
    assert AgentSpec.load(capsule).digest != first.digest
    assert first.runtime == "pi"
    assert isinstance(first.limits, AgentLimits)


def test_agent_spec_digest_changes_for_every_execution_semantic(tmp_path: Path) -> None:
    roots = [tmp_path / name for name in ("base", "model", "tools", "hook", "limit")]
    specs = []
    for root in roots:
        _capsule(root)
    model_manifest = (roots[1] / "agent.toml").read_text(encoding="utf-8")
    (roots[1] / "agent.toml").write_text(
        model_manifest.replace('model = "test-model"', 'model = "other-model"'),
        encoding="utf-8",
    )
    tools_manifest = (roots[2] / "agent.toml").read_text(encoding="utf-8")
    (roots[2] / "agent.toml").write_text(
        tools_manifest.replace('tools = ["write"]', 'tools = ["read", "write"]'),
        encoding="utf-8",
    )
    (roots[3] / "hooks" / "policy.ts").write_text(
        "export default () => { throw new Error('changed') };\n", encoding="utf-8"
    )
    limit_manifest = (roots[4] / "agent.toml").read_text(encoding="utf-8")
    (roots[4] / "agent.toml").write_text(
        limit_manifest.replace("max_turns = 4", "max_turns = 5"), encoding="utf-8"
    )
    specs.extend(AgentSpec.load(root) for root in roots)
    assert len({spec.digest for spec in specs}) == len(specs)


def test_agent_spec_rejects_unsafe_capsules(tmp_path: Path) -> None:
    for mutation in ("symlink", "escape", "credential", "bash"):
        capsule = _capsule(tmp_path / mutation)
        if mutation == "symlink":
            (capsule / "hooks" / "policy.ts").unlink()
            (capsule / "hooks" / "policy.ts").symlink_to(capsule / "SYSTEM.md")
        elif mutation == "escape":
            manifest = (capsule / "agent.toml").read_text(encoding="utf-8")
            (capsule / "agent.toml").write_text(
                manifest.replace('system_prompt = "SYSTEM.md"', 'system_prompt = "../SYSTEM.md"'),
                encoding="utf-8",
            )
        elif mutation == "credential":
            with (capsule / "agent.toml").open("a", encoding="utf-8") as handle:
                handle.write('\napi_key = "secret"\n')
        else:
            manifest = (capsule / "agent.toml").read_text(encoding="utf-8")
            (capsule / "agent.toml").write_text(
                manifest.replace('tools = ["write"]', 'tools = ["bash"]'),
                encoding="utf-8",
            )
        with pytest.raises(ValueError):
            AgentSpec.load(capsule)


def _fake_pi(path: Path) -> Path:
    path.write_text(
        textwrap.dedent(
            """
            #!/usr/bin/env python3
            import json
            import os
            import pathlib
            import sys

            if "--version" in sys.argv:
                print("1.2.3")
                raise SystemExit(0)
            if os.environ.get("ARGS_FILE"):
                pathlib.Path(os.environ["ARGS_FILE"]).write_text(
                    json.dumps(sys.argv[1:]), encoding="utf-8"
                )
            command = json.loads(sys.stdin.readline())
            assert command["type"] == "prompt"
            pathlib.Path("draft.md").write_text("draft", encoding="utf-8")
            accepted = {
                "id": command["id"], "type": "response",
                "command": "prompt", "success": True,
            }
            completion = {
                "status": "completed", "summary": "done",
                "outputs": ["draft.md"], "metrics": {"quality": 1},
            }
            submitted = {
                "type": "tool_execution_end", "toolName": "submit_result",
                "result": {"details": {
                    "completion": completion,
                    "evidence": [{"name": "quality", "value": 1}],
                }},
            }
            message = {
                "type": "message_end", "message": {
                    "role": "assistant", "stopReason": "toolUse",
                    "model": "test-model",
                    "usage": {
                        "input": 3, "output": 2, "totalTokens": 5,
                        "cost": {"total": 0.01},
                    },
                },
            }
            update = {
                "type": "message_update", "message": {
                    "role": "assistant",
                    "content": [{"type": "text", "text": "draft"}],
                },
            }
            print(json.dumps(accepted), flush=True)
            print(json.dumps(update), flush=True)
            print(json.dumps(submitted), flush=True)
            print(json.dumps(message), flush=True)
            print(json.dumps({"type": "agent_settled"}), flush=True)
            """
        ).lstrip(),
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _failing_pi(path: Path) -> Path:
    path.write_text(
        textwrap.dedent(
            """
            #!/usr/bin/env python3
            import json
            import os
            import subprocess
            import sys
            import time

            if "--version" in sys.argv:
                print("1.2.3")
                raise SystemExit(0)
            command = json.loads(sys.stdin.readline())
            mode = os.environ["FAKE_MODE"]
            accepted = {
                "id": command["id"], "type": "response",
                "command": "prompt", "success": True,
            }
            if mode == "malformed":
                sys.stdout.write("{bad\\n")
                sys.stdout.flush()
            elif mode == "crlf":
                sys.stdout.buffer.write(json.dumps(accepted).encode() + b"\\r\\n")
                sys.stdout.flush()
            elif mode == "nonzero":
                sys.stderr.write("secret=very-secret\\n")
                raise SystemExit(7)
            elif mode == "nonzero_after":
                print(json.dumps(accepted), flush=True)
                completion = {
                    "status": "completed", "summary": "done",
                    "outputs": [], "metrics": {},
                }
                submitted = {
                    "type": "tool_execution_end", "toolName": "submit_result",
                    "result": {"details": {"completion": completion, "evidence": []}},
                }
                print(json.dumps(submitted), flush=True)
                print(json.dumps({"type": "agent_settled"}), flush=True)
                raise SystemExit(9)
            elif mode == "missing":
                print(json.dumps(accepted), flush=True)
                print(json.dumps({"type": "agent_settled"}), flush=True)
            elif mode == "interaction":
                print(json.dumps(accepted), flush=True)
                request = {
                    "type": "extension_ui_request", "id": "ui-1",
                    "method": "confirm", "message": "allow?",
                }
                print(json.dumps(request), flush=True)
                sys.stdin.readline()
            elif mode == "turns":
                print(json.dumps(accepted), flush=True)
                for _ in range(5):
                    print(json.dumps({"type": "turn_start"}), flush=True)
                time.sleep(60)
            elif mode == "tools":
                print(json.dumps(accepted), flush=True)
                for index in range(9):
                    event = {
                        "type": "tool_execution_start",
                        "toolCallId": str(index), "toolName": "write", "args": {},
                    }
                    print(json.dumps(event), flush=True)
                time.sleep(60)
            elif mode == "bash":
                print(json.dumps(accepted), flush=True)
                event = {
                    "type": "tool_execution_start",
                    "toolCallId": "1", "toolName": "bash", "args": {},
                }
                print(json.dumps(event), flush=True)
                time.sleep(60)
            elif mode == "thinking_off":
                print(json.dumps(accepted), flush=True)
                event = {
                    "type": "message_update",
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "thinking", "thinking": "hidden"}],
                    },
                }
                print(json.dumps(event), flush=True)
                time.sleep(60)
            elif mode == "reasoning_usage_off":
                print(json.dumps(accepted), flush=True)
                event = {
                    "type": "message_end",
                    "message": {
                        "role": "assistant",
                        "stopReason": "toolUse",
                        "content": [],
                        "usage": {
                            "input": 3,
                            "output": 2,
                            "reasoning": 2,
                            "totalTokens": 5,
                            "cost": {"total": 0.01},
                        },
                    },
                }
                print(json.dumps(event), flush=True)
                time.sleep(60)
            elif mode == "response_model_drift":
                print(json.dumps(accepted), flush=True)
                event = {
                    "type": "message_end",
                    "message": {
                        "role": "assistant",
                        "model": "test-model",
                        "responseModel": "substituted-model",
                        "stopReason": "toolUse",
                        "content": [],
                        "usage": {
                            "input": 3,
                            "output": 2,
                            "reasoning": 0,
                            "totalTokens": 5,
                            "cost": {"total": 0.01},
                        },
                    },
                }
                print(json.dumps(event), flush=True)
                time.sleep(60)
            elif mode == "auto_retry":
                print(json.dumps(accepted), flush=True)
                event = {
                    "type": "auto_retry_start",
                    "attempt": 1,
                    "maxAttempts": 3,
                    "delayMs": 2000,
                    "errorMessage": "Provider returned error",
                }
                print(json.dumps(event), flush=True)
                time.sleep(60)
            elif mode == "provider_error":
                print(json.dumps(accepted), flush=True)
                event = {
                    "type": "message_end",
                    "message": {
                        "role": "assistant",
                        "model": "test-model",
                        "stopReason": "error",
                        "errorMessage": "429 too many requests very-secret",
                        "content": [],
                        "usage": {
                            "input": 0,
                            "output": 0,
                            "totalTokens": 0,
                            "cost": {"total": 0},
                        },
                    },
                }
                print(json.dumps(event), flush=True)
                print(json.dumps({"type": "agent_settled"}), flush=True)
            elif mode == "timeout":
                child = subprocess.Popen(["sleep", "60"])
                with open(os.environ["PID_FILE"], "w") as handle:
                    handle.write(str(child.pid))
                time.sleep(60)
            """
        ).lstrip(),
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def test_pi_rpc_adapter_parses_fixed_completion_and_redacts_raw_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = AgentSpec.load(_capsule(tmp_path / "agent", thinking="off"))
    fake = _fake_pi(tmp_path / "fake-pi")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    capsule_root = workspace / ".kigumi" / "agent"
    spec.stage(capsule_root)
    captured: list[tuple[str, bytes, str]] = []
    events: list[dict[str, object]] = []
    args_file = tmp_path / "args.json"
    monkeypatch.setenv("ARGS_FILE", str(args_file))
    adapter = PiRpcAdapter(
        (str(fake),),
        "1.2.3",
        env_resolver=lambda: {"TEST_TOKEN": "very-secret"},
    )
    result = adapter.run(
        AgentRequest(
            AgentTask(
                "write",
                collect=(AgentFileSelector("draft.md"),),
                publish=(AgentPublish("draft.md", "out.md"),),
            ),
            {},
            spec,
        ),
        AgentRunContext(
            workspace=workspace,
            capsule_root=capsule_root,
            deadline=10**9,
            emit_event=events.append,
            record_evidence=lambda name, data, media: captured.append((name, data, media)),
        ),
    )
    assert result.completion.summary == "done"
    assert result.completion.outputs == ("draft.md",)
    assert result.usage == {
        "input": 3,
        "output": 2,
        "reasoning": 0,
        "total_tokens": 5,
        "cost": 0.01,
    }
    assert result.metadata["stop_reason"] == "toolUse"
    assert result.metadata["thinking_events"] == 0
    assert result.metadata["response_model_checks"] == 1
    assert result.metadata["response_model_substitutions"] == 0
    assert result.metadata["response_models"] == []
    expected_update = {
        "type": "message_update",
        "message": {
            "role": "assistant",
            "content": [{"type": "text", "text": "draft"}],
        },
    }
    assert normalize_pi_trajectory_event(expected_update) in events
    assert not any(event.get("type") == "message_update" and "message" in event for event in events)
    assert json.loads(
        (workspace / ".kigumi" / "pi-home" / "settings.json").read_text(encoding="utf-8")
    ) == {
        "retry": {
            "enabled": False,
            "maxRetries": 0,
            "provider": {"maxRetries": 0},
        }
    }
    assert len(adapter.cache_identity()["settings_sha256"]) == 64
    assert {name for name, _, _ in captured} == {"pi-rpc.jsonl", "pi-stderr.txt"}
    assert b"very-secret" not in b"".join(data for _, data, _ in captured)
    args = json.loads(args_file.read_text(encoding="utf-8"))
    assert {
        "--mode",
        "--no-session",
        "--no-approve",
        "--no-extensions",
        "--no-skills",
        "--no-prompt-templates",
        "--no-themes",
        "--no-context-files",
        "--no-builtin-tools",
        "--tools",
        "--provider",
        "--model",
        "--thinking",
        "--system-prompt",
    } <= set(args)
    extensions = [args[index + 1] for index, value in enumerate(args) if value == "--extension"]
    assert extensions[0].endswith("hooks/policy.ts")
    assert extensions[-1].endswith("kigumi/_pi_bridge.ts")


def test_pi_rpc_adapter_fails_closed_on_version_mismatch(tmp_path: Path) -> None:
    spec = AgentSpec.load(_capsule(tmp_path / "agent"))
    fake = _fake_pi(tmp_path / "fake-pi")
    adapter = PiRpcAdapter((str(fake),), "9.9.9")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    capsule_root = workspace / ".kigumi" / "agent"
    spec.stage(capsule_root)
    with pytest.raises(AgentExecutionFailure) as raised:
        adapter.run(
            AgentRequest(AgentTask("write"), {}, spec),
            AgentRunContext(
                workspace,
                capsule_root,
                10**9,
                lambda event: None,
                lambda name, data, media: None,
            ),
        )
    assert raised.value.runtime_code is AgentRuntimeFailureCode.VERSION_MISMATCH
    assert raised.value.provider_failure is None


def test_pi_adapter_rejects_credentials_in_command_identity() -> None:
    with pytest.raises(ValueError, match="env_resolver"):
        PiRpcAdapter(("pi", "--api-key=secret"), "1.2.3")


@pytest.mark.parametrize(
    ("mode", "runtime_code", "provider_kind", "message"),
    [
        ("malformed", AgentRuntimeFailureCode.PROTOCOL, None, None),
        ("crlf", None, None, "strict LF"),
        ("nonzero", AgentRuntimeFailureCode.PROCESS_EXIT, None, None),
        ("nonzero_after", AgentRuntimeFailureCode.PROCESS_EXIT, None, None),
        ("missing", AgentRuntimeFailureCode.PROTOCOL, None, None),
        ("interaction", AgentRuntimeFailureCode.POLICY, None, None),
        ("turns", AgentRuntimeFailureCode.POLICY, None, None),
        ("tools", AgentRuntimeFailureCode.POLICY, None, None),
        ("bash", AgentRuntimeFailureCode.POLICY, None, None),
        ("thinking_off", None, None, "thinking content"),
        ("reasoning_usage_off", None, None, "reasoning tokens"),
        ("response_model_drift", None, ProviderFailureKind.MODEL_MISMATCH, None),
        ("auto_retry", AgentRuntimeFailureCode.POLICY, None, None),
    ],
)
def test_pi_rpc_adapter_fails_closed_and_keeps_redacted_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    runtime_code: AgentRuntimeFailureCode | None,
    provider_kind: ProviderFailureKind | None,
    message: str | None,
) -> None:
    spec = AgentSpec.load(
        _capsule(
            tmp_path / "agent",
            thinking="off" if mode in {"thinking_off", "reasoning_usage_off"} else "low",
        )
    )
    fake = _failing_pi(tmp_path / "fake-pi")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    capsule_root = workspace / ".kigumi" / "agent"
    spec.stage(capsule_root)
    captured: list[tuple[str, bytes, str]] = []
    monkeypatch.setenv("FAKE_MODE", mode)
    adapter = PiRpcAdapter(
        (str(fake),),
        "1.2.3",
        env_resolver=lambda: {"TEST_TOKEN": "very-secret"},
    )
    with pytest.raises(Exception) as raised:
        adapter.run(
            AgentRequest(AgentTask("write"), {}, spec),
            AgentRunContext(
                workspace,
                capsule_root,
                time.monotonic() + 5,
                lambda event: None,
                lambda name, data, media: captured.append((name, data, media)),
            ),
        )
    if runtime_code is not None:
        assert isinstance(raised.value, AgentExecutionFailure)
        assert raised.value.runtime_code is runtime_code
    if provider_kind is not None:
        assert isinstance(raised.value, AgentExecutionFailure)
        assert raised.value.provider_failure is not None
        assert raised.value.provider_failure.kind is provider_kind
    if message is not None:
        assert message in str(raised.value)
    assert {name for name, _, _ in captured} == {"pi-rpc.jsonl", "pi-stderr.txt"}
    assert b"very-secret" not in b"".join(data for _, data, _ in captured)


def test_pi_provider_error_prose_is_terminal_unclassified_and_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = AgentSpec.load(_capsule(tmp_path / "agent"))
    fake = _failing_pi(tmp_path / "fake-pi")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    capsule_root = workspace / ".kigumi" / "agent"
    spec.stage(capsule_root)
    captured: list[tuple[str, bytes, str]] = []
    monkeypatch.setenv("FAKE_MODE", "provider_error")
    adapter = PiRpcAdapter(
        (str(fake),),
        "1.2.3",
        env_resolver=lambda: {"TEST_TOKEN": "very-secret"},
    )

    with pytest.raises(AgentExecutionFailure) as raised:
        adapter.run(
            AgentRequest(AgentTask("write"), {}, spec),
            AgentRunContext(
                workspace,
                capsule_root,
                time.monotonic() + 5,
                lambda event: None,
                lambda name, data, media: captured.append((name, data, media)),
            ),
        )

    failure = raised.value
    assert failure.provider_failure is not None
    assert failure.provider_failure.kind is ProviderFailureKind.UNKNOWN
    assert failure.runtime_code is None
    assert "429" not in str(failure)
    assert "too many requests" not in str(failure)
    assert "very-secret" not in str(failure)
    assert b"very-secret" not in b"".join(data for _, data, _ in captured)


def test_pi_spawn_and_timeout_failures_are_typed(tmp_path: Path) -> None:
    spec = AgentSpec.load(_capsule(tmp_path / "agent"))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    capsule_root = workspace / ".kigumi" / "agent"
    spec.stage(capsule_root)
    context = AgentRunContext(
        workspace,
        capsule_root,
        time.monotonic() + 5,
        lambda event: None,
        lambda name, data, media: None,
    )

    with pytest.raises(AgentExecutionFailure) as missing:
        PiRpcAdapter((str(tmp_path / "missing-pi"),), "1.2.3").run(
            AgentRequest(AgentTask("write"), {}, spec),
            context,
        )

    assert missing.value.runtime_code is AgentRuntimeFailureCode.SPAWN_NOT_FOUND


def test_pi_timeout_terminates_the_whole_process_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = AgentSpec.load(_capsule(tmp_path / "agent"))
    fake = _failing_pi(tmp_path / "fake-pi")
    pid_file = tmp_path / "child.pid"
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    capsule_root = workspace / ".kigumi" / "agent"
    spec.stage(capsule_root)
    monkeypatch.setenv("FAKE_MODE", "timeout")
    monkeypatch.setenv("PID_FILE", str(pid_file))
    adapter = PiRpcAdapter(
        (str(fake),),
        "1.2.3",
    )
    with pytest.raises(AgentExecutionFailure) as raised:
        adapter.run(
            AgentRequest(AgentTask("write"), {}, spec),
            AgentRunContext(
                workspace,
                capsule_root,
                time.monotonic() + 1.0,
                lambda event: None,
                lambda name, data, media: None,
            ),
        )
    assert raised.value.provider_failure is not None
    assert raised.value.provider_failure.kind is ProviderFailureKind.TIMEOUT
    child_pid = int(pid_file.read_text(encoding="utf-8"))
    status = ""
    for _ in range(20):
        status = subprocess.run(
            ["ps", "-p", str(child_pid), "-o", "stat="],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        if not status or status.startswith("Z"):
            break
        time.sleep(0.05)
    assert not status or status.startswith("Z")


def test_pi_success_and_failure_evidence_is_blob_verified_and_gc_reachable(
    tmp_path: Path,
) -> None:
    success_spec = AgentSpec.load(_capsule(tmp_path / "success-agent"))
    success_dag = _make_dag(tmp_path)
    success_adapter = PiRpcAdapter((str(_fake_pi(tmp_path / "success-pi")),), "1.2.3")

    @success_dag.agent("success", adapter=success_adapter, spec=success_spec, cache="off")
    def success(inputs, ctx):
        return AgentTask(
            "write",
            collect=(AgentFileSelector("draft.md"),),
            publish=(AgentPublish("draft.md", "published.md"),),
        )

    success_result = success_dag.run()
    artifact = success_result.artifacts["success"]
    success_sidecar = json.loads(
        (
            tmp_path / "artifacts" / "runs" / success_result.run_id / "success.json.meta.json"
        ).read_text(encoding="utf-8")
    )
    agent_origin = success_sidecar["origin_provenance"]["agent"]
    references = [agent_origin["trajectory"], *agent_origin["evidence"]]
    for reference in references:
        data = success_dag.blob_store.read_verified(reference["kigumi_attachment"])
        assert len(data) == reference["bytes"]
        if reference["workspace_path"].endswith(".jsonl"):
            assert all(json.loads(line) for line in data.splitlines())
    assert agent_origin["usage"]["total_tokens"] == 5
    assert "trajectory" not in artifact
    assert "evidence" not in artifact

    failure_spec = AgentSpec.load(_capsule(tmp_path / "failure-agent"))
    failure_adapter = PiRpcAdapter(
        (str(_failing_pi(tmp_path / "failure-pi")),),
        "1.2.3",
        env_resolver=lambda: {"FAKE_MODE": "malformed"},
    )
    failure_dag = _make_dag(tmp_path)

    @failure_dag.agent("failure", adapter=failure_adapter, spec=failure_spec, cache="off")
    def failure(inputs, ctx):
        return AgentTask("fail")

    with pytest.raises(AgentExecutionFailure) as raised:
        failure_dag.run()
    assert raised.value.runtime_code is AgentRuntimeFailureCode.PROTOCOL
    failure_path = next((tmp_path / "artifacts" / "runs").glob("*/failures/failure.json"))
    failure_record = json.loads(failure_path.read_text(encoding="utf-8"))
    assert len(failure_record["evidence"]) == 2
    digests = {
        reference["kigumi_attachment"] for reference in [*references, *failure_record["evidence"]]
    }
    gc_artifacts(tmp_path / "artifacts", keep_last=2)
    for digest in digests:
        assert success_dag.blob_store.read_verified(digest) is not None


def test_agent_subject_declares_files_and_disables_target_cache(tmp_path: Path) -> None:
    spec = AgentSpec.load(_capsule(tmp_path / "agent"))

    class Adapter:
        def cache_identity(self) -> dict[str, object]:
            return {"adapter": "fake", "version": 1}

        def capabilities(self):
            from kigumi.agents import AgentCapabilities

            return AgentCapabilities()

        def run(self, request, context):
            from kigumi.agents import AgentCompletion, AgentRunResult

            assert (context.workspace / "input.txt").read_text() == "hello"
            (context.workspace / "draft.md").write_text(request.inputs["example"]["text"])
            return AgentRunResult(AgentCompletion("completed", "done", ("draft.md",), {}))

    subject = AgentSubject(
        Adapter(),
        spec,
        lambda example, ctx: AgentTask(
            "write",
            collect=(AgentFileSelector("draft.md"),),
            publish=(AgentPublish("draft.md", "published.md"),),
        ),
        files=lambda example: {"input.txt": example["text"]},
        output=lambda artifact: artifact["completion"]["summary"],
    )
    context = TrialContext("example", 0, "trial", tmp_path / "project", tmp_path / "evidence")
    context.project_root.mkdir()
    context.evidence_root.mkdir()
    observation = subject.run({"text": "hello"}, context)
    assert observation.output == "done"
    assert observation.evidence["cache"] == "off"
    assert observation.evidence["agent"]["spec"]["digest"] == spec.digest
