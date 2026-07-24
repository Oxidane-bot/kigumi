"""Native Pi RPC adapter with fail-closed JSONL and process-group handling."""

from __future__ import annotations

import contextlib
import contextvars
import copy
import hashlib
import json
import os
import queue
import signal
import subprocess
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from importlib import resources
from pathlib import Path
from typing import Any

from .agents import (
    AgentCapabilities,
    AgentCompletion,
    AgentRequest,
    AgentResultError,
    AgentRunContext,
    AgentRunResult,
)
from .failures import (
    AgentExecutionFailure,
    AgentRuntimeFailureCode,
    ProviderFailure,
    ProviderFailureKind,
    ProviderFailureStage,
)


class _PiFailureCode(StrEnum):
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    MODEL_NOT_ADMITTED = "model_not_admitted"
    POLICY_VIOLATION = "policy_violation"
    MALFORMED_RESPONSE_ENVELOPE = "malformed_response_envelope"
    PROVIDER_FAILURE_UNCLASSIFIED = "provider_failure_unclassified"
    PI_SPAWN_NOT_FOUND = "pi_spawn_not_found"
    PI_SPAWN_PERMISSION = "pi_spawn_permission"
    PI_SPAWN_FAILURE = "pi_spawn_failure"
    PI_PROCESS_EXIT = "pi_process_exit"
    PI_VERSION_MISMATCH = "pi_version_mismatch"


_PI_PROVIDER: contextvars.ContextVar[str] = contextvars.ContextVar(
    "kigumi_pi_provider", default="pi"
)
_PI_RUNTIME_CODES = {
    _PiFailureCode.POLICY_VIOLATION: AgentRuntimeFailureCode.POLICY,
    _PiFailureCode.MALFORMED_RESPONSE_ENVELOPE: AgentRuntimeFailureCode.PROTOCOL,
    _PiFailureCode.PI_SPAWN_NOT_FOUND: AgentRuntimeFailureCode.SPAWN_NOT_FOUND,
    _PiFailureCode.PI_SPAWN_PERMISSION: AgentRuntimeFailureCode.SPAWN_PERMISSION,
    _PiFailureCode.PI_SPAWN_FAILURE: AgentRuntimeFailureCode.SPAWN_FAILURE,
    _PiFailureCode.PI_PROCESS_EXIT: AgentRuntimeFailureCode.PROCESS_EXIT,
    _PiFailureCode.PI_VERSION_MISMATCH: AgentRuntimeFailureCode.VERSION_MISMATCH,
}
_PI_PROVIDER_KINDS = {
    _PiFailureCode.TIMEOUT: ProviderFailureKind.TIMEOUT,
    _PiFailureCode.CONNECTION: ProviderFailureKind.CONNECTION,
    _PiFailureCode.MODEL_NOT_ADMITTED: ProviderFailureKind.MODEL_MISMATCH,
    _PiFailureCode.PROVIDER_FAILURE_UNCLASSIFIED: ProviderFailureKind.UNKNOWN,
}


def _pi_failure(code: _PiFailureCode) -> AgentExecutionFailure:
    runtime_code = _PI_RUNTIME_CODES.get(code)
    if runtime_code is not None:
        return AgentExecutionFailure(runtime_code=runtime_code)
    kind = _PI_PROVIDER_KINDS[code]
    digest = hashlib.sha256(f"pi:{code.value}".encode()).hexdigest()
    return AgentExecutionFailure(
        provider_failure=ProviderFailure(
            provider=_PI_PROVIDER.get(),
            stage=ProviderFailureStage.PROVIDER,
            kind=kind,
            status_code=None,
            retry_after_ms=None,
            provider_request_id=None,
            message_digest=digest,
            retryable_hint=None,
        )
    )


_PI_RPC_SETTINGS = {
    "retry": {
        "enabled": False,
        "maxRetries": 0,
        "provider": {
            "maxRetries": 0,
        },
    },
}


def _pi_rpc_settings_bytes() -> bytes:
    return json.dumps(
        _PI_RPC_SETTINGS,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


@dataclass(frozen=True)
class PiRpcAdapter:
    """Drive one explicitly installed and exactly versioned Pi CLI over RPC."""

    command: tuple[str, ...]
    expected_version: str
    env_resolver: Callable[[], Mapping[str, str]] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.command, tuple)
            or not self.command
            or not all(isinstance(part, str) and part for part in self.command)
        ):
            raise ValueError("Pi command must contain non-empty strings")
        credential_flags = ("--api-key", "--token", "--password", "--secret")
        if any(
            part.lower() == flag or part.lower().startswith(f"{flag}=")
            for part in self.command
            for flag in credential_flags
        ):
            raise ValueError("Pi credentials must come from env_resolver, never command arguments")
        if not isinstance(self.expected_version, str) or not self.expected_version.strip():
            raise ValueError("Pi expected_version must be a non-empty exact version")

    def cache_identity(self) -> dict[str, Any]:
        return {
            "adapter": "pi-rpc",
            "adapter_schema": 1,
            "rpc": "strict-lf-jsonl",
            "command": list(self.command),
            "expected_version": self.expected_version,
            "bridge_sha256": _bridge_digest(),
            "settings_sha256": hashlib.sha256(_pi_rpc_settings_bytes()).hexdigest(),
            "failure_contract": "typed-agent-execution-failure-v2",
            "unclassified_provider_failure_terminal": True,
        }

    def capabilities(self) -> AgentCapabilities:
        return AgentCapabilities(filesystem=True, terminal=False)

    def run(self, request: AgentRequest, context: AgentRunContext) -> AgentRunResult:
        _PI_PROVIDER.set(request.spec.provider)
        environment = copy.deepcopy(dict(os.environ))
        resolved = dict(self.env_resolver()) if self.env_resolver is not None else {}
        if not all(
            isinstance(key, str) and isinstance(value, str) for key, value in resolved.items()
        ):
            raise AgentResultError("Pi env_resolver must return string keys and values")
        environment.update(resolved)
        secrets = tuple(value for value in resolved.values() if value)
        raw_stdout = bytearray()
        stderr = bytearray()
        stderr_truncated = False
        process: subprocess.Popen[bytes] | None = None
        chunks: queue.Queue[bytes | BaseException | None] | None = None
        stdout_thread: threading.Thread | None = None
        stderr_thread: threading.Thread | None = None
        usage: dict[str, Any] | None = None
        stop_reason: str | None = None
        turns = 0
        tool_calls = 0
        thinking_events = 0
        response_model_checks = 0
        response_model_substitutions = 0
        response_models: list[str] = []
        hook_evidence: list[Any] = []
        try:
            version_stdout, version_stderr = self._probe_version(environment, context.deadline)
            if not _append_bounded(stderr, version_stderr, request.spec.limits.stderr_max_bytes):
                stderr_truncated = True
            actual_version = version_stdout.decode("utf-8", errors="strict").strip()
            if actual_version != self.expected_version:
                raise _pi_failure(_PiFailureCode.PI_VERSION_MISMATCH)

            pi_home = context.workspace / ".kigumi" / "pi-home"
            pi_home.mkdir()
            settings_path = pi_home / "settings.json"
            settings_path.write_bytes(_pi_rpc_settings_bytes())
            settings_path.chmod(0o600)
            environment.update(
                {
                    "PI_CODING_AGENT_DIR": str(pi_home),
                    "PI_CODING_AGENT_SESSION_DIR": str(pi_home / "sessions"),
                    "PI_OFFLINE": "1",
                    "PI_SKIP_VERSION_CHECK": "1",
                    "PI_TELEMETRY": "0",
                    "KIGUMI_WORKSPACE": str(context.workspace),
                    "KIGUMI_ALLOWED_TOOLS": ",".join(request.spec.tools),
                    "KIGUMI_MAX_TOOL_CALLS": str(request.spec.limits.max_tool_calls),
                    "KIGUMI_MAX_TURNS": str(request.spec.limits.max_turns),
                }
            )
            args = [
                *self.command,
                "--mode",
                "rpc",
                "--no-session",
                "--no-approve",
                "--no-extensions",
            ]
            for hook in request.spec.hooks:
                args.extend(("--extension", str(context.capsule_root / hook)))
            args.extend(("--extension", str(_bridge_path()), "--no-skills"))
            for skill in request.spec.skills:
                args.extend(("--skill", str(context.capsule_root / skill)))
            args.extend(
                (
                    "--no-prompt-templates",
                    "--no-themes",
                    "--no-context-files",
                    "--no-builtin-tools",
                    "--tools",
                    ",".join((*request.spec.tools, "submit_result")),
                    "--provider",
                    request.spec.provider,
                    "--model",
                    request.spec.model,
                    "--thinking",
                    request.spec.thinking,
                    "--system-prompt",
                    (context.capsule_root / request.spec.system_prompt).read_text(encoding="utf-8"),
                )
            )
            process = _spawn_process(
                args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=context.workspace,
                env=environment,
                start_new_session=True,
            )
            assert process.stdin is not None
            assert process.stdout is not None
            assert process.stderr is not None
            chunks = queue.Queue()

            def read_stdout() -> None:
                try:
                    while chunk := os.read(process.stdout.fileno(), 65536):
                        chunks.put(chunk)
                except OSError as error:
                    chunks.put(error)
                finally:
                    chunks.put(None)

            def read_stderr() -> None:
                nonlocal stderr_truncated
                while chunk := os.read(process.stderr.fileno(), 65536):
                    if not _append_bounded(stderr, chunk, request.spec.limits.stderr_max_bytes):
                        stderr_truncated = True

            stdout_thread = threading.Thread(target=read_stdout, daemon=True)
            stderr_thread = threading.Thread(target=read_stderr, daemon=True)
            stdout_thread.start()
            stderr_thread.start()
            prompt_id = "kigumi-prompt-1"
            _send_json(
                process,
                {"id": prompt_id, "type": "prompt", "message": request.task.instruction},
            )
            buffer = bytearray()
            prompt_accepted = False
            settled = False
            completion: AgentCompletion | None = None
            while not settled:
                remaining = context.deadline - time.monotonic()
                if remaining <= 0:
                    raise _pi_failure(_PiFailureCode.TIMEOUT)
                try:
                    chunk = chunks.get(timeout=min(remaining, 0.1))
                except queue.Empty:
                    if process.poll() is not None and chunks.empty():
                        raise _pi_failure(_PiFailureCode.PI_PROCESS_EXIT) from None
                    continue
                if isinstance(chunk, BaseException):
                    raise _pi_failure(_PiFailureCode.CONNECTION)
                if chunk is None:
                    if buffer:
                        raise AgentResultError("Pi RPC stdout ended with a partial JSONL record")
                    if not settled:
                        raise _pi_failure(_PiFailureCode.PI_PROCESS_EXIT)
                    break
                if len(raw_stdout) + len(chunk) > request.spec.limits.rpc_max_bytes:
                    remaining_bytes = max(request.spec.limits.rpc_max_bytes - len(raw_stdout), 0)
                    raw_stdout.extend(chunk[:remaining_bytes])
                    raise AgentResultError("Pi RPC evidence exceeds rpc_max_bytes")
                raw_stdout.extend(chunk)
                buffer.extend(chunk)
                while True:
                    newline = buffer.find(b"\n")
                    if newline < 0:
                        break
                    raw_line = bytes(buffer[:newline])
                    del buffer[: newline + 1]
                    if raw_line.endswith(b"\r"):
                        raise AgentResultError("Pi RPC stdout must use strict LF framing")
                    if not raw_line:
                        raise AgentResultError("Pi RPC stdout contains an empty JSONL record")
                    event = _decode_event(raw_line)
                    event_type = event.get("type")
                    if event_type in {"message_start", "message_update", "message_end"} and (
                        _contains_thinking_content(event)
                    ):
                        thinking_events += 1
                    redacted_event = _redact_payload(event, secrets)
                    context.emit_event(normalize_pi_trajectory_event(redacted_event))
                    if request.spec.thinking == "off" and thinking_events:
                        raise AgentResultError(
                            "Pi emitted thinking content while AgentSpec thinking=off"
                        )
                    if event_type == "response" and event.get("id") == prompt_id:
                        if event.get("command") != "prompt" or event.get("success") is not True:
                            raise _pi_failure(_PiFailureCode.PROVIDER_FAILURE_UNCLASSIFIED)
                        prompt_accepted = True
                    elif event_type == "extension_ui_request":
                        request_id = event.get("id")
                        if isinstance(request_id, str):
                            _send_json(
                                process,
                                {
                                    "type": "extension_ui_response",
                                    "id": request_id,
                                    "cancelled": True,
                                },
                            )
                        raise _pi_failure(_PiFailureCode.POLICY_VIOLATION)
                    elif event_type in {
                        "extension_error",
                        "auto_retry_start",
                        "auto_retry_end",
                    }:
                        raise _pi_failure(_PiFailureCode.POLICY_VIOLATION)
                    elif event_type == "turn_start":
                        turns += 1
                        if turns > request.spec.limits.max_turns:
                            raise _pi_failure(_PiFailureCode.POLICY_VIOLATION)
                    elif event_type == "tool_execution_start":
                        tool_calls += 1
                        tool_name = event.get("toolName")
                        if tool_calls > request.spec.limits.max_tool_calls:
                            raise _pi_failure(_PiFailureCode.POLICY_VIOLATION)
                        if tool_name in {"bash", "shell", "terminal"}:
                            raise _pi_failure(_PiFailureCode.POLICY_VIOLATION)
                    elif (
                        event_type == "tool_execution_end"
                        and event.get("toolName") == "submit_result"
                    ):
                        if event.get("isError") is True or completion is not None:
                            raise _pi_failure(_PiFailureCode.MALFORMED_RESPONSE_ENVELOPE)
                        details = event.get("result")
                        details = details.get("details") if isinstance(details, Mapping) else None
                        if not isinstance(details, Mapping) or not isinstance(
                            details.get("completion"), Mapping
                        ):
                            raise _pi_failure(_PiFailureCode.MALFORMED_RESPONSE_ENVELOPE)
                        completion_value = _redact_payload(details["completion"], secrets)
                        completion = AgentCompletion.from_mapping(completion_value)
                        evidence_value = details.get("evidence", [])
                        if not isinstance(evidence_value, list) or not all(
                            isinstance(item, Mapping) for item in evidence_value
                        ):
                            raise _pi_failure(_PiFailureCode.MALFORMED_RESPONSE_ENVELOPE)
                        hook_evidence = _redact_payload(evidence_value, secrets)
                    elif event_type == "message_end":
                        message = event.get("message")
                        if isinstance(message, Mapping):
                            if message.get("role") == "assistant":
                                response_model_checks += 1
                                selected_model = message.get("model")
                                response_model = message.get(
                                    "responseModel",
                                    message.get("response_model"),
                                )
                                if response_model is not None:
                                    if (
                                        not isinstance(selected_model, str)
                                        or not selected_model
                                        or not isinstance(response_model, str)
                                        or not response_model
                                    ):
                                        raise _pi_failure(
                                            _PiFailureCode.MALFORMED_RESPONSE_ENVELOPE
                                        )
                                    if response_model != selected_model:
                                        response_model_substitutions += 1
                                        raise _pi_failure(_PiFailureCode.MODEL_NOT_ADMITTED)
                                    response_models.append(response_model)
                            normalized = _normalize_usage(message.get("usage"))
                            if normalized is not None:
                                normalized = _admit_usage(
                                    normalized,
                                    thinking=request.spec.thinking,
                                )
                                usage = _merge_usage(usage, normalized)
                            reason = message.get("stopReason", message.get("stop_reason"))
                            if isinstance(reason, str):
                                stop_reason = reason
                            if message.get("role") == "assistant" and reason in {
                                "error",
                                "aborted",
                            }:
                                raise _pi_failure(_PiFailureCode.PROVIDER_FAILURE_UNCLASSIFIED)
                    elif event_type == "tool_execution_end":
                        tool_result = event.get("result")
                        if isinstance(tool_result, Mapping):
                            normalized = _normalize_usage(tool_result.get("usage"))
                            if normalized is not None:
                                normalized = _admit_usage(
                                    normalized,
                                    thinking=request.spec.thinking,
                                )
                                usage = _merge_usage(usage, normalized)
                    elif event_type == "compaction_end":
                        compaction = event.get("result")
                        if isinstance(compaction, Mapping):
                            normalized = _normalize_usage(compaction.get("usage"))
                            if normalized is not None:
                                normalized = _admit_usage(
                                    normalized,
                                    thinking=request.spec.thinking,
                                )
                                usage = _merge_usage(usage, normalized)
                    elif event_type == "agent_settled":
                        settled = True
            if not prompt_accepted:
                raise _pi_failure(_PiFailureCode.MALFORMED_RESPONSE_ENVELOPE)
            if completion is None:
                raise _pi_failure(_PiFailureCode.MALFORMED_RESPONSE_ENVELOPE)
            if hook_evidence:
                context.emit_event({"type": "hook_evidence", "items": hook_evidence})
            try:
                return_code = process.wait(timeout=0.05)
            except subprocess.TimeoutExpired:
                return_code = None
            if return_code not in {None, 0}:
                raise _pi_failure(_PiFailureCode.PI_PROCESS_EXIT)
            _assert_workspace_secrets_absent(context.workspace, secrets)
            return AgentRunResult(
                completion=completion,
                usage=usage,
                metadata={
                    "pi_version": actual_version,
                    "stop_reason": stop_reason,
                    "turns": turns,
                    "tool_calls": tool_calls,
                    "thinking_events": thinking_events,
                    "response_model_checks": response_model_checks,
                    "response_model_substitutions": response_model_substitutions,
                    "response_models": response_models,
                    "hook_evidence": hook_evidence,
                },
            )
        except AgentExecutionFailure:
            raise
        except AgentResultError as error:
            raise AgentResultError(_redact_text(str(error), secrets)) from None
        except ConnectionError:
            raise _pi_failure(_PiFailureCode.CONNECTION) from None
        except Exception as error:
            raise AgentResultError(f"Pi RPC failed: {type(error).__name__}") from None
        finally:
            if process is not None:
                _terminate_process_group(process)
                if stdout_thread is not None:
                    stdout_thread.join(timeout=0.5)
                if stderr_thread is not None:
                    stderr_thread.join(timeout=0.5)
            trailing_rpc_overflow = False
            if chunks is not None:
                while True:
                    try:
                        trailing = chunks.get_nowait()
                    except queue.Empty:
                        break
                    if trailing is None:
                        continue
                    if isinstance(trailing, BaseException):
                        continue
                    if not _append_bounded(raw_stdout, trailing, request.spec.limits.rpc_max_bytes):
                        trailing_rpc_overflow = True
            rpc_data = _redact_bytes(bytes(raw_stdout), secrets)
            stderr_data = _redact_bytes(bytes(stderr), secrets)
            if stderr_truncated:
                stderr_data = _bounded_marker(
                    stderr_data,
                    request.spec.limits.stderr_max_bytes,
                    b"\n[kigumi: stderr truncated]\n",
                )
            context.emit_event(
                {
                    "type": "pi_execution_summary",
                    "usage": usage,
                    "stop_reason": stop_reason,
                    "turns": turns,
                    "tool_calls": tool_calls,
                    "thinking_events": thinking_events,
                    "response_model_checks": response_model_checks,
                    "response_model_substitutions": response_model_substitutions,
                    "response_models": response_models,
                    "hook_evidence": hook_evidence,
                }
            )
            context.record_evidence("pi-rpc.jsonl", rpc_data, "application/x-ndjson")
            context.record_evidence("pi-stderr.txt", stderr_data, "text/plain")
            if trailing_rpc_overflow:
                raise AgentResultError("Pi RPC evidence exceeds rpc_max_bytes")

    def _probe_version(
        self, environment: Mapping[str, str], deadline: float
    ) -> tuple[bytes, bytes]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _pi_failure(_PiFailureCode.TIMEOUT)
        process = _spawn_process(
            [*self.command, "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            start_new_session=True,
        )
        try:
            try:
                stdout, stderr = process.communicate(timeout=min(remaining, 10.0))
            except subprocess.TimeoutExpired:
                _terminate_process_group(process)
                raise _pi_failure(_PiFailureCode.TIMEOUT) from None
            if process.returncode != 0:
                raise _pi_failure(_PiFailureCode.PI_PROCESS_EXIT)
            return stdout, stderr
        finally:
            _terminate_process_group(process)


def _bridge_path() -> Path:
    return Path(str(resources.files("kigumi").joinpath("_pi_bridge.ts"))).resolve()


def _bridge_policy_path() -> Path:
    return Path(str(resources.files("kigumi").joinpath("_pi_bridge_policy.mjs"))).resolve()


def _bridge_digest() -> str:
    import hashlib

    digest = hashlib.sha256()
    for path in (_bridge_path(), _bridge_policy_path()):
        relative_name = path.name.encode("utf-8")
        payload = path.read_bytes()
        digest.update(len(relative_name).to_bytes(4, "big"))
        digest.update(relative_name)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _spawn_process(args: list[str], **kwargs: Any) -> subprocess.Popen[bytes]:
    try:
        return subprocess.Popen(args, **kwargs)
    except FileNotFoundError:
        raise _pi_failure(_PiFailureCode.PI_SPAWN_NOT_FOUND) from None
    except PermissionError:
        raise _pi_failure(_PiFailureCode.PI_SPAWN_PERMISSION) from None
    except OSError:
        raise _pi_failure(_PiFailureCode.PI_SPAWN_FAILURE) from None


def _send_json(process: subprocess.Popen[bytes], value: Mapping[str, Any]) -> None:
    if process.stdin is None:
        raise _pi_failure(_PiFailureCode.CONNECTION)
    payload = json.dumps(
        dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    try:
        process.stdin.write(payload + b"\n")
        process.stdin.flush()
    except OSError:
        raise _pi_failure(_PiFailureCode.CONNECTION) from None


def _decode_event(raw_line: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw_line.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise _pi_failure(_PiFailureCode.MALFORMED_RESPONSE_ENVELOPE) from None
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise _pi_failure(_PiFailureCode.MALFORMED_RESPONSE_ENVELOPE)
    return value


def normalize_pi_trajectory_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Compact cumulative Pi message updates while binding their canonical content."""
    canonical = json.dumps(
        dict(event),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if event.get("type") == "message_update":
        return {
            "type": "message_update",
            "event_sha256": hashlib.sha256(canonical).hexdigest(),
            "event_bytes": len(canonical),
            "thinking_content": _contains_thinking_content(event),
        }
    return json.loads(canonical)


def _contains_thinking_content(value: Any) -> bool:
    if isinstance(value, Mapping):
        if value.get("type") == "thinking":
            return True
        thinking = value.get("thinking")
        if isinstance(thinking, str) and thinking:
            return True
        return any(_contains_thinking_content(child) for child in value.values())
    if isinstance(value, list):
        return any(_contains_thinking_content(child) for child in value)
    return False


def _normalize_usage(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None

    def integer(*names: str) -> int | None:
        for name in names:
            item = value.get(name)
            if isinstance(item, int) and not isinstance(item, bool):
                return item
        return None

    cost_value = value.get("cost")
    if isinstance(cost_value, Mapping):
        cost_value = cost_value.get("total")
    cost = float(cost_value) if isinstance(cost_value, int | float) else None
    return {
        "input": integer("input", "input_tokens"),
        "output": integer("output", "output_tokens"),
        "reasoning": integer("reasoning", "reasoning_tokens"),
        "total_tokens": integer("totalTokens", "total_tokens"),
        "cost": cost,
    }


def _admit_usage(value: Mapping[str, Any], *, thinking: str) -> dict[str, Any]:
    normalized = dict(value)
    reasoning = normalized.get("reasoning")
    if thinking == "off":
        if reasoning is None:
            normalized["reasoning"] = 0
        elif isinstance(reasoning, int) and reasoning > 0:
            raise AgentResultError(
                "Pi reported non-zero reasoning tokens while AgentSpec thinking=off"
            )
    return normalized


def _merge_usage(current: dict[str, Any] | None, addition: Mapping[str, Any]) -> dict[str, Any]:
    if current is None:
        return dict(addition)
    merged: dict[str, Any] = {}
    for key in ("input", "output", "reasoning", "total_tokens", "cost"):
        values = [value for value in (current.get(key), addition.get(key)) if value is not None]
        merged[key] = sum(values) if values else None
    return merged


def _append_bounded(target: bytearray, data: bytes, limit: int) -> bool:
    remaining = max(limit - len(target), 0)
    target.extend(data[:remaining])
    return len(data) <= remaining


def _bounded_marker(data: bytes, limit: int, marker: bytes) -> bytes:
    if limit == 0:
        return b""
    if len(marker) >= limit:
        return marker[:limit]
    return data[: limit - len(marker)] + marker


def _terminate_process_group(process: subprocess.Popen[Any]) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=0.2)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGKILL)
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=1.0)
    else:
        # The group can outlive its leader. Kill remaining descendants as well.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGKILL)


def _redact_bytes(value: bytes, secrets: tuple[str, ...]) -> bytes:
    redacted = value
    for secret in secrets:
        redacted = redacted.replace(secret.encode("utf-8"), b"***")
    return redacted


def _redact_text(value: str, secrets: tuple[str, ...]) -> str:
    redacted = value
    for secret in secrets:
        redacted = redacted.replace(secret, "***")
    return redacted


def _redact_payload(value: Any, secrets: tuple[str, ...]) -> Any:
    if isinstance(value, str):
        return _redact_text(value, secrets)
    if isinstance(value, Mapping):
        return {key: _redact_payload(child, secrets) for key, child in value.items()}
    if isinstance(value, list):
        return [_redact_payload(child, secrets) for child in value]
    return copy.deepcopy(value)


def _assert_workspace_secrets_absent(root: Path, secrets: tuple[str, ...]) -> None:
    needles = tuple(secret.encode("utf-8") for secret in secrets if secret)
    if not needles:
        return
    overlap_bytes = max(len(needle) for needle in needles) - 1
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if ".kigumi" in relative.parts or path.is_symlink() or not path.is_file():
            continue
        overlap = b""
        with path.open("rb") as handle:
            while chunk := handle.read(64 * 1024):
                searchable = overlap + chunk
                if any(needle in searchable for needle in needles):
                    raise AgentResultError("Pi workspace contains provider credential bytes")
                overlap = searchable[-overlap_bytes:] if overlap_bytes else b""
