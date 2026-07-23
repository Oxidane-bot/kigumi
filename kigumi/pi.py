"""Native Pi RPC adapter with fail-closed JSONL and process-group handling."""

from __future__ import annotations

import contextlib
import copy
import json
import os
import queue
import signal
import subprocess
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
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
        }

    def capabilities(self) -> AgentCapabilities:
        return AgentCapabilities(filesystem=True, terminal=False)

    def run(self, request: AgentRequest, context: AgentRunContext) -> AgentRunResult:
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
        chunks: queue.Queue[bytes | None] | None = None
        stdout_thread: threading.Thread | None = None
        stderr_thread: threading.Thread | None = None
        usage: dict[str, Any] | None = None
        stop_reason: str | None = None
        turns = 0
        tool_calls = 0
        hook_evidence: list[Any] = []
        try:
            version_stdout, version_stderr = self._probe_version(environment, context.deadline)
            if not _append_bounded(stderr, version_stderr, request.spec.limits.stderr_max_bytes):
                stderr_truncated = True
            actual_version = version_stdout.decode("utf-8", errors="strict").strip()
            if actual_version != self.expected_version:
                raise AgentResultError(
                    f"Pi version mismatch: expected {self.expected_version!r}, "
                    f"received {actual_version!r}"
                )

            pi_home = context.workspace / ".kigumi" / "pi-home"
            pi_home.mkdir()
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
            process = subprocess.Popen(
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
                while chunk := os.read(process.stdout.fileno(), 65536):
                    chunks.put(chunk)
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
                    raise AgentResultError(
                        f"Pi agent timed out after {request.spec.limits.timeout_seconds} seconds"
                    )
                try:
                    chunk = chunks.get(timeout=min(remaining, 0.1))
                except queue.Empty:
                    if process.poll() is not None and chunks.empty():
                        raise AgentResultError(
                            f"Pi exited before completion with status {process.returncode}"
                        ) from None
                    continue
                if chunk is None:
                    if buffer:
                        raise AgentResultError("Pi RPC stdout ended with a partial JSONL record")
                    if not settled:
                        code = process.poll()
                        raise AgentResultError(f"Pi exited before completion with status {code}")
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
                    context.emit_event(_redact_payload(event, secrets))
                    event_type = event.get("type")
                    if event_type == "response" and event.get("id") == prompt_id:
                        if event.get("command") != "prompt" or event.get("success") is not True:
                            raise AgentResultError("Pi rejected the prompt RPC command")
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
                        raise AgentResultError("Pi requested an undeclared UI interaction")
                    elif event_type == "extension_error":
                        raise AgentResultError("Pi Extension reported an error")
                    elif event_type == "turn_start":
                        turns += 1
                        if turns > request.spec.limits.max_turns:
                            raise AgentResultError("Pi exceeded max_turns")
                    elif event_type == "tool_execution_start":
                        tool_calls += 1
                        tool_name = event.get("toolName")
                        if tool_calls > request.spec.limits.max_tool_calls:
                            raise AgentResultError("Pi exceeded max_tool_calls")
                        if tool_name in {"bash", "shell", "terminal"}:
                            raise AgentResultError("Pi attempted a reserved generic shell tool")
                    elif (
                        event_type == "tool_execution_end"
                        and event.get("toolName") == "submit_result"
                    ):
                        if event.get("isError") is True or completion is not None:
                            raise AgentResultError("Pi submit_result failed or was submitted twice")
                        details = event.get("result")
                        details = details.get("details") if isinstance(details, Mapping) else None
                        if not isinstance(details, Mapping) or not isinstance(
                            details.get("completion"), Mapping
                        ):
                            raise AgentResultError("Pi submit_result did not return a completion")
                        completion_value = _redact_payload(details["completion"], secrets)
                        completion = AgentCompletion.from_mapping(completion_value)
                        evidence_value = details.get("evidence", [])
                        if not isinstance(evidence_value, list) or not all(
                            isinstance(item, Mapping) for item in evidence_value
                        ):
                            raise AgentResultError("Pi Hook evidence must be a list of objects")
                        hook_evidence = _redact_payload(evidence_value, secrets)
                    elif event_type == "message_end":
                        message = event.get("message")
                        if isinstance(message, Mapping):
                            normalized = _normalize_usage(message.get("usage"))
                            if normalized is not None:
                                usage = _merge_usage(usage, normalized)
                            reason = message.get("stopReason", message.get("stop_reason"))
                            if isinstance(reason, str):
                                stop_reason = reason
                    elif event_type == "tool_execution_end":
                        tool_result = event.get("result")
                        if isinstance(tool_result, Mapping):
                            normalized = _normalize_usage(tool_result.get("usage"))
                            if normalized is not None:
                                usage = _merge_usage(usage, normalized)
                    elif event_type == "compaction_end":
                        compaction = event.get("result")
                        if isinstance(compaction, Mapping):
                            normalized = _normalize_usage(compaction.get("usage"))
                            if normalized is not None:
                                usage = _merge_usage(usage, normalized)
                    elif event_type == "agent_settled":
                        settled = True
            if not prompt_accepted:
                raise AgentResultError("Pi never acknowledged the prompt RPC command")
            if completion is None:
                raise AgentResultError("Pi settled without submit_result completion")
            if hook_evidence:
                context.emit_event({"type": "hook_evidence", "items": hook_evidence})
            try:
                return_code = process.wait(timeout=0.05)
            except subprocess.TimeoutExpired:
                return_code = None
            if return_code not in {None, 0}:
                raise AgentResultError(f"Pi exited with non-zero status {return_code}")
            return AgentRunResult(
                completion=completion,
                usage=usage,
                metadata={
                    "pi_version": actual_version,
                    "stop_reason": stop_reason,
                    "turns": turns,
                    "tool_calls": tool_calls,
                    "hook_evidence": hook_evidence,
                },
            )
        except AgentResultError as error:
            raise AgentResultError(_redact_text(str(error), secrets)) from None
        except Exception as error:
            message = _redact_text(str(error), secrets)
            raise AgentResultError(f"Pi RPC failed: {type(error).__name__}: {message}") from None
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
            raise AgentResultError("Pi version probe timed out")
        process = subprocess.Popen(
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
                raise AgentResultError("Pi version probe timed out") from None
            if process.returncode != 0:
                raise AgentResultError(
                    f"Pi version probe exited with non-zero status {process.returncode}"
                )
            return stdout, stderr
        finally:
            _terminate_process_group(process)


def _bridge_path() -> Path:
    return Path(str(resources.files("kigumi").joinpath("_pi_bridge.ts"))).resolve()


def _bridge_digest() -> str:
    import hashlib

    return hashlib.sha256(_bridge_path().read_bytes()).hexdigest()


def _send_json(process: subprocess.Popen[bytes], value: Mapping[str, Any]) -> None:
    if process.stdin is None:
        raise AgentResultError("Pi RPC stdin is unavailable")
    payload = json.dumps(
        dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    process.stdin.write(payload + b"\n")
    process.stdin.flush()


def _decode_event(raw_line: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw_line.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AgentResultError(f"Malformed Pi RPC JSONL: {error}") from error
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise AgentResultError("Pi RPC records must be objects with a string type")
    return value


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
        "total_tokens": integer("totalTokens", "total_tokens"),
        "cost": cost,
    }


def _merge_usage(current: dict[str, Any] | None, addition: Mapping[str, Any]) -> dict[str, Any]:
    if current is None:
        return dict(addition)
    merged: dict[str, Any] = {}
    for key in ("input", "output", "total_tokens", "cost"):
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
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=0.2)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=1.0)
    else:
        # The group can outlive its leader. Kill remaining descendants as well.
        with contextlib.suppress(ProcessLookupError):
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
