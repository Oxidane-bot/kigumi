"""Opt-in pytest integration for prompt rendering, guards, and response tapes."""

from __future__ import annotations

import json
import os
import threading
import warnings
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .artifacts import atomic_write_json, sha
from .config import KigumiConfig, find_project_root, load_config
from .enforce import Finding, RawIOFinding, check_paths, check_raw_io_node_paths
from .prompt import render_template, slot_names
from .transport import Response, Transport


def skip_unless_env(*names: str) -> Any:
    """Return a pytest skip marker when any named environment variable is absent."""
    if not names:
        raise ValueError("skip_unless_env requires at least one environment variable name")
    missing = [name for name in names if not os.environ.get(name)]
    # pytest 是可选依赖；FakeTransport/CassetteTransport 的普通导入不应依赖它。
    import pytest

    reason = (
        f"Missing required environment variables: {', '.join(missing)}"
        if missing
        else "All required environment variables are present"
    )
    return pytest.mark.skipif(bool(missing), reason=reason)


class FakeTransport:
    """Return configured responses in order and retain every received request.

    ``responses`` accepts :class:`Response` instances or response text strings.
    A string is normalized to a minimal successful response for the requested
    model.  The transport raises instead of silently repeating its final answer.
    """

    def __init__(
        self,
        responses: Iterable[Response | str] | None = None,
        *,
        resolved_models: Mapping[str, str] | None = None,
    ) -> None:
        self._responses = iter(responses) if responses is not None else None
        self.resolved_models = dict(resolved_models or {})
        self.requests: list[tuple[list[dict[str, Any]], str, dict[str, Any]]] = []

    def resolve(self, model: str) -> str:
        """Resolve configured aliases without altering unconfigured model names."""
        return self.resolved_models.get(model, model)

    def complete(self, messages: list[dict[str, Any]], model: str, **params: Any) -> Response:
        """Record one request and return its next configured response."""
        self.requests.append((messages, model, params))
        if self._responses is None:
            return Response("answer", {"total_tokens": 4}, "stop", "private", model)
        try:
            response = next(self._responses)
        except StopIteration as error:
            raise RuntimeError("FakeTransport exhausted: no configured response remains") from error
        if isinstance(response, str):
            return Response(response, {}, "stop", model=model)
        return response


class ScriptedTransport:
    """Route offline responses by markers that occupy a complete request line.

    Responders run while the transport lock is held.  They may therefore keep
    mutable state such as repair-attempt counters without concurrent requests
    observing or updating that state out of order.
    """

    def __init__(
        self,
        routes: dict[str, str | Callable[[str, str], str]],
        *,
        aliases: Mapping[str, str] | None = None,
    ) -> None:
        self.routes = dict(routes)
        self.aliases = dict(aliases) if aliases is not None else None
        self.requests: list[tuple[list[dict[str, Any]], str]] = []
        self._lock = threading.Lock()

    def resolve(self, model: str) -> str:
        """Resolve aliases exactly, or expose the requested scripted model name."""
        if self.aliases is not None:
            return self.aliases[model]
        return f"scripted:{model}"

    def complete(self, messages: list[dict[str, Any]], model: str, **params: Any) -> Response:
        """Record one request and answer from the first complete-line marker match."""
        del params
        text = _scripted_message_text(messages)
        with self._lock:
            self.requests.append((messages, model))
            for marker, responder in self.routes.items():
                if marker in text.splitlines():
                    answer = responder(text, model) if callable(responder) else responder
                    return Response(answer, {"total_tokens": 1}, "stop", model=model)
        markers = ", ".join(repr(marker) for marker in self.routes)
        raise AssertionError(
            f"ScriptedTransport found no route; registered markers: {markers}; "
            f"request: {text[:120]!r}"
        )


def _scripted_message_text(messages: list[dict[str, Any]]) -> str:
    """Flatten the text-bearing request parts used by chat and multimodal callers."""
    parts: list[str] = []
    for message in messages:
        content = message.get("content", "")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            parts.extend(str(part.get("text", "")) for part in content if isinstance(part, dict))
    return "\n".join(parts)


class CassetteTransport:
    """Replay recorded responses in order or append calls through a real transport."""

    def __init__(self, path: Path, record_with: Transport | None = None) -> None:
        self.path = Path(path)
        self.record_with = record_with
        self._cursor = 0
        self._entries = self._read_entries()

    def resolve(self, model: str) -> str:
        """Keep caller-provided model names stable in tapes."""
        return model

    def complete(self, messages: list[dict[str, Any]], model: str, **params: Any) -> Response:
        """Replay the next entry or record a delegated live response."""
        if self.record_with is not None:
            response = self.record_with.complete(messages, model, **params)
            entry = _response_data(response)
            entry["request_sha"] = _request_sha(messages, model, params)
            self._entries.append(entry)
            atomic_write_json(self.path, self._entries)
            return response
        if self._cursor >= len(self._entries):
            raise RuntimeError(f"Cassette exhausted: {self.path}")
        entry = self._entries[self._cursor]
        self._cursor += 1
        # 只按序重放会把换了顺序的调用静默配错答案;录了请求指纹就必须核。
        recorded_sha = entry.get("request_sha")
        if recorded_sha is None:
            raise RuntimeError(
                f"Cassette entry {self._cursor - 1} lacks request_sha; "
                f"re-record the tape: {self.path}"
            )
        if recorded_sha != _request_sha(messages, model, params):
            raise RuntimeError(
                f"Cassette request mismatch at entry {self._cursor - 1}: {self.path}"
            )
        return Response(**{key: value for key, value in entry.items() if key != "request_sha"})

    def _read_entries(self) -> list[dict[str, Any]]:
        if not self.path.is_file():
            return []
        with self.path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, list) or not all(isinstance(entry, dict) for entry in payload):
            raise ValueError(f"Cassette must contain a JSON list of responses: {self.path}")
        return list(payload)


def pytest_configure(config: Any) -> None:
    """Register active behavior only for projects with ``[tool.kigumi]``."""
    root = find_project_root(Path.cwd())
    project_config = load_config(root) if root is not None else None
    if project_config is not None:
        import pytest

        config.pluginmanager.register(_KigumiPlugin(project_config, pytest), "kigumi-active-plugin")


@dataclass
class _KigumiPlugin:
    config: KigumiConfig
    pytest: Any

    def __post_init__(self) -> None:
        # pytest 只在插件类上收集 fixture；实例属性不会被 fixture manager 发现。
        # 写类属性意味着同进程内第二个插件实例会覆盖第一个的 fixture 绑定；
        # 本插件由 pytest_configure 每进程注册一次,这个约束成立。
        type(self).kigumi_cassette = self.pytest.fixture(self._kigumi_cassette)

    def pytest_configure(self, config: Any) -> None:
        config.addinivalue_line("markers", "live: test making real external requests")

    def _kigumi_cassette(self) -> Callable[[str], CassetteTransport]:
        """Build replay transports from the project's ``tests/cassettes`` directory."""
        cassette_dir = self.config.resolve("tests/cassettes")

        def factory(name: str) -> CassetteTransport:
            filename = name if name.endswith(".json") else f"{name}.json"
            return CassetteTransport(cassette_dir / filename)

        return factory

    def pytest_collection_modifyitems(
        self,
        session: Any,
        config: Any,
        items: list[Any],
    ) -> None:
        dry_render_item = _dry_render_item(self.pytest)
        guard_item = _guard_item(self.pytest)
        for prompt_path in sorted(self.config.prompts_path.rglob("*.md")):
            items.append(
                dry_render_item.from_parent(
                    session,
                    name=f"kigumi_dry_render[{prompt_path.name}]",
                    template_path=prompt_path,
                )
            )
        items.append(
            guard_item.from_parent(
                session,
                name="kigumi_guard",
                project_config=self.config,
            )
        )
        if _live_enabled():
            return
        for item in items:
            if "live" in item.keywords:
                item.add_marker(
                    self.pytest.mark.skip(reason="KIGUMI_LIVE=1 is required for live tests")
                )


def _dry_render_item(pytest: Any) -> type[Any]:
    class DryRenderItem(pytest.Item):
        def __init__(self, *, template_path: Path, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.template_path = template_path

        def runtest(self) -> None:
            text = self.template_path.read_text(encoding="utf-8")
            slots = {name: f"<{name}>" for name in slot_names(text)}
            rendered = render_template(text, slots)
            if "{{" in rendered:
                raise AssertionError(f"Unrendered template syntax: {self.template_path}")

        def reportinfo(self) -> tuple[Path, int, str]:
            return self.template_path, 0, self.name

    return DryRenderItem


def _guard_item(pytest: Any) -> type[Any]:
    class GuardItem(pytest.Item):
        def __init__(self, *, project_config: KigumiConfig, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.project_config = project_config

        def runtest(self) -> None:
            llm_findings = check_paths(self.project_config.source_paths)
            raw_io_findings = check_raw_io_node_paths(self.project_config.source_paths)
            llm_waived = [finding for finding in llm_findings if finding.waived]
            if llm_waived:
                warnings.warn(
                    _waiver_message(llm_waived, "raw-llm"), pytest.PytestWarning, stacklevel=1
                )
            raw_io_waived = [finding for finding in raw_io_findings if finding.waived]
            if raw_io_waived:
                warnings.warn(
                    _waiver_message(raw_io_waived, "raw-io"), pytest.PytestWarning, stacklevel=1
                )
            llm_violations = [finding for finding in llm_findings if not finding.waived]
            if llm_violations:
                locations = "\n".join(
                    f"{finding.path}:{finding.lineno}" for finding in llm_violations
                )
                raise AssertionError(f"Raw LLM calls inside loops:\n{locations}")
            raw_io_violations = [finding for finding in raw_io_findings if not finding.waived]
            if raw_io_violations:
                locations = "\n".join(
                    f"{finding.path}:{finding.lineno}" for finding in raw_io_violations
                )
                raise AssertionError(f"Raw file reads inside node functions:\n{locations}")

        def reportinfo(self) -> tuple[Path, int, str]:
            return self.project_config.project_root, 0, self.name

    return GuardItem


def _response_data(response: Response) -> dict[str, Any]:
    return {
        "text": response.text,
        "usage": response.usage,
        "finish_reason": response.finish_reason,
        "reasoning": response.reasoning,
        "model": response.model,
        "provider_response_id": response.provider_response_id,
        "model_observed": response.model_observed,
    }


def _request_sha(messages: list[dict[str, Any]], model: str, params: dict[str, Any]) -> str:
    return sha({"messages": messages, "model": model, "params": params})


def _live_enabled() -> bool:
    return os.environ.get("KIGUMI_LIVE") == "1"


def _waiver_message(findings: list[Finding] | list[RawIOFinding], guard: str) -> str:
    # Git-aware comparison of newly introduced waivers belongs to the P5 guard command.
    locations = ", ".join(
        f"{finding.path}:{finding.lineno} ({finding.waiver_reason})" for finding in findings
    )
    return f"kigumi {guard} waivers: {locations}"
