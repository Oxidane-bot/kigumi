"""Bounded validation repair loops for structured LLM responses."""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import nullcontext
from copy import deepcopy
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from .calling import Caller, prompt_resolution_boundary
from .prompt import (
    WORDING_REPAIR_ECHO,
    WORDING_REPAIR_PREAMBLE,
    WORDING_REPAIR_ROUND,
    WORDING_REPAIR_STUCK,
    PromptResolution,
    ResolvedPrompt,
    schema_format_section,
)

Validated = TypeVar("Validated")
Model = TypeVar("Model", bound=BaseModel)


class RepairExhausted(RuntimeError):
    """Raised after every bounded repair attempt fails validation."""


def repair_loop(
    caller: Caller,
    messages: list[dict[str, Any]] | str,
    validate: Callable[[str], Validated],
    *,
    model: str = "default",
    mode: str = "rebuild",
    max_repairs: int = 2,
    reminder: str | Callable[[ValueError, int], str] | None = None,
    sink: Callable[[dict[str, Any]], None] | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
    _base_resolution: PromptResolution | None = None,
    **params: Any,
) -> Validated:
    """Call, validate, and repair a response within a fixed attempt budget.

    ``continue`` is experimental: it retains invalid assistant turns in history,
    which can anchor the model on bad output; some OpenAI-compatible proxies also
    handle assistant messages combined with ``response_format`` unreliably.
    """
    if mode not in {"rebuild", "continue"}:
        raise ValueError("mode must be 'rebuild' or 'continue'")
    if max_repairs < 0:
        raise ValueError("max_repairs must be non-negative")

    base_resolution = (
        messages.resolution if isinstance(messages, ResolvedPrompt) else _base_resolution
    )
    base_messages = _normalize_messages(messages)
    attempt_messages = deepcopy(base_messages)
    rounds: list[dict[str, Any]] = []
    previous_raw: str | None = None

    for attempt in range(max_repairs + 1):
        lineage = (
            prompt_resolution_boundary(
                base_resolution,
                phase="primary" if attempt == 0 else "repair",
                repair_round=attempt,
            )
            if base_resolution is not None
            else nullcontext()
        )
        with lineage:
            raw = caller.call(attempt_messages, model=model, **params)
        try:
            return validate(raw)
        except ValueError as error:
            error_text = str(error)
            repair_round = attempt + 1
            stuck = previous_raw == raw if previous_raw is not None else False
            round_record = {
                "round": repair_round,
                "raw": raw,
                "error": error_text,
                "stuck": stuck,
            }
            rounds.append(round_record)
            event = {
                "round": repair_round,
                "error": error_text,
                "stuck": stuck,
                "raw_chars": len(raw),
            }
            if on_event is not None:
                on_event(event)

            if attempt == max_repairs:
                record = {
                    "rounds": len(rounds),
                    "raws": [item["raw"] for item in rounds],
                    "errors": [item["error"] for item in rounds],
                    "attempts": rounds,
                    "mode": mode,
                    "model": model,
                }
                if sink is not None:
                    sink(record)
                raise RepairExhausted(
                    f"Validation failed after {max_repairs} repair attempts for model {model!r}: "
                    f"{error_text}"
                ) from error

            correction = _correction_message(
                error=error,
                raw=raw,
                round=repair_round,
                stuck=stuck,
                reminder=reminder,
                # continue 模式的历史里已有上轮输出,回显只属于 rebuild。
                echo=mode == "rebuild",
            )
            previous_raw = raw
            if mode == "rebuild":
                attempt_messages = [
                    *deepcopy(base_messages),
                    {"role": "user", "content": correction},
                ]
            else:
                attempt_messages = [
                    *attempt_messages,
                    {"role": "assistant", "content": raw},
                    {"role": "user", "content": correction},
                ]

    raise AssertionError("bounded repair loop must return or raise")


def call_validated(
    caller: Caller,
    prompt: str,
    model_cls: type[Model],
    *,
    extra_check: Callable[[Model], None] | None = None,
    include_format_section: bool = True,
    model: str = "default",
    mode: str = "rebuild",
    max_repairs: int = 2,
    reminder: str | Callable[[ValueError, int], str] | None = None,
    sink: Callable[[dict[str, Any]], None] | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
    **params: Any,
) -> Model:
    """Call for a Pydantic model, applying deterministic JSON normalization first."""
    base_resolution = prompt.resolution if isinstance(prompt, ResolvedPrompt) else None
    completed_prompt = prompt
    if include_format_section:
        completed_prompt = f"{prompt}\n\n{schema_format_section(model_cls)}"
    params.setdefault("json_mode", True)

    def validate(raw: str) -> Model:
        parsed = json.loads(_strip_json_fence(raw), strict=False)
        try:
            instance = model_cls.model_validate(parsed)
        except ValidationError as error:
            stripped = _strip_extra_fields(parsed, error)
            if stripped is None:
                raise
            instance = model_cls.model_validate(stripped)
        if extra_check is not None:
            extra_check(instance)
        return instance

    return repair_loop(
        caller,
        completed_prompt,
        validate,
        model=model,
        mode=mode,
        max_repairs=max_repairs,
        reminder=reminder,
        sink=sink,
        on_event=on_event,
        _base_resolution=base_resolution,
        **params,
    )


def _normalize_messages(messages: list[dict[str, Any]] | str) -> list[dict[str, Any]]:
    if isinstance(messages, str):
        return [{"role": "user", "content": messages}]
    return deepcopy(messages)


def _correction_message(
    *,
    error: ValueError,
    raw: str,
    round: int,
    stuck: bool,
    reminder: str | Callable[[ValueError, int], str] | None,
    echo: bool,
) -> str:
    parts = [
        WORDING_REPAIR_PREAMBLE,
        str(error),
        WORDING_REPAIR_ROUND.format(round=round),
    ]
    if stuck:
        parts.append(WORDING_REPAIR_STUCK)
    if reminder is not None:
        parts.append(reminder(error, round) if callable(reminder) else reminder)
    if echo:
        parts.append(f"{WORDING_REPAIR_ECHO}\n{raw}")
    return "\n\n".join(parts)


def _strip_json_fence(raw: str) -> str:
    stripped = raw.strip()
    lines = stripped.splitlines()
    if (
        len(lines) >= 2
        and lines[0].strip().lower() in {"```", "```json"}
        and lines[-1].strip() == "```"
    ):
        return "\n".join(lines[1:-1])
    return raw


def _strip_extra_fields(data: Any, error: ValidationError) -> Any | None:
    errors = error.errors()
    if not errors or any(item["type"] != "extra_forbidden" for item in errors):
        return None
    stripped = deepcopy(data)
    try:
        for item in errors:
            location = item["loc"]
            node = stripped
            for part in location[:-1]:
                node = node[part]
            if not isinstance(node, dict):
                return None
            node.pop(location[-1])
    except (IndexError, KeyError, TypeError):
        return None
    return stripped
