from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from kigumi.prompt import WORDING_REPAIR_ECHO, WORDING_REPAIR_ROUND, WORDING_REPAIR_STUCK
from kigumi.repair import RepairExhausted, call_validated, repair_loop


class FakeCaller:
    def __init__(self, outputs: list[str]) -> None:
        self.outputs = list(outputs)
        self.calls: list[dict[str, Any]] = []

    def call(
        self,
        messages: list[dict[str, Any]] | str,
        model: str = "default",
        **params: Any,
    ) -> str:
        self.calls.append(
            {
                "messages": deepcopy(messages),
                "model": model,
                "params": params,
            }
        )
        return self.outputs.pop(0)


class NamedModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str


def _validate_good(raw: str) -> str:
    if raw != "good":
        raise ValueError(f"invalid: {raw}")
    return "validated"


def _last_content(call: dict[str, Any]) -> str:
    messages = call["messages"]
    assert isinstance(messages, list)
    return messages[-1]["content"]


def test_repair_round_numbers_change_prompts() -> None:
    """教训 judge_15: 每轮纠正 prompt 必须不同，才能避开同一缓存键。"""
    caller = FakeCaller(["bad-one", "bad-two", "good"])

    result = repair_loop(caller, "start", _validate_good)

    assert result == "validated"
    assert WORDING_REPAIR_ROUND.format(round=1) in _last_content(caller.calls[1])
    assert WORDING_REPAIR_ROUND.format(round=2) in _last_content(caller.calls[2])
    assert _last_content(caller.calls[1]).encode() != _last_content(caller.calls[2]).encode()


def test_stuck_response_adds_pressure_to_next_round() -> None:
    """教训 stuck_output: 原样重交必须在下一轮得到明确加压。"""
    caller = FakeCaller(["bad", "bad", "good"])

    assert repair_loop(caller, "start", _validate_good) == "validated"
    assert WORDING_REPAIR_STUCK in _last_content(caller.calls[2])


def test_rebuild_echoes_full_previous_output() -> None:
    """教训 draft_prose_14: rebuild 回显默认保留完整输出，不能隐式截断。"""
    long_raw = "x" * 20_000
    caller = FakeCaller([long_raw, "good"])

    assert repair_loop(caller, "start", _validate_good) == "validated"
    assert long_raw in _last_content(caller.calls[1])
    assert WORDING_REPAIR_ECHO in _last_content(caller.calls[1])


def test_continue_mode_does_not_echo_previous_output() -> None:
    """教训 continue_no_echo: 历史里已有上轮输出，纠正消息再回显一遍是 token 翻倍。"""

    def validate(raw: str) -> str:
        if raw != "good":
            raise ValueError("format invalid")
        return "validated"

    caller = FakeCaller(["bad-output", "good"])

    assert repair_loop(caller, "start", validate, mode="continue") == "validated"
    messages = caller.calls[1]["messages"]
    correction = _last_content(caller.calls[1])
    assert WORDING_REPAIR_ECHO not in correction
    assert "bad-output" not in correction
    assert [m["content"] for m in messages].count("bad-output") == 1


def test_call_validated_strips_extra_fields_without_repair() -> None:
    """教训 workbench_10: 纯 extra_forbidden 可机械剥除，不消耗调用轮次。"""
    caller = FakeCaller(['{"name":"木组","hallucinated":true}'])

    result = call_validated(caller, "返回名称", NamedModel, include_format_section=False)

    assert result == NamedModel(name="木组")
    assert len(caller.calls) == 1


def test_call_validated_accepts_control_characters() -> None:
    """教训 control_character_shell: JSON 控制字符容错属于确定性解析，不应重试。"""
    caller = FakeCaller(['{"name":"第一行\n第二行"}'])

    result = call_validated(caller, "返回名称", NamedModel, include_format_section=False)

    assert result.name == "第一行\n第二行"
    assert len(caller.calls) == 1


def test_call_validated_strips_json_fences() -> None:
    """教训 fenced_json: 模型附加围栏时应机械剥壳，不消耗修复轮。"""
    caller = FakeCaller(['```json\n{"name":"木组"}\n```'])

    result = call_validated(caller, "返回名称", NamedModel, include_format_section=False)

    assert result.name == "木组"
    assert len(caller.calls) == 1


def test_repair_exhausted_sends_complete_record_to_sink() -> None:
    """教训 failure_artifact: 耗尽前必须把每轮原文和错误交给调用方归档。"""
    records: list[dict[str, Any]] = []
    caller = FakeCaller(["bad-one", "bad-two", "bad-three"])

    with pytest.raises(RepairExhausted):
        repair_loop(caller, "start", _validate_good, sink=records.append)

    assert len(records) == 1
    assert records[0]["rounds"] == 3
    assert records[0]["raws"] == ["bad-one", "bad-two", "bad-three"]
    assert records[0]["errors"] == [
        "invalid: bad-one",
        "invalid: bad-two",
        "invalid: bad-three",
    ]
    assert records[0]["mode"] == "rebuild"
    assert records[0]["model"] == "default"


def test_on_event_reports_each_failed_round() -> None:
    """教训 repair_events: 每个失败轮次都要产生可累积的结构化事件。"""
    events: list[dict[str, Any]] = []
    caller = FakeCaller(["bad-one", "bad-two", "good"])

    assert repair_loop(caller, "start", _validate_good, on_event=events.append) == "validated"
    assert events == [
        {"round": 1, "error": "invalid: bad-one", "stuck": False, "raw_chars": 7},
        {"round": 2, "error": "invalid: bad-two", "stuck": False, "raw_chars": 7},
    ]


def test_callable_reminder_receives_accumulated_error_context() -> None:
    """教训 lint_fix: 调用方可按跨轮错误历史生成下一轮定制指令。"""
    seen_errors: list[str] = []
    caller = FakeCaller(["bad-one", "bad-two", "good"])

    def reminder(error: ValueError, round: int) -> str:
        seen_errors.append(f"{round}:{error}")
        return "累计错误=" + "|".join(seen_errors)

    assert repair_loop(caller, "start", _validate_good, reminder=reminder) == "validated"
    assert "累计错误=1:invalid: bad-one" in _last_content(caller.calls[1])
    assert "累计错误=1:invalid: bad-one|2:invalid: bad-two" in _last_content(caller.calls[2])


def test_continue_mode_preserves_assistant_history() -> None:
    """教训 continue_history: continue 把失败输出留在对话历史，便于定点纠正。"""
    caller = FakeCaller(["bad", "good"])

    assert repair_loop(caller, "start", _validate_good, mode="continue") == "validated"
    messages = caller.calls[1]["messages"]
    assert messages == [
        {"role": "user", "content": "start"},
        {"role": "assistant", "content": "bad"},
        {"role": "user", "content": _last_content(caller.calls[1])},
    ]


def test_success_returns_validate_result() -> None:
    """教训 validator_result: 修复环返回校验器的值，而非未经处理的模型文本。"""
    caller = FakeCaller(["7"])

    assert repair_loop(caller, "start", lambda raw: int(raw)) == 7


def test_call_validated_appends_schema_and_defaults_json_mode() -> None:
    """教训 schema_single_source: 格式段和 json_mode 必须由校验模型统一决定。"""
    caller = FakeCaller(['{"name":"木组"}'])

    result = call_validated(caller, "返回名称", NamedModel)

    assert result.name == "木组"
    assert "## 输出格式" in _last_content(caller.calls[0])
    assert caller.calls[0]["params"]["json_mode"] is True


def test_extra_check_failure_enters_repair_loop() -> None:
    """教训 business_gate: 业务门禁失败与 schema 错误走同一有界修复路径。"""
    caller = FakeCaller(['{"name":"坏"}', '{"name":"好"}'])

    def extra_check(instance: NamedModel) -> None:
        if instance.name == "坏":
            raise ValueError("名称不合格")

    result = call_validated(caller, "返回名称", NamedModel, extra_check=extra_check)

    assert result.name == "好"
    assert "名称不合格" in _last_content(caller.calls[1])
