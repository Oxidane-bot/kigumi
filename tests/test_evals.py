from __future__ import annotations

from typing import Any

import pytest

from kigumi.evals import Judgment, evaluate, gated_metric, llm_judge, pairwise_judge


class FakeCaller:
    def __init__(self, outputs: list[str]) -> None:
        self.outputs = list(outputs)
        self.calls: list[dict[str, Any]] = []

    def call(self, messages: str, model: str = "default", **params: Any) -> str:
        self.calls.append({"messages": messages, "model": model, "params": params})
        return self.outputs.pop(0)


def test_gate_short_circuit() -> None:
    """教训 gate_short_circuit: 合规闸未过时，品质评委不应被调用。"""
    quality_calls = 0

    def quality(example: dict[str, Any], output: Any) -> Judgment:
        nonlocal quality_calls
        quality_calls += 1
        return Judgment(1.0, "不应执行")

    metric = gated_metric(lambda example, output: Judgment(0.0, "不合规", ("gate",)), quality)

    assert metric({}, "x") == Judgment(0.0, "不合规", ("gate",))
    assert quality_calls == 0


def test_judge_via_repair() -> None:
    """教训 judge_via_repair: 围栏 JSON 必须经既有 repair 校验链解析。"""
    caller = FakeCaller(
        ['```json\n{"score": 0.7, "feedback": "补足结构", "tags": ["structure"]}\n```']
    )

    judgment = llm_judge(caller, rubric="完整性")({"topic": "松"}, "短文")

    assert judgment == Judgment(0.7, "补足结构", ("structure",))
    assert caller.calls[0]["params"]["json_mode"] is True
    with pytest.raises(ValueError):
        Judgment(1.1, "越界")


def test_judge_uses_max_repairs_name_and_forwards_it() -> None:
    """教训 repair_naming: 评委与修复环必须使用同一个重试参数名。"""
    caller = FakeCaller(
        [
            '{"score": 1.2, "feedback": "bad", "tags": []}',
            '{"score": 0.8, "feedback": "ok", "tags": []}',
        ]
    )

    judgment = llm_judge(caller, rubric="完整性", max_repairs=1)({}, "短文")

    assert judgment.score == 0.8
    assert len(caller.calls) == 2


def test_reference_as_bar() -> None:
    """教训 reference_as_bar: 参考是及格线，不是抄写目标。"""
    caller = FakeCaller(['{"verdict":"better","feedback":"水准更高","tags":["quality"]}'])
    metric = pairwise_judge(caller, rubric="文学质量", reference_key="reference")

    # 输出和参考逐字不同；相似度不在裁决路径上。
    assert metric({"reference": "春风拂面，旧句在此。"}, "海潮吞没了黎明，新句至此。").score == 1.0
    with pytest.raises(ValueError, match="reference"):
        metric({}, "任意输出")


def test_broken_example() -> None:
    """教训 broken_example: 一个任务错误必须可见地记零分，不能废掉整批。"""

    def task(example: dict[str, Any]) -> str:
        if example["broken"]:
            raise RuntimeError("坏输入")
        return "ok"

    judgments = evaluate(
        task, [{"broken": True}, {"broken": False}], lambda example, output: Judgment(1.0, "好")
    )

    assert judgments[0].score == 0.0
    assert "RuntimeError: 坏输入" in judgments[0].feedback
    assert judgments[0].tags == ("task_error",)
    assert judgments[1].score == 1.0


def test_metric_error_preserves_other_examples() -> None:
    """教训 metric_error_isolation: 单项指标失败不能丢掉整批其他裁决。"""

    def metric(example: dict[str, Any], output: str) -> Judgment:
        if example["id"] == 2:
            raise RuntimeError("评估器异常")
        return Judgment(1.0, output)

    judgments = evaluate(
        lambda example: str(example["id"]), [{"id": 1}, {"id": 2}, {"id": 3}], metric
    )

    assert [judgment.score for judgment in judgments] == [1.0, 0.0, 1.0]
    assert "RuntimeError: 评估器异常" in judgments[1].feedback
    assert judgments[1].tags == ("metric_error",)
