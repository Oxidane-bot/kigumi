"""评估原语。

本模块刻意不提供 EM、F1 或文本相似度指标。它们会把创作类任务的优化目标退化成
“复写测评集”；合规轴应由调用方编写普通 Python 函数，品质轴使用本模块的评委工厂。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .calling import Caller
from .prompt import inject, section
from .repair import call_validated

JUDGE_WORDING_DEFAULT = """请独立依据评审准则打分。评语应指出问题类型与改进方向，
不要逐条罗列错误实例。"""
PAIRWISE_WORDING_DEFAULT = """请依据评审准则判断输出的水准是否达到或超过参考文本。
两段文字风格与表达完全不同是正常的，只比水准，不比相似。评语应指出问题类型与改进方向，
不要逐条罗列错误实例。"""
PAIRWISE_SCORES = {"better": 1.0, "comparable": 0.8, "worse": 0.3}


@dataclass(frozen=True)
class Judgment:
    """一个样例的主分、可供反思的评语与错误标签。"""

    score: float
    feedback: str
    tags: tuple[str, ...] = ()
    subscores: dict[str, float] | None = None

    def __post_init__(self) -> None:
        if not 0.0 <= self.score <= 1.0:
            raise ValueError("Judgment score must be between 0.0 and 1.0")


Metric = Callable[[dict[str, Any], Any], Judgment]


class _ScoreJudgment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    score: float = Field(ge=0.0, le=1.0)
    feedback: str
    tags: list[str]


class _PairwiseJudgment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    verdict: Literal["better", "comparable", "worse"]
    feedback: str
    tags: list[str]


def gated_metric(gate: Metric, quality: Metric) -> Metric:
    """组合合规硬闸与品质评委，未通过硬闸时不调用品质评委。"""

    def metric(example: dict[str, Any], output: Any) -> Judgment:
        gate_judgment = gate(example, output)
        if gate_judgment.score < 1.0:
            return gate_judgment
        quality_judgment = quality(example, output)
        return Judgment(
            score=quality_judgment.score,
            feedback=quality_judgment.feedback,
            tags=(*gate_judgment.tags, *quality_judgment.tags),
            subscores=quality_judgment.subscores,
        )

    return metric


def llm_judge(
    caller: Caller,
    *,
    rubric: str,
    model: str = "default",
    wording: str | None = None,
    max_repairs: int = 2,
) -> Metric:
    """建立一个仅按 ``rubric`` 独立评分的 LLM 评委。"""
    instructions = JUDGE_WORDING_DEFAULT if wording is None else wording

    def metric(example: dict[str, Any], output: Any) -> Judgment:
        prompt = "".join(
            [
                section("评审准则", rubric),
                section("待评样例", inject(example)),
                section("待评输出", inject(output)),
                section("评委指示", instructions),
            ]
        )
        result = call_validated(
            caller,
            prompt,
            _ScoreJudgment,
            model=model,
            max_repairs=max_repairs,
        )
        return Judgment(result.score, result.feedback, tuple(result.tags))

    return metric


def pairwise_judge(
    caller: Caller,
    *,
    rubric: str,
    reference_key: str,
    model: str = "default",
    wording: str | None = None,
    max_repairs: int = 2,
) -> Metric:
    """建立以参考文本为及格线、而非相似度目标的 LLM 评委。"""
    instructions = PAIRWISE_WORDING_DEFAULT if wording is None else wording

    def metric(example: dict[str, Any], output: Any) -> Judgment:
        if reference_key not in example:
            raise ValueError(f"Missing reference key: {reference_key}")
        prompt = "".join(
            [
                section("评审准则", rubric),
                section("待评样例", inject(example)),
                section("参考文本（及格线）", inject(example[reference_key])),
                section("待评输出", inject(output)),
                section("评委指示", instructions),
            ]
        )
        result = call_validated(
            caller,
            prompt,
            _PairwiseJudgment,
            model=model,
            max_repairs=max_repairs,
        )
        return Judgment(PAIRWISE_SCORES[result.verdict], result.feedback, tuple(result.tags))

    return metric


def evaluate(
    task: Callable[[dict[str, Any]], Any], examples: list[dict[str, Any]], metric: Metric
) -> list[Judgment]:
    """串行执行任务与评估；单一样例任务或指标失败不会中断整批。"""
    judgments: list[Judgment] = []
    for example in examples:
        try:
            output = task(example)
        except Exception as error:  # 一个坏样例必须可见地记为失败，不能吞掉整轮。
            judgments.append(Judgment(0.0, f"{type(error).__name__}: {error}", ("task_error",)))
            continue
        try:
            judgments.append(metric(example, output))
        except Exception as error:  # 指标错误也必须留下失败记录，保住其他样例的评估结果。
            judgments.append(Judgment(0.0, f"{type(error).__name__}: {error}", ("metric_error",)))
    return judgments
