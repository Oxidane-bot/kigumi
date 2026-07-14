"""Deterministic evidence collection for pipeline-segmentation experiments."""

from __future__ import annotations

import statistics
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import artifacts
from .calling import Caller
from .evals import Judgment, Metric, evaluate


@dataclass(frozen=True)
class Variant:
    """One hypothesized structural cut of the same pipeline."""

    name: str
    hypothesis: str
    task: Callable[[dict[str, Any], Caller], Any]
    incumbent: bool = False


def bench(
    variants: Iterable[Variant],
    examples: Iterable[dict[str, Any]],
    metric: Metric,
    caller_factory: Callable[[int], Caller],
    *,
    seeds: Iterable[int] = range(5),
    pass_threshold: float | None = None,
    report_path: Path | None = None,
) -> dict[str, Any]:
    """Evaluate structural variants and return evidence without selecting a winner."""
    variant_items = list(variants)
    example_items = list(examples)
    seed_items = list(seeds)
    _validate_inputs(variant_items, example_items, seed_items)

    example_ids = [artifacts.sha(example) for example in example_items]
    if len(set(example_ids)) != len(example_ids):
        # 样例身份是内容哈希;重复样例会把 by_example 的证据行静默合并。
        raise ValueError("bench examples must not contain duplicate example contents")
    report_variants: list[dict[str, Any]] = []
    for variant in variant_items:
        scores: list[float] = []
        by_example = {example_id: [] for example_id in example_ids}
        judgments: list[dict[str, Any]] = []
        total_tokens = 0
        call_count = 0
        cost_observable = True

        for seed in seed_items:
            caller = caller_factory(seed)
            seed_judgments = evaluate(
                lambda example, variant=variant, caller=caller: variant.task(example, caller),
                example_items,
                metric,
            )
            for example_id, judgment in zip(example_ids, seed_judgments, strict=True):
                scores.append(judgment.score)
                by_example[example_id].append(judgment.score)
                judgments.append(_judgment_record(example_id, seed, judgment))

            calls_meta = getattr(caller, "calls", None)
            if isinstance(calls_meta, list):
                call_count += len(calls_meta)
                total_tokens += _total_tokens(calls_meta)
            else:
                cost_observable = False

        variant_report: dict[str, Any] = {
            "name": variant.name,
            "hypothesis": variant.hypothesis,
            "incumbent": variant.incumbent,
            "mean": statistics.mean(scores),
            "stdev": statistics.pstdev(scores),
            "total_tokens": total_tokens if cost_observable else None,
            "call_count": call_count if cost_observable else None,
            "by_example": by_example,
            "judgments": judgments,
        }
        if pass_threshold is not None:
            variant_report["pass_rate"] = sum(score >= pass_threshold for score in scores) / len(
                scores
            )
        report_variants.append(variant_report)

    report = {
        "examples": example_ids,
        "seeds": seed_items,
        "pass_threshold": pass_threshold,
        "variants": report_variants,
    }
    if report_path is not None:
        artifacts.atomic_write_json(report_path, report)
    return report


def _validate_inputs(
    variants: list[Variant], examples: list[dict[str, Any]], seeds: list[int]
) -> None:
    if not variants:
        raise ValueError("bench variants must not be empty")
    names = [variant.name for variant in variants]
    if len(set(names)) != len(names):
        raise ValueError("bench variant names must be unique; duplicate name found")
    for variant in variants:
        if not isinstance(variant.hypothesis, str) or not variant.hypothesis.strip():
            raise ValueError("假设是变体的准入证,没有假设的变体是结构层面的乱调参")
    if sum(variant.incumbent for variant in variants) != 1:
        raise ValueError('必须恰有一个 incumbent；没有现状对照回答不了"比现状好吗"')
    if not examples:
        raise ValueError("bench examples must not be empty")
    if not seeds:
        raise ValueError("bench seeds must not be empty")
    if len(set(seeds)) != len(seeds):
        raise ValueError("bench seeds must not contain duplicates")


def _judgment_record(example_id: str, seed: int, judgment: Judgment) -> dict[str, Any]:
    return {
        "example_id": example_id,
        "seed": seed,
        "score": judgment.score,
        "feedback": judgment.feedback,
        "tags": list(judgment.tags),
    }


def _total_tokens(calls_meta: list[Any]) -> int:
    total = 0
    for metadata in calls_meta:
        if not isinstance(metadata, dict):
            continue
        usage = metadata.get("usage")
        if not isinstance(usage, dict) or usage.get("total_tokens") is None:
            continue
        total += int(usage["total_tokens"])
    return total
