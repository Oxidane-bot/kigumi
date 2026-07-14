"""带训练/验证隔离机制闸门的反思式提示词进化。

默认反思模板只是起点；防穷举与防过拟合不依赖模板措辞，而依赖训练材料隔离、验证
拒收、泄漏检查与 Pareto 剪除等代码机制。
"""

from __future__ import annotations

import json
import random
import re
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .artifacts import atomic_write_json, canonical_json, sha
from .calling import Caller
from .evals import Judgment, Metric
from .prompt import inject, render_template, slot_names

REFLECTION_TEMPLATE_DEFAULT = """你正在改进一份任务指令。请只做可泛化的原则性修正，
不要针对个别案例打补丁；任务含创作成分时只约束品质底线，不穷举风格清单。

当前模板：
{{current_template}}

训练侧归并反馈：
{{merged_feedback}}

新模板不得超过以下字数预算：
{{max_chars}}

只在一个 ``` 围栏内写出新指令全文，不要在围栏外附加说明。"""

_REFLECTION_SLOTS = {"current_template", "merged_feedback", "max_chars"}
_FENCE_MARKER = re.compile(r"(?m)^```[^\n]*$")
_STATE_SCHEMA_VERSION = 2


@dataclass(frozen=True)
class Candidate:
    """一个候选模板及其两侧独立的评估记录。"""

    text: str
    parent: int | None
    train_scores: dict[str, float]
    val_scores: dict[str, float]
    round: int


@dataclass(frozen=True)
class EvolveResult:
    """一次进化运行的可审计结果。"""

    best: str
    candidates: list[Candidate]
    metric_calls: int
    rounds_run: int
    generalization_gap: float


def evolve_prompt(
    template: str,
    train_examples: list[dict[str, Any]],
    val_examples: list[dict[str, Any]],
    task: Callable[[str, dict[str, Any]], Any],
    metric: Metric,
    caller: Caller,
    *,
    rounds: int = 8,
    minibatch: int = 3,
    max_metric_calls: int | None = None,
    max_chars: int | None = None,
    leak_run_chars: int = 12,
    reflection_template: str | None = None,
    reflection_model: str = "default",
    state_path: Path | None = None,
    seed: int = 0,
) -> EvolveResult:
    """进化 ``template``，但只让训练侧材料进入反思调用。

    验证侧只负责候选去留、前沿与胜出：它的样例内容和评语没有通往反思 LM 的参数路径。
    函数只返回胜出文本，绝不写入 prompts 目录；生产模板变更仍须由调用方人工决定。

    ``max_metric_calls`` 是跨 ``state_path`` 续跑的累计总上限。恢复时必须传入大于
    已消耗次数的总额；state 的 ``round`` 只记录已完成轮数，未完成轮会从反思阶段重进。
    """
    if not train_examples or not val_examples:
        raise ValueError("train_examples and val_examples must both be non-empty")
    if rounds < 0:
        raise ValueError("rounds must be non-negative")
    if minibatch <= 0:
        raise ValueError("minibatch must be positive")
    if leak_run_chars <= 0:
        raise ValueError("leak_run_chars must be positive")
    if max_metric_calls is not None and max_metric_calls < 0:
        raise ValueError("max_metric_calls must be non-negative")

    selected_template = (
        REFLECTION_TEMPLATE_DEFAULT if reflection_template is None else reflection_template
    )
    if set(slot_names(selected_template)) != _REFLECTION_SLOTS:
        raise ValueError(
            "reflection_template slots must be exactly current_template, merged_feedback, "
            f"max_chars; found: {sorted(slot_names(selected_template))}"
        )
    char_budget = len(template) * 2 if max_chars is None else max_chars
    if char_budget < 0:
        raise ValueError("max_chars must be non-negative")
    experiment_fingerprint = _experiment_fingerprint(
        template,
        train_examples,
        val_examples,
        selected_template,
        char_budget,
        leak_run_chars,
        minibatch,
    )

    train_items = [(sha(example), example) for example in train_examples]
    val_items = [(sha(example), example) for example in val_examples]
    train_ids = [example_id for example_id, _ in train_items]
    val_ids = [example_id for example_id, _ in val_items]
    sample_texts = tuple(_strings_from_examples([*train_examples, *val_examples]))
    rng = random.Random(seed)
    judgments: dict[tuple[str, str], Judgment] = {}
    candidates = [Candidate(template, None, {}, {}, 0)]
    metric_calls = 0
    completed_rounds = 0

    restored = _load_state(state_path)
    if restored is not None:
        if restored.get("schema_version") != _STATE_SCHEMA_VERSION:
            print(
                f"Warning: resetting optimize state {state_path}: unsupported schema_version "
                f"{restored.get('schema_version')!r}; expected {_STATE_SCHEMA_VERSION}",
                file=sys.stderr,
            )
        elif restored.get("fingerprint") != experiment_fingerprint:
            print(
                f"Warning: resetting optimize state {state_path}: experiment inputs changed",
                file=sys.stderr,
            )
        elif restored.get("seed") == seed:
            try:
                candidates = [_candidate_from_state(item) for item in restored["candidates"]]
                if not candidates:
                    raise ValueError("empty candidate pool")
                metric_calls = int(restored["metric_calls"])
                completed_rounds = int(restored["round"])
                judgments = _judgments_from_state(restored["judgments"])
            except (KeyError, TypeError, ValueError) as error:
                print(
                    f"Warning: resetting optimize state {state_path}: invalid state: {error}",
                    file=sys.stderr,
                )
                candidates = [Candidate(template, None, {}, {}, 0)]
                metric_calls = 0
                completed_rounds = 0
                judgments = {}

    def persist_state() -> None:
        _write_state(
            state_path,
            seed,
            experiment_fingerprint,
            completed_rounds,
            metric_calls,
            candidates,
            judgments,
        )

    # 每个已完成轮次恰好消费一次随机数，续跑由同一 seed 回放到相同的位置。
    for _ in range(completed_rounds):
        rng.random()

    def capped() -> bool:
        return max_metric_calls is not None and metric_calls >= max_metric_calls

    def score_items(
        text: str,
        items: Iterable[tuple[str, dict[str, Any]]],
        scores: dict[str, float],
    ) -> tuple[dict[str, float], bool]:
        """填充缓存分数；预算耗尽时返回未完成，且不再运行 task 或 metric。"""
        nonlocal metric_calls
        candidate_sha = sha(text)
        updated = dict(scores)
        for example_id, example in items:
            key = (candidate_sha, example_id)
            judgment = judgments.get(key)
            if judgment is None:
                if capped():
                    return updated, False
                try:
                    output = task(text, example)
                except Exception as error:  # 任务错误也必须留下可供反思的失败记录。
                    judgment = Judgment(0.0, f"{type(error).__name__}: {error}", ("task_error",))
                else:
                    judgment = metric(example, output)
                metric_calls += 1
                judgments[key] = judgment
            updated[example_id] = judgment.score
        return updated, True

    # 种子先建立验证基线；没有这条基线就无法用验证侧判断候选去留。
    seed_candidate = candidates[0]
    if not _has_all(seed_candidate.val_scores, val_ids):
        scores, complete = score_items(seed_candidate.text, val_items, seed_candidate.val_scores)
        candidates[0] = Candidate(
            seed_candidate.text,
            seed_candidate.parent,
            seed_candidate.train_scores,
            scores,
            seed_candidate.round,
        )
        if not complete:
            persist_state()
            return _result(candidates, metric_calls, completed_rounds, train_ids, val_ids)

    for round_index in range(completed_rounds, rounds):
        if capped():
            break
        parent_index = _choose_parent(candidates, val_ids, rng)
        parent = candidates[parent_index]
        batch_start = (round_index * minibatch) % len(train_items)
        batch = [
            train_items[(batch_start + offset) % len(train_items)] for offset in range(minibatch)
        ]
        parent_train, complete = score_items(parent.text, batch, parent.train_scores)
        parent = Candidate(
            parent.text,
            parent.parent,
            parent_train,
            parent.val_scores,
            parent.round,
        )
        candidates[parent_index] = parent
        if not complete:
            persist_state()
            break

        # 父本已在本批全部满分时，反思没有可利用的失败信号，直接跳过本轮。
        if all(parent.train_scores[example_id] == 1.0 for example_id, _ in batch):
            completed_rounds = round_index + 1
            persist_state()
            continue

        merged_feedback = _merge_feedback(batch, parent.text, judgments)
        reflection_prompt = render_template(
            selected_template,
            {
                "current_template": inject(parent.text),
                "merged_feedback": inject(merged_feedback),
                "max_chars": inject(str(char_budget)),
            },
        )
        if capped():
            break
        raw_candidate = caller.call(reflection_prompt, model=reflection_model)
        candidate_text = _extract_fenced(raw_candidate)
        if (
            candidate_text is not None
            and len(candidate_text) <= char_budget
            # 只检查相对种子新增的连续片段，允许种子中合法的样例格式说明继续存在。
            and not _contains_leak(candidate_text, template, sample_texts, leak_run_chars)
        ):
            candidate_train, complete = score_items(candidate_text, batch, {})
            if not complete:
                persist_state()
                break
            if all(
                candidate_train[example_id] >= parent.train_scores[example_id]
                for example_id, _ in batch
            ):
                full_train, complete = score_items(candidate_text, train_items, candidate_train)
                if not complete:
                    persist_state()
                    break
                candidate_val, complete = score_items(candidate_text, val_items, {})
                if not complete:
                    persist_state()
                    break
                # train 涨而 val 跌正是过拟合；这道硬闸不能交给提示词自行约束。
                if _mean(candidate_val, val_ids) >= _mean(parent.val_scores, val_ids):
                    candidates.append(
                        Candidate(
                            candidate_text,
                            parent_index,
                            full_train,
                            candidate_val,
                            round_index + 1,
                        )
                    )
                    candidates = _prune_dominated(candidates, val_ids)

        completed_rounds = round_index + 1
        persist_state()

    return _result(candidates, metric_calls, completed_rounds, train_ids, val_ids)


def _merge_feedback(
    train_batch: list[tuple[str, dict[str, Any]]],
    parent_text: str,
    judgments: dict[tuple[str, str], Judgment],
) -> str:
    """仅由训练样例与训练裁决组成反思材料，签名不接收验证侧数据。"""
    groups: dict[str, list[tuple[dict[str, Any], Judgment]]] = defaultdict(list)
    parent_sha = sha(parent_text)
    for example_id, example in train_batch:
        judgment = judgments[(parent_sha, example_id)]
        if judgment.score == 1.0:
            continue
        for tag in judgment.tags or ("untagged",):
            groups[tag].append((example, judgment))

    rendered: list[str] = []
    for tag, items in groups.items():
        lines = [f"问题类型：{tag}"]
        for example, judgment in items[:2]:
            lines.append(f"输入摘要：{_example_summary(example)}")
            lines.append(f"反馈：{judgment.feedback}")
        if len(items) > 2:
            lines.append(f"另有 {len(items) - 2} 例同类。")
        rendered.append("\n".join(lines))
    return "\n\n".join(rendered)


def _example_summary(example: dict[str, Any]) -> str:
    text = canonical_json(example)
    return text if len(text) <= 500 else f"{text[:500]}（已截断）"


def _choose_parent(candidates: list[Candidate], val_ids: list[str], rng: random.Random) -> int:
    best_by_example = {
        example_id: max(candidate.val_scores[example_id] for candidate in candidates)
        for example_id in val_ids
    }
    weights = [
        sum(
            candidate.val_scores[example_id] == best_by_example[example_id]
            for example_id in val_ids
        )
        for candidate in candidates
    ]
    total = sum(weights)
    pick = int(rng.random() * total)
    for index, weight in enumerate(weights):
        if pick < weight:
            return index
        pick -= weight
    raise AssertionError("Pareto weights must select a parent")


def _prune_dominated(candidates: list[Candidate], val_ids: list[str]) -> list[Candidate]:
    return [
        candidate
        for index, candidate in enumerate(candidates)
        if not any(
            other_index != index and _dominates(other, candidate, val_ids)
            for other_index, other in enumerate(candidates)
        )
    ]


def _dominates(left: Candidate, right: Candidate, val_ids: list[str]) -> bool:
    scores_equal = all(left.val_scores[item] == right.val_scores[item] for item in val_ids)
    return all(left.val_scores[item] >= right.val_scores[item] for item in val_ids) and (
        any(left.val_scores[item] > right.val_scores[item] for item in val_ids)
        or (scores_equal and len(left.text) < len(right.text))
    )


def _contains_leak(candidate: str, seed_template: str, texts: tuple[str, ...], run: int) -> bool:
    for start in range(len(candidate) - run + 1):
        window = candidate[start : start + run]
        if window not in seed_template and any(window in text for text in texts):
            return True
    return False


def _strings_from_examples(examples: list[dict[str, Any]]) -> Iterable[str]:
    def visit(value: Any) -> Iterable[str]:
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for item in value.values():
                yield from visit(item)
        elif isinstance(value, list | tuple):
            for item in value:
                yield from visit(item)

    for example in examples:
        yield from visit(example)


def _extract_fenced(raw: str) -> str | None:
    markers = list(_FENCE_MARKER.finditer(raw))
    if len(markers) < 2:
        return None
    return raw[markers[0].end() : markers[-1].start()].strip()


def _has_all(scores: dict[str, float], example_ids: list[str]) -> bool:
    return all(example_id in scores for example_id in example_ids)


def _mean(scores: dict[str, float], example_ids: list[str]) -> float:
    values = [scores[example_id] for example_id in example_ids if example_id in scores]
    return sum(values) / len(values) if values else 0.0


def _result(
    candidates: list[Candidate],
    metric_calls: int,
    rounds_run: int,
    train_ids: list[str],
    val_ids: list[str],
) -> EvolveResult:
    best = min(
        enumerate(candidates),
        key=lambda item: (-_mean(item[1].val_scores, val_ids), len(item[1].text), item[0]),
    )[1]
    return EvolveResult(
        best=best.text,
        candidates=candidates,
        metric_calls=metric_calls,
        rounds_run=rounds_run,
        generalization_gap=_mean(best.train_scores, train_ids) - _mean(best.val_scores, val_ids),
    )


def _write_state(
    state_path: Path | None,
    seed: int,
    fingerprint: str,
    round_number: int,
    metric_calls: int,
    candidates: list[Candidate],
    judgments: dict[tuple[str, str], Judgment],
) -> None:
    if state_path is None:
        return
    atomic_write_json(
        state_path,
        {
            "schema_version": _STATE_SCHEMA_VERSION,
            "seed": seed,
            "fingerprint": fingerprint,
            "round": round_number,
            "metric_calls": metric_calls,
            "candidates": [
                {
                    "text": candidate.text,
                    "parent": candidate.parent,
                    "train_scores": candidate.train_scores,
                    "val_scores": candidate.val_scores,
                    "round": candidate.round,
                }
                for candidate in candidates
            ],
            "judgments": {
                f"{candidate_sha}:{example_id}": {
                    "score": judgment.score,
                    "feedback": judgment.feedback,
                    "tags": list(judgment.tags),
                }
                for (candidate_sha, example_id), judgment in judgments.items()
            },
        },
    )


def _experiment_fingerprint(
    template: str,
    train_examples: list[dict[str, Any]],
    val_examples: list[dict[str, Any]],
    reflection_template: str,
    char_budget: int,
    leak_run_chars: int,
    minibatch: int,
) -> str:
    """Hash every experiment input whose change makes restored judgments unsafe."""
    return sha(
        canonical_json(
            {
                "template": template,
                "train_examples": train_examples,
                "val_examples": val_examples,
                "reflection_template": reflection_template,
                "char_budget": char_budget,
                "leak_run_chars": leak_run_chars,
                "minibatch": minibatch,
            }
        )
    )


def _load_state(state_path: Path | None) -> dict[str, Any] | None:
    if state_path is None:
        return None
    try:
        loaded = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return loaded if isinstance(loaded, dict) else None


def _candidate_from_state(item: Any) -> Candidate:
    if not isinstance(item, dict):
        raise ValueError("candidate state must be an object")
    return Candidate(
        str(item["text"]),
        item["parent"],
        {str(key): float(value) for key, value in item["train_scores"].items()},
        {str(key): float(value) for key, value in item["val_scores"].items()},
        int(item["round"]),
    )


def _judgments_from_state(item: Any) -> dict[tuple[str, str], Judgment]:
    if not isinstance(item, dict):
        raise ValueError("judgments state must be an object")
    restored: dict[tuple[str, str], Judgment] = {}
    for key, value in item.items():
        if not isinstance(key, str) or ":" not in key:
            raise ValueError(
                f"invalid judgment state key {key!r}: expected 'candidate_sha:example_id'"
            )
        candidate_sha, example_id = key.split(":", 1)
        restored[(candidate_sha, example_id)] = Judgment(
            float(value["score"]), str(value["feedback"]), tuple(value["tags"])
        )
    return restored
