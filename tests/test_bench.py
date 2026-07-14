from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from kigumi.artifacts import canonical_json, sha
from kigumi.bench import Variant, bench
from kigumi.calling import LLMCaller
from kigumi.evals import Judgment
from kigumi.testing import FakeTransport


def _variant(
    name: str, *, hypothesis: str = "减少段落间的重复", incumbent: bool = False
) -> Variant:
    return Variant(name, hypothesis, lambda example, caller: example["score"], incumbent)


@pytest.mark.parametrize(
    ("variants", "match"),
    [
        ([], "variants"),
        ([_variant("blank", hypothesis="   ")], "假设是变体的准入证"),
        ([_variant("same"), _variant("same")], "duplicate"),
        ([_variant("none")], "比现状好吗"),
        ([_variant("one", incumbent=True), _variant("two", incumbent=True)], "比现状好吗"),
    ],
)
def test_bench_rejects_invalid_variants(variants: list[Variant], match: str) -> None:
    """教训 bench_admission: 结构探索必须有假设与唯一现状对照。"""
    with pytest.raises(ValueError, match=match):
        bench(variants, [{"score": 0.5}], _score_metric, _fake_factory)


def test_bench_rejects_empty_or_duplicate_seeds() -> None:
    """教训 bench_seed_grid: 每一轮必须是非空、无重复的可重放种子网格。"""
    variants = [_variant("current", incumbent=True)]

    with pytest.raises(ValueError, match="seeds"):
        bench(variants, [{"score": 0.5}], _score_metric, _fake_factory, seeds=[])
    with pytest.raises(ValueError, match="duplicate"):
        bench(variants, [{"score": 0.5}], _score_metric, _fake_factory, seeds=[1, 1])


def test_bench_rejects_empty_examples() -> None:
    with pytest.raises(ValueError, match="examples"):
        bench([_variant("current", incumbent=True)], [], _score_metric, _fake_factory)


def test_bench_rejects_duplicate_examples() -> None:
    """教训 bench_example_identity: 样例身份是内容哈希,重复样例会静默合并证据行。"""
    examples = [{"score": 0.5}, {"score": 0.5}]
    with pytest.raises(ValueError, match="duplicate example"):
        bench([_variant("current", incumbent=True)], examples, _score_metric, _fake_factory)


def test_bench_grid_report_is_deterministic() -> None:
    """教训 bench_grid: 变体×样例×种子应逐格留下可比较、可归档的证据。"""
    examples = [{"id": "a", "score": 0.2}, {"id": "b", "score": 0.8}]
    variants = [
        _variant("current", incumbent=True),
        Variant(
            "split-at-turn",
            "在转折处切段可减少信息丢失",
            lambda example, caller: 1.0 - example["score"],
        ),
    ]

    report = bench(variants, examples, _score_metric, _fake_factory, seeds=(0, 1))

    assert report["examples"] == [sha(example) for example in examples]
    assert report["seeds"] == [0, 1]
    assert len(report["variants"][0]["judgments"]) == 4
    assert report["variants"][0]["mean"] == pytest.approx(0.5)
    assert report["variants"][0]["stdev"] == pytest.approx(0.3)
    assert report["variants"][0]["total_tokens"] is None
    assert report["variants"][0]["call_count"] is None
    assert report["variants"][0]["by_example"] == {
        sha(examples[0]): [0.2, 0.2],
        sha(examples[1]): [0.8, 0.8],
    }
    assert report["variants"][0]["judgments"] == [
        {
            "example_id": sha(examples[0]),
            "seed": 0,
            "score": 0.2,
            "feedback": "score=0.2",
            "tags": ["score"],
        },
        {
            "example_id": sha(examples[1]),
            "seed": 0,
            "score": 0.8,
            "feedback": "score=0.8",
            "tags": ["score"],
        },
        {
            "example_id": sha(examples[0]),
            "seed": 1,
            "score": 0.2,
            "feedback": "score=0.2",
            "tags": ["score"],
        },
        {
            "example_id": sha(examples[1]),
            "seed": 1,
            "score": 0.8,
            "feedback": "score=0.8",
            "tags": ["score"],
        },
    ]


def test_bench_reuses_l1_cache_without_changing_report(tmp_path: Path) -> None:
    """教训 bench_cache_reuse: 固定种子重跑时必须命中既有 L1 证据。"""
    transport = FakeTransport()
    variants = [
        Variant(
            "current",
            "保留当前整段调用以建立对照",
            lambda example, caller: caller.call(f"outline:{example['id']}"),
            incumbent=True,
        )
    ]
    examples = [{"id": "a"}, {"id": "b"}]

    def factory(seed: int) -> LLMCaller:
        return LLMCaller(transport, tmp_path / "cache", seed=seed)

    def metric(example: dict[str, Any], output: str) -> Judgment:
        del example
        return Judgment(1.0 if output == "answer" else 0.0, output)

    first = bench(variants, examples, metric, factory, seeds=(0, 1))
    requests_after_first = len(transport.requests)
    second = bench(variants, examples, metric, factory, seeds=(0, 1))

    assert requests_after_first == 4
    assert len(transport.requests) == requests_after_first
    assert first["variants"][0]["total_tokens"] == 16
    assert first["variants"][0]["call_count"] == 4
    assert second == first


def test_bench_keeps_task_errors_visible() -> None:
    """教训 bench_task_error: 坏样例应记零分而非中断其他样例的结构比较。"""

    def task(example: dict[str, Any], caller: Any) -> str:
        if example["broken"]:
            raise RuntimeError("坏输入")
        return "ok"

    report = bench(
        [Variant("current", "保留当前切法作为对照", task, incumbent=True)],
        [{"broken": True}, {"broken": False}],
        lambda example, output: Judgment(1.0, "好"),
        _fake_factory,
        seeds=(0,),
    )

    judgments = report["variants"][0]["judgments"]
    assert judgments[0]["score"] == 0.0
    assert judgments[0]["tags"] == ["task_error"]
    assert judgments[1]["score"] == 1.0


def test_bench_reports_pass_rate_only_when_requested() -> None:
    """教训 bench_threshold: 合格线是证据切片，不是胜负裁决。"""
    variants = [_variant("current", incumbent=True)]
    examples = [{"score": 0.4}, {"score": 0.8}]

    thresholded = bench(
        variants,
        examples,
        _score_metric,
        _fake_factory,
        seeds=(0,),
        pass_threshold=0.5,
    )
    unthresholded = bench(variants, examples, _score_metric, _fake_factory, seeds=(0,))

    assert thresholded["variants"][0]["pass_rate"] == pytest.approx(0.5)
    assert "pass_rate" not in unthresholded["variants"][0]


def test_bench_report_path_uses_canonical_json(tmp_path: Path) -> None:
    """教训 bench_archive_bytes: 归档报告必须与返回值的规范字节完全一致。"""
    path = tmp_path / "benches" / "outline.json"
    report = bench(
        [_variant("current", incumbent=True)],
        [{"score": 0.5}],
        _score_metric,
        _fake_factory,
        seeds=(0,),
        report_path=path,
    )

    assert path.read_text(encoding="utf-8") == canonical_json(report)


def _score_metric(example: dict[str, Any], output: float) -> Judgment:
    return Judgment(output, f"score={output}", ("score",))


def _fake_factory(seed: int) -> Any:
    del seed
    return object()
