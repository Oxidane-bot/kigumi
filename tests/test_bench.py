from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

from kigumi.artifacts import canonical_json, sha
from kigumi.bench import FunctionSubject, TrialObservation, Variant, bench
from kigumi.evals import Judgment

bench_module = import_module("kigumi.bench")


def _subject() -> FunctionSubject:
    return FunctionSubject(
        lambda example, context: TrialObservation(
            output=example["score"],
            usage=None,
            evidence={"trial": context.trial_id},
            seed_applied=False,
        ),
        identity={"kind": "score"},
        seed_mode="unsupported",
    )


def _variant(name: str, *, hypothesis: str = "减少重复", incumbent: bool = False) -> Variant:
    return Variant(name, hypothesis, _subject(), incumbent)


@pytest.mark.parametrize(
    ("variants", "match"),
    [
        ([], "variants"),
        ([_variant("blank", hypothesis="   ")], "假设"),
        ([_variant("same"), _variant("same")], "duplicate"),
        ([_variant("none")], "比现状好吗"),
        ([_variant("one", incumbent=True), _variant("two", incumbent=True)], "比现状好吗"),
    ],
)
def test_bench_rejects_invalid_variants(
    variants: list[Variant], match: str, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match=match):
        bench(variants, [{"score": 0.5}], _metric, experiment_dir=tmp_path)


def test_old_bench_api_is_a_hard_cut() -> None:
    with pytest.raises(TypeError):
        Variant(name="old", hypothesis="old", task=lambda example, caller: example)
    with pytest.raises(TypeError):
        bench(
            [_variant("current", incumbent=True)],
            [{"score": 1}],
            _metric,
            caller_factory=object(),
        )


def test_report_v3_preserves_trials_judgment_and_null_usage(tmp_path: Path) -> None:
    examples = [{"id": "a", "score": 0.2}, {"id": "b", "score": 0.8}]
    variants = [_variant("current", incumbent=True)]
    path = tmp_path / "report.json"

    report = bench(
        variants,
        examples,
        _metric,
        seeds=(0, 1),
        pass_threshold=0.5,
        experiment_dir=tmp_path / "experiment",
        report_path=path,
    )

    assert report["schema_version"] == 3
    assert report["examples"] == [sha(example) for example in examples]
    assert report["variants"][0]["mean"] == pytest.approx(0.5)
    assert report["variants"][0]["pass_rate"] == pytest.approx(0.5)
    assert report["variants"][0]["outcome_summary"] == {
        "trial_count": 4,
        "subject_successes": 4,
        "metric_successes": 4,
        "subject_failures": 0,
        "metric_failures": 0,
        "subject_failure_rate": 0.0,
        "metric_failure_rate": 0.0,
        "any_failure_rate": 0.0,
    }
    assert len(report["trials"]) == 4
    assert report["trials"][0]["judgment"] == {
        "score": 0.2,
        "feedback": "score=0.2",
        "tags": ["score"],
        "subscores": {"quality": 0.2},
    }
    assert report["trials"][0]["usage"] is None
    assert report["trials"][0]["seed_mode"] == "unsupported"
    assert report["trials"][0]["seed_applied"] is False
    assert report["trials"][0]["error"] is None
    assert path.read_text(encoding="utf-8") == canonical_json(report)
    assert all(Path(trial["project_root"]).is_dir() for trial in report["trials"])
    assert len({trial["project_root"] for trial in report["trials"]}) == 4


def test_trial_errors_are_isolated(tmp_path: Path) -> None:
    def run(example: dict[str, Any], context: Any) -> TrialObservation:
        if example["broken"]:
            raise RuntimeError("bad input")
        return TrialObservation("ok", None, {}, False)

    subject = FunctionSubject(run, identity={"kind": "sometimes"}, seed_mode="unsupported")
    report = bench(
        [Variant("current", "keep baseline", subject, True)],
        [{"broken": True}, {"broken": False}],
        lambda example, output: Judgment(1.0, "ok"),
        seeds=(0,),
        experiment_dir=tmp_path,
    )

    assert report["trials"][0]["judgment"]["tags"] == ["task_error"]
    assert report["trials"][0]["error"]["stage"] == "subject"
    assert report["trials"][1]["judgment"]["score"] == 1.0
    assert "winner" not in report


def test_bench_reports_stage_aware_outcomes_without_changing_quality_scores(
    tmp_path: Path,
) -> None:
    examples = [{"id": "zero", "score": 0.0}, {"id": "good", "score": 1.0}]

    valid_zero = FunctionSubject(
        lambda example, context: TrialObservation(example["score"]),
        identity={"kind": "valid-zero"},
    )

    def fail_zero(example: dict[str, Any], context: Any) -> TrialObservation:
        if example["id"] == "zero":
            raise RuntimeError("subject unavailable")
        return TrialObservation(example["score"])

    subject_failure = FunctionSubject(fail_zero, identity={"kind": "subject-failure"})
    report = bench(
        [
            Variant("valid-zero", "valid zero score", valid_zero, True),
            Variant("failed-zero", "subject failure", subject_failure),
        ],
        examples,
        _metric,
        seeds=(0,),
        pass_threshold=0.5,
        experiment_dir=tmp_path,
    )

    valid_report, failed_report = report["variants"]
    for field in ("mean", "stdev", "pass_rate", "by_example"):
        assert valid_report[field] == failed_report[field]
    assert valid_report["outcome_summary"] == {
        "trial_count": 2,
        "subject_successes": 2,
        "metric_successes": 2,
        "subject_failures": 0,
        "metric_failures": 0,
        "subject_failure_rate": 0.0,
        "metric_failure_rate": 0.0,
        "any_failure_rate": 0.0,
    }
    assert failed_report["outcome_summary"] == {
        "trial_count": 2,
        "subject_successes": 1,
        "metric_successes": 1,
        "subject_failures": 1,
        "metric_failures": 0,
        "subject_failure_rate": 0.5,
        "metric_failure_rate": 0.0,
        "any_failure_rate": 0.5,
    }
    assert report["trials"][2]["error"]["stage"] == "subject"


def test_metric_failure_is_not_counted_as_subject_failure(tmp_path: Path) -> None:
    def metric(example: dict[str, Any], output: Any) -> Judgment:
        del output
        if example["broken"]:
            raise ValueError("metric unavailable")
        return Judgment(1.0, "ok")

    report = bench(
        [
            Variant(
                "current",
                "metric failure",
                FunctionSubject(
                    lambda example, context: TrialObservation("output"),
                    identity={"kind": "metric-failure"},
                ),
                True,
            )
        ],
        [{"broken": True}, {"broken": False}],
        metric,
        seeds=(0,),
        experiment_dir=tmp_path,
    )

    assert report["variants"][0]["outcome_summary"] == {
        "trial_count": 2,
        "subject_successes": 2,
        "metric_successes": 1,
        "subject_failures": 0,
        "metric_failures": 1,
        "subject_failure_rate": 0.0,
        "metric_failure_rate": 0.5,
        "any_failure_rate": 0.5,
    }
    failed_trial = report["trials"][0]
    assert failed_trial["judgment"]["score"] == 0.0
    assert failed_trial["error"]["stage"] == "metric"


@pytest.mark.parametrize("pass_threshold", [None, 0.5])
def test_outcome_summary_counts_every_seed_example_cell(
    pass_threshold: float | None, tmp_path: Path
) -> None:
    def run(example: dict[str, Any], context: Any) -> TrialObservation:
        if example["id"] == "bad" and context.seed == 1:
            raise RuntimeError("one failed cell")
        return TrialObservation(1.0)

    report = bench(
        [
            Variant(
                "current",
                "count planned cells",
                FunctionSubject(run, identity={"kind": "seed-grid"}),
                True,
            )
        ],
        [{"id": "good"}, {"id": "bad"}],
        _metric,
        seeds=(0, 1),
        pass_threshold=pass_threshold,
        experiment_dir=tmp_path,
    )

    assert report["variants"][0]["outcome_summary"] == {
        "trial_count": 4,
        "subject_successes": 3,
        "metric_successes": 3,
        "subject_failures": 1,
        "metric_failures": 0,
        "subject_failure_rate": 0.25,
        "metric_failure_rate": 0.0,
        "any_failure_rate": 0.25,
    }


def test_outcome_summary_does_not_enter_trial_identity(tmp_path: Path) -> None:
    report = bench(
        [Variant("current", "stable identity", _subject(), True)],
        [{"score": 0.25}],
        _metric,
        seeds=(17,),
        experiment_dir=tmp_path,
    )

    trial = report["trials"][0]
    expected_trial_id = sha(
        {
            "variant": trial["variant"],
            "subject": report["variants"][0]["subject_identity"],
            "example": trial["example_id"],
            "seed": trial["seed"],
        }
    )
    assert trial["trial_id"] == expected_trial_id


def test_unknown_error_stage_fails_closed() -> None:
    with pytest.raises(ValueError, match="Unknown benchmark error stage"):
        bench_module._outcome_summary([{"stage": "future"}], trial_count=1)


def _metric(example: dict[str, Any], output: float) -> Judgment:
    del example
    return Judgment(output, f"score={output}", ("score",), {"quality": output})
