from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from kigumi.artifacts import canonical_json, sha
from kigumi.bench import FunctionSubject, TrialObservation, Variant, bench
from kigumi.evals import Judgment


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


def test_report_v2_preserves_trials_judgment_and_null_usage(tmp_path: Path) -> None:
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

    assert report["schema_version"] == 2
    assert report["examples"] == [sha(example) for example in examples]
    assert report["variants"][0]["mean"] == pytest.approx(0.5)
    assert report["variants"][0]["pass_rate"] == pytest.approx(0.5)
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


def _metric(example: dict[str, Any], output: float) -> Judgment:
    del example
    return Judgment(output, f"score={output}", ("score",), {"quality": output})
