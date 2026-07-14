from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from kigumi.evals import Judgment
from kigumi.optimize import evolve_prompt


class FakeCaller:
    def __init__(self, outputs: list[str]) -> None:
        self.outputs = list(outputs)
        self.prompts: list[str] = []

    def call(self, prompt: str, model: str = "default", **params: Any) -> str:
        self.prompts.append(prompt)
        return self.outputs.pop(0)


def _examples() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return ([{"id": "train"}], [{"id": "val"}])


def test_reflection_loop() -> None:
    """教训 reflection_loop: 反思候选只有验证侧变好才会成为胜出者。"""
    train, val = _examples()
    caller = FakeCaller(["```\nbetter\n```"])

    def metric(example: dict[str, Any], output: str) -> Judgment:
        return Judgment(1.0 if output == "better" else 0.4, "待改", ("weak",))

    result = evolve_prompt("seed", train, val, lambda text, example: text, metric, caller, rounds=1)

    assert result.best == "better"
    assert result.candidates[0].val_scores
    assert result.metric_calls == 4


def test_val_isolation() -> None:
    """教训 val_isolation: 验证样例和验证评语没有通往反思 prompt 的通道。"""
    train = [{"id": "train"}]
    val = [{"id": "VAL_SENTINEL_CONTENT"}]
    caller = FakeCaller(["```\nbetter\n```"])

    def metric(example: dict[str, Any], output: str) -> Judgment:
        if example["id"].startswith("VAL_"):
            return Judgment(0.5, "VAL_SENTINEL_FEEDBACK", ("val",))
        return Judgment(0.5, "训练反馈", ("train",))

    evolve_prompt("seed", train, val, lambda text, example: text, metric, caller, rounds=1)

    assert "VAL_SENTINEL_CONTENT" not in caller.prompts[0]
    assert "VAL_SENTINEL_FEEDBACK" not in caller.prompts[0]


def test_overfit_rejected() -> None:
    """教训 overfit_rejected: train 涨而 val 跌就是过拟合，必须剪除。

    候选在 val-a 上比种子高、在 val-b 上大跌：Pareto 支配剪不掉它（互不支配），
    只有 val 均分闸门能拒收——这个测试锚死的是闸门本身，不是剪除的副作用。
    """
    train = [{"id": "train"}]
    val = [{"id": "val-a"}, {"id": "val-b"}]
    caller = FakeCaller(["```\noverfit\n```"])

    def metric(example: dict[str, Any], output: str) -> Judgment:
        if example["id"] == "train":
            return Judgment(1.0 if output == "overfit" else 0.5, "训练", ("weak",))
        if example["id"] == "val-a":
            return Judgment(0.9 if output == "overfit" else 0.8, "验证", ("weak",))
        return Judgment(0.1 if output == "overfit" else 0.8, "验证", ("weak",))

    result = evolve_prompt("seed", train, val, lambda text, example: text, metric, caller, rounds=1)

    assert result.best == "seed"
    assert [candidate.text for candidate in result.candidates] == ["seed"]


def test_leakage_rejected() -> None:
    """教训 leakage_rejected: 新增的样例原文属于作弊，种子原有格式说明不算泄漏。

    泄漏候选刻意比种子更短（否则会被简洁性剪除顺手删掉，掩盖闸门缺失）：
    没有泄漏闸它就会凭简洁性胜出——这个测试锚死的是泄漏闸本身。
    """
    train = [{"source": "TRAIN-LEAK-12345"}]
    val = [{"source": "VAL"}]
    caller = FakeCaller(["```\nTRAIN-LEAK-12345\n```"])

    def metric(example: dict[str, Any], output: str) -> Judgment:
        return Judgment(0.5, "待改", ("weak",))

    rejected = evolve_prompt(
        "seed template much longer than the leak",
        train,
        val,
        lambda text, example: text,
        metric,
        caller,
        rounds=1,
        leak_run_chars=12,
    )
    allowed = evolve_prompt(
        "seed TRAIN-LEAK-12345 plus extra words",
        train,
        val,
        lambda text, example: text,
        metric,
        FakeCaller(["```\nseed TRAIN-LEAK-12345\n```"]),
        rounds=1,
        leak_run_chars=12,
    )

    assert rejected.best == "seed template much longer than the leak"
    assert len(rejected.candidates) == 1
    # 同样片段在种子中已有,新候选只是缩短:不算泄漏,凭简洁性正常胜出。
    assert allowed.best == "seed TRAIN-LEAK-12345"


def test_brevity_dominates() -> None:
    """教训 brevity_dominates: 验证全同分时，短模板属于前沿上的严格优势。"""
    train, val = _examples()
    caller = FakeCaller(["```\nmedium template\n```", "```\nshort\n```"])

    def metric(example: dict[str, Any], output: str) -> Judgment:
        return Judgment(0.5, "待改", ("weak",))

    result = evolve_prompt(
        "seed template much longer",
        train,
        val,
        lambda text, example: text,
        metric,
        caller,
        rounds=2,
    )

    assert result.best == "short"
    assert [candidate.text for candidate in result.candidates] == ["short"]


def test_merged_feedback() -> None:
    """教训 merged_feedback: 同类反馈只举两例，其余应压缩为计数。"""
    train = [{"id": str(index)} for index in range(5)]
    val = [{"id": "val"}]
    caller = FakeCaller(["```\nnew\n```"])

    def metric(example: dict[str, Any], output: str) -> Judgment:
        return Judgment(0.5, f"反馈-{example['id']}", ("same",))

    evolve_prompt(
        "seed long", train, val, lambda text, example: text, metric, caller, rounds=1, minibatch=5
    )

    assert "反馈-0" in caller.prompts[0]
    assert "反馈-1" in caller.prompts[0]
    assert "反馈-2" not in caller.prompts[0]
    assert "另有 3 例同类" in caller.prompts[0]


def test_budget_stop() -> None:
    """教训 budget_stop: 达到指标预算后，任务、评估和反思都必须停下。"""
    train, val = _examples()
    task_calls = 0
    caller = FakeCaller([])

    def task(text: str, example: dict[str, Any]) -> str:
        nonlocal task_calls
        task_calls += 1
        return text

    result = evolve_prompt(
        "seed",
        train,
        val,
        task,
        lambda example, output: Judgment(0.0, "x"),
        caller,
        max_metric_calls=0,
    )

    assert result.metric_calls == 0
    assert task_calls == 0
    assert caller.prompts == []


def test_resume_no_rework(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """教训 resume_no_rework: 相同实验指纹的续跑必须复用已评过的候选-样例裁决。"""
    train, val = _examples()
    state_path = tmp_path / "state.json"
    task_calls = 0

    def task(text: str, example: dict[str, Any]) -> str:
        nonlocal task_calls
        task_calls += 1
        return text

    def metric(example: dict[str, Any], output: str) -> Judgment:
        return Judgment(1.0 if output == "good" else 0.2, "待改", ("weak",))

    evolve_prompt(
        "seed",
        train,
        val,
        task,
        metric,
        FakeCaller(["```\ngood\n```"]),
        rounds=2,
        state_path=state_path,
    )
    before = task_calls
    result = evolve_prompt(
        "seed", train, val, task, metric, FakeCaller([]), rounds=4, state_path=state_path
    )

    assert task_calls == before
    assert result.rounds_run == 4
    assert capsys.readouterr().err == ""


def test_state_fingerprint_resets_when_train_examples_change(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """教训 state_fingerprint: 同 seed 换训练样例必须丢弃旧实验的 judgments。"""
    train, val = _examples()
    state_path = tmp_path / "state.json"
    task_calls = 0

    def task(text: str, example: dict[str, Any]) -> str:
        nonlocal task_calls
        task_calls += 1
        return text

    def metric(example: dict[str, Any], output: str) -> Judgment:
        return Judgment(0.5, "待改", ("weak",))

    evolve_prompt(
        "seed",
        train,
        val,
        task,
        metric,
        FakeCaller(["```\ncandidate\n```"]),
        rounds=1,
        max_chars=16,
        state_path=state_path,
        seed=7,
    )
    calls_before_reset = task_calls
    capsys.readouterr()

    result = evolve_prompt(
        "seed",
        [{"id": "changed-train"}],
        val,
        task,
        metric,
        FakeCaller(["```\ncandidate\n```"]),
        rounds=1,
        max_chars=16,
        state_path=state_path,
        seed=7,
    )

    assert task_calls == calls_before_reset + 4
    assert result.metric_calls == 4
    assert result.rounds_run == 1
    assert [candidate.text for candidate in result.candidates] == ["seed"]
    warning = capsys.readouterr().err
    assert "Warning: resetting optimize state" in warning
    assert "experiment inputs changed" in warning


def test_old_state_is_reset_with_schema_warning(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """教训 state_schema_reset: 无版本旧状态必须可诊断地重置，而非静默丢历史。"""
    state_path = tmp_path / "state.json"
    state_path.write_text('{"seed": 0, "judgments": {}}', encoding="utf-8")
    train, val = _examples()

    result = evolve_prompt(
        "seed",
        train,
        val,
        lambda text, example: text,
        lambda example, output: Judgment(1.0, "好"),
        FakeCaller([]),
        rounds=0,
        state_path=state_path,
    )

    assert result.best == "seed"
    warning = capsys.readouterr().err
    assert "resetting optimize state" in warning
    assert "schema_version" in warning


def test_written_state_has_schema_version_and_fingerprint(tmp_path: Path) -> None:
    """教训 state_schema_write: 新状态必须标明版本和实验输入绑定。"""
    state_path = tmp_path / "state.json"
    train, val = _examples()

    evolve_prompt(
        "seed",
        train,
        val,
        lambda text, example: text,
        lambda example, output: Judgment(1.0, "好"),
        FakeCaller([]),
        rounds=1,
        state_path=state_path,
    )

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["schema_version"] == 2
    assert isinstance(state["fingerprint"], str) and state["fingerprint"]


def test_oversize_rejected() -> None:
    """教训 oversize_rejected: 超字数候选不值得进入任何全量评估。"""
    train, val = _examples()
    task_calls = 0

    def task(text: str, example: dict[str, Any]) -> str:
        nonlocal task_calls
        task_calls += 1
        return text

    result = evolve_prompt(
        "seed",
        train,
        val,
        task,
        lambda example, output: Judgment(0.5, "待改", ("weak",)),
        FakeCaller(["```\nthis is much too long\n```"]),
        rounds=1,
        max_chars=4,
    )

    assert result.best == "seed"
    assert task_calls == 2


def test_perfect_skip() -> None:
    """教训 perfect_skip: minibatch 全满分时不应浪费一次反思调用。"""
    train, val = _examples()
    caller = FakeCaller([])

    result = evolve_prompt(
        "seed",
        train,
        val,
        lambda text, example: text,
        lambda example, output: Judgment(1.0, "好"),
        caller,
        rounds=1,
    )

    assert result.rounds_run == 1
    assert caller.prompts == []


def test_custom_template_contract() -> None:
    """教训 custom_template_contract: 槽位错误开工前报错，正确模板必须实际生效。"""
    train, val = _examples()
    with pytest.raises(ValueError, match="slots"):
        evolve_prompt(
            "seed",
            train,
            val,
            lambda text, example: text,
            lambda example, output: Judgment(0.0, "x"),
            FakeCaller([]),
            reflection_template="{{wrong}}",
        )

    caller = FakeCaller(["```\nnew\n```"])
    custom = "CUSTOM {{current_template}} {{merged_feedback}} {{max_chars}}"
    evolve_prompt(
        "seed",
        train,
        val,
        lambda text, example: text,
        lambda example, output: Judgment(0.0, "x"),
        caller,
        rounds=1,
        reflection_template=custom,
    )

    assert "CUSTOM" in caller.prompts[0]


def test_no_file_side_effects(tmp_path: Path) -> None:
    """教训 no_file_side_effects: 优化器只允许写显式 state_path，不能擅自落模板。"""
    state_path = tmp_path / "checkpoint.json"
    train, val = _examples()

    evolve_prompt(
        "seed",
        train,
        val,
        lambda text, example: text,
        lambda example, output: Judgment(1.0, "好"),
        FakeCaller([]),
        rounds=1,
        state_path=state_path,
    )

    assert {path.name for path in tmp_path.iterdir()} == {"checkpoint.json"}
