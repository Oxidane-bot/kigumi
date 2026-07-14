"""evolve_prompt 的零请求验收演示。

运行：uv run python examples/prompt_evolve/demo.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from kigumi.evals import Judgment
from kigumi.optimize import EvolveResult, evolve_prompt


class ScriptedCaller:
    """以预置围栏文本替代反思模型；不创建 transport，也不发网络请求。"""

    def __init__(self, outputs: list[str]) -> None:
        self.outputs = list(outputs)
        self.prompts: list[str] = []

    def call(
        self, messages: list[dict[str, Any]] | str, model: str = "default", **params: Any
    ) -> str:
        del model, params
        assert isinstance(messages, str)
        self.prompts.append(messages)
        if not self.outputs:
            raise AssertionError("脚本化反思输出已耗尽")
        return self.outputs.pop(0)


def _fenced(text: str) -> str:
    return f"```\n{text}\n```"


def _examples() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        [{"id": "train-a"}, {"id": "train-b"}],
        [{"id": "val-a"}, {"id": "val-b"}],
    )


def _task(template: str, example: dict[str, Any]) -> str:
    del example
    return template


def _metric(example: dict[str, Any], output: str) -> Judgment:
    """确定性 Python 评分：原则模板泛化，train-only 模板只在训练侧得分。"""
    example_name = str(example.get("id", example.get("body", "example")))
    if "原则：先核验证据" in output:
        score = 1.0
    elif "仅训练集优化" in output:
        score = 1.0 if example_name.startswith("train") else 0.1
    else:
        score = 0.4
    return Judgment(score, f"{example_name} 的确定性反馈", ("evidence",))


def _run(
    caller: ScriptedCaller,
    *,
    max_metric_calls: int | None = None,
    state_path: Path | None = None,
    rounds: int = 1,
    max_chars: int = 80,
    train: list[dict[str, Any]] | None = None,
    val: list[dict[str, Any]] | None = None,
) -> EvolveResult:
    default_train, default_val = _examples()
    return evolve_prompt(
        "种子模板：先给出结论。",
        default_train if train is None else train,
        default_val if val is None else val,
        _task,
        _metric,
        caller,
        rounds=rounds,
        minibatch=1,
        max_metric_calls=max_metric_calls,
        max_chars=max_chars,
        leak_run_chars=12,
        state_path=state_path,
        seed=17,
    )


def _accepted_texts(result: EvolveResult) -> list[str]:
    return [candidate.text for candidate in result.candidates]


def main() -> None:
    # 种子先评完整验证集；一轮原则性修正同时通过 train 和 val，进入候选池并成为最优。
    accepted = _run(ScriptedCaller([_fenced("原则：先核验证据，再给出结论。")]))
    assert accepted.best == "原则：先核验证据，再给出结论。"
    assert accepted.metric_calls == 7
    print("[1] 种子 val 基线与成功进化：通过")

    # train 涨但 val 降，验证硬闸拒收，种子仍是唯一候选。
    overfit = _run(ScriptedCaller([_fenced("仅训练集优化")]))
    assert overfit.best == "种子模板：先给出结论。"
    assert _accepted_texts(overfit) == ["种子模板：先给出结论。"]
    print("[2] 验证拒收（过拟合硬闸）：通过")

    leak_train = [{"body": "训练样例原文连续片段-不可复写"}]
    leak_val = [{"body": "验证样例"}]
    leaked = _run(
        ScriptedCaller([_fenced("训练样例原文连续片段-不可复写")]),
        train=leak_train,
        val=leak_val,
    )
    assert _accepted_texts(leaked) == ["种子模板：先给出结论。"]
    print("[3] 样例原文泄漏拒收：通过")

    too_long = _run(ScriptedCaller([_fenced("原则：先核验证据。" * 20)]), max_chars=20)
    assert _accepted_texts(too_long) == ["种子模板：先给出结论。"]
    print("[4] 字符预算拒收：通过")

    # 中途预算留下 judgments 与候选池；同 seed 恢复时随机父本选择回放到同一位置。
    with tempfile.TemporaryDirectory(prefix="kigumi-prompt-evolve-") as temporary:
        state_path = Path(temporary) / "evolve-state.json"
        paused = _run(
            ScriptedCaller([_fenced("原则：先核验证据，再给出结论。")]),
            max_metric_calls=4,
            state_path=state_path,
        )
        assert paused.rounds_run == 0 and paused.metric_calls == 4 and state_path.is_file()
        resumed = _run(
            ScriptedCaller([_fenced("原则：先核验证据，再给出结论。")]),
            max_metric_calls=20,
            state_path=state_path,
        )
        one_shot = _run(ScriptedCaller([_fenced("原则：先核验证据，再给出结论。")]))
        assert resumed.best == one_shot.best
        assert resumed.metric_calls == one_shot.metric_calls == 7
        assert resumed.rounds_run == one_shot.rounds_run == 1
        assert _accepted_texts(resumed) == _accepted_texts(one_shot)
    print("[5] 指标预算中断、状态落盘与同 seed 续跑：通过")

    print("演示通过:基线、成功进化、验证/泄漏/字符拒收、预算续跑全部走通。")


if __name__ == "__main__":
    main()
