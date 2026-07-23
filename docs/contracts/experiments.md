# Experiments 契约

## Purpose

用同一证据网格比较函数、Caller、普通 DAG 与 Agent-backed DAG，而不把实验器变成优化器。

## Scope / source of truth

公开 subject/trial/report 语义以 `kigumi/bench.py` 为准。评估结果结构以 `Judgment` 为准。

## Invariants

1. `Variant` 必须有 hypothesis，且实验恰有一个 incumbent；不生成 winner。
2. subject 在 admission 前给出 canonical identity、`seed_mode` 与 `seed_keyed`。
3. trial id 由 variant、subject identity、example content id 与 seed 可重算得到。
4. 每格拥有 `experiment_dir/trials/<trial_id>/{project,evidence}` 独立根。
5. `DagSubject` 的 Dag project/artifacts root 必须精确等于 trial roots。
6. `AgentSubject(adapter, spec, task, files, output)` 每格构造隔离的单 Agent DAG；example 作为
   canonical upstream，`files(example)` 的文本/字节成为声明输入，target 固定 `cache="off"`。
7. `AgentSubject.identity()` 自动包含 adapter/Pi、AgentSpec、task/files/output 源码摘要和显式
   external fingerprint；Pi seed 不可验证，固定 `seed_mode="unsupported"`。
8. Agent observation 提取 output、usage、duration、trajectory/raw evidence、run id、cache policy
   和 Agent identity；单格失败保留 failure evidence 并按普通 bench 规则记 0 分。
9. v1 的 multi-seed Dag target 使用 `cache=auto` 时在第一格前拒绝；只允许 refresh/off。
10. report schema v2 逐格保存完整 Judgment（含 subscores）、duration、usage/null、evidence、
   seed 声明/实测与 error/null；单格失败不停止其余网格。
11. bench 不修改 subject、Skill、Prompt，不 mutation、promotion 或自动接线。

## Failure behavior

admission 错误在运行第一格前失败；subject/metric 错误只把当前格记 0 分并附明确 stage/tag。

## Affected surfaces

0.5.0 删除 `Variant.task` 和 `caller_factory`，无兼容路径。报告消费者必须读取 schema v2。

## Verification / change policy

见 `tests/test_bench.py`、`tests/test_experiment_subjects.py` 与 `tests/test_pi_first.py`。改变 trial identity 或报告字段需要
递增 report schema；bench 本身不得暗中演化为 winner/optimizer。
