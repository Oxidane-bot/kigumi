# Experiments 契约

Status: Active (0.5.0)

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
10. report schema v3 逐格保存完整 Judgment（含 subscores）、duration、usage/null、evidence、
   seed 声明/实测与 error/null；单格失败不停止其余网格。
11. 每个 variant 的 `outcome_summary` 与 quality 聚合分轴保存。`trial_count` 是该 variant
   的计划格数（`len(examples) * len(seeds)`），失败格也计入；所有 rate 都以它为分母。
12. `bench` 不修改 subject、Skill、Prompt，不 mutation、promotion 或自动接线。

### Schema 3 的 outcome_summary

variant 报告保留原有 `mean`、`stdev`、`by_example` 和可选 `pass_rate`，并新增以下确定性
对象；它不改变 `Judgment.score` 的零分语义：

```json
{
  "outcome_summary": {
    "trial_count": 4,
    "subject_successes": 3,
    "metric_successes": 2,
    "subject_failures": 1,
    "metric_failures": 1,
    "subject_failure_rate": 0.25,
    "metric_failure_rate": 0.25,
    "any_failure_rate": 0.5
  }
}
```

- `error` 为 `null`：`subject_successes` 与 `metric_successes` 各加一。
- `error.stage == "subject"`：只加 `subject_failures`；metric 没有运行，不算 metric failure
  或 success。
- `error.stage == "metric"`：subject 已成功，加 `subject_successes` 与 `metric_failures`；
  不加 `metric_successes`。
- `any_failure_rate` 是
  `(subject_failures + metric_failures) / trial_count`。非空且未知的 stage 必须 fail closed，
  不能静默归类。

质量轴仍只看 score；运行结果轴才看 subject/metric 的可用性与评估覆盖。因此合法的
`Judgment(score=0.0, ...)` 与 subject failure 可以有相同质量聚合，但 outcome summary 不同。
不汇总自由格式的 error message 或 tags；完整 error、evidence 和 Judgment 仍留在 raw trial。

## Failure behavior

admission 错误在运行第一格前失败；subject/metric 错误只把当前格记 0 分并附明确 stage/tag。

## Affected surfaces

0.5.0 删除 `Variant.task` 和 `caller_factory`，无兼容路径。报告消费者必须读取 schema v3；
schema-2 reader 遇到 schema 3 需要升级。旧 schema-2 报告没有 producer 保证的 summary，消费者
可以从保留的 raw trials 自行推导，但不得静默重写旧报告。

## Verification / change policy

见 `tests/test_bench.py`、`tests/test_experiment_subjects.py` 与 `tests/test_pi_first.py`。改变 trial identity 或报告字段需要
递增 report schema；本次只增加 report schema 3 的 variant summary，不改变 trial id、subject identity、provenance、
节点声明或任何 cache key，也不提升 `CACHE_SCHEMA`，因此不轮换缓存族；bench 本身不得暗中演化为 winner/optimizer。
