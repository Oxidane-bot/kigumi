# Durable retry 与 run resume 契约

Status: Active (0.6.0)

## Public surface

`RetryPolicy` 可用于 `Dag.node`、`Dag.agent`、map/scan；默认 `retry=None`，绝不自动重试。
恢复入口为 `Dag.resume(run_id, workers=1)`；`Dag.run(run_id=已有 0.6 run)` 走同一绑定实现。
人工裁决使用：

```text
dag retry-resolve RUN_ID TARGET --attempt N --action retry|fail --reason TEXT
```

## Invariants

1. `max_attempts` 包含首次执行。默认只允许 rate limit、server error、timeout、connection；
   unknown、auth、authorization、invalid request、model mismatch、policy/schema 与所有 Agent
   runtime failure 默认不重试。
2. full jitter 由 `run_id + target + attempt + policy digest` 确定性派生。provider
   `retry_after_ms` 是下界；`max_delay_seconds` 只限制本地指数退避。
3. retry digest 属于 run execution identity 与 attempt receipt，不进入 L3 内容键。durable
   CALL 要求 transport/length/empty hidden retry 全为 0；Pi hidden retry 事件立即失败。
4. `_run.json` schema 1 绑定 graph identity、targets、force、source/libs、retry/evidence
   digests 与状态。0.5.x/缺 manifest run 只读，不可 resume；声明变化 fail closed。
5. 每个 `runs/<run>/attempts/<target_digest>/state.json` 与 `attempt-NNNN.json` 使用 receipt
   schema 1。执行前写 running；provider call/Pi spawn 前写 `side_effect_started=true`；
   成功先写 canonical candidate，再 seal/materialize/sidecar，最后 completed。
6. crash-after-success 可提交 candidate 而不重做 side effect。crash 且 side effect 未开始可
   恢复同 attempt；已开始但无 terminal receipt 必须 ambiguous，未经带 reason 的人工裁决
   不得重试。
7. retryable failure 写 `due_at` 后返回 pending，不在 Kigumi 内 sleep。未到期 resume 不产生
   side effect；外部 supervisor 负责到期再次调用。
8. pending retry 与 checkpoint 一样只阻断下游，不阻断独立分支。map 每 item 独立 attempt；
   scan 复用已验证前缀，只重试失败 item，后缀保持未执行。
9. 同 run completed artifact（含 `cache="off"`）恢复时必须重验 artifact digest、key
   components、输出/blob 字节，并且不重新执行。

## Exactly-once boundary

Kigumi 记录可观察的 CALL/Agent attempt 边界，但不承诺外部 effect exactly-once。ambiguous
状态正是对该不确定性的显式暴露。

## Verification

见 `tests/test_retry.py`、`tests/test_dag_retry_resume.py`、`tests/test_dag_checkpoints.py`、
`tests/test_cli.py`。
