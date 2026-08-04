# Durable retry 与 run resume 契约

Status: Active (0.13.0)

## Public surface

`RetryPolicy` 可用于 `Dag.node`、`Dag.agent`、map/scan；默认 `retry=None`，绝不自动重试。
恢复入口为 `Dag.resume(run_id, workers=1)`；`Dag.run(run_id=已有 0.7 run)` 走同一绑定实现。
人工裁决使用：

```text
dag retry-resolve RUN_ID TARGET --attempt N --action retry|fail --reason TEXT
```

底层 `AttemptStore` 为需要原子记录恢复决定的调用方提供
`schedule_recovery(..., recovery_receipt=payload)` 与
`write_recovery_receipt(payload)`，以及用于 fail/retry 互斥裁决的
`record_recovery_decision(...)`。恢复 receipt 始终保持原有 public JSON shape；
`record_recovery_decision` 在同一 run lock 内排他创建 `recovery-*.json`，并把
target、from/to attempt、decision、相对文件名及 canonical digest 追加到
`_run.json` 的 `recovery_decisions` ledger（同时由
`recovery_decisions_sha256` 锚定）。retry decision 可以把同一绑定复制到新的
current state；fail decision 只更新 manifest ledger，不重写已经 terminal 的
current state 或 `attempt-NNNN.json`。`write_recovery_receipt(payload)` 仍是
兼容的独立 receipt writer，不声称完成 decision binding。

终态 `failed` run 使用 `Dag.recover()` 记录显式 decision、reason 与 evidence，再由
`Dag.resume()` 执行新 attempt；恢复不删除 run 目录，也不改动缓存键族。

## Invariants

1. `max_attempts` 包含首次执行。默认只允许 rate limit、server error、timeout、connection；
   unknown、auth、authorization、invalid request、model mismatch、policy/schema 与所有 Agent
   runtime failure 默认不重试。
2. full jitter 由 `run_id + target + attempt + policy digest` 确定性派生。provider
   `retry_after_ms` 是下界；`max_delay_seconds` 只限制本地指数退避。
3. retry digest 属于 run execution identity 与 attempt receipt，不进入 L3 内容键。durable
   CALL 要求 transport/length/empty hidden retry 全为 0；Pi hidden retry 事件立即失败。
4. `_run.json` schema 2 绑定 graph identity、targets、force、source/libs、retry/evidence
   digests、完整 Prompt 候选 universe、WorkflowProfile 及其 digest。缺少 schema-2 manifest
   的 run 不可 resume；任何声明或未选中候选变化都 fail closed。
5. 每个 `runs/<run>/attempts/<target_digest>/state.json` 与 `attempt-NNNN.json` 使用 receipt
   schema 2，并绑定当前 target 的全部 Prompt resolution。执行前写 running；每次 live
   provider CALL 或 Agent spawn 前原子写 `side_effect_started=true`、active effect、
   actual prompt/instruction digest 与 resolution；成功先写 schema-2 canonical candidate，
   再 seal/materialize/schema-2 sidecar，最后 completed。
   每次 state 快照还带单调的 `receipt_sequence`、`previous_receipt_sha256` 和
   `state_sha256`；`_run.json` 的 `attempt_receipt_chains` 保存每个 target 的追加式链头。
   读取时，state、当前 receipt、每个历史 attempt 的最终 receipt 必须分别与链绑定；链不
   存在、断裂、回退、分叉或只更新了其中一份 durable record 都必须 fail closed。
   active 的 target state 还带一个不参与缓存键或 run identity 的
   `target_owner_token`。`AttemptStore` 用 run 目录外的稳定 advisory lock 串行化
   state/receipt/manifest 的 durable read-modify-write，并用每个 target 唯一且稳定的
   descriptor-backed lock path 阻止两个 live executor 同时接管同一 attempt；token 是
   durable owner 的 fencing token，不能编码进 lock 文件名。所有执行态 mutation 都必须
   同时持有本实例 lease 且匹配磁盘 owner token；active state 缺失 token、token 不匹配或
   lease 已被新 owner/人工裁决取代时必须 fail closed。进程退出后由操作系统释放 lease。
   lease 只表达本地执行所有权，不承诺外部副作用 exactly-once。
6. crash-after-success 可提交 candidate 而不重做 side effect。crash 且 side effect 未开始可
   恢复同 attempt；已开始但无 terminal receipt 必须 ambiguous，未经带 reason 的人工裁决
   不得重试。
7. retryable failure 写 `due_at` 后返回 pending，不在 Kigumi 内 sleep。未到期 resume 不产生
   side effect；外部 supervisor 负责到期再次调用。
8. pending retry 与 checkpoint 一样只阻断下游，不阻断独立分支。map 每 item 独立 attempt；
   scan 复用已验证前缀，只重试失败 item，后缀保持未执行。
9. 同 run completed artifact（含 `cache="off"`）恢复时必须重验 Prompt snapshot/selection/
   resolution digest、candidate、artifact、origin、sidecar、输出/blob 字节，并且不重新执行。
   manifest 记录 `resume_count` 与 `last_resumed_at`，但它们不改变 immutable run identity。
10. terminal `failed` recovery 只能匹配当前失败 attempt。每个 recovery decision 都必须
    追加到 `_run.json` 的 `recovery_decisions` ledger，并由 ledger digest、receipt
    filename 和 receipt digest 共同绑定；ledger 或绑定 receipt 缺失、损坏、重复或
    不匹配时必须 fail closed。retry decision 追加新 attempt 并记录继承的成功节点，
    旧 attempt receipt 与 recovery receipt 保留不删不覆写；fail decision 只更新 ledger，
    当前 failed state 和对应的 `attempt-NNNN.json` 必须保持字节不变。
11. 已存在但 JSON、schema 或 digest 不可信的 durable manifest、attempt receipt、candidate、
    artifact 或 Prompt lineage 是完整性错误，不是缺失状态；`StateIntegrityError` 或
    `RunManifestError` 必须 fail closed，不能把它当成未开始 attempt 创建新执行。只有真正缺失
    的当前 receipt 才能按未开始处理；若已记录 side effect boundary，则
    `state_for`、`pending_retries`、`ambiguous_attempts` 等读取也必须抛出明确的
    `AmbiguousAttemptError` 或完整性错误，不能返回可信的 running 状态或因为 receipt
    缺失而重放。

12. receipt-chain 是 Greenfield 的硬切格式。没有 manifest 链锚或没有上述 chain fields 的
    旧 attempt state 不可 resume；这不是缓存键变更，也不改变默认 Agent/agent-scan 的
    恢复语义。

## Exactly-once boundary

Kigumi 记录可观察的 CALL/Agent attempt 边界，但不承诺外部 effect exactly-once。ambiguous
状态正是对该不确定性的显式暴露。

`AttemptStore` 的锁只覆盖其 durable state transition。`Dag.recover()` 外层写入的
`recovery-<timestamp>.json` 仍与随后排队 attempt 的调用分开，因此不能把整个 public
`recover()` 调用宣称为一个跨进程原子事务。

## Integrity threat model

`state_sha256`、`previous_receipt_sha256` 和 manifest 中的 receipt chain 使用无密钥的
canonical SHA-256。它们用于检测撕裂写入、截断、意外损坏，以及在 manifest 未被同时改写时
对 state/receipt 的单边或协同重写；它们不是 MAC、签名，也不是防篡改证明。

如果攻击者可以同时重写 `_run.json` 的链、state、receipt 及其摘要，那么在没有外部信任锚
（例如签名密钥、远端审计日志或 WORM 存储）的前提下，Kigumi 无法区分该协同重写与一次新的
合法历史。这是条件性限制，不应被 `state_sha256` 的存在掩盖。应用仍对所有无法通过本地
绑定验证的记录 fail closed；外部信任锚属于部署层的额外能力。

## Verification

见 `tests/test_retry.py`、`tests/test_dag_retry_resume.py`、`tests/test_dag_checkpoints.py`、
`tests/test_runstate_integrity.py`、`tests/test_cli.py`。
