# 检查点审批契约

Status: Active

## Purpose

让人工审批只对其实际审阅过的 payload 生效，并允许同一次 run 在审批后可预测地恢复。

## Scope

适用于普通与挂载节点、map/scan item 内的 `ctx.checkpoint()`、`Dag.approve()`、
`kigumi approve`、L3 写入决策和 run 目录中的审批文件。

## Source of truth

`kigumi.dag.NodeContext.checkpoint()` 决定限定名与审批是否可用，`Dag.run()` 及动态执行路径
决定实际调用检查点后的 L3 写入隔离，`kigumi.store.approve_checkpoint()` 写入绑定 payload
摘要的审批记录并清理 pending marker。

## Invariants

1. 审批绑定 payload 内容哈希；同一 0.6 run 的 graph/source/policy 声明另由 schema-1 manifest
   固定，声明变化必须 fail closed，而不是覆盖旧 run。新 payload 声明应使用新 run。
2. 检查点身份先按节点作用域限定，再追加动态 item ID：普通节点为 `approval`，普通
   map/scan 项为 `approval@item`，挂载普通节点为 `approval@namespace.local`，挂载
   map/scan 项为 `approval@namespace.local@item`。重复挂载的同名检查点因此互不混淆。
3. 审批只属于产生挂起的 run；恢复必须以同一 `run_id` 重跑。成功批准会删除匹配的
   `.pending.json`；之后 payload 改变时旧批准失效，并可生成新的 pending 记录。
4. 一次执行只要实际调用过 `ctx.checkpoint()`，其成功 artifact 就不得写入 L3 节点/item
   缓存，避免批准结果经缓存泄漏到其他 run、挂载或 item。条件分支本次未调用检查点时，
   仍遵循节点声明的 `auto`/`refresh`/`off` 策略。
5. `run_id` 与最终检查点名必须是安全的单个非空文件系统路径成分。检查点名可在内部含
   `.` 与 `@`，但不得含 `/`、`\\`，也不得等于 `.` 或 `..`。
6. 挂起只阻断下游，不阻断无关旁支。
7. checkpoint pending 与 retry pending 共用下游阻断语义；completed 的独立分支和
   `cache="off"` artifact 在同 run 恢复时复用，不重新执行。

## Failure behavior

非法 `run_id` 或检查点名单段在访问对应路径前抛 `ValueError`；没有同 run 的 pending 文件时
批准请求也抛 `ValueError`。payload 摘要不一致时旧批准不被读取，再次抛出
`CheckpointPending` 并写出新的 pending 记录。被挂起节点的下游列为 skipped，无关就绪节点
继续运行；普通、map/scan 与重复挂载子图都遵守相同的隔离和恢复规则。

## Affected surfaces

- `kigumi/dag.py:198-212`
- `kigumi/dag.py:288-305`
- `kigumi/dag.py:730-938`
- `kigumi/dag.py:1514-1516`
- `kigumi/dag.py:1629-1774`
- `kigumi/dag.py:1789-1908`
- `kigumi/dag.py:1985-1986`
- `kigumi/dag.py:2088-2112`
- `kigumi/_execution.py:49-63`
- `kigumi/_execution.py:65-67`
- `kigumi/store.py:30-33`
- `kigumi/store.py:249-281`
- `kigumi/cli.py:423-431`

## Verification

锁定测试：`tests/test_dag.py::test_checkpoint_pending_approval_and_resume`、
`tests/test_dag.py::test_approval_binds_to_payload_content`、
`tests/test_dag.py::test_pending_branch_does_not_block_independent_parallel_branch`、
`tests/test_dag.py::test_run_id_must_be_a_safe_single_path_component`、
`tests/test_dag.py::test_checkpoint_names_must_be_safe_single_path_components`、
`tests/test_dag.py::test_map_checkpoint_is_namespaced_and_resumes_one_item`、
`tests/test_dag.py::test_scan_checkpoint_approval_does_not_leak_through_item_cache`、
`tests/test_subgraph.py::test_reused_subgraph_checkpoints_are_scoped_to_mounted_nodes`、
`tests/test_cli.py::test_runs_approve_diff_and_gc_commands_use_persisted_artifacts`。

```bash
uv run pytest -q tests/test_dag.py::test_checkpoint_pending_approval_and_resume tests/test_dag.py::test_approval_binds_to_payload_content tests/test_dag.py::test_pending_branch_does_not_block_independent_parallel_branch tests/test_dag.py::test_run_id_must_be_a_safe_single_path_component tests/test_dag.py::test_checkpoint_names_must_be_safe_single_path_components tests/test_dag.py::test_map_checkpoint_is_namespaced_and_resumes_one_item tests/test_dag.py::test_scan_checkpoint_approval_does_not_leak_through_item_cache tests/test_subgraph.py::test_reused_subgraph_checkpoints_are_scoped_to_mounted_nodes tests/test_cli.py::test_runs_approve_diff_and_gc_commands_use_persisted_artifacts
```

## Change policy

修改 payload 摘要、检查点限定名、审批文件生命周期、恢复 run/L3 隔离、标识符路径校验或
挂起调度规则时，必须同步更新检查点测试、本契约、`docs/adoption.md` 与 `CHANGELOG.md`。
