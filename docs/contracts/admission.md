# 执行准入契约

Status: Active (0.10.1)

## Purpose

在节点执行或付费 provider 请求开始前，先完成进程内的预算与资源准入；并发调度不能
因为 map/scan 的动态展开而绕过同一组资源上限。

## Scope

适用于 `Budget`/`BudgetPermit`、`LLMCaller(budget=...)`、`ResourceRequest`、
`Dag.run(resource_limits=...)`，以及普通节点、map/scan/foreach 和 Agent 节点的
run-local 调度。

## Source of truth

预算准入由 `kigumi.calling.Budget` 与 `LLMCaller.call()` 提供；资源准入由
`kigumi._declarations.ResourceRequest`、`Dag._build_permit_plane()` 与
`kigumi.dag._PermitPlane` 提供。

## Public surface

`Budget.reserve()` 返回 `BudgetPermit`；permit 的成功路径调用 `commit(actual_usage)`，
失败或取消路径调用 `cancel()`。节点用 `resources=(ResourceRequest(name, units),)`
声明资源，run 用 `resource_limits={name: limit}` 给出上限；没有资源声明的节点使用
`None` 默认池。`ResourceRequest.scope` 只接受 `host`、`account`、`global` 三个声明值；
它不会把这个 run-local permit plane 变成跨进程或跨主机锁。

## Invariants

1. L1 cache hit 在 provider 前返回且不预留预算。L1 miss 必须在 provider 请求前以
   `Budget.reserve(estimated_tokens)` 完成 admission；预留同时考虑已花费与其他活动预留，
   不足时抛 `BudgetExceeded`，provider 不得被调用。
2. 成功响应按 provider 返回的 `total_tokens` 调用 `commit(actual_usage)`，把实际用量记入
   `Budget.spent`；失败、空响应、取消或写入前的异常调用 `cancel()` 释放预留。估算是
   best-effort，实际用量可以超过预留；此时 commit 记录实际用量后仍可抛 `BudgetExceeded`，
   不能把已发生的 provider effect 当成未发生。
3. 预算 admission 只在当前进程内协调；它不是跨进程、跨主机或分布式 quota。需要这些
   边界时，调用方必须另配 file lock、`FileSlots` 或外部 quota；不能把 `Budget` 当总账。
4. map、scan、foreach 的预算不足保持 `BudgetExceeded`，停止尚未 admission 的后续 item；
   已经 admission 的 item 按自己的成功/失败路径收尾，不把预算拒绝伪装成普通 item failure。
5. 一次 `Dag.run()` 建立一个 run-wide permit plane。未声明资源的节点与其他未声明节点
   竞争 `None` 池；同名资源按累计 `units` 竞争同一个池；不同命名资源可以按各自上限并行。
   map/scan item 与就绪的普通节点共用该 plane，不再在 scheduler worker 内创建嵌套的
   `workers` 池。
6. `resource_limits` 中的上限必须是正整数；没有显式上限的已使用资源默认以 `workers`
   为上限；单个节点合计请求超过对应上限时，run 在执行前拒绝。资源声明的 cache identity
   由[缓存键契约](cache-key.md)约束。
7. 多资源请求按资源名固定顺序取得，取得失败会释放已取得的部分；节点正常返回或抛错后
   都释放全部 permit。资源池是 run-local、进程内边界，不能替代跨进程 Agent slot、
   provider capacity 或分布式 quota。

## Failure behavior

预算预留不足抛 `BudgetExceeded` 且不发 provider 请求；实际用量超预算的 `commit` 仍保留
调用记录和已写入的成功响应后抛出。非法资源声明或 `resource_limits` 抛 `TypeError`/
`ValueError`；资源请求超过上限在执行前失败，资源等待超时或节点异常不会泄漏 permit。

## Verification

锁定测试见 `tests/test_budget_admission.py`、`tests/test_resource_limits.py`、
`tests/test_calling.py::test_cache_hit_skips_transport_and_budget` 与
`tests/test_dag_cache_keys.py::test_resource_declarations_do_not_change_cache_key`。

```bash
uv run --extra dev pytest -q tests/test_budget_admission.py tests/test_resource_limits.py tests/test_calling.py::test_cache_hit_skips_transport_and_budget tests/test_dag_cache_keys.py::test_resource_declarations_do_not_change_cache_key
```

## Change policy

修改预算 reserve/commit/cancel 顺序、资源池归属、map/scan 共享 plane 或资源是否进入 cache
key 时，必须先更新锁定测试，再同步本契约、`docs/adoption.md`、`docs/api.md` 与
`docs/capabilities.md`；若改变 cache key 成分，还须按缓存键契约更新 `CACHE_SCHEMA` 和
`CHANGELOG.md`。
