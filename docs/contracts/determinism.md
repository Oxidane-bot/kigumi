# 确定性字节契约

Status: Active (0.14.0)

> Agent 可以非确定；Kigumi 只承诺静态 identity、canonical task/artifact、缓存重放和证据边界
> 可审计。builder 必须由已入键事实纯推导；需要重执行时使用 refresh/off。

## Purpose

把同一逻辑结果锁成同一字节形态，使缓存命中、缓存未命中与重放路径对下游没有可见差异。

## Scope

适用于 JSON 产物、摘要、节点缓存、map 聚合、Prompt snapshot/resolution、固定 prompt
措辞，以及 L0/L1 的空响应和截断恢复。

## Source of truth

`kigumi.artifacts.canonical_json()` 是唯一 JSON 序列化，`kigumi.artifacts.sha()` 是唯一哈希入口；
节点产物 canonical 化在 `kigumi.dag`；`kigumi.transport` 只准备并执行单次 provider attempt。

## Invariants

1. `canonical_json(sort_keys/indent=2/ensure_ascii=False)` 是唯一 JSON 序列化；`artifacts.sha` 是唯一哈希入口。
2. miss 路径产物经 canonical 化后喂下游，与命中路径逐字节一致，含 map 聚合。
3. wording 常量由 golden snapshot 锁字节；改动等于换族，记入 `CHANGELOG.md`。
4. 空响应双闸：transport 单次响应为空立即抛错；缓存层拒写空，已经存在的空缓存按
   [缓存键契约](cache-key.md) 的 `CORRUPT` 规则 fail closed，不按 miss 重算。
5. 截断处理：`finish_reason=length` 立即抛 `TruncatedResponseError`；transient、empty 与 length
   都不在 transport 内 sleep 或重试。需要再次调用时，只能由 DAG `RetryPolicy` 创建下一次
   可观察、可恢复的 attempt；截断永不静默。
6. 同一 Prompt snapshot、spec、projected inputs、params、item 与 carry 必须得到逐字节相同
   `ResolvedPrompt` 和 resolution digest。base 控制插入顺序，fragment 原文逐字插入，
   material 只经 `inject()`；框架不自动补分隔符。managed request digest 还绑定 typed
   message 内容、附件 content hash 与 `ResponseSpec`，不绑定附件路径或 transport base64。
7. `preflight()` 在缓存查找和 provider 请求前估算 token、附件数量和总字节；违规抛
   `RequestTooLarge`，不得静默调用 `clip()`。
8. snapshot 在 run 开始后不可变；中途文件修改不造成节点间漂移。下一 run 观察到新字节并
   按 selected-only 缓存规则决定 hit/miss。
9. Agent session transcript 可以包含 provider/Pi 生成的非确定字段；框架不承诺 refresh/off
   重算得到相同 transcript，只承诺首次 canonical bytes 进入 blob 后，item cache hit 与后继
   carry 逐字节重放。Pi session header cwd 在边界确定性规范化为 `"."`。

## Failure behavior

单次空响应抛 `EmptyResponseError`，缓存层拒写空内容，已有空缓存按 `CORRUPT` fail closed；
单次截断抛 `TruncatedResponseError`。字节形态不一致时，锁定测试失败并阻断发布。

## Affected surfaces

- `kigumi/artifacts.py:15-23`
- `kigumi/calling.py:141-223`
- `kigumi/_execution.py:49-63`
- `kigumi/_execution.py:108-140`
- `kigumi/dag.py` 的 `Dag.run()`
- `kigumi/dag.py` 的 `Dag._consumed_view()`、`Dag._function_inputs()`、`Dag._map_entries()` 与 `Dag._execute_map()`
- `kigumi/dag.py` 的 `Dag._execute_map()`
- `kigumi/dag.py` 的 `Dag._execute_map()`、`Dag._aggregate_map_artifact()` 与 `Dag._execute_scan()`
- `kigumi/transport.py:125-177`
- `kigumi/prompt.py:18-24`

## Verification

锁定测试：`tests/test_artifacts.py::test_canonical_json_byte_stable`、
`tests/test_prompt.py::test_prompt_component_golden_snapshot`、
`tests/test_prompt.py::test_schema_format_golden_snapshot`、
`tests/test_dag.py::test_miss_and_hit_paths_feed_downstream_identical_shape`、
`tests/test_calling.py::test_poisoned_empty_cache_raises_integrity_error`、
`tests/test_calling.py::test_empty_transport_response_is_rejected_without_cache`、
`tests/test_transport.py::test_length_response_fails_after_single_send`、
`tests/test_transport.py::test_empty_response_fails_after_one_attempt`、
`tests/test_transport.py::test_transient_failure_is_typed_after_single_send`。

```bash
uv run --extra dev pytest -q tests/test_artifacts.py tests/test_prompt.py tests/test_dag.py tests/test_calling.py tests/test_transport.py
```

## Change policy

修改序列化、哈希、canonical 化位置、固定措辞或恢复上限时，必须同步更新 golden/回归测试、本契约和 `CHANGELOG.md`；影响键字节的改动按换族发布。
