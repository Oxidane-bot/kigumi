# 确定性字节契约

Status: Active

> Agent 可以非确定；Kigumi 只承诺静态 identity、canonical task/artifact、缓存重放和证据边界
> 可审计。builder 必须由已入键事实纯推导；需要重执行时使用 refresh/off。

## Purpose

把同一逻辑结果锁成同一字节形态，使缓存命中、缓存未命中与重放路径对下游没有可见差异。

## Scope

适用于 JSON 产物、摘要、节点缓存、map 聚合、固定 prompt 措辞，以及 L0/L1 的空响应和截断恢复。

## Source of truth

`kigumi.artifacts.canonical_json()` 是唯一 JSON 序列化，`kigumi.artifacts.sha()` 是唯一哈希入口；
节点产物 canonical 化在 `kigumi.dag`，恢复策略在 `kigumi.transport`。

## Invariants

1. `canonical_json(sort_keys/indent=2/ensure_ascii=False)` 是唯一 JSON 序列化；`artifacts.sha` 是唯一哈希入口。
2. miss 路径产物经 canonical 化后喂下游，与命中路径逐字节一致，含 map 聚合。
3. wording 常量由 golden snapshot 锁字节；改动等于换族，记入 `CHANGELOG.md`。
4. 空响应双闸：传输层重试后仍空则抛错；缓存层拒写空、读到空缓存按 miss。
5. 截断处理：`finish_reason=length` 时只有调用方设了 `max_tokens` 才加倍重试，至多 2 次；否则直接抛 `TruncatedResponseError`；截断永不静默。

## Failure behavior

空响应耗尽时抛 `EmptyResponseError`，缓存层遇到空内容拒写或重算；无 `max_tokens` 的截断及两次扩容后仍截断均抛 `TruncatedResponseError`。字节形态不一致时，锁定测试失败并阻断发布。

## Affected surfaces

- `kigumi/artifacts.py:15-23`
- `kigumi/calling.py:141-223`
- `kigumi/_execution.py:49-63`
- `kigumi/_execution.py:108-140`
- `kigumi/dag.py:730-938`
- `kigumi/dag.py:1629-1774`
- `kigumi/dag.py:1776-1787`
- `kigumi/dag.py:1789-1908`
- `kigumi/transport.py:125-177`
- `kigumi/prompt.py:18-24`

## Verification

锁定测试：`tests/test_artifacts.py::test_canonical_json_byte_stable`、
`tests/test_prompt.py::test_prompt_component_golden_snapshot`、
`tests/test_prompt.py::test_schema_format_golden_snapshot`、
`tests/test_dag.py::test_miss_and_hit_paths_feed_downstream_identical_shape`、
`tests/test_calling.py::test_poisoned_empty_cache_treated_as_miss`、
`tests/test_calling.py::test_empty_transport_response_is_rejected_without_cache`、
`tests/test_transport.py::test_length_finish_doubles_max_tokens`、
`tests/test_transport.py::test_length_without_max_tokens_returns_as_is`、
`tests/test_transport.py::test_length_retry_exhaustion_raises`、
`tests/test_transport.py::test_empty_response_exhaustion_raises_with_backoff`。

```bash
uv run pytest -q tests/test_artifacts.py::test_canonical_json_byte_stable tests/test_prompt.py::test_prompt_component_golden_snapshot tests/test_prompt.py::test_schema_format_golden_snapshot tests/test_dag.py::test_miss_and_hit_paths_feed_downstream_identical_shape tests/test_calling.py::test_poisoned_empty_cache_treated_as_miss tests/test_calling.py::test_empty_transport_response_is_rejected_without_cache tests/test_transport.py::test_length_finish_doubles_max_tokens tests/test_transport.py::test_length_without_max_tokens_returns_as_is tests/test_transport.py::test_length_retry_exhaustion_raises tests/test_transport.py::test_empty_response_exhaustion_raises_with_backoff
```

## Change policy

修改序列化、哈希、canonical 化位置、固定措辞或恢复上限时，必须同步更新 golden/回归测试、本契约和 `CHANGELOG.md`；影响键字节的改动按换族发布。
