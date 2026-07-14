# 输出所有权契约

Status: Active

## Purpose

阻止同一次运行中的不同生产者静默覆盖框架管理的项目物化输出，并保持缓存命中重物化可预测。

## Scope

适用于 `Dag.run()` 内普通节点、map/scan item、动态聚合 artifact 的顶层 `files` 与任意嵌套
`kigumi_blob.path`，以及 run sidecar/trace 的 `outputs` 证据。

## Source of truth

`kigumi._execution.ExecutionEnvelope` 持有单次 run 的所有权表与锁；
`kigumi.store.materialize_artifact()` 负责完整预检、路径规范化和物化。

## Invariants

1. 每个 artifact 在任何写盘前一次性预检并原子认领全部项目相对输出路径；认领线程安全。
2. 一个路径只归一个普通节点或 `node@item`；同一生产者重物化允许。map/scan 聚合只可重物化
   自己 item 已拥有的路径，不存在其他 parent/child 例外。
3. 同一 artifact 内规范化后重复路径必拒绝，包含 text/text、blob/blob 与 text/blob 冲突。
4. 路径认领按解析符号链接后的项目内目标归一化，并遵循目标文件系统的大小写与 Unicode
   名称等价规则；指向同一目标的名称别名视为重复/冲突，解析到项目根外的输出在认领和
   写盘前拒绝。不同硬链接目录项仍是不同输出路径，原子替换时各自独立。
5. 所有 sidecar 都有稳定排序的 `outputs: list[str]`，无输出时为 `[]`；`trace_run()` 原样暴露。
6. `ctx.ingest_file` 只复制外部 source 到 blob store，不移动或删除 source。GC 只管理 kigumi
   的 run、节点缓存和 blob，不拥有外部 source 或项目物化结果。
7. “输出所有权”只表示框架管理物化路径的执行排他性，不表示法律数据权利、ACL、数据库或
   通用 effects 系统。

## Failure behavior

不同生产者或同一 artifact 的重复路径抛公开 `OutputOwnershipError`，且在冲突 artifact 的
任一输出写盘前失败；已由赢家物化的内容不被失败方覆盖。非法/逃逸路径继续按既有
`TypeError`/`ValueError` 失败，blob 缺失或摘要不符继续明确失败。

## Affected surfaces

- `kigumi/errors.py:4-5`
- `kigumi/_execution.py:21-38`
- `kigumi/_execution.py:69-106`
- `kigumi/_execution.py:108-140`
- `kigumi/store.py:117-234`
- `kigumi/dag.py:307-317`
- `kigumi/dag.py:730-938`
- `kigumi/dag.py:1629-1774`
- `kigumi/dag.py:1776-1787`
- `kigumi/dag.py:1789-1908`
- `kigumi/inspect.py:17-66`
- `kigumi/inspect.py:112-164`

## Verification

锁定测试：`tests/test_output_ownership.py::test_serial_output_collision_preserves_winner_and_claims_atomically`、
`tests/test_output_ownership.py::test_parallel_output_collision_is_thread_safe`、
`tests/test_output_ownership.py::test_symlink_aliases_cannot_bypass_output_ownership`、
`tests/test_output_ownership.py::test_symlink_aliases_are_duplicate_paths_within_one_artifact`、
`tests/test_output_ownership.py::test_case_aliases_follow_target_filesystem_output_identity`、
`tests/test_output_ownership.py::test_unicode_aliases_are_duplicate_paths_within_one_artifact`、
`tests/test_output_ownership.py::test_unicode_casefold_expansion_remains_distinct_when_filesystem_does`、
`tests/test_output_ownership.py::test_distinct_hardlink_names_can_be_materialized_independently`、
`tests/test_output_ownership.py::test_symlink_output_cannot_escape_project_root`、
`tests/test_output_ownership.py::test_sibling_map_items_cannot_claim_same_output`、
`tests/test_output_ownership.py::test_sibling_scan_items_raise_public_ownership_error`、
`tests/test_output_ownership.py::test_text_and_nested_blob_duplicate_is_rejected_before_writing`、
`tests/test_output_ownership.py::test_cache_hit_can_rematerialize_output_for_same_producer`、
`tests/test_output_ownership.py::test_dynamic_aggregate_may_rematerialize_its_own_item_blob_paths`、
`tests/test_output_ownership.py::test_ingest_file_source_remains_caller_owned_after_gc`、
`tests/test_cache_policy.py::test_sidecar_and_trace_expose_outputs_and_cache_policy`。

```bash
uv run pytest -q tests/test_output_ownership.py::test_serial_output_collision_preserves_winner_and_claims_atomically tests/test_output_ownership.py::test_parallel_output_collision_is_thread_safe tests/test_output_ownership.py::test_symlink_aliases_cannot_bypass_output_ownership tests/test_output_ownership.py::test_symlink_aliases_are_duplicate_paths_within_one_artifact tests/test_output_ownership.py::test_case_aliases_follow_target_filesystem_output_identity tests/test_output_ownership.py::test_unicode_aliases_are_duplicate_paths_within_one_artifact tests/test_output_ownership.py::test_unicode_casefold_expansion_remains_distinct_when_filesystem_does tests/test_output_ownership.py::test_distinct_hardlink_names_can_be_materialized_independently tests/test_output_ownership.py::test_symlink_output_cannot_escape_project_root tests/test_output_ownership.py::test_sibling_map_items_cannot_claim_same_output tests/test_output_ownership.py::test_sibling_scan_items_raise_public_ownership_error tests/test_output_ownership.py::test_text_and_nested_blob_duplicate_is_rejected_before_writing tests/test_output_ownership.py::test_cache_hit_can_rematerialize_output_for_same_producer tests/test_output_ownership.py::test_dynamic_aggregate_may_rematerialize_its_own_item_blob_paths tests/test_output_ownership.py::test_ingest_file_source_remains_caller_owned_after_gc tests/test_cache_policy.py::test_sidecar_and_trace_expose_outputs_and_cache_policy
```

## Change policy

修改认领粒度、生产者例外、路径规范化、sidecar outputs 或 GC/source 边界时，必须先更新
锁定测试，再同步本契约、`DESIGN.md`、`docs/adoption.md` 与 `CHANGELOG.md`。
