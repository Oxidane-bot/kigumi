# 缓存与产物保留契约

Status: Active

> retained run 中的 `kigumi_attachment`（含 trajectory evidence）与 `kigumi_blob` 分别扫描、
> 共同保护同一内容寻址 blob 仓；这不改变 materializable blob 语义。

## Purpose

回收不可达缓存和 blob，同时保证保留窗口内的 run 仍能解释、复用并重新物化其声明的交付物。

## Scope

适用于 `gc_cache()`、`gc_artifacts()`、run sidecar、map 聚合 sidecar、blob store 与缓存命中后的物化路径。

## Source of truth

保留集由 `kigumi.store.gc_cache()` 和 `_referenced_blob_digests()` 构造；run 排序由
`kigumi.store.run_sort_key()` 定义；blob 物化由 `kigumi.blobs.BlobStore.materialize()` 执行。

## Invariants

1. `gc_cache` 扫描每个被保留 run 目录下所有 `*.json.meta.json`；sidecar 的 `cache_key`
   为字符串则该键入保留集，为字符串列表则全部入保留集。任何入保留集的键对应缓存不删。
2. map item 键的契约来源是聚合 sidecar 的 `cache_key` 列表；逐项 sidecar 的字符串键是刻意冗余——聚合 sidecar 缺失或损坏时，逐项 sidecar 仍足以保住 item 缓存，不得误删。
3. 无法解析或非 `dict` 的 sidecar 跳过，不影响其他 sidecar 的保留贡献；跳过意味着其引用的键可能被删，这是 fail-open 的已知边界。
4. run 目录排序数字感知（`store.run_sort_key`），保留窗、blob 引用、explain 最新 run 三处一致。
5. blob 物化前必须重验摘要；缓存命中也重物化声明的交付物。
6. `cache="off"` sidecar 仍记录确定性 cache_key 供 provenance，但对应 L3 条目不存在；GC
   对不存在的引用无副作用。refresh 写入的条目按普通 run 引用保留。
7. GC 的管理边界只包含 kigumi 的 node cache、blob 与 run 数据；当前回收只删除不可达
   node cache/blob，不删除 run 目录、项目物化路径，也不移动/删除 `ctx.ingest_file` 的外部 source。
8. blob reachability 递归扫描 retained run 的 sidecar、failure、attempt receipt、resolution 与
   success candidate JSON。`kigumi_attachment` 和 `kigumi_blob` 引用都保护同一 blob；
   `hash_only` descriptor 没有 blob 引用，因此不会虚构 retained 内容。

## Failure behavior

无效 sidecar 被跳过而不阻断 GC；未被任何可读 sidecar 引用的缓存可删除。off sidecar
指向不存在的 L3 键不报错。blob 摘要不匹配抛 `ValueError`，声明的 blob 缺失抛带节点与
摘要的 `FileNotFoundError`；缓存命中不会跳过物化，外部 ingest source 不受 GC 影响。

## Affected surfaces

- `kigumi/store.py:83-86`
- `kigumi/store.py:117-234`
- `kigumi/store.py:284-403`
- `kigumi/blobs.py:65-94`
- `kigumi/_execution.py:40-47`
- `kigumi/_execution.py:49-63`
- `kigumi/_execution.py:69-79`
- `kigumi/_execution.py:108-140`
- `kigumi/dag.py:730-938`
- `kigumi/dag.py:1185-1210`
- `kigumi/dag.py:1629-1774`
- `kigumi/dag.py:1789-1908`

## Verification

锁定测试：`tests/test_store.py::test_gc_retains_numeric_latest_run_and_its_blob_references`、
`tests/test_dag.py::test_map_gc_retains_item_cache_keys_referenced_by_aggregate`、
`tests/test_dag.py::test_map_item_sidecars_retain_item_cache_without_aggregate_sidecar`、
`tests/test_dag.py::test_gc_skips_invalid_sidecars_without_losing_normal_references`、
`tests/test_dag.py::test_cache_hit_materializes_files_and_runs_post_node`、
`tests/test_dag.py::test_blob_cache_hit_rematerializes_deleted_binary_output`、
`tests/test_dag.py::test_gc_keeps_blobs_referenced_by_the_latest_run`、
`tests/test_dag.py::test_explain_without_run_id_uses_numeric_latest_run`、
`tests/test_output_ownership.py::test_ingest_file_source_remains_caller_owned_after_gc`。

```bash
uv run pytest -q tests/test_store.py::test_gc_retains_numeric_latest_run_and_its_blob_references tests/test_dag.py::test_map_gc_retains_item_cache_keys_referenced_by_aggregate tests/test_dag.py::test_map_item_sidecars_retain_item_cache_without_aggregate_sidecar tests/test_dag.py::test_gc_skips_invalid_sidecars_without_losing_normal_references tests/test_dag.py::test_cache_hit_materializes_files_and_runs_post_node tests/test_dag.py::test_blob_cache_hit_rematerializes_deleted_binary_output tests/test_dag.py::test_gc_keeps_blobs_referenced_by_the_latest_run tests/test_dag.py::test_explain_without_run_id_uses_numeric_latest_run tests/test_output_ownership.py::test_ingest_file_source_remains_caller_owned_after_gc
```

## Change policy

修改保留集来源、sidecar 容错、run 排序、blob 引用或物化校验时，必须同步更新 GC/物化测试、本契约、`docs/adoption.md` 与 `CHANGELOG.md`。
