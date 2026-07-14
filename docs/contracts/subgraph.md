# 静态子图契约

Status: Active

## Purpose

让多阶段 DAG 结构可声明一次、在不同 namespace 重复挂载，同时保持原有调度、缓存、审批和
可观测边界唯一。

## Scope

适用于 `Subgraph` 的 inputs/outputs/node/map/scan 声明、`Dag.mount()`、挂载节点函数输入、
注册守卫、describe/render、plan/run/explain/check 与重复挂载。

## Source of truth

`kigumi.subgraph.Subgraph` 只保存声明；`kigumi.dag.Dag.mount()` 唯一负责解析、验证、冻结和
事务性写入现有 `_nodes` 注册表。

## Invariants

1. namespace、input/output port、本地节点名均为不含 `.`、`@`、`/`、`\\` 的单个非空段；
   输入绑定键必须与声明端口完全一致，值指向已注册的外层 Dag 节点；本地节点名不得遮蔽
   input port。
2. 本地 dependency/items source/carry source 只可指向声明 input 或本地节点；output target
   只可指向本地节点。完整 mount 在任何注册表修改前验证名称、引用、环、冲突和注册守卫。
3. qualified 名固定为 `namespace.local_name`；函数看到本地 dependency 键。map 的 item source
   与 scan 的 item/carry source 仍按既有语义从函数 inputs 省略。
4. 模板首次成功 mount 后冻结，可在不同 namespace 重复 mount；失败 mount 不留下部分节点。
5. 挂载保留原始用户函数对象，AST 守卫/metadata 与普通节点一致，source 哈希不经过 wrapper。
   上游键成分按本地端口语义标注，因此反向绑定或同一外层节点承担多个本地角色都不会混淆。
6. 子图复用同一 scheduler、L1/L3 cache、store、checkpoint 与 views；没有第二调度器/第二缓存、
   nesting、recursion 或任意 executable expansion plan。
7. 拓扑始终静态。模型只决定内容，不决定可执行图；运行时动态展开只允许既有 map/scan，
   foreach 仍在注册期固化。
8. 重复挂载把检查点身份限定到 qualified 挂载节点：普通节点使用
   `approval@namespace.local`，map/scan 项再追加 `@item`；不同挂载可独立挂起和批准。

## Failure behavior

非法段、缺失/多余 binding、未知本地引用/输出、环、重复 namespace 或 qualified 冲突均抛
`ValueError` 且 Dag 注册表不发生部分修改。冻结后的 decorator mutation 抛 `RuntimeError`。

## Affected surfaces

- `kigumi/_declarations.py:30-40`
- `kigumi/subgraph.py:23-225`
- `kigumi/dag.py:288-305`
- `kigumi/dag.py:597-728`
- `kigumi/dag.py:730-938`
- `kigumi/dag.py:1327-1356`
- `kigumi/dag.py:1572-1584`
- `kigumi/dag.py:1629-1774`
- `kigumi/dag.py:1776-1908`
- `kigumi/dag.py:1910-1970`
- `kigumi/dag.py:2157-2164`
- `kigumi/views.py:12-65`
- `kigumi/views.py:262-266`

## Verification

锁定测试：`tests/test_subgraph.py::test_two_stage_subgraph_wiring_local_keys_and_output_binding`、
`tests/test_subgraph.py::test_subgraph_cache_keys_preserve_local_port_roles`、
`tests/test_subgraph.py::test_subgraph_dynamic_source_alias_keeps_other_local_role_in_key`、
`tests/test_subgraph.py::test_frozen_subgraph_mounts_twice_and_rejects_later_mutation`、
`tests/test_subgraph.py::test_subgraph_rejects_local_node_that_shadows_an_input_port`、
`tests/test_subgraph.py::test_subgraph_reused_decorator_cannot_overwrite_local_node`、
`tests/test_subgraph.py::test_mount_rejects_bindings_refs_outputs_and_namespace_transactionally`、
`tests/test_subgraph.py::test_mount_collision_does_not_partially_register`、
`tests/test_subgraph.py::test_subgraph_rejects_invalid_single_segments`、
`tests/test_subgraph.py::test_reused_subgraph_checkpoints_are_scoped_to_mounted_nodes`、
`tests/test_subgraph.py::test_subgraph_map_scan_use_existing_scheduler_and_all_views`、
`tests/test_cache_policy.py::test_subgraph_cache_policy_is_validated_at_declaration`。

```bash
uv run pytest -q tests/test_subgraph.py::test_two_stage_subgraph_wiring_local_keys_and_output_binding tests/test_subgraph.py::test_subgraph_cache_keys_preserve_local_port_roles tests/test_subgraph.py::test_subgraph_dynamic_source_alias_keeps_other_local_role_in_key tests/test_subgraph.py::test_frozen_subgraph_mounts_twice_and_rejects_later_mutation tests/test_subgraph.py::test_subgraph_rejects_local_node_that_shadows_an_input_port tests/test_subgraph.py::test_subgraph_reused_decorator_cannot_overwrite_local_node tests/test_subgraph.py::test_mount_rejects_bindings_refs_outputs_and_namespace_transactionally tests/test_subgraph.py::test_mount_collision_does_not_partially_register tests/test_subgraph.py::test_subgraph_rejects_invalid_single_segments tests/test_subgraph.py::test_reused_subgraph_checkpoints_are_scoped_to_mounted_nodes tests/test_subgraph.py::test_subgraph_map_scan_use_existing_scheduler_and_all_views tests/test_cache_policy.py::test_subgraph_cache_policy_is_validated_at_declaration
```

## Change policy

修改名称/路径段规则、绑定规则、冻结点、事务边界、本地输入映射、挂载检查点作用域、source
哈希或运行时动态拓扑边界时，必须先更新锁定测试，再同步本契约、`DESIGN.md`、
`docs/adoption.md` 与 `CHANGELOG.md`。
