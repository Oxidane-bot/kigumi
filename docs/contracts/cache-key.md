# 缓存键契约

Status: Active

## Purpose

让同一语义输入稳定复用缓存，让任一会改变结果的输入改动都换键，避免陈旧结果静默回放。

## Scope

适用于 L1 `LLMCaller.call()`，以及 L3 `Dag`/`Subgraph` 的普通节点、map 项、scan 项、
cache policy、external fingerprint、run、plan 和 explain 入口。

## Source of truth

L1 键由 `kigumi.calling.LLMCaller.call()` 构造；L3 成分唯一由
`kigumi.dag.Dag._key_components()` 推导。

## Invariants

1. L1 键等于 `sha({messages, model=resolved 后模型, params(调用方原样,传输层归一化不回写), seed})`；`seed` 只是键命名空间，不发给供应商。
2. L3 成分标签固定为 `source`、`libs`、`upstream:<dep>`、`prompts:<t>`、`files:<p>`、
   `params`、`item`、`item_files:<p>`、`carry`、`kigumi`，声明外部指纹时额外且仅额外出现
   `external=sha(external_fingerprint)`；普通依赖边默认取完整上游产物摘要，声明
   `consumes[dep]` 时同一 `upstream:<dep>` 标签改取 canonical 投影视图摘要，不新增标签；
   推导单点在 `dag._key_components`，原始指纹不落盘。
3. `items_from` 与 scan 的 `carry_from` 源不入共享 `upstream`；`item` 按内容入键；`carry` 只按
   本项实收内容入键，`carry_fn` 源码不入键。消费投影的源码同样不入键；节点函数收到 canonical
   JSON round-trip 后的投影视图而非完整上游产物，未声明投影的依赖输入形态不变。
4. `source` 与 `libs` 都按剥除 docstring/注释后的 AST 哈希；`libs` 的语法残破文件退回原文。
5. `cache="auto"|"refresh"|"off"` 只控制 L3 读写，不是键成分；force 只旁路本次读取。
   refresh/off 仍计算确定性 key components/cache_key 供 provenance 与 explain。L1 不变。
6. `kigumi` 成分等于 `sha({prompt_source, schema=CACHE_SCHEMA=3, pydantic})`；其中
   `prompt_source` 是按文件名固定排序的 `prompt.py`、`repair.py` 文件字节哈希联合值，
   不含发行版本号。
7. 改变键成分推导、prompt 生成字节语义或 artifact 规范化形态时必须递增
   `CACHE_SCHEMA`；生成字节模块集合（当前为 `prompt.py`、`repair.py`）成员变化视同键成分变化。
8. 键成分任何变化等于全项目缓存换族，必须记入 `CHANGELOG.md`。0.2.0 将
   `CACHE_SCHEMA` 从 1 升至 2，是为可选 external 成分进行的有意完整 L3 换族。
   0.3.0 将 schema 从 2 升至 3，是为普通依赖边的可选消费投影进行的有意完整换族。

## Failure behavior

键成分不同会得到不同摘要并按缓存未命中处理；空、撕裂或无效缓存按 miss 重算。未在
`CHANGELOG.md` 记录的键成分演进不得进入发布件。非法 cache 值或不可 canonical JSON
序列化的 external fingerprint 在注册期抛 `ValueError`。

## Affected surfaces

- `kigumi/calling.py:141-223`
- `kigumi/_declarations.py:9-27`
- kigumi/dag.py 的 `CACHE_SCHEMA` 与 `_kigumi_key_inputs`
- kigumi/dag.py 的 `Dag._key_components` 与 `Dag._libs_hash`
- kigumi/dag.py 的 `_module_code_text`
- `kigumi/artifacts.py:15-23`

## Verification

锁定测试：`tests/test_calling.py::test_cache_key_ignores_param_order`、
`tests/test_calling.py::test_resolved_model_changes_cache_key_and_provenance`、
`tests/test_calling.py::test_seed_changes_cache_key`、
`tests/test_dag.py::test_docstring_does_not_change_cache_but_code_does`、
`tests/test_dag.py::test_kigumi_component_tracks_repair_bytes_and_uses_schema`、
`tests/test_dag.py::test_key_components_lock_exact_label_set`、
`tests/test_cache_policy.py::test_key_component_labels_add_only_external_when_supplied`、
`tests/test_cache_policy.py::test_external_fingerprint_changes_owner_then_downstream_and_uses_exact_digest`、
`tests/test_cache_policy.py::test_cache_policy_repeated_runs_and_plan`、
`tests/test_cache_policy.py::test_map_item_cache_policy_executes_every_item_and_plan_reports_miss`、
`tests/test_cache_policy.py::test_scan_explain_without_initial_carry_uses_run_key_components`、
`tests/test_dag.py::test_kigumi_component_tracks_prompt_bytes_and_pydantic_version`、
`tests/test_dag.py::test_explain_records_key_components_and_reports_one_changed_input`、
`tests/test_dag.py::test_scan_carry_fn_code_is_irrelevant_when_extracted_content_is_equal`、
`tests/test_dag.py::test_scan_carry_from_content_invalidates_the_whole_chain`、
`tests/test_dag.py::test_libs_hash_ignores_comment_and_docstring_edits`、
`tests/test_dag.py::test_libs_hash_tolerates_broken_syntax_by_hashing_raw_text`，以及
`tests/test_consumes.py` 中对投影键、输入隔离、plan/run/explain、动态节点、Subgraph、
注册校验、错误上下文、标签集与 schema 的锁定测试。

```bash
uv run pytest -q tests/test_consumes.py tests/test_calling.py::test_cache_key_ignores_param_order tests/test_calling.py::test_resolved_model_changes_cache_key_and_provenance tests/test_calling.py::test_seed_changes_cache_key tests/test_dag.py::test_docstring_does_not_change_cache_but_code_does tests/test_dag.py::test_kigumi_component_tracks_repair_bytes_and_uses_schema tests/test_dag.py::test_key_components_lock_exact_label_set tests/test_cache_policy.py::test_key_component_labels_add_only_external_when_supplied tests/test_cache_policy.py::test_external_fingerprint_changes_owner_then_downstream_and_uses_exact_digest tests/test_cache_policy.py::test_cache_policy_repeated_runs_and_plan tests/test_cache_policy.py::test_map_item_cache_policy_executes_every_item_and_plan_reports_miss tests/test_cache_policy.py::test_scan_explain_without_initial_carry_uses_run_key_components tests/test_dag.py::test_kigumi_component_tracks_prompt_bytes_and_pydantic_version tests/test_dag.py::test_explain_records_key_components_and_reports_one_changed_input tests/test_dag.py::test_scan_carry_fn_code_is_irrelevant_when_extracted_content_is_equal tests/test_dag.py::test_scan_carry_from_content_invalidates_the_whole_chain tests/test_dag.py::test_libs_hash_ignores_comment_and_docstring_edits tests/test_dag.py::test_libs_hash_tolerates_broken_syntax_by_hashing_raw_text
```

## Change policy

修改键成分、哈希归一化、`CACHE_SCHEMA` 或其推导位置时，必须同步更新锁定测试、本契约、
`DESIGN.md` 中的缓存说明和 `CHANGELOG.md` 的换族记录。
