# 缓存键契约

Status: Active (0.9.0)

> Unreleased：`CACHE_SCHEMA=7`，node cache envelope schema 3 保持不变；
> `agent_executor_schema=5`。这是 Agent scan/session canonical artifact 的完整 L3 cache
> 硬切，不迁移 0.7.x 条目。
> EvidencePolicy、RetryPolicy 与 Agent capacity 不进入内容键；前两者绑定 run/origin identity。
> 本次 `libs` 按节点静态 import 闭包细化，沿用这个尚未发布的 7 轮换，不再把
> `CACHE_SCHEMA` 递增为 8。

## Purpose

让同一语义输入稳定复用缓存，让任一会改变结果的输入改动都换键，避免陈旧结果静默回放。

## Scope

适用于 L1 `LLMCaller.call()`，以及 L3 `Dag`/`Subgraph` 的普通节点、map 项、scan 项、
cache policy、external fingerprint、run、plan 和 explain 入口。

## Source of truth

L1 键由 `kigumi.calling.LLMCaller.call()` 构造；L3 成分唯一由
`kigumi.dag.Dag._key_components()` 推导。

## Invariants

1. L1 键等于 `sha({messages, model=resolved 后模型, params(调用方原样,传输层归一化不回写), seed})`；当请求带有非默认 `ResponseSpec` 时，键额外绑定其格式与 `schema_sha256`；`seed` 只是键命名空间，不发给供应商。
2. L3 成分标签固定为 `source`、`libs`、`upstream:<dep>`、`prompts:<t>`、
   `prompt_specs:<name>`、`files:<p>`、`params`、`item`、`item_files:<p>`、`carry`、
   `kigumi`，声明外部指纹时额外且仅额外出现
   `external=sha(external_fingerprint)`；普通依赖边默认取完整上游产物摘要，声明
   `consumes[dep]` 时同一 `upstream:<dep>` 标签改取 canonical 投影视图摘要，不新增标签；
   推导单点在 `dag._key_components`，原始指纹不落盘。
3. `items_from` 与 scan 的 `carry_from` 源不入共享 `upstream`；`item` 按内容入键；`carry` 只按
   本项实收内容入键，`carry_fn` 源码不入键。消费投影的源码同样不入键；节点函数收到 canonical
   JSON round-trip 后的投影视图而非完整上游产物，未声明投影的依赖输入形态不变。
4. `source` 与 `libs` 都按剥除 docstring/注释后的 AST 哈希；`libs` 只纳入从节点函数所属
   模块出发、在 `config.source_paths` 内由静态 import 图可达的文件，并按传递闭包计算。
   静态分析只接受模块顶层、无歧义的 import；配置源码快照中任一文件语法残破、节点模块
   无法定位或读取、可达模块无法解析，或发现条件/嵌套 import、`importlib` 及其子模块/动态导入调用、
   已识别的动态可调用引用（无论是否调用，也不论赋值目标是简单名、walrus、属性、下标/容器、
   解构或链式赋值）、动态导入别名、star-import、模块级 `__getattr__`、相对导入无法解析、同一导入对应多个
   配置源码候选，或已加载模块的 `__file__` 偏离静态候选且仍落在项目/配置源码范围内等
   不确定情形时，该节点退回当前全文件 digest。模块 AST 中出现常见反射原语
   （`getattr`、`globals`、`locals`、`vars`、`__dict__`、`__getattribute__`、
   `__builtins__`、显式 `builtins` 导入或模块注册表属性如 `sys.modules`）也一律回退，
   即使反射与导入无关，或被查找的名称/键是计算出来的；不做常量传播。无配置源码候选且
   已能证明属于标准库、内置模块或项目外已加载模块的绝对 import 不纳入 `libs`。
   这种保守 false positive 是有意的：扩大输入可以多失效，但不能用未覆盖的反射路径复用
   陈旧产物。该规则是源码 AST 分析边界，不宣称覆盖整个 Python 运行时或任意外部/native
   代码。
5. `cache="auto"|"refresh"|"off"` 只控制 L3 读写，不是键成分；force 只旁路本次读取。
   refresh/off 仍计算确定性 key components/cache_key 供 provenance 与 explain。L1 不变。
6. `kigumi` 成分等于 `sha({prompt_source, schema=CACHE_SCHEMA=7, pydantic})`；其中
   `prompt_source` 是按文件名固定排序的 `prompt.py`、`repair.py` 文件字节哈希联合值，
   不含发行版本号。
7. 改变键成分推导、prompt 生成字节语义或 artifact 规范化形态时原则上必须递增
   `CACHE_SCHEMA`；生成字节模块集合（当前为 `prompt.py`、`repair.py`）成员变化视同键成分变化。
   若当前值只是尚未发布的 Unreleased 轮换，变更必须搭载该轮换，不得为同一批未发布缓存
   再造一次递增。
8. 键成分任何变化等于全项目缓存换族，必须记入 `CHANGELOG.md`。0.2.0 将
   `CACHE_SCHEMA` 从 1 升至 2，是为可选 external 成分进行的有意完整 L3 换族。
   0.3.0 将 schema 从 2 升至 3，是为普通依赖边的可选消费投影进行的有意完整换族。
   0.6.0 将 schema 从 3 升至 4，并将 cache envelope 升至 schema 2，以绑定 immutable
   origin provenance、Agent schema 2 和 evidence miss 语义。0.7.0 再从 4 升至 5，并把
   envelope 升至 schema 3，以引入声明式 Prompt resolution、selected-only L3 成分和
   hash-bound origin。0.8.0 从 5 升至 6，以引入 `agent_schema=3` 的 session attachment
   与 Agent scan executor 语义。Unreleased 从 6 升至 7，以绑定 managed request 的
   attachment content hash、typed message digest 和 response schema identity；本次 `libs`
   细化搭载同一未发布的 7 轮换，不新增第 8 次全项目换族。
9. `prompt_specs:<name>` 取当前 resolution digest：包含 spec/binding 结构、base、固定 layer、
   axis 实际 selection 与所选 fragment、material digest 和 rendered digest；不包含未选中
   variant 的内容 digest。resolution digest 还绑定 typed message 内容、附件 content hash
   与 `ResponseSpec` schema identity，不绑定附件原始路径或 transport base64。 同节点声明的所有 PromptSpec 都保守入键，即使本次函数未调用。
   未选中候选的完整字节 universe 只进入 run manifest graph identity，因此改它可复用相同
   selected-only L3 条目，但旧 run 因声明 identity 漂移拒绝 resume。未声明的字符串 CALL
   不伪造 PromptSpec 成分，receipt 只记录为 unmanaged。
10. node cache envelope schema 3 固定保存 canonical artifact、artifact SHA-256、首次执行的
    immutable origin provenance 与 origin digest。warm hit 不得以 replay metadata 覆盖 origin。
11. EvidencePolicy digest 不匹配按 evidence miss 执行，但不改变 key components；RetryPolicy
    digest、Agent slots/lock/timeout、`ResourceRequest` 与 `resource_limits` 也不属于内容键。

## Failure behavior

键成分不同会得到不同摘要并按缓存未命中处理；不存在的缓存才是普通 miss。已经存在但为空、
撕裂、摘要不匹配或 schema/provenance 无效的缓存属于 `CORRUPT`，必须 fail closed，不能按
miss 重算或静默重新计费；`CacheLookup` 保留 `MISSING`、`VALID`、`CORRUPT` 三态。未在
`CHANGELOG.md` 记录的键成分演进不得进入发布件。非法 cache 值或不可 canonical JSON
序列化的 external fingerprint 在注册期抛 `ValueError`。`libs` 静态分析遇到上述无法证明的
情况时必须使用当前全文件 digest，不得猜测较小闭包；全文件 digest 仍沿用
`_module_code_text`，语法残破文件使用原文。

## Affected surfaces

- `kigumi/calling.py:141-223`
- `kigumi/_declarations.py:9-27`
- kigumi/dag.py 的 `CACHE_SCHEMA` 与 `_kigumi_key_inputs`
- kigumi/dag.py 的 `Dag._key_components` 与 `Dag._libs_hash`
- kigumi/dag.py 的 per-node 静态 import 闭包与全文件 fallback
- kigumi/dag.py 的 `_module_code_text`
- `kigumi/artifacts.py:15-23`

## Verification

锁定测试：`tests/test_calling.py::test_cache_key_ignores_param_order`、
`tests/test_calling.py::test_resolved_model_changes_cache_key_and_provenance`、
`tests/test_calling.py::test_seed_changes_cache_key`、
`tests/test_dag_cache_keys.py::test_docstring_does_not_change_cache_but_code_does`、
`tests/test_dag_cache_keys.py::test_kigumi_component_tracks_repair_bytes_and_uses_schema`、
`tests/test_dag_cache_keys.py::test_key_components_lock_exact_label_set`、
`tests/test_cache_policy.py::test_key_component_labels_add_only_external_when_supplied`、
`tests/test_cache_policy.py::test_external_fingerprint_changes_owner_then_downstream_and_uses_exact_digest`、
`tests/test_cache_policy.py::test_cache_policy_repeated_runs_and_plan`、
`tests/test_cache_policy.py::test_map_item_cache_policy_executes_every_item_and_plan_reports_miss`、
`tests/test_cache_policy.py::test_scan_explain_without_initial_carry_uses_run_key_components`、
`tests/test_dag_cache_keys.py::test_kigumi_component_tracks_prompt_bytes_and_pydantic_version`、
`tests/test_dag_plan_explain.py::test_explain_records_key_components_and_reports_one_changed_input`、
`tests/test_dag_scan.py::test_scan_carry_fn_code_is_irrelevant_when_extracted_content_is_equal`、
`tests/test_dag_scan.py::test_scan_carry_from_content_invalidates_the_whole_chain`、
`tests/test_dag_cache_keys.py::test_libs_hash_ignores_comment_and_docstring_edits`、
`tests/test_dag_cache_keys.py::test_libs_hash_tolerates_broken_syntax_by_hashing_raw_text`、
`tests/test_dag_cache_keys.py::test_libs_hash_follows_transitive_imports_per_node`、
`tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_ambiguous_imports`、
`tests/test_dag_cache_keys.py::test_libs_hash_falls_back_when_node_module_is_unknown`、
`tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_multiple_configured_candidates`、
`tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_loaded_runtime_module_mismatch`、
`tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_aliased_dynamic_imports`、
`tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_importlib_submodule_alias`、
`tests/test_dag_cache_keys.py::test_libs_hash_tracks_ancestor_package_init`、
`tests/test_dag_cache_keys.py::test_libs_hash_tracks_unresolved_import_inside_source_tree`，以及
`tests/test_consumes.py` 中对投影键、输入隔离、plan/run/explain、动态节点、Subgraph、
注册校验、错误上下文、标签集与 schema 的锁定测试。

```bash
uv run pytest -q tests/test_consumes.py tests/test_calling.py::test_cache_key_ignores_param_order tests/test_calling.py::test_resolved_model_changes_cache_key_and_provenance tests/test_calling.py::test_seed_changes_cache_key tests/test_dag_cache_keys.py::test_docstring_does_not_change_cache_but_code_does tests/test_dag_cache_keys.py::test_kigumi_component_tracks_repair_bytes_and_uses_schema tests/test_dag_cache_keys.py::test_key_components_lock_exact_label_set tests/test_cache_policy.py::test_key_component_labels_add_only_external_when_supplied tests/test_cache_policy.py::test_external_fingerprint_changes_owner_then_downstream_and_uses_exact_digest tests/test_cache_policy.py::test_cache_policy_repeated_runs_and_plan tests/test_cache_policy.py::test_map_item_cache_policy_executes_every_item_and_plan_reports_miss tests/test_cache_policy.py::test_scan_explain_without_initial_carry_uses_run_key_components tests/test_dag_cache_keys.py::test_kigumi_component_tracks_prompt_bytes_and_pydantic_version tests/test_dag_plan_explain.py::test_explain_records_key_components_and_reports_one_changed_input tests/test_dag_scan.py::test_scan_carry_fn_code_is_irrelevant_when_extracted_content_is_equal tests/test_dag_scan.py::test_scan_carry_from_content_invalidates_the_whole_chain tests/test_dag_cache_keys.py::test_libs_hash_ignores_comment_and_docstring_edits tests/test_dag_cache_keys.py::test_libs_hash_tolerates_broken_syntax_by_hashing_raw_text tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_multiple_configured_candidates tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_loaded_runtime_module_mismatch tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_aliased_dynamic_imports tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_importlib_submodule_alias tests/test_dag_cache_keys.py::test_libs_hash_tracks_ancestor_package_init tests/test_dag_cache_keys.py::test_libs_hash_tracks_unresolved_import_inside_source_tree
```

## Change policy

修改键成分、哈希归一化、`CACHE_SCHEMA` 或其推导位置时，必须同步更新锁定测试、本契约、
`DESIGN.md` 中的缓存说明和 `CHANGELOG.md` 的换族记录。
