# 缓存键契约

Status: Active (0.13.0)

> Unreleased 将 L3 内容键 `CACHE_SCHEMA=8`，L1 则硬切到 transport identity 与 effective
> prepared request；这是一次统一换族，不提供旧键兼容读取。
> node cache envelope 继续为 schema 4；
> 旧 schema 3 条目（即使已经带有 `cache_key`）按 `CORRUPT` 拒绝，不迁移；
> `agent_executor_schema=5`。这是 Agent scan/session canonical artifact 的完整 L3 cache
> 硬切，不迁移 0.7.x 条目。
> EvidencePolicy、RetryPolicy 与 Agent capacity 不进入内容键；前两者绑定 run/origin identity。

## Purpose

让同一语义输入稳定复用缓存，让任一会改变结果的输入改动都换键，避免陈旧结果静默回放。

## Scope

适用于 L1 `LLMCaller.call()`，以及 L3 `Dag`/`Subgraph` 的普通节点、map 项、scan 项、
cache policy、external fingerprint、run、plan 和 explain 入口。

## Source of truth

L1 键由 `kigumi.calling.LLMCaller.call()` 构造；L3 成分唯一由
`kigumi.dag.Dag._key_components()` 推导。

## Invariants

1. transport 必须先以无 provider I/O 的 `prepare(messages, model, params)` 返回冻结的
   `PreparedRequest`。L1 键等于
   `sha({transport=transport.cache_identity(), prepared=prepared.canonical(), seed})`；当请求带有
   非默认 `ResponseSpec` 时，键额外绑定其格式与 `schema_sha256`。`cache_identity()` 必须稳定、
   credential-free，并区分可能改变 wire 语义的 adapter/config；`prepared.canonical()` 是
   effective messages、resolved model 与 normalized params 的稳定 JSON 投影。`seed` 只是键
   命名空间，不发给供应商。
   附件 canonical identity 只保存内容 SHA-256、MIME 与 detail 等稳定逻辑表示，不含 base64
   或临时绝对路径；`send(prepared)` 仍从同一个 prepared request 展开并获得实际 wire 内容。
2. L3 成分标签固定为 `source`、`libs`、`upstream:<dep>`、`prompts:<t>`、
   `prompt_specs:<name>`、`files:<p>`、`params`、`item`、`item_files:<p>`、`carry`、
   `kigumi`；引用 `call_validated` 检测到的 Pydantic 模型时额外出现
   `validated_models`，声明外部指纹时额外且仅额外出现
   `external=sha(external_fingerprint)`；普通依赖边默认取完整上游产物摘要，声明
   `consumes[dep]` 时同一 `upstream:<dep>` 标签改取 canonical 投影视图摘要，不新增标签；
   推导单点在 `dag._key_components`，原始指纹不落盘。
3. `items_from` 与 scan 的 `carry_from` 源不入共享 `upstream`；`item` 按内容入键；`carry` 只按
   本项实收内容入键，`carry_fn` 源码不入键。消费投影的源码同样不入键；节点函数收到 canonical
   JSON round-trip 后的投影视图而非完整上游产物，未声明投影的依赖输入形态不变。
4. `source` 与 `libs` 都按剥除 docstring/注释后的 AST 哈希；`libs` 不覆盖 `source_dirs` 下所有
   源码，而是对每个节点只纳入从节点函数所属模块出发、由静态 import 图可达的
   `config.source_paths` 文件，并按传递闭包计算。`source_dirs` 中对该节点不可达的文件不进入该节点的
   `libs` 身份；项目根下未列入配置源码路径的 imported helper 不进入 `libs`；独立的 `source`
   成分只覆盖注册节点函数本身，其他外部输入必须加入 `source_dirs` 或声明显式指纹。
   静态分析只接受模块顶层、无歧义的 import；配置源码快照中任一文件语法残破、节点模块
   无法定位或读取、可达模块无法解析，或发现条件/嵌套 import、`importlib` 及其子模块/动态导入调用、
   已识别的动态可调用引用（无论是否调用，也不论赋值目标是简单名、walrus、属性、下标/容器、
   解构或链式赋值）、动态导入别名、star-import、模块级 `__getattr__`、相对导入无法在所有配置源码根中
   唯一解析、相对导入存在多个配置候选、`ImportFrom` child 未能独立解析（即使 base package 已解析）、同一导入对应多个
   配置源码候选，已加载模块的 `__file__` 偏离静态候选且仍落在配置源码路径内，或表面为外部的
   已加载 package 通过运行时 `__path__` 伸入配置源码宇宙等不确定情形时，该节点退回当前全文件
   digest。普通 `ImportFrom` 属性只有在 base 模块顶层 AST 明确证明函数、
   类、赋值或安全 import binding 时才保持闭包粒度；未解析 child、动态 `__getattr__`、star import
   和反射仍然 fallback。相对导入携带实际限定模块 identity 并校验所有 dotted loaded prefixes；
   向上相对导入按 climb 后的 package suffix 校验，缺失 expected `sys.modules` 条目也按不确定处理。
   选中闭包额外绑定每个文件的限定模块名；owner 分析从当前注册节点函数出发，只跟进实际到达的
   nested function、class body 与直接调用的 Python callable，不让未调用 sibling 污染粒度。剥除
   docstring 后的可执行 AST 若直接观察 `__name__`、`__package__`、`__file__`、`__spec__`、
   `__loader__`，通过函数对象的 `__module__`/`__globals__` 观察 owner，或通过直接、属性、partial
   绑定、closure 别名及保守的下标选择形式调用 `globals`、`locals`、`vars`、`eval`、`exec`、
   `compile`、`getattr`/`object.__getattribute__` 观察实际 owner-module/registered-function receiver，
   两种路径还会绑定 owner 的限定模块名与稳定项目相对/canonical 路径；判断遵守 Load/Store 与
   词法局部绑定，所以 `getattr(helper, "__name__")`、`helper.eval(...)`、`obj.globals` 和局部
   `__name__` 不会自动绑定 owner。只在 docstring、非查找字符串中提到这些名称，或执行常量且
   无关的 `getattr(helper, "VALUE")` 的等价函数，仍可跨模块名或文件名复用。
   detached function 只有在 `__module__`、函数 globals 名称及 `co_filename`/inspect source path
   事实一致时恢复 owner；冲突事实不作 owner 证明而进入 fail-closed fallback。全文件 fallback
   额外绑定当前已加载/函数实际引用的配置源码模块选择、脱离 `sys.modules` 后仍保留的
   function/class 与已识别 callable wrapper 的 outer、partial target 和 `__wrapped__` target
   provenance；callable provenance 同时包含限定名与不含文件名/行表噪声的可执行 code digest，
   避免同一配置文件甚至同一源码行内切换 callable 仍复用旧产物。纳入 configured-source
   provenance 的 retained callable 通过一张共享、确定性且有 node/depth/member 硬上限的对象图绑定
   closure、defaults/kwdefaults、annotations、function dict、bound method 的 function/receiver、
   partial 参数、wrapper target、实例 dict、每个 MRO slot、class/base/metaclass state，以及 slot
   或容器中实际到达的 callable；dict insertion order、普通 `"__wrapped__"` 数据键、float bit
   pattern、cycle 与跨多个 callable/global root 的 shared-alias topology 都保留。图超预算，或
   遇到 native container subclass、custom descriptor/property/`__getattribute__`、不安全路径对象
   等无法通过 exact built-in type/getset/member descriptor 静态读取的状态时，确定性 `libs`
   identity 标记为不可复用，并单独强制对应节点及 map/scan item 跳过 L3 读写；该标记不随机化
   cache key、run manifest、resume/recover 或 explain 所依赖的声明 identity。可动态观察完整
   globals namespace 的函数也一律按不可复用处理，而不是声称已经证明任意 namespace 等价。
   不属于 configured-source provenance、且不能安全表示的外部复杂 global 仍在 managed `libs`
   源码宇宙之外：fallback 跳过其值状态而不因此关闭整个节点的 L3；若该值具有 configured-source
   provenance，或完整 globals namespace 可观察，则仍按不可复用 fail closed。
   fallback 仍绑定函数及嵌套 code object 实际引用的简单全局值，以及配置源码根本身或 package
   parent 在 `sys.path` 中的相对顺序；运行时路径条目只接受 exact string，不调用 `__fspath__`、
   truthiness、用户 equality/hash 或 mapping hooks。外部 dotted import 的每个已加载 package
   prefix 也校验运行时 `__path__` 是否触及 managed source；运行时取证不得执行用户代码。
   两种摘要都绑定按 `source_dirs` 顺序的稳定 source-root/file identity：项目内源码根使用相对
   项目根路径和文件相对路径，不把临时绝对路径引入键；项目外配置根使用其 canonical identity，
   配置根顺序本身是运行时选择语义的一部分。模块 AST 中出现常见反射原语
   （`getattr`、`globals`、`locals`、`vars`、`__dict__`、`__getattribute__`、
   `__builtins__`、显式 `builtins` 导入或模块注册表属性如 `sys.modules`）也一律回退，
   即使反射与导入无关，或被查找的名称/键是计算出来的；不做常量传播。无配置源码候选且
   已能证明属于标准库、内置模块或项目外已加载模块的绝对 import 不纳入 `libs`。
   这种保守 false positive 是有意的：扩大输入可以多失效，但不能用未覆盖的反射路径复用
   陈旧产物。该规则是源码 AST 分析边界，不宣称覆盖整个 Python 运行时或任意外部/native
   代码。
   `validated_models` 对每个检测到的模型绑定限定模块名、限定类名、JSON schema 与规范化的
   可见类源码摘要；模型位于 `source_dirs` 之外时也不能从键中静默消失。运行时展开 Pydantic
   metaclass 的不可表示状态时，缓存身份使用同一受限模型摘要，不因此关闭整个节点的 L3 复用。
5. `cache="auto"|"refresh"|"off"` 只控制 L3 读写，不是键成分；force 只旁路本次读取。
   refresh/off 仍计算确定性 key components/cache_key 供 provenance 与 explain。若 `libs` fallback
   遇到不可安全/有限表示的运行时状态、完整 globals namespace 观察或遍历预算耗尽，框架内部把
   该节点视为本次 `off`，且 run、plan、explain、普通节点、map 与 scan 必须使用同一有效策略：
   不读取/写入 L3，不报告空 item 集合的 vacuous aggregate hit，但仍使用同一确定性声明 identity
   支持 durable resume/recovery。L1 不变。
6. `kigumi` 成分等于 `sha({prompt_source, schema=CACHE_SCHEMA=8, pydantic})`；其中
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
   与 Agent scan executor 语义。0.11.0 从 6 升至 7，以绑定 managed request 的
   attachment content hash、typed message digest 和 response schema identity；随后发布的
   `libs` 细化搭载同一第 7 族，当时未新增全项目换族。0.13.0 将 node cache
   envelope 从 schema 3 升至 schema 4，以正式绑定请求的 L3 `cache_key`；这是 Greenfield
   envelope 硬切，不迁移旧 schema 3 条目，也不改变当时的内容键 `CACHE_SCHEMA`。
   Unreleased 从 7 升至 8，使 L1 transport/prepared identity 与 L3 内容族同步硬切；这是
   本批变更唯一一次缓存族轮换。
9. `prompt_specs:<name>` 取当前 resolution digest：包含 spec/binding 结构、base、固定 layer、
   axis 实际 selection 与所选 fragment、material digest 和 rendered digest；不包含未选中
   variant 的内容 digest。resolution digest 还绑定 typed message 内容、附件 content hash
   与 `ResponseSpec` schema identity，不绑定附件原始路径或 transport base64。 同节点声明的所有 PromptSpec 都保守入键，即使本次函数未调用。
   未选中候选的完整字节 universe 只进入 run manifest graph identity，因此改它可复用相同
   selected-only L3 条目，但旧 run 因声明 identity 漂移拒绝 resume。未声明的字符串 CALL
   不伪造 PromptSpec 成分，receipt 只记录为 unmanaged。
10. node cache envelope schema 4 固定保存请求绑定的 `cache_key`、canonical artifact、artifact
    SHA-256、首次执行的 immutable origin provenance 与 origin digest。warm hit 不得以 replay
    metadata 覆盖 origin；schema 3 或缺少/错绑 `cache_key` 的 envelope 都是 `CORRUPT`。
11. EvidencePolicy digest 不匹配按 evidence miss 执行，但不改变 key components；RetryPolicy
    digest、Agent slots/lock/timeout、`ResourceRequest` 与 `resource_limits` 也不属于内容键。

## Failure behavior

键成分不同会得到不同摘要并按缓存未命中处理；不存在的缓存才是普通 miss。已经存在但为空、
撕裂、摘要不匹配或 schema/provenance 无效的缓存属于 `CORRUPT`，必须 fail closed，不能按
miss 重算或静默重新计费；`CacheLookup` 保留 `MISSING`、`VALID`、`CORRUPT` 三态。未在
`CHANGELOG.md` 记录的键成分演进不得进入发布件。非法 cache 值或不可 canonical JSON
序列化的 external fingerprint 在注册期抛 `ValueError`。`libs` 静态分析遇到上述无法证明的情况时
必须使用当前全文件 digest，不得猜测较小闭包；全文件 digest 仍沿用 `_module_code_text`，语法残破
文件使用原文，并绑定有序的稳定源码根/文件 identity 与当前可观察的配置源码运行时选择。

## Affected surfaces

- `kigumi.calling.LLMCaller.call`
- `kigumi.transport.PreparedRequest` 与 `kigumi.transport.Transport`
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
`tests/test_calling.py::test_plain_messages_use_prepared_request_and_transport_identity_in_cache_key`、
`tests/test_calling.py::test_transport_cache_identity_is_part_of_l1_key`、
`tests/test_calling.py::test_prepared_attachment_canonical_uses_digest_not_path_or_base64`、
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
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_distinguishes_equal_candidates_by_source_identity`、
`tests/test_dag_cache_keys.py::test_libs_hash_invalidates_relative_import_across_configured_roots`、
`tests/test_dag_cache_keys.py::test_libs_hash_invalidates_unresolved_importfrom_child_on_extended_package_path`、
`tests/test_dag_cache_keys.py::test_libs_hash_external_package_path_into_configured_source_falls_back`、
`tests/test_dag_cache_keys.py::test_libs_hash_preserves_importfrom_attribute_granularity`、
`tests/test_dag_cache_keys.py::test_libs_hash_unresolved_importfrom_child_is_ambiguous`、
`tests/test_dag_cache_keys.py::test_libs_hash_relative_import_uses_qualified_package_identity`、
`tests/test_dag_cache_keys.py::test_libs_hash_upward_relative_import_validates_qualified_sibling`、
`tests/test_dag_cache_keys.py::test_libs_hash_selected_closure_binds_source_identity`、
`tests/test_dag_cache_keys.py::test_libs_hash_selected_closure_binds_qualified_module_name`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_loaded_configured_candidate`、
`tests/test_dag_cache_keys.py::test_libs_hash_selected_closure_binds_identity_sensitive_unmanaged_owner`、
`tests/test_dag_cache_keys.py::test_libs_hash_binds_reflective_owner_identity`、
`tests/test_dag_cache_keys.py::test_libs_hash_binds_bound_owner_getattribute_lookup`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_aliased_builtin_getattr_dynamic_owner`、
`tests/test_dag_cache_keys.py::test_libs_hash_recovers_detached_owner_from_retained_function_facts`、
`tests/test_dag_cache_keys.py::test_libs_hash_fails_closed_for_inconsistent_detached_owner_facts`、
`tests/test_dag_cache_keys.py::test_libs_hash_selected_closure_binds_registered_code_filename`、
`tests/test_dag_cache_keys.py::test_libs_hash_code_filename_ignores_unrelated_callable_receiver`、
`tests/test_dag_cache_keys.py::test_libs_hash_ignores_identity_sensitive_sibling_function`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_referenced_sibling_owner_identity`、
`tests/test_dag_cache_keys.py::test_libs_hash_ignores_module_identity_name_in_docstring`、
`tests/test_dag_cache_keys.py::test_libs_hash_ignores_non_identity_reflection_for_owner_name`、
`tests/test_dag_cache_keys.py::test_libs_hash_owner_reflection_is_receiver_and_scope_aware`、
`tests/test_dag_cache_keys.py::test_libs_hash_selected_closure_binds_identity_sensitive_owner_path`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_retained_imported_function_origin`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_callable_within_configured_file`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_retained_partial_origin`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_configured_wrapped_target`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_wrapper_cycle_is_safe`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_retained_callable_runtime_state`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_retained_callable_own_globals`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_ignores_unreferenced_callable_global`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_dynamically_observable_globals`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_retained_imported_value`、
`tests/test_dag_cache_keys.py::test_libs_hash_retained_callable_ignores_prefix_comments_and_docstrings`、
`tests/test_dag_cache_keys.py::test_libs_hash_fallback_binds_package_parent_sys_path_order`、
`tests/test_dag_cache_keys.py::test_libs_hash_validates_descendant_external_package_prefix_path`、
`tests/test_dag_cache_keys.py::test_libs_hash_comprehension_target_does_not_shadow_owner_load`、
`tests/test_dag_cache_keys.py::test_libs_hash_ignores_unexecuted_nested_identity_bodies`、
`tests/test_dag_cache_keys.py::test_libs_hash_does_not_execute_custom_receiver_during_analysis`、
`tests/test_dag_cache_keys.py::test_unrepresentable_callable_state_is_stable_but_not_cache_reusable`、
`tests/test_dag_cache_keys.py::test_unrepresentable_callable_state_can_resume_same_run`、
`tests/test_dag_cache_keys.py::test_libs_hash_binds_mutable_callable_class_state`、
`tests/test_dag_cache_keys.py::test_libs_hash_binds_callable_alias_topology`、
`tests/test_dag_cache_keys.py::test_libs_hash_does_not_execute_partial_subclass_attributes`、
`tests/test_dag_cache_keys.py::test_libs_hash_does_not_execute_custom_metaclass_during_analysis`、
`tests/test_dag_cache_keys.py::test_representable_callable_instance_state_remains_cache_reusable`、
`tests/test_dag_cache_keys.py::test_called_nested_body_binds_owner_module_identity`、
`tests/test_dag_cache_keys.py::test_owner_identity_tracks_reached_reflection_forms`、
`tests/test_dag_cache_keys.py::test_callable_state_preserves_dict_order_and_wrapped_data`、
`tests/test_dag_cache_keys.py::test_bound_method_defaults_are_part_of_callable_state`、
`tests/test_dag_cache_keys.py::test_callable_state_preserves_aliases_across_global_roots`、
`tests/test_dag_cache_keys.py::test_runtime_provenance_does_not_execute_pathlike_entries`、
`tests/test_dag_cache_keys.py::test_runtime_state_depth_limit_is_stable_and_non_reusable`、
`tests/test_dag_cache_keys.py::test_runtime_state_node_limit_is_stable_and_non_reusable`、
`tests/test_dag_cache_keys.py::test_runtime_provenance_member_limit_is_stable_and_non_reusable`、
`tests/test_dag_cache_keys.py::test_complete_globals_observation_disables_l3_reuse`、
`tests/test_dag_cache_keys.py::test_non_reusable_map_plan_skips_item_cache_reads`、
`tests/test_dag_cache_keys.py::test_empty_map_never_reports_vacuous_cache_hit`、
`tests/test_dag_cache_keys.py::test_empty_scan_never_reports_vacuous_cache_hit`、
`tests/test_dag_cache_keys.py::test_libs_source_universe_matches_configured_snapshot`、
`tests/test_dag_cache_keys.py::test_libs_hash_validates_loaded_dotted_import_prefixes`、
`tests/test_dag_cache_keys.py::test_libs_hash_missing_canonical_import_entry_fails_closed`、
`tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_loaded_runtime_module_mismatch`、
`tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_aliased_dynamic_imports`、
`tests/test_dag_cache_keys.py::test_libs_hash_falls_back_for_importlib_submodule_alias`、
`tests/test_dag_cache_keys.py::test_libs_hash_tracks_ancestor_package_init`、
`tests/test_dag_cache_keys.py::test_libs_hash_tracks_unresolved_import_inside_source_tree`，以及
`tests/test_consumes.py` 中对投影键、输入隔离、plan/run/explain、动态节点、Subgraph、
注册校验、错误上下文、标签集与 schema 的锁定测试。

```bash
uv run --extra dev pytest -q \
  tests/test_calling.py \
  tests/test_cache_policy.py \
  tests/test_consumes.py \
  tests/test_dag_cache_keys.py \
  tests/test_dag_plan_explain.py \
  tests/test_dag_scan.py
```

## Change policy

修改键成分、哈希归一化、`CACHE_SCHEMA` 或其推导位置时，必须同步更新锁定测试、本契约、
`DESIGN.md` 中的缓存说明和 `CHANGELOG.md` 的换族记录。
