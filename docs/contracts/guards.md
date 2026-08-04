# 守卫环与豁免契约

Status: Active (0.13.0)

> `@dag.agent` builder 与其他节点装饰器同样进入 raw-I/O 扫描。`raw-io-ok` 必须带理由，
> 且不能由 `raw-llm-ok` 代替。

## Purpose

在节点边界阻止循环内裸 LLM 调用和未声明的原始文件读取，同时让必要豁免可审计而不被静默吞掉。

## Scope

适用于 `Dag` 注册、`dag check`、pytest 插件守卫和 `kigumi guard`，覆盖 `raw-llm-ok` 与 `raw-io-ok`。

## Source of truth

注册环的权威入口是 `kigumi.dag._validate_registration()`；项目级扫描与豁免理由比较由
`kigumi.enforce` 和 `kigumi.cli` 提供。

## Invariants

1. raw-I/O 注册检查从节点函数体递归跟随**可达的局部 helper/lambda**；未被节点执行路径引用的普通 helper 函数体不产生误报。
   但是顶层节点和 nested helper 的默认参数、decorator 与 annotation 在函数定义时执行，因此这些表达式始终扫描。
2. 节点执行范围内禁止 nested class。class method 是 AST 无法证明的执行边界，不能靠扫描方法体声称已经覆盖。
3. raw callable 的直接别名（如 `open_alias = open`、`reader = Path(...).read_text`）、已知 callback 位置（`map`/`filter`）以及可证明的静态容器传播（序列的正/负下标、字面量 `dict`、包含嵌套 `**` 的静态字典、`helper(*readers)`、静态 `**kwargs`、`map(*[open], ...)`）必须被识别。
   `globals`、`locals`、`getattr`、`eval`、`exec`、直接 `__import__`、`builtins.__dict__` 的 alias、动态字典的 `.get`/`.__getitem__`、`importlib`/`sys.modules` 链、已知 raw/dynamic callable lookup，以及下标得到的 opaque callable 采用硬切：使用或调用即违规。
   `builtins.__dict__["__import__"]` 及纯动态 `importlib.import_module(...)` 本身是动态 import 边界：纯 import 不制造 raw-I/O finding，由动态 import 分析负责；一旦其结果继续到 raw/opaque callable，整条链必须硬切。
   任意未知数据下标仍保持 unknown，不因存在 `readers[index]` 就作无边界推断。
4. `ctx` 只有在仍绑定到节点上下文参数时才豁免 `read_text`/`read_bytes`；重新绑定后同名方法按 raw read 扫描。comprehension target 的重绑定只在其隐式作用域内失效；match pattern 的绑定在 case 内及后续可达语句中失效。
   硬切结构违规不接受 `raw-io-ok` 豁免；真正的、静态可证明的 raw read 仍可使用带理由的 `raw-io-ok`。
5. 两类豁免（`raw-llm-ok`/`raw-io-ok`）必须出现在 tokenizer 确认的 source comment 中，token 必须精确结束（例如 `raw-io-okhidden` 不匹配），并带理由；字符串内容不是注释。两类豁免各自独立留痕，比对互不吞并。
6. `guard --changed` 按理由文本（非行号）比对 `HEAD`，新增豁免必须上报。

这些规则是一个有边界的 AST 检查器，不是 Python 解释器，也不承诺发现任意动态调用。
为了让契约可证明，节点必须避免 opaque callable、动态名称解析和 nested class，使用显式命名
helper 或 `ctx.read_text`/`ctx.read_bytes`；只有静态字面量序列/dict 的 callable 事实才会传播。

## Failure behavior

注册环发现未豁免的违规即抛 `ValueError` 拒绝节点注册；外环报告违规并以非零状态失败。
无理由豁免仍是违规；硬切结构违规即使同一行写了 `raw-io-ok` 也仍是违规；新增理由在
`--changed` 中被报告。

## Affected surfaces

- kigumi/dag.py 的 `_validate_registration` 与 `_extract_node_ast_metadata`
- `kigumi/enforce.py:15-51`
- `kigumi/enforce.py:54-118`
- `kigumi/enforce.py:121-174`
- `kigumi/enforce.py:177-620`
- `kigumi/cli.py:180-256`
- `kigumi/testing.py:205-234`
- `kigumi/testing.py:256-291`

## Verification

锁定测试：`tests/test_dag_registration.py::test_registration_rejects_raw_io_and_allows_a_reasoned_waiver`、
`tests/test_dag_registration.py::test_registration_rejects_raw_io_waiver_without_a_reason`、
`tests/test_dag_registration.py::test_registration_hard_fails_raw_io_fact_propagation_escapes`、
`tests/test_dag_registration.py::test_registration_hard_fails_p1_ast_boundary_escapes`、
`tests/test_dag_registration.py::test_node_registration_blocks_loop_calls_and_allows_reasoned_waivers`、
`tests/test_enforce.py::test_waiver_reason_is_visible_and_empty_waiver_remains_violation`、
`tests/test_enforce.py::test_waivers_require_source_comments_and_exact_tokens`、
`tests/test_enforce.py::test_raw_io_hard_fails_nested_classes_but_keeps_unreachable_helpers_out_of_scope`、
`tests/test_enforce.py::test_raw_io_rejects_raw_callable_aliases_and_callback_use`、
`tests/test_enforce.py::test_raw_io_rejects_builtin_import_lookup_chained_to_open`、
`tests/test_enforce.py::test_raw_io_rejects_direct_static_container_callable_index`、
`tests/test_enforce.py::test_raw_io_propagates_static_container_through_helper_star_args`、
`tests/test_enforce.py::test_raw_io_rejects_starred_static_map_callback`、
`tests/test_enforce.py::test_raw_io_reaches_lambda_from_static_container_index_call`、
`tests/test_enforce.py::test_raw_io_rejects_context_read_after_context_rebinding`、
`tests/test_enforce.py::test_raw_io_hard_cuts_dict_method_and_module_open_lookup_chains`、
`tests/test_enforce.py::test_raw_io_propagates_nested_static_dict_unpack_callable_arguments`、
`tests/test_enforce.py::test_raw_io_invalidates_context_for_comprehension_and_match_bindings`、
`tests/test_enforce.py::test_raw_io_scans_nested_helper_decorators_and_annotations`、
`tests/test_enforce.py::test_raw_io_rejects_opaque_dynamic_callables_and_scans_executed_defaults`、
`tests/test_enforce.py::test_raw_io_path_guard_checks_only_decorated_top_level_node_bodies`、
`tests/test_cli.py::test_guard_reports_violations_waivers_and_new_changed_waivers`、
`tests/test_cli.py::test_guard_checks_decorated_raw_io_but_not_helpers_and_tracks_its_waivers`。

```bash
uv run pytest -q tests/test_dag_registration.py::test_registration_rejects_raw_io_and_allows_a_reasoned_waiver tests/test_dag_registration.py::test_registration_rejects_raw_io_waiver_without_a_reason tests/test_dag_registration.py::test_registration_hard_fails_p1_ast_boundary_escapes tests/test_enforce.py::test_waiver_reason_is_visible_and_empty_waiver_remains_violation tests/test_enforce.py::test_waivers_require_source_comments_and_exact_tokens tests/test_enforce.py::test_raw_io_hard_cuts_dict_method_and_module_open_lookup_chains tests/test_enforce.py::test_raw_io_propagates_nested_static_dict_unpack_callable_arguments tests/test_enforce.py::test_raw_io_invalidates_context_for_comprehension_and_match_bindings tests/test_enforce.py::test_raw_io_hard_fails_nested_classes_but_keeps_unreachable_helpers_out_of_scope tests/test_enforce.py::test_raw_io_rejects_raw_callable_aliases_and_callback_use tests/test_enforce.py::test_raw_io_rejects_opaque_dynamic_callables_and_scans_executed_defaults tests/test_enforce.py::test_raw_io_path_guard_checks_only_decorated_top_level_node_bodies tests/test_cli.py::test_guard_reports_violations_waivers_and_new_changed_waivers tests/test_cli.py::test_guard_checks_decorated_raw_io_but_not_helpers_and_tracks_its_waivers
```

## Change policy

修改检测边界、装饰器集合、豁免格式或 `--changed` 比对规则时，必须同步更新守卫测试、本契约、`docs/adoption.md` 与 `CHANGELOG.md`。
