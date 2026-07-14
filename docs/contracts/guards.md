# 守卫环与豁免契约

Status: Active

## Purpose

在节点边界阻止循环内裸 LLM 调用和未声明的原始文件读取，同时让必要豁免可审计而不被静默吞掉。

## Scope

适用于 `Dag` 注册、`dag check`、pytest 插件守卫和 `kigumi guard`，覆盖 `raw-llm-ok` 与 `raw-io-ok`。

## Source of truth

注册环的权威入口是 `kigumi.dag._validate_registration()`；项目级扫描与豁免理由比较由
`kigumi.enforce` 和 `kigumi.cli` 提供。

## Invariants

1. 注册环只查节点函数体，是精确且权威的边界；外环（`dag check`、pytest 守卫、`kigumi guard`）对 raw-I/O 用装饰器启发式（`.node`/`.map`/`.scan`/`.foreach`），可能漏报、注册环兜底。
2. 两类豁免（`raw-llm-ok`/`raw-io-ok`）必须带理由；各自独立留痕，比对互不吞并。
3. `guard --changed` 按理由文本（非行号）比对 `HEAD`，新增豁免必须上报。

## Failure behavior

注册环发现未豁免的违规即抛 `ValueError` 拒绝节点注册；外环报告违规并以非零状态失败。无理由豁免仍是违规；新增理由在 `--changed` 中被报告。

## Affected surfaces

- kigumi/dag.py 的 `_validate_registration` 与 `_extract_node_ast_metadata`
- `kigumi/enforce.py:15-51`
- `kigumi/enforce.py:54-118`
- `kigumi/enforce.py:121-174`
- `kigumi/enforce.py:177-244`
- `kigumi/cli.py:180-256`
- `kigumi/testing.py:205-234`
- `kigumi/testing.py:256-291`

## Verification

锁定测试：`tests/test_dag.py::test_registration_rejects_raw_io_and_allows_a_reasoned_waiver`、
`tests/test_dag.py::test_registration_rejects_raw_io_waiver_without_a_reason`、
`tests/test_dag.py::test_node_registration_blocks_loop_calls_and_allows_reasoned_waivers`、
`tests/test_enforce.py::test_waiver_reason_is_visible_and_empty_waiver_remains_violation`、
`tests/test_enforce.py::test_raw_io_path_guard_checks_only_decorated_top_level_node_bodies`、
`tests/test_cli.py::test_guard_reports_violations_waivers_and_new_changed_waivers`、
`tests/test_cli.py::test_guard_checks_decorated_raw_io_but_not_helpers_and_tracks_its_waivers`。

```bash
uv run pytest -q tests/test_dag.py::test_registration_rejects_raw_io_and_allows_a_reasoned_waiver tests/test_dag.py::test_registration_rejects_raw_io_waiver_without_a_reason tests/test_dag.py::test_node_registration_blocks_loop_calls_and_allows_reasoned_waivers tests/test_enforce.py::test_waiver_reason_is_visible_and_empty_waiver_remains_violation tests/test_enforce.py::test_raw_io_path_guard_checks_only_decorated_top_level_node_bodies tests/test_cli.py::test_guard_reports_violations_waivers_and_new_changed_waivers tests/test_cli.py::test_guard_checks_decorated_raw_io_but_not_helpers_and_tracks_its_waivers
```

## Change policy

修改检测边界、装饰器集合、豁免格式或 `--changed` 比对规则时，必须同步更新守卫测试、本契约、`docs/adoption.md` 与 `CHANGELOG.md`。
