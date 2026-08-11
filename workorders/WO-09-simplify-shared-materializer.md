# WO-09: 精简#1 — 共享 runtime-global 物化器

> 状态：READY ｜ 波次 1 ｜ 风险：中（核心但纯重构）｜ 缓存换族：**否（字节必须逐位不变）**
> 来源：complexity 报告 ranked target #1

## 目标
合并 `kigumi/dag.py` 中**结构相同**的 runtime-global 物化决策，消除"改一处忘另一处"的双胞胎回归源。#1/#3 即长在这条路径上。

## 实证（complexity 报告 confirmed）
两段物化决策结构一致：
- callable-global 路径 `dag.py:8765-8786`（外层 `_collect_callable_globals` 8716-8797）
- global 路径 `dag.py:8949-8972`（外层 `_runtime_selection_material` 8890-9034）

共同步骤：收集 runtime module records → 算 `has_configured_provenance` → 调 `_transactional_runtime_state_material` → pydantic 特判 → 否则按 `has_configured_provenance or all_globals_observable` 判 uncacheable 并发类型 identity → 否则省略该值。
**差异仅限**：binding 标签（`callable-global` vs `global`）、命名（`f"{binding}:{name}"` vs `binding_name`）、depth（`depth+1` vs `1`）、子节点处理（callable-global 入 `pending` 队列 `:8787-8797`；ordinary global 递归 `_collect_runtime_callable` `:8973-8987`）。

## 改动
1. 抽出一个共享 helper，负责：**单个 global 值的物化 + provenance 计算 + uncacheable 判定 + pydantic 特判**。签名形如接收 (scope_label, binding_name, value, traversal/state_context, all_globals_observable) 并返回 (material | None, uncacheable_flag, runtime_records)。
2. 共享 safe-global-name 选择逻辑（`:8744-8759` 与 `:8928-8943`）。
3. **保留**两条路径各自的子节点遍历机制（pending 队列 vs 递归）在 helper 之外——不盲目合并整个 collector。

## 约束（关键）
- **缓存键字节逐位不变**：抽取前后，同一节点必须产生完全相同的 key components / libs hash。这是行为保持型重构。
- 不改变保守回退语义、`all_globals_observable`、configured provenance、pydantic fallback 的任何分支。
- 用 mutation 思路自查：若删掉 helper 里某分支，必须有现有测试变红（guard-tests-need-mutation-proof 教训）。

## 验收（proof）
- `uv run pytest tests/test_dag_cache_keys.py -q` 全绿（重点：runtime fallback/unrepresentable 矩阵 `:210-249`、pydantic identity `:377-492`）。
- `uv run pytest tests/test_dag_map.py tests/test_dag_scan.py -q` 全绿。
- **证明字节不变**：抽取前后对一组代表性节点（普通类/函数/实例/pydantic/不可表示值）的 `_libs_hash` 完全一致（在报告里给出对比）。
- `uv run pytest -q` 全绿；ruff 通过。

## CHANGELOG
纯内部重构、无使用者可见变更、无键变化 → 不改 CHANGELOG（若报告判定有行为/键变化，停止并上报）。

## 输出
report：改动文件 + 抽取前后 libs-hash 对比证据 + 完整测试输出。
