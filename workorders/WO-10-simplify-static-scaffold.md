# WO-10: 精简#3 — 静态分析脚手架去重

> 状态：READY ｜ 波次 1 ｜ 风险：低 ｜ 缓存换族：**否（行为保持）**
> 来源：complexity 报告 ranked target #3

## 目标
消除 `dag.py` 静态分析层的**复制粘贴**，低风险维护性补丁，不改变保守回退行为。

## 实证（complexity 报告）
1. **函数树包装器完全重复**:`_function_uses_module_identity` vs `_function_observes_all_globals`，归一化 token 完全匹配 1.000（`dag.py:7000-7059`）。
2. **两个 import-prefix 校验器**相似度 .811(`dag.py:9193-9217` vs `:9438-9474`)。

complexity 报告明确警告:**不要**合并 `_tree_uses_module_identity` 与 `_tree_observes_all_globals`——它们的 visitor 实现不同决策。

## 改动
1. 抽出 `_inspect_function_tree(..., evaluator)` 共享 helper，让 `_function_uses_module_identity` 与 `_function_observes_all_globals` 共用遍历框架；**保留各自独立的 visitor/evaluator 语义**。
2. 把两个 import-prefix 校验器参数化为一个通用 prefix 循环 + resolver 回调，消除 .811 重复。

## 约束
- 行为完全保持：不改变任何分类结果、不改保守回退。
- 不合并 visitor 决策逻辑，只共享遍历脚手架。
- 缓存键字节不变。

## 验收
- `uv run pytest tests/test_dag_cache_keys.py -q` 全绿（runtime provenance/import 矩阵 `:210-250`）。
- `uv run pytest tests/test_enforce.py -q` 全绿（raw-I/O 遍历/waiver `:263-355,:632-711`）。
- `uv run pytest -q` 全绿；ruff 通过。

## CHANGELOG
纯内部重构 → 不改 CHANGELOG。

## 输出
report：改动文件 + 测试输出。
