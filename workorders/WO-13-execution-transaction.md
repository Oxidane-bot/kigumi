# WO-13: 精简#4 — 抽取共享 execution 事务

> 状态：SCOPED（分阶段，先出方案）｜ 波次 4 ｜ 风险：高 ｜ 缓存换族：否
> 来源：complexity 报告 ranked target #4

## 目标
抽出 `Dag.run`、`_execute_map`、`_execute_scan` 三处重复的"执行单个 target"事务：
`resume/cache 查找 → invoke → candidate 持久化 → seal → materialize → sidecar`。

## 实证
重复生命周期位于 `dag.py:1558-2048`（run）、`:4227-4662`（map）、`:4744-5210`（scan）。
complexity 报告警告：**不要**合并整个 map/scan——归一化相似度仅 .422，且 scan 的串行 carry 语义是真实的。调度策略、map 并行、scan carry 时序保留在事务之外。`Dag.run` 本身是 801 行、b=120 的巨型函数（`dag.py:1417-2217`）。

## 执行方式（分两个子工单，避免大爆炸）
- **WO-13a（设计）**：只读分析 + 写出"单 target 事务"的精确接口方案：输入/输出、哪些步骤入事务、哪些留在外层、错误/恢复语义如何对齐三处现有差异。**不改代码**，产出设计文档供审批。
- **WO-13b（实现）**：按批准的方案实现 + 三处接入。

## 约束
- 行为完全保持；缓存键字节不变。
- scan carry 串行语义、map 并行语义不得改变。
- 先 13a 方案经维护者审批，再派 13b。

## 验收
- 13a：方案文档 + 三处现有生命周期差异对照表。
- 13b：`uv run pytest tests/test_dag_map.py tests/test_dag_scan.py tests/test_dag_agent.py tests/test_dag_agent_scan.py tests/test_dag_retry_resume.py tests/test_runstate_integrity.py tests/test_cache_integrity.py -q` 全绿；`uv run pytest -q` 全绿；ruff 通过。

## CHANGELOG
纯内部重构 → 不改 CHANGELOG。

## 输出
13a：设计文档。13b：改动文件 + 完整测试输出。
