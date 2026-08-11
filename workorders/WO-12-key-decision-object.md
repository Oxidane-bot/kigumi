# WO-12: 精简#2 — 内部 key-decision 对象（NodeKeyDecision）

> 状态：READY ｜ 波次 2（在波次 1 之后）｜ 风险：高 ｜ 缓存换族：**否（`_key_components` 字节必须不变）**
> 来源：complexity 报告 ranked target #2

## 目标
把 run/plan/explain/map/scan 五处**重复的缓存键准备+判定流程**收敛为一个内部决策对象，复用之。`_key_components()` 保持字节级 source-of-truth 不变。

## 实证（complexity 报告）
一次 L3 判定穿过约 18–25 个函数边界；键准备/哈希/查找/解释逻辑在五处重复：
- ordinary `run`（`dag.py:1558-1582`）
- `plan` ordinary/map/scan 分支（`dag.py:3000-3133`）
- `explain` 经 `_current_key_components`（`dag.py:3278-3420`）
- map 项执行（`dag.py:4253-4265`）
- scan 项执行（`dag.py:4775-4788`）
`_key_components()` 组装 source/libs/pydantic/prompts/files/upstream/item/carry(`dag.py:5212-5321`)，是字节 source-of-truth。

## 改动
1. 引入内部 `NodeKeyDecision`（含 key components、cache key、effective cache policy、prompt records、相关 snapshot）。
2. 一个构造它的内部方法，供上述五处复用，替换各自重复的 prepare/key/policy/lookup 序列。
3. **不重新设计键内容**；`_key_components()` 仍是字节 source-of-truth。

## 约束（关键）
- **键字节逐位不变**：同一节点五处路径产出的 cache key 与改造前完全一致。
- 不改变 resume/cache 命中语义、plan 与 explain 的输出（plan/explain parity 测试必须通过）。
- 纯内部重构；不改公开 API。

## 验收（proof）
- `uv run pytest tests/test_dag_cache_keys.py tests/test_dag_plan_explain.py -q` 全绿（129 cache-key 测试 + plan/explain parity `:210-495`）。
- `uv run pytest tests/test_cache_policy.py tests/test_consumes.py tests/test_dag_map.py tests/test_dag_scan.py -q` 全绿。
- **证明五处键一致**：改造前后 plan/explain/run 对同一组节点的 cache key 完全一致（报告给对比）。
- `uv run pytest -q` 全绿；ruff 通过。

## CHANGELOG
纯内部重构、无键变化 → 不改 CHANGELOG（若有键变化，停止上报）。

## 输出
report：改动文件 + 五处键一致性对比证据 + 完整测试输出。
