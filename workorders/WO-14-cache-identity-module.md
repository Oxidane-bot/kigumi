# WO-14: 精简#5 — 缓存身份独立成模块（_cache_identity.py）

> 状态：SCOPED（在 WO-09/WO-12 落地后做）｜ 波次 4 ｜ 风险：很高 ｜ 缓存换族：否
> 来源：complexity 报告 ranked target #5

## 目标
把缓存身份计算从 10149 行的 `dag.py` 中迁出，让回归二分定位更窄、归属更清晰。

## 迁移内容
- `_StaticLibsAnalyzer`
- runtime-state materialization(`_runtime_state_value_material` 等）
- runtime callable/global 遍历（`_collect_*`、`_runtime_*`）
- pydantic identity(`_validated_model_cache_identity`、`_pydantic_model_runtime_material`)

迁入新模块 `_cache_identity.py`。保留 `Dag._key_components()` 与小兼容 wrapper，使公开/内部接缝稳定。

## 实证
缓存分析区占据 `dag.py` 从 `_key_components` 到 callable provenance 的大部（约 `dag.py:5212-9943`）。
**blast radius 大**：测试直接访问私有内部——`_StaticLibsAnalyzer`（`tests/test_dag_cache_keys.py:1082,:2715,:3290-3299,:5646`）、`_key_components`（`:223-287`）。

## 执行方式
- 必须在 WO-09（共享物化器）、WO-12（key-decision 对象）落地后做，避免在迁移中同时改逻辑。
- 分两个子工单：WO-14a（迁移映射 + 兼容层方案，只读）→ WO-14b（实现 + 测试 import 更新）。
- 测试对私有符号的 import 需同步迁移；可提供短暂 re-export 兼容，再收敛。

## 约束
- 纯移动 + import 调整，**不改任何逻辑、不改缓存键字节**。
- 公开 API(`Dag` 方法）签名不变。

## 验收
- `uv run pytest tests/test_dag_cache_keys.py -q` 全绿；`uv run pytest -q` 全绿；ruff 通过。
- 报告给出迁移前后文件行数对比与 import 映射。

## CHANGELOG
纯内部重构 → 不改 CHANGELOG。

## 输出
14a：迁移映射文档。14b：改动文件 + 完整测试输出。
