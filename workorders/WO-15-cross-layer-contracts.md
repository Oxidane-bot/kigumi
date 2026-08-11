# WO-15: 精简#6 — 打破跨层契约循环

> 状态：SCOPED ｜ 波次 4 ｜ 风险：中-高 ｜ 缓存换族：否
> 来源：complexity 报告 ranked target #6

## 目标
消除三处跨层/循环依赖，纯维护性精简，不改缓存键。

## 实证（complexity 报告）
1. **L1 `calling.py` import 存储层 `CacheLookup` 类型**(`calling.py:55`，用于 `:325-390`)。
2. **低层 `_runstate.py` import 高层 prompt 校验**两处(`_runstate.py:2144-2153,:2871-2902`)。
3. **`artifacts.py` ↔ `_safe_io.py` 循环**:`artifacts.py:13` import `_safe_io`，而 `_safe_io:373-378` 惰性 import `artifacts.canonical_json`（有意的循环）。
4. 附带：高层 `dag.py` 触及私有 `AttemptStore` 持久化方法（`dag.py:625-627,:1588-1589,:3693-3698,:5653-5657`）——记录但可单独评估。

## 改动
引入中立契约模块，承载被跨层引用的类型/函数：
- `CacheLookup`/`CacheEntry` → 中立模块，解除 `calling.py → store.py`。
- prompt-resolution record 校验 → 中立模块，解除 `_runstate.py → prompt.py`。
- canonical JSON + hashing → 中立模块，解除 `artifacts.py ↔ _safe_io.py` 循环。

## 约束
- 纯移动 + import 调整，不改逻辑、不改缓存键、不改 canonical_json 字节。
- 不改变 `_safe_io` 的安全语义。

## 验收
- `uv run pytest tests/test_cache_integrity.py tests/test_store_cache_rollback.py tests/test_runstate_integrity.py tests/test_prompt.py tests/test_calling.py tests/test_safe_io.py -q` 全绿。
- `uv run pytest -q` 全绿；ruff 通过。
- 报告确认三处循环/跨层 import 已消除（给 import 图前后对比）。

## CHANGELOG
纯内部重构 → 不改 CHANGELOG。

## 输出
report：改动文件 + import 图前后对比 + 完整测试输出。
