# WO-08: 预算/缓存键 effective-params（#13）

> 状态：NEEDS-DESIGN（先 WO-08a 设计子工单）｜ 波次 3 ｜ 风险：高 ｜ 缓存换族：**可能（见约束）**
> Issue: #13 ｜ 实测：0.13.0 REPRODUCIBLE

## 实测现状
缓存键用 caller 提供的 `params`（**transport 归一化之前**）(`calling.py:604`)；预算预留调 `_estimate_tokens(normalized_messages, params)`(`calling.py:648`)，transport 执行在其后(`:673`)。`_estimate_tokens` 仅当 caller 提供 `max_tokens` 才计入(`:903`)。内建 transport 归一化发生在 `complete()` 内(`transport.py:135`)；截断重试把 `current_params["max_tokens"]` 翻倍且**不重新预留**(`transport.py:163`)。实测：wrapper 注入 `max_tokens=100` 时预留仍按估算 20；不同 wrapper max_tokens **共用同一缓存键**。`Transport` 协议仅暴露 `resolve()`/`complete()`(`transport.py:42`)，无 effective-params prepare hook。

## 目标
让预算记账与缓存身份基于 transport 的**有效请求**(effective messages+params)，而非 caller 估算。

## 关键设计决策（WO-08a 必须先定）
1. **Transport prepare hook** 形态：新增可选方法（如 `prepare(messages, params) -> (messages, params)`)，在缓存键创建、preflight、预留**之前**返回 canonical effective 请求。
2. effective 请求/transport-policy 身份如何进入 provenance。
3. **是否把 transport 归一化后的 params 纳入缓存键**——这是缓存换族点，需明确决策与 CHANGELOG/schema 处理。
4. 截断重试翻倍 max_tokens 的重新预留/计数策略。
5. 对不实现 prepare 的旧 transport 的向后兼容（保持现状估算 = best-effort 模式）。

## 执行方式
- **WO-08a（设计，Claude 侧）**：prepare hook 协议 + 缓存键是否含 effective params 的决策 + 兼容性，供审批。
- **WO-08b（实现，Codex）**：按批准方案实现 + 测试。

## 约束
- 若缓存键成分变化 → **缓存换族**，必须同提交更新 `CHANGELOG.md` 并按 `docs/contracts/cache-key.md:110-115,:141-147` 处理。
- 不实现 prepare 的 transport 行为不变。
- RED→GREEN。

## 验收（WO-08b）
- 新增测试：wrapper 注入 max_tokens 后预留/缓存键反映 effective 值；不实现 prepare 的 transport 不变。
- `uv run pytest tests/test_calling.py tests/test_managed_request.py -q` 全绿；`uv run pytest -q` 全绿；ruff 通过。

## CHANGELOG
写入 `[Unreleased]`（中文）；若换族须显著标注。

## 输出
WO-08a：设计文档。WO-08b：改动文件 + 测试输出。
