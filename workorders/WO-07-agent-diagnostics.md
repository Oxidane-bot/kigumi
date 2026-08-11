# WO-07: agent 失败诊断（#6、#7）

> 状态：READY ｜ 波次 1 ｜ 风险：中 ｜ 缓存换族：否
> Issues: #6, #7 ｜ 实测：均为 PARTIAL（0.13 已部分改进）

## 实测现状
- **#6 PARTIAL**:0.13 已加子码 `envelope/bridge_policy/submit_contract/config_policy`（`kigumi/failures.py:138,141,143`）。已知 `AgentRuntimeResultError` 保留子码（`agents.py:1192`），但**其它一切异常仍坍缩为裸 `PROTOCOL`**（`agents.py:1197-1198`）。durable `AgentExecutionFailure` 只含 runtime/provider code+subcode，**不含原始异常类型/消息**（`failures.py:221,228`）；generic fallback 只留 `exception_type`+消息摘要（`failures.py:320`）。
- **#7 PARTIAL**:thinking-content 拒绝信息已含 `thinking=off`（`kigumi/pi.py:418`），但**不含 model id**；reasoning-usage 拒绝同样只含 `thinking=off` 无 provider/model（`pi.py:798,802-803`）。经 `execute_agent_task` 后，已知 thinking 错误丢失其消息，普通 reasoning-usage 错误落入裸 PROTOCOL（`agents.py:1192,1198`）。

## 目标
让失败的**诊断信息跨越边界存活**，不弱化任何 fail-closed 行为，不引入密钥泄漏。

## 改动
1. **#6 — `kigumi/failures.py` + `kigumi/agents.py`**:
   - 在坍缩点（`agents.py:1197-1198`）把**原始异常类型名 + 消息摘要**（沿用 `failures.py:320` 已有的 digest 机制，**不放明文消息以免泄漏**）挂到 `AgentExecutionFailure` 的可序列化字段上，使调用方能区分 envelope/bridge/submit 之外的未知失败来源。
   - 不新增子码枚举值（taxonomy 设计属后续），只做"保留 origin"。
2. **#7 — `kigumi/pi.py`**:thinking-content 与 reasoning-usage 两条拒绝信息，在现有 `thinking=off` 基础上**补充 provider 与 model id**（二者在 adapter 处可得）。使 `execute_agent_task` 路径下该信息不丢失（与 #6 的 origin 保留协同）。

## 约束
- RED→GREEN。
- **绝不把 API key / 环境值 / 明文敏感信息写进失败记录**——消息用摘要，类型用限定名。
- fail-closed 行为不变；只增强信息。

## 非目标
- 重设计 runtime code taxonomy / 新增子码枚举（#6 的完整方案，需设计决策，暂缓）。
- #5 provider descriptor。

## 验收
- `uv run pytest tests/test_failures.py tests/test_dag_agent_failures.py tests/test_pi_first.py -q` 全绿。
- 新增测试：未知异常失败记录含异常类型+消息摘要且无明文敏感串；thinking 拒绝信息含 provider/model id。
- `uv run pytest -q` 全绿；ruff 通过。

## CHANGELOG
写入 `[Unreleased]`（中文）。

## 输出
report：改动文件 + before/after 失败记录对比 + 完整测试输出。
