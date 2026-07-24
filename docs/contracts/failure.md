# Failure 契约

Status: Active (0.7.0)

## Purpose / source of truth

CALL 与 Agent 共享 provider-neutral、可序列化的失败事实；实现权威为
`kigumi/failures.py`，transport、Caller、Pi 和 Agent executor 只能填入该模型。

## Invariants

1. `ProviderFailure` 固定保存 `provider`、`stage`、`kind`、`status_code`、
   `retry_after_ms`、`provider_request_id`、`message_digest`、`retryable_hint`。
2. kind 只能由 HTTP/wire status、异常类型或 typed SDK `code/type/kind` 字段得出。
   provider prose 不参与分类和重试判断，只计算 SHA-256 摘要。
3. 429、5xx、timeout、connection、401、403、其他 4xx/model mismatch 分别映射为
   `rate_limit`、`server_error`、`timeout`、`connection`、`authentication`、
   `authorization`、`invalid_request`/`model_mismatch`；无结构化事实时为 `unknown`。
4. `AgentRuntimeFailureCode` 只描述 spawn、version、process、protocol、policy、capacity；
   provider 失败必须嵌入同一个 `ProviderFailure`，两者不能同时存在。
5. Adapter 只报告事实；`retryable_hint` 不是调度决策，是否重试只由显式
   `RetryPolicy` 决定。
6. CALL failed metadata、Agent failure JSON、attempt receipt 和 run manifest failure
   保存 canonical typed failure，不保存可用于控制流的异常 prose。
7. schema-2 Agent failure receipt 与 CALL attempt calls 在 managed 输入存在时必须同时保存
   Prompt resolution；capacity、timeout、provider、hidden-retry、protocol 与 ambiguous 路径
   同形，画像默认不展开 instruction/request 内容。

## Failure behavior / verification

非法 canonical 字段在构造或恢复时拒绝。未知错误保留异常类型与 message digest，但默认不
重试。锁定测试见 `tests/test_failures.py`、`tests/test_transport.py`、
`tests/test_calling.py`、`tests/test_dag_agent_failures.py`。
