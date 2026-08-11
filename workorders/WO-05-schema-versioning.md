# WO-05: prompt-resolution schema 版本化错误与迁移（#21）

> 状态：READY ｜ 波次 1 ｜ 风险：低 ｜ 缓存换族：否（不动 schema 1 内容）
> Issue: #21 ｜ 实测：0.13.0 REPRODUCIBLE

## 实测现状
`PROMPT_RESOLUTION_SCHEMA = 1`（`kigumi/prompt.py:38`）。`validate_prompt_resolution_record`（`prompt.py:780`）在 `:788-792` 校验 schema 类型/相等，`:793` 对 schema 0 和 schema 2 抛**同一句** generic 错误 `"persisted Prompt resolution has invalid schema"`。无任何 `migrat`/`version` 分发；`:974` 仅 `"unsupported prompt resolution schema"`，无源/目标版本指引。契约 `docs/contracts/prompt-resolution.md:47-54` 只认 schema 1。

## 目标
让 schema 不匹配时给出**带版本号、可操作**的错误，并为未来迁移建立分发骨架。fail-closed 不变，但不再是"无路径的 fail-closed"。

## 改动
1. **版本化错误**：schema 不匹配时，错误信息包含**持久化的版本**与**当前支持的版本**，以及操作指引。例：
   - 持久化版本 < 当前：`"persisted Prompt resolution schema 0 is older than supported schema 1; no migration available — rebuild required"`（若为该版本注册了迁移则执行之）。
   - 持久化版本 > 当前：`"persisted Prompt resolution schema N is newer than supported schema 1; upgrade kigumi"`。
2. **迁移分发骨架**：在 `prompt.py` 增加一个 `dict[int, Callable[[record], record]]` 形态的版本→迁移函数注册表（当前可为空）。校验先查表：有迁移则迁移到当前版本并保留原记录字段；无迁移则抛上述版本化错误。分发逻辑独立成小函数，便于后续版本填充。
3. **契约文档** `docs/contracts/prompt-resolution.md`：说明版本不匹配的错误形态与 rebuild/upgrade 指引。

## 约束
- RED→GREEN。
- **不引入 schema 2**，不改变 schema 1 的字段或字节——否则即缓存换族。本工单只加错误信息 + 空迁移骨架。
- fail-closed 语义不弱化：未知/损坏记录仍拒绝。

## 非目标
- 真正编写 0→1 的迁移逻辑（无旧格式可迁）。
- 其它持久化格式（cache envelope/runstate）的迁移——后续按同模式推广。

## 验收
- `uv run pytest tests/test_prompt.py tests/test_schema_consistency.py -q` 全绿。
- 新增测试：schema 0/schema 2 的错误信息分别含版本号与 rebuild/upgrade 指引；注册表分发路径可测。
- `uv run pytest -q` 全绿；ruff 通过。

## CHANGELOG
写入 `[Unreleased]`（中文）。

## 输出
report：改动文件 + before/after 错误信息对比 + 完整测试输出。
