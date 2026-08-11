# WO-04: guard 三值判定（#9、#10、#16）

> 状态：NEEDS-DESIGN（先 WO-04a 设计子工单）｜ 波次 3 ｜ 风险：高 ｜ 缓存换族：否
> Issues: #9, #10, #16 ｜ 实测：三者均 REPRODUCIBLE

## 实测现状
- `Finding`/`RawIOFinding` **无 severity/rule 字段**(`enforce.py:32,43`)，仅 location+snippet+waiver 状态。
- guard 是**二元**(waived vs violation)，任何 unwaived finding 在 `cli.py:855,868` 非零退出。
- **#9**:unresolved alias 转 OPAQUE(`enforce.py:565`)，`_is_model_call` 对 MODEL 与 OPAQUE 同返 True(`:885,:899`)。
- **#10**:`.call`/`.llm` 仅按拼写分类，不看 receiver provenance(`enforce.py:426`)——`Formatter().call(value)` 被误判。
- **#16**:`getattr` 在动态 callable 集(`enforce.py:25`)，非调用字面量 `getattr(value,"model_dump",None)` 也产生不可豁免结构 finding(`:1585,:1828`)。
- **契约硬约束**:`docs/contracts/guards.md:27,31` 目前把 getattr 与 opaque 调用**写成有意的硬切**。

## 目标
引入**三值判定**:`proven-unsafe`(error) / `unknown`(warning/review) / `proven-safe`。让"分析器不确定"不再冒充 error，一次收掉 OPAQUE、拼写、getattr 三类误报。

## 关键设计决策（WO-04a 必须先定，不在本工单实现）
1. `Finding` 如何承载 severity/verdict——新增字段的序列化与消费端（cli exit code、explain、CI）语义。
2. `unknown` 的默认处置：warning（不阻塞）还是 review（阻塞但可分诊）？exit code 如何区分。
3. receiver provenance 证据规则（#10）：何种 receiver 算 proven deterministic。
4. 非调用字面量 getattr probe 的安全判定（#16）：如何区分"字面量名探测"与"动态执行"。
5. **必须先改契约** `guards.md`：三值语义取代现行"硬切"表述——这是行为契约变更，需维护者认可。
6. 可配置 qualified-method allowlist 的形态。

## 执行方式
- **WO-04a（设计，Claude 侧）**：产出三值 verdict 的类型设计 + 契约修订草案 + 迁移/兼容方案，供审批。**Codex 不实现**。
- **WO-04b（实现，Codex）**：按批准方案实现 + guard/explain/cli 消费端 + 测试。

## 约束
- 不弱化对**已证实** model 调用的硬切（proven-unsafe 仍 error）。
- 行为契约变更需 RED 测试锚定 + CHANGELOG。

## 验收（WO-04b）
- `uv run pytest tests/test_enforce.py -q` 全绿 + 新增三值矩阵测试。
- #9/#10/#16 三个 repro 分别落入预期 verdict。
- `uv run pytest -q` 全绿；ruff 通过。

## CHANGELOG
行为变更，写入 `[Unreleased]`（中文）。

## 输出
WO-04a：设计文档 + 契约修订草案。WO-04b：改动文件 + 测试输出。
