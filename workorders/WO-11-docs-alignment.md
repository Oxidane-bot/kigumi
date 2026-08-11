# WO-11: 文档与实现对齐（#24、#4）

> 状态：READY ｜ 波次 1 ｜ 风险：极低 ｜ 缓存换族：否
> Issues: #24, #4 ｜ 对应 P5（文档不可跑在实现前面）

## 目标
收窄两处**超出实现**的文档/ changelog 表述，使其只承诺实现真正保证的事。

## 现状
- **#24**:`docs/adoption.md:247-250` 与 `docs/contracts/cache-key.md:40-43` 称缓存身份包含 `source_dirs` 下**所有**源码，但实现只哈希每个节点**静态可达 import 闭包**(`dag.py:5395-5425,8064-8093,8105-8155`)。改 `src/unrelated.py`（在 source_dirs 但不可达）不进入该节点 libs 身份。
- **#4**:0.11.0 changelog 称"附件一致性由框架保证……消费者不必再手动核对「已声明」与「已发送」附件是否一致"。实测：附件**内容**确实进缓存键（a.png vs b.png 键不同），`_expand_file_reference` 也会在哈希后变更时拒绝（`calling.py:862`）；但 `files=` 声明 a.png 而实际 attach b.png **不被强制**——attachment expansion 不查 `node.files`。changelog 那句读作"可以删掉声明核对"，会误导。

## 改动
1. **#24 — 文档**:把 `adoption.md` / `cache-key.md` 中"source_dirs 下所有源码"改为"每个节点**静态可达 import 闭包**"，并说明不可达文件不进入该节点身份。（选择改文档而非改实现——成本最低且语义正确。）
2. **#4 — changelog/文档**：把"消费者不必再手动核对已声明与已发送附件"收窄为"附件**内容**已进入缓存键，因此不必手动核对**内容哈希**"；明确 `files=` 声明与实际 attach 路径的一致性**框架不强制**，声明核对仍属调用方责任。

## 约束
- 纯文档，不改运行行为。
- 措辞准确，不引入新的过度承诺。

## 验收
- `uv run pytest tests/test_docs.py -q` 全绿（若有 pin 文档的测试）。
- `uv run pytest -q` 全绿。
- 报告给出改动前后的句子对比。

## CHANGELOG
文档修正，写入 `[Unreleased]`（中文）。

## 输出
report：改动文件 + 前后句子对比 + 测试输出。
