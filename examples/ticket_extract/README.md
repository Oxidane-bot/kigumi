# 客服工单抽取换族试点

运行：`uv run python examples/ticket_extract/demo.py`。示例以固定 seed 生成 150 张合成工单，
不用真实请求或 API key。`kigumi.testing.ScriptedTransport` 的 callable responder 从请求内
的工单正文解析字段；5 张工单先返回坏 JSON 后修复，2 张工单固定抽错一个字段，用来验证
非满分的确定性指标。

流水线为 `ingest -> extract(map) -> validate -> stats -> report`。`extract` 用每张工单
的 `files_fn` 进入逐项缓存键，artifact 只保留抽取字段；`validate` 从 ingest 的真值按字段
比较，完全没有 LLM 评委；`report` 是唯一集合级 LLM 调用。

## 框架摩擦与缺口

1. **已文档澄清：map 聚合状态混入 `certain`。** 场景：改 3 张工单并对 report 预告。期望：
   `certain` 仅列出确定会执行的 3 个 `extract@...` 项。实际：除三项外还包含 `extract`
   聚合节点；它只是从 item cache 重建聚合，零 LLM 调用。裁决：语义正确（聚合重建是确定
   发生的工作），接入指南已写明读法——估算成本按展开项数，节点级条目只代表聚合重建。
2. **已文档澄清：逐项输入与清单节点的职责切分。** 场景：既要让 ingest 读工单集，又要让
   改 3 张正文时预告展开并只失效 3 项。期望：接入指南给出这一形态的完整范式。实际：若
   ingest 把全部正文放进 artifact，任一变更会使 map 源未知，无法展开预告。裁决："目录
   清单 + files_fn"范式已进接入指南 map 一节，含"item 不携带节点不消费的字段"的缓存键
   卫生要求。

## 规模测量

以下数字由本机一次 `demo.py` 实跑后回填；计时包含框架调度与本地脚本化响应，artifacts
文件数在 gc 前统计，适合观察 150 项 map 的落盘规模。

- 首跑：0.2450s
- 预告：0.0611s
- 全命中重跑：0.1516s
- artifacts 文件数：1246（gc 删除 3 个过期缓存条目）
