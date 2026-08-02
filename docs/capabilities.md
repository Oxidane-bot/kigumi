# 能力索引

按"我要做什么"查符号名,一行一个能力。这里不解释用法:拿到符号后,叙述见
[接入指南](adoption.md),签名与失效见 [API 速查](api.md),承诺见
[契约索引](contracts/README.md),命令见 [CLI 参考](cli.md)。

装了这个库之后,本页与上述各页都能用 `kigumi docs <name>` 离线读;
`kigumi brief` 是更短的 agent 进场页(见 [brief.md](brief.md))。

先扫这张表再动手。撞不到需要的行,大概率是这个库不做那件事——边界见
adoption.md 的"设计边界"一节。

## Prompt 与材料

| 需要 | 用 |
| --- | --- |
| 把动态内容拼进 prompt(唯一入口,自动围栏) | `inject` |
| 版本化提示词模板 + 严格槽位渲染 | `load_template` / `render_template` / `slot_names` |
| 声明式组合的多层 prompt | `PromptSpec` / `PromptLayer` |
| A/B 或多分支提示词(有限选择轴) | `PromptAxis` |
| 运行期材料进 prompt 但保留 lineage | `PromptMaterial` / `FileRef` |
| 类型化请求消息、附件与响应 schema | `Message` / `Attachment` / `ResponseSpec` |
| 让选择轴读上游/参数/item/carry | `InputRef` / `ParamRef` / `ItemRef` / `CarryRef` |
| 具名条件段(值为 None 整段不渲染) | `section` |
| 从 pydantic model 生成输出格式说明段 | `schema_format_section` |
| 安全边界截断长文本 | `clip` |
| 渲染列表为 JSON 或 bullets | `render_items` |
| 节点内取已解析 prompt | `ctx.resolve_prompt` |

## 调用、缓存与校验

| 需要 | 用 |
| --- | --- |
| 内容寻址缓存 + 确定性重放 | `LLMCaller(cache_dir=..., seed=...)` |
| 结构化输出 + 有界修复环 | `call_validated` / `ctx.call_validated` |
| 自定义校验的修复环 | `repair_loop` / `ctx.repair` |
| token 预算上限 | `Budget` |
| 预算调用前预留、成功提交、失败退款 | `BudgetPermit` / `Budget` |
| 排练全流程不发真实请求 | `LLMCaller(dry=True)`(miss 抛 `DryRunError`) |
| 换 provider / 自实现传输层 | `Transport` 协议 / `LiteLLMTransport` / `StdlibTransport` |
| 429 自适应并发 | `AdaptiveCapacity` / `FileSlots` |
| 抓取每次调用的结构化事件 | `observe` |
| 区分缓存缺失与损坏并阻止静默重算 | `CacheLookup` / `CacheIntegrityError` |
| 在 cache/provider 前检查请求大小 | `preflight` / `PreflightPolicy` / `RequestTooLarge` |

## DAG 编排

| 需要 | 用 |
| --- | --- |
| 声明流水线节点 | `Dag` / `@dag.node` |
| 数据驱动扇出(项独立) | `@dag.map` |
| 线性扇出带 carry | `@dag.scan` |
| 注册期固化的清单 | `@dag.foreach` |
| 按节点限制 GPU、API 或 CPU 并发 | `ResourceRequest` / `dag.run(resource_limits=...)` |
| 静态可复用子图 | `Subgraph` / `dag.mount` |
| 只消费上游局部(缩小失效面) | `consumes=` |
| 强制重算某节点 | `dag.run(force=[...])` |
| 花钱前预览爆炸半径 | `dag.plan()` |
| 查某节点为什么重算 | `dag.explain("node")` / `dag.explain("node@item")` |
| 比较两次 run | `dag.diff()` / `diff_runs` |
| 人工审批卡点 | `ctx.checkpoint` / `dag.approve` |
| 声明式重试(不在进程内 sleep) | `RetryPolicy` + `dag.resume()` |
| 从 terminal failed run 做带理由的 append-only recovery | `dag.recover()` / `RecoveryReceipt` |
| durable 状态损坏时 fail closed | `StateIntegrityError` |
| 声明外部不确定输入 | `external_fingerprint=` |
| 图形状整体过目 | `dag.render_mermaid` / `dag.describe` / `dag.profile` |

## 二进制与文件

| 需要 | 用 |
| --- | --- |
| 让文件内容进缓存键 | `files=` / `files_fn=` / 消息里的 `{"kigumi_file": path}` |
| 节点内读文件(过声明校验) | `ctx.read_text` / `ctx.read_bytes` |
| 产出大文件不进 artifact | `ctx.emit_file` / `ctx.ingest_file` / `BlobStore` |
| 读上游 Agent 的 attachment | `ctx.agent_result` |

## 外部 Agent

| 需要 | 用 |
| --- | --- |
| 把外部 coding agent 当普通节点跑 | `@dag.agent` |
| 多轮串行修订(session 作 carry) | `@dag.agent_scan` + `session_carry=True` |
| 内容寻址的 agent 配置胶囊 | `AgentSpec` / `AgentLimits` / `AgentTask` / `AgentPublish` |
| 接 Pi runtime | `PiRpcAdapter` |
| 接自己的 agent runtime | `AgentAdapter` 协议 |
| 控制证据保留形态 | `EvidencePolicy` |
| 跨进程限制 agent 并发 | `[tool.kigumi].agent_slots` / `KIGUMI_AGENT_SLOTS` |

## 评估与实验

| 需要 | 用 |
| --- | --- |
| 变体 x 样例 x 种子的隔离证据网格 | `bench` / `Variant` |
| 实验主体(函数/caller/DAG/Agent) | `FunctionSubject` / `CallerSubject` / `DagSubject` / `AgentSubject` |
| LLM 评委 | `llm_judge` / `pairwise_judge` / `Judgment` |
| 闸门没过就不烧评委调用 | `gated_metric` |
| 提示词自动进化(可续跑) | `evolve_prompt` |

内置评委与反思提示词默认是中文文本,三者都可覆盖,见 adoption.md。

## 测试与守卫

| 需要 | 用 |
| --- | --- |
| 零真实请求的单测 | `ScriptedTransport` / `kigumi.testing.FakeTransport` |
| 录制一次真实响应后离线重放 | `kigumi.testing.CassetteTransport` |
| 真实请求测试的凭证门 | `@pytest.mark.live` + `kigumi.testing.skip_unless_env` + `KIGUMI_LIVE=1` |
| 模板槽位自动体检 | pytest 插件自动生成 `kigumi_dry_render[...]` |
| 拒绝裸循环调用与未声明读文件 | 注册环 + `kigumi guard` + `kigumi_guard` 测试项 |
| 直接取得循环裸 LLM findings | `kigumi.enforce.check_source` / `kigumi.enforce.check_paths` / `kigumi.enforce.Finding` |
| 直接取得节点 raw-I/O findings | `kigumi.enforce.check_raw_io_source` / `kigumi.enforce.check_raw_io_node_source` / `kigumi.enforce.check_raw_io_node_paths` / `kigumi.enforce.RawIOFinding` |
| 豁免守卫(必写理由) | `# kigumi: raw-llm-ok <理由>` / `# kigumi: raw-io-ok <理由>` |

## 运维与排障

| 需要 | 用 |
| --- | --- |
| 脚手架一个新项目 | `kigumi init [--hooks]` |
| 让 agent 一眼看清这个库已有什么(别重造) | `kigumi brief` |
| 离线读随 wheel 交付的文档页 | `kigumi docs [name]` |
| 路径/密钥/模板体检 | `kigumi doctor` |
| 看一次 run 的完整证据链 | `kigumi trace <run_id>` / `kigumi runs show` |
| 取某次 LLM 调用的载荷 | `kigumi call <key_prefix> --field messages\|response` |
| 清理旧缓存与产物 | `kigumi gc --keep N` |
| 程序内只读联接 run 与调用 | `kigumi.inspect.trace_run` / `kigumi.inspect.load_call` |
| 项目路径与容量配置 | `[tool.kigumi]`(见 adoption.md 环境变量总表) |
