# 变更日志

本项目遵循 Keep a Changelog 体例记录面向使用者的变更。

## [0.6.0] - 2026-07-24

### 新增

- 新增 CALL/Agent 共用的 `ProviderFailure`、`ProviderFailureStage`、
  `ProviderFailureKind`，以及仅描述 Agent spawn/version/process/protocol/policy/capacity
  故障的 `AgentExecutionFailure` 与 `AgentRuntimeFailureCode`。控制流只依赖 wire status、
  typed SDK 字段和运行时事实；provider prose 只保留脱敏摘要。
- 新增 `EvidencePolicy`，分别控制 request、response、stderr 与 trajectory 的 `full`、
  `redacted`、`hash_only` 保留模式。所有模式先强制清理 credential、header secret 和 URL
  query；canonical artifact 与执行 evidence 分离，policy digest 绑定不可变 origin provenance。
- 新增默认 1 slot 的跨线程/跨进程 Agent 容量门禁；项目配置与
  `KIGUMI_AGENT_SLOTS`、`KIGUMI_AGENT_LOCK_DIR`、
  `KIGUMI_AGENT_SLOT_TIMEOUT_SECONDS` 可覆盖。cache hit 不占 slot，排队不消耗执行 timeout。
- 新增显式 `RetryPolicy`、`RetrySchedule`、`RetryExhausted` 与
  `AmbiguousAttemptError`。`Dag.node`、`Dag.agent`、map/scan 可声明 durable retry；默认
  `retry=None`，不会自动重试。
- 新增 schema-1 run manifest 与 attempt receipt、成功 candidate、确定性 full jitter、
  provider retry-after 下界、ambiguous side-effect 裁决和 `Dag.resume()`。
  `dag resume`、`dag retry-resolve`、`kigumi runs show`、`kigumi trace` 与 run-aware graph
  暴露 attempt、due time、typed failure、evidence policy 和恢复状态。

### 变更

- **硬切**：`CACHE_SCHEMA` 从 3 升至 4，node cache envelope 升至 schema 2；
  `agent_executor_schema` 升至 3，canonical Agent artifact 升至 `agent_schema=2`。
  0.5.x L3/Agent cache 自然 miss，不提供迁移或兼容 shim。
- **硬切**：0.6 run 由 `_run.json` 绑定 graph、targets、force、源码/libs、retry 和 evidence
  policy identity。缺少 manifest 的 0.5.x run 仅 best-effort 只读，明确拒绝 resume；
  同一 0.6 run 的声明漂移 fail closed。
- Agent canonical artifact 只保留 task/completion、Agent identity、collected attachments、
  published outputs 和 `files`；usage、duration、workspace manifest、RPC、stderr、
  trajectory、Hook 与执行 metadata 迁入 hash-bound origin provenance。
- CALL failed metadata 与 Agent failure JSON 改为 canonical typed failure。429、5xx、
  timeout、connection、401/403、invalid request、model mismatch 与 unknown 均有稳定分类。
- Pi bridge 绑定独立路径策略资源，拒绝 `.kigumi/**` 的大小写别名；adapter 关闭并监测
  hidden retry，验证 thinking-off 和 response model observation，扫描 workspace credential
  泄漏，并压缩累积 streaming trajectory。
- 标准 transport 分别配置 transport、length 与 empty-response retry；durable CALL 在任一
  hidden retry 非零时于 provider side effect 前拒绝。`Response.model_observed` 明确区分实际
  wire observation 与 requested-model fallback。
- GC 的 blob reachability 扩展到 retained sidecar、failure、attempt receipt 和 candidate；
  普通 materializer 仍不解释 evidence。

### 移除

- 移除旧 Agent failure code API；不保留装饰性 failure 枚举或兼容别名。
- 不引入 ArtifactRef、跨图 handoff、outbox、自动 winner、Agent factory 或动态 Agent 拓扑。

## [0.5.0] - 2026-07-23

### 新增

- 新增 `Dag.agent` 与 provider-neutral `AgentAdapter` 契约。外部 Agent 在唯一 staging
  workspace 中运行；只有声明输入会被复制进去，只有 collect 的文件会成为
  `kigumi_attachment`，只有 exact publish 映射会进入既有 `files` / `kigumi_blob`
  物化和输出所有权路径。
- 新增内容寻址目录胶囊 `AgentSpec`、显式 `AgentLimits`、固定 `AgentCompletion` 与原生
  `PiRpcAdapter`。Pi 在启动前精确校验版本；固定 bridge Extension 提供 workspace-rooted
  文件工具与 `submit_result`，严格 LF JSONL、进程组 timeout、脱敏 RPC/stderr/trajectory、
  Hook evidence 和 usage/cost 全部进入可由 GC 追踪的 attachment。Pi 由用户安装，Kigumi
  不安装或升级 Node/Pi；staging 和工具 root 不是 OS sandbox。
- `bench` 新增 `ExperimentSubject`、`FunctionSubject`、`CallerSubject`、`DagSubject`、
  `AgentSubject`、`TrialContext` 与 `TrialObservation`，普通函数、Caller、DAG 和单 Agent
  可进入同一隔离实验网格。Agent trial 固定 target `cache="off"`；报告 schema v2 保留完整
  Judgment、usage/null、seed 声明、trajectory/raw evidence 与逐格错误。

### 变更

- **硬切**：`Variant.task` 与 `bench(..., caller_factory=...)` 已删除；改用
  `Variant(subject=...)`。不提供兼容 shim。
- **硬切**：删除旧 optional agent-protocol 实现、extra、测试和公开表述；删除 `AgentConfig`，
  `Dag.agent(..., config=...)` 改为 `Dag.agent(..., spec=AgentSpec)`，不提供 shim。
- Agent 静态执行语义通过既有 `external` 键成分中的 `agent_executor_schema=2` 换族；键包含
  capsule、adapter/Pi exact version、bridge、model、tool 与 limits，Skill/Hook/manifest 的增删改
  都会 miss。这是有意的 Agent 缓存换族。
  普通节点的键标签、规范 JSON、`files`/`kigumi_blob` 字节语义均未改变，因此
  `CACHE_SCHEMA` 保持 3，不发生全局 L3 缓存换族。

## [0.4.0] - 2026-07-14

### 变更

- `CassetteTransport` 现在拒绝缺少 `request_sha` 的旧格式磁带并要求重录（此前静默按序重放）。

## [0.3.1] - 2026-07-14

### 新增

- `Response` 新增 `provider_response_id`:stdlib transport 从 provider 原始响应的 `id` 提取,
  L1 缓存 sidecar 的 `meta` 与 `caller.calls` 溯源记录均保留它;缓存命中时从 sidecar 回读,
  旧缓存条目缺失该字段时记为 `null`。该字段只做溯源,不进入任何缓存键。

## [0.3.0] - 2026-07-14

### 新增

- DAG 普通节点、`foreach`、map/scan 共享依赖与 `Subgraph` 节点新增边上消费投影
  `consumes`。声明后，节点只接收 canonical JSON 投影视图，`upstream:<dep>` 也只按该视图
  摘要入键；`plan`、`run`、`explain` 与 `describe` 使用同一声明语义。

### 变更

- `CACHE_SCHEMA` 从 2 升至 3，以纳入可选 `consumes` 键推导分支；这是有意的完整 L3
  节点/item 缓存换族。未声明投影的依赖标签、摘要与输入形态不变，L1 语义不变。

## [0.2.0] - 2026-07-14

### 新增

- 新增 `OutputOwnershipError` 与单次 `Dag.run()` 内的框架物化路径所有权：普通节点、
  map/scan 项在写盘前原子认领完整输出集合；重复路径、文本/blob 冲突及跨生产者覆盖会
  在写盘前失败；符号链接与目标文件系统等价的大小写/Unicode 别名按同一目标处理。
  该所有权只描述 kigumi 管理的物化路径，不表示法律上的数据权利。
- 新增公开 `CachePolicy = Literal["auto", "refresh", "off"]`，覆盖普通节点、
  foreach 节点和 map/scan 项；新增只入摘要的 `external_fingerprint`，用于声明外部状态。
- 新增可重复挂载的静态 `Subgraph` 模板与 `Dag.mount()`。子图复用现有注册表、调度器、
  缓存、检查点和 store；运行时动态拓扑仍仅有 map/scan，模型只决定内容，不决定可执行图。
- run sidecar 与 `kigumi trace` 新增稳定排序的 `outputs` 和 `cache_policy` 溯源字段；
  `Dag.describe()` 新增缓存策略、外部指纹存在性和子图边界。

### 变更

- `CACHE_SCHEMA` 从 1 升至 2，允许可选 `external` 键成分；这是有意的完整 L3
  节点/item 缓存换族。缓存策略本身不入键，L1 语义不变。
- 挂载节点的检查点身份按 qualified 节点限定：普通挂载为
  `approval@namespace.local`，map/scan 项再追加 `@item`；重复挂载可独立挂起和批准。
- 实际调用过检查点的执行不再写 L3 节点/item artifact，审批结果不会经缓存泄漏到新 run、
  其他挂载或其他 item；新 run 会重新挂起，未实际调用检查点的条件分支仍遵循声明的缓存策略。
- 成功批准会删除当前 `.pending.json`；之后 payload 变化会使旧批失效并生成新的 pending
  记录，再次批准会替换批准数据并清理该 marker。
- 强化标识符路径边界：节点名、动态 item ID、Subgraph 单段、`run_id` 与检查点名均拒绝
  路径分隔符及 `.`/`..` 逃逸形态；Subgraph 单段同时拒绝 `.`/`@`，检查点名仍允许内部
  `.` 与 `@` 作为限定符。

- `kigumi` 键成分改为联合覆盖 `prompt.py` 与 `repair.py` 的 prompt 生成字节，并以
  `CACHE_SCHEMA` 管理显式缓存语义版本；**本版本导致 L3 节点缓存全量失效（键成分
  `kigumi` 换构）。**
- 执行信封收敛至私有 `kigumi._execution`，并公开 `observe()` 以收集上下文内的
  `LLMCaller` 调用；差分探针已验证键成分与 sidecar 字节不变。
- 新增 `bench` / `Variant`：以唯一现状对照和显式假设约束结构切法探索，产出可归档的
  变体×样例×种子评估报告、样例级 judgments 与调用成本；不包含胜负裁决或自动接线，
  **不改变任何缓存键成分。**
- 新增 coding-agent 可观测性 CLI：`kigumi trace` 从 run 直接联接节点/map 项、键成分、
  LLM 调用与 L1 载荷；`kigumi call` 用 key 前缀读取完整输入输出；`kigumi diff` 增加
  键成分差分与 `--json`。新增独立的 `llm_cache_dir` 配置，默认
  `artifacts/_llm`；**不改变任何缓存键或 L1 载荷结构。**
- `2174f73`：raw-I/O 守卫补齐注册、项目扫描和测试三环；run 目录改为数字感知排序；
  并发失败保留其余失败附注。真实请求标记统一为 `live`，旧 `kigumi_live` 已移除，
  运行 live 测试必须显式设置 `KIGUMI_LIVE=1`。
- `4ac2b60`：四种 DAG 渲染迁入 `kigumi.views`，运行态渲染数据提取为共用边界，
  缩小 `dag.py` 的职责面。
- `db87336`：节点键成分收敛到 `Dag._key_components` 单点；run、plan、explain
  分别注入已解析的上游结果，并以差分探针确认键字节不变。
- `1e40b20`：`libs` 哈希改为剥离 docstring/注释后的 AST 归一化；语法残破文件退回
  原文哈希。**这是 `libs` 成分的缓存换族，所有既有节点缓存必须视为失效。**

### 移除

- 移除四个兼容入口：`from kigumi.dag import approve_checkpoint` 等存储层转发、
  `FakeTransport.calls`、点分顶层键解析，以及私有 `_next_run_id`。
