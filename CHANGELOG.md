# 变更日志

本项目遵循 Keep a Changelog 体例记录面向使用者的变更。

## [Unreleased]

### 重大变更

- map 与 parent 节点现共享同一并发平面,嵌套 map 线程池已移除:既有图的实际并发度可能变化。
  此前每个 map 在调度器 worker 里再开一个 `workers` 大小的池,总线程数是 workers 的平方。

### 新增

- `ResourceRequest` 与 `Dag.run(resource_limits=...)`:按节点声明资源(GPU、供应商配额等),
  运行时分别限流;未声明的节点走 `None` 默认池。多资源按名字确定序获取,不会互等。
- `BudgetPermit` 与 `Budget.reserve()` / `commit()` / `cancel()` 预算 admission API:
  付费调用前先预留额度,缓存命中不占额度,失败与空响应会退还预留。
- 为 terminal `failed` run 增加 `Dag.recover()` 与 `RecoveryReceipt`:恢复决定、理由和证据
  以 append-only receipt 落盘,成功节点继承,旧 attempt 不再需要删除即可安全重试。
  新增 `docs/recovery.md`,`kigumi docs recovery` 可读。
- `kigumi runs show` 在 run 处于终态 `failed` 时打印非破坏性恢复路径。

### 修复

- `BudgetExceeded` 现从 map、scan 和 foreach 保持原类型传播;预算超限会中止后续 fan-out
  item,已在途 item 完成后再统一收尾,而不是把超限埋进聚合失败里。
- `Budget` 预留估算现同时计入 prompt 与 `max_tokens`;此前声明 `max_tokens` 会让预留
  只按输出额度计算,输入 token 不设防。
- 副作用边界改按 executor 类型安装,不再要求节点声明 `retry`:此前未声明重试策略的节点
  在付费调用后崩溃不会留下 `side_effect_started`,恢复时会误判为「未产生副作用」。
- 损坏与缺失现被区分:损坏的 attempt 状态抛 `StateIntegrityError`,损坏的缓存条目按
  `CORRUPT` 上报而非静默当作 miss 重新调用供应商。新增 `CacheLookup` 三态读取结果与
  `CacheIntegrityError`。

## [0.10.1] - 2026-07-30

### 修复

- 修正 `kigumi describe` 与 `Dag.render_summary()`:0.10.0 起 `describe()` 只投影
  结构化的 `prompt_specs`(dict),而摘要渲染仍按扁平字符串拼接,于是任何一个节点声明了
  `prompt_specs=` 就让整张表以 `TypeError: sequence item 0: expected str instance,
  dict found` 崩掉——发布前的图审阅对声明了提示词的项目完全不可用。现在按 `name` 压成
  单元格。

## [0.10.0] - 2026-07-31

### 新增

- 全部 8 条图命令新增 `--graph-arg KEY=VALUE`（可重复）：把参数按名传给 `dag_entry`
  工厂。此前 `dag_entry` 只能是零参 callable，图形状或 `params` 依赖运行时输入的项目
  （按 episode 展开 `foreach`、按输入文件声明 `files`）因此只有两条路：不声明
  `dag_entry`，让这 8 条命令全部没有入口；或者用占位参数构图，让结论失真——`params`
  是 L3 键成分，占位值下 `plan` 预告的是不会被任何真实运行使用的键空间、`explain` 把
  每个节点都报成 `params` 变化，而 `resume` 会带着错误的 `graph_identity` 真的执行节点。
  现在传真实值即可，`plan` / `explain` / `check` 检视的就是真实运行会构出的图。
  缓存键成分与键序未变，不换缓存族。
- `--graph-arg` 的绑定按工厂真实签名进行，不靠调用后捕获 `TypeError`：否则工厂自身
  抛的 `TypeError` 会被误报成 CLI 用法错误。缺必需参数时以 2 退出并指名缺哪几个、给出
  要敲的 `--graph-arg`；给了工厂不接受的名字时列出它实际接受的参数名（静默丢弃会构出
  另一个图）；同名给两次、缺 `=`、positional-only 参数各自报出可执行的修复动作。
  声明 `**kwargs` 的工厂自行裁决参数名，CLI 不代为拒绝；值一律以 `str` 传入，
  只按首个 `=` 切分。零参工厂行为不变。
- `--graph-arg` 只出现在 `kigumi <命令>` 一侧：它才负责构图，而 `Dag.cli()` 拿到的是
  已构好的图，一个不起作用的旗标比没有更糟。`register_graph_commands(graph_args=...)`
  仍是唯一一份子命令定义，`tests/test_graph_cli_entry.py` 把这处不对称钉成"恰好只有
  `--graph-arg`"，其余参数面继续禁止漂移。
- `kigumi doctor` 现在报告配置的 `dag_entry`（未声明时明说图命令不可用）。按配置原文
  报告、不解析工厂：项目运维命令从不 import 项目代码，该边界由测试守住。
- `kigumi init` 骨架说明如何给 `build_dag` 加运行时参数，并写明不要用占位默认值
  换取命令能跑。
- 新增 `tests/test_graph_entry_args.py`：钉住参数真的到达工厂、每种错误形状各自报出
  可执行修复、零参与默认值工厂不回归、`dag` 一侧不提供该旗标、`doctor` 不 import
  项目代码。

## [0.9.0] - 2026-07-31

### 新增

- `PiRpcAdapter` 新增 `extra_config_files`：调用方可向其独占的临时 Pi home 放入额外的
  单段配置文件；内容仅以 SHA-256 纳入非空 adapter identity，拒绝 Pi 自有文件名和已解析
  环境变量值，避免配置覆盖或 credential 进入缓存/证据。
- 新增 `FileRef` 作为 `PromptMaterial` 的第四种来源：读取节点已通过 `files=` / `files_fn=`
  声明的文件内容。声明文件字节本就是 L3 键成分，因此不改变缓存语义，也不绕过 raw-io
  守卫；它补上了“节点内读文件再拼 prompt”这一处声明式缺口。
- 8 个图命令（`check`、`plan`、`graph`、`profile`、`explain`、`describe`、`resume`、
  `retry-resolve`）现在可以经 `kigumi <命令>` 直接使用。此前它们只挂在 `Dag.cli` 上，
  而 `dag` 从来不是一个真实可执行文件——它只是 argparse 的 `prog` 名，`[project.scripts]`
  里没有它，仓库与两个 examples 里也没有任何地方调用 `Dag.cli()`。结果是下游项目装完
  kigumi 之后，`plan` 与 `describe` 这类能力没有任何入口可敲。
- 新增 `[tool.kigumi] dag_entry`（`"module:callable"`，返回 `Dag`）：图命令据此 import
  项目的构图工厂。这是唯一一个"打开一组命令"的配置键，可选；不声明时其余命令照常工作，
  图命令以 2 退出并指名要补的键。模块不可导入、属性不存在、不可调用或返回值不是 `Dag`
  时同样是 2，stderr 指出错在哪一段。
- `kigumi init` 现在生成 `nodes/graph.py` 骨架（`build_dag()` 加可选 `main()`）并写入
  `dag_entry`，同时提示如何注册独立 `dag` 命令。骨架可直接运行：新建项目 `init` 之后
  立刻能跑 `kigumi describe` / `check` / `plan`。
- 新增 `Dag.run_command(args)`：接收已解析的 args 并返回退出码。`Dag.cli()` 与
  `kigumi <命令>` 都经由它 dispatch，`register_graph_commands()` 提供唯一一份子命令定义，
  两条入口不会各自漂移。独立 `dag` 命令仍然可用，且不需要 `dag_entry`。
- 新增 `tests/test_graph_cli_entry.py`：钉住两条入口共享定义与 dispatch、参数面与独立
  写死的期望表一致（避免"对比彼此"在共享 builder 下失效）、`init` 骨架可编译且只 import
  真实导出的名字、缺失与配错 `dag_entry` 各自报出可执行的修复动作。
- 新增 `kigumi brief` 与 `kigumi docs [name]`：把 agent 进场页与全部交付文档随 wheel
  发出，装完即可离线读，不需要回到仓库。两条命令都**不要求**有效 `[tool.kigumi]`，
  因为 agent 需要在项目配好之前就看清能力面；其余命令的 exit 2 行为不变。
- 新增 `docs/brief.md`（英文，`kigumi brief` 打印）：一张"别重造什么"对照表把常见的
  手写实现映射到已有符号，改节点前的只读命令，`kigumi` 与 `dag` 两套 CLI 的完整分工
  （含 `dag describe` / `plan` / `explain` / `check` / `graph` / `profile`），以及节点
  返回值、守卫豁免、静态拓扑、blob 引用等工作纪律。它是下游 coding agent 的第一入口，
  失败形状是不知道库已经拥有什么而另写一份。
- 打包改为用 hatch `force-include` 把 `docs/`、`DESIGN.md` 与 `CHANGELOG.md` 映射进
  wheel，`docs/` 仍是唯一 source of truth。此前 `kigumi/docs/` 是手抄副本且只含四页，
  安装后 `api.md` 的 13 条与 `contracts/README.md` 的 15 条相对链接全部指空；现在
  adoption 与 15 份契约一并交付，链接在两种布局下都可解析。
- 新增 `kigumi/docs.py`（`SHIPPED_DOCS` / `resolve_doc` / `read_doc`）：优先读 wheel 内
  副本，源码树回退到仓库路径，缺页报错而非静默返回空。
- 新增 `tests/test_shipped_docs.py`：锁定 `SHIPPED_DOCS` 与 `force-include` 双向一致、
  仓库里不得再出现手抄的 `kigumi/docs/`、brief 只指向真实符号与已交付页、每个
  `kigumi`/`dag` 子命令都在 brief 中有据可查、按 wheel 布局复现后页间相对链接可达。
  `scripts/verify_dist.py` 与 `scripts/smoke_installed.py` 把交付文档纳入 release 契约。
- `kigumi/__init__.py` docstring 开头加入三行进场协议：改节点前先运行
  `kigumi trace`、`dag plan`、`dag explain`，明确"看清楚再动"的主动工作流。
- `docs/adoption.md` 开头新增「进场协议」段落（在接入步骤之前），把「改节点前做这
  三步」从排障末尾提到文档最显眼的位置，并说明 `dag.plan` / `dag.explain` 需要已注册
  `Dag` 实例的查找方式。
- 新增 `docs/capabilities.md`:扁平、可 grep 的能力索引,左列是"我要做什么"、右列是
  符号名,覆盖 Prompt、调用缓存、DAG、二进制、Agent、实验、测试守卫与运维八组。这是
  面向 coding agent 的第一入口——它的失败形状不是不知道库存在,而是不知道能力面有多宽;
  `tests/test_docs.py` 断言索引里的每个符号都真实存在。
- 新增 `docs/cli.md`：`kigumi` 与 `dag` 两套 CLI 的全部子命令、参数、默认值与退出码，
  并说明两者不互相替代的分工。
- 新增 `docs/api.md`：公开导出速查，覆盖此前无处可查的异常、枚举与策略值、结果与视图
  类型和工具函数。
- 新增 `docs/contracts/README.md`：15 份契约的分组索引，并明确 `Status: Active (X.Y.Z)`
  表示该契约文本最后一次实质修订所在的 release。
- 新增 `tests/test_docs.py`：锁定 README 状态版本与 `kigumi.__version__` 一致、每份契约
  都有版本化 `Status:` 且在索引中、相对 Markdown 链接可解析、`__all__` 全部出现在用户
  文档中。
- `docs/adoption.md` 补齐环境变量总表(14 个 `KIGUMI_*`,含 `FileSlots.from_env` 的三项
  与 Pi Extension 协议的四项)、pytest 插件自动生成的 `kigumi_dry_render[...]` /
  `kigumi_guard` 测试项与 `kigumi_cassette` fixture、`ctx.agent_result`、两个零请求示例
  入口,以及"工作流只用 Python 声明,没有 declarative loader"这条设计边界。
- `README.md` 与 `README.zh-CN.md` 说明内置 judge、pairwise 与 reflection prompt 默认为
  中文文本且三者均可覆盖;`docs/adoption.md` 记录对应常量、参数与槽位契约。
- `AGENTS.md` 补入三条已经生效但未成文的硬规矩:`EvidencePolicy` 只控制保留形态、
  Agent session carry 默认关闭、摘要损坏一律 fail closed。

### 修复

- 修正版本漂移:`README.md` 状态由 0.7.1、`README.zh-CN.md` 由 0.7.0 更新至 0.8.0;
  中文 README 补齐英文版已有的 blob-backed session carry 描述。
- 规范化全部 `docs/contracts/*.md` 的 `Status:` 行:此前 7 份缺版本、2 份完全缺失、
  5 份停留在 0.7.0。

### 变更

- **硬切**：移除 schema-1/0.6 run 的只读降级投影。`profile.py`、`inspect.py` 与 `cli.py`
  现在要求 `run_manifest_schema == 2`；旧 run 以 unsupported manifest 失败，不再伪造
  `unavailable_legacy`。
- **硬切**：移除 `prompts=()` 参数与 `ctx.render()` 方法。节点现在只能通过
  `prompt_specs=()` 声明 Prompt 输入面，以获得注册期可见的完整输入面、managed lineage
  与 selected-only cache；`load_template` / `render_template` 仍用于非 LLM 输出。
- 修复 schema 常量分散和损坏 sidecar 的解释降级：run sidecar、failure、candidate 与
  attempt receipt 都由集中常量校验，缺少或损坏 `key_components` 的 sidecar 直接 fail closed。

## [0.8.0] - 2026-07-26

### 新增

- 新增 `Dag.agent_scan()`：每个运行时 item 仍按 scan 的线性 carry 语义串行执行，但 miss
  通过既有 Agent adapter、slot、evidence、retry/resume、attachment 与 exact publish 边界。
  Agent builder 显式接收 `(item, carry, inputs, ctx)`；每项独立缓存，命中前缀可重建 carry。
- `AgentRunContext` 新增可选 `session_in` 与 `record_session`。adapter 记录的 transcript 进入
  内容寻址 blob store，canonical Agent artifact 以非物化 `session` attachment 引用它；
  scan 使用 `carry_fn=lambda artifact: artifact["session"]` 将其传给下一项。
- `PiRpcAdapter` 新增 `session_carry=False` 与 `session_max_bytes=2097152`。启用后使用显式
  `--session <workspace>/.kigumi/pi-home/session.jsonl`，首轮自动创建、后续从 carry 恢复，
  成功后完整回收；超限、空、损坏、未落盘或 credential 泄漏均失败，不静默截断。

### 变更

- Pi session header 的 `cwd` 在输入和输出边界规范化为 `"."`，避免上一 scan item 的临时
  workspace 删除后导致下一项 RPC 启动失败，也避免临时绝对路径进入可缓存 transcript。
- **0.8 硬切**：`CACHE_SCHEMA` 从 5 升至 6，`agent_schema` 从 2 升至 3，
  `agent_executor_schema` 从 4 升至 5，Pi adapter identity schema 从 2 升至 3。
  这是 canonical Agent session attachment 与 Agent scan 执行语义的有意完整 L3 缓存换族；
  0.7 cache 自然 miss，不提供迁移或兼容 shim。

## [0.7.1] - 2026-07-25

### 修复

- Pi RPC evidence 不再重复保存 `message_update` 的累计完整消息。每个 update 改为保存
  脱敏 canonical event 的 SHA-256、原始字节数和 thinking-content 标记；`message_end`、
  tool、response 与 settled 等非累计事件仍保存完整脱敏记录。`rpc_max_bytes` 现在约束
  规范化 JSONL evidence，同时继续作为单个 wire JSONL record 的硬上限。
- `AgentLimits` 现在拒绝 `rpc_max_bytes > max_single_file_bytes`，避免 finally 阶段记录
  `pi-rpc.jsonl` 时用第二个额度错误掩盖首个执行结果。

### 变更

- **硬切**：`agent_executor_schema` 从 3 升至 4，Pi adapter identity schema 从 1 升至 2，
  RPC identity 改为 `strict-lf-jsonl+normalized-evidence-v2`。旧 Agent L3 cache 自然 miss，
  不提供原始累计 RPC evidence 的兼容或恢复路径。

## [0.7.0] - 2026-07-24

### 新增

- 新增声明式分层 Prompt：`PromptRef`、`InputRef`、`ParamRef`、`ItemRef`、
  `CarryRef`、`PromptAxis`、`PromptLayer`、`PromptMaterial`、`PromptSpec`、
  `ResolvedPrompt` 与 `PromptResolution` 全部顶层导出。Dag 的 node/agent/map/scan/
  foreach 及 Subgraph 的 node/map/scan 支持 `prompt_specs=()`；严格 selector、精确 slot、
  无 slot fragment 与统一 `inject()` material 在缓存 lookup 和副作用前解析。
- 每个 run 新增不可变 `PromptCatalogSnapshot`。同一 run 一次读取全部声明文件；axis 只把
  实际 selection 与所选 fragment 内容放入 L3 key，未选中候选仍进入完整 run graph identity。
  修改未选中 variant 可复用 selected-only L3 cache，但旧 run 因声明 universe 漂移拒绝
  resume。
- CALL、validated repair 与 Agent instruction 新增 Prompt lineage。直接调用
  `ResolvedPrompt` 为 managed；字符串拼接后自然丢失 lineage 并标记 unmanaged。多 CALL、
  L1 hit、primary/repair round、Agent success/failure/capacity/ambiguous 与 live side-effect
  boundary 均保存当前 resolution。
- 新增 `Dag.profile(run_id=None, include_content=False)`、`dag profile` 和
  `dag graph --prompts`。`workflow_profile_schema=1` canonical IR 汇总静态图、Subgraph、
  Prompt 候选与来源边；运行画像只读持久化 state，展示 node/item/attempt/CALL 的
  current/origin selection、cache、model、usage、repair、failure/retry/ambiguous/resume。
  `describe`、trace 与 runs show 复用该 IR；Markdown 同时输出 Mermaid 总图与 Prompt 总表。

### 变更

- **0.7 硬切**：`CACHE_SCHEMA` 从 4 升至 5，node cache envelope 从 schema 2 升至 3，
  run manifest 从 schema 1 升至 2，attempt receipt、success candidate 与 run sidecar
  升至 schema 2；新增 `prompt_resolution_schema=1`、`workflow_profile_schema=1`。
  `agent_schema=2` 与 `agent_executor_schema=3` 保持不变。0.6 L3 cache 自然 miss，不提供
  迁移器或兼容 shim。
- schema-2 run manifest 绑定完整 Prompt 候选 universe 与 WorkflowProfile digest；
  resume 重新验证 snapshot、selection、resolution、candidate、artifact、origin、sidecar
  与 blob，并记录 `resume_count`/`last_resumed_at`。0.6/schema-1 run 仅只读显示
  `resolution_status=unavailable_legacy`，明确拒绝 resume。
- cache-hit sidecar 同时保存本 run 重新解析的 current resolution 与 immutable cache origin
  的历史实际调用。`full`、`redacted`、`hash_only` 下 resolution 结构同形且不保存原文；
  profile 内容展开仍服从该 run 的 EvidencePolicy，后者不是访问控制。
- Agent builder 先构造并绑定 managed instruction，再申请全局 slot；cache hit 仍跳过
  builder 和 slot，capacity failure 则保留已解析 instruction lineage。

### 保持兼容

- 既有 `prompts=()`、`ctx.render()`、字符串 `ctx.call()`、字符串 Agent instruction 和
  chat message list 保留；未采用新声明时明确显示为 unmanaged。没有远端 registry、
  Jinja/宏、隐式 default/override、动态 Prompt/DAG 拓扑、模型生成 variant 或自动 promotion。

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
