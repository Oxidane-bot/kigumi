# 变更日志

本项目遵循 Keep a Changelog 体例记录面向使用者的变更。

## [Unreleased]

### 重大变更

- `Dag.recover()` 发现已存在但不可信的 schema-2 durable manifest、target bindings 或 force
  bindings 时现在抛出 `RunManifestError`；该异常继承自 `RuntimeError` 而非 `ValueError`，捕获
  `ValueError` 的调用方需相应更新。manifest 可信但状态不满足前置条件（run 不处于终态
  failed、恢复目标未注册）仍抛 `ValueError`。

### 修复

- `sha256_file()` 现在会在读取后重新校验绑定文件描述符的身份；哈希期间文件被截断等变化会
  fail closed，不再返回与完整文件不一致的摘要。
- 修复 `Dag.resume()` 对普通节点与完整恢复的 map/scan 聚合节点的 `post_node` 回调行为不一致；
  从 durable state 完整恢复的节点现在都不会调用该回调。
- Agent scan 节点因 slot 容量超时失败时，现在与普通 Agent 节点一样写出 `failures/<node>.json`
  失败证据，不再只抛出异常。
- map 的重复 item_id 检测由逐项重复计数改为单遍统计，行为与错误消息不变。

### 变更

- `profile --format json` 与 `describe --format json` 现在通过 `canonical_json` 输出，键序稳定，
  便于 CLI diff 与下游字节比较。
- 修复 WorkflowProfile 节点声明遗漏 `has_key_fn`：画像与 `describe()` 现在能区分带自定义
  `key_fn` 和未声明 `key_fn` 的 map/scan 节点。该字段进入 canonical profile bytes，因此嵌入
  `_run.json` 的 profile digest 会变化；本次不改变 L3 内容键，也不递增 `CACHE_SCHEMA`。

### 文档

- 更正公开 `write_artifact()` 的原子性说明：artifact 与 metadata sidecar 分别原子替换，二者
  不是事务；两次写入之间崩溃可能留下更新的 artifact 与旧的或缺失的 sidecar。
- 更正 `DESIGN.md` 的 L3/L1 缓存键描述、命令清单与文档索引，补齐 `recovery` 页与
  `check`/`resume`/`retry-resolve`/`recover` 四个命令。

## [0.14.0] - 2026-08-12

### 重大变更

- `Transport` 现在显式实现 `cache_identity()`、`prepare(messages, model, params)` 与
  `send(prepared)`；新增冻结的 `PreparedRequest`。L1 preflight、缓存键、预算估算、durable
  effect metadata、调用记录和实际发送统一使用 transport 归一化后的 effective request。
  自定义 transport 必须迁移到新协议。
- transport 的 `send()` 只执行一次 provider attempt；transient、空响应和
  `finish_reason="length"` 都立即显式失败，不再内部重试或 sleep。需要重试时必须使用 DAG
  `RetryPolicy`，使每次 provider attempt 都有独立的 durable 边界。
- **缓存族轮换**：`CACHE_SCHEMA` 升至 8。L1 key 现在绑定 credential-free
  `transport.cache_identity()` 与 `PreparedRequest.canonical()`；L3 同步换族，旧缓存首次访问
  将 miss 并可能重新计费。附件 identity 只绑定内容摘要和稳定 MIME/detail 表示，不绑定
  base64 或临时绝对路径，实际发送仍展开为 provider wire 内容。
- 静态守卫改为最小三值模型：`Finding` / `RawIOFinding` 新增稳定 `rule` 与
  `GuardVerdict.ERROR` / `GuardVerdict.UNKNOWN`，不再兼容旧构造形状。已证明的
  `ctx.call` / `ctx.llm` 与 raw read 仍为 `ERROR`；opaque callable、未知 receiver 的
  `.call` / `.llm` 拼写和动态 `getattr` 为 `UNKNOWN`。非调用 literal `getattr` probe 不再
  误报，literal `getattr(..., "read_text")()` 仍是 `ERROR`。
- 注册期只拒绝未豁免 `ERROR`，并用 `GuardUnknownWarning` 显式报告 `UNKNOWN`；pytest
  守卫与图检查沿用同一 verdict。`raw-llm-ok` / `raw-io-ok` 继续各自要求非空理由且不互代。

### 新增

- 新增 `PiModelConfig` 与 `PiProviderConfig`，`PiRpcAdapter(providers=...)` 可把严格校验的
  provider/model 描述经 `canonical_json` 渲染为临时 Pi home 的 `models.json`；
  `AgentProfileConfig` 与 `[tool.kigumi.agent_profiles.<name>]` 的嵌套 TOML provider/model 表使用
  同一类型，并由 `Dag._resolve_agent_profile` 传入 adapter。`api_key_env` 只渲染为 `$ENV_NAME`，
  resolved secret 不进入配置字节或 identity；模型可严格声明 `reasoning`、`context_window` 与
  `max_tokens`，并映射到 Pi 的 `reasoning`、`contextWindow`、`maxTokens`。typed providers 与手写
  `extra_config_files["models.json"]` 不能共存，其他 extra 配置文件继续作为 escape hatch。
- `kigumi guard`、`kigumi check` 与 `dag check` 新增 `--strict-unknown`；默认
  `UNKNOWN` warning 退出 0，严格模式下退出 1。

### 修复

- L1 cache snapshot reader 现在可在有限的连续原子发布竞争中继续绑定完整 descriptor，且不再要求
  被替换的旧 descriptor 必须立即报告 `nlink=0`，避免高并发 writer 把合法的旧/新完整 payload
  误报为 `CORRUPT`；同 inode 原地改写仍然 fail closed。
- 有限 `Budget(max_tokens=...)` 现在按 effective prepared request 预留；一旦 provider attempt
  可能已发出，provider 失败、空/截断响应以及缺失或非法 `usage.total_tokens` 都保守地把准入
  估算记入 `spent`，不能再按零 token 退款。缺失用量仍在写入成功缓存前 fail closed；
  `Budget(max_tokens=None)` 继续支持无用量 transport 的 best-effort 记账。
- typed Pi provider 引用的 `api_key_env` 在 process environment 与 `env_resolver` 合并后缺失或为空时，
  现在会在 version probe/spawn 前以 `CONFIG_POLICY` fail closed，不再先启动 Pi 才暴露凭据错误。

### 兼容性

- typed provider 配置没有在上述第 8 缓存族之外引入额外换族：最终仍沿用既有
  `extra_config_files_sha256["models.json"]` 字节摘要身份；typed 与手写配置在最终
  `models.json` 字节完全相同时具有相同 adapter identity。
- 守卫判定与 finding 数据形状变更不在上述第 8 缓存族之外产生额外换族。

## [0.13.1] - 2026-08-12

### 新增

- `Dag.run()` 与 `Dag.resume()` 新增 `resource_timeout_seconds`，可限制节点等待运行资源的时间；默认值 `None` 保持无限等待。
- `FileSlots.acquire_key()` 与 `LLMCaller(key_lock_timeout_seconds=...)` 支持有界 key lock 等待，超时抛出 `SlotTimeoutError`；默认行为不变。
- `resource_limits` 接受 `0` 表示禁用资源池；需求该资源的节点会在执行前带资源名确定性失败，未使用该资源的节点不受影响。

### 修复

- 配置加载现在会拒绝形状非法的 `source_dirs`，守卫扫描支持单个 `.py` 文件并会对缺失或无效路径报错；`FileSlots.from_env()` 对已设置但无法解析的 `KIGUMI_REQUEST_SLOTS` 也会以带变量名的配置错误失败。
- Prompt resolution 持久化 schema 不匹配时现在报告持久化版本、当前支持版本和可操作指引：无可用迁移的旧版本要求 rebuild，新版本要求 upgrade kigumi；增加了后续迁移用的空注册表与分发骨架。schema-1 字段、canonical 字节和缓存键保持不变。
- Agent 未知运行时失败现在保留异常类型名与消息 SHA-256 摘要；Pi 的 thinking/reasoning 拒绝诊断补充 provider 与 model，失败记录仍不保存明文消息或凭据。
- 修复 single-flight 锁在调用完成后未释放的内存泄漏（#20）

### 兼容性

- 本次不改变缓存键成分或缓存族；合法 `source_dirs` 的 `source_paths` 结果保持不变。

### 文档

- 对齐缓存键与附件说明：每个节点的 `libs` 只覆盖静态可达 import 闭包，`source_dirs` 中不可达的源码不进入该节点身份；附件内容哈希进入缓存键，因此不必手动核对内容哈希，但 `files=` 声明与实际 attach 路径的一致性不由框架强制，仍由调用方负责。

## [0.13.0] - 2026-08-04

### 重大变更

- Durable retry/resume 现在以 per-target append-only receipt chain 绑定 run state、attempt receipt
  与 manifest；链缺失、断裂、回退、分叉或摘要不一致均 fail closed，已开始的外部副作用不会因
  receipt 缺失而被重放。该完整性链是 Greenfield 硬切格式，不改变默认 `retry=None` 语义。
- 守卫环对可达 helper、raw callable 别名、callback 与 opaque 动态调用采用更明确的静态边界；
  nested class、`globals`/`locals`/`getattr`/`eval`/`exec`/`__import__` 等结构按硬切规则拒绝，
  `raw-llm-ok` 与 `raw-io-ok` 仍保持独立且必须说明理由。
- `kigumi init` 生成的 DAG 示例可直接执行；wheel 与 sdist 的安装后 CLI、文档、生成图运行和
  metadata 均纳入发行物 smoke 验证。

### 新增

- 新增项目级 `[tool.kigumi.agent_profiles.<name>]`：一次绑定 Agent Capsule 与 Pi runtime，节点可用
  `profile="name"` 复用；Capsule 的 `agent.toml` 仍是 provider、model、thinking、system prompt
  与运行限制的唯一来源，profile 名不额外改变既有缓存身份。

### 修复

- `kigumi init` 现在对已有 `[tool.kigumi]` 的项目执行幂等文档同步：只补齐 `CLAUDE.md`/`AGENTS.md`，
  保留项目配置与目录，不再因初始化表已存在而阻断 Agent 使用规范的注入。
- 收紧 blob 与原始文件输入的 regular-file、descriptor-bound 和 preflight 边界，避免 FIFO、设备、
  symlink、TOCTOU 与错误内容 blob 穿透校验。
- 输出物化改为可回滚提交，失败时恢复 output ownership 与 staging；managed PromptResolution
  缺失必需字段时 fail closed。
- L1/L3/BlobStore 的发布写入统一经过 descriptor-relative、no-follow 的安全目录与 atomic
  write 边界；父级 symlink 不再能把缓存或 blob 写到项目根外，不支持该能力的平台显式返回
  `ENOTSUP`。
- `Dag.plan`、scan、EvidencePolicy、workers 与损坏 cache 的错误路径统一为不重算的 fail-closed
  行为，并补齐真实安装入口和发行物测试语义。
- 节点引用的 Pydantic 模型（包括 `ctx.call_validated()` 使用的模型）现在把 schema 与可见模型源码
  摘要纳入节点缓存键；模型位于 `source_dirs` 外时不会再静默复用陈旧产物，位于其中时也不会
  仅因 Pydantic metaclass 不可展开而关闭整个节点的 L3 复用。
- Agent 运行失败现在在保留原有宽码的同时记录受限的 `envelope`、`bridge_policy`、
  `submit_contract` 或 `config_policy` sub-code；未知异常仍是通用 `protocol`，失败记录不保存
  原始异常文本或凭据。

### 兼容性

- L3 内容键成分不变，`CACHE_SCHEMA=7` 继续沿用且不新增缓存族；但 node cache envelope
  从 schema 3 正式升至 schema 4，以绑定请求的 `cache_key`。这是 Greenfield 格式硬切，旧
  schema 3 条目按 `CORRUPT` 拒绝，不提供迁移或兼容 shim。

## [0.12.0] - 2026-08-03

### 新功能

- `kigumi init` 现在自动将 kigumi 使用规范（框架边界、命令速查、工作规则）注入项目根目录的
  `CLAUDE.md` 与 `AGENTS.md`；文件已存在则追加，已含注入标记则跳过（幂等）。

### 文档

- 补充 `docs/brief.md` 对 `source_dirs` 与 `dag_entry` 框架边界的说明，并将每次变更前后的只读检查流程写成明确步骤。
- 修复 `kigumi init` 注入 brief 时的标题嵌套：围栏外的 ATX 标题统一下沉一级，代码围栏内的注释保持不变。

## [0.11.0] - 2026-08-03

### 文档

- 对齐 Agent node 与 WorkflowProfile 契约文档中的 schema 声明，并新增测试守卫，自动核对活动文档
  的 schema 值与源码常量；`docs/reviews/` 中的历史记录不纳入检查。
- 将契约文档中 30 处陈旧的数值行号引用（`dag.py:123-456`）替换为稳定的符号引用（`Dag.run`、
  `_execute_map` 等），避免代码演进后引用漂移；测试守卫现扫描活动契约文档，拒绝新增数值引用。
- 将 `optimize.py` / `evolve_prompt` 定位为实验性、内容级提示词 recipe：保留
  `Candidate`、`EvolveResult` 与 `evolve_prompt` 的现有导入兼容性，不再将其描述为
  DAG/Agent 优化器、durable run recovery 或无偏的泛化估计器，也不提供自动晋升。证据建议
  使用 `bench` 加 `FunctionSubject`/`CallerSubject`/`DagSubject`/`AgentSubject`。采用候选由
  调用方/人工负责：先审阅 `result.best`，再手动把批准文本写入 `prompts/*.md`，最后由项目
  通过 `PromptRef` 引用该既有文件，或用 `PromptSpec` 组合它；`PromptSpec` 只声明组合。
  没有晋升 API，也不会自动写入。验证反馈隔离仅在 train 与 validation 内容由调用方预先验证为
  互斥时成立；框架不做运行时检查。保留有界指标评估与可续跑本地 JSON 算法检查点的说明，并
  明确它不是带副作用感知的 durable run receipt。本次仅变更定位，不修改运行时、state schema
  或算法，也不改变任何缓存族。

### 重大变更

- `StdlibTransport` 现在在构造时校验 `api_base`：scheme 必须是 `http` 或 `https`，且必须带
  host。传入 `file:` 等非 HTTP scheme、不带 scheme 的地址，或 `http://` 这类缺 host 的地址，
  都会抛出 `ValueError`。这是有意的破坏性变更，一方面阻止 `urlopen` 通过本地或其他非 HTTP
  handler 处理调用方提供的 endpoint，另一方面把原先要到真正发请求时才暴露的无效 endpoint
  提前到构造期。
- `bench` 报告 schema 升至 3：每个 variant 新增 stage-aware `outcome_summary`，把质量聚合与
  subject/metric 运行结果覆盖分开；这是报告格式变化，不轮换缓存族，`CACHE_SCHEMA` 不变。
- **缓存族轮换**:`CACHE_SCHEMA` 升至 7。提示摘要现纳入附件内容哈希与响应 schema 标识,
  L1 调用缓存条目也开始写 `response_sha256`,因此旧缓存整体失效,首次运行会重新计费。
  本次 `libs` 改为按节点静态 import 闭包取值，沿用这次已发布的 7 轮换，不再新增
  `CACHE_SCHEMA=8` 的第二次全项目缓存换族。
- 附件成为 `PromptResolution` 的一等成员(`Attachment` / `Message` / `ResponseSpec`),
  `FileRef` 端到端可用:换掉一个附件的内容就换缓存键,不再复用按旧文件算出的响应。
- 新增请求预检:超过 `PreflightPolicy` 上限时抛 `RequestTooLarge`,不静默截断。
- map 与 parent 节点现共享同一并发平面,嵌套 map 线程池已移除:既有图的实际并发度可能变化。
  此前每个 map 在调度器 worker 里再开一个 `workers` 大小的池,总线程数是 workers 的平方。

### 新增

- `LLMCaller` 复用已配置的 `FileSlots` lock root，为 L1 cache key 增加可选的跨进程
  single-flight：同 key 的第二个进程会在二次 cache check 后重放首个结果；未启用时不创建
  锁文件。`acquire_key()` 没有 timeout，活着但卡住的持锁进程会让同 key 等待方无限阻塞。
  预算预留仍是进程内协调，没有跨进程 durable 总账、崩溃恢复或失败退款协议；本次
  不改变 L1/L3 cache key、`CACHE_SCHEMA` 保持 7。
- `Attachment` / `Message` / `ResponseSpec`:类型化的请求表示。`Attachment` 只记录路径、
  内容哈希、MIME 与字节数,不把文件字节拖进 provenance。
- `preflight()` 与 `PreflightPolicy` / `PreflightReport` / `PreflightViolation`:在缓存查找
  和 provider 请求之前估算 token、统计附件数量与总字节,超限请求不会先计费再失败。
- `ResourceRequest` 与 `Dag.run(resource_limits=...)`:按节点声明资源(GPU、供应商配额等),
  运行时分别限流;未声明的节点走 `None` 默认池。多资源按名字确定序获取,不会互等。
- `BudgetPermit` 与 `Budget.reserve()` / `commit()` / `cancel()` 预算 admission API:
  付费调用前先预留额度,缓存命中不占额度,失败与空响应会退还预留。
- 为 terminal `failed` run 增加 `Dag.recover()` 与 `RecoveryReceipt`:恢复决定、理由和证据
  以 append-only receipt 落盘,成功节点继承,旧 attempt 不再需要删除即可安全重试。
  新增 `docs/recovery.md`,`kigumi docs recovery` 可读。
- `kigumi runs show` 在 run 处于终态 `failed` 时打印非破坏性恢复路径。
- 新增图命令 `kigumi recover RUN_ID TARGET`（`dag recover` 共享同一 parser/dispatch）：
  可用显式 decision、reason 和可重复 evidence 对 terminal `failed` run 做 append-only recovery，
  并报告返回的 `RecoveryReceipt`；命令不会自动 resume。

### 修复

- 修复 Python 3.13 深层嵌套共享结构的测试挂起：120 层共享 tuple 树在 3.13 触发指数级哈希
  行为，改用 frozenset 图避开该问题；3.13 全套测试通过，不影响 Python 3.10-3.12。
- 修复静态 `libs` 闭包成功时跳过 runtime state 的 stale replay：现在只有静态闭包与所有已到达配置 callable 的
  defaults、closure、annotations、wrapper、partial、receiver 和 class state 都可表示时才允许 L3 复用。
- 修复自定义容器隐藏配置 callable 的情况：遇到带配置源码 provenance 的非内建迭代/映射容器时保守关闭
  L3 复用，外部 provenance 容器仍保持边界隔离。
- 修复 `sys.modules` provenance 扫描预算溢出状态在 analyzer memoization 后丢失的问题：重复 identity 查询现在
  始终保持相同的不可复用结论。
- 修复 complete-globals 观察传播：class body 会被检查，且实际调用的 nested callable 会递归追踪到所有传递
  层级，避免 reflection 观察漏出缓存键分析。
- 修复 set/frozenset 排序展开缺少共享预算与 memo 的问题：深层或共享别名 tuple 结构遇到稳定 overflow marker
  即 fail closed，不再无界工作或触发 `RecursionError`。

- `libs` 静态闭包遇到多个配置源码候选、已加载模块路径偏离静态候选、已识别的动态可调用引用
  （`__import__`、`import_module`、`find_spec`、`eval`、`exec`、`compile`，无论是否实际调用、
  也不论赋值目标是简单名、walrus、属性、下标/容器、解构或链式赋值），或模块 AST 中出现
  常见反射原语（`getattr`、`globals`、`locals`、`vars`、`__dict__`、
  `__getattribute__`、`__builtins__`、显式 `builtins` 导入、`importlib` 子模块导入及模块注册表访问如
  `sys.modules`）时，或表面为外部的已加载 package 通过运行时 `__path__` 伸入配置源码宇宙时，
  现在统一退回精确全文件摘要，即使名称/键是计算出来的也不尝试常量传播；
  保守的额外失效是有意的，避免动态 helper 编辑复用陈旧节点产物。libs 只管理
  `config.source_paths`，项目根下未列入配置源码路径的文件不属于该成分；选中闭包和全文件
  fallback 都绑定按 `source_dirs` 顺序排列的稳定源码根/文件 identity（项目内使用相对项目根路径，
  项目外使用配置的 canonical identity）；选中闭包另绑定限定模块名。owner identity 只分析当前注册
  节点函数，避免未注册 sibling 污染粒度；该函数剥除 docstring 后直接观察 `__name__`、
  `__package__`、`__file__`、`__spec__`、`__loader__`，通过函数对象的 `__module__`/`__globals__`
  观察 owner，或通过直接、属性及被函数引用的全局别名形式调用 `globals`、`locals`、`vars`、
  `eval`、`exec`、`compile`，以及动态 `getattr`/`__getattribute__` 查找观察这些值时，还绑定
  owner 限定名与稳定项目相对/canonical 路径。只在 docstring/非查找字符串中提到这些名称，或执行
  常量且无关的 `getattr(helper, "VALUE")` 的等价函数仍可跨模块名或文件名复用。fallback 另绑定
  当前已加载/函数全局可见的配置源码模块选择、脱离 `sys.modules` 后保留的 function/class 与已识别
  callable wrapper 的源文件 provenance、callable 限定名与不含文件名/行表噪声的可执行 code
  digest、函数及嵌套 code object 实际引用的简单全局值，以及配置源码根本身或 package parent
  在 `sys.path` 中的相对顺序。相对导入携带实际限定模块名并校验所有加载前缀，
  向上导入使用 climb 后的 package suffix；`ImportFrom` child 只有在 base 模块顶层 AST 明确
  证明为属性绑定时才保持节点粒度，缺失的 canonical `sys.modules` 条目也按不确定处理。这是
  `CACHE_SCHEMA=7` 已发布后的正确性修复，
  不新增缓存族轮换；该边界是源码 AST 分析，不宣称覆盖任意外部/native 运行时代码。
- 继续修复 `libs` 残余粒度与 owner 边界：单参数/partial 绑定的 owner 查找、closure 别名、
  下标选择 lookup、实际到达的 nested function/class body/直接调用 Python function graph 都参与
  owner 判断；脱离 `sys.modules` 的注册函数只在 `__module__`、globals 名称与 code/source 路径
  一致时恢复 owner，事实冲突进入 fail-closed fallback。判断继续区分 Load/Store、词法局部绑定和
  实际 receiver，避免把 `getattr(helper, "__name__")`、`helper.eval(...)`、`obj.globals` 或局部
  `__name__` 误当 owner。
- fallback 的 retained callable 状态改为一张跨 callable/global root 共享、带 node/depth/member
  硬上限的确定性对象图：覆盖 closure、defaults/kwdefaults、annotations、function dict、bound method、
  partial/wrapper、实例 dict、每个 MRO slot、class/base/metaclass state 与 slot/container 中的
  callable；保留 dict insertion order、普通 `"__wrapped__"` 数据键、float bit pattern、cycle 与
  shared-alias topology，并对 code DAG、源码归一化、模块注册表和重复 node function 做 query 内
  复用。遍历超预算，或遇到 native container subclass、custom descriptor/property/
  `__getattribute__`、不安全路径对象等无法静态读取的状态时，使用稳定声明 identity 并把有效 L3
  策略单独降为 `off`；完整 globals namespace 观察同样降级，不再声称任意 namespace 已被证明
  等价。运行时取证只接受 exact string path，不调用 `__fspath__`、truthiness、用户 equality/hash、
  mapping hook、descriptor、partial subclass hook 或 custom metaclass hook。
- 普通节点、map、scan、plan 与 explain 现在共用上述有效 L3 策略：不可复用时不再读取/写入残留
  cache，空 map/scan 也不会因 `all([])` 被误报为 aggregate hit。确定性 key、run manifest 与
  resume/recover 保持稳定，不使用随机 nonce。这些修复搭载已发布的 `CACHE_SCHEMA=7`，不新增
  缓存族轮换。
- 修复 `kigumi runs show` 的 recovery 建议：参数改为 shell-safe 命令渲染，要求在 `recover` 与
  随后的 `resume` 两条命令中都补回构造该 run 时使用的同一组实际重复
  `--graph-arg KEY=VALUE`（放在 `--` 之前，以支持 option-like ID）；不涉及 `CACHE_SCHEMA`。
- 收紧节点 raw-I/O 守卫：只有 `ctx.read_text()` / `ctx.read_bytes()` 是受控读取，`ctx.open()`
  现在会被报告为违规。
- `BudgetExceeded` 现从 map、scan 和 foreach 保持原类型传播;预算超限会中止后续 fan-out
  item,已在途 item 完成后再统一收尾,而不是把超限埋进聚合失败里。
- `Budget` 预留估算现同时计入 prompt 与 `max_tokens`;此前声明 `max_tokens` 会让预留
  只按输出额度计算,输入 token 不设防。
- 副作用边界改按 executor 类型安装,不再要求节点声明 `retry`:此前未声明重试策略的节点
  在付费调用后崩溃不会留下 `side_effect_started`,恢复时会误判为「未产生副作用」。
- 附件内容哈希进入提示摘要和缓存键,发送前会复核哈希,因此消费者不必再手动核对附件内容哈希；但
  框架不强制校验 `files=` 声明与实际发送的附件路径是否一致,这项声明核对仍由调用方负责。
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
