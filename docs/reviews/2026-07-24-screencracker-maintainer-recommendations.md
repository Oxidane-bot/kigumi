# Kigumi 维护者建议：来自 ScreenCracker 生产化接入的框架压力

Status: Historical pressure record (not an API, contract, roadmap, or acceptance source)

Date: 2026-07-24

Audience: Kigumi maintainers only

> 本文只记录评审时点的外部压力。0.6.0 的公开行为与验收以实现、测试、
> `docs/contracts/` 和 `CHANGELOG.md` 为准；本文提到但未进入发布范围的 handoff、outbox
> 等能力不构成承诺。

## 1. 结论

ScreenCracker 证明了 Kigumi 0.5 的两个核心选择是正确的：

1. 静态 DAG 应拥有执行拓扑，模型只能产生内容；
2. Pi Agent 应是普通节点执行器，而不是第二个 scheduler。

这两个边界已经替接入项目承担了最危险的基础设施：Agent capsule、staging、文件工具、
collect/publish、版本锁定、RPC/进程管理、输出所有权、节点依赖、人工 checkpoint 和
canonical hash。没有 Kigumi，ScreenCracker 还需要自行实现并验证一套 Agent runtime 和
DAG 执行器。

但本次接入也暴露出一个明确问题：Kigumi 当前足以承载“单张 DAG 内的可审计执行”，还不足以
自然承载“跨 DAG、跨进程、可恢复、带外部副作用的生产流水线”。项目因此在 Kigumi 外围重新
实现了 CALL、retry、debug、实验、receipt、checkpoint ledger、phase stepper 和 durable
handoff。若不处理这些共性压力，后续接入项目也可能形成相同的“双控制面”。

维护方向不应是把 ScreenCracker 的领域模型搬进 Kigumi，而应补齐以下最小公共能力：

- 结构化 provider failure；
- 显式、持久、可恢复的 CALL/Agent retry；
- 可配置的证据隐私策略；
- schema-bound artifact/provenance envelope；
- source-addressed durable handoff；
- 受限的 external-effect/outbox primitive。

在新增能力之前，应先让接入项目充分使用 Kigumi 已有的 CALL、Bench、Subgraph、map/scan、
plan/trace/diff/explain 和 materialization 能力。当前一部分压力来自框架能力缺失，另一部分
来自接入层没有走框架的最短路径。

## 2. 证据范围

本报告只把 ScreenCracker 当作框架压力样本，不把其当前实现当成正确参考实现。

2026-07-24 快照中：

- 104 个生产 Python 文件中，55 个直接导入 Kigumi；
- 约有 213 个 `Dag.node` 声明和 12 个 `Dag.agent` 声明；
- 约 191 个节点显式 `cache="off"`，只有约 16 个显式 `cache="auto"`；
- 生产代码没有使用 `ctx.call`、`ctx.render`、`call_validated`、`map`、`scan`、
  `Subgraph`、`DagSubject` 或 `AgentSubject`；
- 生产路径也没有直接使用 `plan`、`trace`、`diff` 或 `explain`；
- 项目外围与框架能力相邻的自建模块约 13,700 行，其中包括 structured CALL、retry、
  Agent retry、debug/A-B、workspace、phase backend、receipt/checkpoint store 和
  canonical store。

这些数字会随接入项目继续开发而变化，只用于说明压力形状：

> Kigumi 的 DAG/Agent 边界被大量使用，但 L0–L2、实验、局部调试和动态项原语几乎没有进入
> 正式路径。

不能据此推导“13,700 行都应迁入 Kigumi”。其中 canonical screenplay authority、Season
完整性、Episode 提交顺序和领域 ledger 明确属于 ScreenCracker。

## 3. 已验证有效的 Kigumi 边界

### 3.1 `Dag.agent` 的定位应保持不变

ScreenCracker 的 Writer、Planner、Localization 和 Blind Reader 都能被表达成静态 DAG 中
的 bounded Agent。Agent 不拥有 successor、canonical write 或 approval 权限。

维护者应继续拒绝：

- Agent factory；
- 模型返回可执行拓扑；
- Agent 自动创建 Agent；
- Agent 自动 winner/promotion；
- Agent 绕过 collect/publish 写项目输出；
- 隐式 session、project context、Skill、Extension 或 tool discovery。

ScreenCracker 的压力不是“Agent 不够自治”，而是 Agent 失败后的公共错误和恢复证据还不够
结构化。

### 3.2 `consumes` 是正确的接入面

大型内容流水线必须让 cache identity 与实际消费视图一致。ScreenCracker 已在 Writer、
Outline、Planning、Localization 和 Season Review 中使用 `consumes`，证明边上投影比新增
ModelNode/PromptNode API 更合适。

不要为 ScreenCracker 新增第二套语义节点注册 API。应继续增强现有普通节点、边投影和
artifact envelope 的可审阅性。

### 3.3 Checkpoint 的 run-local、payload-bound 语义应保持

审批绑定 payload hash、恢复限定同一 run、调用过 checkpoint 的执行不进入共享 L3 cache，
这些规则正确。ScreenCracker 另建 checkpoint ledger 的原因是需要长期审计和领域 authority，
不是当前 checkpoint 语义错误。

Kigumi 可以增加通用审计导出，但不应让一次审批跨 run 自动升级为业务 authority。

## 4. 先解决接入方式，不急于扩 API

以下能力 Kigumi 已经提供。维护者应先增加面向大型生产项目的 cookbook、迁移检查器或审阅
工具，确认接入项目为何没有使用，再决定是否扩展内核。

### 4.1 标准 CALL 路径

ScreenCracker 的 structured CALL 直接使用 `Transport.complete()`，自行实现：

- 内容寻址 cache；
- provider retry；
- JSON decode；
- schema validation；
- structural repair；
- attempt receipt；
- raw/payload digest；
- failure evidence。

Kigumi 已有 `LLMCaller`、`call_validated`、prompt schema format、repair loop、Budget 和
transport retry。维护者应先提供一个“领域 admission 包裹标准 CALL”的参考形态：

```text
project admission/binding
→ Kigumi LLMCaller + call_validated
→ project domain receipt
```

文档必须明确：

- 如何从 `LLMCaller.calls` 和 L1 sidecar 构造领域 receipt；
- 如何关闭 transport 内部 retry，让项目级 durable retry 精确计数 side effect；
- 如何让 resolved model、observed response model、prompt hash、params、usage 和 response ID
  进入项目 provenance；
- 哪些信息进入 Kigumi cache family，哪些只属于项目 authority。

如果标准路径仍无法满足 hash-only evidence，再实现第 5.3 节的 EvidencePolicy；不要先增加
另一套 CALL API。

### 4.2 Bench 作为统一实验执行器

ScreenCracker 自建 model-policy experiment 和 QA-placement A/B，但 Kigumi 已有
`FunctionSubject`、`CallerSubject`、`DagSubject`、`AgentSubject` 和隔离 trial evidence。

维护者应增加一个生产项目示例，展示：

- incumbent + 2–4 个 hypothesis-bearing variants；
- 每 variant `N >= 5`；
- exact cohort 与唯一实验变量；
- deterministic gate + model/human judgment；
- usage/cost/duration；
- blind commitment/reveal；
- bench 不选 winner；
- 项目 checkpoint 只批准 bench report digest。

Bench 不应自动 promotion。需要补的是“报告合法性验证器”，不是 optimizer。

### 4.3 `targets`、`plan`、`trace`、`diff`、`explain`

ScreenCracker 自建单节点/局部链 Debug Runner，说明现有调试能力没有自然进入接入者心智模型。

应补一份“生产故障定位最短路径”：

```text
dag.plan(targets=...)
→ dag.run(targets=..., force=...)
→ kigumi trace
→ kigumi call
→ dag.explain / kigumi diff
```

若项目仍需要自建 debug runner，应要求其书面列出 Kigumi 现有入口缺少的具体字段。没有具体
缺口，不新增第三套 run slice API。

### 4.4 Subgraph、map 与 scan

ScreenCracker 中大量 DAG 具有重复形状：

```text
admission
→ compile projection
→ assemble
→ semantic execution
→ validate
→ candidate
```

以及：

```text
draft
→ AST/lint
→ reviewer fan-out
→ join
→ bounded repair
→ fresh QA
```

维护者应提供三个接近真实生产的示例：

1. 用 `Subgraph` 重复挂载 review/repair tail；
2. 用 `map` 执行多个独立 reviewer 或 episode candidate；
3. 用 `scan` 传递跨集 closing state，并展示 carry 收窄和后缀 early cutoff。

不要为了接入便利放宽静态拓扑约束。

## 5. 建议新增的公共能力

### 5.1 P0：结构化 Pi provider failure

这是当前最明确、最先应解决的框架缺口。

Pi/Kigumi 成功或失败证据至少需要区分：

```python
class ProviderFailure:
    provider: str
    stage: Literal["connect", "request", "response", "stream", "agent"]
    kind: Literal[
        "rate_limit",
        "server_error",
        "timeout",
        "connection",
        "authentication",
        "authorization",
        "model_unavailable",
        "policy",
        "invalid_request",
        "malformed_response",
        "unknown",
    ]
    status_code: int | None
    retry_after_ms: int | None
    provider_request_id: str | None
    message_digest: str | None
    retryable_hint: bool | None
```

要求：

- 原始 provider prose 不作为控制值；
- `kind` 只能来自 wire/status/SDK typed error，不从字符串猜测；
- 不可观察时明确为 `unknown`；
- credential、URL query 和 header secret 永不进入 evidence；
- CALL 与 Agent 尽量共享同一公共 failure vocabulary；
- adapter 不负责最终 retry 裁决，只报告可观察事实。

验收：

- 429、5xx、timeout、connection reset、401/403、invalid request、model mismatch 和未知错误
  都有锁定测试；
- 每个测试同时验证脱敏；
- unknown 不得被默认视为 retryable；
- 变更 Agent evidence/schema 时按既有 executor schema 政策换族。

### 5.2 P0：显式 durable retry

Kigumi 当前 transport 有有界 retry，Pi 0.5 又刻意关闭隐藏 Agent/provider retry。这个选择对
精确 side-effect 计数是正确的，但把 durable retry 压给了每个接入项目。

建议新增节点级或 adapter-level retry primitive：

```python
RetryPolicy(
    max_attempts=3,
    backoff="exponential",
    jitter="full",
    retry_on=frozenset({"rate_limit", "server_error", "timeout", "connection"}),
    max_delay_seconds=120,
)
```

必须提供：

- policy digest 进入 execution identity；
- 每次 attempt 独立 receipt；
- `due_at` 持久化，进程重启后不提前连打；
- CALL attempt 精确记录是否发生 provider side effect；
- Agent 每次 attempt 使用同一 canonical snapshot 的全新 workspace；
- 只有成功 attempt 可进入 collect/publish；
- 失败 attempt 的 evidence 可达并受 GC 保护；
- auth、policy、schema、model substitution 和 unknown failure 默认不重试；
- retry exhaustion 返回单一 typed terminal failure。

不要把 retry 隐藏在 transport、Pi settings 和 node wrapper 三层同时执行。框架必须能证明一次
业务节点最多发生多少次真实 provider side effect。

### 5.3 P0：EvidencePolicy

不同项目对 L1 payload 的保留要求不同。ScreenCracker 需要 hash-bound receipt，但不希望原始
prompt、response 或剧情正文普遍落入失败证据。

建议引入正交于 cache policy 的 evidence policy：

```python
EvidencePolicy(
    request="full" | "redacted" | "hash_only",
    response="full" | "redacted" | "hash_only",
    stderr="redacted" | "hash_only",
    trajectory="full" | "redacted" | "hash_only",
)
```

关键边界：

- cache 是否保存可重放 payload，与审计 sidecar 展示什么必须分开；
- `hash_only` 仍需记录 byte count、digest、model、params、usage 和 provider response ID；
- `redacted` 必须记录 redaction policy digest；
- policy 改变是否换 cache family要按“是否改变可重放字节”裁决，不能一律换族；
- 先支持静态 policy，不引入运行时内容分类 Agent；
- 不承诺加密存储，除非另有完整 key lifecycle 设计。

### 5.4 P1：Schema-bound Artifact Envelope

多个接入项目都会需要比裸 `dict` 更强的跨节点引用：

```python
ArtifactRef(
    artifact_id=...,
    artifact_type=...,
    schema_id=...,
    schema_version=...,
    raw_sha256=...,
    payload_sha256=...,
    producer_run_id=...,
    producer_node=...,
)
```

建议提供可选公共 envelope，不改变普通 `dict` artifact：

- envelope 内容仍走 `canonical_json`；
- schema identity 与 payload digest 可机械复算；
- materialized file/blob 可以签发 ref；
- cache hit 与 miss 签发相同 ref；
- ref 不自动授予业务 authority；
- consumer 可以在 `consumes` 中只接收 ref 或 ref 指向的投影；
- schema validator 由项目注入，Kigumi 不维护领域 schema registry。

这是减少项目重复 provenance/引用代码的合适层级。不要让 Kigumi 判断“这个 artifact 是否是
正式剧本”。

### 5.5 P1：Source-addressed durable handoff

大型项目会把一条生产流程拆成多个固定 DAG。下游不能依赖调用方重新构造上游语义对象，也
不能只接受临时内存参数。

建议探索：

```python
DeferredArtifactInput(
    producer_graph="outline-v1",
    producer_node="episode_planning_product",
    selector={"project_id": "...", "episode_id": "..."},
    artifact_type="EpisodePlanningProductV1",
    schema_id="...",
)
```

框架职责：

- 从 retained run/artifact index 中解析唯一候选；
- 验证 producer graph/node、schema、digest 和 materialized bytes；
- 把 resolved ref 进入下游 key/external identity；
- 上游 digest 改变时下游自然失效；
- 多候选、缺失、损坏或 GC 不可达时 fail closed；
- 禁止 caller 用同名内存 payload 覆盖 resolved artifact；
- `plan` 能把 unresolved handoff 报为 unknown/pending，而不是执行 resolver side effect。

项目职责：

- 哪个 producer 有资格成为正式来源；
- selector 的领域含义；
- 是否需要人工批准；
- artifact 是否具有 canonical authority。

先实现只读 resolver。不要一开始加入自动跨图调度或第二 scheduler。

### 5.6 P1：受限 external-effect/outbox primitive

纯 artifact DAG 最终常需写数据库、发布对象或更新 canonical pointer。ScreenCracker 为此实现
了 permit、transition、commit 和 crash reconciliation。

Kigumi 可以考虑一个非常窄的 effect primitive：

```text
prepare intent
→ persist intent
→ execute idempotent effect
→ persist completion
→ reconcile incomplete intent
```

最低合同：

- effect identity 和 idempotency key；
- input artifact refs；
- pre-effect durable intent；
- result/effect digest；
- completion receipt；
- crash-after-effect-before-receipt reconciliation；
- effect 不进入普通 L3 cache；
- `plan` 只报告 effect pending，不执行；
- 需要调用方提供 idempotency probe 或 reconciliation function；
- 无法证明幂等时拒绝注册或要求显式 reasoned waiver。

止损线：

- 不做通用数据库；
- 不做 Temporal；
- 不做业务 FSM；
- 不做跨项目权限系统；
- 不让 effect 自动签发领域 authority；
- 第二个真实项目没有同形压力前，不把实验 API 升为稳定 API。

### 5.7 P2：Bench admission validator

Bench 应继续不选择 winner，但可提供报告结构合法性检查：

- 唯一 incumbent；
- 每个 variant 有 hypothesis；
- exact cohort；
- 每 variant `N >= 5`；
- seed/unsupported seed 声明一致；
- rubric/metric identity 固定；
- 唯一实验变量有声明；
- failure cell 不被静默删除；
- usage/cost/duration 完整；
- blind experiment 可验证 commitment/reveal；
- report digest 可绑定 checkpoint。

输出只应是：

```text
admissible / inadmissible + structured findings
```

不能输出 winner，也不能修改 Prompt、Skill、model policy 或生产 DAG。

## 6. 不应进入 Kigumi 的 ScreenCracker 能力

以下内容即使代码量很大，也不应因“减少项目代码”而下沉：

- R00–R120 phase catalog 的业务含义；
- Story、Boundary、Closing State、relationship experience；
- screenplay AST 与平台格式；
- Episode/Season 完整性的定义；
- 单集、三集和整季质量门槛；
- commercial、physical、continuity rubric；
- canonical screenplay commit authority；
- 哪类剧本问题应返回哪个上游阶段；
- holdout 是否有权用于某个项目；
- 人工是否批准一份大纲或剧本。

Kigumi 可以提供 provenance、resolver、checkpoint、effect 和 evidence；是否足以构成业务
authority 必须由项目代码裁决。

## 7. 建议的实施顺序

### Stage A：不扩内核，先验证现有能力

1. 编写 structured CALL 的领域 receipt cookbook；
2. 编写 Bench N≥5 + checkpoint 示例；
3. 编写 Subgraph review/repair 示例；
4. 编写 map reviewer fan-out 和 scan state-chain 示例；
5. 编写 `plan → target run → trace → diff → explain` 调试指南；
6. 用一个小型 ScreenCracker slice 验证能删除多少外围代码。

成功标准：在不改变 Kigumi API 的情况下，至少删除或替代一套项目自建 debug/experiment
执行路径，并证明缓存、证据和结果语义不退化。

### Stage B：解除真实生产阻断

1. 结构化 Pi provider failure；
2. durable retry；
3. EvidencePolicy。

顺序不可倒置：没有可信 failure taxonomy，就不能实现可信 retry；没有明确 evidence policy，
接入项目仍会为了隐私边界绕开标准 CALL。

### Stage C：减少跨 DAG 胶水

1. ArtifactEnvelope/ArtifactRef；
2. read-only durable handoff resolver；
3. Bench admission validator。

这些能力应先标实验，并用至少两个项目验证字段与语义。

### Stage D：谨慎探索 external effects

只有出现第二个非 ScreenCracker 项目需要相同的 intent/effect/reconcile 合同时，再实现受限
primitive。否则保留项目侧实现和止损线。

## 8. 每项改动的维护要求

任何建议进入实现前，都必须先形成独立设计修订，至少回答：

1. 观察到的项目侧失败或重复实现是什么；
2. 它违反了哪个 Kigumi 现有边界，还是暴露了新边界；
3. 为什么不能用现有 API 解决；
4. 最小公共数据模型是什么；
5. 哪些字段进入 cache key；
6. 是否换 cache family、L1 schema、Agent executor schema 或 report schema；
7. cold、warm、resume、failure 和 GC 路径是否同形；
8. `plan`、`run`、`trace`、`explain` 是否一致；
9. 是否引入隐式 side effect、隐式 retry 或第二 scheduler；
10. 哪些领域判断明确留在项目侧；
11. RED 测试坐标是什么；
12. 第二个项目的验证门槛是什么。

不能以“ScreenCracker 已经写了很多代码”作为抽象进入框架的充分理由。只有在公共失败形状、
可验证不变式和第二项目复用面都明确时，能力才应进入稳定 API。

## 9. 最终维护裁决

短期内，Kigumi 不需要扩大自治范围，也不需要成为业务 workflow engine。它需要做的是把已经
承诺的确定性、可验证和可恢复边界向生产故障再推进一层：

```text
现在：
静态 DAG + 可验证 CALL/Agent + run-local checkpoint

下一层：
结构化 failure + durable retry + privacy-aware evidence
+ schema-bound handoff + narrowly scoped effects
```

如果这条路线执行正确，接入项目应能删除大量 retry/debug/experiment/handoff 胶水，同时继续
独占其领域 schema、质量判断和 canonical authority。若新增能力迫使 Kigumi理解“剧本、
Episode、Season 或业务批准”，说明抽象已经越界。
