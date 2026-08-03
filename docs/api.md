# 公共 API 参考

本页是查名字、签名与失效处置的速查，不重复[接入指南](adoption.md)的叙述。除明确写出
子模块导入路径的三个内部运维异常外，条目都可从 `kigumi` 顶层导入。包版本的唯一源码是
`kigumi._version.__version__`；顶层导出同一个 `__version__`。

## 异常与警告

### 修复、调用与 transport

- `class RepairExhausted(RuntimeError)`（`kigumi.repair`，顶层导出）：`repair_loop` /
  `call_validated` 的首次校验与全部 `max_repairs` 修复仍失败。检查异常链中的最后一次校验
  错误，修正 schema、prompt 或 validator；需要审计各轮时传 `sink=` / `on_event=`。见
  [L2 修复环](adoption.md#2-组装调用栈)。
- `class BudgetPermit`（`kigumi.calling`，顶层导出）：`Budget.reserve()` 返回的预留句柄；
  成功响应调用 `commit(actual_usage)`，失败或取消调用 `cancel()`。
- `class BudgetExceeded(RuntimeError)`（`kigumi.calling`，顶层导出）：预留额度不足，或一次已完成
  调用的实际用量使 `Budget.spent` 推过 `max_tokens` 时抛出。缓存命中不做预留；miss 在 provider
  请求前按 prompt 长度加 `max_tokens` 做 best-effort 预留，实际用量可能超过预留并在 commit 时
  被记录。预算只在进程内协调；`LLMCaller` 只有在传入启用的 `FileSlots` 时，才用同一 lock root
  对同 key 做跨进程 single-flight。
- `class DryRunError(RuntimeError)`（`kigumi.calling`，顶层导出）：`LLMCaller(dry=True)` 遇到
  L1 miss、原本必须发真实请求。先补齐缓存，或在明确允许真实请求的运行中关闭 `dry`。见
  [零真实请求的测试](adoption.md#零真实请求的测试)。
- `class EmptyResponseError(RuntimeError)`（`kigumi.transport`，顶层导出）：transport 的空文本
  响应超过 `max_empty_retries`。检查 provider 响应与模型，或调整有界 transport 配置。
- `class TruncatedResponseError(RuntimeError)`（`kigumi.transport`，顶层导出）：
  `finish_reason="length"` 时没有显式 `max_tokens`，或倍增额度后仍超过
  `max_length_retries`。显式设置合适的 `max_tokens`，并检查输出任务是否过大。
- `ProviderFailure(*, provider: str, stage: ProviderFailureStage, kind: ProviderFailureKind, status_code: int | None, retry_after_ms: int | None, provider_request_id: str | None, message_digest: str, retryable_hint: bool | None) -> None`
  （`kigumi.failures`，顶层导出）：L0/CALL 的结构化 provider 失败事实；只依据 wire/status/
  typed SDK 字段分类。按 `kind` 修复凭证、权限、请求或模型，只有 retry policy 允许的 kind
  才进入 durable retry。见 [Failure 契约](contracts/failure.md)。

### Prompt

- `class TemplateSlotError(ValueError)`（`kigumi.prompt`，顶层导出）：`render_template` 收到的
  槽位集合与模板实际槽位不全等。按异常列出的 missing/extra 修正调用。见
  [分层 Prompt 解析契约](contracts/prompt-resolution.md)。
- `class PromptDefinitionError(ValueError)`（`kigumi.prompt`，顶层导出）：Prompt 名称、路径、
  axis、layer、material、重复槽位或节点边界声明不安全或不一致。在注册或 snapshot 阶段修正
  `PromptSpec`；不要绕过声明。
- `class PromptResolutionError(ValueError)`（`kigumi.prompt`，顶层导出）：运行时绑定不存在、
  路径类型不符，或持久化 resolution schema/digest 不可信。修正输入/参数/item/carry 绑定；
  digest 损坏时新建可信 run，不能猜测恢复。
- `class RequestTooLarge(ValueError)`（`kigumi.prompt`，顶层导出）：`preflight` 发现估算 token、
  附件数量或附件总字节超过 `PreflightPolicy` 上限；读取 `.report` 拿到每一项
  `PreflightViolation`。在缓存查找和 provider 请求之前抛出，因此超大请求不会先计费再失败。
- `class KigumiPromptWarning(UserWarning)`（`kigumi.prompt`，顶层导出）：`inject` 发现 dict 的
  键全是数字字符串，`canonical_json(sort_keys=True)` 可能破坏调用方想表达的数值顺序。把
  有序数据改为 list。

### DAG、重试与存储

- `ResourceRequest(name: str, units: int = 1, scope: str = "global") -> None`
  （`kigumi._declarations`，顶层导出）：声明节点运行时容量需求（GPU、供应商配额、CPU 槽位）。
  节点用 `resources=(ResourceRequest("gpu"),)` 声明，`dag.run(resource_limits={"gpu": 1})`
  给出运行期上限；未声明资源的节点走 `None` 默认池。多资源按名字确定序获取，避免互等。
- `class CacheIntegrityError(RuntimeError)`（`kigumi.errors`，顶层导出）：缓存条目存在但不可
  安全复用（JSON 撕裂、响应为空、`response_sha256` 不匹配）。损坏不再退化成 miss，因此不会
  静默重新计费；先核对该条目再决定是删除重算还是修复。`CacheLookup`（`kigumi.store`）是缓存
  读取结果的三态事实：`MISSING` / `VALID` / `CORRUPT`，附带期望与实际摘要。
- `class StateIntegrityError(RuntimeError)`（`kigumi._runstate`，顶层导出）：durable attempt
  状态损坏。损坏与缺失不再共用"从未开始"这一条路径，避免把越过 side-effect 边界的 attempt
  当成没跑过。见 [retry/resume 契约](contracts/retry-resume.md)。
- `RetryExhausted(target: str, attempts: int, failure: dict[str, object]) -> None`
  （`kigumi.retry`，顶层导出）：durable `RetryPolicy` 已消费最后一次允许的 attempt。通过
  `trace` / `runs show` 检查 canonical failure，修复根因后用新 run 执行；不要继续隐式重试。
  见 [retry/resume 契约](contracts/retry-resume.md)。
- `AmbiguousAttemptError(run_id: str, target: str, attempt: int) -> None`
  （`kigumi.retry`，顶层导出）：崩溃 attempt 已越过外部 side-effect 边界，但没有可信成功
  candidate，框架无法判断 effect 是否发生。先核对 provider/Pi 证据，再用
  `kigumi retry-resolve ... --action retry|fail --reason ...` 明确裁决。
- `class UndeclaredInputError(RuntimeError)`（`kigumi.dag`，顶层导出）：节点通过
  `ctx.read_text` / `ctx.read_bytes` 访问了 `files` 与当前 `files_fn` 之外的路径。补声明并让
  缓存自然换键；不要改回裸文件读取。见 [缓存键契约](contracts/cache-key.md)。
- `class OutputOwnershipError(RuntimeError)`（`kigumi.errors`，顶层导出）：同一 run 的两个
  producer 认领等价物化路径，或目标解析越出项目根。给每个节点/item 唯一路径；失败方尚未
  覆盖赢家。见 [输出所有权契约](contracts/output-ownership.md)。
- `CheckpointPending(name: str, payload: Any) -> None`（`kigumi.dag`，顶层导出）：
  `ctx.checkpoint` 没有找到绑定当前 payload 摘要的批准。通常由 `Dag.run` 汇总进
  `RunResult.pending_checkpoints`；批准后使用同一个 `run_id` 恢复。见
  [检查点契约](contracts/checkpoint.md)。
- `SlotTimeoutError(wait_seconds: float) -> None`（仅
  `from kigumi.slots import SlotTimeoutError`）：`FileSlots.acquire` 在受保护工作开始前未能
  取得 slot。降低并发、提高 timeout 或检查共享 lock/capacity 配置；Agent 路径会把它转成
  `AgentRuntimeFailureCode.CAPACITY`。见 [Agent 容量契约](contracts/agent-capacity.md)。
- `class WorkflowProfileError(RuntimeError)`（仅
  `from kigumi.profile import WorkflowProfileError`）：schema-2 WorkflowProfile 或关联
  receipt 缺失、schema 不支持、结构无效或 digest 不匹配。保留损坏现场并重新生成可信 run；
  不要手改摘要。见 [WorkflowProfile 契约](contracts/workflow-profile.md)。
- `class RunManifestError(RuntimeError)`（仅
  `from kigumi._runstate import RunManifestError`）：既有 schema-2 run 与当前声明不匹配，
  或 manifest、attempt state、candidate、artifact/Prompt lineage 摘要损坏。用原声明恢复，
  声明已变则新建 run；损坏一律 fail closed。见 [retry/resume 契约](contracts/retry-resume.md)。

`FileSlots.acquire_key(key)` 与 `acquire()` 使用同一启用条件和 lock root：未配置时是 no-op，启用
时用 `key_<sha256(key)>.lock` 保护一次 L1 key 的二次 cache check、provider 请求和缓存写入，
并在 `finally` 中释放。它只消除同 key 的重复穿透，不提供跨进程预算总账。与
`acquire(timeout_seconds=...)` 不同，`acquire_key()` 没有 timeout，也不会抛
`SlotTimeoutError`；等待方会一直阻塞到持锁方释放锁或进程消失。正常情况下持锁时长由 transport
timeout 约束，但 SIGSTOP 或无 timeout 的 transport 会让等待没有上界。等待方没有 timeout 诊断；
运维应检查 lock root 下对应的 `key_<sha256>.lock` 文件及其持锁进程。

### Agent

- `class AgentError(RuntimeError)`（`kigumi.agents`，顶层导出）：外部 Agent 边界错误族的基类；
  当前具体子类 `AgentCapabilityError` 与 `AgentResultError` 不从顶层导出。前者表示 task 请求
  adapter 不具备的 capability，后者表示 adapter result、attachment/session 或 canonical
  artifact 违反契约；修正 capsule/task/adapter，不要当作 provider transient failure 重试。
- `AgentExecutionFailure(*, provider_failure: ProviderFailure | None = None, runtime_code: AgentRuntimeFailureCode | None = None) -> None`
  （`kigumi.failures`，顶层导出）：Agent 执行恰好产生一个 provider failure 或一个封闭的
  runtime code。检查 `provider_failure` / `runtime_code` 与 origin evidence；只有声明的
  provider kind 可自动 retry，runtime failure 默认需修配置或人工处置。见
  [Agent node 契约](contracts/agent-node.md)与 [Failure 契约](contracts/failure.md)。

## 枚举与策略值

### `CachePolicy`

`CachePolicy = Literal["auto", "refresh", "off"]`，定义于 `kigumi._declarations` 并从顶层导出：

- `"auto"`：读取并写入 L3 cache。
- `"refresh"`：跳过读取，执行后替换同一 L3 条目。
- `"off"`：不读也不写 L3 cache。

三种值只控制 L3 读写，不进入内容键，也不改变 L1 行为。见
[只消费上游局部](adoption.md#只消费上游局部consumes)与[缓存键契约](contracts/cache-key.md)。

### `EvidenceMode`

`EvidenceMode = Literal["full", "redacted", "hash_only"]`，定义于 `kigumi.evidence` 并从顶层
导出。`full` 保存清理后的内容，`redacted` 保存结构、摘要与必要 metadata，`hash_only`
只保存摘要/字节数/media type 等描述符；三者都先清理 secret。见
[EvidencePolicy 契约](contracts/evidence.md)。

### Provider failure

`ProviderFailureKind(StrEnum)` 的 9 个成员：

- `RATE_LIMIT = "rate_limit"`
- `SERVER_ERROR = "server_error"`
- `TIMEOUT = "timeout"`
- `CONNECTION = "connection"`
- `AUTHENTICATION = "authentication"`
- `AUTHORIZATION = "authorization"`
- `INVALID_REQUEST = "invalid_request"`
- `MODEL_MISMATCH = "model_mismatch"`
- `UNKNOWN = "unknown"`

`ProviderFailureStage(StrEnum)` 的 4 个成员：

- `REQUEST = "request"`
- `TRANSPORT = "transport"`
- `PROVIDER = "provider"`
- `RESPONSE = "response"`

两者定义于 `kigumi.failures` 并从顶层导出；分类边界见
[Failure 契约](contracts/failure.md)。

### Agent runtime failure

`AgentRuntimeFailureCode(StrEnum)` 定义于 `kigumi.failures` 并从顶层导出，共 8 个成员：

- `SPAWN_NOT_FOUND = "spawn_not_found"`
- `SPAWN_PERMISSION = "spawn_permission"`
- `SPAWN_FAILURE = "spawn_failure"`
- `VERSION_MISMATCH = "version_mismatch"`
- `PROCESS_EXIT = "process_exit"`
- `PROTOCOL = "protocol"`
- `POLICY = "policy"`
- `CAPACITY = "capacity"`

这些值只描述非 provider 的 Agent runtime 失败，见 [Failure 契约](contracts/failure.md)。

### Retry schedule

- `RetrySchedule(next_attempt: int, delay_seconds: float, due_at: str) -> None`：`RetryPolicy.schedule`
  返回的不可变调度值；`due_at` 是持久化的绝对时间。详见
  [retry/resume 契约](contracts/retry-resume.md)。

## 结果与只读视图

### 评估与进化

`evolve_prompt` 是实验性、内容级 recipe，只对普通提示词字符串做候选演化。它不是
DAG/Agent 优化器、durable run recovery 或无偏的泛化估计器，也不自动晋升候选；其
`Candidate`、`EvolveResult` 与 `evolve_prompt` 导入继续保持兼容。需要可比较证据时见
`bench` 与 `FunctionSubject`/`CallerSubject`/`DagSubject`/`AgentSubject`。验证反馈隔离只有在
train 与 validation 内容互斥时才成立；调用方应在运行前自行验证不重叠，框架不做运行时检查。
采用候选由调用方/人工负责：先审阅 `result.best`，再手动把批准文本写入 `prompts/*.md`，最后
项目用 `PromptRef` 引用该既有文件，或用 `PromptSpec` 组合它；`PromptSpec` 只声明组合。没有
晋升 API，也不会自动写入。

- `Judgment(score: float, feedback: str, tags: tuple[str, ...] = (), subscores: dict[str, float] | None = None) -> None`：
  单样例的 `[0, 1]` 主分、反思评语、错误标签与可选子分。见
  [指标怎么写](adoption.md#指标怎么写)。
- `Candidate(text: str, parent: int | None, train_scores: dict[str, float], val_scores: dict[str, float], round: int) -> None`：
  一个已接受候选及其父本、train/val 分数和轮次；不是每轮拒绝原因的完整事件记录。
- `EvolveResult(best: str, candidates: list[Candidate], metric_calls: int, rounds_run: int, generalization_gap: float) -> None`：
  `evolve_prompt` 的返回结果；`best` 只返回给调用方，不自动写入 `prompts/`。`generalization_gap`
  只是当前已记录 train/val 分数的均值差，不能作为无偏的通用泛化估计。见
  [进化怎么跑](adoption.md#进化怎么跑)。

传入 `state_path` 时，续跑依赖本地 JSON 算法检查点；它不是带副作用感知的 durable run
receipt，也不能替代 DAG 的 retry/resume/recovery 语义。

### DAG

- `ExplainResult(status: str, changed: list[str], details: dict[str, dict[str, str]], pending_on: tuple[str, ...] = ()) -> None`：
  `Dag.explain` 的缓存判断、变化成分与 unknown 依赖链。见
  [排障链路](adoption.md#排查链路agent-视角)。
- `PlanResult(nodes: dict[str, str], pending_on: dict[str, tuple[str, ...]]) -> None`：
  `Dag.plan` 的节点/item 状态；属性 `misses`、`certain`、`at_risk` 给出工作上界、下界与额外
  风险。见 [请求前成本预览](adoption.md#请求前成本预览plan)。
- `RunResult(artifacts: dict[str, dict[str, Any]], cache_hits: list[str], pending_checkpoints: list[str], run_id: str, skipped: list[str], map_items: dict[str, dict[str, str]], pending_retries: list[str], ambiguous_attempts: list[str], run_status: str) -> None`：
  一次 run 的产物、命中、挂起、跳过与 durable 状态总览。
- `RecoveryReceipt(recovery_time: str, from_attempt: int, to_attempt: int, decision: Literal[...], reason: str, evidence_refs: list[str], recovered_by: str) -> None`：
  `Dag.recover()` 的 append-only 决策记录；终态失败恢复后调用同一 run 的 `Dag.resume()`。
  见 [恢复终态失败 run](recovery.md)。

### Agent

- `AgentCapabilities(filesystem: bool = True, terminal: bool = False) -> None`：adapter 声明的
  capability；task admission 会据此拒绝不支持的请求。
- `AgentRequest(task: AgentTask, inputs: dict[str, dict[str, Any]], spec: AgentSpec) -> None`：
  框架交给 `AgentAdapter.run` 的规范请求。
- `AgentRunContext(workspace: Path, capsule_root: Path, deadline: float, emit_event: Callable[[Mapping[str, Any]], None], record_evidence: Callable[[str, bytes, str], None], session_in: bytes | None = None, record_session: Callable[[bytes], None] | None = None) -> None`：
  adapter 执行期的 staged 路径、deadline、事件/证据回调与显式 session 通道。
- `AgentRunResult(completion: AgentCompletion, usage: Mapping[str, Any] | None = None, metadata: Mapping[str, Any] = field(default_factory=dict)) -> None`：
  adapter 返回的 completion 与非 canonical 执行 metadata。
- `AgentResultView(artifact: Mapping[str, Any], blob_store: BlobStore) -> None`：先验证 Agent
  artifact 与 blob digest，再提供 `list`、`select`、`read_bytes`、`read_text`、`publish`
  只读访问。节点内由 `ctx.agent_result(artifact)` 构造。见
  [外部 Agent 节点](adoption.md#外部-agent-节点)。

### 实验

- `TrialContext(example_id: str, seed: int, trial_id: str, project_root: Path, evidence_root: Path) -> None`：
  一格实验的稳定身份与隔离根。
- `TrialObservation(output: Any, usage: Mapping[str, Any] | None = None, evidence: Mapping[str, Any] = field(default_factory=dict), seed_applied: bool = False, duration_seconds: float | None = None) -> None`：
  subject 返回的输出、usage、证据和 seed/duration 事实。见
  [统一实验主体](adoption.md#统一实验主体workflow-与-agent-使用同一证据网格)。
- `Metric = Callable[[dict[str, Any], Any], Judgment]`：只依据 example 与 subject output 返回
  `Judgment`；它定义质量轴，不接收 trial、seed 或运行时失败上下文。
- `bench(variants, examples, metric, *, seeds=range(5), pass_threshold=None, experiment_dir=None, report_path=None) -> dict[str, Any]`：
  返回 report schema 3。variant 保留 `mean`、`stdev`、`by_example` 与可选 `pass_rate` 质量聚合，
  另有 `outcome_summary` 运行结果轴：`trial_count`、`subject_successes`、`metric_successes`、
  `subject_failures`、`metric_failures` 及三个以计划格数为分母的 rate。合法的零分 Judgment
  仍是质量结果，不等同于 subject failure；stage 细节仍在 raw trial 的 `error`。

### Prompt

- `Attachment(path: str, content_hash: str, mime_type: str, size_bytes: int) -> None`：内容寻址的
  附件 manifest。只记录路径、内容摘要、MIME 与字节数，不保存文件字节本身，因此可以进
  provenance 而不把二进制内容拖进 artifacts。
- `Message(role: str, parts: list[str | dict[str, Any]]) -> None`：类型化请求消息。`parts` 里
  的 dict 表示非文本部件（如附件引用），provider 看到的顺序就是列表顺序。
- `ResponseSpec(schema_sha256: str | None = None, format: str = "text") -> None`：响应格式与
  schema identity；`format` 取 `text`、`json` 或 `structured`。schema 变化会改变缓存键，
  因此换 schema 不会复用旧的结构化响应。
- `PromptResolution(spec_name: str, structure_digest: str, base: Mapping[str, Any], layers: tuple[Mapping[str, Any], ...], axes: tuple[Mapping[str, Any], ...], materials: tuple[Mapping[str, Any], ...], rendered_sha256: str, rendered_bytes: int, schema: int = 1, messages: list[Message] = [], attachments: list[Attachment] = [], response_spec: ResponseSpec = ResponseSpec()) -> None`：
  不含 Prompt 原文、但携带完整请求 manifest 的不可变解析 provenance。见
  [分层 Prompt 解析契约](contracts/prompt-resolution.md)。
- `PreflightPolicy(max_tokens: int = 200_000, max_attachments: int = 50, max_attachment_bytes: int = 104_857_600) -> None`：
  请求预检上限。
- `PreflightViolation(check: str, limit: int, actual: int, message: str) -> None`：单条超限事实，
  `check` 为 `token_count` / `attachment_count` / `byte_size`。
- `PreflightReport(violations: list[PreflightViolation], estimated_tokens: int, total_bytes: int) -> None`：
  预检结论；`is_valid()` 在没有任何 violation 时为真。
- `ResolvedPrompt(value: str, resolution: PromptResolution) -> ResolvedPrompt`：携带
  `PromptResolution` lineage 的不可变 `str` 子类；普通字符串运算可能抹去 lineage。
- `Clipped(text: str, clipped: bool, original_chars: int, kept_chars: int, event: dict[str, int | str] | None) -> None`：
  `clip` 的文本与显式截断事件。

### L0 transport

- `Response(text: str, usage: dict[str, Any], finish_reason: str | None, reasoning: str | None = None, model: str = "", provider_response_id: str | None = None, model_observed: bool = False) -> None`：
  所有 transport 返回的规范结果。`ProviderFailure` 的签名与处置见
  [异常一节](#修复调用与-transport)。

## 工具函数

### L1.5 Prompt

- `load_template(path: Path) -> str`：以 UTF-8 读取显式模板路径。
- `render_template(text: str, slots: dict[str, str]) -> str`：按全等槽位集合渲染；不接受 missing
  或 extra。见 [零真实请求的测试](adoption.md#零真实请求的测试)。
- `slot_names(text: str) -> list[str]`：按首次出现顺序返回去重后的 `{{slot}}` 名。
- `schema_format_section(model_cls: type[BaseModel], *, with_example: bool = True) -> str`：
  从 Pydantic model 生成字段说明与可选递归 JSON skeleton。
- `render_items(items: list[Any], *, format: Literal["json", "bullets"] = "json") -> str`：
  确定渲染 JSON fenced material 或缩进项目符号。
- `section(title: str, value: str | None) -> str`：只在 body 非空时生成带标题、末尾换行的小节。
- `inject(obj: Any, *, title: str | None = None) -> str`：把字符串或 JSON 数据放入不会被内容
  提前闭合的确定性 fence。见[核心心智模型](adoption.md#二核心心智模型)。
- `clip(text: str, limit: int, *, boundary: Literal["line", "sentence"] = "line") -> Clipped`：
  在安全边界截断并返回公开的截断事件。
- `preflight(resolution: PromptResolution, policy: PreflightPolicy = PreflightPolicy()) -> PreflightReport`：
  在缓存查找与 provider 请求之前估算 token、统计附件数量与总字节。返回报告而不抛异常；
  调用层在 `is_valid()` 为假时抛 `RequestTooLarge`。

### 评估

- `evaluate(task: Callable[[dict[str, Any]], Any], examples: list[dict[str, Any]], metric: Metric) -> list[Judgment]`：
  串行执行任务与评估；单样例 task/metric 异常记为 0 分而不中断整批。
- `gated_metric(gate: Metric, quality: Metric) -> Metric`：合规 gate 未满分时不调用 quality。
- `llm_judge(caller: Caller, *, rubric: str, model: str = "default", wording: str | None = None, max_repairs: int = 2) -> Metric`：
  构造独立 rubric 评分器；默认中文措辞与覆盖方法见
  [内置中文提示词](adoption.md#内置中文提示词与覆盖)。
- `pairwise_judge(caller: Caller, *, rubric: str, reference_key: str, model: str = "default", wording: str | None = None, max_repairs: int = 2) -> Metric`：
  构造以 reference 为水准线的评委；verdict 映射见同一
  [提示词小节](adoption.md#内置中文提示词与覆盖)。

### 存储与内容寻址

- `diff_runs(runs_root: Path, run_a: str, run_b: str) -> dict[str, list[str]]`：按 canonical
  artifact hash 返回 `changed` / `only_a` / `only_b`。
- `write_artifact(path: str | Path, data: str, meta: Mapping[str, Any]) -> None`：原子写文本产物及
  带默认 `created_at` 的 `.meta.json` sidecar。
- `sha256_file(path: str | Path) -> str`：流式计算文件 SHA-256。
- `atomic_write_json(path: str | Path, obj: Any) -> None`：以 `canonical_json` 原子替换 JSON 文件。
- `atomic_write_text(path: str | Path, text: str) -> None`：以 UTF-8、fsync 和同目录 rename 原子
  替换文本。
- `canonical_json(obj: Any) -> str`：唯一确定性 JSON 格式：UTF-8 字符、键排序、两空格缩进。
- `sha(obj: Any) -> str`：字符串直接按 UTF-8，其余对象先经 `canonical_json`，再返回 SHA-256。
- `gc_cache(cache_root: Path, runs_root: Path, keep_last: int) -> int`：删除最近保留 run 不可达的
  node cache，返回删除数。
- `gc_artifacts(artifacts_path: Path, keep_last: int) -> int`：同时 GC node cache 与 blob。见
  [保留契约](contracts/retention.md)。
- `approve_checkpoint(runs_root: Path, run_id: str, name: str, data: Any) -> None`：写入绑定 pending
  payload 摘要的批准并删除 pending marker。见[检查点契约](contracts/checkpoint.md)。

### 配置

- `find_project_root(start: Path) -> Path | None`：向上寻找最近的 `pyproject.toml`。
- `load_config(project_root: Path) -> KigumiConfig | None`：仅在存在 `[tool.kigumi]` 时加载已知
  配置键，未知键报错。
- `load_env(env_path: Path) -> list[str]`：从简单 `.env` 加载进程中尚未设置的键，并返回实际
  加载键名。优先级与完整变量表见[环境变量总表](adoption.md#环境变量总表)。

### 调用观测

- `observe() -> Iterator[list[dict[str, Any]]]`：上下文管理器，收集当前 context 内每次
  `LLMCaller` 调用事实；并行 context 之间不串线。

### 静态守卫（`kigumi.enforce`）

这些符号是模块级 AST 工具，不从 `kigumi` 顶层导出，也不执行项目代码；它们返回 finding
列表，不定义 CLI 或 pytest 的退出语义。项目接入时的选择、wrapper 和失败码见
[守卫四环](adoption.md#守卫四环)。

- `Finding(path: Path, lineno: int, snippet: str, waived: bool, waiver_reason: str | None) -> None`：
  循环裸 LLM finding；`waived` 只有在行尾 `raw-llm-ok` 后确实有非空理由时为真。
- `RawIOFinding(path: Path, lineno: int, snippet: str, waived: bool, waiver_reason: str | None) -> None`：
  节点直接文件读取 finding；其 `raw-io-ok` 豁免与 `Finding` 的豁免独立。
- `waiver_reasons(text: str) -> list[str]`：按源码行序提取所有 `raw-llm-ok` 理由，保留重复项。
- `raw_io_waiver_reasons(text: str) -> list[str]`：只提取 `raw-io-ok` 理由，不与另一类混合。
- `check_source(text: str, path: Path) -> list[Finding]`：检查一段源码中循环/推导式下的
  `.call()` 与 `.llm()`。
- `check_paths(source_dirs: list[Path]) -> list[Finding]`：递归检查目录中的 Python 文件的
  循环裸 LLM 调用；不存在的目录跳过。
- `check_raw_io_node_source(text: str, path: Path) -> list[RawIOFinding]`：检查一个模块中
  顶层 `node`/`map`/`scan`/`foreach`/`agent` 装饰器函数的最外层函数体。
- `check_raw_io_node_paths(source_dirs: list[Path]) -> list[RawIOFinding]`：对目录递归执行
  上一项节点装饰器过滤后的 raw-I/O 检查。
- `check_raw_io_source(text: str, path: Path, *, context_name: str = "ctx") -> list[RawIOFinding]`：
  检查已知单个节点函数体；只把指定上下文对象的 `read_text`/`read_bytes` 视为受控读取。
