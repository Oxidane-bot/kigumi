# kigumi 接入指南与最佳实践

本文回答三个问题:怎么把一个项目接到 kigumi 上;用的时候要注意什么;
我们推荐的写法是什么。设计动机见 [DESIGN.md](../DESIGN.md)。

可验证不变式的权威文本见 [契约索引](contracts/README.md)，面向使用者的发布变化见
[CHANGELOG.md](../CHANGELOG.md)。

想先在零真实请求下看完整路径，可运行[客服工单抽取 DAG](https://github.com/Oxidane-bot/kigumi/blob/master/examples/ticket_extract/README.md)
或[提示词进化环](https://github.com/Oxidane-bot/kigumi/blob/master/examples/prompt_evolve/README.md)（示例只在仓库里，不随 wheel 交付）。前者用 150 张固定 seed 合成工单测量
map/cache/GC，后者验收 train/val 隔离、拒收与 state 续跑；两者都保留已知框架摩擦。

## 进场协议（改节点前先做这三步）

不熟悉这个库时，先读两页；装好之后不必回仓库，两页都随 wheel 交付：

```bash
kigumi brief          # 已有能力、别重造什么、CLI 命令说明（见 brief.md）
kigumi docs           # 列出全部交付页，再 kigumi docs <name> 读其中一页
```

改节点之前，先用框架自己的工具看清楚状态，再动手。这三步不会发真实请求，不花钱：

```bash
# 1. 看现在的状态：节点、map 项、缓存策略与每次 LLM 调用的证据链
kigumi trace <run_id>
kigumi trace <run_id> --node <name> --json   # 只看某个节点，适合 json 消费

# 2. 预览改动之后哪些会重算（哪些会花钱）
#    certain = 确定会 miss；at_risk = 可能 miss；pending_on = 因 unknown 上游待定
kigumi plan

# 3. 查某个节点上次为什么命中或失效
kigumi explain <node>
kigumi explain <node@item>   # map/scan 的单个 item
```

这三条里后两条要读内存里的图，因此需要 `[tool.kigumi]` 声明 `dag_entry`
（`kigumi init` 会写好）；未声明时命令以 2 退出并告诉你补哪个键。程序内可以直接调用
`dag.plan()` 与 `dag.explain()`。完整命令参数见 [CLI 参考](cli.md)；
所有症状的排查流程见[六、排障](#六排障按症状查)。



### 1. 安装与配置

```bash
uv add --editable ~/Documents/Projects/kigumi        # 本地开发
uv add --editable "~/Documents/Projects/kigumi[litellm]"  # 需要真实模型调用时
```

项目 `pyproject.toml` 里声明(全部有默认值,零配置可跑):

```toml
[tool.kigumi]
prompts_dir = "prompts"          # 模板根目录,模板文件为 <name>.md
artifacts_dir = "artifacts"      # runs/ 与节点缓存的根
llm_cache_dir = "artifacts/_llm" # L1 LLM 载荷；须与 LLMCaller.cache_dir 一致
source_dirs = ["nodes", "lib"]   # 两个用途:helper 源码哈希 + 守卫扫描
env_file = ".env"                # KIGUMI_MODEL_DEFAULT / KIGUMI_MODEL_PRO / api key
agent_slots = 1                  # 外部 Agent 默认全局串行
agent_lock_dir = "artifacts/_locks/agents"
agent_slot_timeout_seconds = 300
dag_entry = "nodes.graph:build_dag"  # 图命令的入口；不声明则只有项目运维命令可用
```

`dag_entry` 是唯一一个"打开一组命令"的键。`kigumi plan` / `describe` / `explain` /
`check` / `graph` / `profile` / `resume` / `retry-resolve` 要读内存里的图，而节点是靠
装饰器在 import 时注册的，所以这些命令必须 import 一个返回 `Dag` 的工厂函数。指向它，
这组命令就能在项目里直接敲；不指向，其余命令照常工作。`kigumi init` 会生成
`nodes/graph.py` 骨架并写好这个键。

工厂函数被 import 时必须只注册节点、不产生副作用——图命令只读拓扑，不应触发任何执行。

工厂可以带**按名传入**的参数，供图形状或 `params` 依赖运行时输入的项目使用：

```python
def build_dag(episode: str) -> Dag:  # dag_entry = "nodes.graph:build_dag"
    ...
```

```bash
kigumi plan --graph-arg episode=E2S4
```

`--graph-arg` 可重复，值一律以 `str` 传入，由工厂自己转成 `Path` / `int` 并校验。
零参工厂不需要它。缺必需参数、给了工厂不接受的名字、同名给两次、参数是
positional-only 时都以 2 退出并指名问题所在，完整规则见
[CLI 参考的 `--graph-arg`](cli.md#--graph-arg-keyvalue)。

**不要为了让图命令能跑而给占位默认值。** `params` 是缓存键成分，占位值下 `plan`
预告的是一个不会被任何真实运行使用的键空间、`explain` 把每个节点都报成 `params`
变化，而 `resume` 会带着错误的 `graph_identity` 真的执行。传真实值，或者接受这组
命令在该项目里给不出可信结论。

`.env` 里放模型别名与密钥(密钥永远不进 git,`kigumi doctor` 只报键名不报值)。
`KIGUMI_AGENT_SLOTS`、`KIGUMI_AGENT_LOCK_DIR`、
`KIGUMI_AGENT_SLOT_TIMEOUT_SECONDS` 可覆盖容量配置；跨项目共享容量时把 lock dir
显式指到同一个机器级目录。

### 环境变量总表

这是 `KIGUMI_*` 环境变量的单一索引。Agent capacity 的进程环境值覆盖
`[tool.kigumi]` TOML；`load_env()` 从配置的 `.env` 读取时，只补进程中尚未存在的键，
绝不覆盖已经设置的 process variable。显式传给 transport 的 `aliases=` 则不读取
`KIGUMI_MODEL_*`。

| 变量 | 读取方与值 | 缺省/作用 |
| --- | --- | --- |
| `KIGUMI_MODEL_DEFAULT` | `LiteLLMTransport` / `StdlibTransport`；具体模型名 | `aliases=None` 时定义 `default` alias；未设置时使用该 alias 会报错。 |
| `KIGUMI_MODEL_PRO` | 同上；具体模型名 | `aliases=None` 时定义 `pro` alias；未设置时使用该 alias 会报错。 |
| `KIGUMI_AGENT_SLOTS` | `KigumiConfig`；至少为 1 的整数 | 覆盖 `agent_slots=1`。 |
| `KIGUMI_AGENT_LOCK_DIR` | `KigumiConfig`；非空路径 | 覆盖 `agent_lock_dir="artifacts/_locks/agents"`。 |
| `KIGUMI_AGENT_SLOT_TIMEOUT_SECONDS` | `KigumiConfig`；正数 | 覆盖 `agent_slot_timeout_seconds=300.0`。 |
| `KIGUMI_LIVE` | pytest 插件；必须精确等于 `"1"` | 其他值或缺席时自动 skip `live` 测试。 |
| `KIGUMI_REQUEST_LOCK_DIR` | `FileSlots.from_env()`；去空白后的 lock 目录 | 默认 prefix 下的名称；空值使 limiter 不具备 lock root。 |
| `KIGUMI_REQUEST_CAPACITY_FILE` | `FileSlots.from_env()`；去空白后的容量文件 | 可选；设置后动态限制有效 slots。 |
| `KIGUMI_REQUEST_SLOTS` | `FileSlots.from_env()`；整数 | 缺席或无法解析时为 `0`；只有 lock dir 存在且 slots 至少为 1 才启用。 |
| `KIGUMI_WORKSPACE` | `PiRpcAdapter` 注入 Pi 子进程；staged workspace 绝对路径 | Kigumi/Pi Extension 协议值，不是用户配置入口。 |
| `KIGUMI_ALLOWED_TOOLS` | `PiRpcAdapter` 注入；逗号连接的 `AgentSpec.tools` | Extension 据此限制已声明工具。 |
| `KIGUMI_MAX_TOOL_CALLS` | `PiRpcAdapter` 注入；`AgentLimits.max_tool_calls` 的十进制文本 | Extension 的工具调用上限。 |
| `KIGUMI_MAX_TURNS` | `PiRpcAdapter` 注入；`AgentLimits.max_turns` 的十进制文本 | Extension 的 turn 上限。 |

`FileSlots.from_env(prefix="KIGUMI_REQUEST")` 的 prefix 是调用方参数；传
`prefix="ACME_LLM"` 时读取 `ACME_LLM_LOCK_DIR`、`ACME_LLM_CAPACITY_FILE`、
`ACME_LLM_SLOTS`，不是固定读取默认三项。后四个 Pi 变量由 adapter 在 spawn 前覆盖写入，
Extension 只应读取，不应要求操作者手工设置。

### 2. 组装调用栈

自下而上三层,每层都可以单独用:

```python
from pathlib import Path
from kigumi import AdaptiveCapacity, Budget, EvidencePolicy, LLMCaller
from kigumi.transport import LiteLLMTransport

capacity = AdaptiveCapacity("artifacts/request_capacity", max_slots=32)
transport = LiteLLMTransport(..., capacity=capacity)  # L0:发请求/429 自适应容量
caller = LLMCaller(  # L1:缓存/预算/dry-run/溯源
    transport,
    cache_dir=Path("artifacts/_llm"),
    budget=Budget(max_tokens=2_000_000),
    evidence_policy=EvidencePolicy(response="redacted"),
)
```

- **命令式链项目**(脚本一条路跑到底):到这一层就够了,直接
  `caller.call(...)` / `repair.call_validated(...)`。
- **多阶段流水线**:再上 `Dag`(L3),换取节点级缓存、断点续跑、
  人工检查点、run diff。

### 预算准入：先预留，再计费

如果一个 map 会展开成很多付费调用，问题不是“调用完成后再统计”，而是多个并发
miss 可能在额度被观察到以前一起发出。`Budget` 的上限按 token 计：先用
`Budget.reserve()` 拿到 `BudgetPermit`，成功用 provider 返回的 `total_tokens`
提交，失败或取消释放预留。

```python
from kigumi import Budget, BudgetPermit

budget = Budget(max_tokens=10)
permit: BudgetPermit = budget.reserve(6)
permit.commit({"total_tokens": 4})

refunded = budget.reserve(6)
refunded.cancel()
print(budget.spent)
```

输出：

```text
4
```

实际使用 `LLMCaller` 时只需把同一个 `budget` 传给
`LLMCaller(..., budget=budget)`：L1 miss 会在 provider 请求前自动 `reserve`，
cache hit 不预留，provider/transport 失败会 `cancel`，成功响应按实际用量 `commit`。
预估值是 prompt 长度加显式 `max_tokens` 的 best effort；provider 实际用量可能更高，
所以 `commit` 仍可能在请求完成后抛 `BudgetExceeded`，但响应已经写入缓存。

不要把它当作跨进程或跨主机的总账：预留和同 key single-flight 只在当前进程内协调。
已经有 file lock、分布式 quota 服务，或调用根本不产生 provider 费用时，不要再叠一层
`BudgetPermit`；跨进程限流应使用应用自己的协调器。

### 3. 声明节点(DAG 项目)

```python
from kigumi import Dag, PromptRef, PromptSpec
from kigumi.config import KigumiConfig


def observe(node_name, artifact, cache_hit):
    log_node(node_name, cache_hit=cache_hit)  # 指标、日志或追踪的观测钩子


dag = Dag(KigumiConfig(project_root=Path(__file__).parent), caller, post_node=observe)


OUTLINE = PromptSpec(name="outline", base=PromptRef("outline"))


@dag.node("outline", prompt_specs=(OUTLINE,), files=("fixtures/style.md",))
def outline(inputs, ctx):
    return {"text": ctx.call(ctx.resolve_prompt("outline"))}


@dag.node("review", deps=("outline",))
def review(inputs, ctx):
    approved = ctx.checkpoint("outline_ok", inputs["outline"])  # 人工闸
    return {"approved": approved}


result = dag.run(workers=4)
```

节点需要项目内绝对路径时使用只读的 `ctx.project_root`，它始终是配置项目根的已解析
`Path`；不要为此把 `root` 通过 `build_dag` 闭包传进节点体。

节点必须把**所有会影响输出的东西声明出来**:上游用 `deps`、Prompt 用
`prompt_specs`、数据文件用 `files`、标量参数用 `params`。缓存键由这些声明 +
节点函数源码 + source_dirs 全部源码共同决定——没声明的输入 = 缓存不知道
它变了 = 陈旧结果静默复用。这是接入时最容易犯的错。

节点体内读文件必须走 `ctx.read_text(path)` / `ctx.read_bytes(path)`:它们按与缓存键
相同的解析规则校验路径属于本节点的 `files` ∪ 本项 `files_fn` 声明,未声明直接抛
`UndeclaredInputError`,把"静默陈旧"变成响亮报错。节点体里的裸 `open()`/
`path.open()`/`.read_text()`/`.read_bytes()` 会在注册期被 AST 守卫拒绝(见守卫一节);
守卫边界刻意只限节点函数体,helper 内的读取不扫描——所以经 helper 读文件时,请把
文本读好后传给 helper,或让 helper 收 `ctx`。

`post_node` 的签名是 `(node_name, artifact, cache_hit)`；它在每个节点产物
可用后调用，适合写日志、指标或追踪，不能用来改变 DAG 执行语义。

`workers=1` 保持串行执行；`workers>=2` 会并行提交所有依赖均已产出的就绪节点。
结果中的缓存命中、检查点和跳过节点仍按本次拓扑序排列；并发节点同时失败时，
对外仍抛拓扑序最靠前的异常，其余失败会作为异常附注一并保留。检查点或跳过只阻断
其下游，不阻断无关旁支。

### 节点级资源准入：GPU、供应商配额与 CPU 槽位

当“这个节点需要一张 GPU，而且两个同类节点不能同时运行”时，单独调小全局
`workers` 会连不需要 GPU 的节点一起限住。把需求声明在节点上，再在本次 run
给出容量上限：

```python
from kigumi import ResourceRequest


@dag.node("render_a", resources=(ResourceRequest("gpu"),))
def render_a(inputs, ctx):
    del inputs, ctx
    return {"done": True}


@dag.node("render_b", resources=(ResourceRequest("gpu"),))
def render_b(inputs, ctx):
    del inputs, ctx
    return {"done": True}


result = dag.run(workers=4, resource_limits={"gpu": 1})
print(sorted(result.artifacts))
```

输出：

```text
['render_a', 'render_b']
```

同名 `ResourceRequest` 共享一个 run 内资源池；`units` 是一次执行占用的单位数。
没有 `resources=` 的节点使用 `None` 默认池，可以用
`resource_limits={None: 2}` 显式限制它们；未在 `resource_limits` 中出现的资源名
默认使用 `workers` 作为上限。资源声明只影响调度，不进入缓存键。需要同时占用多种
资源时，框架按名称固定获取顺序；请求单位数超过上限会在 run 开始时拒绝。

一个重要的迁移点：map item 与普通父节点现在共用同一个 run-wide concurrency plane，
不再由 map 节点在某个 scheduler worker 内另开一个 `workers` 大小的嵌套池。于是旧图里
调过 `workers` 的有效并发不再是“父池乘以 map 子池”；没有 `resource_limits` 时，
map item 和就绪普通节点共同竞争这组 `workers`，有命名资源时再由各资源池分别限额。
如果真正要限制的是跨进程、跨主机的供应商配额，`resource_limits` 不够，它是进程内
调度边界；请用 `FileSlots`、`AdaptiveCapacity` 或外部 quota 协调。

如果所有节点都只需要同一个均匀的并发上限，`workers` 已经能表达需求，就不必为每个
节点添加 `ResourceRequest`。

### 分层 Prompt：有限选择、统一材料与 selected-only cache

节点只能用 `prompt_specs=()` 声明 Prompt；节点体内用 `ctx.resolve_prompt()` 取得
managed `ResolvedPrompt`。`load_template` / `render_template` 仍可用于生成 Markdown
报告或配置文件等非 LLM 输出，不用于构造传给 `ctx.call()` 的 prompt：

```python
from kigumi import (
    InputRef,
    ItemRef,
    PromptAxis,
    PromptLayer,
    PromptMaterial,
    PromptRef,
    PromptSpec,
)

PROCESS_ITEM = PromptSpec(
    name="process_item",
    base=PromptRef("base/task"),
    layers=(
        PromptLayer(slot="common", source=PromptRef("common/rules")),
        PromptLayer(
            slot="mode",
            source=PromptAxis(
                name="mode",
                selector=InputRef("config", path=("mode",)),
                variants={
                    "concise": PromptRef("variants/concise"),
                    "detailed": PromptRef("variants/detailed"),
                },
            ),
        ),
    ),
    materials=(
        PromptMaterial(
            slot="context",
            source=InputRef("config", path=("context",)),
            title="运行上下文",
        ),
        PromptMaterial(slot="item", source=ItemRef(), title="当前项"),
    ),
)


@dag.map(
    "process",
    items_from=("source", "items"),
    deps=("config",),
    key_fn=lambda item: item["id"],
    prompt_specs=(PROCESS_ITEM,),
)
def process(item, inputs, ctx):
    prompt = ctx.resolve_prompt("process_item")
    return {"text": ctx.call(prompt)}
```

`PromptRef("base/task")` 对应 `prompts/base/task.md`。base 的槽位必须与
layer/material 完全一致；fragment 不得再含槽位；selector path 必须写成严格 tuple。
`InputRef` 读取的是节点实际函数输入，因此会服从 `consumes` 投影；`ItemRef` 只用于
map/scan，`CarryRef` 只用于 scan，`ParamRef` 读取 `params`。若材料来自节点已经通过
`files=` / `files_fn=` 声明的文件，用 `FileRef`；它读取框架注入的 UTF-8 文件字节，
不把原文写入下游 artifact，也不会绕过 raw-io 守卫。

每次 run 在任何节点执行前一次 snapshot 所有声明 Prompt 文件。当前 axis selection、所选
fragment 和 material digest 进入 L3 key，未选中 variant 的内容不进入；但完整候选 universe
进入 run identity。因此只改未选中候选可以命中相同 selected cache，却会拒绝用旧声明
resume 已有 run。selector 缺失、非字符串或未知 variant 都在 cache lookup、CALL 和 Agent
spawn 前失败。

直接 `ctx.call(ctx.resolve_prompt(...))` 为 managed CALL；拼接、f-string 或显式 `str()` 后
对象变回普通字符串，receipt 如实标记 unmanaged。Agent builder 也可把同一对象直接作为
`AgentTask.instruction`。解析但未实际调用的 spec 只作为节点输入，不会伪造 CALL 记录。

### 托管请求清单：附件、响应 schema 与 preflight

当一次请求同时带文件和结构化输出要求时，只有渲染后的 prompt 字符串是不完整的：
换了附件内容或响应 schema，旧响应都不应该被复用。`PromptResolution` 的 managed
manifest 用 `Message` 保存消息顺序，用 `Attachment` 保存文件路径、内容摘要、MIME
和字节数，用 `ResponseSpec` 保存响应格式与 schema identity；附件原文不会被塞进
provenance。

在低层直接构造 manifest 时，先用 `preflight()` 检查，再决定是否继续：

```python
from kigumi import (
    Attachment,
    Message,
    PreflightPolicy,
    PromptResolution,
    RequestTooLarge,
    ResponseSpec,
    preflight,
)

resolution = PromptResolution(
    spec_name="summarize",
    structure_digest="declared",
    base={},
    layers=(),
    axes=(),
    materials=(),
    rendered_sha256="rendered",
    rendered_bytes=15,
    messages=[Message(role="user", parts=["请总结附件"])],
    attachments=[Attachment("report.txt", "a" * 64, "text/plain", 4)],
    response_spec=ResponseSpec("b" * 64, "structured"),
)
report = preflight(
    resolution,
    PreflightPolicy(max_tokens=100, max_attachments=1, max_attachment_bytes=1024),
)
if not report.is_valid():
    raise RequestTooLarge(report)
print(report.is_valid(), report.total_bytes)
```

输出：

```text
True 4
```

节点路径通常不需要手写 `Attachment`：已声明的 `FileRef` 会在 `ctx.resolve_prompt()`
时读取框架注入的字节并生成 attachment manifest。例如，`prompts/summarize.md` 含有
`{{document}}` 时：

```python
FILE_REQUEST = PromptSpec(
    name="summarize_file",
    base=PromptRef("summarize"),
    materials=(
        PromptMaterial(
            slot="document",
            source=FileRef(ParamRef("path")),
            title="文档",
        ),
    ),
)


@dag.node(
    "summarize_file",
    prompt_specs=(FILE_REQUEST,),
    params={"path": "fixtures/report.txt"},
    files=("fixtures/report.txt",),
)
def summarize_file(inputs, ctx):
    del inputs
    return {"response": ctx.call(ctx.resolve_prompt("summarize_file"))}


result = dag.run()
print(result.artifacts["summarize_file"])
```

`FileRef` 的路径必须同时属于 `files=` 或 `files_fn=` 声明；内容摘要进入 prompt digest，
所以换文件内容会失效而只换路径不会伪造一次新的内容身份。`ctx.call_validated()` 会把
Pydantic schema identity 绑定到 `ResponseSpec`；换 schema 也会换 prompt/cache identity。
`LLMCaller` 会在 cache lookup 和 provider 请求之前自动调用 `preflight()`，超限抛
`RequestTooLarge`，可从异常的 `.report` 读取每项 `PreflightViolation`。不要把这个估算
当作精确计费、访问控制或截断器；如果已经通过 `LLMCaller` 发 managed request，也不要
再手工绕一层同样的预检。

如果输入只是少量普通文本、没有附件或 schema，直接使用现有 `PromptSpec` 和
`ctx.resolve_prompt()` 即可；不要为了让每段文本都显式包一层 `Attachment`。

### Durable retry 与 resume

默认没有 DAG 自动重试；显式 `RetryPolicy` 决定哪些失败可以自动进入 durable retry，
但它不是 side-effect boundary 的开关。普通 provider 节点以及 map/scan item 在 cache
miss、准备执行时都会有 durable attempt；对非 `pure`/`agent` executor，框架在 provider 请求前
持久化 side-effect boundary。这样一个没有声明 `retry` 的节点若在付费调用后崩溃，
恢复时也不会被当成“从未开始”。

```python
from kigumi import AmbiguousAttemptError


@dag.node("paid_call")
def paid_call(inputs, ctx):
    del inputs
    return {"answer": ctx.call("charge")}


# 没有 retry=... 仍会记录 provider side-effect boundary。
try:
    dag.resume("run-0042")
except AmbiguousAttemptError:
    print("先核对 provider 证据，再决定 retry 或 fail")
```

这里的异常表示进程在 provider 请求边界之后没有留下可信成功 candidate；先核对 provider
证据，再用 `retry-resolve` 或 [终态失败 recovery](recovery.md) 作出带理由的决定。

```python
from kigumi import RetryPolicy

retry = RetryPolicy(
    max_attempts=3,
    initial_delay_seconds=1,
    multiplier=2,
    max_delay_seconds=120,
    jitter="full",
)


@dag.node("outline", retry=retry)
def outline(inputs, ctx):
    return {"text": ctx.call("...")}
```

`max_attempts` 含首次执行，默认只重试 rate limit、server error、timeout、connection。
失败后 Kigumi 写 `due_at` 并返回 `RunResult(run_status="pending_retry")`，不会在进程内 sleep；
外部 supervisor 到期调用 `dag.resume(run_id)`。同 run 的 graph、targets、force、源码/libs、
retry/evidence policy、完整 Prompt 候选与 WorkflowProfile 都由 schema-2 `_run.json` 固定，
任何变化 fail closed；缺少 schema-2 manifest 的旧 run 不能 resume。

durable CALL 的 transport/length/empty internal retry 必须全为 0，Pi hidden retry 也必须关闭。
若进程在 provider call/Pi spawn 后崩溃且没有 terminal receipt，状态为 ambiguous，必须人工：

```bash
kigumi retry-resolve run-0042 outline --attempt 1 --action retry \
  --reason "provider logs confirm no accepted result"
kigumi resume run-0042
```

`retry|fail` 都要求 reason 并进入 trace。Kigumi 不承诺外部 effect exactly-once；它保证的是
attempt 边界可见、不能把不确定执行静默当成可安全重试。map 按 item 独立恢复；scan 复用已
验证前缀，只重试失败 item。

### 缓存与 durable state 的完整性边界

缓存读取有三个事实状态：`CacheLookup.state` 为 `MISSING`、`VALID` 或 `CORRUPT`。
缺失才是普通 miss；JSON 撕裂、响应为空、摘要不匹配或 provenance/schema 损坏都属于
`CORRUPT`。DAG 的节点缓存和 `LLMCaller` 的调用缓存遇到损坏会抛
`CacheIntegrityError`，不把它降级成 miss，也不会静默重新计费。若直接使用存储层，
保留三态而不是用 `None` 抹平差异：

```python
from pathlib import Path

from kigumi import CacheIntegrityError, StateIntegrityError
from kigumi.store import node_cache_path, read_node_cache


def read_without_rebilling(artifacts: Path, cache_key: str):
    lookup = read_node_cache(artifacts, cache_key)
    if lookup.state == "MISSING":
        print("cache not generated")
        return None
    if lookup.state == "CORRUPT":
        raise CacheIntegrityError(node_cache_path(artifacts, cache_key), lookup)
    return lookup.data


def resume_without_guessing(dag, run_id: str):
    try:
        return dag.resume(run_id)
    except StateIntegrityError as error:
        print(f"inspect durable state: {error.path}")
        raise


read_without_rebilling(Path("artifacts"), "known-cache-key")
```

输出（该 key 尚不存在时）：

```text
cache not generated
```

`StateIntegrityError` 表示 durable manifest、attempt receipt 或相关 JSON 已经存在但
不可信；与“receipt 缺失、尚未开始”严格区分。看到任一完整性异常时先保留现场并核对
文件、摘要和 provider 证据，再决定修复或新建 run；不要捕获后删除文件然后重跑。

### 终态失败 run：append-only recovery

如果 run 已经是 terminal `failed`，而且你已经知道该失败可以安全再试或完成了外部
修复，问题是“如何保留失败证据并只重跑失败分支”，不是“删掉 attempt 目录”。
用 `Dag.recover()` 写入显式 decision、reason 和 evidence，再用同一个 run 的
`Dag.resume()`：

```python
receipt = dag.recover(
    run_id="run-0042",
    target="transcode",
    from_attempt=3,
    decision="retry_after_external_check",
    reason="已验证替换后的 ffmpeg 能处理失败场景",
    evidence=["validation-report.md"],
)
print(receipt.from_attempt, receipt.to_attempt)
dag.resume("run-0042")
```

输出：

```text
3 4
```

可用 decision 只有 `retry_not_started`、`retry_after_external_check` 和 `fail`。
前者要求你确认失败操作没有越过外部 side-effect boundary，后者记录你完成的外部
核验；`fail` 只记录最终裁决，不排队新 attempt。旧 attempt、`RecoveryReceipt`、
成功节点和 run 目录都保留，`resume()` 会重新验证继承的成功节点，只运行失败节点及
其下游。`recover()` 只能接受 terminal failed run 的当前失败 attempt；pending retry、
approval 或 ambiguous 状态应分别使用 `resume()` 或 `retry-resolve`。完整字段、文件
命名、`recovered_by` 的非认证语义以及并发 recovery 的无锁限制见
[终态失败恢复说明](recovery.md)。

### 只消费上游局部：`consumes`

下游只读取上游 artifact 的一部分时，在依赖边声明实际消费视图：

```python
def prompt_slots(artifact):
    return {"title": artifact["title"], "summary": artifact["summary"]}


@dag.node(
    "draft",
    deps=("outline",),
    consumes={"outline": prompt_slots},
)
def draft(inputs, ctx):
    return {"text": ctx.call(ctx.resolve_prompt("draft"))}
```

投影必须是“完整上游 artifact → JSON 可序列化 `dict`”的纯函数。框架先做 canonical JSON
round-trip，再把视图同时用于 `inputs[dep]` 和既有 `upstream:<dep>` 键成分；因此未消费字段
变化不会击穿下游缓存，消费字段变化仍会准确失效。投影函数源码本身不入键，项目级代码版本
仍由 `source_dirs` 的 `libs` 成分管理。

`node`、`foreach`、map/scan 的共享 `deps` 以及 `Subgraph.node/map/scan` 都支持 `consumes`；
Subgraph 使用挂载前的本地依赖名。不能投影未声明依赖，也不能投影 `items_from` 或
`carry_from` 来源，因为它们分别由 `item` / `carry` 成分表达。投影异常和非法返回会同时标出
节点与依赖名，`plan()` 与 `run()` 对同一故障给出同形态错误；`describe()` 只为确有声明的
节点列出依赖名。

L3 缓存策略在注册时显式声明，非法值（包括 bool、别名和大小写变体）直接拒绝：

```python
@dag.node(
    "inventory",
    cache="refresh",  # auto=读写；refresh=跳读后替换；off=不读不写
    external_fingerprint={"etag": etag},  # 只把 sha(...) 作为 external 键成分
)
def inventory(inputs, ctx): ...
```

`auto` 是既有正常读写；`refresh` 每次执行并替换同一 L3 条目；`off` 每次执行且绝不写
L3。`force=` 仍只是本次 run 的读旁路，不会把 `off` 的写打开。策略不进缓存键，
`external_fingerprint` 的原值也不进 sidecar/describe；后者必须是 canonical JSON 可序列化
数据。`plan()` 把 refresh/off 节点或可独立判断的 item 报为 miss；拿不到其产物时下游保持
unknown。普通节点、map item、scan item 都遵循同一策略，L1 `LLMCaller` 缓存完全不变。

`foreach` 的共享文件写在 `files=`，逐项文件写在 `files_fn=`，两者可并用：

```python
@dag.foreach(
    "analyze_{chunk_id}",
    chunks,
    files=("fixtures/style.md",),
    files_fn=lambda chunk: (chunk["clip"],),
)
def analyze(inputs, ctx): ...
```

逐项文件在注册时固化进各自节点缓存键，绝对路径也可用。
`files=(...)` 声明的每个路径必须在 `run()` 时真实存在；缓存键计算会对缺失文件
直接抛出 `FileNotFoundError`。

每次 `run()` 都会返回 `run_id`。显式传入的 `run_id` 必须是安全的单个非空路径成分：
不得含 `/`、`\\`，也不得等于 `.` 或 `..`。需要比较两次持久化结果时，调用
`dag.diff(run_a, run_b)`；它按节点 artifact 的规范内容哈希给出 `changed`、`only_a`
与 `only_b`，无需重跑节点。

### 静态可复用子图：`Subgraph`

重复的多阶段结构用声明式模板复用，挂载后仍是同一张 Dag、同一调度器和同一缓存：

```python
from kigumi import Subgraph

editorial = Subgraph(inputs=("source",), outputs={"result": "publish"})


@editorial.node("draft", deps=("source",))
def draft(inputs, ctx):
    return {"text": make_draft(inputs["source"])}  # 本地键，不是 qualified 名


@editorial.node("publish", deps=("draft",), cache="auto")
def publish(inputs, ctx):
    return {"text": publish_text(inputs["draft"])}


mounted = dag.mount(editorial, "editorial", inputs={"source": "outline"})
assert mounted == {"result": "editorial.publish"}
```

namespace、端口、本地节点名都只能是单个非空段（不能含 `.`、`@`、`/`、`\\`）。输入绑定
键必须与声明端口完全一致；本地依赖、items/carry source 与输出目标在 mount 时一次性校验，
任何失败都不会留下半挂载节点。模板首次成功挂载后冻结，但可在不同 namespace 重复挂载。
`Subgraph.node/map/scan` 与 Dag 对应装饰器共享注册守卫、缓存策略、外部指纹和检查点语义。

子图拓扑永远静态；运行时动态展开仍只有 map/scan。模型可以生成节点内容，不能返回或修改
可执行图。没有递归/嵌套子图、第二调度器、第二缓存或任意 expansion plan。

### 数据驱动扇出：`map`

`foreach` 适合注册时已经知道的配置项；当列表只能由上游产物在运行时产生时，
用一级 `map` 节点。外图仍是静态 DAG，`map` 内部按有序列表逐项执行：

```python
@dag.map(
    "process",
    items_from=("scan", "items"),
    key_fn=lambda item: item["id"],
    deps=("style_guide",),
    prompt_specs=(DESCRIBE,),
    files=("fixtures/rubric.md",),
    files_fn=lambda item: (item["clip"],),
    params={"tone": "brief"},
)
def process(item, inputs, ctx):
    return {"text": ctx.call(ctx.resolve_prompt("describe"))}
```

`items_from` 的上游会自动成为调度依赖，但其整体 artifact 不进入任何 item
缓存键。第二项可写成点分嵌套键路径，如 `("lint_structure", "structure.segments")`，
并一律逐段按 Mapping 下钻。路径任一段缺失或不是 Mapping 时，错误会报告 map 节点、完整
路径和断裂段。最终值必须是 `list`；每项必须 JSON 可序列化。`key_fn` 生成的 item ID 必须
全体唯一，且是安全的单个非空路径成分：必须为字符串，不得含 `@`、`/`、`\\`，也不得等于
`.` 或 `..`。`@` 保留给运行时 item 寻址，不能出现在普通节点名或 item ID 中。
mapper 的 `inputs` 只含 `style_guide` 等共享 deps，不含 `scan.items` 全列表；item
始终由第一参数传入，`ctx.params` 也只返回共享 params。

每个 item 独立缓存、独立 sidecar 和独立审批文件：
`runs/<run_id>/process@<item_id>.json`。缓存键包含 mapper 源码、运行开始时固定的
libs 哈希、共享 deps、item 内容、共享 prompts/files/params 和 `files_fn(item)` 的
内容哈希；**不包含**列表下标、整列表哈希或 `item_id`。因此增删/修改只影响相关
item，重排不重跑 mapper；改 `key_fn` 只改寻址名称，不会重算内容相同的 item。

map 对下游产出 `{"items": {...}, "order": [...], "count": N}`，所以清单的增删和
重排仍会让依赖该聚合物的下游正确失效。普通 item 失败时其余 item 继续并写缓存，
全部结束后 map 以带 item ID 的 `RuntimeError` 失败；重跑只会补失败项。预算准入失败
是例外：`BudgetExceeded` 会停止后续尚未 admission 的 item，它们不会创建 attempt 或
伪装成普通 item failure；先调整预算，再重跑即可。普通 map/scan item
内的 `ctx.checkpoint("review", payload)` 自动命名为 `review@<item_id>`，批准仍用
`dag.approve(run_id, "review@<item_id>", data)`；其余 item 不会被挂起项拖停。用
`force=["process"]` 重算全体，`force=["process@id"]` 只重算该项。聚合 sidecar 引用
每个 item 缓存键，`dag.gc(keep_last=...)` 会保留这些条目。

当下游只需要索引、计数或统计值时，可用 `aggregate_fn` 收窄聚合体，避免数百个大 item
artifact 再被完整复制一次：

```python
@dag.map(
    "process",
    items_from=("scan", "items"),
    key_fn=lambda item: item["id"],
    aggregate_fn=lambda items, order: {
        "order": order,
        "completed": len(order),
    },
)
def process(item, inputs, ctx):
    return {"large_result": make_large_result(item)}
```

`aggregate_fn(items, order)` 必须是只消费这两个参数的纯函数，并返回 `dict`。它不会进入
item 缓存键；每次 run（包括 item 全命中）都从 item 缓存重建聚合。改动聚合函数会改变
上游 artifact 的内容哈希，从而让下游自然失效，但不会重跑 item。默认 `None` 保持原有
`{"items", "order", "count"}` 形状和缓存行为不变。

本次实际执行过 map 时，`RunResult.map_items` 提供稳定的逐项缓存结果：

```python
result = dag.run()
assert result.map_items["process"] == {"chunk-a": "hit", "chunk-b": "miss"}
```

键是 map 节点名，值是 `{item_id: "hit" | "miss"}`；不需要读取 `.meta.json` sidecar。

**想保留逐项预告,清单节点要瘦。**`plan()` 只有在清单来源节点命中时才能展开
`process@item` 逐项状态;若来源节点把全部正文塞进自己的 artifact,任何一份正文
变更都会让来源 miss、全部 item 变成 unknown。推荐形态是"目录清单 + `files_fn`":
来源节点只产出稳定的 `{"id", "source"}` 清单,正文由 `files_fn` 以内容哈希进入
各自的 item 缓存键——改 3 份正文,预告恰好 3 个 item miss,其余照常展开为 hit。
同理,item 里不要携带节点不消费的字段(如评估真值):缓存键会与之耦合,无谓失效。

另外,item 有 miss 时 map 节点本身也会出现在 `PlanResult.certain` 里:聚合体每次
由 item 缓存重建,这一步是确定发生的工作,但它零 LLM 调用、成本可忽略。读 certain
估算成本时,按展开的 `name@item` 条目数;节点级条目只代表聚合重建。

### 线性运行时清单：`scan`

三种扇出按依赖形状选择：`map` 用于互不依赖的运行时项；`scan` 用于第 N 项消费第 N-1 项
产物的线性 carry；`foreach` 用于注册期已知清单和任意显式项间 deps。`scan` 仍是一个静态
外图节点，但逐项严格串行；图中的无关旁支仍可按 `workers` 并行。

```python
@dag.scan(
    "draft_scenes",
    items_from=("workbench", "scenes"),
    key_fn=lambda scene: scene["id"],
    carry_from=("review_ledger", "ledger"),
    carry_fn=lambda artifact: artifact["delta"],
    prompt_specs=(DRAFT_SCENE,),
)
def draft_scene(scene, carry, inputs, ctx):
    return {"delta": ctx.call(ctx.resolve_prompt("draft_scene"))}
```

`carry_from` 缺省时首项 carry 为 `None`，`carry_fn` 缺省时下一项接收完整项 artifact。
每项键复用 map 的全部成分，另加入实际收到 carry 的内容哈希；因此改第 K 项输入时 1..K-1
命中、K..末尾顺序重算。命中项从缓存 artifact 经 `carry_fn` 重建 carry；改 `carry_fn`
源码本身不会失效，只有提取出的内容变了才会从该处向后传导。初始 carry 内容变动则从首项
开始失效。中途失败前缀已落盘，重跑从失败项继续；`force=["draft_scenes"]` 强算全链，
`force=["draft_scenes@scene-2"]` 强算一项，若其 carry 改变会自然传到后缀。

scan 聚合与 map 一样每次由项缓存重建、不单独缓存；`RunResult.map_items` 也用 scan 名称
记录逐项 hit/miss，聚合 sidecar 列出全部项键，所以 `dag.gc()` 不会误删链上缓存。

**carry 要收窄到下一项真正消费的内容。**prompt 里怎么裁剪材料与缓存怎么失效是两层独立的
决定：项键含的是 carry 的完整内容哈希。缺省 carry 是前一项的整个 artifact——若里面带着
正文摘要这类逐项易变字段，任何一项的无关改动都会击穿全部后缀。用 `carry_fn` 只提取滚动
状态本身（如上例的 `ledger` 增量），上游改动没有改变该状态时后缀原样命中（早停）；状态
真变化时才向后传导。残余粒度边界要心里有数：carry 是单个整体哈希，其中无关部分的变化也
会失效不消费它的后缀项——把 prompt 侧裁剪配合 L1 调用缓存，可以让这类后缀项重算而不重新
付费。同理，artifact 只保留下游真正消费的字段：多带一个易变字段，聚合体就会无谓变化，
拖累下游节点的命中（与 map 一节的 item 卫生是同一条纪律的两个方向）。

### 请求前成本预览：`plan`

`dag.plan(targets=(...))` 与 `run()` 复用同一套 L3 缓存键计算，但不执行任何节点函数、
不创建 run 目录，也不会发 LLM 请求。`PlanResult.nodes` 的值为 `"hit"`、`"miss"` 或
`"unknown"`：上游 miss 让下游 artifact 哈希无法诚实计算时即为 `unknown`，不会猜测。
map 会同时在 `nodes` 中以 `process@chunk-a` 这类扁平键展开 item 状态；若 source artifact
本身未知，整个 map 记为 `unknown` 而不展开。`PlanResult.misses` 是所有 `miss` 与 `unknown`
键的有序列表。检查点不影响缓存键，仍按相同 hit/miss/unknown 规则报告。

`PlanResult.certain` 是确定重算的 `miss`，即成本下界；`PlanResult.at_risk` 是依赖上游
内容的 `unknown`，两者并集是成本上界。`pending_on` 给每个 unknown 直接等待的上游节点或
展开项，便于追原因链。unknown 实跑常会落成 hit：上游虽重算但产物未变时，下游键仍能命中
（early cutoff），所以这些字段不改变原有判定或 `misses` 语义。

scan 在 source artifact 与初始 carry 已知时会逐项预告：命中前缀从缓存重建 carry；首个
miss 后的后缀标为 unknown，且 `pending_on` 指向前一展开项。

refresh/off 的普通节点与 map item 总是 miss；scan 首个可计算项为 miss，后缀因 carry
不可用保持 unknown。即使策略禁止读写，run 仍计算确定性的 key components/cache_key 供
sidecar、trace 和 explain 取证。

```python
plan = dag.plan(targets=("deliver",))
assert len(plan.misses) <= 3  # 真实请求前的成本闸门
assert len(plan.certain) <= len(plan.misses)
result = dag.run(targets=("deliver",))
```

### 图审阅:describe 与渲染

设计完或接手别人的流水线,先渲染整张图再读代码。三个方法都是只读的,注册完
即可调用,不要求跑过:

```python
description = dag.describe()  # 含 cache、外部指纹存在性、subgraph 边界
print(dag.render_summary())  # Markdown 声明表,一节点一行
print(dag.render_mermaid())  # Mermaid 图源,GitHub/编辑器直接渲染
print(dag.render_mermaid(run_id))  # 叠加某次 run:hit/miss/挂起/跳过着色
```

声明表是缺声明洞的第一道人肉检查:某节点明明读文件,`files` 一栏却是空的——
这类洞渲染出来一眼可见,读代码容易漏。`validated_models` 与 `checkpoints`
来自注册期 AST 尽力检测(`ctx.call_validated` 的模型、`ctx.checkpoint` 的字面量名),
解析不出就如实缺席,是审阅辅助不是运行时契约。运行态叠加只读该 run 的 sidecar,
不会重算任何键;map/scan 节点上注记 "N hit / M miss"。顶层 `subgraphs` 显示每个
namespace 的显式输入、输出和有序 qualified 节点名；`models` 仍保留且不会被视为节点。

0.7 的统一画像入口同时覆盖 Prompt 声明与持久化实际选择：

```python
static = dag.profile()
runtime = dag.profile(run_id)
redacted_runtime = dag.profile(run_id, include_content=True)
```

```bash
kigumi profile --format md
kigumi profile --run-id run-0042 --format json
kigumi graph --run-id run-0042 --prompts
```

静态画像不执行节点；运行画像不重跑 provider，只读取 schema-2 manifest、sidecar/origin、
attempt、candidate 与 failure receipt。warm hit 分别显示本 run 的 current selection 和 cache
artifact 的 immutable origin selection。默认不展开 CALL/Agent 内容；
`--include-content` 也只能展示该 run 按 EvidencePolicy 已保留并 scrub 的 evidence。
0.8 digest 损坏 fail closed；缺少 schema-2 manifest 的旧 run 直接拒绝读取。

## 二、核心心智模型

**一切皆内容寻址。**同输入(Prompt selection/material、模型、参数、seed、上游产物、源码)
必然同输出;任何一项变了,缓存自动失效重算。缓存永远不会"差不多命中":
要么声明的输入逐字节没变(必命中),要么必重算——所有排障都是在问
"键的哪个成分变了"或"哪个输入漏声明了"(见排障一节)。由此推论:

- 改了节点函数源码或 source_dirs 里任何 .py 的代码 → 相关节点全部重算
  (故意的,代码就是配方的一部分)。注释与 docstring 不算代码:source 与
  libs 两个成分都按剥离后的 AST 哈希,修文档注不换族。
- 改了 Prompt 模板、PromptSpec base、固定/已选 layer、selector/binding 或 material →
  节点重算；只改未选中 variant 内容 → selected-only L3 key 不变，但旧 run 不可 resume。
- 想强制重算单个节点用 `dag.run(force=["node_name"])`;名字打错会直接报错,
  不会静默全量命中。
- 缓存粒度固定为 L1 调用、L3 普通节点和 L3 map/scan item，不缓存任意切片。
  L1 缓存(`caller` 级)与 L3 缓存(节点/item 级)是两层:节点命中时根本不会
  执行函数体;节点未命中但函数内的 `ctx.llm` 命中时,只省模型调用。

**材料注入只有一个入口。**任何进 prompt 的动态内容都在 `PromptSpec` 的
`PromptMaterial` 中声明并由框架注入；节点只取 `ctx.resolve_prompt()` 的结果。手写
f-string 拼 prompt 是本库要消灭的第一号事故来源。

**结构化输出走 `ctx.call_validated`。**pydantic 模型进,校验实例出;围栏剥离、
extra 字段剥壳、有界修复(默认 2 轮)、stuck 检测全部内置。不要自己写
`json.loads` + while 重试。节点内的 `ctx.call(...)` 与 `ctx.llm(...)` 等价，
`ctx.repair(messages, validate, ...)` 直接进入修复环，不需要为 `.call` 写适配层。

### 设计边界(明说的选择,不是没做完)

kigumi 只服务一类任务:**可缓存、DAG 形状、批处理式的 LLM 流水线**。类内它是
任务无关的;类外有四条刻意边界,撞上时请换工具,不要指望后续版本"补齐":

1. **跑批,不做在线服务。**run 有始有终,挂起等审批是正常返回而非异常;
   没有常驻进程、没有请求路由、没有流式输出。需要在线服务时,kigumi 的
   产出(已验证的 prompt、缓存的产物)可以喂给服务层,但服务层不是它。
2. **模型决定内容,不决定图。**普通 Dag 与 Subgraph 拓扑都在注册/挂载期声明,
   运行时不许模型改变走哪个
   节点。可预告(plan)、可缓存、可审计、失效可推理,全部建立在这个前提上;
   放弃它就是换一个物种(agent 框架),不是加一个 feature。运行时的数据驱动
   仅限两种受控形态:map(项独立)、scan(线性 carry)；foreach 是注册期固化清单。
3. **大二进制走引用,不进 artifact。**artifact dict 全量参与内容寻址,大字节
   放进去会把哈希与序列化成本放大到不可用。输入侧用 `files=`/`files_fn`/
   `kigumi_file`(内容哈希进键),输出侧用 `emit_file`/blob 引用;见下节。
4. **工作流只用 Python 声明。**没有 YAML 或其他 declarative workflow loader；节点函数、
   类型、静态 Subgraph 与 map/scan 边界都在 Python 注册。需要非 Python 配置时，把它作为
   已声明输入读取，不要期待配置文件生成可执行拓扑。

### 二进制交付物

**不要在节点函数体里自己写二进制文件。**那是缓存看不见的副作用：节点缓存命中
时函数不会执行，先前写出的文件若被清理，run 仍会显示成功，交付物却已消失。
二进制统一放入 `artifacts/_cache/blobs` 内容寻址仓；artifact 只保存纯 JSON 引用，
因此缓存命中也会校验仓内容并重新物化文件。

内存中已有字节时用 `emit_file`：

```python
@dag.node("render")
def render(inputs, ctx):
    pdf = render_pdf(inputs["script"])
    return {"deliverable": ctx.emit_file("output/script.pdf", pdf)}
```

外部工具直接写到临时目录时用 `ingest_file`；它会复制入仓，不会移动仍属于调用方的
源文件：

```python
@dag.node("transcode")
def transcode(inputs, ctx):
    temporary_mp4 = run_ffmpeg_to_tempfile()
    return {"video": ctx.ingest_file(temporary_mp4, "output/preview.mp4")}
```

引用形态固定为 `{"kigumi_blob": "<sha256>", "path": "<relative_path>", "bytes": N}`。
`path` 必须是项目内相对路径，不能是绝对路径或含 `..`。引用可嵌在 artifact 的任意
dict/list 中；其 digest 是 artifact 内容的一部分，所以下游会在二进制内容变化时自然
失效，而内容不变时继续命中。仓文件缺失或哈希不符会明确失败，绝不静默交付错误字节。

文本文件仍可沿用 `{"files": {"相对路径": "文本"}}`。调用 `dag.gc(keep_last=N)`
会同时回收未被保留 runs 引用的节点缓存和 blob，返回两者删除数量之和；保留 run 引用的
blob 不会被删除。

同一次 `Dag.run()` 内，所有顶层 `files` 路径与嵌套 `kigumi_blob.path` 都先整组原子认领，
再开始写盘。两个节点、两个 sibling item 或同一 artifact 的文本/blob 指向同一路径会抛
`OutputOwnershipError`，失败方不会覆盖赢家；map/scan 聚合仅可重新物化自己 item 已拥有的
路径。认领解析项目内符号链接，并按目标文件系统的大小写/Unicode 等价规则识别同一目标；
解析到项目根外的输出会在写盘前拒绝。run sidecar 的 `outputs` 记录稳定排序的项目相对路径
（无输出时也是 `[]`）。这里的
“所有权”只表示框架管理的物化路径，不表示法律上的数据所有权、ACL 或通用 effects 系统。

`ctx.ingest_file` 的 source 始终属于调用方：kigumi 只复制到 blob store，不移动/删除源。
GC 只管理 kigumi 的 run、节点缓存和 blob 数据，不删除外部 source，也不删除项目物化结果。

### foreach 前项依赖

`foreach` 的依赖在注册期已经确定。前项/首项 fallback 这类场景，把**依赖节点名**也在
`params_fn` 中固定，函数体以该参数精确读取，不扫描 `inputs` 的前缀：

```python
@dag.foreach(
    "workbench_{episode}",
    episodes,
    deps=lambda item: (
        ("review_ledger",) if item["episode"] == 1 else (f"update_ledger_{item['episode'] - 1}",)
    ),
    params_fn=lambda item: {
        **item,
        "ledger_dep": "review_ledger"
        if item["episode"] == 1
        else f"update_ledger_{item['episode'] - 1}",
    },
)
def workbench(inputs, ctx):
    ledger = inputs[ctx.params["ledger_dep"]]
    return {"ledger": ledger}
```

这保持依赖图、参数和函数访问同一份注册期事实；不要在函数体用
`next(... if name.startswith(...))` 猜测某个输入。

### 外部工具节点

外部命令保持应用层小封装：用 `tempfile` 仅作传输介质，所有业务数据来自显式 `inputs`，
并在 `files=` 声明会影响工具行为的脚本或配置。产物以 `emit_file`/`ingest_file` 收编，避免
缓存命中时丢失副作用：

```python
@dag.node("gate", deps=("draft",), files=("tools/phase_gate.py",))
def gate(inputs, ctx):
    with tempfile.TemporaryDirectory() as directory:
        request = Path(directory) / "input.json"
        request.write_text(json.dumps(inputs["draft"]), encoding="utf-8")
        completed = subprocess.run([sys.executable, "tools/phase_gate.py", str(request)])
    if completed.returncode:
        raise RuntimeError("gate failed")
    return {"passed": True}
```

框架刻意不提供隐式的 tool-runner；工具的命令约定、错误翻译和输入协议属于应用边界。

### 自适应容量

生产环境可把一个容量文件交给 `FileSlots` 和 `AdaptiveCapacity` 共同使用：

```python
import os
from pathlib import Path

from kigumi import AdaptiveCapacity, FileSlots, LLMCaller
from kigumi.transport import LiteLLMTransport

capacity_path = Path(os.environ["KIGUMI_REQUEST_CAPACITY_FILE"])
capacity = AdaptiveCapacity(capacity_path, max_slots=32, min_slots=1, ramp_successes=8)
transport = LiteLLMTransport(..., capacity=capacity)
slots = FileSlots(os.environ["KIGUMI_REQUEST_LOCK_DIR"], slots=32, capacity_file=capacity_path)
caller = LLMCaller(transport, cache_dir=..., slots=slots)
```

429/5xx 每次将共享容量折半（不低于 `min_slots`）；连续 `ramp_successes` 次成功才加一。
容量文件始终是纯整数，因而现有跨进程 `FileSlots` 会立刻读取新容量。
也可用 `FileSlots.from_env()` 读取默认的三项 `KIGUMI_REQUEST_*`；完整名称、启用条件和
自定义 prefix 规则见[环境变量总表](#环境变量总表)。

## 三、测试与运行纪律

### 零真实请求的测试

```python
from kigumi import ScriptedTransport
from kigumi.testing import CassetteTransport, FakeTransport
```

- `kigumi.testing` 是 Fake/Cassette 的规范导入路径；`ScriptedTransport` 同时从顶层
  `kigumi` 导出，便于示例和测试共享。
- **FakeTransport**:单测里给定响应序列，逐个返回；用 `requests` 断言收到的请求，
  耗尽会明确报错。
- **CassetteTransport**:录一次真实响应到磁带文件,之后离线重放;
  磁带带请求指纹,调用顺序或内容变了会直接报错,不会静默配错答案。
- **ScriptedTransport**:按请求文本中**完整一行**的 marker 分派离线响应；routes 的定义
  顺序就是多命中时的优先级，responder 在锁内执行，可安全维护修复轮次等计数器。整行锚定
  是必要的：模板正文里的小节标题或示例子串不能意外撞到阶段路由。

  ```python
  transport = ScriptedTransport(
      {"STAGE: analyze": '{"summary": "ok"}'},
      aliases={"default": "fixture-default"},
  )
  ```

  每个阶段 marker 应独占一行；无匹配会列出已注册 marker 与请求开头，别名漏配则保留
  `KeyError`，不会悄悄回退到网络模型。
- **dry-run**:`LLMCaller(dry=True)` 下任何会打真实请求的路径直接抛
  `DryRunError`——排练整条流水线的缓存命中情况而不花一分钱。

本包通过 `pyproject.toml` 的 `pytest11` entry point 自动加载插件，但只有从当前目录向上发现
带 `[tool.kigumi]` 的 `pyproject.toml` 时才激活。激活后，它会追加调用方没有手写的测试项：

- 每个 `prompts/**/*.md` 追加一个 `kigumi_dry_render[<file>]`（`<file>` 是
  `prompt_path.name`，例如 `kigumi_dry_render[extract.md]`）。该项读取模板，以
  `<槽位名>` 填满 `slot_names()` 发现的全部槽位，经严格 `render_template()` 渲染，并断言
  不残留 `{{` 语法；全程不调用模型。
- 每次 collection 追加一个 `kigumi_guard`。它扫描配置的 `source_dirs`，对未豁免的循环裸
  LLM 调用或节点原始文件读取失败；有理由的 `raw-llm-ok` / `raw-io-ok` 以 pytest warning
  保持可见。

插件同时注册 `live` marker，并提供 `kigumi_cassette` fixture。fixture 是一个
`Callable[[str], CassetteTransport]`：`kigumi_cassette("review")` 解析到项目根下
`tests/cassettes/review.json`，传入已带 `.json` 的名称则不再追加扩展名。

源码没有 Kigumi 专属的 collection opt-out 或按目录缩窄开关；只要项目保留
`[tool.kigumi]`，上述生成项都会收集。标准 pytest 的 `-k` 等选择器可以只执行匹配项，但
不会阻止它们被创建；移除 `[tool.kigumi]` 会让整个插件变成 no-op，也同时关闭项目集成。

### 真实请求测试(live tests)

真实请求测试一律显式标记为 `@pytest.mark.live`，并用 transport 无关的
`skip_unless_env(...)` 声明所需凭证；任一环境变量缺失就跳过，普通环境零成本：

```python
import pytest
from kigumi.testing import skip_unless_env


@pytest.mark.live
@skip_unless_env("MODEL_API_KEY", "MODEL_BASE_URL")
def test_live_delivery(): ...
```

标准分三档：

1. **全链路**：使用独立 artifacts 目录、真实 transport 和 `dag.run()`，断言最终交付物。
2. **受影响链路**：复用持久 artifacts 目录，先 `dag.plan()` 预览 `misses` 并可断言其数量
   上限，再 `dag.run()`；L3 缓存会天然只重跑受影响链路。
3. **单节点**：`dag.run(targets=("node",), force=["node"])`，上游仍走缓存。

插件只在配置了 `[tool.kigumi]` 的项目中激活。跑真实请求还必须显式设
`KIGUMI_LIVE=1`，并备齐 `skip_unless_env` 声明的凭证；两道门叠加。CI 的 live job
也必须设置 `KIGUMI_LIVE=1`。在 `pyproject.toml` 注册 `live` marker；普通 `pytest -q`
不提供该环境变量或凭证时会跳过，不产生真实请求。

### 守卫四环

裸循环里打 LLM(`for ... ctx.llm(...)`)会在节点注册时被 AST 检查拒绝——
循环调用必须走 `foreach` fan-out 或 `repair_loop`,让每次调用有独立缓存键
与产物。确实需要豁免时在行尾写 `# kigumi: raw-llm-ok <理由>`,理由必填,
git hook 会把新增豁免摆到评审面前。

节点体内的直接文件读取(`open()`、`path.open()`、`.read_text()`、`.read_bytes()`)
同样在注册期被拒绝,必须改走 `ctx.read_text`/`ctx.read_bytes` 使读取过声明校验;
豁免写 `# kigumi: raw-io-ok <理由>`,理由必填。两类豁免互不吞并。项目级 raw-I/O
外环只扫装饰节点函数体的最外层,不递归 helper 与 lambda——helper 合法读文件的场景很多,
扫了全是误报;代价是经 helper 的未声明读取只能靠 `ctx.read_text` 的运行期校验和 review
兜底。raw-LLM 的 `check_source`/`check_paths` 则会递归扫描源码里的 helper 循环。

#### 直接调用 `kigumi.enforce` 时选哪个 helper

`kigumi.enforce` 是不导入项目模块、也不执行节点的纯 AST 检查层；它返回 findings，
本身没有退出码。按输入边界选择入口：

| 需要检查 | 使用 | 检查范围 |
| --- | --- | --- |
| 一段源码中的循环裸 LLM 调用 | `check_source(text, path)` | `for`/`while`/`async for` 与四种推导式下的 `.call()`/`.llm()` |
| 配置的整个源码目录中的循环裸 LLM 调用 | `check_paths(source_dirs)` | 递归 `*.py`；不存在的目录跳过 |
| 一个模块中带装饰器的节点直接读文件 | `check_raw_io_node_source(text, path)` | 顶层 `node`/`map`/`scan`/`foreach`/`agent` 函数的最外层函数体 |
| 配置的整个源码目录中的节点直接读文件 | `check_raw_io_node_paths(source_dirs)` | 上一行的项目级版本 |
| 注册环已经拿到的单个节点函数体 | `check_raw_io_source(text, path, context_name="ctx")` | 只查这个节点体；`context_name` 是实际上下文参数名 |

`check_paths()` 只做 raw-LLM 扫描，不能替代 raw-I/O 检查；反过来也一样。所有结果的
`path`、`lineno`、`snippet`、`waived` 和 `waiver_reason` 都可用于自定义报告；
`waiver_reasons()` 与 `raw_io_waiver_reasons()` 只是分别读取两类豁免理由（包括重复项），
不负责判断违规。空理由的注释仍是违规，`raw-llm-ok` 也不能替代 `raw-io-ok`。

例如，直接嵌入测试或自定义检查时可以只消费结构化结果：

```python
from pathlib import Path

from kigumi.enforce import check_raw_io_source, check_source

llm = check_source("for item in items:\n    client.call(item)\n", Path("nodes/batch.py"))
io = check_raw_io_source(
    "def node(inputs, ctx):\n    return open('input.txt').read()\n",
    Path("nodes/node.py"),
)
print([(finding.lineno, finding.waived) for finding in llm])
print([(finding.lineno, finding.waived) for finding in io])
```

输出：

```text
[(2, False)]
[(2, False)]
```

项目的 CI 和提交环应使用 wrapper，而不是自行把 findings 猜成状态：

```bash
# CI：扫描配置的全部 source_dirs
uv run kigumi guard

# 新项目一次性安装由 Kigumi 生成的 pre-commit
uv run kigumi init --hooks
# 完成首个 Git commit 后，pre-commit 会执行这条 changed 检查
uv run kigumi guard --changed
```

`kigumi guard` 无未豁免 finding 时退出 `0`，有违规时退出 `1`；未配置项目、
`--changed` 不在有 `HEAD` 的 Git 仓库中等使用错误退出 `2`。`--changed` 还会把相对 `HEAD` 新增的
豁免理由打印为 `new waiver`，但新增理由本身不是失败条件；有理由的豁免始终打印清单。
pytest 插件激活后会追加 `kigumi_guard`：未豁免 finding 使该测试失败，有理由的豁免发
`PytestWarning`；它不会做 `HEAD` 比较，所以提交审查仍用 `guard --changed`。

不要把项目级 raw-I/O 扫描当成运行时沙箱：它有意不递归 helper，注册环才是已注册节点的
精确边界；helper 内仍应通过 `ctx.read_text`/`ctx.read_bytes` 和 `files=` 声明输入。
也不要在只需要统一 `workers` 上限时额外手写守卫调用，或把两类豁免混用。

### 人工检查点

`ctx.checkpoint(name, payload)` 抛出 `CheckpointPending` 中断本轮;
下游节点进入 `result.skipped`(可见,不静默)。人审后:

```bash
uv run kigumi approve run-0001 outline_ok --data '{"reviewer":"human"}'
# 或代码里 dag.approve(run_id, name, data)
```

检查点名先按节点作用域限定，再追加动态 item ID。设调用名为 `approval`，四种精确形态是：

- 普通节点：`approval`
- 普通 map/scan item：`approval@item`
- 挂载普通节点：`approval@namespace.local`
- 挂载 map/scan item：`approval@namespace.local@item`

因此同一 `Subgraph` 重复挂载时，各 qualified 节点独立挂起、独立批准。最终检查点名必须是
安全的单个非空路径成分；内部可以含 `.` 与 `@`，但不得含 `/`、`\\`，也不得等于 `.` 或
`..`。

审批文件绑定在产生挂起的 run 目录。成功 `approve` 会删除当前匹配的 `.pending.json`。
批准后必须用同一个 `run_id` 重跑，才能读到审批：

```python
dag.run(run_id="run-0001")
```

直接调用 `dag.run()` 会新开 run，旧 run 的审批不会生效；本次实际调用过检查点的普通节点或
map/scan item 即使成功也不写 L3 artifact，所以审批结果不会经节点/item 缓存泄漏到新 run
或其他挂载，新 run 会重新挂起。条件分支本次没有调用 `ctx.checkpoint()` 时，仍按节点声明的
`auto`/`refresh`/`off` 策略读写 L3。

审批绑定 payload 内容哈希——上游内容变了，旧批自动作废并写出新的 pending 记录；再次批准
会替换批准内容并删除这次 pending marker。

### 推荐形态：生成与批准拆成两个节点

需要人审的产物,不要把生成和 `ctx.checkpoint` 写进同一个节点。调用过检查点的执行不写
L3,同体意味着昂贵的生成也放弃节点缓存;更重要的是"补批准不得改变已生成的结果"
只能靠约定维持。拆成两个节点后,这条不变式是结构性的:

```python
@dag.node("repair_proposal", deps=("draft", "review"), prompt_specs=(REPAIR,))
def repair_proposal(inputs, ctx):
    """纯生成:正常进 L3 缓存,批准与否不影响本节点的键与产物。"""
    return ctx.call_validated(ctx.resolve_prompt("repair"), RepairProposal).model_dump()


@dag.node("repair_gate", deps=("repair_proposal",))
def repair_gate(inputs, ctx):
    """只做人工闸:不发调用,批准绑定提案内容本身。"""
    approved = ctx.checkpoint("repair_ok", inputs["repair_proposal"])
    return {"approval": approved, "proposal": inputs["repair_proposal"]}
```

- 生成节点保持纯函数,享受缓存与 `plan` 预告;闸节点不发请求,放弃缓存的代价可忽略。
- 批准 payload 就是提案内容:上游重新生成,旧批自动作废、重新挂起(既有绑定语义)。
- 下游一律依赖闸节点,不越过闸直接依赖生成节点;闸挂起时下游进 `skipped`,可见不静默。

## 四、评估与提示词进化(evals / optimize)

### 指标怎么写

双轴,不混:

```python
from kigumi import Judgment, gated_metric, llm_judge, pairwise_judge


def format_gate(example, output) -> Judgment:  # 合规轴:普通 Python 函数
    ok = validate_output_format(output)
    return Judgment(1.0 if ok else 0.0, feedback=..., tags=("format",))


quality = pairwise_judge(caller, rubric=..., reference_key="reference")
metric = gated_metric(format_gate, quality)  # 闸没过不烧评委调用
```

- 合规(格式、长度、必含要素)是硬闸,写成确定性函数,满分才放行品质评委。
- 品质用 LLM 评委:`llm_judge`(按 rubric 独立打分)或 `pairwise_judge`
  (参考文本作**及格线**——评委只裁决水准是否达到/超过参考,不比相似)。
- 两类评委用 `max_repairs=` 设结构化输出的最多修复次数，名称与底层修复环一致。
- 库刻意不提供 EM/F1/文本相似度指标:相似度会把创作类任务的优化目标退化成
  "复写测评集"。不要自己往品质轴里加相似度。
- `Judgment.tags` 认真填:它是反思材料按错误类型归并的分组键,空 tags 会
  让同类失败无法压缩。

### 内置中文提示词与覆盖

三份内置措辞当前都是中文，这是公开默认值，不随调用语言自动切换：

- `kigumi.evals.JUDGE_WORDING_DEFAULT` 供 `llm_judge(..., wording=None)` 使用；传
  `wording="..."` 完整替换。
- `kigumi.evals.PAIRWISE_WORDING_DEFAULT` 供 `pairwise_judge(..., wording=None)` 使用；
  同样传 `wording="..."` 完整替换。
- `kigumi.optimize.REFLECTION_TEMPLATE_DEFAULT` 供
  `evolve_prompt(..., reflection_template=None)` 使用；传 `reflection_template="..."`
  完整替换。

```python
quality = llm_judge(caller, rubric=rubric, wording=english_judge_instructions)
pairwise = pairwise_judge(
    caller,
    rubric=rubric,
    reference_key="reference",
    wording=english_pairwise_instructions,
)
result = evolve_prompt(
    seed_text,
    train,
    val,
    task,
    metric,
    caller,
    reflection_template=english_reflection_template,
)
```

自定义 reflection template 的槽位集合必须**严格等于**
`current_template`、`merged_feedback`、`max_chars`：少一个或多一个都会在反思调用前报错。
pairwise verdict 的固定分数映射是
`PAIRWISE_SCORES = {"better": 1.0, "comparable": 0.8, "worse": 0.3}`；该映射不由
`wording` 改写。

### 进化怎么跑

```python
from kigumi import evolve_prompt

result = evolve_prompt(
    template=seed_text,
    train_examples=train,  # 反思材料只来自这里
    val_examples=val,  # 去留、前沿、胜出只看这里
    task=lambda text, ex: run_pipeline(text, ex),
    metric=metric,
    caller=caller,
    state_path=Path("artifacts/evolve_state.json"),  # 断点续跑
)
print(result.generalization_gap)  # train 均分 - val 均分,过拟合观测值
```

必须知道的机制(这些是代码闸门,不是建议):

1. **train/val 由你切**,库不猜。两侧都不许为空。经验起点:小测评集
   对半切;样例总数 < 6 时先扩测评集再谈优化。
2. **val 的内容与评语永远不会进反思 prompt**;候选 val 均分低于父本直接
   拒收;候选新增文本含 ≥ `leak_run_chars`(默认 12)字的样例原文直接拒收。
   这三道闸不可配置关闭。`leak_run_chars` 是可调的防泄漏闸，窗口按字符计：
   中文 12 字符就是 12 个汉字；英文约两个词，常见套话可能误伤合法候选，可按
   语料调大。误判方向是保守拒绝，不会污染结果。
3. **同分更短者胜**:简洁性是 Pareto 前沿的一个维度。种子模板本身也可能
   被更短的同分候选剪掉。
4. `max_chars` 默认是种子长度的 2 倍——想要更紧的指令就调小它。
5. **胜出文本不落盘**:`result.best` 由你人工审阅后决定是否写进
   prompts/ 并提交 git。优化器改提示词是生产变更,必须过人手。
6. 反思模板与评委措辞都是参数(`reflection_template` / `wording`),
   默认常量只是合理起点;自定义反思模板槽位必须恰好是
   `current_template` / `merged_feedback` / `max_chars`。
7. 成本账:每轮 ≈ minibatch 次父本评估 + 1 次反思调用 + (过闸后)
   train+val 全量评估。用 `max_metric_calls` 设硬顶;`(候选, 样例)` 对
   全程只评一次,含续跑。使用 `state_path` 时，`max_metric_calls` 是跨续跑的**累计总上限**，
   恢复时须传入大于已消耗次数的总额；state 的 `round` 只表示已完成轮数，未完成轮会从反思阶段
   重进。

### 统一实验主体：workflow 与 Agent 使用同一证据网格

`bench` 在 0.5.0 硬切为 `ExperimentSubject`。每个 trial 都有独立 project/evidence root，
报告只保留证据，不选择 winner，也不修改 Skill/Prompt：

```python
from pathlib import Path

from kigumi import FunctionSubject, Judgment, TrialObservation, Variant, bench

subject = FunctionSubject(
    lambda example, trial: TrialObservation(
        output=write_outline(example),
        usage=None,
        evidence={"trial_id": trial.trial_id},
        seed_applied=False,
    ),
    identity={"pipeline": "outline-v2"},
    seed_mode="unsupported",
)

report = bench(
    [Variant("current", "当前切法是对照", subject, incumbent=True)],
    [{"outline": "..."}],
    metric=lambda example, output: Judgment(score_outline(example, output), "人工评语"),
    experiment_dir=Path("experiments/outline-2026-07-23"),
    report_path=Path("experiments/outline-2026-07-23/report.json"),
)
```

报告 schema 3 把两个问题分开读：`mean`、`stdev`、`pass_rate` 与 `by_example` 是质量轴，
`outcome_summary` 是计划网格上的运行结果/评估覆盖轴。不要把 score 为 `0.0` 的合法输出
当成 subject 不可用，也不要从质量均分反推 metric 是否实际跑过：

```python
variant_report = report["variants"][0]
quality = {
    "mean": variant_report["mean"],
    "stdev": variant_report["stdev"],
    "pass_rate": variant_report.get("pass_rate"),
    "by_example": variant_report["by_example"],
}
operational = variant_report["outcome_summary"]
assert operational["trial_count"] == len(report["examples"]) * len(report["seeds"])
```

`outcome_summary` 的所有 rate 都以 `trial_count`（包含失败格的计划格数）为分母。无 error
同时计 subject/metric success；`error.stage == "subject"` 只计 subject failure，metric 未运行；
`error.stage == "metric"` 计 subject success 与 metric failure。完整错误消息、tags 和 evidence
仍只在 raw trial 中保留，不进入汇总。

`CallerSubject` 自己构造 Caller 并提取 usage；`DagSubject` factory 必须使用传入
`TrialContext.project_root` 和 `evidence_root`。multi-seed Dag 实验的 target 必须显式
`cache="refresh"` 或 `cache="off"`，v1 不接受 `auto`。`AgentSubject` 则自动为每格创建隔离
单 Agent DAG 并固定 target `cache="off"`。

### 外部 Agent 节点

Pi 是 runtime，Kigumi 负责 capsule 内容寻址、staging、cache/artifact、实验和证据边界。先建
固定目录胶囊：

```text
agents/writer/
├── agent.toml
├── SYSTEM.md
├── skills/
└── hooks/
```

`agent.toml` schema v1 的最小完整形态（所有额度都必须显式）：

```toml
schema_version = 1
runtime = "pi"
provider = "anthropic"
model = "claude-sonnet-5"
thinking = "high"
system_prompt = "SYSTEM.md"
skills = ["skills/article"]
hooks = ["hooks/policy.ts"]
tools = ["read", "write", "edit", "grep", "find", "ls"]

[limits]
timeout_seconds = 300
max_turns = 24
max_tool_calls = 100
max_files = 100
max_bytes = 10485760
max_single_file_bytes = 2097152
inline_text_max_bytes = 65536
trajectory_max_events = 200
trajectory_max_bytes = 262144
rpc_max_bytes = 2097152
stderr_max_bytes = 262144
```

Skill 目录与 Hook 文件必须位于 capsule 内并显式引用；未引用普通文件不入键。绝对/越界路径、
symlink、重复资源、credential 和 `bash|shell|terminal` 会在 `AgentSpec.load()` 时报错。需要命令
能力时，由可信 Hook 注册用途窄、名称窄的工具，不开放通用 shell。

```python
from kigumi import (
    AgentFileSelector,
    AgentPublish,
    AgentSpec,
    AgentTask,
    EvidencePolicy,
    PiRpcAdapter,
    RetryPolicy,
)

spec = AgentSpec.load("agents/writer")
adapter = PiRpcAdapter(("pi",), expected_version="0.82.1")


@dag.agent(
    "draft",
    adapter=adapter,
    spec=spec,
    deps=("brief",),
    files=("source/brief.md",),
    evidence_policy=EvidencePolicy(
        request="redacted",
        response="redacted",
        stderr="hash_only",
        trajectory="redacted",
    ),
    retry=RetryPolicy(max_attempts=3),
)
def draft(inputs, ctx):
    return AgentTask(
        "按 SYSTEM 与 Skill 完成文章，并用 submit_result 提交。",
        collect=(AgentFileSelector("draft.md"), AgentFileSelector("notes/*.md")),
        publish=(AgentPublish("draft.md", "generated/draft.md"),),
    )
```

Pi 由用户显式安装。adapter 先运行 `pi --version` 并与 `expected_version` 精确匹配，不自动安装
或升级 Node/Pi。运行参数默认关闭 session，并始终关闭 project context、隐式资源发现和
built-in tools，只加载 staged capsule 与 Kigumi bridge。bridge 的同名文件工具把模型访问限定
到 workspace；可信 Extension 仍有宿主进程权限，因此这些限制与 staging workspace 都
**不是 OS sandbox**。

需要线性多轮修订时使用 `agent_scan`，把 session 当作 carry 值：

```python
session_adapter = PiRpcAdapter(
    ("pi",),
    expected_version="0.82.1",
    session_carry=True,
    session_max_bytes=2 * 1024 * 1024,
)


@dag.agent_scan(
    "revise",
    adapter=session_adapter,
    spec=spec,
    items_from=("drafts", "items"),
    key_fn=lambda item: item["id"],
    carry_fn=lambda artifact: artifact["session"],
)
def revise(item, carry, inputs, ctx):
    return AgentTask(f"修订 {item['id']}，并用 submit_result 提交。")
```

首项 carry 为 `None`，Pi 创建空 session；成功后完整 transcript 进入 blob store，下一项收到
session attachment 描述符，框架在 Agent 边界验证摘要并解出 bytes。同一 item 与同一 carry
命中同一 L3 item cache；顺序或 transcript 改变会自然换键。`carry_from` 只用于来自其他节点的
可选初始 session，后继 session 必须用 `carry_fn` 投影；不能把 scan 自身写进
`carry_from`。session header cwd 固定规范化为 `"."`，防止临时 workspace 路径泄漏或失效。
超出 `session_max_bytes` 会报错，不会静默截断。

默认同一 `agent_lock_dir` 只有一个 Agent miss 可进入 staging/Pi；cache hit 不申请 slot。
排队时间不消耗 `AgentLimits.timeout_seconds`，slot timeout 在 spawn/provider side effect 前
产生 typed capacity failure。需要并发时显式提高 `agent_slots`，不要依赖每个 Python 进程各自
的线程池上限。

`agent_schema=3` 的 canonical artifact 只保留 task/completion、Agent identity、
attachments、published outputs、`files` 与可选 session attachment。usage、duration、workspace manifest、
RPC/stderr/trajectory/Hook evidence、queue/slot 和退出原因在 sidecar 的 immutable origin
provenance。cold 与 warm cache hit 暴露同一个 origin，`AgentSubject` 也从这里取 usage/evidence。

`EvidencePolicy` 的三种模式都先清除 credential、authorization/header secret 与 URL query。
`redacted` 保留事件/工具/model/usage/status 与内容摘要；`hash_only` 不存 raw blob。
它不是加密或 ACL，L1 为重放仍保存 request/response payload；需要机密存储控制时必须由文件
权限、磁盘加密和 artifact 生命周期另行承担。policy digest 改变不会换内容键，但会造成
evidence miss；普通 CALL 可由 L1 重建 evidence，Agent 必须重新执行。

Pi Extension 与 adapter 之间的环境协议是 `KIGUMI_WORKSPACE`、
`KIGUMI_ALLOWED_TOOLS`、`KIGUMI_MAX_TOOL_CALLS`、`KIGUMI_MAX_TURNS`；值均由
`PiRpcAdapter` 从 staged workspace、`AgentSpec.tools` 和 `AgentLimits` 注入，不由 Extension
自行推断。精确值形态见[环境变量总表](#环境变量总表)。

普通下游节点读取 Agent attachment 时先调用 `ctx.agent_result(artifact)`，得到已验证、
只读的 `AgentResultView`；构造时会校验 canonical artifact 与 blob digest，之后再用
`list()` / `select()` / `read_bytes()` / `read_text()` / `publish()`。这避免下游直接相信
artifact 中的 attachment 描述符：

```python
@dag.node("consume_agent", deps=("draft",))
def consume_agent(inputs, ctx):
    result = ctx.agent_result(inputs["draft"])
    return {"draft": result.read_text("draft.md")}
```

单 Agent 可直接进入 bench；每格 target 固定 `cache="off"`，重复 seed 不会被 L3 hit 吞掉：

```python
from kigumi import AgentSubject

subject = AgentSubject(
    adapter,
    spec,
    task=lambda example, ctx: AgentTask(
        f"处理 example：{example['text']}",
        collect=(AgentFileSelector("draft.md"),),
    ),
    files=lambda example: {"input.txt": example["text"]},
    output=lambda artifact: artifact["completion"]["summary"],
)
```

identity 自动包含 adapter/Pi、capsule、task/files/output 源码摘要；closure 或外部状态通过
`external_fingerprint=` 显式声明。Pi 不提供可验证 seed，`seed_mode` 固定为 `unsupported`。

## 五、注意事项(会咬人的边)

- **foreach 的 items 会被立刻固化**:传生成器没问题,但 items 的内容在
  注册时就决定了节点集;运行期不会再看它。另外 `name_template` 里的
  `{i}` 是序号占位；若 item 是 Mapping，其键会覆盖内置索引（`format_values.update(item)`），
  因此 item 自己带 `"i"` 键时优先使用该值——item 键名应避开 `i`。
- **泄漏检查是朴素子串匹配**:训练样例里的常见短语(如格式说明套话)若
  恰好不在种子模板中,包含它的候选会被误拒。误拒率高时调大
  `leak_run_chars`,或把这类套话预先写进种子模板(种子中已有的不算泄漏)。
- **大二进制走 blob 引用**:`ctx.emit_file` / `ctx.ingest_file` 把字节放进
  内容寻址仓，artifact 只留 digest、物化路径与字节数；不要手工保存路径和摘要，
  更不要在节点函数里直接写盘。
- **节点函数必须返回 dict**;文本交付物仍用 `{"files": {"相对路径": "文本"}}`
  由框架原子写入，二进制交付物用上述 blob API。
- **物化路径一次 run 只归一个生产者**:冲突抛 `OutputOwnershipError`；这不是 ACL 或
  法律权利声明，只是防止框架管理输出被并发/串行静默覆盖。
- **calls 溯源由节点上下文观察器归属**,并行调度不会混入其他节点的调用；
  不要自己在节点函数内再开线程打 `ctx.call`，应把并发拆成 DAG 节点并配合
  `FileSlots` 限流。
- **`kigumi_file` 引用**:多模态附件写
  `{"kigumi_file": "path/to.mp4", "format": "video/mp4"}`,缓存键用内容
  哈希而非路径;发送前会重验哈希,文件中途被改会拒发而不是发错的。
- **磁带(cassette)按顺序 + 请求指纹重放**:改了 prompt 措辞、模型名或
  参数,磁带会报 mismatch——这是提醒你重录,不是 bug。
- **`Budget` 在 miss 发 provider 请求前先预留额度**:显式 `max_tokens` 优先作为预估值,
  否则按 prompt 长度保守估算。成功调用把 provider 实际用量 commit 到 `Budget.spent`,
  失败调用释放预留；实际用量可能超过预估值,因此 commit 仍可能抛 `BudgetExceeded`,但该次
  调用已经完成并进入缓存。缓存命中不做预留。预留与同 key single-flight 都只在当前进程
  内协调；多个进程可能同时为同一个 key 预留并请求 provider,需要 file lock 或分布式协调
  时由应用层另行提供。
- **缓存损坏不是普通 miss**：`CacheIntegrityError` 会阻止节点或 L1 调用缓存静默
  重放/重新计费；`CacheLookup` 的 `CORRUPT` 必须先核对现场。`_run.json`、attempt state、
  candidate 或已完成 artifact 摘要不一致时同样 fail closed，不能把可能已经产生外部
  side effect 的损坏状态当成“从头开始”。

## 六、排障:按症状查

### 排查链路(agent 视角)

不需要读 sidecar 或拼 L1 缓存路径。先从一次 run 取节点、map 项和每次调用的
完整证据链；输出中的 `key` 可以直接交给 `call`，`payload_path` 是已解析的绝对路径，
仅用于可见性而非要求手工读取。
全部命令参数、默认值与退出码见 [CLI 参考](cli.md)。

```bash
# 1. 定位:节点、map 项、缓存策略、物化输出、键成分和每次 LLM 调用
kigumi trace run-0042
kigumi trace run-0042 --node outline --json
kigumi runs show run-0042 --json   # run/attempt/due_at/failure/policy 状态

# 2. 查看调用:前者取完整 L1 载荷，后两者直接取输入与输出
kigumi call 4f7a2c
kigumi call 4f7a2c --field messages
kigumi call 4f7a2c --field response

# 3. 比较:先列产物变化，再列每个节点/项变化的键成分
kigumi diff run-0041 run-0042
kigumi diff run-0041 run-0042 --json
```

`trace`、`diff`、`runs list` 与 `runs show` 的 `--json` 都使用稳定的
`canonical_json` 输出，适合 agent 用 `json.loads` 消费；`call --field response`
刻意输出裸文本，其余 `call` 输出 JSON。`kigumi call` 会按 key 前缀唯一匹配，未命中或
歧义会报出 L1 目录或候选键。若 `trace` 提示载荷缺失，修正
`[tool.kigumi].llm_cache_dir`，使其与构造 `LLMCaller` 时的 `cache_dir` 完全一致。

排障的总原则见第二节:所有症状最终归结为"键的哪个成分变了"或"哪个输入漏声明"。

| 症状 | 第一反应 |
| --- | --- |
| 节点意外重算 | `dag.explain("node")` 或 `dag.explain("node@item")` 直接列出变化成分;记住 source_dirs 里任何 `.py` 的代码变更(注释/docstring 除外)都在 `libs` 成分里 |
| 明明改了输入却命中 / 结果陈旧 | 该节点是否绕过 `ctx.read_text` 读了未声明文件;逐个核对 `files`/`files_fn` 声明,`render_summary()` 的声明表适合快速扫 |
| `force` 了还是老答案 | L3 与 L1 是两层:`force` 只重执行函数体,prompt 没变时 `ctx.call` 仍从 L1 重放;想要新答案换 `seed` |
| 两次 run 结果哪里不同 | `dag.diff(run_a, run_b)`,按内容哈希报 changed,不重跑节点 |
| 想在花钱前看改动爆炸半径 | `dag.plan()`;`certain` 是下界、加 `at_risk` 是上界,`pending_on` 追 unknown 的原因链 |
| 图形状/声明想整体过目 | `dag.render_mermaid(run_id)` 叠加运行态,挂起与被跳过节点会着色标出 |
| retry 到期前 resume 没动作 | 这是正确行为；看 `pending_retries[].due_at`，由外部 supervisor 到期再调用 |
| resume 报 ambiguous | 先核对 provider/Pi 日志，再用带 reason 的 `kigumi retry-resolve ... retry|fail` 裁决 |
| 同 run 报 declaration changed | 0.6 manifest 禁止覆盖；用原声明 resume，或为新声明创建新 run_id |

`explain` 的判定语义与 `plan` 完全一致:上游 miss 导致成分无法诚实计算时报
`unknown`(并给 `pending_on`),不猜测原因;对照的 run 没有该节点 sidecar 报
`no_entry`;sidecar 缺少或损坏成分记录直接 fail closed。成分标签
固定为 `source`、`libs`、`upstream:<dep>`、`prompt_specs:<spec>`、`files:<路径>`、
`params`、`item`、`item_files:<路径>`、`carry`、`kigumi`，声明外部指纹时额外且仅额外
出现 `external=sha(external_fingerprint)`。缓存策略不入键；`kigumi` 由 prompt 生成字节、
`CACHE_SCHEMA` 与 Pydantic 版本组成，不直接使用发行版本号。

```python
report = dag.explain("p2_variants@E2S4")
print(report)  # 中文可读报告:状态 + 变化成分 + 新旧摘要
report.changed  # 如 ["item_files:fixtures/scenes/E2S4.txt"]
```
