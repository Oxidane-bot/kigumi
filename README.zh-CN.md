<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/kigumi-logo.png">
    <img src="docs/assets/kigumi-logo-light.png" alt="kigumi logo" width="220">
  </picture>
</p>

# kigumi(木組)

[English](README.md) | 中文

无钉咬合的木工工艺。LLM 内容流水线的承重结构层——项目(屋顶)与模型(立柱)
之间靠精确咬合连接,不合榫就打回重做。

给"用 coding agent 开发 LLM 流水线"提供地基:

- **注入与拼接**:材料注入唯一入口,严格模板渲染,schema 自动生成格式说明段
- **分层 Prompt 声明**:有限 selector axis、固定 fragment 与定界运行材料在缓存 lookup 前
  解析，使用 selected-only L3 cache 并保留不含原文的 lineage
- **修复环**:校验不达标 → 纠正指令打回,保留模型上文,有界重试,学费固化
- **确定性重放**:内容寻址缓存,同输入逐字节同输出
- **DAG 编排**(可选):显式节点/item 缓存策略、静态可复用子图、动态 map/scan、
  物化输出所有权、人工检查点、durable retry/resume 与 run diff
- **外部 Agent 节点与串行 scan**:provider-neutral staging、attachment、exact publish、普通 DAG 缓存，
  内容寻址 `AgentSpec` 胶囊、跨进程全局容量、证据保留策略与原生、精确版本锁定的
  Pi RPC adapter，以及显式、blob-backed 的 session carry
- **Typed failure 与显式恢复**:CALL/Agent 共用 provider failure 事实、确定性 retry schedule、
  durable attempt receipt，以及对 ambiguous side effect 的 fail-closed 裁决
- **工作流画像**:一份 canonical 静态/运行 IR 同时供应 Prompt-aware Mermaid、Markdown、
  JSON、`describe`、trace 与 runs show
- **统一实验主体**:函数、Caller、workflow 与 Agent DAG 使用同一隔离证据网格，不自动选赢家
- **守卫四环**:注册环拒载,外加 kigumi check / pytest 自动收集 / git hook 三个外环,让规矩自动执行

## 快速上手

```python
from pathlib import Path

from pydantic import BaseModel

from kigumi import LiteLLMTransport, LLMCaller, call_validated


class Verdict(BaseModel):
    score: int
    reason: str


transport = LiteLLMTransport(aliases={"default": "anthropic/claude-sonnet-5"})
caller = LLMCaller(transport, cache_dir=Path("artifacts/_llm"), seed=20260713)

verdict = call_validated(caller, "给这段开场白打分并给出理由:……", Verdict)
```

`call_validated` 自动附上由 `Verdict` 生成的格式说明段;返回若不合榫,
带着校验错误打回重试(默认至多 2 次)。整次交互内容寻址落缓存,
同输入重跑逐字节复现,不再计费。

## 状态

0.9.0,API 未冻结。Agent 边界只负责执行兼容与实验取证，不是 Agent factory 或优化器。

内置 judge、pairwise 与 reflection prompt 默认使用中文文本，三者都可覆盖；参数与槽位契约见
[评估与提示词进化](docs/adoption.md#四评估与提示词进化evals--optimize)。

## 分层 Prompt 示例

```python
from kigumi import InputRef, PromptAxis, PromptLayer, PromptRef, PromptSpec

WRITE = PromptSpec(
    name="write",
    base=PromptRef("base/task"),
    layers=(
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
)


@dag.node("write", deps=("config",), prompt_specs=(WRITE,))
def write(inputs, ctx):
    return {"text": ctx.call(ctx.resolve_prompt("write"))}
```

Kigumi 在每次 run 开始时一次 snapshot 全部声明 Prompt 文件。实际选中 variant 进入 L3 key；
未选中候选字节仍进入 run identity，因此修改它可复用当前 selected cache，却不能静默恢复
旧 run。使用 `kigumi profile` 和 `kigumi graph --prompts` 查看完整声明与持久化实际选择。

## 安装

```bash
uv add "kigumi[litellm]"
```

不装 litellm extra 时可用 `StdlibTransport`(纯标准库 HTTP)或自实现 transport。Pi 是外部
runtime：由用户自行安装、固定版本，并把命令与精确版本交给 `PiRpcAdapter`；Kigumi 不安装或
升级 Node/Pi。staging 与 root-scoped 工具限制模型 I/O，但不是 OS sandbox，可信 Extension
仍有宿主进程权限。

DAG 自动重试默认关闭。节点显式声明 `RetryPolicy` 后，Kigumi 持久化 run/attempt 并返回
pending，不在进程内 sleep；外部 supervisor 到期调用 `Dag.resume()`。`EvidencePolicy`
在强制 secret scrub 后控制保留形态，但不是加密或访问控制。缺少 schema-2 manifest 的旧
run 直接 fail closed，不能 resume。

想先零真实请求跑通一遍，可从[客服工单抽取 DAG](examples/ticket_extract/README.md)或
[提示词进化环](examples/prompt_evolve/README.md)开始。两者都记录了实际接入时遇到的框架摩擦；
工单示例还保留了本地实测数字。

## 文档地图

装好之后不必回到这个仓库：`kigumi brief` 打印 agent 进场页，`kigumi docs` 列出随 wheel
交付的全部页，`kigumi docs <name>` 打印其中一页。仓库 `docs/` 是唯一 source of truth，
wheel 只做映射不做复制。

| 文档 | 回答的问题 |
| --- | --- |
| [docs/brief.md](docs/brief.md) | **agent 先看这个**(`kigumi brief`)。这个库已经拥有什么、别另写什么;改节点前先跑哪几条只读命令。英文写成,因为下游项目里的 agent 要读它 |
| [docs/capabilities.md](docs/capabilities.md) | **先看这个。**这个库能做什么;一行一个能力,左边是需求、右边是符号 |
| [DESIGN.md](DESIGN.md) | 为什么这样设计;分层、边界与已裁决的取舍 |
| [docs/adoption.md](docs/adoption.md) | 怎么接入;从单 caller 到 DAG 的路径与排障 |
| [docs/cli.md](docs/cli.md) | `kigumi` 统一 CLI 与可选 `dag` 脚本;全部命令、参数、默认值与退出码 |
| [docs/api.md](docs/api.md) | 公开名称是什么意思;签名、结果类型、策略、异常与工具函数速查 |
| [docs/contracts/README.md](docs/contracts/README.md) | 哪些行为是承诺;索引化的不变式、失效行为与验证坐标 |
| [设计审查](docs/reviews/2026-07-13-design-review.md) / [consumes 审查](docs/reviews/2026-07-14-consumes-projection-design.md) | 某个时点审查出了什么;实然记录,不是规范 |
| [CHANGELOG.md](CHANGELOG.md) | 什么变了;缓存换族与破坏性变更必录 |
| [AGENTS.md](AGENTS.md) | agent 进场先读什么;红线与验证命令 |

## 许可证

[MIT](LICENSE)
