# CLI 参考

全部命令都由 `kigumi`（`kigumi.cli:main`）提供。按"回答问题需要什么"分成两组：

- **项目运维命令**读落盘 artifacts。它们从当前目录向上发现 `pyproject.toml` 与
  `[tool.kigumi]`，从不导入项目代码。
- **图命令**需要内存里的图——节点靠装饰器在 import 时注册，所以必须执行项目的构图代码。
  它们 import `[tool.kigumi] dag_entry` 指向的工厂函数（`"module:callable"`，返回 `Dag`），
  在其结果上作业。

除 `kigumi init`、`kigumi brief` 与 `kigumi docs` 外，命令若没有发现有效的
`[tool.kigumi]`，以 2 退出（这三条在未 init 的目录里同样可用）。图命令另外要求
`dag_entry`：未声明时以 2 退出并给出要补的键。`kigumi init` 会生成
`nodes/graph.py` 骨架并写入该键。

图命令也可以有一个独立的 `dag` 命令——`Dag.cli(argv)` 是同一套 dispatch，但需要项目
自己在 `[project.scripts]` 注册（`kigumi init` 的骨架里 `main()` 就是给它用的），
此时不经过 `dag_entry`：

```toml
[project.scripts]
dag = "nodes.graph:main"
```

这条路径要求项目本身是可安装的（有 build backend、包目录声明正确，并已 install），
console script 才会出现。`kigumi` 走 `dag_entry`，只需要 `pyproject.toml` 与可 import
的模块，不要求项目被打包安装——所以脚本化、CI 与 agent 用前者更省事。

parser 及其子命令都提供 argparse 自动生成的 `-h` / `--help`；下表列出其余全部参数。
环境变量集中见[接入指南的环境变量总表](adoption.md#环境变量总表)。

## A. 项目运维命令

### `kigumi init`

在当前目录的既有 `pyproject.toml` 追加 `[tool.kigumi]` 默认配置，创建配置目录与
`.gitkeep`，并把 artifacts 目录加入 `.gitignore`。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--hooks` | 否 | `False` | 额外安装执行 `uv run kigumi guard --changed` 的 pre-commit hook。 |

成功为 0。没有 `pyproject.toml`、TOML 无效、配置块已存在、`--hooks` 不在 Git 仓库内，
或目标 hook 已存在时为 1；命令不会覆盖既有 hook。

### `kigumi brief`

把 [brief.md](brief.md) 原样打印到 stdout。它是 agent 进场页：说明这个库已经拥有哪些
能力、不要另写什么、改节点前先跑哪几条只读命令、统一 CLI 与可选 `dag` 脚本的边界。

无参数。成功为 0；安装缺失该页时为 1。与 `kigumi docs` 一样，**不需要**有效的
`[tool.kigumi]`：未 init 的目录里也能读。

### `kigumi docs`

列出随 wheel 交付的文档页，或把其中一页原样打印到 stdout。仓库 `docs/` 是唯一
source of truth，wheel 通过 hatch `force-include` 映射而非复制，因此 site-packages
里的文本与仓库一致。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `name` | 否 | 无 | 页名；缺省时打印清单。取值：`brief`、`capabilities`、`adoption`、`api`、`cli`、`recovery`、`contracts`、`design`、`changelog`。 |

成功为 0；安装缺失该页时为 1；页名不在上表时由 argparse 判 2。

### `kigumi guard`

扫描 source dirs 中循环裸 LLM 调用与节点内原始文件读取，并显示已豁免项。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--changed` | 否 | `False` | 只检查 Git 中相对 `HEAD` 已修改、已暂存与未跟踪的 Python 源文件，并报告新增豁免。 |

没有未豁免 violation 时为 0，有 violation 时为 1。`--changed` 不在 Git 仓库内或无法取得
变更清单时为 2。

### `kigumi doctor`

报告项目根、配置路径是否存在、`.env` 实际加载的键名、litellm 可用性、Prompt 模板数量，
以及配置的 `dag_entry`（未声明时明说图命令不可用）。`dag_entry` 按配置原文报告、不解析：
本组命令从不 import 项目代码，import 是图命令的代价。要知道工厂需要哪些参数，敲任一图
命令即可，缺参数的报错会指名它们。无命令专属参数；完成时为 0。

### `kigumi render TEMPLATE`

加载 `prompts_dir/TEMPLATE.md`，严格渲染模板；未显式给值的槽位使用 `<槽位名>`。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `TEMPLATE` | 是 | 无 | 不带 `.md` 的模板名。 |
| `--slot NAME=VALUE` | 否 | `[]` | 覆盖一个槽位；可重复。 |

文件不存在、`--slot` 不是 `NAME=VALUE`、槽位契约错误或仍有未渲染语法时为 1；成功为 0。

### `kigumi runs list`

列出持久 run 的节点命中、挂起、retry 与 ambiguous attempt 摘要。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--json` | 否 | `False` | 输出稳定的 `canonical_json`。 |

完成时为 0；没有 run 时输出空清单。

### `kigumi runs show RUN_ID`

查看一个 run 的节点、审批、attempt、policy digest 与 WorkflowProfile。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `RUN_ID` | 是 | 无 | run 目录名。 |
| `--json` | 否 | `False` | 输出稳定的 `canonical_json`。 |

run id 不安全、run 不存在或画像 receipt 校验失败时为 1；成功为 0。非 JSON 输出在终态
`failed` run 上会附带可复制的 recovery 建议。该建议从落盘状态无法得知构图工厂参数；
`recover` 与随后显式执行的 `resume` 都必须使用构造该 run 时的同一组、可重复的实际
`--graph-arg KEY=VALUE` 参数。把这些选项放在建议命令的 `--` 分隔符之前，再把
`RUN_ID`（以及 recovery 的 `TARGET`）放在分隔符之后；不要用占位值代替，也不要假设
run state 会重建这些值。

### `kigumi approve RUN_ID NAME`

为同一 run 中名为 `NAME` 的 pending checkpoint 写入与 payload 摘要绑定的批准数据。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `RUN_ID` | 是 | 无 | 挂起 checkpoint 所在 run。 |
| `NAME` | 是 | 无 | 完整限定的 checkpoint 名。 |
| `--data JSON` | 否 | `"{}"` | 写入审批记录的 JSON 值。 |

JSON 无效、run/name 路径无效、pending 不存在或 payload 绑定失败时为 1；成功为 0。

### `kigumi diff RUN_A RUN_B`

按 canonical artifact 摘要比较两个 run，并补充各节点或 item 的缓存键成分差异。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `RUN_A` | 是 | 无 | 左侧 run id。 |
| `RUN_B` | 是 | 无 | 右侧 run id。 |
| `--json` | 否 | `False` | 输出稳定的 `canonical_json`。 |

任一 run id 不安全或不存在时为 1；成功为 0。

### `kigumi trace RUN_ID`

沿 run、节点、map/scan item 与 L1 CALL 展开证据链；可只取一个节点。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `RUN_ID` | 是 | 无 | 要追踪的 run id。 |
| `--node NAME` | 否 | `None` | 只显示指定节点。 |
| `--json` | 否 | `False` | 输出稳定的 `canonical_json`。 |

run/节点/载荷不存在或输入无效时为 1；成功为 0。

### `kigumi call KEY_PREFIX`

按唯一 L1 key 前缀读取调用载荷或其中一个字段。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `KEY_PREFIX` | 是 | 无 | 必须唯一匹配的缓存键前缀。 |
| `--field FIELD` | 否 | `None` | 可选 `messages`、`response`、`reasoning`、`meta`；缺省输出完整载荷。 |

未指定 `--field` 或选择 `messages` / `reasoning` / `meta` 时输出 `canonical_json`；
`response` 输出裸文本。前缀无匹配、匹配不唯一或 response 不是文本时为 1；成功为 0。

### `kigumi gc --keep N`

保留最近 N 个 run 的可达节点缓存与 blob，删除其余不可达条目。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--keep N` | 是 | 无 | 非负整数；`0` 表示不按 run 保留。 |

负数等无效保留值为 1；成功输出删除总数并以 0 退出。

## B. 图命令

这一组需要构造好的图。经 `kigumi <命令>` 时由 `dag_entry` 提供，经独立 `dag` 命令时
由应用自己的 `Dag.cli()` 提供；两条路径共用 `Dag.run_command`，同名命令参数完全一致，
下表两种写法都适用。唯一的例外是 `--graph-arg`（见下），它只出现在 `kigumi` 一侧。

参数缺失、choice 无效等 parser 错误统一为 2；`dag_entry` 缺失、模块不可导入、属性不存在、
不可调用或返回值不是 `Dag` 时也是 2，stderr 指出错在哪一段。除特别说明外，命令成功为 0；
未捕获的图声明、文件或画像错误会直接传播给宿主应用。

### `--graph-arg KEY=VALUE`

全部 9 条图命令都接受，可重复。把一个 `key=value` 按名传给 `dag_entry` 工厂，
供图形状或 `params` 依赖运行时输入的项目使用（按 episode 展开 `foreach`、按输入文件
声明 `files` 等）。只在 `kigumi <命令>` 上提供：它才负责构图，而 `Dag.cli()` 拿到的是
已经构好的图，这个旗标在那一侧不可能起作用。

```bash
kigumi plan --graph-arg episode=E2S4
kigumi explain scene_E2S4 --graph-arg episode=E2S4
```

绑定按工厂真实签名进行，不靠调用后捕获 `TypeError`（否则工厂自身抛的 `TypeError`
会被误报成用法错误）：

| 情况 | 行为 |
| --- | --- |
| 工厂零参 | 不需要 `--graph-arg`，与 0.9 行为一致。 |
| 必需参数未给 | 2，stderr 指名缺哪几个参数并给出要敲的 `--graph-arg`。 |
| 参数有默认值 | 可不给；给了就覆盖默认值。 |
| 给了工厂不接受的名字 | 2，并列出工厂实际接受的参数名；静默丢弃会构出另一个图。 |
| 工厂声明 `**kwargs` | 由工厂自行裁决参数名，CLI 不代为拒绝。 |
| positional-only 参数 | 2，明说该参数无法按名传入，而不是报成"缺参数"。 |
| 同名参数给两次 | 2；静默取一个会构出说不清的图。 |
| 值里含 `=` | 只按首个 `=` 切分，其余属于值。 |

值一律以 `str` 传入；需要 `Path`、`int` 等类型时由工厂自己转换和校验——它才知道
合法取值范围。

**不要为了让命令闭嘴而给占位默认值。** `params` 是 L3 缓存键成分
（见[缓存键契约](contracts/cache-key.md)），占位值下 `plan` 预告的是一个不会被任何
真实运行使用的键空间、`explain` 把每个节点都报成 `params` 变化、`check` 校验的是
不存在的声明文件；而 `resume` 会带着错误的 `graph_identity` 真的执行节点。传真实值，
否则这组命令的结论无法用于判断花钱前的爆炸半径。

### `kigumi check`

只读检查图声明、声明文件、source guards、节点 docstring 与 Pydantic 字段说明。
无命令专属参数。存在 error（包括未豁免 guard violation 或声明文件缺失）时为 1；
只有 warning 时仍为 0。

### `kigumi plan`

只读预告目标闭包的 `certain`、`at_risk` 与 hit，不运行节点。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--targets A,B` | 否 | `None` | 逗号分隔的目标名；缺省规划整张图。 |

### `kigumi graph`

输出终端图、Prompt-aware Mermaid，或写入自包含 HTML。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--html PATH` | 否 | `None` | 将 HTML 图写到路径，而不是打印终端图。 |
| `--run-id RUN_ID` | 否 | `None` | 叠加指定 run 的运行态。 |
| `--prompts` | 否 | `False` | 输出带 Prompt 声明的 Mermaid；该模式优先于 `--html`。 |

### `kigumi profile`

输出 canonical WorkflowProfile 的静态视图或指定 run 视图。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--run-id RUN_ID` | 否 | `None` | 加载持久运行态；缺省为当前注册图的静态画像。 |
| `--format {json,md}` | 否 | `md` | 输出格式。 |
| `--include-content` | 否 | `False` | 在运行画像中展开允许保留的 CALL/Agent 内容证据。 |

这里的 JSON 使用带缩进的普通 JSON 输出，不承诺 `kigumi ... --json` 的
`canonical_json` 字节格式。

### `kigumi explain NODE_NAME`

对照最近或指定 run，解释一个节点或 `map@item` 当前缓存判断的变化成分。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `NODE_NAME` | 是 | 无 | 节点名或动态 item 名。 |
| `--run-id RUN_ID` | 否 | `None` | 指定对照 run；缺省选择实现定义的现有对照。 |

### `kigumi describe`

输出注册图的节点、边、模型、Prompt 与检查点声明摘要。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--format {md,json}` | 否 | `md` | 输出 Markdown 或带缩进的普通 JSON。 |

### `kigumi resume RUN_ID`

按原 manifest 绑定恢复一个 durable run。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `RUN_ID` | 是 | 无 | 要恢复的 schema-2 run。 |
| `--workers N` | 否 | `1` | DAG worker 数。 |

恢复成功（包括返回 pending 状态）为 0；恢复抛错时捕获并以 1 退出。

### `kigumi recover RUN_ID TARGET`

为 terminal `failed` run 写入一次 append-only recovery receipt，并按显式 decision
决定是否排队一个新的 attempt。完整语法如下；`--graph-arg` 只适用于 `kigumi` 入口，
独立 `dag recover` 使用同样的其余参数：

```bash
kigumi recover RUN_ID TARGET \
  --attempt N \
  --decision {retry_not_started,retry_after_external_check,fail} \
  --reason TEXT \
  [--evidence REF ...] \
  [--graph-arg KEY=VALUE ...]
```

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `RUN_ID` | 是 | 无 | 要恢复的 schema-2 terminal `failed` run。 |
| `TARGET` | 是 | 无 | 失败节点或 map/scan item，例如 `outline` 或 `outline@item-7`。 |
| `--attempt N` | 是 | 无 | 当前失败 attempt 序号；正整数校验由 `Dag.recover()` 执行。 |
| `--decision {retry_not_started,retry_after_external_check,fail}` | 是 | 无 | 明确失败操作尚未开始、已完成外部核验后重试，或作最终失败裁决。 |
| `--reason TEXT` | 是 | 无 | 必填并持久化的操作员理由。 |
| `--evidence REF` | 否 | `[]` | 一个证据引用；可重复，每次传一个引用。 |
| `--graph-arg KEY=VALUE` | 否 | `[]` | 传给 `dag_entry` 工厂的参数；可重复，仅 `kigumi` 入口提供。 |

retry decision 会由 API 排队恰好一个新的 attempt；`fail` 的 `to_attempt` 等于
`from_attempt`，不会排队 retry。成功输出一行包含 run、target、两个 attempt、decision
和 evidence 数量，并以 0 退出。API 抛错时 stderr 输出 `error: <message>` 并以 1 退出；
参数缺失或 choice 无效仍是 argparse 的 2。recover **不会自动 resume**；需要执行排队的
retry 时，操作员必须另行明确运行 `kigumi resume RUN_ID`。

重试示例：

```bash
kigumi recover run-0042 transcode \
  --attempt 3 \
  --decision retry_after_external_check \
  --reason "已验证替换后的 ffmpeg 能处理失败场景" \
  --evidence validation-report.md
# 之后如果要执行排队的 attempt，再明确运行：
kigumi resume run-0042
```

最终失败示例：

```bash
kigumi recover run-0042 transcode \
  --attempt 3 \
  --decision fail \
  --reason "外部核验确认该场景不可恢复" \
  --evidence incident-2026-08-02.md
```

### `kigumi retry-resolve RUN_ID TARGET`

为一个 ambiguous attempt 持久化人工裁决。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `RUN_ID` | 是 | 无 | attempt 所属 run。 |
| `TARGET` | 是 | 无 | 节点或 `node@item` target。 |
| `--attempt N` | 是 | 无 | 要裁决的 attempt 序号。 |
| `--action {retry,fail}` | 是 | 无 | 明确允许重试，或把该 attempt 判为失败。 |
| `--reason TEXT` | 是 | 无 | 必填、持久化的操作员理由。 |

裁决成功为 0；目标、attempt、action 或当前状态不接受裁决时捕获并以 1 退出。

## 稳定机器输出

带 `--json` 的 `kigumi trace`、`kigumi diff`、`kigumi runs list` 与
`kigumi runs show` 都使用稳定 `canonical_json`，适合 `json.loads` 消费。
`kigumi call` 没有 `--json`：除 `--field response` 的裸文本外，它的输出同样是
`canonical_json`。`kigumi profile --format json` 与 `kigumi describe --format json` 是可读的
缩进 JSON，不属于这项字节稳定承诺。
