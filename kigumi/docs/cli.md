# CLI 参考

kigumi 有两套刻意分开的 CLI：

- `kigumi` 是项目运维入口，由 `kigumi.cli:main` 提供；它从当前目录向上发现
  `pyproject.toml` 与 `[tool.kigumi]`，不导入图注册表。
- `dag` 是已注册图的入口，由应用中的 `Dag.cli(argv)` 提供；它操作当前 Python 代码已经
  注册好的图，不会替代项目运维命令。

除 `kigumi init` 外，`kigumi` 命令若没有发现有效的 `[tool.kigumi]`，以 2 退出。
`dag` 不做项目发现：调用它之前，应用必须已经构造 `Dag` 并注册节点。两套 parser 及其
子命令都提供 argparse 自动生成的 `-h` / `--help`；下表列出其余全部参数。环境变量集中见
[接入指南的环境变量总表](adoption.md#环境变量总表)。

## A. `kigumi`：项目运维

### `kigumi init`

在当前目录的既有 `pyproject.toml` 追加 `[tool.kigumi]` 默认配置，创建配置目录与
`.gitkeep`，并把 artifacts 目录加入 `.gitignore`。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--hooks` | 否 | `False` | 额外安装执行 `uv run kigumi guard --changed` 的 pre-commit hook。 |

成功为 0。没有 `pyproject.toml`、TOML 无效、配置块已存在、`--hooks` 不在 Git 仓库内，
或目标 hook 已存在时为 1；命令不会覆盖既有 hook。

### `kigumi guard`

扫描 source dirs 中循环裸 LLM 调用与节点内原始文件读取，并显示已豁免项。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--changed` | 否 | `False` | 只检查 Git 中相对 `HEAD` 已修改、已暂存与未跟踪的 Python 源文件，并报告新增豁免。 |

没有未豁免 violation 时为 0，有 violation 时为 1。`--changed` 不在 Git 仓库内或无法取得
变更清单时为 2。

### `kigumi doctor`

报告项目根、配置路径是否存在、`.env` 实际加载的键名、litellm 可用性与 Prompt 模板数量。
无命令专属参数；完成时为 0。

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

run id 不安全、run 不存在或画像 receipt 校验失败时为 1；成功为 0。

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

## B. `dag`：已注册图

应用通常在构造和注册图后调用 `dag.cli()`；下列 `dag` 是 argparse 显示的程序名。
参数缺失、choice 无效等 parser 错误统一为 2。除特别说明外，命令成功为 0；未捕获的图声明、
文件或画像错误会直接传播给宿主应用。

### `dag check`

只读检查图声明、声明文件、source guards、节点 docstring 与 Pydantic 字段说明。
无命令专属参数。存在 error（包括未豁免 guard violation 或声明文件缺失）时为 1；
只有 warning 时仍为 0。

### `dag plan`

只读预告目标闭包的 `certain`、`at_risk` 与 hit，不运行节点。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--targets A,B` | 否 | `None` | 逗号分隔的目标名；缺省规划整张图。 |

### `dag graph`

输出终端图、Prompt-aware Mermaid，或写入自包含 HTML。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--html PATH` | 否 | `None` | 将 HTML 图写到路径，而不是打印终端图。 |
| `--run-id RUN_ID` | 否 | `None` | 叠加指定 run 的运行态。 |
| `--prompts` | 否 | `False` | 输出带 Prompt 声明的 Mermaid；该模式优先于 `--html`。 |

### `dag profile`

输出 canonical WorkflowProfile 的静态视图或指定 run 视图。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--run-id RUN_ID` | 否 | `None` | 加载持久运行态；缺省为当前注册图的静态画像。 |
| `--format {json,md}` | 否 | `md` | 输出格式。 |
| `--include-content` | 否 | `False` | 在运行画像中展开允许保留的 CALL/Agent 内容证据。 |

这里的 JSON 使用带缩进的普通 JSON 输出，不承诺 `kigumi ... --json` 的
`canonical_json` 字节格式。

### `dag explain NODE_NAME`

对照最近或指定 run，解释一个节点或 `map@item` 当前缓存判断的变化成分。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `NODE_NAME` | 是 | 无 | 节点名或动态 item 名。 |
| `--run-id RUN_ID` | 否 | `None` | 指定对照 run；缺省选择实现定义的现有对照。 |

### `dag describe`

输出注册图的节点、边、模型、Prompt 与检查点声明摘要。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `--format {md,json}` | 否 | `md` | 输出 Markdown 或带缩进的普通 JSON。 |

### `dag resume RUN_ID`

按原 manifest 绑定恢复一个 durable run。

| 参数 | 必需 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `RUN_ID` | 是 | 无 | 要恢复的 schema-2 run。 |
| `--workers N` | 否 | `1` | DAG worker 数。 |

恢复成功（包括返回 pending 状态）为 0；恢复抛错时捕获并以 1 退出。

### `dag retry-resolve RUN_ID TARGET`

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
`canonical_json`。`dag profile --format json` 与 `dag describe --format json` 是可读的
缩进 JSON，不属于这项字节稳定承诺。
