# kigumi coding-agent 指南

kigumi 是给 LLM 内容流水线提供确定性调用、可验证产物与 DAG 编排边界的 Python 库；
修改时优先保护缓存、重放和人工审批的可观测性。

常用命令：

```bash
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run kigumi trace <run_id>
uv run kigumi call <key_prefix> --field response
```

`kigumi` CLI 负责项目运维（guard、runs、approve、diff、gc 等）；`dag.cli()` 负责已注册图的 check、plan、explain、describe、graph、profile、resume、retry-resolve、recover。
`kigumi brief` 与 `kigumi docs` 不需要有效配置即可运行。

硬规矩：

- 改动任一缓存键成分就是缓存换族，必须同步更新 `CHANGELOG.md`。
- `raw-llm-ok` 与 `raw-io-ok` 豁免都必须写清理由，二者不得互相代替。
- 行为变更先写会失败的测试，再实现到测试转绿；不要以文档替代回归测试。
- 不绕过 `canonical_json`、`artifacts.sha`、节点声明和审批 payload 绑定。
- 同一次 run 的框架物化路径必须由唯一节点/item 拥有；不得绕过输出认领直接覆盖。
- Subgraph 只承载静态声明；运行时动态展开仍限 map/scan，模型不得返回可执行拓扑。
- `EvidencePolicy` 只控制清理后的证据保留形态，不是加密或访问控制。
- Agent session carry 默认关闭，只能通过显式 `session_carry` 启用。
- receipt、manifest、candidate、artifact 或 blob 摘要损坏一律 fail closed，不按 miss 重跑。

文档地图（下游项目里用 `kigumi docs <name>` 离线读同样的文本）：

- [docs/brief.md](docs/brief.md) 是 agent 进场页(`kigumi brief`):已有能力、别重造什么、
  改节点前的只读命令、CLI 命令说明。
- [docs/capabilities.md](docs/capabilities.md) 是能力索引:动手前先扫一遍,避免重造已有能力。
- [DESIGN.md](DESIGN.md) 说明设计哲学、边界和止损线。
- [docs/adoption.md](docs/adoption.md) 说明接入方式与使用约定。
- [docs/cli.md](docs/cli.md) 说明 CLI 命令、参数与退出码。
- [docs/recovery.md](docs/recovery.md) 说明终态 run 的恢复与显式决策。
- [docs/api.md](docs/api.md) 是公开 API、结果类型、策略与异常速查。
- [docs/contracts/README.md](docs/contracts/README.md) 索引可验证不变式；修改实现时先读对应契约。
- [CHANGELOG.md](CHANGELOG.md) 记录面向使用者的发布变化，不在此重复细节。
