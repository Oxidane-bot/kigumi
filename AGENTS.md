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

`kigumi` CLI 负责项目运维（guard、runs、approve、diff、gc 等）；`dag.cli()` 负责已注册图的 check、plan、graph、explain、describe。

硬规矩：

- 改动任一缓存键成分就是缓存换族，必须同步更新 `CHANGELOG.md`。
- `raw-llm-ok` 与 `raw-io-ok` 豁免都必须写清理由，二者不得互相代替。
- 行为变更先写会失败的测试，再实现到测试转绿；不要以文档替代回归测试。
- 不绕过 `canonical_json`、`artifacts.sha`、节点声明和审批 payload 绑定。
- 同一次 run 的框架物化路径必须由唯一节点/item 拥有；不得绕过输出认领直接覆盖。
- Subgraph 只承载静态声明；运行时动态展开仍限 map/scan，模型不得返回可执行拓扑。

文档地图：

- [DESIGN.md](DESIGN.md) 说明设计哲学、边界和止损线。
- [docs/adoption.md](docs/adoption.md) 说明接入方式与使用约定。
- [docs/contracts/](docs/contracts/) 是可验证不变式的权威文本；修改实现时先读对应契约。
- [CHANGELOG.md](CHANGELOG.md) 记录面向使用者的发布变化，不在此重复细节。
