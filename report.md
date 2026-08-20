## What changed

- `kigumi/dag.py`: 抽取 `_write_agent_capacity_failure`，普通 Agent 与 Agent scan 的 slot timeout 共用同一个 failure receipt 写入逻辑；普通路径的字段顺序和值保持不变。
- `tests/test_agent_capacity.py`: 新增 RED 回归测试，验证 scan 生成与普通节点完全相同的 failure JSON 字节、schema 和必需字段。
- `report.md`: 记录 RED、gate、提交和最终工作树状态。

## RED proof

测试先于实现运行；普通节点会生成 receipt，而 scan 节点没有生成 `failures/work.json`：

```text
$ uv run pytest -q tests/test_agent_capacity.py::test_agent_scan_capacity_failure_matches_ordinary_failure_evidence
F                                                                        [100%]
______ test_agent_scan_capacity_failure_matches_ordinary_failure_evidence ______
...
E   FileNotFoundError: [Errno 2] No such file or directory: '.../scan/artifacts/runs/capacity/failures/work.json'
...
1 failed in 0.22s
```

## Gate output

按规定顺序执行：

```text
$ uv run pytest -q
1242 passed, 6 skipped in 91.61s (0:01:31)

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
164 files already formatted
```

## Commit

```text
0ef3f3c43cbf780e26a3012349f631f5f9467f79 Fix scan Agent capacity evidence

- Extract one shared capacity-failure receipt writer so ordinary and scan handlers cannot drift.
- Persist node-name-keyed canonical failure evidence for scan slot timeouts while preserving ordinary output bytes.
- RED-verified; tests: 1242 passed, 6 skipped.
```

## Notes

- 依据 `docs/contracts/agent-capacity.md` 不变式 4，slot timeout 仍产生 typed `AgentRuntimeFailureCode.CAPACITY`；依据 `docs/contracts/failure.md` 不变式 6–7，scan 现在保存与普通 Agent 同形的 canonical typed failure receipt。
- 两份契约没有规定 scan failure 文件名；本次选择 `failures/<node>.json`，与普通路径一致。scan 每次在首个 capacity failure 处停止，因此不会在同一 run 内覆盖多个 item；该命名选择仍值得后续契约评审。
- 未修改 map 路径：源码中没有第三个 Agent slot-timeout handler；本 work unit 只处理 scan gap。
- 未修改 `CHANGELOG.md`：没有改变缓存键、canonical bytes 或 failure schema/field set，只把既有 schema 写入此前缺失的 scan 路径。
- 首次运行 pytest 时项目环境缺少开发依赖，`uv run` 返回 `Failed to spawn: pytest`；随后使用 `uv sync --extra dev` 补齐 pytest/ruff，再执行了上述 RED 和 gate。
- 最终检查：

```text
$ git status -sb
## fix/scan-capacity-failure

$ git diff --name-only
(无输出)
```
