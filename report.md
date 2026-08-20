## What changed

- `kigumi/dag.py`：将 map/scan 的“所有 item 已从 durable state 恢复”状态传播到外层节点回调门，使普通节点、map 聚合和 scan 聚合在完整 resume 时一致跳过 `post_node`。
- `tests/test_dag_retry_resume.py`：新增覆盖普通节点、map 聚合和 scan 聚合的 resume 回调一致性回归测试。
- `CHANGELOG.md`：在 `[Unreleased]` 下记录用户可见的 `post_node` resume 行为修复。
- `report.md`：记录 RED、gate、契约判断和提交结果。

## RED proof

契约判断先于实现：`docs/contracts/retry-resume.md` 不变量 9 规定同 run 的 completed artifact 在 resume 时必须重验且“不重新执行”，但没有规定 `post_node` 或 observer 在 resume 时是否调用；对全部 `docs/contracts/` 搜索 callback/observer/post_node 也没有发现 resume 相关条款。`docs/adoption.md:328-330` 只说明一般情况下产物可用后调用，没有 resume 特例。因此契约本身属于 (c) silent；按工作单要求选择 (b)，即 resumed nodes 不调用 `post_node`。这是一个需要后续 review 的契约沉默决策。

新增测试在实现前运行失败：

```text
________ test_resume_post_node_is_consistent_for_ordinary_map_and_scan _________
E       AssertionError: assert ['mapped', 'scanned'] == []
E         Left contains 2 more items, first extra item: 'mapped'
1 failed in 0.56s
```

修复后该测试通过：`1 passed in 2.42s`。

## Gate output

按要求依次运行：

```text
$ uv run pytest -q
1242 passed, 6 skipped in 101.02s (0:01:41)

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
164 files already formatted
```

## Commit

实现 commit：`8277ec6`

```text
Fix: Align post_node behavior for resumed aggregates

- Propagate full dynamic resume state to the outer callback gate so ordinary, map, and scan nodes skip post_node consistently.
- Add a RED-verified regression test and document the user-visible callback fix.
- tests: 1242 passed, 6 skipped
```

## Notes

- 未修改 `kigumi/prompt.py`、`kigumi/repair.py`；未改变缓存键成分、canonical 字节、receipt/manifest/sidecar/evidence schema，也未修改 `post_node` 签名或 callback protocol。
- 空 dynamic aggregate 不被视为从 durable item state 完整恢复；含有新执行 item 的 map/scan aggregate 也继续调用 `post_node`。
- 初次 RED 命令因 worktree 的 uv 环境尚未安装 pytest 而未执行测试，随后使用 `uv sync --locked --extra dev` 补齐项目声明的开发依赖；实际 RED 证据见上文。

最终工作树核验：

```text
$ git status -sb
## fix/resume-post-node

$ git diff --name-only
```
