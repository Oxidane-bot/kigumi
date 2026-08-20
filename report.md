## What changed

- `report.md`：记录本工作单的 RED 验证、代码路径核对、既有回归测试和三道 gate 结果。
- 未修改 `kigumi/dag.py` 或测试实现：工作单要求的 scan 行为在当前分支的可达路径上已经成立，按“前提错误即停止”规则未进行无依据的重构。

## RED proof

为满足 RED-first 要求，先新增了临时回归测试
`tests/test_dag_retry_resume.py::test_scan_resume_with_fresh_item_invokes_post_node`，再运行实现前的当前分支代码。场景是 scan 的 `a` item 已 durable 完成，`b` item 首次 retry pending；恢复时 `b` fresh completion，预期 scan aggregate 调用 `post_node`。

实现前测试没有失败，而是直接通过：

```text
============================= test session starts =============================
.                                                                        [100%]
1 passed in 0.44s
```

因此没有合法的 RED failure 可粘贴。进一步核对 `kigumi/dag.py`：retry pending、checkpoint pending 和 item failure 都在 scan 聚合前直接抛出；只有成功完成的 item 才能到达 `completed[item_id] = artifact`、恢复标志更新和最终 aggregate return。当前累加器在所有可达 aggregate 路径上已会把 fresh miss 置为 `False`。临时测试随后撤销，未将一个 GREEN-from-the-start 测试作为回归测试提交。

## Gate output

按要求依次运行：

```text
$ uv run pytest -q
1242 passed, 6 skipped in 69.66s (0:01:09)

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
164 files already formatted
```

指定既有回归测试也通过：

```text
$ uv run pytest -q tests/test_dag_retry_resume.py::test_resume_post_node_is_consistent_for_ordinary_map_and_scan
1 passed in 2.17s
```

## Commit

- 实现 commit：N/A。因工作单的 RED 前提不成立，按指令停止，未提交实现改动。
- 本报告作为唯一改动归档；报告提交的完整英文消息见最终 `git log` 和交付回复。

## Notes

- 未触碰 `kigumi/prompt.py`、`kigumi/repair.py`。
- 未改变缓存键成分、canonical bytes、receipt/manifest/sidecar/evidence schema，因此没有更新 `CHANGELOG.md`。
- 未改变 ordinary 或 map 路径。
- 未实现“把 scan 改成 outcomes 结构”的纯结构重构，因为当前可达行为没有 RED 证据；失败项若继续聚合会改变 scan 的失败/恢复语义。
- 未解决风险：如果审计要求的是静态结构统一而非可观察行为修复，需要补充一个能让 scan 在非 success item 后仍返回 aggregate 的明确契约或复现器；当前代码中该路径不可达。

最终工作树核验：

```text
$ git status -sb
## fix/resume-post-node

$ git diff --name-only
```
