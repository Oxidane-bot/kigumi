## What changed

- `kigumi/dag.py`：用 `Counter` 单次统计重复 `item_id`，再按原有 `sorted()` 顺序生成重复列表，避免 `list.count()` 的 O(n²) 扫描。
- `tests/test_dag_map.py`：新增多个重复值的完整异常消息测试，锁定排序后的 `a, z` 顺序。

## RED proof

N/A：工单明确这是保持可观察行为不变的纯性能重构，因此不要求行为 RED。新增消息顺序测试在实现替换前已运行并通过（`1 passed`），作为既有行为基线；实现后相关两项测试为 `2 passed`。

## Gate output

按要求顺序运行：

```text
$ uv run pytest -q
1242 passed, 6 skipped in 81.30s (0:01:21)

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
164 files already formatted
```

## Commit

待提交后填写 commit hash；提交信息为：

```text
Optimize map duplicate item detection

- Replace quadratic list.count duplicate scanning with a single Counter pass
- Lock the exact sorted error message for multiple duplicate item IDs
- RED-verified by the pre-change behavior baseline; tests: 1242 passed, 6 skipped
```

## Notes

- 未更新 `CHANGELOG.md`：本次只改变重复检测实现，不改变缓存键成分、canonical bytes、摘要或 receipt/manifest/sidecar/evidence schema；异常类型、文案和排序保持不变。
- 未触及 `kigumi/prompt.py`、`kigumi/repair.py`，也未改动 item-expansion 周边逻辑、item ID 派生或 `key_fn` 处理。

```text
$ git status -sb
待提交后填写

$ git diff --name-only
待提交后填写
```
