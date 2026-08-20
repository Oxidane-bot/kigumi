# Work Order Report

## What changed

- `tests/test_consumes.py`: 给上游产物增加未消费字段，并断言 `upstream:source` 的投影值等于 canonical 投影视图摘要，且不同于完整上游产物摘要；保留原有 label-set 与声明描述断言。
- `report.md`: 记录 RED 突变证明、三道 gate、提交信息和最终工作区状态。

## RED proof

正确实现下强化测试通过：

```text
.                                                                        [100%]
1 passed in 0.02s
```

按 work order 临时让 `_key_components` 忽略 `consumes` 投影、直接使用完整上游摘要后，同一测试按预期变 RED：

```text
F                                                                        [100%]
___ test_consumes_preserves_label_set_and_describe_reports_only_declarations ___
E       AssertionError: assert '19aeeefd2e47...c8032959646f6' == '61d591749673...0cc1bf7a0f759'
E         - 61d591749673e1c1007224ab9667a99535ea96489de3471250e0cc1bf7a0f759
E         + 19aeeefd2e473711524394f242fedfbdea285234494184d64f1c8032959646f6
1 failed in 0.03s
```

其中期望值是 `{\"used\": 1}` 的 `sha`，实际值是包含 `ignored` 字段的完整产物摘要；突变随后已恢复。

## Gate output

```text
$ uv run pytest -q
........................................... [ 98%]
ss.....................                                                  [100%]
1241 passed, 6 skipped in 100.04s (0:01:40)

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
164 files already formatted
```

## Commit

测试变更提交：`edfab70ce3707b64772f911958cd583b00d475f5`

完整提交信息：

```text
Tests: Assert consumes projection digest

- Assert the projected upstream component hashes the canonical consumed view and differs from the full artifact digest.
- Add an ignored upstream field so the projection distinction is observable.
- RED-verified with a temporary projection bypass; tests: 1241 passed, 6 skipped.
```

## Notes

- `CACHE-FAMILY-BREAK: no`。只修改测试；没有改动 `kigumi/`、缓存键推导、canonical bytes、artifact schema 或 `CHANGELOG.md`。
- 已阅读 `docs/contracts/README.md`、`docs/contracts/cache-key.md` 和 `docs/contracts/determinism.md`；本单元没有删除或削弱既有检查。
- 同文件没有其他同型的 label-set-only 弱断言。`test_consumes_view_is_canonical_and_hides_unprojected_fields` 与 `test_subgraph_consumes_uses_local_dependency_names_after_mount` 已直接断言摘要值，其余 sibling tests 覆盖运行、缓存或注册行为。
- 初次运行因环境未安装 pytest 而未进入测试；随后使用 `uv sync --locked --extra dev` 补齐锁定的开发依赖。三道 gate 均使用规定的 `uv run` 命令完成。
- `git status -sb`：

  ```text
  ## fix/consumes-projection-test
  ```

- `git diff --name-only`：无输出。
