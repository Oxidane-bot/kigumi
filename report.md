## What changed

- `kigumi/dag.py`：将 `profile --format json` 与 `describe --format json` 路由到
  `canonical_json`，保证 CLI stdout 的键序和字节形态稳定。
- `tests/test_cli.py`：新增同时覆盖两个 CLI 路径的字节级回归测试。
- `CHANGELOG.md`：在 `[Unreleased]` 下记录本次用户可见的 CLI 输出变化。
- `docs/cli.md`：更新 profile/describe JSON 输出及稳定机器输出的说明。
- `docs/brief.md`：同步更新 graph CLI JSON 稳定性说明。

契约原文（`docs/contracts/determinism.md:24`）：

> 1. `canonical_json(sort_keys/indent=2/ensure_ascii=False)` 是唯一 JSON 序列化；`artifacts.sha` 是唯一哈希入口。

## RED proof

实现前运行：

```text
$ uv run --extra dev pytest -q tests/test_cli.py::test_cli_json_outputs_use_canonical_json
F                                                                        [100%]
=================================== FAILURES ===================================
___________________ test_cli_json_outputs_use_canonical_json ___________________
...
E       assert '{\n  "workfl...n": null\n}\n' == '{\n  "graph"...hema": 2\n}'
E         {
E       +   "workflow_profile_schema": 2,
E       +   "mode": "static",
E       +   "resolution_status": "unresolved",
E           "graph": {
E       -     "edges": [],...
...
1 failed in 0.10s
```

失败证明原实现按字典插入顺序输出，而不是按 `canonical_json` 的排序键输出。

## Gate output

按要求顺序执行：

```text
$ uv run pytest -q
1242 passed, 6 skipped in 102.28s (0:01:42)

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
164 files already formatted
```

## Commit

实现提交：`1887c1dea543e37e092faf44c825a7eccdacd4ab`

```text
CLI: Canonicalize graph JSON output

- Route profile and describe JSON display through canonical_json so CLI bytes remain stable across dict construction order changes.
- Document the stable output contract and add regression coverage for both commands.
- RED-verified; tests: 1242 passed, 6 skipped.
```

## Notes

- 初次直接运行 `uv run pytest -q` 时环境尚未安装开发 extra，无法 spawn `pytest`；按仓库契约用
  `uv run --extra dev` 运行 RED 测试后，指定的完整 gate 命令正常通过。
- 未触碰 `kigumi/prompt.py` 或 `kigumi/repair.py`，未改变缓存键、canonical artifact、receipt、manifest、sidecar 或 evidence schema。
- 现有 CLI 测试没有未排序 JSON 快照；文档中发现的旧“不保证字节稳定”说明已在 `docs/cli.md` 和
  `docs/brief.md` 同步修正。
- 本报告作为独立审计产物提交；上方 `## Commit` 记录的是实现提交及其完整消息。

最终检查：

```text
$ git status -sb
## fix/cli-canonical-json

$ git diff --name-only
```
