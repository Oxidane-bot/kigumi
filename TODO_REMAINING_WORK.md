# Remaining Work for Greenfield Cleanup

**Branch**: `greenfield-cleanup-wip`  
**Plan**: See `GREENFIELD_CLEANUP_PLAN.md` for full context  
**Current Status**: ~40% complete, **211 tests failing** due to signature breaks

---

## CRITICAL BLOCKERS (Must Fix First)

### 1. Fix NodeContext signature break (211 test failures)

`kigumi/dag.py` — `NodeContext.__init__` removed `prompt_snapshot` parameter but 16 call sites still pass it. Remove `prompt_snapshot=prompt_snapshot,` from these lines:

- Line 1140
- Line 1214
- Line 1287
- Line 1873
- Line 1941
- Line 1990
- Line 2029
- Line 2056
- Line 2136
- Line 2172
- Line 2202
- Line 2233
- Line 2933
- Line 3020
- Line 3289
- Line 3386

**How to verify**: `uv run pytest tests/test_prompt_specs.py::test_prompt_spec_resolves_layers_axis_and_fenced_material_before_call` should pass after fix.

### 2. Fix AgentBuildContext signature break

`kigumi/dag.py` lines 1303 and 3401 — remove `render=context.render,` from AgentBuildContext construction calls.

### 3. Remove prompts= from Subgraph signatures

`kigumi/subgraph.py` lines ~75, ~107, ~155 — remove `prompts: Iterable[str] = (),` parameter from:
- `Subgraph.node()`
- `Subgraph.map()`
- `Subgraph.scan()`

Also remove the `tuple(prompts)` assignments and `legacy_prompts=...` kwargs in validation calls inside these methods.

### 4. Fix kigumi init scaffold

`kigumi/cli.py` line 67 — change `prompts=()` to `prompt_specs=()` in the DAG_ENTRY_TEMPLATE.

### 5. Run ruff fixes

```bash
uv run ruff check --fix .
uv run ruff format .
```

**Verification**: After 1-5, run `uv run pytest -q` — should go from 211 failures to 0.

---

## Phase 3 Completion (Schema-1 Hard-Cut)

### 6. Update 3 contract files

Remove schema-1 legacy promises from:

**`docs/contracts/workflow-profile.md`**:
- Delete item 8 (lines ~44-45): "0.6/schema-1 run 可只读展示持久信息，并固定 `resolution_status=unavailable_legacy`"
- Delete from change policy (lines ~62-63): "破坏既有 schema-1 profile 读取时必须递增 `workflow_profile_schema`"

**`docs/contracts/retry-resume.md`**:
- Item 4 (line ~25): change "schema-1/0.6 run 只读，不可 resume" to "缺少 schema-2 manifest 的 run 不可 resume"

**`docs/contracts/checkpoint.md`**:
- Line ~24: delete "0.6/schema-1 run 只读不可恢复"

### 7. Update README.zh-CN.md

Line ~118: delete "0.6 run 在 0.7 中仍可作为 legacy profile 查看，不可 resume。"

---

## Phase 3b Completion (prompts=() Hard-Cut)

### 8. Migrate examples/ticket_extract

`examples/ticket_extract/pipeline.py` currently uses `ctx.render()` which was deleted. Rewrite using `PromptSpec` + `FileRef`:

**extract node** (currently uses `prompts=("extract",)` + `ctx.render`):
```python
# Add before @dag.map
EXTRACT_SPEC = PromptSpec(
    name="extract",
    base=PromptRef("extract"),
    materials=(
        PromptMaterial(
            slot="ticket",
            source=FileRef(path_from=ItemRef("source")),
            title="工单原文",
        ),
    ),
)

# Change decorator
@dag.map(
    "extract",
    items_from=("ingest", "tickets"),
    key_fn=lambda ticket: str(ticket["id"]),
    prompt_specs=(EXTRACT_SPEC,),  # changed from prompts=
    files_fn=lambda ticket: (str(ticket["source"]),),
)
def extract(ticket, inputs, ctx):
    """从单张工单抽取字段；原文不进入下游 artifact。"""
    del inputs
    # Remove: raw_text = ctx.read_text(...)
    # Remove: prompt = ctx.render(...)
    # Change to:
    extraction = ctx.call_validated(
        ctx.resolve_prompt("extract"),
        TicketExtraction,
        max_repairs=1
    )
    return extraction.model_dump()
```

**report node** (similar pattern):
```python
REPORT_SPEC = PromptSpec(
    name="report",
    base=PromptRef("report"),
    materials=(
        PromptMaterial(
            slot="stats",
            source=InputRef("stats"),
            title="统计数据",
        ),
    ),
)

# Change decorator to use prompt_specs=(REPORT_SPEC,)
# Change body to use ctx.resolve_prompt("report")
```

Verify: `uv run python examples/ticket_extract/pipeline.py` should still work.

### 9. Update prompt-resolution contract

`docs/contracts/prompt-resolution.md`:
- Items 7-8 (lines ~36-40): delete "legacy `prompts=()` 与 PromptSpec name 不得冲突" and "既有 `prompts=()` 与 `ctx.call()` 和字符串 Agent instruction 继续可用"
- Rewrite to: "节点只能通过 `prompt_specs=()` 声明 Prompt；节点内 `ctx.resolve_prompt()` 返回 managed `ResolvedPrompt`。字符串直接传给 `ctx.call()` 标记为 unmanaged。"

### 10. Update capabilities.md

`docs/capabilities.md` line 27 — remove `ctx.render` from the capability list (it was deleted). Keep `ctx.resolve_prompt`.

---

## Phase 4 Completion (Documentation Unification)

### 11. Fix remaining "两套 CLI" sites

**`kigumi/docs.py` line 64**: Change cli summary from `"both CLIs: every command, flag, default and exit code"` to `"unified kigumi CLI: all commands, flags, defaults and exit codes; optional standalone dag script"`

**`AGENTS.md` lines 34, 38**: Change "两套 CLI 分工" (2 occurrences) to "CLI 命令说明"

**`docs/adoption.md` line 18**: Change "两套 CLI 分工（见 brief.md）" to "CLI 命令说明（见 cli.md 与 brief.md）"

**`README.zh-CN.md` line 131**: Change "两套 CLI 怎么分工;全部命令、参数、默认值与有意义的退出码" to "`kigumi` 统一 CLI 与可选 `dag` 脚本;全部命令、参数、默认值与退出码"

Note: `CHANGELOG.md` lines 31, 54 are historical records of the 0.8.0 CLI unification work — leave them unchanged.

---

## Phase 5 Completion (Repository Hygiene)

### 12. Delete empty downloads/ directory

```bash
rm -rf downloads
```

### 13. Clean .gitignore

Line 10: delete `examples/layered_prompts/` (that directory doesn't exist)

Line with `uv.lock`: add comment above it:
```gitignore
# Lock is not tracked (library convention); application projects should track it
uv.lock
```

### 14. Fold DEGRADATION.md into agent-node contract

Add to `docs/contracts/agent-node.md` after the `## Verification / change policy` section:

```markdown
### 未完成的验证

Pi session carry 的 live 腿尚未跑过。2026-07-26 交付 `agent_scan` session carry 时，
应跑而未跑的是 `KIGUMI_PI_LIVE=1` 下配真实、已配置凭据的 Pi 0.82.1 的一致性测试
（含两项 `agent_scan`，持久化并恢复一个显式 session）；实际只跑了确定性 fake-Pi RPC
测试（覆盖缺失文件创建、显式 `--session`、header cwd 规范化、blob carry/cache 重放、
大小上限与失败行为），并审读了 Pi 0.82.1 `SessionManager` 与 RPC 持久化路径。

残留风险：真实 Pi/provider 组合可能存在 fake 进程未表达的运行时 session 格式、
持久化时序或 extension 交互。把 live Pi session carry 当作 provider-conformant 之前，
必须按文档环境跑 `tests/test_pi_live.py::test_real_pi_rpc_conformance`。
```

Then delete `DEGRADATION.md` from the repository root.

### 15. Move this plan to docs/reviews/

```bash
git mv GREENFIELD_CLEANUP_PLAN.md docs/reviews/2026-07-31-greenfield-cleanup.md
```

---

## Final Verification

```bash
uv run pytest -q                        # should be 499+ passed, 3 skipped
uv run ruff check .                     # should pass
uv run ruff format --check .            # should pass
uv build
uv run python scripts/verify_dist.py --expected-version 0.8.0
uv run python scripts/smoke_installed.py
```

---

## Commit Strategy (6 commits after all work done)

1. `fix: centralize schema version constants`
2. `fix: fail closed on corrupted sidecar (ExplainResult.legacy → exception)`
3. `Hard-cut: remove schema-1/0.6 legacy read path (greenfield)`
4. `Hard-cut: remove prompts=() and ctx.render() for LLM input; add FileRef (greenfield API)`
5. `docs: update stale "two CLIs" prose after unification`
6. `chore: fold degradation record into agent-node contract, clean stray files`

---

## Estimated Time

- Blockers 1-5: **30 min**
- Phase 3 items 6-7: **30 min**
- Phase 3b items 8-10: **45 min**
- Phase 4 item 11: **15 min**
- Phase 5 items 12-15: **15 min**
- Final verification: **10 min**

**Total: ~2.5 hours** of focused work to reach 100% completion.
