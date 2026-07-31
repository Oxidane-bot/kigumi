# kigumi Greenfield Cleanup Plan

**Date**: 2026-07-31  
**Context**: 0.8.0, API not frozen, pure greenfield (no external users)  
**Goal**: Remove legacy compatibility shims, fix schema constant duplication bugs, unify CLI documentation, and improve repository hygiene.
**Status**: Completed 2026-07-31; full suite passes with 513 tests and 2 skips.

---

## Executive Summary

This repository retains compatibility code for 0.6 runs that cannot exist in a greenfield project (cache families rotated twice since 0.6). Three categories of issues were found:

1. **Critical bugs** (schema constant duplication) — will cause silent cross-schema reads on next version bump
2. **Dead legacy paths** — schema-1 read branches, `ExplainResult.legacy`, 0.6 prose in user docs
3. **Documentation drift** — "two CLIs" prose after CLI unification, stale `kigumi/docs.py` summary
4. **Repository hygiene** — empty `downloads/`, phantom gitignore entry, `DEGRADATION.md` unreferenced

**Recommended execution order**: Section 2 (bugs) → Section 3 (hard-cut legacy) → Section 4 (docs) → Section 5 (hygiene) → Section 6 (decide on `prompts=` future).

---

## 1. Scope and Constraints

### What this audit covered
- Legacy/compatibility markers (`legacy`, `0.6`, `schema-1`)
- Schema version constant distribution and literal duplication
- CLI unification documentation drift
- Repository hygiene (untracked dirs, gitignore phantoms, orphaned docs)

### What this audit did NOT cover
- Full correctness verification (assumes existing tests are correct)
- Performance profiling
- API ergonomics or naming consistency
- Module boundary refactoring beyond what's necessary for the fixes below

### Hard constraints from AGENTS.md
- **Cache family changes** require same-commit `CHANGELOG.md` update
- **Behavior changes** require failing test first (RED → GREEN)
- **Corrupted receipts/manifests/artifacts** must fail closed, never retry as cache miss
- One commit does one thing; fixes and features do not mix

---

## 2. Critical Bugs (Priority 0 — do first)

### 2.1 Schema constant duplication will cause silent failures

**Problem**: Four schema fields are written and read with hardcoded `2` instead of module constants, scattered across 3-4 modules each. Next version bump will miss some sites → silent cross-schema read → violates fail-closed contract.

| Field | Has constant? | Writers | Readers | Risk |
|-------|--------------|---------|---------|------|
| `run_sidecar_schema` | **NO** | `_execution.py:168` (literal `2`) | `profile.py:199`, `dag.py:1837` (both literal `2`) | High |
| `failure_schema` | **NO** | `agents.py:869`, `dag.py:1409` (both literal `2`) | `profile.py:326` (literal `2`) | High |
| `candidate_schema` | YES (`SUCCESS_CANDIDATE_SCHEMA = 2` in `dag.py:89`) | `dag.py` uses constant (3 sites) | `profile.py:282` uses literal `2` | Medium |
| `attempt_receipt_schema` | YES (`ATTEMPT_RECEIPT_SCHEMA = 2` in `_runstate.py:15`) | `_runstate.py` uses constant (2 sites) | `profile.py:267` literal `2`; `inspect.py:86` derives from manifest schema | Medium |

**Evidence**:
- `CACHE_SCHEMA`, `WORKFLOW_PROFILE_SCHEMA`, `PROMPT_RESOLUTION_SCHEMA` are correctly centralized constants
- CHANGELOG shows these fields bump almost every release
- `profile.py` reads all four with literals, indicating it was written to a snapshot rather than to the contracts

**Fix**:
1. Add constants to appropriate modules:
   - `RUN_SIDECAR_SCHEMA = 2` in `_execution.py` (where it's written)
   - `FAILURE_SCHEMA = 2` in `failures.py` or `_runstate.py` (neutral ground)
2. Replace all 10+ literal `2` sites with the constants
3. Add comment at each constant: `# Increment on schema break; update CHANGELOG.md same commit`
4. No cache family change (these are already schema 2; we're fixing the management, not the value)

**Test verification**: Mutation test — change one constant to `999`, verify readers catch the mismatch with `WorkflowProfileError` or `RunManifestError`.

**Estimated changes**: ~15 lines across 5 files.

---

### 2.2 `ExplainResult.legacy` contradicts fail-closed contract

**Problem**: `dag.py:2086` returns `status="legacy"` when sidecar lacks `key_components`, with `__str__` suggesting "重跑一次即可获得解释" (retry to get explanation).

But:
- `_execution.py:189` always writes `key_components` since schema 2
- `dag.py:1837` validates `run_sidecar_schema == 2`
- A sidecar missing `key_components` is **corrupted**, not "legacy"
- AGENTS.md: "receipt、manifest、candidate、artifact 或 blob 摘要损坏一律 fail closed，不按 miss 重跑"

The only path to this branch is `tests/test_dag_plan_explain.py:90` manually popping the field to fabricate corruption.

**Fix**:
1. Replace `return ExplainResult("legacy", [], {})` with:
   ```python
   raise ValueError(
       f"Sidecar for {name!r} in run {run_id!r} is missing key_components; "
       "corrupted sidecar cannot be explained"
   )
   ```
2. Update test `test_explain_reports_unknown_no_entry_and_legacy_without_guessing`:
   - Rename to `test_explain_fails_closed_on_corrupted_sidecar`
   - Change `assert first.explain("source", run.run_id).status == "legacy"` to `with pytest.raises(ValueError, match="corrupted sidecar")`
3. Remove `if self.status == "legacy":` branch from `ExplainResult.__str__` (lines 203-204)
4. Update `docs/adoption.md:1287` — remove "`legacy`" from the status list
5. Update `docs/api.md:171` — `ExplainResult` only has `hit`/`miss`/`unknown`/`no_entry`

**Estimated changes**: 1 exception raise, 1 test rename + assertion change, 3 doc lines, 2 removed lines. Total ~8 lines net change.

---

## 3. Hard-cut Legacy Schema-1 Compatibility (Priority 1)

### 3.1 Schema-1 run read path is unreachable dead code

**Background**: 
- `RUN_MANIFEST_SCHEMA = 2` is the only write value (since 0.7)
- 0.6 runs used schema 1
- 0.6 L3 cache rotated in 0.7 (`CACHE_SCHEMA` 3→4), rotated again in 0.8 (4→6)
- **No schema-1 run can have usable cache in 0.8**

**Current state**: 5 modules contain schema-1 read branches:

| File | Lines | Branch | Behavior |
|------|-------|--------|----------|
| `profile.py` | 24-34, 355-381 | `if schema == 1:` + `_legacy_nodes()` | Returns `mode="legacy"`, `resolution_status="unavailable_legacy"` |
| `inspect.py` | 69, 80, 86, 114 | `in {1, 2}`, manifest→receipt schema derivation, `unavailable_legacy` | Allows schema-1 in trace/runs commands |
| `cli.py` | 500, 526, 575, 585 | `in {1, 2}`, `.get("run_status", "legacy")` | Prints "legacy" for missing status |
| `dag.py` | 1678 | Error message prose | "is legacy/read-only or has no valid 0.7 manifest" |

Plus 2 tests fabricating schema-1 runs:
- `tests/test_workflow_profile.py:216-228` (`test_legacy_profile_is_read_only_and_marks_resolution_unavailable`)
- `tests/test_cli.py:300-360` (`test_runs_show_and_trace_include_durable_attempt_state`) — uses schema 1 + `attempt_receipt_schema: 1` to test a 0.7 feature, should use schema 2

**Contract implications**: 3 contracts promise this behavior:
- `docs/contracts/workflow-profile.md:44-45` — "0.6/schema-1 run 可只读展示持久信息"
- `docs/contracts/retry-resume.md:25-26` — "schema-1/0.6 run 只读，不可 resume"
- `docs/contracts/checkpoint.md:24` — "应使用新 run；0.6/schema-1 run 只读不可恢复"

**Fix** (breaking change, but greenfield justifies it):

1. **Remove schema-1 read branches**:
   - `profile.py`: delete `if schema == 1:` block (lines 24-34) and `_legacy_nodes()` (355-381)
   - `profile.py:38`: change `if schema != 2:` to `if not isinstance(schema, int) or schema != 2:`
   - `inspect.py:69`: change `in {1, 2}` to `== 2`
   - `inspect.py:80,86,114`: remove schema-1 branches, require schema 2
   - `cli.py:500,575,585`: change `.get("run_status", "legacy")` to `.get("run_status", "unknown")`
   - `cli.py:526`: change `in {1, 2}` to `== 2`
   - `dag.py:1678`: "or has no valid manifest" (drop "legacy/read-only")

2. **Update tests**:
   - Delete `test_legacy_profile_is_read_only_and_marks_resolution_unavailable` entirely
   - In `test_runs_show_and_trace_include_durable_attempt_state`, change `"run_manifest_schema": 1` to `2`, change `"attempt_receipt_schema": 1` to `2`, remove assertion `assert shown["workflow_profile"]["resolution_status"] == "unavailable_legacy"`

3. **Update contracts** (same commit):
   - `workflow-profile.md:44-46`: delete lines 8-9 (schema-1 read-only promise)
   - `workflow-profile.md:62-63`: delete "破坏既有 schema-1 profile 读取时必须递增 `workflow_profile_schema`"
   - `retry-resume.md:25-26`: change "schema-1/0.6 run 只读，不可 resume" to "缺少 schema-2 manifest 的 run 不可 resume"
   - `checkpoint.md:24`: delete "0.6/schema-1 run 只读不可恢复"

4. **Update `CHANGELOG.md`** (same commit, under `[Unreleased]` → `变更`):
   ```markdown
   - **硬切**：移除 schema-1/0.6 run 的只读降级投影。0.8 greenfield 内不存在 0.6 artifacts
     （L3 cache 已换族两次），保留该路径只增加 `resolution_status` 与 `run_status` 的假取值。
     `profile.py`、`inspect.py` 与 `cli.py` 现在要求 `run_manifest_schema == 2`；旧 run 以
     "unsupported manifest" 失败，不再降级为 `unavailable_legacy`。删除
     `test_legacy_profile_is_read_only_and_marks_resolution_unavailable`；
     `workflow-profile.md`、`retry-resume.md` 与 `checkpoint.md` 同步更新契约承诺。
   ```

5. **Update README** (English and Chinese):
   - `README.md:135-136`: delete "0.6 runs remain inspectable as legacy profiles but cannot be resumed under the 0.7 manifest."
   - `README.zh-CN.md:112-113`: delete "0.6 run 在 0.7 中仍可作为 legacy profile 查看，不可 resume。"

**Estimated changes**: ~120 lines deleted (mostly `_legacy_nodes()` and one test), ~15 lines modified, 6 doc updates, 1 CHANGELOG entry.

**Commit message**: `Hard-cut: remove schema-1/0.6 legacy read path (greenfield)`

---

## 4. Documentation Drift from CLI Unification (Priority 2)

The `kigumi <command>` unification left 6 stale "two CLIs" references and one incorrect summary in `kigumi/docs.py`.

### 4.1 Fix `kigumi/docs.py` stale summary

**Current**: Line 64, `cli` entry summary is `"both CLIs: every command, flag, default and exit code"`

**Fix**: Change to `"unified kigumi CLI: all commands, flags, defaults and exit codes; optional standalone dag script"`

### 4.2 Update "两套 CLI" prose (6 sites)

| File | Line(s) | Current prose | Fix |
|------|---------|---------------|-----|
| `AGENTS.md` | 16 | "`dag.cli()` 负责已注册图的 check、plan、graph、explain、describe" | "`kigumi` 统一入口提供全部命令；图命令(`check`/`plan`/`graph`/`explain`/`describe`/`profile`/`resume`/`retry-resolve`)需要 `[tool.kigumi] dag_entry`。可选独立 `dag` 脚本走同一 dispatch。" |
| `AGENTS.md` | 34, 38 | "两套 CLI 分工" (2 occurrences) | "CLI 命令说明" |
| `docs/cli.md` | 49 | "两套 CLI 各自负责什么" | "`kigumi` 命令与可选 `dag` 脚本" |
| `docs/adoption.md` | 18 | "两套 CLI 分工（见 brief.md）" | "CLI 命令说明（见 cli.md 与 brief.md）" |
| `README.zh-CN.md` | 131 | "两套 CLI 怎么分工;全部命令、参数、默认值与有意义的退出码" | "`kigumi` 统一 CLI 与可选 `dag` 脚本;全部命令、参数、默认值与退出码" |
| `CHANGELOG.md` | 31, 54 | "两套 CLI 的完整分工"、"两套 CLI 的全部子命令" (entries documenting the 0.8.0 work) | Leave as-is (historical record of what 0.8.0 did) |

**Estimated changes**: 1 line in `docs.py`, ~6 lines across 5 doc files.

**Commit message**: `docs: update stale "two CLIs" prose after unification`

---

## 5. Repository Hygiene (Priority 3)

### 5.1 Empty `downloads/` directory

**Current**: Untracked, not gitignored, empty.

**Fix**: `rm -rf downloads`

### 5.2 Phantom gitignore entry

**Current**: `.gitignore` line 10 ignores `examples/layered_prompts/`, which does not exist.

**Context**: All 2 actual examples (`prompt_evolve`, `ticket_extract`) are tracked and ship with the repo.

**Fix**: Delete line 10 from `.gitignore`.

### 5.3 `uv.lock` tracking decision

**Current**: `.gitignore` ignores `uv.lock`, it is not tracked.

**Trade-off**:
- **Not tracking** (current): standard for libraries, avoids lock churn, trusts resolver
- **Tracking**: CI/dev get identical resolution, `uv sync` is deterministic

**Recommendation**: Keep untracked (library convention), but add comment to `.gitignore`:
```gitignore
# Lock is not tracked (library convention); application projects should track it
uv.lock
```

### 5.4 Orphaned `DEGRADATION.md` — fold into the contract it belongs to

**What the file is**: an honesty ledger. One entry, added by `44a1e82` (agent scan session
carry), in three parts — *should have run* (`KIGUMI_PI_LIVE=1` conformance against a real,
credentialed Pi 0.82.1, including a two-item `agent_scan` that persists and resumes one
explicit session), *ran instead* (deterministic fake-Pi RPC tests), *residual risk* (a real
Pi may expose a session format, persistence timing or extension interaction the fake does
not model). **The risk is still open — nobody has run that live test.**

So it must not be deleted: deleting it silently upgrades "unverified" to "verified". But
the root of the repo is the wrong home, and AGENTS.md is the wrong home too — AGENTS.md is
a *reference map for coding agents* ("read the contract before changing the implementation"),
not a place that tracks outstanding verification debt. Linking it there makes it discoverable
by exactly the audience least able to act on it, and it would go stale the moment a second
entry lands.

**The right home is the contract whose verification is incomplete.** `docs/contracts/agent-node.md`
already has a `## Verification / change policy` section naming the exact test files; a reader
who is about to touch `pi.py` or agent session semantics reads that section, and that is
precisely who needs to know the live leg was never run.

**Fix**:
1. Add to `docs/contracts/agent-node.md` under `## Verification / change policy`:
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
2. Delete `DEGRADATION.md` from the repository root (content preserved, not lost)
3. If a future degradation does not belong to any single contract, it goes in that release's
   CHANGELOG entry — not in a root-level ledger nobody links

### 5.5 This plan document itself

`GREENFIELD_CLEANUP_PLAN.md` is currently at the repo root, which is the same mistake as
`DEGRADATION.md`. `docs/reviews/` already holds exactly this genre (two dated design reviews
from 2026-07-13 and 2026-07-14). Move it to
`docs/reviews/2026-07-31-greenfield-cleanup.md` as part of Phase 5.

`docs/reviews/`' existing two files are historical records, not active docs — leave them.

**Estimated changes**: `rm -rf downloads`, 1 gitignore line deleted, 1 comment added,
~15 lines added to `agent-node.md`, `DEGRADATION.md` deleted, this plan moved.

**Commit message**: `chore: fold degradation record into agent-node contract, clean stray files`

---

## 6. Hard-cut: Remove `prompts=()` for LLM Prompts (Priority 1)

**Rationale** (from user discussion):

1. **Observability**: `PromptSpec` declares complete input surface at registration time — reviewable without entering node functions
2. **Prevent overfitting**: Imperative `ctx.render()` allows for-loops, if-branches, string concatenation inside nodes → coding agents will create 10 templates + 50 lines of prompt assembly logic
3. **Framework responsibility**: kigumi's design philosophy is deterministic, observable, L3-cacheable. Prompt construction logic scattered in node functions defeats this.
4. **Lineage**: Declarative `PromptSpec` gives framework full control: which variant selected, which materials injected, all tracked in sidecar

**Current state**:
- `prompts=("template",)` parameter on all `dag.node/map/scan/foreach` and `Subgraph.node/map/scan`
- `ctx.render(template_name, **slots)` for manual rendering
- Internally called `legacy_prompts` (9 call sites: `dag.py` 6×, `subgraph.py` 3×, `prompt.py` 2× validation)
- Actual usage: 3 `prompts=()` in tests/examples vs 22 `PromptSpec`

**Decision**: Remove `prompts=()` entirely. Only `prompt_specs=()` allowed for LLM input prompts.

### Changes (breaking, greenfield-justified)

1. **Remove `prompts` parameter**:
   - `dag.py`: delete `prompts` param from `node()`, `map()`, `scan()`, `foreach()` signatures (lines ~434, ~485, ~558, ~643)
   - `subgraph.py`: delete `prompts` param from `node()`, `map()`, `scan()` (3 sites)
   - Remove all `node_prompts = tuple(prompts)` assignments
   - Remove all `legacy_prompts=node_prompts` kwargs in validation calls

2. **Remove `ctx.render()` method**:
   - Delete `dag.py:342-354` (the `NodeContext.render()` method)
   - `load_template` and `render_template` remain as public exports for non-LLM output templates (reports, config files)

3. **Simplify `validate_prompt_specs()`**:
   - Remove `legacy_prompts` parameter from `prompt.py:304` and `:495`
   - Remove conflict check `prompt.py:320-324`
   - Remove `names = set(legacy_prompts)` logic `prompt.py:500`
   - Simplify docstring to only mention `PromptSpec`

4. **Close the declarative gap first: add `FileRef` material source**

   **This is a blocker discovered while validating the migration.** `PromptSpec` materials
   can only source from `InputRef` / `ParamRef` / `ItemRef` / `CarryRef` (`prompt.py:168`),
   all of which are known *before* the node body runs — by design, since resolution happens
   before the L3 lookup and before any side effect. There is **no way to inject content the
   node reads from a declared file inside its own body**.

   That is exactly the shape of `examples/ticket_extract`'s `extract` node:

   ```python
   raw_text = ctx.read_text(str(ticket["source"]))  # declared via files_fn
   prompt = ctx.render("extract", ticket=inject({"id": ticket["id"], "text": raw_text}))
   ```

   `ItemRef()` yields `{"id": ..., "source": ...}` — the *path*, not the text. And
   `ctx.params` returns a deep copy (`dag.py:277-279`), so runtime mutation is impossible;
   `ParamRef` is also validated against *declared* params at registration (`prompt.py:355`).
   So the naive migration does not work, and neither does any current combination of refs.

   The only alternative without a new ref is splitting into a `read` node that emits ticket
   text into its artifact plus an `extract` node with `InputRef` on it — but that puts raw
   ticket text into a downstream artifact, destroying the invariant the example exists to
   demonstrate (`"原文不进入下游 artifact"`, `pipeline.py:69`).

   **Resolution**: add `FileRef(path_from=...)` as a fourth material source that reads a file
   the node has *already declared* via `files=` / `files_fn=`. This is principled rather than
   an escape hatch:
   - No undeclared IO — it can only read what the node declared, so the raw-io guard is unaffected
   - No new cache semantics — declared file bytes are *already* an L3 key component, so reading
     them at resolution time changes nothing about the key
   - Keeps the whole prompt surface declarative and reviewable, which is the point of the cut

   Ship `FileRef` in the same commit, before deleting `ctx.render()`. Write the failing test
   first (RED): a map node whose prompt material comes from a per-item declared file.

5. **Migrate tests, example and scaffold**:
   - `tests/test_dag_cache_keys.py` — replace `prompts=()` with `PromptSpec`
   - `examples/ticket_extract/pipeline.py` — `extract` uses `PromptSpec` with
     `PromptMaterial(slot="ticket", source=FileRef(...))`; `report` uses
     `PromptMaterial(slot="stats", source=InputRef("stats"))`. Both keep the
     "raw text never reaches a downstream artifact" property.
   - `kigumi/cli.py:66` — `kigumi init` scaffold writes `prompts=()`; change to `prompt_specs=()`

6. **Update contracts** (same commit):
   - `docs/contracts/prompt-resolution.md:36-40` — delete lines about "legacy `prompts=()` 与 PromptSpec name 不得冲突" and "既有 `prompts=()` 与 `ctx.call()` 和字符串 Agent instruction 继续可用"
   - Rewrite to: "节点只能通过 `prompt_specs=()` 声明 Prompt；节点内 `ctx.resolve_prompt()` 返回 managed `ResolvedPrompt`。字符串直接传给 `ctx.call()` 标记为 unmanaged，应当豁免或迁移到 PromptSpec。"

7. **Update documentation**:
   - `docs/adoption.md:186-252` — delete "既有 `prompts=()` 与 `ctx.render()` 仍保留"
   - Rewrite section to only show `PromptSpec` usage
   - Add note: "`load_template` / `render_template` 仍作为公开 API 存在，用于生成非 LLM 输出（Markdown 报告、配置文件）；不应用于构造传给 `ctx.call()` 的 prompt。"
   - `docs/brief.md` — check if it mentions `ctx.render()` in the "Do not reimplement" table; if so, replace with "`PromptSpec` / `PromptAxis` / `PromptLayer`"

8. **Update `CHANGELOG.md`** (same commit — `新增` for FileRef, `变更` for the cut):
   ```markdown
   ### 新增

   - 新增 `FileRef` 作为 `PromptMaterial` 的第四种来源：读取节点已通过 `files=` /
     `files_fn=` 声明的文件内容。声明文件字节本就是 L3 键成分，因此不改变缓存语义，
     也不绕过 raw-io 守卫；它补上了"节点内读文件再拼 prompt"这唯一一处声明式缺口。

   ### 变更

   - **硬切**：移除 `prompts=()` 参数与 `ctx.render()` 方法。节点现在只能通过
     `prompt_specs=()` 声明 Prompt 输入面，以获得注册期可见的完整输入面、managed
     lineage 与 selected-only cache；命令式拼装无法被框架观测，也让 prompt 分支
     在节点函数里蔓延。`load_template` / `render_template` 仍是公开 API，用于生成
     非 LLM 输出（报告、配置文件）。迁移示例见 `examples/ticket_extract`。
     这是 greenfield API 收敛，无兼容路径与 deprecation 期。
   ```

### Estimated changes
- ~80 lines deleted (parameter removal, `ctx.render()` deletion, validation simplification)
- ~50 lines modified (3 test migrations, 1 example rewrite, contract/doc updates)
- Net: **~130 lines change** across 10 files

### Test verification
- All 499 tests must pass after migration
- Verify `examples/ticket_extract` runs end-to-end with `PromptSpec`
- Check that `kigumi init` scaffold compiles

### Commit message
`Hard-cut: remove prompts=() and ctx.render() for LLM input (greenfield API)`

**This is now Priority 1** — same tier as the schema-1 hard-cut, because both are removing dead/legacy paths in greenfield.

---

## 7. Out of Scope (Noted, Not Fixing)

These were observed but are outside this cleanup's scope:

### 7.1 `kigumi/dag.py` size (4252 lines, 27% of package)

Combines registration, execution, retry/resume, key derivation, CLI parsing, and view rendering. Refactoring would be a multi-commit arc with its own risks; leave for a dedicated effort.

### 7.2 `kigumi/docs.py` vs `kigumi/docs/` namespace collision

Works (regular module beats namespace package), but fragile. Renaming `kigumi/docs.py` → `kigumi/shipped_docs.py` or `kigumi/doc_reader.py` would avoid the collision. Low priority.

### 7.3 `active_effect_schema` and `adapter_schema` have no constants

Both are written as literal `1` and `3` respectively, but each only appears in 1-2 files (not scattered like the Section 2 cases). Add constants if/when they bump.

---

## 8. Execution Plan

### Phase 1: Critical bugs (1 commit)
- [x] Add `RUN_SIDECAR_SCHEMA`, `FAILURE_SCHEMA` constants
- [x] Replace 10+ literal `2` sites with constants
- [x] Add mutation test verifying mismatch detection
- [x] Run `uv run pytest -q && uv run ruff check .`
- Commit: `fix: centralize run_sidecar_schema and failure_schema constants`

### Phase 2: Fail-closed on corrupted sidecar (1 commit)
- [x] Replace `ExplainResult("legacy", ...)` with `raise ValueError`
- [x] Update test to expect exception
- [x] Remove `.status == "legacy"` branch from `__str__`
- [x] Update `docs/adoption.md` and `docs/api.md`
- [x] Run tests
- Commit: `fix: fail closed on corrupted sidecar (ExplainResult.legacy → exception)`

### Phase 3: Hard-cut schema-1 legacy path (1 commit, large)
- [x] Delete `profile.py` schema-1 branches and `_legacy_nodes()`
- [x] Update `inspect.py`, `cli.py`, `dag.py` to require schema 2
- [x] Delete `test_legacy_profile_is_read_only_and_marks_resolution_unavailable`
- [x] Fix `test_runs_show_and_trace_include_durable_attempt_state` to use schema 2
- [x] Update 3 contracts (`workflow-profile.md`, `retry-resume.md`, `checkpoint.md`)
- [x] Update both READMEs (remove 0.6 legacy prose)
- [x] Add `CHANGELOG.md` entry under `[Unreleased]` → `变更`
- [x] Run full test suite
- Commit: `Hard-cut: remove schema-1/0.6 legacy read path (greenfield)`

### Phase 3b: Hard-cut prompts=() for LLM input (1 commit, large)
- [x] Remove `prompts` parameter from all `dag.node/map/scan/foreach` and `Subgraph.node/map/scan`
- [x] Delete `ctx.render()` method from `dag.py`
- [x] Simplify `validate_prompt_specs()` (remove `legacy_prompts` param and conflict check)
- [x] Migrate 3 test uses + 1 example (`examples/ticket_extract`) to `PromptSpec`
- [x] Update `kigumi init` scaffold to use `prompt_specs=()`
- [x] Update `docs/contracts/prompt-resolution.md` (remove legacy prompts prose)
- [x] Update `docs/adoption.md` (only show PromptSpec, note load_template for non-LLM output)
- [x] Check `docs/brief.md` for ctx.render mentions
- [x] Add `CHANGELOG.md` entry under `[Unreleased]` → `变更`
- [x] Run full test suite + verify ticket_extract example runs
- Commit: `Hard-cut: remove prompts=() and ctx.render() for LLM input (greenfield API)`

### Phase 4: Documentation unification (1 commit)
- [x] Fix `kigumi/docs.py:64` summary
- [x] Update 6 "两套 CLI" prose sites
- [x] Run `uv run pytest tests/test_shipped_docs.py -v` (brief test will catch stale prose)
- Commit: `docs: update stale "two CLIs" prose after unification`

### Phase 5: Repository hygiene (1 commit)
- [x] Confirm no `downloads/` directory remains
- [x] Delete `examples/layered_prompts/` from `.gitignore`
- [x] Add comment to `uv.lock` gitignore line recording the deliberate library-project choice
- [x] Fold `DEGRADATION.md` into `docs/contracts/agent-node.md` (see 5.4), delete the root file
- [x] Move this plan to `docs/reviews/2026-07-31-greenfield-cleanup.md` (matches the two existing review docs)
- [x] Run `git status` to verify clean state
- Commit: `chore: fold degradation record into agent-node contract, clean stray files`

### Pre-commit gate (every phase)
```bash
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
```

### Post-completion verification
```bash
uv run pytest -q  # all tests green
uv run ruff check . && uv run ruff format --check .  # clean
uv build
uv run python scripts/verify_dist.py --expected-version 0.9.0
uv run python scripts/smoke_installed.py
git log --oneline -6  # 6 commits (Phases 1, 2, 3, 3b, 4, 5)
```

---

## 9. Risk Assessment

| Change | Risk Level | Mitigation |
|--------|-----------|------------|
| Schema constant centralization | **Low** | Mutation test + existing test coverage |
| ExplainResult.legacy → exception | **Low** | Only 1 test fabricates corruption; real runs never hit this |
| Schema-1 hard-cut | **Medium** | 2 tests deleted/modified; contracts updated same commit; 0.6 artifacts physically cannot exist |
| prompts=() hard-cut | **Medium** | 3 test migrations + 1 example rewrite; forces PromptSpec adoption; no escape hatch |
| Documentation updates | **Minimal** | Pure prose, verified by `test_shipped_docs.py` |
| Repository hygiene | **Minimal** | File deletions, no code change |

**Overall risk**: Low-Medium. The two hard-cuts (schema-1 and prompts=) are the largest changes, but both remove code paths that shouldn't exist in greenfield. The prompts=() removal forces a migration, but it aligns with the framework's observability goals. Test coverage is strong (0.81 test:impl ratio, 12686 test lines).

---

## 10. Post-Cleanup State

After executing this plan:
- **0 schema version literals** scattered across readers/writers (all centralized)
- **0 legacy compatibility paths** for schema-1 runs
- **0 imperative prompt construction** in nodes (only declarative `PromptSpec`)
- **0 contract contradictions** (fail-closed applies to all corrupted artifacts)
- **Unified CLI documentation** (no "two CLIs" confusion)
- **Clean repository** (no phantom gitignore entries, no orphaned docs, no empty dirs)
- **Single prompt pattern** (only `PromptSpec` for LLM input; `load_template`/`render_template` for non-LLM output)

Net change estimate: **~230 lines deleted**, **~130 lines added** (`FileRef` plus its test),
**~90 lines modified**, across ~20 files.

---

## 11. Decisions Recorded (no open questions)

Everything the plan needed decided is decided here; nothing is deferred back to the user.

| Question | Decision | Rationale |
| --- | --- | --- |
| `prompts=()` future | Hard-cut, no deprecation period | Greenfield, no external users; declarative-only keeps the prompt surface reviewable |
| Migration blocker (node reads its own declared file) | Add `FileRef` material source in the same commit | Only remaining declarative gap; declared file bytes are already an L3 key component |
| `DEGRADATION.md` | Fold into `docs/contracts/agent-node.md`, delete the root file | The risk is still open, so it can't be dropped; it belongs with the contract whose verification is incomplete, not in AGENTS.md's reference map |
| This plan's location | `docs/reviews/2026-07-31-greenfield-cleanup.md` | Matches the two existing dated review documents |
| Execution scope | All 6 phases now, in order | Later phases delete code the earlier ones would otherwise touch twice |
| `uv.lock` | Stay untracked, add a comment recording why | Library project; the comment turns a silent default into a deliberate choice |

---

**End of plan.**
