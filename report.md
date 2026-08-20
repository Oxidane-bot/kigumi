# Documentation drift work order report

## What changed

- `DESIGN.md` — aligned L1/L3 cache prose with the active contracts, documented the non-default `ResponseSpec` identity and implemented file references, removed the unsupported `clips` sidecar promise, and narrowed the fresh Agent-session determinism claim.
- `AGENTS.md` — listed all nine registered graph commands and added `docs/recovery.md` to the document map.
- `docs/brief.md` — listed all nine registered graph commands in the abbreviated boundary summary.
- `kigumi/__init__.py` — added `recovery` to the shipped-document list in the module docstring.
- `README.md` — added `docs/recovery.md` to the reader-facing documentation map.
- `README.zh-CN.md` — added `docs/recovery.md` and changed the review entry to the complete reviews directory, using the same GitHub directory target as the English README.
- `workorders/INDEX.md` — introduced `COMPLETED` and applied it to the ten verified merged workorders while retaining the three `NEEDS-DESIGN` statuses.
- `report.md` — recorded the required verification and commit report.

## RED proof

N/A. The work order explicitly classifies this as documentation-only; no behavior changed, so no RED test was required.

## Gate output

Commands were run in the required order after installing the declared development extra with `uv sync --extra dev`.

`uv run pytest -q`

```text
1241 passed, 6 skipped in 66.77s (0:01:06)
```

`uv run ruff check .`

```text
All checks passed!
```

`uv run ruff format --check .`

```text
164 files already formatted
```

## Commit

`b90c063e0ec9a16897591eebbd9e5f02819767fb`

```text
docs: correct DESIGN cache and determinism prose

- Align L1 and L3 cache descriptions with the active contracts and implementation.
- Document implemented file references and remove the unsupported clips sidecar promise.
- Narrow determinism wording for fresh Agent session execution.
- Documentation-only change; no RED test required.
```

`32eaa9028379bf12a1ee5a1846b8910233a7499e`

```text
docs: align command and page indexes

- List all nine registered graph commands in the agent-facing summaries.
- Add recovery to the shipped-doc and README indexes.
- Point the Chinese README at the complete reviews directory.
- Documentation-only change; no RED test required.
```

`ed4aa41874487e9eae2a4cf7af7d111c29c19a3f`

```text
docs: mark completed workorders

- Add a COMPLETED status and apply it to the ten verified merged workorders.
- Keep WO-04, WO-06, and WO-08 at NEEDS-DESIGN pending acceptance audits.
- Verification: each listed merge commit is an ancestor of HEAD and matches its WO title.
- Documentation-only change; no RED test required.
```

## Notes

- The three implementation commits were grouped as DESIGN accuracy, command/page index consistency, and workorder status accuracy.
- Before changing statuses, each named merge commit was checked with `git show --format=fuller --stat --summary` and its merge title, then with `git merge-base --is-ancestor <commit> HEAD` (all returned 0): WO-01 `95b4954`, WO-02 `9cfff16`, WO-03 `01694fb`, WO-05 `0789315`, WO-07 `281c9ba`, WO-09 `ffc07b1`, WO-10 `b07738c`, WO-11 `b8f0850`, WO-12 `09432e7`, and WO-16 `e736bd0`.
- The normative cache-key and determinism contracts, the `LLMCaller` response-spec key path, `Dag._key_components()`, the nine-command registry, and the absence of `clips` in `kigumi/` were verified before editing.
- No `CHANGELOG.md`, contract page, implementation code, `kigumi/prompt.py`, or `kigumi/repair.py` was changed. There are no unresolved blockers or risks for this documentation-only work.
- The first pytest attempt exposed that the dev extra was not installed; `uv sync --extra dev` resolved that environment issue. The first full test run then caught the relative directory link in the Chinese README; it was corrected to the absolute GitHub directory link and the final gate passed.

## Final working tree

`git status -sb`

```text
## fix/doc-drift
```

`git diff --name-only`

```text
(no output)
```
