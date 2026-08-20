## What changed

- `kigumi/enforce.py` — removed four unreachable boolean projections from `_LoopCallVisitor`; the live `*_verdict` methods and both `_visit_defaults` methods were left unchanged.

## RED proof

N/A. This work order specifies a pure dead-code deletion, so no behavior-changing test was required before the edit.

## Gate output

- `uv run pytest -q`

  `1241 passed, 6 skipped in 100.46s (0:01:40)`

- `uv run ruff check .`

  `All checks passed!`

- `uv run ruff format --check .`

  `164 files already formatted`

## Commit

Pending commit.

## Notes

- Exact AST reachability scan across `kigumi/` and `tests/` found one definition and no attribute read, call, or string-reflection reference for each target name. The similarly named live methods are the four `*_verdict` methods and remain referenced.
- The contract search covered all `docs/contracts/*.md`; only `docs/contracts/guards.md` mentions `GuardVerdict`, requiring stable finding `rule`/`verdict`/`waived` semantics and not these predicate methods. The `getattr` fence was rechecked; occurrences are analysis subjects, and the relevant analyzers are `ast.NodeVisitor` subclasses whose dispatch names are `visit_<Type>`.
- The initial `uv run pytest -q` could not spawn because the dev tools were not installed. `uv sync --extra dev` installed the project-declared test and lint tools; this made no repository source change.
- Deliberately did not touch `kigumi/prompt.py`, `kigumi/repair.py`, `CHANGELOG.md`, or the two `_visit_defaults` methods.

Final `git status -sb` and `git diff --name-only` will be recorded after commit.
