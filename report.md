## What changed

- `kigumi/artifacts.py` — `sha256_file()` now delegates bounded hashing to the shared
  `digest_open_file()` boundary, including the post-read descriptor identity check.
- `tests/test_artifacts.py` — added a regression test that truncates the file after the first
  hash chunk and requires a fail-closed `ValueError`.
- `CHANGELOG.md` — documented the public hashing-integrity fix and corrected the public
  `write_artifact()` atomicity promise.
- `kigumi/artifacts.py` — narrowed `write_artifact()`'s docstring to state that the artifact
  and metadata sidecar are separate atomic replacements, not a transaction.

## RED proof

Defect 1 was behavior-changing, so the regression test was run before the implementation.
The project-base invocation initially could not spawn pytest because the declared `dev` extra
was not installed; the same test then ran with the project-declared `--extra dev` environment:

```text
$ uv run --extra dev pytest -q tests/test_artifacts.py::test_sha256_file_rejects_truncation_during_hash
F                                                                        [100%]
=================================== FAILURES ===================================
_______________ test_sha256_file_rejects_truncation_during_hash ________________
...
E       Failed: DID NOT RAISE ValueError
...
1 failed in 0.02s
```

Defect 2 is documentation-only, so RED is N/A by design; no behavior test was required for
that commit.

## Gate output

The required commands were run in order before each functional commit. Final gate tails:

```text
$ uv run pytest -q
...................... [ 92%]
........................................................................ [ 98%]
.ss.....................                                                 [100%]
1242 passed, 6 skipped in 64.99s (0:01:04)

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
164 files already formatted
```

The first functional commit's full gate was also green: `1242 passed, 6 skipped in 99.10s`.

## Commit

```text
ca56a7644e392d0d4079cce807709a55922c9a4d
Artifacts: Verify file identity after hashing

- Reuse the shared descriptor digest boundary so sha256_file rejects changes during a read.
- Add a regression test for truncation after the first hash chunk and document the fail-closed behavior.
- RED-verified; tests: 1242 passed, 6 skipped.
```

```text
3ac06f0a0ebded0cd12f86cdae909834df8e4a58
Artifacts: Clarify sidecar atomicity

- State that write_artifact replaces the artifact and metadata sidecar in separate atomic writes.
- Document that the pair is not a transaction and a crash can leave the artifact newer than its sidecar.
- Documentation-only correction; RED test not applicable; tests: 1242 passed, 6 skipped.
```

## Notes

- `docs/contracts/determinism.md` was checked. Its “Source of truth” clause states that
  `kigumi.artifacts.canonical_json()` is the sole JSON serializer and `kigumi.artifacts.sha()`
  is the sole hash entry point; invariant 1 repeats that `artifacts.sha` is the unique hash
  entry. The fix reuses the shared safe digest boundary and does not change canonical bytes,
  digest format, or cache-key components.
- The existing import cycle was not widened: `artifacts.py` already imported `_safe_io`, and
  `digest_open_file` is imported from that same module. `_safe_io`'s existing lazy import of
  `artifacts` remains unchanged.
- The `sha256_file()` behavior is user-facing because it is re-exported by `kigumi`; the new
  fail-closed race behavior therefore received a `CHANGELOG.md` entry. `write_artifact()` is
  also public, so its corrected promise received a changelog entry even though behavior did
  not change.
- Deliberately did not implement pair-level transactions, change `docs/contracts/retry-resume.md`,
  or touch `kigumi/prompt.py` or `kigumi/repair.py`.

Final `git status -sb`:

```text
## fix/artifacts-hash-docstring
```

Final `git diff --name-only`:

```text
(empty; clean tree)
```
