# Work Order Report

## What changed

- `kigumi/dag.py`: changed only the three `Dag.recover()` integrity branches for the
  schema-2 manifest, target bindings, and force bindings from `ValueError` to
  `RunManifestError`, preserving message text; trusted status and graph-target
  preconditions remain `ValueError`.
- `tests/test_recovery.py`: added RED-verified coverage for all three integrity branches and
  explicit `ValueError` coverage for the trusted status and unregistered-target branches.
- `CHANGELOG.md`: documented the public exception-type change in Chinese under `[Unreleased]`.

## RED proof

The new tests were added before the implementation change. The focused run failed as expected:

```text
FFF.                                                                     [100%]
...
E           ValueError: Run 'untrusted-run_manifest_schema' has no valid schema-2 manifest
...
E           ValueError: Run 'untrusted-targets' has invalid target bindings
...
E           ValueError: Run 'untrusted-force' has invalid force bindings
=========================== short test summary info ============================
FAILED tests/test_recovery.py::test_recover_rejects_untrusted_manifest_fields_with_run_manifest_error[run_manifest_schema-1-no valid schema-2 manifest]
FAILED tests/test_recovery.py::test_recover_rejects_untrusted_manifest_fields_with_run_manifest_error[targets-value1-invalid target bindings]
FAILED tests/test_recovery.py::test_recover_rejects_untrusted_manifest_fields_with_run_manifest_error[force-value2-invalid force bindings]
3 failed, 1 passed, 8 deselected in 0.64s
```

## Gate output

Commands ran in the required order:

```text
$ uv run pytest -q
.................................................. [ 86%]
........................................................................ [ 92%]
........................................................................ [ 97%]
....ss.....................                                              [100%]
1245 passed, 6 skipped in 103.21s (0:01:43)

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
164 files already formatted
```

## Commit

```text
62c14f1d6b88e5327c4f9937cc641618e181725a
Recover: Classify untrusted manifest errors consistently

- Raise RunManifestError for schema-2 and binding integrity failures in Dag.recover
- Preserve ValueError for trusted terminal-state and graph-target preconditions
- Document the RuntimeError-based public exception change and add RED-verified regression coverage
- tests: 1245 passed, 6 skipped
```

## Notes

- No existing test asserting one of the three changed `recover()` branches needed updating. The
  existing missing/unsafe-manifest and trusted-status tests exercise separate branches and were
  deliberately left unchanged.
- `RunManifestError` is already imported by `kigumi/dag.py`; no shared validator was extracted.
- `CACHE-FAMILY-BREAK: no`; no cache key, canonical bytes, or durable schema changed.
- `kigumi/prompt.py` and `kigumi/repair.py` were not touched.
- The initial `uv run pytest` setup attempt found no pytest executable in the base environment;
  `uv sync --extra dev` installed the declared development tools, after which the required RED
  and gate commands ran successfully.

Final repository check:

```text
$ git status -sb
## fix/recover-error-types
$ git diff --name-only
(no output)
```
