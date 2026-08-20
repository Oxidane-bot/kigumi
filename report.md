# Work Order Report

## What changed

- `tests/test_runstate_integrity.py`: retargeted the symlinked-final-file, FIFO, and symlinked-parent test to `store._read_owned_json`, with the parent alias inside the owned run tree.
- `kigumi/_runstate.py`: removed the unreferenced `_read_json_bytes_safe`, `_read_json_safe`, `_parse_error`, and `_integrity_error` methods; retained the live `_owned_*` reader family.

## RED proof

The original dead-reader test passed before retargeting:

```text
.                                                                        [100%]
1 passed in 0.06s
```

The retargeted test also passed before the production deletion:

```text
.                                                                        [100%]
1 passed in 0.03s
```

For the required load-bearing mutation, `_read_owned_bytes` was temporarily changed to open the final entry without `O_NOFOLLOW` or the regular-file check. The retargeted test failed at the final-file symlink assertion:

```text
F                                                                        [100%]
_____ test_durable_json_read_rejects_symlinked_parent_final_file_and_fifo ______
E       AssertionError: assert {'target': 'work'} is None
=========================== short test summary info ============================
FAILED tests/test_runstate_integrity.py::test_durable_json_read_rejects_symlinked_parent_final_file_and_fifo
1 failed in 0.03s
```

The temporary mutation was reverted before either product commit.

## Gate output

Commands ran in the required order:

```text
$ uv run pytest -q
................................................................... [ 98%]
ss.....................                                                  [100%]
1241 passed, 6 skipped in 102.97s (0:01:42)

$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
164 files already formatted
```

## Commit

```text
c773975d876b0fccd3752b3b6569ae0a4aeab2c7
Test: exercise owned durable JSON reader

- Retarget the symlink, FIFO, and parent-directory integrity test to AttemptStore._read_owned_json.
- Keep the attack shapes inside the owned run boundary so the live descriptor binding is covered.
- RED-verified with a temporary no-follow mutation; targeted test passes after restoration.

7ce25b3c7036f3e31a17c757f7483292dc00e5b4
Remove dead durable JSON readers

- Delete the unreferenced plain-SecureDirectory JSON reader and its parse-error helpers.
- Keep the descriptor-bound _read_owned_* family as the sole AttemptStore durable JSON reader path.
- RED-verified the live integrity test mutation; tests: 1241 passed, 6 skipped.
```

## Notes

- The required reachability grep now reports only `_owned_integrity_error` references and unrelated names containing the generic `integrity_error` substring; none of the four deleted methods remains.
- The full `docs/contracts/` fence search found no clause requiring a second independent reader or a plain `SecureDirectory` durable JSON read path.
- `docs/contracts/cache-key.md` and `kigumi/dag.py` confirm that the cache-key source digest covers `prompt.py` and `repair.py`, not `_runstate.py`; no `CHANGELOG.md` update was needed. Neither prohibited file was touched.
- The first exact `uv run pytest -q <target>` invocation found no pytest executable in the base environment. `uv run --extra dev` bootstrapped the declared development tools; all required gate commands then ran exactly as specified and passed.

Final repository check:

```text
$ git status -sb
## fix/runstate-dead-reader
$ git diff --name-only
(no output)
```
