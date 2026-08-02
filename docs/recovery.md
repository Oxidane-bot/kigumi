# Terminal-state recovery

`Dag.resume()` continues a run that is pending retry, approval, or a verified
crash. A run whose manifest is terminally `failed` requires an explicit recovery
decision instead of deleting its run directory.

```python
receipt = dag.recover(
    run_id="run-0042",
    target="transcode",
    from_attempt=3,
    decision="retry_after_external_check",
    reason="ffmpeg 7.1 was installed and the failing scene passed validation",
    evidence=["validation-report.md", "https://tickets.example/PROJ-123"],
)
dag.resume("run-0042")
```

The two retry decisions both queue one new attempt. `retry_not_started` records
that the failed operation did not cross its external side-effect boundary;
`retry_after_external_check` records that an operator verified an external fix.
`fail` records a final operator verdict and does not queue work. Recovery never
changes cache-key components.

`recover()` accepts only a terminal failed run and the target's current failed
attempt. It raises `ValueError` for a missing run, a non-terminal run, an
unknown target, or an attempt mismatch.

Each decision is written to
`artifacts/runs/<run_id>/recovery-<timestamp>.json`:

```json
{
  "recovery_time": "2026-08-02T15:30:00.000000Z",
  "from_attempt": 3,
  "to_attempt": 4,
  "decision": "retry_after_external_check",
  "reason": "ffmpeg 7.1 was installed and the failing scene passed validation",
  "evidence_refs": ["validation-report.md", "https://tickets.example/PROJ-123"],
  "recovered_by": "hongyu"
}
```

`recovered_by` reads `KIGUMI_RECOVERED_BY`, then `USER`, then the process owner.
It is an unverified convenience label: the environment variables are freely
settable, and the receipt does not record which source supplied the value. Treat
it as a hint about who ran the recovery, not as an attestation.

`recover()` takes no lock. It rejects a run that is still `running`, so it cannot
race an executing run, but two concurrent `recover()` calls against the same
terminally failed run are not serialised.

Attempt receipts are append-only across recovery. Completed nodes from the
failed run remain run-local evidence and are revalidated as inherited nodes;
the failed target and its downstream dependents are the work eligible to run on
the new attempt. Existing attempt files and recovery receipts are never removed
or rewritten by recovery.
