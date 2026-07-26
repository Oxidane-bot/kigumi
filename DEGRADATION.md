# Degradation record

## 2026-07-26 — Pi 0.82.1 session carry release verification

- Should have run: `KIGUMI_PI_LIVE=1` conformance with an installed, credentialed Pi 0.82.1,
  including a two-item `Dag.agent_scan` that persists and resumes one explicit session.
- Ran instead: deterministic fake-Pi RPC tests covering missing-file creation, explicit
  `--session`, header cwd normalization, blob carry/cache replay, size limits and failure
  behavior; Pi 0.82.1 `SessionManager` and RPC persistence paths were also inspected.
- Residual risk: an installed Pi/provider combination may expose a runtime-only session format,
  persistence timing or extension interaction not represented by the fake process. Run
  `tests/test_pi_live.py::test_real_pi_rpc_conformance` with the documented environment before
  treating live Pi session carry as provider-conformant.
