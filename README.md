<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/Oxidane-bot/kigumi/master/docs/assets/kigumi-logo.png">
    <img src="https://raw.githubusercontent.com/Oxidane-bot/kigumi/master/docs/assets/kigumi-logo-light.png" alt="kigumi logo" width="220">
  </picture>
</p>

# kigumi (木組)

English | [中文](https://github.com/Oxidane-bot/kigumi/blob/master/README.zh-CN.md)

Nail-free interlocking joinery. The load-bearing structural layer for LLM
content pipelines — connecting your project (the roof) to the model (the
pillars) through precise joints: output that does not fit the mortise gets
sent back for rework.

A foundation for building LLM pipelines with coding agents:

- **Injection and assembly**: a single entry point for material injection,
  strict template rendering, format sections auto-generated from schemas
- **Layered Prompt declarations**: finite selector axes, fixed fragments and
  fenced runtime materials resolve before cache lookup, with selected-only L3
  caching and content-free lineage
- **Repair loop**: failed validation turns into corrective instructions,
  model context is preserved, retries are bounded, lessons are locked in
- **Deterministic replay**: content-addressed caching — same input,
  byte-identical output
- **DAG orchestration** (optional): explicit node/item cache policy, static
  reusable subgraphs, dynamic map/scan, owned materialized outputs, human
  checkpoints, durable retry/resume, and run diffs
- **External Agent nodes and serial scans**: provider-neutral staged execution with captured
  attachments, exact publication, ordinary DAG caching, content-addressed
  `AgentSpec` capsules, global cross-process capacity, evidence retention
  policies, and a native, exactly versioned Pi RPC adapter with explicit
  blob-backed session carry
- **Typed failures and explicit recovery**: shared provider failure facts,
  deterministic retry schedules, persisted attempt receipts, and fail-closed
  handling of ambiguous side effects
- **Workflow profiles**: one canonical static/runtime IR for Prompt-aware
  Mermaid, Markdown, JSON, `describe`, trace, and run inspection
- **Experiment subjects**: one isolated evidence grid for functions, callers,
  ordinary workflows, and Agent-backed DAGs—without automatic winner selection
- **Four guard rings**: registration-time refusal plus three outer rings
  (`kigumi check` / pytest auto-collection / git hooks), so the rules enforce
  themselves

## Quick start

```python
from pathlib import Path

from pydantic import BaseModel

from kigumi import LiteLLMTransport, LLMCaller, call_validated


class Verdict(BaseModel):
    score: int
    reason: str


transport = LiteLLMTransport(aliases={"default": "anthropic/claude-sonnet-5"})
caller = LLMCaller(transport, cache_dir=Path("artifacts/_llm"), seed=20260713)

verdict = call_validated(caller, "Score this opening scene and explain why: ...", Verdict)
```

`call_validated` automatically appends a format section generated from
`Verdict`; a response that does not fit is sent back with the validation
errors for a bounded number of retries (2 by default). The whole exchange
lands in a content-addressed cache, so the same input replays byte-for-byte
with no further API cost.

## Status

0.10.1, API not frozen. The Agent boundary is intentionally an execution adapter,
not an autonomous factory or optimizer.

The retained `Candidate`, `EvolveResult`, and `evolve_prompt` imports form an
experimental, content-only recipe for evolving plain prompt strings. It is not a
DAG/Agent optimizer, durable run recovery, or an unbiased generalization estimator,
and it never promotes a candidate automatically. For evidence about functions,
callers, DAGs, or Agents, use `bench` with `FunctionSubject`, `CallerSubject`,
`DagSubject`, or `AgentSubject`. Adopting a candidate is caller-owned: (1) the
caller/human reviews `result.best`; (2) the caller manually writes the approved
text to `prompts/*.md`; and (3) the project references that existing file with
`PromptRef` or composes it with `PromptSpec`, which only declares composition.
There is no promotion API and no automatic write. Validation-feedback isolation
applies only when the train and validation sets are content-disjoint; callers
should validate that before running because the framework does not enforce
disjointness at runtime. The recipe still provides bounded metric evaluation and
resumable local JSON state; that state is a local algorithm checkpoint, not a
durable side-effect-aware run receipt.

The built-in judge, pairwise, and reflection prompts default to Chinese text;
all three are overridable. See the
[experimental evaluation and prompt-evolution recipe](https://github.com/Oxidane-bot/kigumi/blob/master/docs/adoption.md#%E5%9B%9B%E8%AF%84%E4%BC%B0%E4%B8%8E%E6%8F%90%E7%A4%BA%E8%AF%8D%E8%BF%9B%E5%8C%96evals--optimize).

## Layered Prompt example

```python
from kigumi import InputRef, PromptAxis, PromptLayer, PromptRef, PromptSpec

WRITE = PromptSpec(
    name="write",
    base=PromptRef("base/task"),
    layers=(
        PromptLayer(
            slot="mode",
            source=PromptAxis(
                name="mode",
                selector=InputRef("config", path=("mode",)),
                variants={
                    "concise": PromptRef("variants/concise"),
                    "detailed": PromptRef("variants/detailed"),
                },
            ),
        ),
    ),
)


@dag.node("write", deps=("config",), prompt_specs=(WRITE,))
def write(inputs, ctx):
    return {"text": ctx.call(ctx.resolve_prompt("write"))}
```

Kigumi snapshots all declared Prompt files once per run. The selected variant
enters the L3 key; unselected variant bytes remain in run identity, so editing
one can reuse the selected cache but cannot silently resume an old run. Inspect
the complete declaration or persisted selections with `kigumi profile` and
`kigumi graph --prompts`.

## Install

```bash
uv add "kigumi[litellm]"
```

Without the litellm extra you can use `StdlibTransport` (pure-stdlib HTTP)
or implement your own transport. Pi is an external runtime: install it yourself,
pin its version, and pass the executable plus exact version to `PiRpcAdapter`.
Kigumi never installs or upgrades Node/Pi. The staged, root-scoped tool boundary
limits model tool I/O but is **not** an OS sandbox; trusted Pi Extensions retain
host-process permissions.

Automatic DAG retry is off by default. When a node declares `RetryPolicy`,
Kigumi persists run/attempt state and returns pending instead of sleeping;
an external supervisor calls `Dag.resume()` when due. `EvidencePolicy` controls
retention after mandatory secret scrubbing, but is not encryption or access
 control. Runs without a schema-2 manifest fail closed and cannot be resumed.

For a zero-request first run, try the
[ticket-extraction DAG](https://github.com/Oxidane-bot/kigumi/tree/master/examples/ticket_extract)
or the
[experimental content-only prompt-evolution recipe](https://github.com/Oxidane-bot/kigumi/tree/master/examples/prompt_evolve).
Both examples record the framework friction found while putting the workflow into practice;
the ticket example also includes measured local-run numbers.

## Documentation map

Documentation is currently written in Chinese, except `docs/brief.md`, which is
English because coding agents read it in downstream projects.

Once kigumi is installed you do not need this repository to read any of it. Run
`kigumi brief` for the agent entry page, `kigumi docs` to list every page shipped
inside the wheel, and `kigumi docs <name>` to print one. `docs/` stays the single
source of truth; the wheel maps those files in rather than copying them.

| Document | The question it answers |
| --- | --- |
| [docs/brief.md](https://github.com/Oxidane-bot/kigumi/blob/master/docs/brief.md) | **Agents start here** (`kigumi brief`). What kigumi already owns so you do not reimplement it; the read-only commands to run before editing a node |
| [docs/capabilities.md](https://github.com/Oxidane-bot/kigumi/blob/master/docs/capabilities.md) | **Start here.** What can this library do; one grep-able line per capability, need on the left, symbol on the right |
| [DESIGN.md](https://github.com/Oxidane-bot/kigumi/blob/master/DESIGN.md) | Why it is designed this way; layers, boundaries, settled trade-offs |
| [docs/adoption.md](https://github.com/Oxidane-bot/kigumi/blob/master/docs/adoption.md) | How to adopt it; the path from a single caller to a DAG, plus troubleshooting |
| [docs/cli.md](https://github.com/Oxidane-bot/kigumi/blob/master/docs/cli.md) | Which CLI owns an operation; every command, flag, default, and meaningful exit code |
| [docs/api.md](https://github.com/Oxidane-bot/kigumi/blob/master/docs/api.md) | What the public names mean; terse signatures, result types, policies, exceptions, and utilities |
| [docs/contracts/README.md](https://github.com/Oxidane-bot/kigumi/blob/master/docs/contracts/README.md) | Which behaviors are promises; indexed invariants, failure behavior, verification coordinates |
| [docs/reviews/](https://github.com/Oxidane-bot/kigumi/blob/master/docs/reviews/) | What a review found at a point in time; descriptive records, not specs |
| [CHANGELOG.md](https://github.com/Oxidane-bot/kigumi/blob/master/CHANGELOG.md) | What changed; cache-family rotations and breaking changes are always recorded |
| [AGENTS.md](https://github.com/Oxidane-bot/kigumi/blob/master/AGENTS.md) | What an agent reads before entering; red lines and verification commands |

## License

[MIT](https://github.com/Oxidane-bot/kigumi/blob/master/LICENSE)
