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
- **Repair loop**: failed validation turns into corrective instructions,
  model context is preserved, retries are bounded, lessons are locked in
- **Deterministic replay**: content-addressed caching — same input,
  byte-identical output
- **DAG orchestration** (optional): explicit node/item cache policy, static
  reusable subgraphs, dynamic map/scan, owned materialized outputs, human
  checkpoints, and run diffs
- **Four guard rings**: registration-time refusal plus three outer rings
  (`dag check` / pytest auto-collection / git hooks), so the rules enforce
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

0.4.0, API not frozen. All four core layers are in place, with 351 tests passed and 1 skipped,
refined through three clean-room pilots (structured extraction /
multimodal / DAG orchestration).

## Install

```bash
uv add "kigumi[litellm]"
```

Without the litellm extra you can use `StdlibTransport` (pure-stdlib HTTP)
or implement your own transport.

## Documentation map

Documentation is currently written in Chinese.

| Document | The question it answers |
| --- | --- |
| [DESIGN.md](https://github.com/Oxidane-bot/kigumi/blob/master/DESIGN.md) | Why it is designed this way; layers, boundaries, settled trade-offs |
| [docs/adoption.md](https://github.com/Oxidane-bot/kigumi/blob/master/docs/adoption.md) | How to adopt it; the path from a single caller to a DAG, plus troubleshooting |
| [docs/contracts/](https://github.com/Oxidane-bot/kigumi/blob/master/docs/contracts/) | Which behaviors are promises; invariants, failure behavior, verification coordinates |
| [docs/reviews/](https://github.com/Oxidane-bot/kigumi/blob/master/docs/reviews/) | What a review found at a point in time; descriptive records, not specs |
| [CHANGELOG.md](https://github.com/Oxidane-bot/kigumi/blob/master/CHANGELOG.md) | What changed; cache-family rotations and breaking changes are always recorded |
| [AGENTS.md](https://github.com/Oxidane-bot/kigumi/blob/master/AGENTS.md) | What an agent reads before entering; red lines and verification commands |

## License

[MIT](https://github.com/Oxidane-bot/kigumi/blob/master/LICENSE)
