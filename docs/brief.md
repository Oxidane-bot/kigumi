# kigumi brief (read this first)

This project depends on kigumi. Read this page before writing code against it.

kigumi is load-bearing joinery for LLM content pipelines: deterministic calls,
content-addressed caching, validated repair loops, DAG orchestration and external
Agent nodes. It already owns the plumbing below. Do not write your own.

Print this page any time with `kigumi brief`. Other shipped pages: `kigumi docs`.

## Code the framework cannot see

kigumi only analyzes what you declare. Two boundaries decide what it can do
for you:

- `kigumi guard` scans only the paths listed in `[tool.kigumi] source_dirs`
  (default `["nodes", "lib"]`). A bare model loop in main.py or in an ad-hoc
  `scripts/` file is never reported.
- Graph commands (describe, plan, explain, graph, profile) only see nodes
  registered through `@dag.node` / `@dag.map` / `@dag.scan` / `@dag.agent` in the
  module named by `[tool.kigumi] dag_entry`.

Pipeline code written outside those boundaries gets no caching, no plan forecast,
no explain, no guard and no resume. It is not a lighter way to use kigumi; it is
not using kigumi. Put pipeline code in a source_dirs path and register it as
a node.

## Do not reimplement

| If you are about to write | Use instead |
| --- | --- |
| String-concatenate dynamic content into a prompt | `inject` (the only entry point; auto-fences) |
| An f-string prompt template with manual slots | `load_template` / `render_template` |
| A/B or multi-branch prompt selection | `PromptSpec` / `PromptLayer` / `PromptAxis` |
| A response cache, hash key, or "skip if already done" check | `LLMCaller(cache_dir=..., seed=...)` |
| A retry-until-JSON-parses loop | `call_validated` / `repair_loop` |
| A token counter and spend guard | `Budget` |
| A loop over items calling the model | `@dag.map` (bare loops are rejected by the guard) |
| A sequential loop threading state forward | `@dag.scan` |
| Step ordering, "which steps need rerunning", progress state | `Dag` / `@dag.node` |
| A subprocess wrapper around a coding agent | `@dag.agent` / `AgentSpec` |
| Writing binary output with `open(..., "wb")` | `ctx.emit_file` / `ctx.ingest_file` |
| Reading a file inside a node with `open()` | `ctx.read_text` / `ctx.read_bytes` plus `files=` |
| A prompt A/B harness with per-variant bookkeeping | `bench` / `Variant` |
| An LLM-grades-output scorer | `llm_judge` / `pairwise_judge` |
| A fake transport or recorded HTTP fixture for tests | `ScriptedTransport` / `kigumi.testing.CassetteTransport` |
| 429 backoff and concurrency limiting | `AdaptiveCapacity` / `FileSlots` / `RetryPolicy` |

Full index with one grep-able line per capability: `kigumi docs capabilities`.
If nothing there matches, kigumi likely does not do it on purpose — see the
design boundaries in `kigumi docs adoption`.

## Run these before and after every change

These are read-only. They send no requests and cost nothing.

```bash
kigumi describe                    # what exists: nodes, edges, models, prompts
kigumi plan                        # what would recompute (and cost money) next run
kigumi explain <node>              # why this node hit or missed the cache
kigumi explain <node@item>         # one map/scan item
kigumi check                       # are declarations, files, guards, docstrings sound
```

Before you edit: `kigumi describe` to see the graph you are changing, then
`kigumi plan` to see what your edit will force to recompute.
After you edit: `kigumi check`, then `kigumi plan` again to confirm the
blast radius is what you intended.

To inspect a run that already happened:

```bash
kigumi trace <run_id>              # current state: nodes, map items, every LLM call
kigumi trace <run_id> --node NAME --json
```

Skipping `kigumi plan` before a change to a shared upstream node is how an agent
turns a one-node edit into a full-graph rerun that spends real money.

## Project commands and graph commands

Everything runs under `kigumi`. The split that matters is what a command needs to
answer you.

Project commands read artifacts from disk. They find `pyproject.toml` and
`[tool.kigumi]` upward from the working directory and never import your code.
Without a valid `[tool.kigumi]` they exit 2 — except `kigumi init`, `kigumi brief`
and `kigumi docs`, which work anywhere.

`kigumi init` scaffolds a fresh project. If `[tool.kigumi]` already exists, it leaves the
project configuration and layout unchanged and synchronizes CLAUDE.md and AGENTS.md.
The shipped guidance is appended only when its injection sentinel is absent, so repeated
init calls do not duplicate the injected section.

Graph commands need the graph itself, which only exists once your Python has run.
They import the factory named by `[tool.kigumi] dag_entry` (`"module:callable"`,
returning a `Dag`) and inspect what it builds. Without that key they exit 2 and tell
you to add it. Importing your module is the cost of asking about the graph.

```toml
[tool.kigumi]
dag_entry = "nodes.graph:build_dag"   # kigumi init scaffolds this
```

外部 Agent 也可以在项目里只绑定一次 Capsule，节点按名字复用：

```toml
[tool.kigumi.agent_profiles.writer]
capsule = "agents/writer"
runtime = "pi"
expected_version = "0.83.0"
```

```python
from kigumi import AgentTask


@dag.agent("draft", profile="writer")
def draft(inputs, ctx):
    return AgentTask("完成任务")
```

command 默认是 `["pi"]`，session_carry 默认关闭；`@dag.agent_scan` 也接受同一个
`profile=`。项目配置只绑定 Capsule 与 Pi adapter，Capsule 的 "agent.toml" 仍是
provider/model 选择、thinking、system prompt 及其它 Agent 行为设置的唯一来源。profile 可用
typed providers 定义 Pi endpoint，但只接受 api_key_env 引用，不接受明文 secret；需要
extra_config_files 时，继续显式传 `adapter=` 和 `spec=`，不要把它们放进项目 TOML。

If the graph's shape or params depend on runtime input, give the factory
keyword parameters and pass them per invocation with `--graph-arg`, which every
graph command accepts:

```python
def build_dag(episode: str) -> Dag: ...
```

```bash
kigumi plan --graph-arg episode=E2S4
```

Do not default those parameters to placeholder values to keep the commands quiet.
A node's `params` is a cache-key component, so placeholders make `kigumi plan`
forecast a key space no real run will use, make `kigumi explain` report every node as
changed, and make `kigumi resume` execute under a graph identity that does not match
the run. Pass the values a real run uses.

The same commands are also available as a standalone `dag` command if the project
registers one — `Dag.cli(argv)` is the same dispatch, reached without the config key:

```toml
[project.scripts]
dag = "nodes.graph:main"              # then: dag describe
```

That requires the project to be installable and installed. `kigumi <command>` only
needs `pyproject.toml` and an importable module, so prefer it in scripts and CI.
If a project has both, they inspect the same graph.

| Question | Command |
| --- | --- |
| What is in this graph: nodes, edges, models, prompts, checkpoints | `kigumi describe` (add `--format json`) |
| Are declarations, declared files, guards and docstrings sound | `kigumi check` (exit 1 on errors) |
| What will recompute if I run now | `kigumi plan` (`--targets A,B` to scope) |
| Why is this node recomputing | `kigumi explain NODE [--run-id ID]` |
| Show me the shape | `kigumi graph` (`--prompts` for Mermaid, `--html PATH`) |
| Give me the canonical IR | `kigumi profile [--run-id ID] [--format json]` |
| Continue a run that stopped for retry or approval | `kigumi resume RUN_ID` |
| Record an explicit decision for a terminal failed run | `kigumi recover RUN_ID TARGET --attempt N --decision ... --reason TEXT` |
| Rule on an ambiguous attempt | `kigumi retry-resolve RUN_ID TARGET --attempt N --action retry\|fail --reason TEXT` |
| Which run IDs exist, what happened in one | `kigumi runs list` / `kigumi runs show ID` |
| The payload of one LLM call | `kigumi call <key_prefix> --field messages\|response` |
| Approve a human checkpoint | `kigumi approve RUN_ID NAME` |
| What changed between two runs | `kigumi diff RUN_A RUN_B` |
| Are paths, keys and templates healthy | `kigumi doctor` |
| Reject bare LLM loops and undeclared file reads | `kigumi guard [--changed]` |
| Drop old caches and artifacts | `kigumi gc --keep N` |
| Render a template with explicit slots | `kigumi render TEMPLATE --slot k=v` |
| Scaffold a fresh project or sync its agent docs | `kigumi init [--hooks]` |
| Re-read this page, or list every shipped page | `kigumi brief` / `kigumi docs` |

`--json` on `kigumi trace`, `kigumi diff`, `kigumi runs list` and `kigumi runs show` is stable
`canonical_json`, safe to parse. `kigumi profile --format json` and
`kigumi describe --format json` also use byte-stable `canonical_json` output.
Every flag, default and exit code: `kigumi docs cli`.

## Working rules

- Node functions take `(inputs, ctx)` and must return a dict. Text deliverables go in
  `{"files": {"relative/path": "text"}}` so the framework writes them atomically.
- Never call a model outside `ctx.call` / `ctx.call_validated`, and never read a
  project file inside a node without declaring it. Both are guard violations.
- A waiver must state a reason: `# kigumi: raw-llm-ok <why>` or
  `# kigumi: raw-io-ok <why>`. The two are not interchangeable.
- Use `consumes=` to depend on part of an upstream artifact instead of all of it.
  That shrinks the blast radius when the upstream changes.
- Topology is declared in Python at registration time. A model decides content,
  never which node runs. Runtime fan-out is only `@dag.map` (independent items)
  and `@dag.scan` (linear carry).
- Large binaries go through blob references, never into artifact dicts.
- Behavior change means writing a failing test first, then making it pass. Docs
  are not a substitute for a regression test.
- Touching any cache-key component is a cache family change: update `CHANGELOG.md`
  in the same commit.
- Corrupt receipts, manifests, candidates, artifacts or blob digests fail closed.
  Never treat them as a cache miss and rerun.

## Where to read more

| Need | Page |
| --- | --- |
| One line per capability, need on the left, symbol on the right | `kigumi docs capabilities` |
| How to adopt it, recommended shapes, troubleshooting by symptom | `kigumi docs adoption` |
| Signatures, result types, exceptions and what to do about each | `kigumi docs api` |
| Every command, flag, default and exit code | `kigumi docs cli` |
| Promises you must not break while changing the implementation | `kigumi docs contracts` |

Zero-request end-to-end examples live in the repository under `examples/`.
