# WorkflowProfile 画像契约

Status: Active (0.7.0)

## Purpose / source of truth

用一个 `workflow_profile_schema=1` canonical IR 同时表达静态图、Prompt 声明和持久化运行证据，
避免 profile、describe、图渲染、trace 与 runs show 各自猜测。构建与验证权威为
`Dag._static_workflow_profile()` 和 `kigumi.profile.load_run_profile()`。

## Public surface

```python
dag.profile(run_id=None, *, include_content=False) -> dict[str, Any]
```

```text
dag profile [--run-id RUN_ID] [--format json|md] [--include-content]
dag graph [--run-id RUN_ID] --prompts
```

## Invariants

1. 静态画像只读注册声明和 Prompt snapshot，不执行节点、builder、provider、checkpoint 或
   materializer。所有运行前未知的 axis selection 明确为 `unresolved`。
2. 静态 IR 包含 node/map/scan/Agent、Subgraph mount、dependency/items/carry 边、PromptSpec
   base/layer/axis 全候选、selector/material 来源、cache/retry/evidence policy 与模型说明。
   `Dag.describe()` 从该 IR 的 declaration 投影生成；Prompt Markdown 与 Mermaid 也只消费
   同一 IR。
3. schema-2 run manifest 保存静态 IR 与其 digest。运行画像只读 manifest、sidecar、origin、
   attempt、candidate 和 failure receipt，不导入可执行图，不重跑节点或 provider。
4. 运行画像按 target（普通 node 或 `node@item`）、attempt 与 CALL 展示 current/origin
   Prompt resolution、axis selection、material/rendered digest、model、cache、usage、
   primary/repair round、failure/retry/ambiguous/resume 状态以及 managed/unmanaged。
5. warm L3 hit 的 `current_prompt_resolutions` 是本 run 重新解析和验证的 selection；
   `origin_prompt_resolutions`/`origin_calls` 来自不可变 cache origin。Agent cache hit 明确
   `executed=false`，但仍可展示 origin instruction resolution。
6. 0.7 profile 必须验证 manifest profile digest、artifact/origin/sidecar digest、
   Prompt resolution digest、attempt schema、candidate digest 与 candidate resolution。
   任一不一致抛 `WorkflowProfileError`，不得降级为“未知”继续展示。
7. `include_content=False` 不展开 CALL request/response 或 Agent instruction evidence。
   `include_content=True` 只展示该 run 已按 `EvidencePolicy` 保留和强制 scrub 后的 evidence；
   不从 L1 或 provider 重新取原文。EvidencePolicy 不是访问控制。
8. 0.6/schema-1 run 可只读展示持久信息，并固定
   `resolution_status=unavailable_legacy`；不可伪造 Prompt lineage，不可 resume。
9. JSON 字段和排序来源稳定；Markdown 固定含 Mermaid 总图和 Prompt 总表。图与表不得各自
   重解析 Prompt 声明。

## Failure behavior

缺失/损坏的 0.7 manifest、sidecar、origin、artifact、attempt、candidate 或 resolution
一律 fail closed。缺少 0.7 manifest 的旧 run 只进入 legacy 投影；不存在的 run 抛
`ValueError`/`FileNotFoundError`。

## Verification

锁定测试见 `tests/test_workflow_profile.py`、`tests/test_cli.py`、
`tests/test_dag_describe_render.py` 与 `tests/test_dag_retry_resume.py`。

## Change policy

改变 IR 字段、receipt 验证、内容展开或 legacy 降级语义时，必须先更新锁定测试，再同步本
契约、adoption、README 与 CHANGELOG。破坏既有 schema-1 profile 读取时必须递增
`workflow_profile_schema`。
