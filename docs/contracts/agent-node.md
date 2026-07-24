# Agent node 契约

## Purpose

让原生 Pi Agent 成为 Kigumi 静态 DAG 中可缓存、可重放、可审计的普通节点执行器，同时保留
provider-neutral `AgentAdapter` 边界。

## Scope / source of truth

公开 capsule、task、completion 与 staging 语义以 `kigumi/agents.py` 为准；Pi RPC 以
`kigumi/pi.py` 和随 wheel 分发的 `kigumi/_pi_bridge.ts` 为准；调度、物化与保留分别以
`Dag.agent` / `Dag.run` 和 `kigumi/store.py` 为准。

## Capsule schema v1

`AgentSpec.load(path)` 只接受目录胶囊：

```text
agent/
├── agent.toml
├── SYSTEM.md
├── skills/
└── hooks/
```

manifest 必须显式声明 `schema_version=1`、`runtime="pi"`、provider、model、thinking、
`SYSTEM.md`、Skill 目录、Hook Extension 文件、工具白名单和全部 `[limits]`。manifest 与所有
引用文件/目录按稳定相对路径、类型和内容摘要形成 `AgentSpec.digest`。未引用普通文件不入键；
绝对/越界路径、symlink、重复资源、credential 字段/文件和 `bash|shell|terminal` 一律拒绝。

## Invariants

1. `Dag.agent(..., spec=AgentSpec)` 无 `AgentConfig` 兼容入口；L3 lookup 在 builder/Pi 之前。
   miss 先运行 builder 以绑定可选 `ResolvedPrompt` instruction，再申请 slot。
2. Agent 复用普通 node 的拓扑、cache lookup、seal、output claim、materialize、sidecar 和 run。
3. `external` 摘要包含 `agent_executor_schema=3`、adapter/Pi expected version、bridge 与路径
   policy digest、capsule digest、provider/model/thinking、工具和 limits；普通 L3
   `CACHE_SCHEMA=5`。
4. miss 只 staging 声明文件、canonical upstream 和 capsule snapshot；scratch 不保留。
5. collect 产生无项目目标路径的 `kigumi_attachment`；`AgentCompletion.outputs` 必须全部来自
   本次 collect，并覆盖全部 publish source；只有 exact publish 进入普通物化。
6. `submit_result` 的固定结果只有 `status="completed"`、`summary`、`outputs`、`metrics`。
   未提交、重复提交、Hook 拒绝、schema 错误或输出缺失均 fail closed。
7. Pi 启动前以 `--version` 与 `expected_version` 精确匹配；Kigumi 不安装、升级 Node/Pi。
8. Pi 固定关闭 session、project context、隐式 Extension/Skill/Prompt/Theme 发现和 built-in
   tools，只显式加载 staged capsule 与 Kigumi bridge。
9. bridge 的 `read/write/edit/grep/find/ls` 同名工具拒绝绝对路径和 `..`，且只以 staging
   workspace 为 root；这约束模型工具访问，但不是 OS sandbox，可信 Extension 仍有宿主权限。
10. RPC 是严格 LF JSONL；stdout/stderr 并发排空；timeout/异常终止整个进程组；unknown UI、
    Extension error、非零退出、超 turn/tool/evidence 额度均拒绝。
11. env resolver 的值在写入 error、trajectory、RPC、stderr、Hook evidence 或 completion 前
    强制脱敏；workspace 完成前扫描 credential bytes，命中即 fail closed。
12. `agent_schema=2` canonical artifact 只保留 task/completion、Agent identity、collected
    attachments、published outputs 与 `files`。RPC、stderr、trajectory、Hook/policy evidence、
    usage/cost、duration、workspace manifest、queue/slot 与退出原因只进 hash-bound origin。
13. `EvidencePolicy` 控制证据 retention；普通 materializer 不解释 evidence attachment，GC 从
    retained sidecar/failure/attempt receipt 追踪引用。
14. Pi 固定关闭 hidden Agent/provider retry；`auto_retry_start/end`、thinking-off 仍出现 thinking
    或 observed response model substitution 都立即 fail closed。
15. miss 的 builder 完成后、staging/Pi spawn 前获得全局 Agent slot；hit 不运行 builder、
    不占 slot。slot timeout 为 typed capacity failure，queue wait 不消耗 execution timeout，
    failure receipt 保留 builder 已绑定的 managed/unmanaged instruction lineage。
16. `AgentTask.instruction` 可直接接收 `ResolvedPrompt`；success、failure、timeout、capacity、
    hidden retry 与 ambiguous active effect 使用同一 resolution 形态。普通字符串保持
    unmanaged。
17. v1 capsule 没有自动进化、winner/promotion、Agent factory、动态多 Agent 拓扑；
    writer/reviewer/arbiter 由用户用静态 DAG 组合。

## Failure behavior

capsule、版本、RPC、交互、quota、completion、attachment 或 publish 任何违约都在普通物化前
失败；失败 workspace 清理、不写成功 cache，但 run-local failure JSON 和已捕获证据保留。

## Verification / change policy

见 `tests/test_pi_first.py`、`tests/test_agent_contract.py`、`tests/test_agent_attachments.py`、
`tests/test_dag_agent.py` 和 `tests/test_dag_agent_failures.py`。改变 Agent executor/result/key 语义
必须递增 `agent_executor_schema` 并记入 CHANGELOG；改变普通键或 canonical artifact 语义必须
递增 `CACHE_SCHEMA`。Evidence、capacity 与 retry 的专门契约分别见 `evidence.md`、
`agent-capacity.md`、`retry-resume.md`。
