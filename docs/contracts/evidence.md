# EvidencePolicy 契约

Status: Active (0.6.0)

## Purpose / source of truth

把 canonical artifact 与请求、响应、stderr、trajectory 等执行证据解耦，同时保持 cold/warm
replay 的同一 origin。实现权威为 `kigumi/evidence.py`、`kigumi/calling.py`、
`kigumi/agents.py` 与 `kigumi/_execution.py`。

## Invariants

1. `EvidencePolicy(request, response, stderr, trajectory)` 的每项只能是 `full`、
   `redacted` 或 `hash_only`，默认均为 `full`。
2. 所有模式先清理显式/env credential、authorization/cookie/header secret 和 URL query
   secret。`full` 保存清理后的内容；`redacted` 将 prompt/content/text/thinking/reasoning/
   arguments/input/output 值替换为摘要与字节数；`hash_only` 只留 SHA-256、字节数、
   media type 和必要执行 metadata。
3. `agent_schema=2` canonical artifact 只含 task/completion、Agent identity、collected
   attachments、published outputs 和可选 `files`。usage、duration、workspace manifest、
   RPC、stderr、trajectory、Hook/policy evidence 和 queue/slot metadata 属于 origin
   provenance，不得回流 canonical artifact。
4. node cache envelope schema 2 保存 artifact 与 hash-bound immutable origin；cold/warm
   sidecar 读取同一 origin。policy canonical digest 写入 origin 与 run manifest。
5. policy digest 不匹配是 evidence miss，不改变 L3 内容键。普通 CALL 可从 L1 replay payload
   重建新证据而不再次请求 provider；Agent miss 必须重新执行。
6. L1 仍保存确定性重放所需 payload。EvidencePolicy 不是加密、权限控制、密钥管理或文件
   访问控制。
7. 普通 materializer 不解释 evidence；GC 只按 retained JSON 中的 attachment/blob 引用做
   reachability。`AgentSubject` 从 sidecar origin/failure receipt 读取 usage 与 raw evidence。

## Verification

见 `tests/test_evidence.py`、`tests/test_calling.py`、`tests/test_dag_agent.py`、
`tests/test_dag_store_blobs.py`、`tests/test_testing.py`。
