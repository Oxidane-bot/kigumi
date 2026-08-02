# 契约索引

本目录是 kigumi 可验证不变式的权威文本。每份契约说明承诺的行为、失效方式、实现
Source of truth（公开入口见
[`kigumi/__init__.py`](https://github.com/Oxidane-bot/kigumi/blob/master/kigumi/__init__.py)）和验证坐标；
它不是教程，也不以示例代替
约束。修改实现前先读对应契约。

`Status: Active (X.Y.Z)` 中的版本表示这份契约文本最后一次实质修订所在的 release，
不是最低兼容版本。修改任一缓存键成分就是缓存换族，必须在同一次提交中更新
[`CHANGELOG.md`](../../CHANGELOG.md)。

| 契约 | 状态 | 回答的问题 |
| --- | --- | --- |
| [缓存键契约](cache-key.md) | Active (0.9.0) | 哪些输入进入 L1/L3 内容键，什么变化必须换键或换族？ |
| [确定性字节契约](determinism.md) | Active (0.8.0) | cold、warm 与重放路径怎样保持相同的 canonical 字节结果？ |
| [缓存与产物保留契约](retention.md) | Active (0.8.0) | GC 可以删除什么，保留 run 必须保护哪些 cache 与 blob？ |
| [分层 Prompt 解析契约](prompt-resolution.md) | Active (0.9.0) | Prompt 声明怎样确定解析、入键并留下不含内容的 lineage？ |
| [静态子图契约](subgraph.md) | Active (0.8.0) | 可复用子图怎样挂载而不突破静态拓扑与运行时展开边界？ |
| [输出所有权契约](output-ownership.md) | Active (0.8.0) | 同一次 run 中谁可以物化某个项目路径，冲突怎样失败？ |
| [检查点审批契约](checkpoint.md) | Active (0.9.0) | 人工批准怎样绑定精确 payload，并在同一 run 中恢复？ |
| [Failure 契约](failure.md) | Active (0.7.0) | CALL 与 Agent 怎样共享可序列化、provider-neutral 的失败事实？ |
| [执行准入契约](admission.md) | Active (0.10.1) | 预算与节点资源怎样在副作用前准入，并如何共享并发平面？ |
| [Durable retry 与 run resume 契约](retry-resume.md) | Active (0.9.0) | retry、resume 与 ambiguous side effect 在什么边界上裁决？ |
| [Agent node 契约](agent-node.md) | Active (0.9.0) | 外部 Agent 与 Agent scan 怎样成为可缓存、可审计的 DAG 执行器？ |
| [Agent 全局容量契约](agent-capacity.md) | Active (0.7.0) | 多进程 Agent miss 如何共享容量、排队并报告 slot timeout？ |
| [EvidencePolicy 契约](evidence.md) | Active (0.8.0) | canonical artifact 与 request/response/trajectory 证据怎样分离保留？ |
| [WorkflowProfile 画像契约](workflow-profile.md) | Active (0.9.0) | 哪一份 IR 同时供应静态声明、运行检查、trace 与图渲染？ |
| [守卫环与豁免契约](guards.md) | Active (0.5.0) | 四个守卫入口怎样拒绝裸循环调用和未声明文件读取？ |
| [Experiments 契约](experiments.md) | Active (0.5.0) | 不同执行主体怎样进入同一隔离证据网格而不自动选赢家？ |
