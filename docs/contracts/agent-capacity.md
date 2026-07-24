# Agent 全局容量契约

Status: Active (0.7.0)

## Configuration

```toml
[tool.kigumi]
agent_slots = 1
agent_lock_dir = "artifacts/_locks/agents"
agent_slot_timeout_seconds = 300
```

环境变量 `KIGUMI_AGENT_SLOTS`、`KIGUMI_AGENT_LOCK_DIR`、
`KIGUMI_AGENT_SLOT_TIMEOUT_SECONDS` 覆盖项目值。需要跨项目共享机器级容量时，各项目必须
显式指向同一绝对 lock root。

## Invariants

1. 默认共享 lock root 同时最多运行一个外部 Agent；只有用户显式提高 slots 才并发。
2. cache hit 不申请 slot、不运行 builder/Pi。miss 先运行 builder 并绑定可选 managed
   instruction resolution，再在 staging 和进程启动前申请 slot。
3. queue wait 不计入 Agent execution timeout；sidecar origin 记录 queue wait、slot identity、
   execution seconds 和退出原因。容量配置不进入内容缓存键。
4. slot timeout 产生 typed `AgentRuntimeFailureCode.CAPACITY`，且在 provider/Agent side
   effect 前失败。
5. 正常、异常、timeout 和进程组终止均释放 slot；builder 失败发生在申请前。锁同时覆盖线程
   与进程。

## Verification

见 `tests/test_agent_capacity.py`、`tests/test_slots.py` 与 fake Pi 并发测试。
4/8/16 ready-node 压力基准可显式运行
`uv run python benchmarks/agent_capacity.py`；真实 Pi 基准/live conformance 不进入默认 CI。
