# WO-06: provider 配置面（#5）

> 状态：NEEDS-DESIGN（先 WO-06a 设计子工单）｜ 波次 3 ｜ 风险：中 ｜ 缓存换族：否
> Issue: #5 ｜ 实测：0.13.0 REPRODUCIBLE

## 实测现状
`AgentSpec` 仍把 `provider/model/thinking` 建模为裸字符串(`agents.py:400,406`)，manifest 加载只校验字符串(`:466`)。`PiRpcAdapter` 暴露 raw command/env/session + `extra_config_files: Mapping[str, bytes]`(`pi.py:129,137`)，extra 文件原样写入 Pi home(`pi.py:247,255`)。grep 无 `models.json/ModelSpec/base_url/baseUrl`。`AgentProfileConfig` 仅是 capsule/runtime/version/command/session 绑定(`config.py:62,100`);profile 解析构造 `PiRpcAdapter` 时不带 provider 配置或 extra_config_files(`dag.py:691-692`)。测试仍手写 JSON 序列化 models.json(`test_pi_live.py:54,65`)。

## 目标
在 `AgentSpec` 与 `PiRpcAdapter` 之间提供一个**类型化 provider 描述**，由 kigumi 渲染成 adapter 运行时所需配置，替代调用方手写 models.json 裸字节。

## 关键设计决策（WO-06a 必须先定）
1. 描述符形态：如 `OpenAICompatibleProvider(id, base_url_env, api_key_env, models=[ModelSpec(id, reasoning, context_window, max_tokens)])`。
2. 落在 adapter 上还是独立 renderer；`extra_config_files` 保留为逃生舱。
3. 密钥引用约定（`$ENV` 占位 vs env_resolver），**绝不把明文密钥写进配置字节**。
4. 与 `AgentProfileConfig`/profile 解析(`dag.py:691-692`）的关系。
5. issue 备选：若维护者认为 route config 属调用方，则退而在 kigumi 文档记录 Pi 期望的 models.json 形态——WO-06a 需给出推荐。

## 执行方式
- **WO-06a（设计，Claude 侧）**：provider descriptor API + 渲染方案 + 密钥约定，供审批。
- **WO-06b（实现，Codex）**：按批准方案实现 + 测试（含一个把 typed provider 渲染为 Pi 配置的用例，替代手写 models.json）。

## 约束
- 密钥不明文落盘；`extra_config_files` 保留为逃生舱（向后兼容）。
- 不改 AgentSpec 现有裸字符串字段的既有语义（新增而非破坏）。

## 验收（WO-06b）
- 新增测试：typed provider → 渲染出正确 Pi 配置；密钥走 env 引用不明文。
- `uv run pytest tests/test_dag_agent.py tests/test_agent_contract.py -q` 全绿；`uv run pytest -q` 全绿；ruff 通过。

## CHANGELOG
新功能，写入 `[Unreleased]`（中文）。

## 输出
WO-06a：设计文档。WO-06b：改动文件 + 测试输出。
