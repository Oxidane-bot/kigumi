# 分层 Prompt 解析契约

Status: Active (0.7.0)

## Purpose / source of truth

让 Prompt 的固定片段、有限变体和运行时材料都成为显式声明、可确定解析、可内容寻址和可审计
的节点输入。实现权威为 `kigumi.prompt`、`Dag`/`Subgraph` 的 `prompt_specs` 声明，以及
CALL/Agent receipt 中的 `prompt_resolution_schema=1`。

## Public surface

顶层导出 `PromptRef`、`InputRef`、`ParamRef`、`ItemRef`、`CarryRef`、`PromptAxis`、
`PromptLayer`、`PromptMaterial`、`PromptSpec`、`ResolvedPrompt`、`PromptResolution`、
`PromptDefinitionError` 与 `PromptResolutionError`。`Dag.node/agent/map/scan/foreach` 和
`Subgraph.node/map/scan` 接受 `prompt_specs=()`；执行时通过
`ctx.resolve_prompt(spec_name)` 取得预解析的 `ResolvedPrompt`。

## Invariants

1. `PromptRef` 使用不带扩展名的项目相对名；框架只读取 `prompts_path` 下对应的 UTF-8
   常规 `.md` 文件。绝对路径、空段、`.`/`..`、反斜杠、NUL、显式 `.md` 和逃逸 symlink
   均拒绝；不支持 URL 或远端 registry。
2. 每次 run 在节点执行前一次读取所有已声明 Prompt 文件，形成不可变 snapshot。同一 run
   中途改文件不改变后续节点；下一 run 才观察新字节。静态 `profile` 同样只读声明文件，
   不执行节点或 provider。
3. base 的槽位集合必须精确等于所有 layer/material slot；slot、spec name 和 axis name
   不得重复。fragment 不得含槽位；片段逐字插入，不自动添加换行或分隔符。
4. selector/material path 必须是 `tuple[str | int, ...]`，逐层严格读取，不解析点号字符串、
   不转换类型、不 fallback。`InputRef` 读取节点实际函数输入并服从 `consumes` 投影；
   `ParamRef` 读取声明参数；`ItemRef` 只用于 map/scan；`CarryRef` 只用于 scan。
5. axis selector 结果必须是字符串并精确匹配声明 variant key。缺失、路径类型不符、非字符串
   或未知值在 L3 lookup、CALL、Agent spawn、文件物化和其他副作用前失败。
6. material 必须经 `inject()` 定界；没有 raw material、隐式 override、默认 variant、嵌套
   fragment、宏、Jinja 或模型生成 variant。首版只组合单个文本 Prompt；chat message list
   保持 legacy/unmanaged。
7. 同一节点的 legacy `prompts` 与 `PromptSpec` name 不得冲突。未采用新声明的字符串
   `ctx.call()` 和字符串 Agent instruction 继续可用，但 receipt 明确为 unmanaged。
8. `ResolvedPrompt` 是不可变 `str` 子类。只有对象本身携带 resolution；拼接、格式化、切片或
   `str()` 后元数据自然丢失，后续 CALL 必须记为 unmanaged，不得猜测或误归因。
9. `PromptResolution` 只保存 spec 结构摘要、base/layer/axis 来源、实际 selection、material
   来源与摘要、渲染摘要和字节数，不保存 Prompt 或 material 原文。canonical
   `resolution_digest` 绑定完整结构。
10. `ctx.resolve_prompt()` 只返回预解析对象；只有 `ctx.call(resolved)` 或把该对象直接作为
    `AgentTask.instruction` 才生成实际使用记录。一个节点可有多次 CALL；声明但未调用的 spec
    只属于节点输入，不伪装成实际 CALL。
11. `call_validated`/repair 的 primary 与每轮 repair 保存共同
    `base_resolution_digest`，并分别保存 `phase`、`repair_round` 和本轮实际 `prompt_sha`。
    L1 hit 必须使用当前调用的 lineage，不能回放缓存文件里的旧 lineage。

## Failure behavior

静态声明、路径、slot 或 fragment 错误抛 `PromptDefinitionError`；运行时 selector/material
解析错误抛 `PromptResolutionError`。两类错误都在缓存查找与副作用前失败。0.7 receipt 中
resolution schema 或 digest 损坏时 resume/profile fail closed。

## Verification

锁定测试见 `tests/test_prompt_specs.py`、`tests/test_workflow_profile.py`、
`tests/test_dag_retry_resume.py`、`tests/test_dag_agent_failures.py`。

## Change policy

改变解析字节、selector/material binding、resolution canonical 形态或任何 L3 键成分时，
必须先补失败测试，再更新本契约、`cache-key.md`、`determinism.md` 和 `CHANGELOG.md`。
