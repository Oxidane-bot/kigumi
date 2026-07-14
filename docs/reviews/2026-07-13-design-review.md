# 2026-07-13 设计评审记录

Status: 已整改完毕
Baseline: 705151c(评审时 HEAD);文中坐标均以该快照为准,整改后已漂移。
整改落点: 2174f73 / 4ac2b60 / db87336 / 1e40b20

本文是某日实然的审查记录,不是目标规范。应然的可验证不变式见
docs/contracts/;两者刻意分开,防止规范与某次审查的现状混写。

## 背景与方法

Greenfield 阶段(零外部用户)的全量设计审查:通读 16 个库模块约 5,900 行、
DESIGN.md、docs/adoption.md 与提交史,以"设计还改得起的窗口期该修什么"为
问题展开。

## 立得住的部分

- 分层依赖方向干净:`dag -> store` 单向(store.py 头部声明);L0-L2 不碰 L3。
- 确定性贯彻到字节:miss 路径产物过 canonical JSON 再喂下游,与命中路径
  逐字节一致(dag.py:622-624、聚合 1839-1840)。
- fail-loud 全线成立:空响应三道闸(transport.py:166-174 →
  calling.py:188-191 → 读缓存拒空 363-365);force 名字错直接抛且
  plan/run 对齐;多模态文件发送前重验哈希;审批绑定 payload 哈希。
- 过程治理:三条设计边界立字据、止损线预先写下、clean-room 试点回写摩擦。

## 发现(按杠杆排序)与整改结果

1. **DESIGN.md 宪法失真**:包结构写 9 个模块实际 16 个;evals/optimize
   约 700 行在设计文档缺席;守卫运行环声明("扫描 source_dirs 全部模块")
   与实现(只查节点函数体,dag.py:2490-2508)相反。
   → 整改(2174f73):按实际枚举、补定位裁决(evals/optimize 收编为评估与
   进化层)、声明如实化。
2. **raw-io 守卫三环残缺**:测试环与提交环只查 raw-llm;`--changed` 新增
   豁免上报不覆盖 raw-io-ok;dag check 对 source_dirs 无差别扫描与
   adoption.md"只扫节点体"承诺矛盾。
   → 整改(2174f73):三环补齐,外环用装饰器启发式过滤,两类豁免独立留痕。
3. **dag.py 内聚断裂**(2,621 行):三个渲染后端加 CLI 挤在调度模块,
   pending/skipped 提取三处逐字重复;plan/run/explain 三份键遍历独立实现,
   代码里已有"手工保持一致"注释——正是本库要消灭的漂移类别。
   → 整改(4ac2b60 渲染出走 views.py、运行态提取合并;db87336 键成分推导
   统一为 _key_components 单点)。两步均经 HEAD worktree 差分探针验证
   输出/键逐字节不变。
4. **Greenfield 已积累 4 处兼容 shim**:点分顶层键精确匹配、_next_run_id
   垫片、dag 对 store 的再导出、FakeTransport.calls 别名。
   → 整改(2174f73):全部删除。
5. **libs 哈希粒度与 DESIGN.md 吸收的 Hamilton code_version 原则不一致**:
   节点 source 剥 docstring/注释,libs 是原始文本——lib/ 改注释全流水线换族。
   → 整改(1e40b20):libs 统一 AST 归一,残破文件退回原文;libs 成分换族
   已记 CHANGELOG。
6. **检查点恢复文档缺关键一步**:审批绑定 run 目录,必须同 run_id 重跑,
   adoption.md 未写。→ 整改(2174f73)。
7. **边角**:并发多失败只抛第一个其余无声丢弃(→ add_note 附注);
   run-NNNN 字典序在第 10000 个 run 后 gc 排序错(→ 数字感知排序);
   双 CLI 与双 live 标记并存(→ 分工入宪、live 统一为双确认门)。
   均整改于 2174f73。
8. **evals/optimize 定位悬空**:自带状态文件与判分缓存,像第二个产品。
   → 裁决:收编为库内评估与进化层,评委调用复用 L1 缓存,状态文件只服务
   断点续跑;已写入 DESIGN.md。

## 验证方式

每轮整改由独立差分探针复核,不采信执行方自查:渲染探针(四种渲染 ×
有/无运行态,七份输出)与键探针(普通/map/scan/foreach 的缓存键文件名、
plan 三态、explain 五类输出)在新旧树上逐字节一致;全量测试 236 → 246,
ruff check/format 全程干净。

## 遗留

- 契约层(docs/contracts/)与发布件(LICENSE/CHANGELOG/CI/元数据)在本记录
  写就时正在落地,见 CHANGELOG。
- PyPI 包名可用性未查证;远端仓库尚未建立,pyproject 暂不写 urls。
