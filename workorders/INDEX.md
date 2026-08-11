# kigumi 工单索引（0.13.0 基线 f5da841）

> 21 个 issue + 6 个精简目标 → 16 份工单。
> 3 条 issue(#1/#2/#3）实测已被 0.13 修复，仅 #2 留文档收尾（WO-02）。
> 状态图例：READY=可派发 ｜ NEEDS-DESIGN=先设计子工单 ｜ SCOPED=分阶段

## 实测基线（Codex 7 路调查 + 我实测）
- 已修复（0.13):#1 #2 #3
- 可复现：#5 #9 #10 #11 #12 #13 #15 #16 #18 #19 #20 #21
- 部分修复：#6 #7 #14

## 工单清单

| WO | 标题 | 覆盖 | 波次 | 状态 | 风险 | 换族 |
|----|------|------|------|------|------|------|
| WO-01 | 配置 fail-closed | #11 #12 #19 | 1 | READY | 低 | 否 |
| WO-02 | init 文档对齐 | #2 | 1 | READY | 极低 | 否 |
| WO-03 | 有界等待超时 | #14 #15 #18 | 1 | READY | 低-中 | 否 |
| WO-05 | schema 版本化 | #21 | 1 | READY | 低 | 否 |
| WO-07 | agent 诊断 | #6 #7 | 1 | READY | 中 | 否 |
| WO-09 | 精简#1 共享物化器 | complexity | 1 | READY | 中 | **否** |
| WO-10 | 精简#3 静态脚手架 | complexity | 1 | READY | 低 | 否 |
| WO-11 | 文档对齐 | #24 #4 | 1 | READY | 极低 | 否 |
| WO-16 | single-flight 生命周期 | #20 | 2 | READY | 中 | 否 |
| WO-12 | 精简#2 key-decision 对象 | complexity | 2 | READY | 高 | **否** |
| WO-04 | guard 三值判定 | #9 #10 #16 | 3 | NEEDS-DESIGN | 高 | 否 |
| WO-06 | provider 配置面 | #5 | 3 | NEEDS-DESIGN | 中 | 否 |
| WO-08 | 预算 effective-params | #13 | 3 | NEEDS-DESIGN | 高 | **可能** |
| WO-13 | 精简#4 execution 事务 | complexity | 4 | SCOPED(13a设计→13b) | 高 | 否 |
| WO-14 | 精简#5 cache_identity 模块 | complexity | 4 | SCOPED(14a→14b) | 很高 | 否 |
| WO-15 | 精简#6 跨层契约 | complexity | 4 | SCOPED | 中-高 | 否 |

## 波次执行顺序
- **波次 1**:WO-01, 02, 03, 05, 07, 09, 10, 11 —— 独立小修，并行派发。
- **波次 2**:WO-16, WO-12 —— WO-12 在波次 1（尤其 WO-09）落地后做。
- **波次 3**:WO-04, 06, 08 —— 各先出设计子工单（a)，审批后实现（b)。
- **波次 4**:WO-13, 14, 15 —— 大精简，SCOPED 分阶段；WO-14 依赖 WO-09/WO-12。

## 缓存换族警戒线
WO-09、WO-12 触碰缓存键核心，**必须保证键字节逐位不变**（工单内已设验收）。
WO-08 若把 effective params 纳入缓存键 → 换族，需设计决策 + CHANGELOG + schema 处理。
任何键成分变化，同提交必须更新 CHANGELOG.md(AGENTS.md 纪律)。

## 派发约定
- 每份 READY 工单 = 一个独立 Codex dispatcher 调用 + 独立 worktree + 唯一 task id。
- 模型 Luna + reasoning max。
- RED→GREEN：行为变更先写会失败的测试。
- 回收后 Claude 验收：读 diff + 跑测试 + 对照工单验收项。
