# 波次 1 合并计划（orchestrated merge）

> 8 个 worktree 分支 wo01~wo11 都基于 f5da841。共享文件是冲突热区，**统一编排合并**，不逐个 cherry-pick（避免重复 heading / 双版本测试）。

## 已完成验收
- wo02 ✅ init 文档对齐 — `0156768`
- wo09 ✅ 精简#1 共享物化器 — `6044bba`(键字节不变已独立验证)
- wo10 ✅ 精简#3 静态脚手架 — `cb7081f`(键字节不变)
- wo11 ✅ 文档对齐 #24/#4 — `4b6d233`
- wo01 ✅ 配置 fail-closed — `00eadee`

## 共享文件冲突决策
### tests/test_version.py — 同一测试两个版本
同一测试 `test_release_candidate_identity_*` 被 wo01 与 wo11 各改一版：
- **采用 wo01 版**(`..._unreleased_is_documented`,DOTALL capture + 断言 Unreleased 非空)——更严格,符合"行为变更必须写 CHANGELOG"。
- 丢弃 wo11 版(`..._precedes_release`,negative-lookahead)。
- 若 wo03/05/07 也改了它,以同样标准裁决,只留一版。

### CHANGELOG.md `[Unreleased]`
- wo01 加了 `### 修复` + `### 兼容性`;wo11 加了 `### 文档`。
- 合并时**保留所有小节**(修复/兼容性/文档…),按 Keep a Changelog 顺序排列,不重复 heading。
- wo03/05/07 的条目进来后统一归并到对应小节。

### docs/adoption.md
- wo01 与 wo11 都改了。合并时逐段核对(wo01 改 source_dirs 校验相关段,wo11 改缓存闭包/附件段),两者语义不冲突,可并留;冲突处人工对齐。

### docs/brief.md / docs/api.md
- wo01 改了 brief.md、api.md;wo11 改 brief.md。合并核对。

## 合并顺序建议
先合行为保持的精简(wo09、wo10)→ 再合行为修复(wo01、wo03、wo05、wo07)→ 最后合文档(wo02、wo11),共享文件在最后一次统一收口。

## 波次 2 触发条件
波次 1 全部合并到 master 且全量测试绿后,再派 wo16、wo12。wo12 依赖 wo09 已落地。

## 收尾波次:测试语义过时审查
全部合并后,派一路 Codex 审查 tests/ 是否把已被本轮改变的行为当成"正确"钉死(典型:test_version.py 的 Unreleased-empty 假设;check_paths 单文件返回 [] 的旧假设等)。
