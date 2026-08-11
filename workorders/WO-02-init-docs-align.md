# WO-02: kigumi init 文档行为对齐（#2 已修复的收尾）

> 状态：READY ｜ 波次 1 ｜ 风险：极低 ｜ 缓存换族：否
> Issue: #2 ｜ 实测：0.13.0 已 FIXED（commit c062405）

## 背景
Codex 实测确认 #2 在 0.13.0 **已修复**：`_init` 现在走 `_plan_agent_doc_writes`（`kigumi/cli.py:679-692`），既有项目 repro 返回 exit 0 + `synchronized kigumi agent docs`，由 `c062405` 修复。行为已正确。

## 目标（仅收尾，非行为修复）
0.13 修复后，确认没有**残留的过时注释/文档**仍在描述旧的"exit 1 防重复"语义。issue #2 特别点名旧测试注释把"防重复"错误归因于 exit-1 守卫（真正防重复的是 sentinel）。

## 改动
1. 通读 `kigumi/cli.py` 的 `_init` / `_plan_agent_doc_writes` / `_write_agent_docs`，以及 `tests/test_cli.py` 中 repeat-init 相关用例（约 `:346-364`、`:570-610`），核对注释与文档是否与现行行为一致。
2. 若发现仍把"幂等"归因于 exit-code 守卫的过时注释/文档字符串，更新为正确归因（sentinel 保证幂等）。
3. `docs/adoption.md`、`docs/brief.md` 若描述 `kigumi init` 对既有项目的行为，核对与 `_plan_agent_doc_writes` 现行语义一致，不一致则更新。

## 约束
- 纯文档/注释对齐，**不改任何运行行为**。
- 若实测发现行为仍与文档不符（即 #2 未完全修复），**停止并在报告中标注**，不要顺手改行为——那需要单独评估。

## 验收
- `uv run pytest tests/test_cli.py -q` 全绿。
- `uv run ruff check .` 通过。
- 报告列出每处核对过的注释/文档 + 是否需改 + 改了什么。

## 输出
report：核对清单 + 改动文件 + 测试输出。
