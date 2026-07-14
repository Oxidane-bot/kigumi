# 提交流程与规范

工程硬规矩(缓存契约、豁免、测试纪律等)见 [AGENTS.md](AGENTS.md),此处只约定 commit 流程。

## 提交信息

- **一律用英文**,不要引入中文提交信息。
- 标题行:首词大写、不加句尾句号,概括"做了什么";可用 `scope: summary` 冒号形态
  (如 `P5a: dag.py orchestration layer -- ...`、`Release 0.2.0: ...`)。
  尽量精炼,细节放正文。
- 正文用 `- ` 列表,写清改了什么、为什么改;行为变更注明测试锚点
  (如 `RED-verified`、`tests: 38 -> 52`);收尾可附验证状态
  (如 `107 tests green, ruff clean`)。
- 一个提交只做一件事;修复与新功能不混在同一提交。

## 提交前检查

依次通过后才能提交:

```bash
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
```

- 行为变更先写会失败的测试,转绿后再提交(见 AGENTS.md)。
- 动了任一缓存键成分即缓存换族,同一提交内必须更新 `CHANGELOG.md`。
- 面向使用者的变更写入 `CHANGELOG.md` 的 `[Unreleased]` 小节(中文,Keep a Changelog 体例)。

## 分支与推送

- `master` 是发布主干;成块的工作走 `feature/*` 分支,完成后并入 master。
- 禁止对 master force-push,历史视为稳定。
- **`v*` 开头的 tag 会触发 PyPI 自动发布**(`.github/workflows/release.yml`),
  只在正式发布时打,不要随手推 `v` 前缀 tag。

## 发布流程

1. `CHANGELOG.md`:把 `[Unreleased]` 归档为 `[X.Y.Z]` 版本小节。
2. `pyproject.toml`:提升 `version`。
3. 提交,标题形如 `Release X.Y.Z: <一句话主题>`。
4. 打 tag 并推送:`git tag vX.Y.Z && git push origin master vX.Y.Z`。
5. Release workflow 经 PyPI trusted publishing 自动构建并发布,推送后到
   Actions 确认两个 job 都绿。

## 语言约定

- 提交信息:英文。
- README.md 主文:英文;README.zh-CN.md、CHANGELOG.md、docs/、AGENTS.md:中文。
- 代码内日志与控制台输出不用 emoji。
