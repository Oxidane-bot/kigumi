# WO-01: 配置边界 fail-closed（#12、#11、#19）

> 状态：READY ｜ 波次 1 ｜ 风险：低 ｜ 缓存换族：否（但见"约束"）
> Issues: #12, #11, #19 ｜ 实测基线 0.13.0 (f5da841)

## 目标
把三个"配置非法却被静默放过"的缺陷改为**加载期即报错**，并命名出错字段。

## 实测现状（已确认 REPRODUCIBLE）
- **#12** `KigumiConfig(source_dirs="src")`（裸字符串非 list）不校验，`source_paths` 逐字符迭代成 `[.../s, .../r, .../c]`。`source_dirs` 注解在 `kigumi/config.py:136`，`source_paths` 直接迭代于 `config.py:247`。
- **#11** `check_paths()` 只扫 `Path.is_dir()` 为真的条目（`kigumi/enforce.py:291`），单文件 source_dirs 条目被静默跳过；无 include/exclude。
- **#19** `FileSlots.from_env()` 把不可解析的 `KIGUMI_REQUEST_SLOTS` 在 `ValueError` 处理器里置 `0`（`kigumi/slots.py:134`），`enabled` 需 `_slots>=1`（`slots.py:139`），于是配置锁目录时被静默禁用。

## 改动（精确）
1. **#12 — `kigumi/config.py`**:在 `KigumiConfig.__post_init__`（`config.py:151`，这是直接构造与 `load_config()` 的共同收口点，`config.py:301`）校验 `source_dirs` 必须是 `list`/`tuple` 且每项为非空 `str`。裸 `str` 抛带字段名的 `ValueError`（如 `"source_dirs must be a list of non-empty strings, got str"`）。注意：`source_dirs` 当前允许为空 list（见现有测试），保持允许空 list，只拒绝非序列/非字符串项。
2. **#11 — `kigumi/enforce.py` + `kigumi/config.py`**:`check_paths()`（`enforce.py:287-309`）同时接受**文件与目录**：对存在的单个 `.py` 文件直接纳入扫描；对**不存在**或**既非文件也非目录**的条目显式报错（不得静默跳过）。include/exclude glob **不在本工单**（记入非目标）。
3. **#19 — `kigumi/slots.py`**:`from_env()` 区分"变量缺失"与"值非法"。`KIGUMI_REQUEST_SLOTS` 被设置但不可解析为 int 时，抛出带变量名的配置错误；仅当变量未设置时才走默认。保留现有合法值语义。

## 约束
- 行为变更**先写会失败的测试再转绿**（RED→GREEN）。
- 不改动任何缓存键成分、不改动 `source_paths` 对**合法**输入的返回值。
- 不改 CLI 参数面。

## 非目标
- source_dirs 的 include/exclude glob 机制（后续单独工单）。
- 把 #19 的校验上移进 `KigumiConfig`（#19 不走 KigumiConfig，保持独立）。

## 验收（proof）
- `uv run pytest tests/test_config.py tests/test_enforce.py -q` 全绿。
- 新增测试：裸字符串 source_dirs 报错；单文件 source_dirs 被扫描；缺失路径报错；非法 KIGUMI_REQUEST_SLOTS 报错。
- `uv run pytest -q` 全绿；`uv run ruff check .` 与 `uv run ruff format --check .` 通过。
- 全量测试数不下降（1178 → 增加）。

## CHANGELOG
面向使用者变更，写入 `CHANGELOG.md` `[Unreleased]`（中文，Keep a Changelog）。

## 输出
report：改动文件清单 + 每项 issue 的 before/after repro 输出 + 完整测试输出。
