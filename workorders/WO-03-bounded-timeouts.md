# WO-03: 有界等待超时（#14、#15、#18）

> 状态：READY ｜ 波次 1 ｜ 风险：低-中 ｜ 缓存换族：否
> Issues: #14, #15, #18 ｜ 实测基线 0.13.0

## 目标
把"能力已存在但未暴露"或"完全无界"的等待/限制，补上公开超时/禁用入口。三条都属 P3（每个 acquire/reserve/lock 有界或显式无限）。

## 实测现状
- **#14 PARTIAL**:`_PermitPlane` 已支持 `timeout_seconds`（`kigumi/dag.py:160,163-189`），但 `_build_permit_plane()` 构造时不传（`dag.py:1415`），`Dag.run()` 无资源等待超时参数（`dag.py:1417`）。
- **#15 REPRODUCIBLE**:`FileSlots.acquire()` 有 `timeout_seconds`（`kigumi/slots.py:142`），但 `acquire_key()` 只收 `key`，内部阻塞 `fcntl.flock(LOCK_EX)`（`slots.py:164,172-179`）；`LLMCaller` 无超时调用它（`calling.py:1257`）。
- **#18 REPRODUCIBLE**:`resource_limits={"gpu":0}` 被校验拒绝，`dag.py:1381` 抛 "must be a positive integer"；无法用 0 表示"资源禁用"。

## 改动
1. **#14 — `kigumi/dag.py`**:`Dag.run()`（及 resume 路径）新增可选 `resource_timeout_seconds: float | None = None`，透传进 `_build_permit_plane()` → `_PermitPlane(timeout_seconds=...)`。默认 `None`（保持现状无限）。超时抛带资源名与等待时长的错误。语义（fail / 可重试 / 与 resume 集成）遵循 `_PermitPlane` 现有 TimeoutError 行为，新增参数仅暴露之。
2. **#15 — `kigumi/slots.py` + `kigumi/calling.py`**:`FileSlots.acquire_key()` 新增可选 `timeout_seconds: float | None = None`，超时抛 `SlotTimeoutError`（若不存在则新增，置于 slots 模块）。用带超时的 flock 等待（如 `LOCK_EX|LOCK_NB` 轮询或 `fcntl` 超时包装），不得改变默认无超时行为。`LLMCaller` 增加对应可选配置并透传。
3. **#18 — `kigumi/dag.py`**:`resource_limits` 接受 `0` 表示"该资源池禁用"；任何声明需要该资源的节点在**执行前**确定性失败（带资源名的错误），而非校验期拒绝 0。正整数语义不变。文档 `docs/contracts/admission.md:58-60` 同步。

## 约束
- RED→GREEN：先写会失败的测试。
- 所有新参数默认保持现状（向后兼容）。
- 不改缓存键成分。
- flock 超时实现须避免忙等；进程死亡仍须正确释放锁（flock 语义）。

## 非目标
- #13（预算 effective-params）、#20（per-key 锁清理）——属别的工单。

## 验收
- `uv run pytest tests/test_dag_run.py tests/test_concurrency.py tests/test_calling.py -q` 全绿。
- 新增测试：resource_timeout 到期报错；acquire_key 超时抛 SlotTimeoutError；resource_limits=0 时需求节点执行前确定性失败。
- `uv run pytest -q` 全绿；ruff check + format 通过。

## CHANGELOG
写入 `[Unreleased]`（中文）。

## 输出
report：改动文件 + 每项 issue before/after repro + 完整测试输出。
