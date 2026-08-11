# WO-16: per-key single-flight 状态生命周期（#20）

> 状态：READY ｜ 波次 2 ｜ 风险：中 ｜ 缓存换族：否
> Issue: #20 ｜ 实测：0.13.0 REPRODUCIBLE

## 实测现状
`LLMCaller.__init__` 初始化一个"只增不减"的锁 map(`calling.py:526`)；`_lock_for_key()` 用 `setdefault()` 且从不移除(`calling.py:1253`)。`FileSlots.acquire_key()` 为每个 key 创建 `key_<sha256>.lock` 且只解锁、从不 unlink(`slots.py:172`)。实测：循环 `_lock_for_key(f"unique-{i}")` 后 `caller._key_locks` 增长到 256；`FileSlots` 释放后 256 个 `key_*.lock` 文件残留。长驻服务/高基数负载下内存与锁目录条目线性累积。

## 目标
给 per-key single-flight 状态（进程内锁 + 锁文件）加生命周期管理，消除线性泄漏。

## 关键设计点（实现前在工单内确认一处策略）
进程内锁清理需避免"清理正在等待的锁"的竞态。候选策略（WO 实现者择一并说明理由，倾向 b）：
- a) 引用计数锁项，计数归零且无等待者时移除；
- b) **有界/条带化(striped)锁**：固定 N 个锁，key 哈希到槽位——内存有界、无清理竞态，代价是不同 key 可能共享锁（对 single-flight 去重语义可接受，因为正确性由缓存键保证，锁只是优化）；
- c) 惰性 GC：访问时顺带清理空闲项。
锁文件（FileSlots）：提供安全清理——进程退出/释放时对**无主**锁文件 unlink；需处理"锁文件可能被另一活跃进程持有"的竞态（不得删除活跃持有者的文件）。

## 改动
1. `kigumi/calling.py`：为 `_key_locks` 加生命周期（按选定策略），内存有界。
2. `kigumi/slots.py`：`acquire_key` 的锁文件增加安全清理/GC，不删除活跃持有者的文件。
3. 文档 `docs/contracts/admission.md` 说明生命周期语义。

## 约束
- 不改变 single-flight 正确性（同 key 仍互斥）。
- 不得引入"删除了活跃锁"的竞态。
- 默认行为向后兼容（短生命周期调用方无感）。

## 验收
- 新增测试：高基数 key 后锁 map 有界/被清理；锁文件在无活跃持有者时被清理、活跃持有者文件不被删。
- `uv run pytest tests/test_calling.py tests/test_concurrency.py -q` 全绿；`uv run pytest -q` 全绿；ruff 通过。

## CHANGELOG
写入 `[Unreleased]`（中文）。

## 输出
report：改动文件 + 选定策略说明 + 清理前后对比 + 完整测试输出。
