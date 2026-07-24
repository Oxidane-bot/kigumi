from __future__ import annotations

from itertools import repeat
from pathlib import Path
from typing import Any

import pytest

import kigumi.dag as dag_module
import kigumi.prompt as prompt_module
import kigumi.repair as repair_module
from kigumi import __version__
from kigumi.artifacts import sha
from kigumi.calling import LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.testing import FakeTransport
from kigumi.transport import Response
from tests._dag_helpers import _load_work, _make_dag


def test_docstring_does_not_change_cache_but_code_does(tmp_path: Path) -> None:
    """教训 code_version: 注释文档不换缓存族，逻辑变更必须换。"""
    first = _load_work(tmp_path / "first.py", "first documentation", 1)
    second = _load_work(tmp_path / "second.py", "rewritten documentation", 1)
    changed = _load_work(tmp_path / "changed.py", "rewritten documentation", 2)
    events: list[tuple[str, bool]] = []

    first_dag = _make_dag(tmp_path, lambda name, artifact, hit: events.append((name, hit)))
    first_dag.node("work")(first)
    assert first_dag.run().artifacts["work"] == {"value": 1}

    second_dag = _make_dag(tmp_path, lambda name, artifact, hit: events.append((name, hit)))
    second_dag.node("work")(second)
    assert second_dag.run().cache_hits == ["work"]

    changed_dag = _make_dag(tmp_path, lambda name, artifact, hit: events.append((name, hit)))
    changed_dag.node("work")(changed)
    assert changed_dag.run().artifacts["work"] == {"value": 2}
    assert events == [("work", False), ("work", True), ("work", False)]


def test_kigumi_component_tracks_repair_bytes_and_uses_schema(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """repair.py 参与生成 prompt 字节，改它必须换 L3 的 kigumi 成分。"""
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    node = dag._nodes["work"]
    baseline = dag._key_components(node, {}, dag._libs_hash())["kigumi"]
    original_read_bytes = Path.read_bytes
    repair_path = Path(repair_module.__file__).resolve()

    def changed_read_bytes(path: Path) -> bytes:
        contents = original_read_bytes(path)
        if path.resolve() == repair_path:
            return contents + b"\n# cache-key probe\n"
        return contents

    monkeypatch.setattr(Path, "read_bytes", changed_read_bytes)
    changed = dag._key_components(node, {}, dag._libs_hash())["kigumi"]

    assert changed != baseline
    inputs = dag_module._kigumi_key_inputs()
    assert inputs["schema"] == dag_module.CACHE_SCHEMA
    assert inputs["schema"] == 5
    assert "version" not in inputs
    assert __version__ not in inputs.values()


def test_key_components_lock_exact_label_set(tmp_path: Path) -> None:
    """最小普通节点的标签集合变化必须显式更新缓存键契约。"""
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    components = dag._key_components(dag._nodes["work"], {}, dag._libs_hash())

    assert set(components) == {"source", "libs", "params", "kigumi"}


def test_kigumi_component_tracks_prompt_bytes_and_pydantic_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """prompt.py 字节和 Pydantic 版本都必须换 L3 的 kigumi 成分。"""
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    node = dag._nodes["work"]
    baseline = dag._key_components(node, {}, dag._libs_hash())["kigumi"]
    original_read_bytes = Path.read_bytes
    prompt_path = Path(prompt_module.__file__).resolve()

    def changed_read_bytes(path: Path) -> bytes:
        contents = original_read_bytes(path)
        if path.resolve() == prompt_path:
            return contents + b"\n# cache-key probe\n"
        return contents

    monkeypatch.setattr(Path, "read_bytes", changed_read_bytes)
    prompt_changed = dag._key_components(node, {}, dag._libs_hash())["kigumi"]
    monkeypatch.setattr(dag_module.pydantic, "__version__", "cache-key-probe")
    pydantic_changed = dag._key_components(node, {}, dag._libs_hash())["kigumi"]

    assert prompt_changed != baseline
    assert pydantic_changed != prompt_changed


def test_prompt_upstream_and_params_changes_invalidate_caches(tmp_path: Path) -> None:
    """教训 cache_inputs: 声明输入任一变化都必须级联换节点键。"""
    prompts = tmp_path / "prompts"
    prompts.mkdir()
    prompt = prompts / "draft.md"
    prompt.write_text("version one", encoding="utf-8")
    events: list[tuple[str, bool]] = []

    def run_with(value: int) -> tuple[dict[str, Any], list[str]]:
        dag = _make_dag(tmp_path, lambda name, artifact, hit: events.append((name, hit)))

        @dag.node("source", params={"value": value})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": ctx.params["value"]}

        @dag.node("leaf", deps=("source",), prompts=("draft",))
        def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": inputs["source"]["value"]}

        result = dag.run()
        return result.artifacts, result.cache_hits

    first, first_hits = run_with(1)
    prompt.write_text("version two", encoding="utf-8")
    second, second_hits = run_with(1)
    third, third_hits = run_with(2)

    assert first == {"source": {"value": 1}, "leaf": {"value": 1}}
    assert first_hits == []
    assert second_hits == ["source"]
    assert second == first
    assert third == {"source": {"value": 2}, "leaf": {"value": 2}}
    assert third_hits == []
    assert events.count(("leaf", False)) == 3


def test_map_hashes_shared_upstream_once_per_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """教训 upstream_sha_once: map 的共享上游摘要必须跨 item 复用。"""
    dag = _make_dag(tmp_path)
    shared_artifact = {"payload": "shared input"}
    original_sha = sha
    shared_sha_calls = 0

    def counting_sha(value: Any) -> str:
        nonlocal shared_sha_calls
        if value == shared_artifact:
            shared_sha_calls += 1
        return original_sha(value)

    monkeypatch.setattr("kigumi.dag.sha", counting_sha)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "one"}, {"id": "two"}, {"id": "three"}]}

    @dag.node("shared")
    def shared(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return shared_artifact

    @dag.map("work", items_from=("scan", "items"), deps=("shared",))
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"id": item["id"], "payload": inputs["shared"]["payload"]}

    dag.run()

    assert shared_sha_calls == 1


def test_declared_files_and_library_sources_invalidate_caches(tmp_path: Path) -> None:
    """教训 declared_inputs: File 与 helper 源码都属于节点内容寻址输入。"""
    source_dir = tmp_path / "lib"
    source_dir.mkdir()
    helper = source_dir / "helper.py"
    helper.write_text("VALUE = 1\n", encoding="utf-8")
    source_file = tmp_path / "source.txt"
    source_file.write_text("first", encoding="utf-8")
    config = KigumiConfig(project_root=tmp_path, source_dirs=["lib"])
    events: list[bool] = []

    def run_once() -> list[str]:
        dag = Dag(
            config,
            LLMCaller(FakeTransport(), tmp_path / "llm"),
            post_node=lambda name, artifact, hit: events.append(hit),
        )

        @dag.node("work", files=("source.txt",))
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"value": "stable"}

        return dag.run().cache_hits

    assert run_once() == []
    source_file.write_text("second", encoding="utf-8")
    assert run_once() == []
    helper.write_text("VALUE = 2\n", encoding="utf-8")
    assert run_once() == []
    assert events == [False, False, False]


def test_torn_node_cache_is_recomputed(tmp_path: Path) -> None:
    """教训 torn_cache: 半截缓存文件按 miss 重算重写,不崩 run。"""

    def run_once() -> Any:
        dag = _make_dag(tmp_path)

        @dag.node("work")
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"value": "stable"}

        return dag.run()

    assert run_once().cache_hits == []
    cache_files = list((tmp_path / "artifacts" / "_cache" / "nodes").glob("*.json"))
    assert len(cache_files) == 1
    cache_files[0].write_text('{"artifact": {"val', encoding="utf-8")

    torn = run_once()
    assert torn.cache_hits == []
    assert torn.artifacts["work"] == {"value": "stable"}
    assert run_once().cache_hits == ["work"]


def test_libs_hash_ignores_comment_and_docstring_edits(tmp_path: Path) -> None:
    """教训 libs_granularity: lib 注释/docstring 修订不得让全流水线换族重算。"""
    lib = tmp_path / "lib"
    lib.mkdir()
    module = lib / "util.py"
    module.write_text(
        '"""旧模块说明。"""\n\n\ndef helper():\n    # 旧注释\n    return 1\n', encoding="utf-8"
    )
    config = KigumiConfig(project_root=tmp_path, source_dirs=["lib"])
    transport = FakeTransport(repeat(Response("out", {"total_tokens": 1}, "stop")))
    dag = Dag(config, LLMCaller(transport, tmp_path / "llm"))

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"value": 1}

    assert dag.run().cache_hits == []

    module.write_text(
        '"""新模块说明。"""\n\n\ndef helper():\n    # 新注释\n    return 1\n', encoding="utf-8"
    )
    assert dag.run().cache_hits == ["work"]

    module.write_text(
        '"""新模块说明。"""\n\n\ndef helper():\n    # 新注释\n    return 2\n', encoding="utf-8"
    )
    assert dag.run().cache_hits == []


def test_libs_hash_tolerates_broken_syntax_by_hashing_raw_text(tmp_path: Path) -> None:
    """教训 libs_broken_file: 中途编辑的残破文件不该让只读 plan 崩溃。"""
    lib = tmp_path / "lib"
    lib.mkdir()
    module = lib / "util.py"
    module.write_text("def helper():\n    return 1\n", encoding="utf-8")
    config = KigumiConfig(project_root=tmp_path, source_dirs=["lib"])
    transport = FakeTransport(repeat(Response("out", {"total_tokens": 1}, "stop")))
    dag = Dag(config, LLMCaller(transport, tmp_path / "llm"))

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"value": 1}

    dag.run()
    module.write_text("def helper(:\n", encoding="utf-8")

    assert dag.plan().nodes["work"] == "miss"

    module.write_text("def helper():\n    return 1\n", encoding="utf-8")
    assert dag.plan().nodes["work"] == "hit"
