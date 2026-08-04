from __future__ import annotations

import ast
import functools
import importlib
import sys
import types
from dataclasses import replace
from itertools import repeat
from pathlib import Path
from typing import Any

import pytest

import kigumi._execution as execution_module
import kigumi.dag as dag_module
import kigumi.prompt as prompt_module
import kigumi.repair as repair_module
from kigumi import __version__
from kigumi.artifacts import sha
from kigumi.calling import CacheIntegrityError, LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import Dag, ResourceRequest
from kigumi.prompt import (
    Attachment,
    Message,
    PromptRef,
    PromptResolution,
    PromptSpec,
    ResolvedPrompt,
    ResponseSpec,
)
from kigumi.testing import FakeTransport
from kigumi.transport import Response
from tests._dag_helpers import _load_work, _make_dag


def _assert_registration_rejected(dag: Dag, function: Any) -> None:
    """动态/opaque 节点构造必须在注册期以明确的 raw-I/O 错误硬失败。"""
    with pytest.raises(
        ValueError,
        match=r"Raw file reads are not allowed in node registration",
    ):
        dag.node("work")(function)


def _capture_sidecar_cache_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    """Observe the immutable cache snapshot handed to each run sidecar."""
    captured: dict[str, Any] = {}
    original = execution_module.ExecutionEnvelope.write_sidecar

    def capture(
        self: Any,
        label: str,
        artifact: dict[str, Any],
        cache_key: str | list[str],
        *,
        cache_hit: bool,
        cache_entry: Any = None,
        **kwargs: Any,
    ) -> None:
        if cache_hit:
            captured[label] = cache_entry
        original(
            self,
            label,
            artifact,
            cache_key,
            cache_hit=cache_hit,
            cache_entry=cache_entry,
            **kwargs,
        )

    monkeypatch.setattr(execution_module.ExecutionEnvelope, "write_sidecar", capture)
    return captured


def test_plain_cache_hit_passes_its_single_snapshot_to_sidecar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "cached"}

    captured = _capture_sidecar_cache_entries(monkeypatch)
    dag.run(run_id="plain-prime")
    dag.run(run_id="plain-hit")

    entry = captured["work"]
    assert entry is not None
    assert entry.state == "VALID"
    assert entry.artifact == {"value": "cached"}


@pytest.mark.parametrize("dynamic_kind", ["map", "scan"])
def test_dynamic_cache_hits_pass_each_item_snapshot_to_sidecar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dynamic_kind: str,
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "one"}, {"id": "two"}]}

    if dynamic_kind == "map":

        @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
        def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            del inputs, ctx
            return {"id": item["id"]}

    else:

        @dag.scan(
            "work",
            items_from=("source", "items"),
            key_fn=lambda item: item["id"],
            carry_fn=lambda artifact: artifact["id"],
        )
        def work(
            item: dict[str, str],
            carry: Any,
            inputs: dict[str, Any],
            ctx: Any,
        ) -> dict[str, str]:
            del carry, inputs, ctx
            return {"id": item["id"]}

    captured = _capture_sidecar_cache_entries(monkeypatch)
    dag.run(run_id=f"{dynamic_kind}-prime")
    dag.run(run_id=f"{dynamic_kind}-hit")

    assert captured["work@one"].state == "VALID"
    assert captured["work@two"].state == "VALID"


def test_corrupt_map_item_cache_propagates_without_calling_item(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dag = _make_dag(tmp_path)
    item_calls: list[str] = []
    item_cache_key: str | None = None
    original_write_sidecar = execution_module.ExecutionEnvelope.write_sidecar

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs, ctx
        return {"items": [{"id": "one"}, {"id": "two"}]}

    @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        item_calls.append(item["id"])
        return {"id": item["id"]}

    def capture_item_key(
        self: Any,
        label: str,
        artifact: dict[str, Any],
        cache_key: str | list[str],
        **kwargs: Any,
    ) -> None:
        nonlocal item_cache_key
        if label == "work@one":
            assert isinstance(cache_key, str)
            item_cache_key = cache_key
        original_write_sidecar(self, label, artifact, cache_key, **kwargs)

    monkeypatch.setattr(execution_module.ExecutionEnvelope, "write_sidecar", capture_item_key)
    dag.run(run_id="map-corrupt-prime")
    assert item_calls == ["one", "two"]
    assert item_cache_key is not None

    cache_path = dag.config.artifacts_path / "_cache" / "nodes" / f"{item_cache_key}.json"
    cache_path.write_text('{"artifact": {"broken": true}', encoding="utf-8")

    with pytest.raises(CacheIntegrityError):
        dag.run(run_id="map-corrupt-hit")
    assert item_calls == ["one", "two"]


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
    baseline = dag._key_components(node, {}, dag._libs_hash(node))["kigumi"]
    original_read_bytes = Path.read_bytes
    repair_path = Path(repair_module.__file__).resolve()

    def changed_read_bytes(path: Path) -> bytes:
        contents = original_read_bytes(path)
        if path.resolve() == repair_path:
            return contents + b"\n# cache-key probe\n"
        return contents

    monkeypatch.setattr(Path, "read_bytes", changed_read_bytes)
    changed = dag._key_components(node, {}, dag._libs_hash(node))["kigumi"]

    assert changed != baseline
    inputs = dag_module._kigumi_key_inputs()
    assert inputs["schema"] == dag_module.CACHE_SCHEMA
    assert inputs["schema"] == 7
    assert "version" not in inputs
    assert __version__ not in inputs.values()


def test_key_components_lock_exact_label_set(tmp_path: Path) -> None:
    """最小普通节点的标签集合变化必须显式更新缓存键契约。"""
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    node = dag._nodes["work"]
    components = dag._key_components(node, {}, dag._libs_hash(node))

    assert set(components) == {"source", "libs", "params", "kigumi"}


def test_cache_key_components(tmp_path: Path) -> None:
    """Keep the exact cache-key smoke-test name used by the release checklist."""
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    node = dag._nodes["work"]
    components = dag._key_components(node, {}, dag._libs_hash(node))

    assert set(components) == {"source", "libs", "params", "kigumi"}


def test_resource_declarations_do_not_change_cache_key(tmp_path: Path) -> None:
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    first = _make_dag(tmp_path)
    first.node("work", resources=(ResourceRequest("gpu"),))(work)
    second = _make_dag(tmp_path)
    second.node("work", resources=(ResourceRequest("cpu"),))(work)

    first_node = first._nodes["work"]
    second_node = second._nodes["work"]
    first_components = first._key_components(first_node, {}, first._libs_hash(first_node))
    second_components = second._key_components(second_node, {}, second._libs_hash(second_node))

    assert first_components == second_components


def test_prompt_key_component_tracks_attachment_hash_and_response_schema(
    tmp_path: Path,
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"value": "ok"}

    base = PromptResolution(
        spec_name="managed",
        structure_digest="structure",
        base={},
        layers=(),
        axes=(),
        materials=(),
        rendered_sha256="rendered",
        rendered_bytes=0,
        messages=[Message("user", ["same prompt"])],
        attachments=[Attachment("source.txt", "a" * 64, "text/plain", 4)],
        response_spec=ResponseSpec("schema-a", "structured"),
    )
    changed_attachment = replace(
        base,
        attachments=[Attachment("source.txt", "b" * 64, "text/plain", 4)],
    )
    changed_schema = replace(base, response_spec=ResponseSpec("schema-b", "structured"))
    node = dag._nodes["work"]

    first = dag._key_components(
        node,
        {},
        dag._libs_hash(node),
        prompt_resolutions={"managed": ResolvedPrompt("same prompt", base)},
    )
    attachment_changed = dag._key_components(
        node,
        {},
        dag._libs_hash(node),
        prompt_resolutions={"managed": ResolvedPrompt("same prompt", changed_attachment)},
    )
    schema_changed = dag._key_components(
        node,
        {},
        dag._libs_hash(node),
        prompt_resolutions={"managed": ResolvedPrompt("same prompt", changed_schema)},
    )

    assert first["prompt_specs:managed"] == base.digest
    assert first["prompt_specs:managed"] != attachment_changed["prompt_specs:managed"]
    assert first["prompt_specs:managed"] != schema_changed["prompt_specs:managed"]


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
    baseline = dag._key_components(node, {}, dag._libs_hash(node))["kigumi"]
    original_read_bytes = Path.read_bytes
    prompt_path = Path(prompt_module.__file__).resolve()

    def changed_read_bytes(path: Path) -> bytes:
        contents = original_read_bytes(path)
        if path.resolve() == prompt_path:
            return contents + b"\n# cache-key probe\n"
        return contents

    monkeypatch.setattr(Path, "read_bytes", changed_read_bytes)
    prompt_changed = dag._key_components(node, {}, dag._libs_hash(node))["kigumi"]
    monkeypatch.setattr(dag_module.pydantic, "__version__", "cache-key-probe")
    pydantic_changed = dag._key_components(node, {}, dag._libs_hash(node))["kigumi"]

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

        @dag.node(
            "leaf",
            deps=("source",),
            prompt_specs=(PromptSpec("draft", PromptRef("draft")),),
        )
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


def test_torn_node_cache_fails_authority_bound_run(tmp_path: Path) -> None:
    """Authority-bound execution must not re-enter a provider path after cache corruption."""

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

    with pytest.raises(CacheIntegrityError):
        run_once()
    assert cache_files[0].read_text(encoding="utf-8") == '{"artifact": {"val'


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


def test_libs_hash_falls_back_for_multiple_configured_candidates(
    tmp_path: Path,
) -> None:
    """教训 libs_multiple_candidates: 运行时选择不同源码时不得复用旧产物。"""
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    source_a.mkdir()
    source_b.mkdir()
    module_name = "libs_multiple_candidates_case"
    node_name = "libs_multiple_candidates_node"
    helper_a = source_a / f"{module_name}.py"
    helper_b = source_b / f"{module_name}.py"
    helper_a.write_text("VALUE = 'A'\n", encoding="utf-8")
    helper_b.write_text("VALUE = 'B'\n", encoding="utf-8")
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import {module_name} as helper\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    runtime_paths = {str(source_a), str(source_b)}

    def load_node(runtime_first: Path, runtime_second: Path) -> Any:
        sys.path[:] = [
            str(runtime_first),
            str(runtime_second),
            *(entry for entry in original_sys_path if entry not in runtime_paths),
        ]
        sys.modules.pop(module_name, None)
        sys.modules.pop(node_name, None)
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        assert Path(module.helper.__file__).resolve() == (runtime_first / f"{module_name}.py")
        return module.run

    try:
        first_config = KigumiConfig(
            project_root=tmp_path,
            source_dirs=["src_a", "src_b"],
        )
        first_dag = Dag(first_config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(source_a, source_b))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "A"}

        second_config = KigumiConfig(
            project_root=tmp_path,
            source_dirs=["src_b", "src_a"],
        )
        second_dag = Dag(second_config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(source_b, source_a))
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(module_name, None)
        sys.modules.pop(node_name, None)


def test_libs_hash_fallback_distinguishes_equal_candidates_by_source_identity(
    tmp_path: Path,
) -> None:
    """教训 libs_equal_candidates: 同文源码的运行时选择变化也必须换键。"""
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    source_a.mkdir()
    source_b.mkdir()
    module_name = "libs_equal_candidates_case"
    node_name = "libs_equal_candidates_node"
    helper_a = source_a / f"{module_name}.py"
    helper_b = source_b / f"{module_name}.py"
    helper_source = "from pathlib import Path\nVALUE = Path(__file__).parent.name\n"
    helper_a.write_text(helper_source, encoding="utf-8")
    helper_b.write_text(helper_source, encoding="utf-8")
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import {module_name} as helper\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    runtime_paths = {str(source_a), str(source_b)}

    def load_node(runtime_first: Path, runtime_second: Path) -> Any:
        sys.path[:] = [
            str(runtime_first),
            str(runtime_second),
            *(entry for entry in original_sys_path if entry not in runtime_paths),
        ]
        sys.modules.pop(module_name, None)
        sys.modules.pop(node_name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        assert Path(module.helper.__file__).resolve() == (runtime_first / f"{module_name}.py")
        return module.run

    try:
        first_config = KigumiConfig(
            project_root=tmp_path,
            source_dirs=["src_a", "src_b"],
        )
        first_dag = Dag(first_config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(source_a, source_b))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "src_a"}

        second_config = KigumiConfig(
            project_root=tmp_path,
            source_dirs=["src_b", "src_a"],
        )
        second_dag = Dag(second_config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(source_b, source_a))
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "src_b"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(module_name, None)
        sys.modules.pop(node_name, None)


def test_libs_hash_invalidates_relative_import_across_configured_roots(
    tmp_path: Path,
) -> None:
    """教训 libs_relative_namespace: 相对导入不能漏掉另一源码根的包成员。"""
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    package_name = "libs_relative_namespace_case"
    package_a = source_a / package_name
    package_b = source_b / package_name
    package_a.mkdir(parents=True)
    package_b.mkdir(parents=True)
    helper_a = package_a / "helper.py"
    helper_b = package_b / "helper.py"
    node_path = package_a / "node.py"
    helper_a.write_text("VALUE = 'A'\n", encoding="utf-8")
    helper_b.write_text("VALUE = 'B-v1'\n", encoding="utf-8")
    node_path.write_text(
        "from . import helper\n\ndef run(inputs, ctx):\n    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    runtime_paths = {str(source_a), str(source_b)}
    module_names = {package_name, f"{package_name}.node", f"{package_name}.helper"}
    try:
        sys.path[:] = [
            str(source_b),
            str(source_a),
            *(entry for entry in original_sys_path if entry not in runtime_paths),
        ]
        for name in module_names:
            sys.modules.pop(name, None)
        importlib.invalidate_caches()
        module = importlib.import_module(f"{package_name}.node")
        assert Path(module.helper.__file__).resolve() == helper_b.resolve()

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src_a", "src_b"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "B-v1"}

        helper_b.write_text("VALUE = 'B-version-two'\n", encoding="utf-8")
        sys.modules.pop(f"{package_name}.helper", None)
        importlib.invalidate_caches()
        reloaded = importlib.import_module(f"{package_name}.helper")
        assert reloaded.VALUE == "B-version-two"
        module.helper = reloaded

        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B-version-two"}
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


def test_libs_hash_invalidates_unresolved_importfrom_child_on_extended_package_path(
    tmp_path: Path,
) -> None:
    """教训 libs_importfrom_child: 已解析 base 不能掩盖未解析的运行时 child。"""
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    package_name = "libs_importfrom_child_case"
    package = source_a / package_name
    package.mkdir(parents=True)
    source_b.mkdir()
    package_init = package / "__init__.py"
    node_path = package / "node.py"
    child = source_b / "ghost.py"
    package_init.write_text(
        "from pathlib import Path\n"
        "__path__.append(str(Path(__file__).resolve().parents[2] / 'src_b'))\n",
        encoding="utf-8",
    )
    child.write_text("VALUE = 'B-v1'\n", encoding="utf-8")
    node_path.write_text(
        f"from {package_name} import ghost\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': ghost.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    runtime_paths = {str(source_a), str(source_b)}
    module_names = {package_name, f"{package_name}.node", f"{package_name}.ghost"}
    try:
        sys.path[:] = [
            str(source_a),
            str(source_b),
            *(entry for entry in original_sys_path if entry not in runtime_paths),
        ]
        for name in module_names:
            sys.modules.pop(name, None)
        importlib.invalidate_caches()
        module = importlib.import_module(f"{package_name}.node")
        assert Path(module.ghost.__file__).resolve() == child.resolve()
        # Keep the already-bound runtime child usable while making the static
        # analyzer prove the child from source, rather than trusting sys.modules.
        sys.modules.pop(f"{package_name}.ghost", None)

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src_a", "src_b"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "B-v1"}

        child.write_text("VALUE = 'B-version-two'\n", encoding="utf-8")
        sys.modules.pop(f"{package_name}.ghost", None)
        importlib.invalidate_caches()
        reloaded = importlib.import_module(f"{package_name}.ghost")
        assert reloaded.VALUE == "B-version-two"
        module.ghost = reloaded
        sys.modules.pop(f"{package_name}.ghost", None)

        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B-version-two"}
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


def test_libs_hash_external_package_path_into_configured_source_falls_back(
    tmp_path: Path,
) -> None:
    """教训 libs_external_package_path: external package path 可伸入 configured source。"""
    package_name = "libs_external_package_path_case"
    external_root = tmp_path / "external"
    package = external_root / package_name
    configured = tmp_path / "src" / "plugins"
    package.mkdir(parents=True)
    configured.mkdir(parents=True)
    child = configured / "child.py"
    child.write_text("VALUE = 'v1'\n", encoding="utf-8")
    (package / "__init__.py").write_text(
        f"__path__.append({str(configured)!r})\n",
        encoding="utf-8",
    )
    node_path = tmp_path / "node.py"
    node_path.write_text(
        f"import {package_name}.child as child\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': child.VALUE}\n",
        encoding="utf-8",
    )

    node_name = "libs_external_package_path_node"
    child_name = f"{package_name}.child"
    original_sys_path = list(sys.path)
    try:
        sys.path.insert(0, str(external_root))
        for name in (package_name, child_name, node_name):
            sys.modules.pop(name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        assert Path(module.child.__file__).resolve() == child.resolve()

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src/plugins"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "v1"}

        child.write_text("VALUE = 'version-two'\n", encoding="utf-8")
        sys.modules.pop(child_name, None)
        importlib.invalidate_caches()
        reloaded = importlib.import_module(child_name)
        module.child = reloaded

        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "version-two"}
    finally:
        sys.path[:] = original_sys_path
        for name in (package_name, child_name, node_name):
            sys.modules.pop(name, None)


@pytest.mark.parametrize(
    ("binding_name", "binding_source"),
    [
        ("function", "def function():\n    return 1\n"),
        ("Thing", "class Thing:\n    pass\n"),
        ("CONSTANT", "CONSTANT = 1\n"),
    ],
    ids=["function", "class", "constant"],
)
def test_libs_hash_preserves_importfrom_attribute_granularity(
    tmp_path: Path,
    binding_name: str,
    binding_source: str,
) -> None:
    """教训 libs_importfrom_attributes: 可证明的属性导入仍保持节点粒度。"""
    package = "libs_importfrom_attributes_case"
    source = tmp_path / "src"
    package_dir = source / package
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "base.py").write_text(binding_source, encoding="utf-8")
    unrelated = source / "unrelated.py"
    unrelated.write_text("VALUE = 1\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        f"from {package}.base import {binding_name}\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': 'stable'}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    module_name = "libs_importfrom_attributes_node"
    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        sys.modules.pop(package, None)
        sys.modules.pop(f"{package}.base", None)
        sys.modules.pop(module_name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        assert dag.run().cache_hits == []
        unrelated.write_text("VALUE = 2\n", encoding="utf-8")
        assert dag.run().cache_hits == ["work"]
    finally:
        sys.path[:] = original_sys_path
        for name in (package, f"{package}.base", module_name):
            sys.modules.pop(name, None)


def test_libs_hash_unresolved_importfrom_child_is_ambiguous(tmp_path: Path) -> None:
    """教训 libs_importfrom_unresolved: base 属性不能猜测成 child 安全。"""
    source = tmp_path / "src"
    package = source / "libs_importfrom_unresolved_case"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    base = package / "base.py"
    base.write_text("VALUE = 1\n", encoding="utf-8")
    node = package / "node.py"
    node.write_text(
        "from libs_importfrom_unresolved_case.base import ghost\n",
        encoding="utf-8",
    )
    snapshot = dag_module._capture_libs_source_snapshot([source], project_root=tmp_path)
    analyzer = dag_module._StaticLibsAnalyzer(tmp_path, [source], snapshot)
    tree = ast.parse(node.read_text(encoding="utf-8"))
    statement = tree.body[0]
    assert isinstance(statement, ast.ImportFrom)

    resolution = analyzer._resolve_import(node, "libs_importfrom_unresolved_case.node", statement)

    assert resolution.ambiguous


def test_libs_hash_relative_import_uses_qualified_package_identity(
    tmp_path: Path,
) -> None:
    """教训 libs_relative_package_root: source root 是 package 目录时仍检查 pkg.helper。"""
    package = "libs_relative_package_root_case"
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    package_a = source_a / package
    package_b = source_b / package
    nested_b = package_b / "nested"
    package_a.mkdir(parents=True)
    nested_b.mkdir(parents=True)
    package_init = (
        "from pathlib import Path\n"
        f"__path__.insert(0, str(Path(__file__).resolve().parents[2] / 'src_b' "
        f"/ '{package}' / 'nested'))\n"
    )
    (package_a / "__init__.py").write_text(package_init, encoding="utf-8")
    (package_a / "helper.py").write_text("VALUE = 'A-static'\n", encoding="utf-8")
    runtime_helper = nested_b / "helper.py"
    runtime_helper.write_text("VALUE = 'B-v1'\n", encoding="utf-8")
    node_path = package_a / "node.py"
    node_path.write_text(
        "from . import helper\n\ndef run(inputs, ctx):\n    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    module_names = {package, f"{package}.node", f"{package}.helper"}
    try:
        sys.path[:] = [
            str(source_a),
            str(source_b),
            *(entry for entry in original_sys_path if entry not in {str(source_a), str(source_b)}),
        ]
        for name in module_names:
            sys.modules.pop(name, None)
        importlib.invalidate_caches()
        module = importlib.import_module(f"{package}.node")
        assert Path(module.helper.__file__).resolve() == runtime_helper.resolve()

        config = KigumiConfig(
            project_root=tmp_path,
            source_dirs=[f"src_a/{package}", f"src_b/{package}"],
        )
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "B-v1"}

        runtime_helper.write_text("VALUE = 'B-version-two'\n", encoding="utf-8")
        sys.modules.pop(f"{package}.helper", None)
        importlib.invalidate_caches()
        reloaded = importlib.import_module(f"{package}.helper")
        assert reloaded.VALUE == "B-version-two"
        module.helper = reloaded

        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B-version-two"}
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


def test_libs_hash_selected_closure_binds_source_identity(tmp_path: Path) -> None:
    """教训 libs_selected_identity: 单根 selected closure 也必须区分 A/B。"""
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    source_a.mkdir()
    source_b.mkdir()
    module_name = "libs_selected_identity_case"
    node_name = "libs_selected_identity_node"
    helper_source = "from pathlib import Path\nVALUE = Path(__file__).parent.name\n"
    (source_a / f"{module_name}.py").write_text(helper_source, encoding="utf-8")
    (source_b / f"{module_name}.py").write_text(helper_source, encoding="utf-8")
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import {module_name} as helper\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    runtime_paths = {str(source_a), str(source_b)}

    def load_node(runtime_root: Path) -> Any:
        sys.path[:] = [
            str(runtime_root),
            *(entry for entry in original_sys_path if entry not in runtime_paths),
        ]
        sys.modules.pop(module_name, None)
        sys.modules.pop(node_name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        assert Path(module.helper.__file__).resolve() == (runtime_root / f"{module_name}.py")
        return module.run

    try:
        first_config = KigumiConfig(project_root=tmp_path, source_dirs=["src_a"])
        first_dag = Dag(first_config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(source_a))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "src_a"}

        second_config = KigumiConfig(project_root=tmp_path, source_dirs=["src_b"])
        second_dag = Dag(second_config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(source_b))
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "src_b"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(module_name, None)
        sys.modules.pop(node_name, None)


def test_libs_hash_fallback_binds_loaded_configured_candidate(tmp_path: Path) -> None:
    """教训 libs_fallback_runtime: 固定源码集合也必须绑定实际加载的候选。"""
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    source_a.mkdir()
    source_b.mkdir()
    helper_name = "libs_fallback_runtime_helper"
    node_name = "libs_fallback_runtime_node"
    (source_a / f"{helper_name}.py").write_text("VALUE = 'A'\n", encoding="utf-8")
    (source_b / f"{helper_name}.py").write_text("VALUE = 'B'\n", encoding="utf-8")
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import {helper_name} as helper\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    runtime_paths = {str(source_a), str(source_b)}

    def load_node(runtime_first: Path, runtime_second: Path) -> Any:
        sys.path[:] = [
            str(runtime_first),
            str(runtime_second),
            *(entry for entry in original_sys_path if entry not in runtime_paths),
        ]
        sys.modules.pop(helper_name, None)
        sys.modules.pop(node_name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        assert Path(module.helper.__file__).resolve().parent == runtime_first.resolve()
        return module.run

    try:
        config = KigumiConfig(
            project_root=tmp_path,
            source_dirs=["src_a", "src_b"],
        )
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(source_a, source_b))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "A"}

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(source_b, source_a))
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(helper_name, None)
        sys.modules.pop(node_name, None)


def test_libs_hash_selected_closure_binds_identity_sensitive_unmanaged_owner(
    tmp_path: Path,
) -> None:
    """教训 libs_owner_identity: 未托管 owner 观察模块身份时也必须换键。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text("VALUE = 'configured'\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "import helper\n\ndef run(inputs, ctx):\n    return {'value': __name__}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)

    def load_node(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module.run

    first_name = "libs_unmanaged_owner_identity_a"
    second_name = "libs_unmanaged_owner_identity_b"
    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        sys.modules.pop("helper", None)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])

        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(first_name))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": first_name}

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(second_name))
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": second_name}
    finally:
        sys.path[:] = original_sys_path
        for name in ("helper", first_name, second_name):
            sys.modules.pop(name, None)


def test_libs_hash_binds_bound_owner_getattribute_lookup(tmp_path: Path) -> None:
    """教训 libs_bound_owner_lookup: owner 的单参数反射也不能复用旧别名。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text("VALUE = 'configured'\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "import sys\n"
        "import helper\n\n"
        "OWNER = sys.modules[__name__]\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': OWNER.__getattribute__('__name__')}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    module_names = ("libs_bound_owner_lookup_a", "libs_bound_owner_lookup_b")

    def load_node(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module.run

    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])

        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(module_names[0]))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": module_names[0]}

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(module_names[1]))
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": module_names[1]}
    finally:
        sys.path[:] = original_sys_path
        for name in (*module_names, "helper"):
            sys.modules.pop(name, None)


def test_libs_hash_recovers_detached_owner_from_retained_function_facts(
    tmp_path: Path,
) -> None:
    """教训 libs_detached_owner: 模块摘除后仍须用一致的函数事实区分 owner。"""
    source = tmp_path / "src"
    source.mkdir()
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "def run(inputs, ctx):\n    return {'value': __name__}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    module_names = ("libs_detached_owner_a", "libs_detached_owner_b")

    def load_detached(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        function = module.run
        sys.modules.pop(module_name, None)
        return function

    try:
        sys.path[:] = [entry for entry in original_sys_path if entry != str(source)]
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])

        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_detached(module_names[0]))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": module_names[0]}

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_detached(module_names[1]))
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": module_names[1]}
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


def test_libs_hash_fails_closed_for_inconsistent_detached_owner_facts(
    tmp_path: Path,
) -> None:
    """教训 libs_detached_owner_consistency: 不一致事实不能信任任一可变字段。"""
    source = tmp_path / "src"
    source.mkdir()
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "def run(inputs, ctx):\n    return {'value': __name__}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    module_names = ("libs_inconsistent_owner_a", "libs_inconsistent_owner_b")

    def load_inconsistent(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        module.run.__module__ = "libs_inconsistent_owner_mutation"
        function = module.run
        sys.modules.pop(module_name, None)
        return function

    try:
        sys.path[:] = [entry for entry in original_sys_path if entry != str(source)]
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])

        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_inconsistent(module_names[0]))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": module_names[0]}

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_inconsistent(module_names[1]))
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": module_names[1]}
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


@pytest.mark.parametrize(
    ("prelude", "expression"),
    [
        pytest.param(
            "import builtins\nidentity_globals = builtins.globals\n",
            'identity_globals()["__name__"]',
            id="aliased-globals",
        ),
        pytest.param("", "run.__module__", id="function-module"),
        pytest.param("", 'run.__globals__["__name__"]', id="function-globals"),
        pytest.param(
            "",
            'object.__getattribute__(run, "__globals__")["__name__"]',
            id="object-getattribute-function-globals",
        ),
        pytest.param(
            "import builtins\nlookup = builtins.getattr\n",
            'lookup(run, "__globals__")["__name__"]',
            id="aliased-getattr-function-globals",
        ),
    ],
)
def test_libs_hash_binds_reflective_owner_identity(
    tmp_path: Path, prelude: str, expression: str
) -> None:
    """仍可证明的 owner reflection 继续覆盖 L3 identity 绑定。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text("VALUE = 'configured'\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        prelude
        + "import helper\n\n"
        + "def run(inputs, ctx):\n"
        + f"    return {{'value': {expression}}}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)

    def load_node(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module.run

    first_name = "libs_owner_dynamic_identity_a"
    second_name = "libs_owner_dynamic_identity_b"
    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        sys.modules.pop("helper", None)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])

        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(first_name))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": first_name}

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(second_name))
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": second_name}
    finally:
        sys.path[:] = original_sys_path
        for name in ("helper", first_name, second_name):
            sys.modules.pop(name, None)


def test_registration_rejects_builtins_globals(tmp_path: Path) -> None:
    """0.13 hard cut: builtins.globals() is opaque at registration time."""
    node_name = "libs_builtins_globals_rejected"
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "import builtins\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': builtins.globals()['__name__']}\n",
        encoding="utf-8",
    )
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=[]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.modules.pop(node_name, None)


def test_registration_rejects_getattr_function_globals(tmp_path: Path) -> None:
    """0.13 hard cut: getattr(run, "__globals__") 必须注册期拒绝。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text("VALUE = 'configured'\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "import helper\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': getattr(run, '__globals__')['__name__']}\n",
        encoding="utf-8",
    )
    node_name = "libs_owner_getattr_globals_rejected"
    original_sys_path = list(sys.path)
    try:
        sys.path.insert(0, str(source))
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=["src"]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop("helper", None)
        sys.modules.pop(node_name, None)


@pytest.mark.parametrize(
    ("prelude", "expression"),
    [
        pytest.param("", 'globals()["__name__"]', id="direct-globals"),
        pytest.param("", 'eval("__name__")', id="eval"),
        pytest.param(
            "import sys\nOWNER = sys.modules[__name__]\nlookup_name = '__name__'\n",
            "getattr(OWNER, lookup_name)",
            id="dynamic-owner-lookup",
        ),
    ],
)
def test_registration_rejects_opaque_owner_reflection(
    tmp_path: Path, prelude: str, expression: str
) -> None:
    """opaque namespace/eval/dynamic getattr 不再进入 L3 owner 分析。"""
    node_path = tmp_path / "node.py"
    node_path.write_text(
        prelude + "\ndef run(inputs, ctx):\n" + f"    return {{'value': {expression}}}\n",
        encoding="utf-8",
    )
    node_name = "libs_owner_dynamic_rejected"
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=[]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.modules.pop(node_name, None)


def test_libs_hash_ignores_identity_sensitive_sibling_function(tmp_path: Path) -> None:
    """教训 libs_owner_sibling: 未注册 sibling 的反射不应污染节点身份。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text("VALUE = 'configured'\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "import helper\n\n"
        "def sibling():\n"
        "    return globals()['__name__']\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': 1}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)

    def load_node(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module.run

    first_name = "libs_owner_sibling_a"
    second_name = "libs_owner_sibling_b"
    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        sys.modules.pop("helper", None)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])

        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(first_name))
        assert first_dag.run().cache_hits == []

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(second_name))
        assert second_dag.run().cache_hits == ["work"]
    finally:
        sys.path[:] = original_sys_path
        for name in ("helper", first_name, second_name):
            sys.modules.pop(name, None)


def test_libs_hash_binds_partial_getattr_on_registered_function(tmp_path: Path) -> None:
    """partial 绑定 receiver 的 getattr 仍须把注册函数视作 owner 事实。"""
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "import functools\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': lookup('__globals__')['__name__']}\n\n"
        "lookup = functools.partial(getattr, run)\n",
        encoding="utf-8",
    )
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    names = ("libs_partial_getattr_a", "libs_partial_getattr_b")
    try:
        first = _load_libs_runtime_module(node_path, names[0])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(first.run)
        assert first_dag.run().artifacts["work"] == {"value": names[0]}

        second = _load_libs_runtime_module(node_path, names[1])
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(second.run)
        result = second_dag.run()
        assert result.cache_hits == []
        assert result.artifacts["work"] == {"value": names[1]}
    finally:
        for name in names:
            sys.modules.pop(name, None)


def test_libs_hash_ignores_module_identity_name_in_docstring(tmp_path: Path) -> None:
    """教训 libs_owner_docstring: 身份名称只在 docstring 中出现时仍应复用。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text("VALUE = 'configured'\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        'import helper\n\ndef run(inputs, ctx):\n    """__name__"""\n    return {\'value\': 1}\n',
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)

    def load_node(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module.run

    first_name = "libs_owner_docstring_a"
    second_name = "libs_owner_docstring_b"
    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        sys.modules.pop("helper", None)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])

        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(first_name))
        assert first_dag.run().cache_hits == []

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(second_name))
        assert second_dag.run().cache_hits == ["work"]
    finally:
        sys.path[:] = original_sys_path
        for name in ("helper", first_name, second_name):
            sys.modules.pop(name, None)


def test_registration_rejects_getattr_helper_value(tmp_path: Path) -> None:
    """0.13 hard cut: getattr(helper, "VALUE") 必须注册期拒绝。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text("VALUE = 'configured'\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "import helper\n\ndef run(inputs, ctx):\n    return {'value': getattr(helper, 'VALUE')}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)

    def load_node(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module.run

    first_name = "libs_owner_reflection_a"
    second_name = "libs_owner_reflection_b"
    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        sys.modules.pop("helper", None)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])

        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        _assert_registration_rejected(dag, load_node(first_name))
    finally:
        sys.path[:] = original_sys_path
        for name in ("helper", first_name, second_name):
            sys.modules.pop(name, None)


@pytest.mark.parametrize(
    ("helper_source", "node_source", "expected"),
    [
        pytest.param(
            "VALUE = 'configured'\n",
            "import helper\n\n"
            "def run(inputs, ctx):\n"
            "    __name__ = 'constant'\n"
            "    return {'value': __name__}\n",
            "constant",
            id="local-name-binding",
        ),
        pytest.param(
            "def eval(value):\n    return 'helper-eval'\n",
            "import helper\n\n"
            "def run(inputs, ctx):\n"
            "    return {'value': helper.eval('constant')}\n",
            "helper-eval",
            id="helper-eval",
        ),
        pytest.param(
            "VALUE = 'configured'\n",
            "class Box:\n"
            "    globals = 'object-globals'\n\n"
            "obj = Box()\n\n"
            "def run(inputs, ctx):\n"
            "    return {'value': obj.globals}\n",
            "object-globals",
            id="object-globals-attribute",
        ),
    ],
)
def test_libs_hash_owner_reflection_is_receiver_and_scope_aware(
    tmp_path: Path,
    helper_source: str,
    node_source: str,
    expected: str,
) -> None:
    """教训 libs_owner_receiver_scope: helper/局部同名反射不得绑定 owner。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text(helper_source, encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(node_source, encoding="utf-8")
    original_sys_path = list(sys.path)
    module_names = ("libs_owner_receiver_scope_a", "libs_owner_receiver_scope_b")

    def load_node(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module.run

    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])

        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(module_names[0]))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": expected}

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(module_names[1]))
        second = second_dag.run()
        assert second.cache_hits == ["work"]
        assert second.artifacts["work"] == {"value": expected}
    finally:
        sys.path[:] = original_sys_path
        for name in (*module_names, "helper"):
            sys.modules.pop(name, None)


def test_registration_rejects_getattr_helper_name(tmp_path: Path) -> None:
    """0.13 hard cut: getattr(helper, "__name__") 必须注册期拒绝。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text("VALUE = 'configured'\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "import helper\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': getattr(helper, '__name__')}\n",
        encoding="utf-8",
    )
    node_name = "libs_owner_getattr_name_rejected"
    original_sys_path = list(sys.path)
    try:
        sys.path.insert(0, str(source))
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=["src"]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop("helper", None)
        sys.modules.pop(node_name, None)


def test_libs_hash_selected_closure_binds_identity_sensitive_owner_path(
    tmp_path: Path,
) -> None:
    """教训 libs_owner_path: owner 观察 __file__ 时稳定路径也必须换键。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text("VALUE = 'configured'\n", encoding="utf-8")
    node_a = tmp_path / "node_a.py"
    node_b = tmp_path / "node_b.py"
    node_source = "import helper\n\ndef run(inputs, ctx):\n    return {'value': __file__}\n"
    node_a.write_text(node_source, encoding="utf-8")
    node_b.write_text(node_source, encoding="utf-8")
    module_name = "libs_unmanaged_owner_path"
    original_sys_path = list(sys.path)

    def load_node(path: Path) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module.run

    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        sys.modules.pop("helper", None)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])

        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(node_a))
        first = first_dag.run()
        assert first.cache_hits == []
        assert Path(first.artifacts["work"]["value"]).resolve() == node_a.resolve()

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(node_b))
        second = second_dag.run()
        assert second.cache_hits == []
        assert Path(second.artifacts["work"]["value"]).resolve() == node_b.resolve()
    finally:
        sys.path[:] = original_sys_path
        for name in ("helper", module_name):
            sys.modules.pop(name, None)


def test_libs_hash_fallback_binds_retained_imported_function_origin(
    tmp_path: Path,
) -> None:
    """教训 libs_retained_binding: 脱离模块注册表的 imported function 仍须绑定来源。"""
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    source_a.mkdir()
    source_b.mkdir()
    helper_name = "libs_retained_binding_helper"
    node_name = "libs_retained_binding_node"
    (source_a / f"{helper_name}.py").write_text("def value():\n    return 'A'\n", encoding="utf-8")
    (source_b / f"{helper_name}.py").write_text("def value():\n    return 'B'\n", encoding="utf-8")
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"from {helper_name} import value\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': value()}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    runtime_paths = {str(source_a), str(source_b)}

    def load_helper(path: Path) -> Any:
        sys.modules.pop(helper_name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(helper_name, path)
        assert spec is not None
        assert spec.loader is not None
        helper = importlib.util.module_from_spec(spec)
        sys.modules[helper_name] = helper
        spec.loader.exec_module(helper)
        value = helper.value
        sys.modules.pop(helper_name, None)
        return value

    try:
        sys.path[:] = [
            str(source_a),
            str(source_b),
            *(entry for entry in original_sys_path if entry not in runtime_paths),
        ]
        sys.modules.pop(node_name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        sys.modules.pop(helper_name, None)

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src_a", "src_b"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(module.run)
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "A"}

        module.value = load_helper(source_b / f"{helper_name}.py")
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(module.run)
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(helper_name, None)
        sys.modules.pop(node_name, None)


def test_libs_hash_fallback_binds_callable_within_configured_file(tmp_path: Path) -> None:
    """教训 libs_callable_identity: 同一 configured file 内不同 callable 也必须换键。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_callable_identity_helper"
    node_name = "libs_callable_identity_node"
    (source / f"{helper_name}.py").write_text(
        "first, second = (lambda: 'first'), (lambda: 'second')\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import {helper_name} as helper\n\n"
        "loader = __import__\n"
        "runner = helper.first\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        for name in (helper_name, node_name):
            sys.modules.pop(name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        sys.modules.pop(helper_name, None)

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(module.run)
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "first"}

        module.runner = module.helper.second
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(module.run)
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "second"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(helper_name, None)
        sys.modules.pop(node_name, None)


@pytest.mark.parametrize(
    "prefix",
    ["# module comment\n\n", '"""module documentation"""\n\n'],
    ids=["comment", "docstring"],
)
def test_libs_hash_retained_callable_ignores_prefix_comments_and_docstrings(
    tmp_path: Path, prefix: str
) -> None:
    """教训 libs_callable_location_noise: retained callable 不应使用首行号换键。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_callable_prefix_helper"
    node_name = "libs_callable_prefix_node"
    helper_path = source / f"{helper_name}.py"
    function_source = "def value():\n    return 'stable'\n"
    helper_path.write_text(function_source, encoding="utf-8")
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "import importlib\n"
        f"from {helper_name} import value\n\n"
        "runner = value\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        for name in (helper_name, node_name):
            sys.modules.pop(name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(module.run)
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "stable"}

        helper_path.write_text(prefix + function_source, encoding="utf-8")
        sys.modules.pop(helper_name, None)
        importlib.invalidate_caches()
        reloaded = importlib.import_module(helper_name)
        module.runner = reloaded.value

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(module.run)
        second = second_dag.run()
        assert second.cache_hits == ["work"]
        assert second.artifacts["work"] == {"value": "stable"}
    finally:
        sys.path[:] = original_sys_path
        for name in (helper_name, node_name):
            sys.modules.pop(name, None)


def test_libs_hash_fallback_binds_retained_partial_origin(tmp_path: Path) -> None:
    """教训 libs_retained_partial: callable wrapper 仍须追踪底层函数来源。"""
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    source_a.mkdir()
    source_b.mkdir()
    helper_name = "libs_retained_partial_helper"
    node_name = "libs_retained_partial_node"
    helper_a = source_a / f"{helper_name}.py"
    helper_b = source_b / f"{helper_name}.py"
    helper_a.write_text("def value():\n    return 'A'\n", encoding="utf-8")
    helper_b.write_text("def value():\n    return 'B'\n", encoding="utf-8")
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "from functools import partial\n"
        f"from {helper_name} import value\n\n"
        "runner = partial(value)\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    runtime_paths = {str(source_a), str(source_b)}

    def load_function(path: Path) -> Any:
        sys.modules.pop(helper_name, None)
        spec = importlib.util.spec_from_file_location(helper_name, path)
        assert spec is not None
        assert spec.loader is not None
        helper = importlib.util.module_from_spec(spec)
        sys.modules[helper_name] = helper
        spec.loader.exec_module(helper)
        function = helper.value
        sys.modules.pop(helper_name, None)
        return function

    try:
        sys.path[:] = [
            str(source_a),
            str(source_b),
            *(entry for entry in original_sys_path if entry not in runtime_paths),
        ]
        sys.modules.pop(node_name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        sys.modules.pop(helper_name, None)

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src_a", "src_b"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(module.run)
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "A"}

        module.runner = functools.partial(load_function(helper_b))
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(module.run)
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(helper_name, None)
        sys.modules.pop(node_name, None)


def test_libs_hash_fallback_binds_configured_wrapped_target(tmp_path: Path) -> None:
    """教训 libs_wrapped_target: wrapper 自身有来源时仍须绑定 __wrapped__。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_wrapped_target_helper"
    node_name = "libs_wrapped_target_node"
    helper_path = source / f"{helper_name}.py"
    helper_path.write_text(
        "from functools import wraps\n\n"
        "def target_a():\n    return 'A'\n\n"
        "def target_b():\n    return 'B'\n\n"
        "def wrapper(target):\n"
        "    @wraps(target)\n"
        "    def call():\n"
        "        return target()\n"
        "    return call\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "import importlib\n"
        f"from {helper_name} import target_a, wrapper\n\n"
        "runner = wrapper(target_a)\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        for name in (helper_name, node_name):
            sys.modules.pop(name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        helper = sys.modules[helper_name]

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(module.run)
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "A"}

        module.runner = module.wrapper(helper.target_b)
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(module.run)
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        for name in (helper_name, node_name):
            sys.modules.pop(name, None)


def test_libs_hash_fallback_wrapper_cycle_is_safe(tmp_path: Path) -> None:
    """教训 libs_wrapper_cycle: __wrapped__ 环必须只展开一次并安全停止。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_wrapper_cycle_helper"
    node_name = "libs_wrapper_cycle_node"
    (source / f"{helper_name}.py").write_text(
        "def target():\n    return 'stable'\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "import importlib\n"
        f"from {helper_name} import target\n\n"
        "runner = target\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    try:
        sys.path[:] = [
            str(source),
            *(entry for entry in original_sys_path if entry != str(source)),
        ]
        for name in (helper_name, node_name):
            sys.modules.pop(name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        module.runner.__wrapped__ = module.runner

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        assert isinstance(dag._libs_hash(dag._nodes["work"]), str)
    finally:
        sys.path[:] = original_sys_path
        for name in (helper_name, node_name):
            sys.modules.pop(name, None)


def test_libs_hash_fallback_ignores_unreferenced_callable_global(tmp_path: Path) -> None:
    """教训 libs_unreferenced_global: fallback 不应绑定节点未观察的 callable。"""
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    source_a.mkdir()
    source_b.mkdir()
    helper_name = "libs_unreferenced_global_helper"
    node_name = "libs_unreferenced_global_node"
    helper_source = "def value():\n    return 'A'\n"
    (source_a / f"{helper_name}.py").write_text(helper_source, encoding="utf-8")
    (source_b / f"{helper_name}.py").write_text(
        helper_source.replace("'A'", "'B'"), encoding="utf-8"
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "import importlib\n\ndef run(inputs, ctx):\n    return {'value': 'stable'}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    runtime_paths = {str(source_a), str(source_b)}

    def load_value(path: Path) -> Any:
        sys.modules.pop(helper_name, None)
        spec = importlib.util.spec_from_file_location(helper_name, path)
        assert spec is not None
        assert spec.loader is not None
        helper = importlib.util.module_from_spec(spec)
        sys.modules[helper_name] = helper
        spec.loader.exec_module(helper)
        function = helper.value
        sys.modules.pop(helper_name, None)
        return function

    try:
        sys.path[:] = [
            str(source_a),
            str(source_b),
            *(entry for entry in original_sys_path if entry not in runtime_paths),
        ]
        sys.modules.pop(node_name, None)
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src_a", "src_b"])
        module.unused = load_value(source_a / f"{helper_name}.py")
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(module.run)
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "stable"}

        module.unused = load_value(source_b / f"{helper_name}.py")
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(module.run)
        second = second_dag.run()
        assert second.cache_hits == ["work"]
        assert second.artifacts["work"] == {"value": "stable"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(node_name, None)
        sys.modules.pop(helper_name, None)


@pytest.mark.parametrize(
    ("prelude", "body"),
    [
        pytest.param(
            "import builtins\nget_globals = builtins.globals\n",
            "return {'value': get_globals()['VALUE']}",
            id="aliased-builtins-globals",
        ),
    ],
)
def test_libs_hash_fallback_binds_allowed_global_alias(
    tmp_path: Path, prelude: str, body: str
) -> None:
    """保留可证明的全局 alias cache-key 覆盖。"""
    source = tmp_path / "src"
    source.mkdir()
    node_name = "libs_dynamic_globals_node"
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "import importlib\n"
        + prelude
        + "VALUE = 'A'\n\n"
        + "def run(inputs, ctx):\n"
        + "    "
        + body
        + "\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    try:
        sys.modules.pop(node_name, None)
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(module.run)
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "A"}

        module.VALUE = "B"
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(module.run)
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(node_name, None)


@pytest.mark.parametrize(
    ("prelude", "body"),
    [
        pytest.param("", "return {'value': eval('VALUE')}", id="eval"),
        pytest.param("", "return {'value': globals()['VALUE']}", id="globals"),
        pytest.param(
            "",
            "namespace = {}\n"
            "    exec('result = VALUE', globals(), namespace)\n"
            "    return {'value': namespace['result']}",
            id="exec",
        ),
    ],
)
def test_registration_rejects_dynamic_global_introspection(
    tmp_path: Path, prelude: str, body: str
) -> None:
    """globals/eval/exec 的不透明命名空间观察必须在注册期硬失败。"""
    node_name = "libs_dynamic_globals_rejected"
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "VALUE = 'A'\n\n" + prelude + "\ndef run(inputs, ctx):\n" + "    " + body + "\n",
        encoding="utf-8",
    )
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=[]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.modules.pop(node_name, None)


def test_libs_hash_fallback_binds_retained_imported_value(tmp_path: Path) -> None:
    """教训 libs_retained_value: 脱离模块注册表的 imported value 也必须绑定内容。"""
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    source_a.mkdir()
    source_b.mkdir()
    helper_name = "libs_retained_value_helper"
    node_name = "libs_retained_value_node"
    helper_a = source_a / f"{helper_name}.py"
    helper_b = source_b / f"{helper_name}.py"
    helper_a.write_text("VALUE = 'A'\n", encoding="utf-8")
    helper_b.write_text("VALUE = 'B'\n", encoding="utf-8")
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"from {helper_name} import VALUE\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': (lambda: VALUE)()}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    runtime_paths = {str(source_a), str(source_b)}

    def load_value(path: Path) -> str:
        sys.modules.pop(helper_name, None)
        spec = importlib.util.spec_from_file_location(helper_name, path)
        assert spec is not None
        assert spec.loader is not None
        helper = importlib.util.module_from_spec(spec)
        sys.modules[helper_name] = helper
        spec.loader.exec_module(helper)
        value = helper.VALUE
        sys.modules.pop(helper_name, None)
        return value

    try:
        sys.path[:] = [
            str(source_a),
            str(source_b),
            *(entry for entry in original_sys_path if entry not in runtime_paths),
        ]
        sys.modules.pop(node_name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        sys.modules.pop(helper_name, None)

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src_a", "src_b"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(module.run)
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "A"}

        module.VALUE = load_value(helper_b)
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(module.run)
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(helper_name, None)
        sys.modules.pop(node_name, None)


def test_registration_rejects_node_dynamic_import(tmp_path: Path) -> None:
    """节点内 __import__ 不再绕过注册期 raw-I/O 硬切。"""
    package_name = "libs_package_parent_order_case"
    root_a = tmp_path / "root_a"
    root_b = tmp_path / "root_b"
    package_a = root_a / package_name
    package_b = root_b / package_name
    package_a.mkdir(parents=True)
    package_b.mkdir(parents=True)
    (package_a / "__init__.py").write_text("VALUE = 'A'\n", encoding="utf-8")
    (package_b / "__init__.py").write_text("VALUE = 'B'\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "def run(inputs, ctx):\n"
        f"    package = __import__({package_name!r})\n"
        "    return {'value': package.VALUE}\n",
        encoding="utf-8",
    )

    spec = importlib.util.spec_from_file_location("libs_package_parent_node", node_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    config = KigumiConfig(
        project_root=tmp_path,
        source_dirs=[f"root_a/{package_name}", f"root_b/{package_name}"],
    )
    dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
    _assert_registration_rejected(dag, module.run)


def test_libs_hash_selected_closure_binds_qualified_module_name(tmp_path: Path) -> None:
    """教训 libs_selected_module_name: 同一路径换限定模块名也必须换键。"""
    source = tmp_path / "src"
    source.mkdir()
    node_path = source / "node.py"
    node_path.write_text(
        "def run(inputs, ctx):\n    return {'value': __name__}\n",
        encoding="utf-8",
    )

    def load_node(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module.run

    first_name = "libs_selected_module_identity_a"
    second_name = "libs_selected_module_identity_b"
    try:
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(first_name))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": first_name}

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(second_name))
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": second_name}
    finally:
        sys.modules.pop(first_name, None)
        sys.modules.pop(second_name, None)


def test_libs_hash_upward_relative_import_validates_qualified_sibling(
    tmp_path: Path,
) -> None:
    """教训 libs_relative_upward: 向上相对导入必须校验限定 sibling 路径。"""
    package_name = "libs_relative_upward_case"
    package = tmp_path / package_name
    subpackage = package / "sub"
    alternate = package / "alternate"
    subpackage.mkdir(parents=True)
    alternate.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (subpackage / "__init__.py").write_text("", encoding="utf-8")
    (package / "config.py").write_text("VALUE = 'static'\n", encoding="utf-8")
    runtime_config = alternate / "config.py"
    runtime_config.write_text("VALUE = 'runtime-v1'\n", encoding="utf-8")
    node_path = subpackage / "node.py"
    node_path.write_text(
        "from .. import config\n\ndef run(inputs, ctx):\n    return {'value': config.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    module_names = {
        package_name,
        f"{package_name}.sub",
        f"{package_name}.config",
        f"{package_name}.sub.node",
    }

    def load_runtime_config(value: str) -> Any:
        runtime_config.write_text(f"VALUE = {value!r}\n", encoding="utf-8")
        qualified_name = f"{package_name}.config"
        sys.modules.pop(qualified_name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(qualified_name, runtime_config)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[qualified_name] = module
        spec.loader.exec_module(module)
        sys.modules[package_name].config = module
        return module

    try:
        sys.path[:] = [
            str(tmp_path),
            *(entry for entry in original_sys_path if entry != str(tmp_path)),
        ]
        for name in module_names:
            sys.modules.pop(name, None)
        importlib.invalidate_caches()
        importlib.import_module(package_name)
        load_runtime_config("runtime-v1")
        module = importlib.import_module(f"{package_name}.sub.node")
        assert Path(module.config.__file__).resolve() == runtime_config.resolve()

        config = KigumiConfig(project_root=tmp_path, source_dirs=[package_name])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "runtime-v1"}

        module.config = load_runtime_config("runtime-version-two")
        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "runtime-version-two"}
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


def test_libs_source_universe_matches_configured_snapshot(tmp_path: Path) -> None:
    """教训 libs_source_universe: project root 其余文件不属于 libs 管理域。"""
    source = tmp_path / "src"
    source.mkdir()
    configured = source / "helper.py"
    configured.write_text("VALUE = 1\n", encoding="utf-8")
    outside = tmp_path / "runtime_mod.py"
    outside.write_text("VALUE = 1\n", encoding="utf-8")
    snapshot = dag_module._capture_libs_source_snapshot([source], project_root=tmp_path)
    analyzer = dag_module._StaticLibsAnalyzer(tmp_path, [source], snapshot)

    assert configured.resolve() in snapshot.source_files
    assert outside.resolve() not in snapshot.source_files
    assert analyzer._is_source_universe_path(configured)
    assert not analyzer._is_source_universe_path(outside)


def test_libs_hash_validates_loaded_dotted_import_prefixes(tmp_path: Path) -> None:
    """教训 libs_import_prefix: pkg parent 偏离时不能只相信 pkg.child。"""
    package = "libs_import_prefix_case"
    source_a = tmp_path / "src_a"
    source_b = tmp_path / "src_b"
    package_a = source_a / package
    package_b = source_b / package
    package_a.mkdir(parents=True)
    package_b.mkdir(parents=True)
    child = package_a / "child.py"
    child.write_text("VALUE = 'child'\n", encoding="utf-8")
    (package_a / "__init__.py").write_text("VALUE = 'static-parent'\n", encoding="utf-8")
    parent_b = package_b / "__init__.py"
    parent_b.write_text(
        "from pathlib import Path\n"
        f"__path__.append(str(Path(__file__).resolve().parents[2] / 'src_a' / '{package}'))\n"
        "VALUE = 'runtime-parent-v1'\n",
        encoding="utf-8",
    )
    node_path = tmp_path / "node.py"
    node_path.write_text(
        f"import {package}.child\n\n"
        "def run(inputs, ctx):\n"
        f"    return {{'value': {package}.VALUE}}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    module_name = "libs_import_prefix_node"
    module_names = {package, f"{package}.child", module_name}
    try:
        sys.path[:] = [
            str(source_b),
            str(source_a),
            *(entry for entry in original_sys_path if entry not in {str(source_a), str(source_b)}),
        ]
        for name in module_names:
            sys.modules.pop(name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        assert Path(module.__dict__[package].__file__).resolve() == parent_b.resolve()
        assert Path(module.__dict__[package].child.__file__).resolve() == child.resolve()

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src_a", "src_b"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "runtime-parent-v1"}

        parent_b.write_text(
            parent_b.read_text(encoding="utf-8").replace(
                "runtime-parent-v1", "runtime-parent-version-two"
            ),
            encoding="utf-8",
        )
        importlib.invalidate_caches()
        importlib.reload(sys.modules[package])

        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "runtime-parent-version-two"}
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


def test_libs_hash_missing_canonical_import_entry_fails_closed(tmp_path: Path) -> None:
    """教训 libs_missing_sys_modules: 保留的 helper 对象不能掩盖 registry 缺失。"""
    source = tmp_path / "src"
    deep = source / "deep"
    deep.mkdir(parents=True)
    module_name = "libs_missing_sys_modules_node"
    helper_name = "libs_missing_sys_modules_helper"
    static_helper = source / f"{helper_name}.py"
    runtime_helper = deep / f"{helper_name}.py"
    static_helper.write_text("VALUE = 'static'\n", encoding="utf-8")
    runtime_helper.write_text("VALUE = 'deep-v1'\n", encoding="utf-8")
    node_path = tmp_path / f"{module_name}.py"
    node_path.write_text(
        f"import {helper_name} as helper\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    runtime_paths = {str(source), str(deep)}

    def load_runtime_helper(value: str) -> Any:
        runtime_helper.write_text(f"VALUE = {value!r}\n", encoding="utf-8")
        sys.modules.pop(helper_name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(helper_name, runtime_helper)
        assert spec is not None
        assert spec.loader is not None
        helper = importlib.util.module_from_spec(spec)
        sys.modules[helper_name] = helper
        spec.loader.exec_module(helper)
        sys.modules.pop(helper_name, None)
        return helper

    try:
        sys.path[:] = [
            str(deep),
            str(source),
            *(entry for entry in original_sys_path if entry not in runtime_paths),
        ]
        sys.modules.pop(module_name, None)
        sys.modules.pop(helper_name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        assert Path(module.helper.__file__).resolve() == runtime_helper.resolve()
        sys.modules.pop(helper_name, None)

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "deep-v1"}

        module.helper = load_runtime_helper("deep-version-two")
        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "deep-version-two"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(module_name, None)
        sys.modules.pop(helper_name, None)


def test_libs_hash_falls_back_for_loaded_runtime_module_mismatch(
    tmp_path: Path,
) -> None:
    """教训 libs_loaded_runtime: 已加载模块路径偏离静态候选时不得漏算。"""
    source = tmp_path / "src"
    deep = source / "deep"
    deep.mkdir(parents=True)
    module_name = "libs_loaded_runtime_case"
    node_name = "libs_loaded_runtime_node"
    static_helper = source / f"{module_name}.py"
    runtime_helper = deep / f"{module_name}.py"
    static_helper.write_text("VALUE = 'static'\n", encoding="utf-8")
    runtime_helper.write_text("VALUE = 'deep-runtime-v1'\n", encoding="utf-8")
    node_path = source / f"{node_name}.py"
    node_path.write_text(
        f"import {module_name} as helper\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    try:
        sys.path[:] = [
            str(deep),
            str(source),
            *(entry for entry in original_sys_path if entry not in {str(deep), str(source)}),
        ]
        sys.modules.pop(module_name, None)
        sys.modules.pop(node_name, None)
        module = importlib.import_module(node_name)
        assert Path(module.helper.__file__).resolve() == runtime_helper.resolve()

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "deep-runtime-v1"}

        runtime_helper.write_text("VALUE = 'deep-runtime-version-two'\n", encoding="utf-8")
        sys.modules.pop(module_name, None)
        reloaded = importlib.import_module(module_name)
        assert Path(reloaded.__file__).resolve() == runtime_helper.resolve()
        assert reloaded.VALUE == "deep-runtime-version-two"
        module.helper = reloaded

        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "deep-runtime-version-two"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(module_name, None)
        sys.modules.pop(node_name, None)


@pytest.mark.parametrize(
    ("case_name", "dynamic_import", "dynamic_call"),
    [
        (
            "builtins_alias",
            "from builtins import __import__ as load\n",
            "    helper = load('{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "importlib_function_alias",
            "from importlib import import_module as load\n",
            "    helper = load('{package_name}.helper')\n",
        ),
        (
            "importlib_module_alias",
            "import importlib as il\n",
            "    helper = il.import_module('{package_name}.helper')\n",
        ),
        (
            "builtins_assignment_alias",
            "load = __import__\n",
            "    helper = load('{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "builtins_module_assignment_alias",
            "import builtins\nload = builtins.__import__\n",
            "    helper = load('{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "builtins_getattr_assignment_alias",
            'import builtins\nload = getattr(builtins, "__import__")\n',
            "    helper = load('{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "builtins_dict_assignment_alias",
            'import builtins\nload = builtins.__dict__["__import__"]\n',
            "    helper = load('{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "builtins_dict_direct",
            "import builtins\n",
            "    helper = builtins.__dict__['__import__']('"
            "{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "builtins_getattr_computed_assignment_alias",
            'import builtins\nname = "__" + "import__"\nload = getattr(builtins, name)\n',
            "    helper = load('{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "builtins_dict_computed_assignment_alias",
            'import builtins\nname = "__" + "import__"\nload = builtins.__dict__[name]\n',
            "    helper = load('{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "globals_builtins_mapping_computed_assignment_alias",
            'name = "__" + "import__"\n'
            'builtins_map = globals()["__builtins__"]\n'
            "load = builtins_map[name]\n",
            "    helper = load('{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "locals_builtins_mapping_computed_assignment_alias",
            'name = "__" + "import__"\n'
            'builtins_map = locals()["__builtins__"]\n'
            "load = builtins_map[name]\n",
            "    helper = load('{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "vars_builtins_mapping_computed_assignment_alias",
            'name = "__" + "import__"\n'
            'builtins_map = vars()["__builtins__"]\n'
            "load = builtins_map[name]\n",
            "    helper = load('{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "builtins_getattribute_computed_direct",
            'import builtins\nname = "__" + "import__"\n',
            "    helper = builtins.__getattribute__(name)('"
            "{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "sys_modules_builtins_dict_computed_alias",
            'import sys\nname = "__" + "import__"\nload = sys.modules["builtins"].__dict__[name]\n',
            "    helper = load('{package_name}.helper', fromlist=['VALUE'])\n",
        ),
        (
            "walrus_target",
            "",
            '    helper = (load := __import__)("{package_name}.helper", fromlist=["VALUE"])\n',
        ),
    ],
)
def test_libs_hash_falls_back_for_aliased_dynamic_imports(
    tmp_path: Path,
    case_name: str,
    dynamic_import: str,
    dynamic_call: str,
) -> None:
    """教训 libs_dynamic_alias: 无法静态证明动态导入引用时必须退回全量源码。"""
    package_name = f"libs_dynamic_alias_{case_name}"
    source = tmp_path / "src" / package_name
    source.mkdir(parents=True)
    package_init = source / "__init__.py"
    helper = source / "helper.py"
    node_path = source / "node.py"
    package_init.write_text("", encoding="utf-8")
    helper.write_text("VALUE = 1\n", encoding="utf-8")
    node_path.write_text(
        dynamic_import
        + "\n"
        + "def run(inputs, ctx):\n"
        + dynamic_call.format(package_name=package_name)
        + "    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    module_names = (package_name, f"{package_name}.node", f"{package_name}.helper")
    try:
        sys.path[:] = [
            str(tmp_path / "src"),
            *(entry for entry in original_sys_path if entry != str(tmp_path / "src")),
        ]
        for name in module_names:
            sys.modules.pop(name, None)
        module = importlib.import_module(f"{package_name}.node")

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": 1}
        loaded_helper = sys.modules[f"{package_name}.helper"]
        assert Path(loaded_helper.__file__).resolve() == helper.resolve()

        helper.write_text("VALUE = 222\n", encoding="utf-8")
        sys.modules.pop(f"{package_name}.helper", None)
        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": 222}
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


@pytest.mark.parametrize(
    ("case_name", "dynamic_import", "dynamic_call"),
    [
        pytest.param(
            "attribute_target",
            "class Box:\n    pass\n\nbox = Box()\n",
            "    box.load = __import__\n"
            '    helper = box.load("{package_name}.helper", fromlist=["VALUE"])\n',
            id="attribute-target",
        ),
        pytest.param(
            "container_target",
            "loaders = {}\n",
            '    loaders["x"] = __import__\n'
            '    helper = loaders["x"]("{package_name}.helper", fromlist=["VALUE"])\n',
            id="container-target",
        ),
    ],
)
def test_registration_rejects_dynamic_import_alias_containers(
    tmp_path: Path,
    case_name: str,
    dynamic_import: str,
    dynamic_call: str,
) -> None:
    """0.13 hard cut: opaque __import__ aliases cannot enter a node."""
    package_name = f"libs_dynamic_alias_rejected_{case_name}"
    source = tmp_path / "src" / package_name
    source.mkdir(parents=True)
    (source / "__init__.py").write_text("", encoding="utf-8")
    (source / "helper.py").write_text("VALUE = 1\n", encoding="utf-8")
    node_path = source / "node.py"
    node_path.write_text(
        dynamic_import
        + "\n"
        + "def run(inputs, ctx):\n"
        + dynamic_call.format(package_name=package_name)
        + "    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    module_names = (package_name, f"{package_name}.node")
    try:
        sys.path.insert(0, str(tmp_path / "src"))
        for name in module_names:
            sys.modules.pop(name, None)
        module = importlib.import_module(f"{package_name}.node")
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=["src"]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


def test_registration_rejects_direct_builtin_getattr_import(tmp_path: Path) -> None:
    """0.13 hard cut: getattr(builtins, "__import__") 必须注册期拒绝。"""
    package_name = "libs_direct_getattr_import_rejected"
    source = tmp_path / "src" / package_name
    source.mkdir(parents=True)
    (source / "__init__.py").write_text("", encoding="utf-8")
    (source / "helper.py").write_text("VALUE = 1\n", encoding="utf-8")
    node_path = source / "node.py"
    node_path.write_text(
        "import builtins\n\n"
        "def run(inputs, ctx):\n"
        f"    helper = getattr(builtins, '__import__')({package_name + '.helper'!r}, "
        "fromlist=['VALUE'])\n"
        "    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    module_names = (package_name, f"{package_name}.node")
    try:
        sys.path.insert(0, str(tmp_path / "src"))
        for name in module_names:
            sys.modules.pop(name, None)
        module = importlib.import_module(f"{package_name}.node")
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=["src"]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


def test_registration_rejects_computed_dynamic_import_lookup(tmp_path: Path) -> None:
    """通过动态 getattr 取得 __import__ 的节点必须注册期失败。"""
    package_name = "libs_dynamic_import_rejected"
    source = tmp_path / "src" / package_name
    source.mkdir(parents=True)
    (source / "__init__.py").write_text("", encoding="utf-8")
    (source / "helper.py").write_text("VALUE = 1\n", encoding="utf-8")
    node_path = source / "node.py"
    node_path.write_text(
        "import builtins\n"
        'name = "__" + "import__"\n\n'
        "def run(inputs, ctx):\n"
        f"    helper = getattr(builtins, name)({package_name + '.helper'!r}, fromlist=['VALUE'])\n"
        "    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    module_names = (package_name, f"{package_name}.node")
    try:
        sys.path.insert(0, str(tmp_path / "src"))
        for name in module_names:
            sys.modules.pop(name, None)
        module = importlib.import_module(f"{package_name}.node")
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=["src"]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


def test_libs_hash_falls_back_for_importlib_submodule_alias(tmp_path: Path) -> None:
    """教训 libs_importlib_submodule: importlib 子模块别名也必须退回全量源码。"""
    package_name = "libs_importlib_submodule_case"
    source = tmp_path / "src" / package_name
    source.mkdir(parents=True)
    helper = source / "helper.py"
    node_path = source / "node.py"
    (source / "__init__.py").write_text("", encoding="utf-8")
    helper.write_text("VALUE = 1\n", encoding="utf-8")
    node_path.write_text(
        "import types\n"
        "from importlib.util import find_spec as load\n\n"
        "def run(inputs, ctx):\n"
        f"    spec = load({f'{package_name}.helper'!r})\n"
        "    helper = types.ModuleType(spec.name)\n"
        "    spec.loader.exec_module(helper)\n"
        "    return {'value': helper.VALUE}\n",
        encoding="utf-8",
    )

    original_sys_path = list(sys.path)
    module_names = (package_name, f"{package_name}.node")
    try:
        sys.path[:] = [
            str(tmp_path / "src"),
            *(entry for entry in original_sys_path if entry != str(tmp_path / "src")),
        ]
        for name in module_names:
            sys.modules.pop(name, None)
        module = importlib.import_module(f"{package_name}.node")

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": 1}

        helper.write_text("VALUE = 222\n", encoding="utf-8")
        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": 222}
    finally:
        sys.path[:] = original_sys_path
        for name in (*module_names, f"{package_name}.helper"):
            sys.modules.pop(name, None)


def test_libs_hash_tracks_ancestor_package_init(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """教训 libs_ancestor_init: 节点包初始化器执行后也必须让节点换键。"""
    source = tmp_path / "src"
    package_name = "libs_ancestor_case_m4"
    package = source / package_name
    package.mkdir(parents=True)
    package_init = package / "__init__.py"
    package_init.write_text("SETTING = 1\n", encoding="utf-8")
    node_path = package / "nodes.py"
    node_path.write_text(
        "import sys\n\n"
        "def run(inputs, ctx):\n"
        f"    return {{'value': sys.modules[{package_name!r}].SETTING}}\n",
        encoding="utf-8",
    )
    for index in range(3):
        (source / f"padding_{index}.py").write_text(f"VALUE = {index}\n", encoding="utf-8")

    synthetic_name = "graph_mod_m4"
    monkeypatch.syspath_prepend(str(source))
    monkeypatch.delitem(sys.modules, package_name, raising=False)
    monkeypatch.delitem(sys.modules, synthetic_name, raising=False)
    package_module = importlib.import_module(package_name)
    assert package_module.SETTING == 1

    spec = importlib.util.spec_from_file_location(synthetic_name, node_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, synthetic_name, module)
    spec.loader.exec_module(module)
    assert module.__name__ == synthetic_name

    config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
    dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
    dag.node("work")(module.run)

    ancestor_calls: list[Path] = []
    original_ancestor_package_inits = dag_module._StaticLibsAnalyzer._ancestor_package_inits

    def record_ancestor_package_inits(
        analyzer: dag_module._StaticLibsAnalyzer, path: Path
    ) -> set[Path]:
        ancestor_calls.append(path)
        return original_ancestor_package_inits(analyzer, path)

    monkeypatch.setattr(
        dag_module._StaticLibsAnalyzer,
        "_ancestor_package_inits",
        record_ancestor_package_inits,
    )

    assert dag.run().cache_hits == []
    assert ancestor_calls == [node_path.resolve()]

    package_init.write_text("SETTING = 2\n", encoding="utf-8")
    assert dag.run().cache_hits == []


def test_libs_hash_tracks_unresolved_import_inside_source_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """教训 libs_unresolved_import: 运行时落入源码树的未解析导入也必须让节点换键。"""
    source = tmp_path / "src"
    deep = source / "deep"
    deep.mkdir(parents=True)
    inner = deep / "inner.py"
    inner.write_text("VALUE = 1\n", encoding="utf-8")
    node_path = source / "nodes_m5.py"
    node_path.write_text(
        "import inner\n\ndef run(inputs, ctx):\n    return {'value': inner.VALUE}\n",
        encoding="utf-8",
    )
    for index in range(3):
        (source / f"padding_{index}.py").write_text(f"VALUE = {index}\n", encoding="utf-8")

    monkeypatch.syspath_prepend(str(source))
    monkeypatch.syspath_prepend(str(deep))
    for name in ("nodes_m5", "inner"):
        monkeypatch.delitem(sys.modules, name, raising=False)

    try:
        module = importlib.import_module("nodes_m5")
        assert Path(module.inner.__file__).resolve() == inner.resolve()

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        assert dag.run().cache_hits == []
        inner.write_text("VALUE = 2\n", encoding="utf-8")
        assert dag.run().cache_hits == []
    finally:
        sys.modules.pop("nodes_m5", None)
        sys.modules.pop("inner", None)


def test_libs_hash_follows_transitive_imports_per_node(tmp_path: Path) -> None:
    """教训 libs_granularity: 每个节点只因自己可达的库文件失效。"""
    package = "libs_graph_case"
    source = tmp_path / package
    source.mkdir()
    (source / "__init__.py").write_text("", encoding="utf-8")
    (source / "used.py").write_text("def value():\n    return 1\n", encoding="utf-8")
    (source / "transitive.py").write_text(
        f"from {package}.used import value\n\ndef compute():\n    return value()\n",
        encoding="utf-8",
    )
    unrelated = source / "unrelated.py"
    unrelated.write_text("VALUE = 1\n", encoding="utf-8")
    (source / "node_a.py").write_text(
        f"from {package}.transitive import compute\n\ndef run(inputs, ctx):\n"
        "    return {'value': compute()}\n",
        encoding="utf-8",
    )
    (source / "node_b.py").write_text(
        "def run(inputs, ctx):\n    return {'value': 'independent'}\n",
        encoding="utf-8",
    )

    sys.path.insert(0, str(tmp_path))
    try:
        node_a = importlib.import_module(f"{package}.node_a")
        node_b = importlib.import_module(f"{package}.node_b")
        config = KigumiConfig(project_root=tmp_path, source_dirs=[package])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("a")(node_a.run)
        dag.node("b")(node_b.run)

        assert dag.run().cache_hits == []
        unrelated.write_text("VALUE = 2\n", encoding="utf-8")
        assert dag.run().cache_hits == ["a", "b"]

        (source / "used.py").write_text("def value():\n    return 2\n", encoding="utf-8")
        assert dag.run().cache_hits == ["b"]
    finally:
        sys.path.remove(str(tmp_path))
        for name in list(sys.modules):
            if name == package or name.startswith(f"{package}."):
                sys.modules.pop(name, None)


@pytest.mark.parametrize(
    "ambiguous_source",
    [
        "import importlib\n",
        "if True:\n    import libs_ambiguous_case.exported\n",
        "from libs_ambiguous_case.exported import *\n",
        "def __getattr__(name):\n    raise AttributeError(name)\n",
    ],
    ids=["importlib", "conditional-import", "star-import", "module-getattr"],
)
def test_libs_hash_falls_back_for_ambiguous_imports(tmp_path: Path, ambiguous_source: str) -> None:
    """教训 libs_fallback: 不能证明闭包时必须退回全量源码。"""
    package = "libs_ambiguous_case"
    source = tmp_path / package
    source.mkdir()
    (source / "__init__.py").write_text("", encoding="utf-8")
    (source / "exported.py").write_text("VALUE = 1\n", encoding="utf-8")
    unrelated = source / "unrelated.py"
    unrelated.write_text("VALUE = 1\n", encoding="utf-8")
    (source / "node.py").write_text(
        f"{ambiguous_source}\ndef run(inputs, ctx):\n    return {{'value': 1}}\n",
        encoding="utf-8",
    )

    sys.path.insert(0, str(tmp_path))
    try:
        module = importlib.import_module(f"{package}.node")
        config = KigumiConfig(project_root=tmp_path, source_dirs=[package])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        assert dag.run().cache_hits == []
        unrelated.write_text("VALUE = 2\n", encoding="utf-8")
        assert dag.run().cache_hits == []
    finally:
        sys.path.remove(str(tmp_path))
        for name in list(sys.modules):
            if name == package or name.startswith(f"{package}."):
                sys.modules.pop(name, None)


@pytest.mark.parametrize(
    "reflection_source",
    [
        "def run(inputs, ctx):\n    return getattr(inputs, 'value', None)\n",
        "def run(inputs, ctx):\n    return globals()\n",
        "def run(inputs, ctx):\n    return locals()\n",
        "def run(inputs, ctx):\n    return vars(inputs)\n",
        "def run(inputs, ctx):\n    return inputs.__dict__\n",
        "def run(inputs, ctx):\n    return inputs.__getattribute__('value')\n",
        "def run(inputs, ctx):\n    return __builtins__\n",
        "import builtins\n\ndef run(inputs, ctx):\n    return builtins\n",
        "from builtins import object\n\ndef run(inputs, ctx):\n    return object\n",
        "import sys\n\ndef run(inputs, ctx):\n    return sys.modules\n",
    ],
    ids=[
        "getattr",
        "globals",
        "locals",
        "vars",
        "dict",
        "getattribute",
        "builtins-name",
        "builtins-import",
        "builtins-from-import",
        "sys-modules",
    ],
)
def test_common_reflection_is_conservatively_ambiguous(reflection_source: str) -> None:
    """教训 libs_reflection_boundary: 普通反射也必须宁可多失效而不漏算。"""
    assert dag_module._module_imports_are_ambiguous(ast.parse(reflection_source))


def test_dynamic_callable_reference_without_call_is_ambiguous() -> None:
    """教训 libs_dynamic_reference: 引用动态原语本身就必须退回全量源码。"""
    assert dag_module._module_imports_are_ambiguous(ast.parse("load = __import__\n"))


def test_libs_hash_falls_back_when_node_module_is_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """教训 libs_module_identity: 无法定位节点模块时不得猜测可达文件。"""
    lib = tmp_path / "lib"
    lib.mkdir()
    unrelated = lib / "unrelated.py"
    unrelated.write_text("VALUE = 1\n", encoding="utf-8")
    config = KigumiConfig(project_root=tmp_path, source_dirs=["lib"])
    dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))

    @dag.node("work")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        return {"value": 1}

    node = dag._nodes["work"]
    monkeypatch.setattr(dag_module.inspect, "getmodule", lambda function: None)
    before = dag._libs_hash(node)
    unrelated.write_text("VALUE = 2\n", encoding="utf-8")
    after = dag._libs_hash(node)

    assert after != before


def test_libs_hash_fallback_binds_aliased_builtin_getattr_dynamic_owner(
    tmp_path: Path,
) -> None:
    """教训 libs_builtin_getattr_alias: 别名动态读取 owner 全局时必须失效。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text("VALUE = 'helper'\n", encoding="utf-8")
    node_path = tmp_path / "libs_builtin_getattr_alias_node.py"
    node_path.write_text(
        "import builtins\n"
        "import sys\n"
        "import helper\n\n"
        "OWNER = sys.modules[__name__]\n"
        "identity_getattr = builtins.getattr\n"
        "dynamic_name = 'VALUE'\n"
        "VALUE = 'A'\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': identity_getattr(OWNER, dynamic_name)}\n",
        encoding="utf-8",
    )
    module_name = "libs_builtin_getattr_alias_node"

    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, node_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "A"}

        module.VALUE = "B"
        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(module_name, None)
        sys.modules.pop("helper", None)


def test_libs_hash_selected_closure_binds_registered_code_filename(
    tmp_path: Path,
) -> None:
    """教训 libs_code_filename: registered run 的 code filename 属于 owner 事实。"""
    source = tmp_path / "src"
    source.mkdir()
    node_a = tmp_path / "node_a.py"
    node_b = tmp_path / "node_b.py"
    code = "def run(inputs, ctx):\n    return {'value': run.__code__.co_filename}\n"
    node_a.write_text(code, encoding="utf-8")
    node_b.write_text(code, encoding="utf-8")
    module_name = "libs_code_filename_node"

    def load_node(path: Path) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    try:
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_module = load_node(node_a)
        first_dag.node("work")(first_module.run)
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": str(node_a)}

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_module = load_node(node_b)
        second_dag.node("work")(second_module.run)
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": str(node_b)}
    finally:
        sys.modules.pop(module_name, None)


def test_libs_hash_code_filename_ignores_unrelated_callable_receiver(
    tmp_path: Path,
) -> None:
    """教训 libs_code_filename_receiver: 同名属性不能替代 registered receiver。"""
    source = tmp_path / "src"
    source.mkdir()
    helper = source / "helper.py"
    helper.write_text("def target():\n    return 'stable'\n", encoding="utf-8")
    node_path = source / "node.py"
    node_path.write_text(
        "from helper import target\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': target.__code__.co_filename}\n",
        encoding="utf-8",
    )
    module_names = ("libs_code_filename_receiver", "libs_code_filename_receiver")

    def load_node(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(module_names[0]).run)
        first = first_dag.run()
        assert first.cache_hits == []

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(module_names[1]).run)
        second = second_dag.run()
        assert second.cache_hits == ["work"]
        assert second.artifacts["work"] == {"value": str(helper)}
    finally:
        sys.path[:] = original_sys_path
        for name in (*module_names, "helper"):
            sys.modules.pop(name, None)


@pytest.mark.parametrize(
    "case",
    [
        "closure",
        "defaults",
        "kwdefaults",
        "bound_method",
        "callable_instance",
        "callable_slots",
        "opaque_descriptor",
        "partial",
        "wrapper",
    ],
)
def test_libs_hash_fallback_binds_retained_callable_runtime_state(
    tmp_path: Path, case: str
) -> None:
    """教训 libs_callable_state: 保留 callable 的结果状态必须进入 fallback 键。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_callable_state_helper"
    helper = source / f"{helper_name}.py"
    helper.write_text(
        "from functools import wraps\n\n"
        "def make_closure(value):\n"
        "    def target():\n"
        "        return value\n"
        "    return target\n\n"
        "def default_target(value='A'):\n"
        "    return value\n\n"
        "def kw_target(*, value='A'):\n"
        "    return value\n\n"
        "class Box:\n"
        "    def __init__(self, value):\n"
        "        self.value = value\n\n"
        "    def get(self):\n"
        "        return self.value\n\n"
        "class CallableBox:\n"
        "    def __init__(self, value):\n"
        "        self.value = value\n\n"
        "    def __call__(self):\n"
        "        return self.value\n\n"
        "class CallableSlots:\n"
        "    __slots__ = ('value',)\n\n"
        "    def __init__(self, value):\n"
        "        self.value = value\n\n"
        "    def __call__(self):\n"
        "        return self.value\n\n"
        "class OpaqueDescriptor:\n"
        "    def __init__(self, value):\n"
        "        self.value = value\n\n"
        "    def __get__(self, instance, owner):\n"
        "        return self.value\n\n"
        "class OpaqueCallable:\n"
        "    result = OpaqueDescriptor('A')\n\n"
        "    def __call__(self):\n"
        "        return self.result\n\n"
        "def partial_target(prefix='A'):\n"
        "    return prefix\n\n"
        "def base_target():\n"
        "    return 'target'\n\n"
        "def make_wrapper(target, tag):\n"
        "    @wraps(target)\n"
        "    def wrapped():\n"
        "        return target() + wrapped.tag\n"
        "    wrapped.tag = tag\n"
        "    return wrapped\n",
        encoding="utf-8",
    )
    setup = {
        "closure": f"from {helper_name} import make_closure\nrunner = make_closure('A')\n",
        "defaults": f"from {helper_name} import default_target\nrunner = default_target\n",
        "kwdefaults": f"from {helper_name} import kw_target\nrunner = kw_target\n",
        "bound_method": f"from {helper_name} import Box\nrunner = Box('A').get\n",
        "callable_instance": f"from {helper_name} import CallableBox\nrunner = CallableBox('A')\n",
        "callable_slots": f"from {helper_name} import CallableSlots\nrunner = CallableSlots('A')\n",
        "opaque_descriptor": (
            f"from {helper_name} import OpaqueCallable\nrunner = OpaqueCallable()\n"
        ),
        "partial": (
            "from functools import partial\n"
            f"from {helper_name} import partial_target\n"
            "runner = partial(partial_target, prefix='A')\n"
        ),
        "wrapper": (
            f"from {helper_name} import base_target, make_wrapper\n"
            "runner = make_wrapper(base_target, 'A')\n"
        ),
    }[case]
    node_name = f"libs_callable_state_{case}"
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "import importlib\n" + setup + "\ndef run(inputs, ctx):\n    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    sys.modules.pop(node_name, None)
    sys.modules.pop(helper_name, None)
    spec = importlib.util.spec_from_file_location(node_name, node_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[node_name] = module
    try:
        spec.loader.exec_module(module)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        first = dag.run()
        assert first.cache_hits == []
        expected_first = "targetA" if case == "wrapper" else "A"
        assert first.artifacts["work"] == {"value": expected_first}

        if case == "closure":
            module.runner = module.make_closure("B")
        elif case == "defaults":
            module.runner.__defaults__ = ("B",)
        elif case == "kwdefaults":
            module.runner.__kwdefaults__ = {"value": "B"}
        elif case == "bound_method":
            module.runner.__self__.value = "B"
        elif case in {"callable_instance", "callable_slots"}:
            module.runner.value = "B"
        elif case == "opaque_descriptor":
            type(module.runner).__dict__["result"].value = "B"
        elif case == "partial":
            module.runner.keywords["prefix"] = "B"
        else:
            module.runner.tag = "B"

        second = dag.run()
        assert second.cache_hits == []
        expected_second = "targetB" if case == "wrapper" else "B"
        assert second.artifacts["work"] == {"value": expected_second}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


@pytest.mark.parametrize("dynamic", [False, True], ids=["simple-global", "dynamic-namespace"])
def test_libs_hash_fallback_binds_retained_callable_own_globals(
    tmp_path: Path, dynamic: bool
) -> None:
    """教训 libs_callable_globals: retained callable 的 globals 也要递归取证。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_callable_globals_helper"
    helper = source / f"{helper_name}.py"
    body = "return globals()['VALUE']" if dynamic else "return VALUE"
    helper.write_text(
        f"VALUE = 'A'\nUNUSED = 'unrelated'\n\ndef target():\n    {body}\n",
        encoding="utf-8",
    )
    node_name = "libs_callable_globals_node"
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "import importlib\n"
        f"from {helper_name} import target\n\n"
        "runner = target\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    sys.modules.pop(node_name, None)
    sys.modules.pop(helper_name, None)
    spec = importlib.util.spec_from_file_location(node_name, node_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[node_name] = module
    try:
        spec.loader.exec_module(module)
        helper_module = sys.modules.pop(helper_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "A"}

        module.runner.__globals__["VALUE"] = "B"
        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "B"}
        assert helper_module.__dict__["VALUE"] == "B"
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(node_name, None)
        sys.modules.pop(helper_name, None)


def test_libs_hash_fallback_binds_referenced_sibling_owner_identity(
    tmp_path: Path,
) -> None:
    """教训 libs_referenced_sibling: 被调用 sibling 的 owner 事实不能漏算。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_path = source / "helper.py"
    helper_path.write_text(
        "def sibling():\n    return __name__\n",
        encoding="utf-8",
    )
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "import importlib\n\ndef run(inputs, ctx):\n    return {'value': sibling()}\n",
        encoding="utf-8",
    )
    module_names = ("libs_referenced_sibling_a", "libs_referenced_sibling_b")
    node_name = "libs_referenced_sibling_node"

    def load_function(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        helper_spec = importlib.util.spec_from_file_location(module_name, helper_path)
        assert helper_spec is not None
        assert helper_spec.loader is not None
        helper_module = importlib.util.module_from_spec(helper_spec)
        sys.modules[module_name] = helper_module
        helper_spec.loader.exec_module(helper_module)
        helper_function = helper_module.sibling
        helper_function.__module__ = "shared_referenced_sibling"
        sys.modules.pop(module_name, None)

        sys.modules.pop(node_name, None)
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        module.sibling = helper_function
        return module.run

    try:
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_function(module_names[0]))
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": module_names[0]}

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_function(module_names[1]))
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": module_names[1]}
    finally:
        for name in (*module_names, node_name):
            sys.modules.pop(name, None)


def test_libs_hash_validates_descendant_external_package_prefix_path(
    tmp_path: Path,
) -> None:
    """教训 libs_external_descendant: dotted external import 的 descendant path 也受管。"""
    external_root = tmp_path / "external"
    package = external_root / "libs_external_descendant_pkg"
    subpackage = package / "subpkg"
    configured = tmp_path / "src"
    external_root.mkdir()
    subpackage.mkdir(parents=True)
    configured.mkdir()
    child = configured / "child.py"
    child.write_text("VALUE = 'v1'\n", encoding="utf-8")
    (package / "__init__.py").write_text("", encoding="utf-8")
    (subpackage / "__init__.py").write_text(
        f"__path__.append({str(configured)!r})\n", encoding="utf-8"
    )
    node_path = tmp_path / "libs_external_descendant_node.py"
    node_path.write_text(
        "import libs_external_descendant_pkg.subpkg.child as child\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': child.VALUE}\n",
        encoding="utf-8",
    )
    node_name = "libs_external_descendant_node"
    module_names = (
        "libs_external_descendant_pkg",
        "libs_external_descendant_pkg.subpkg",
        "libs_external_descendant_pkg.subpkg.child",
        node_name,
    )
    original_sys_path = list(sys.path)
    try:
        sys.path[:] = [
            str(external_root),
            *(entry for entry in original_sys_path if entry != str(external_root)),
        ]
        for name in module_names:
            sys.modules.pop(name, None)
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(node_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[node_name] = module
        spec.loader.exec_module(module)
        assert Path(module.child.__file__).resolve() == child.resolve()

        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        first = dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": "v1"}

        child.write_text("VALUE = 'version-two'\n", encoding="utf-8")
        sys.modules.pop("libs_external_descendant_pkg.subpkg.child", None)
        importlib.invalidate_caches()
        module.child = importlib.import_module("libs_external_descendant_pkg.subpkg.child")
        second = dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": "version-two"}
    finally:
        sys.path[:] = original_sys_path
        for name in module_names:
            sys.modules.pop(name, None)


def test_libs_hash_comprehension_target_does_not_shadow_owner_load(
    tmp_path: Path,
) -> None:
    """教训 libs_comprehension_scope: comprehension target 不是外围函数 local。"""
    source = tmp_path / "src"
    source.mkdir()
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "import importlib\n\n"
        "def run(inputs, ctx):\n"
        "    [__name__ for __name__ in ('local',)]\n"
        "    return {'value': __name__}\n",
        encoding="utf-8",
    )
    module_names = ("libs_comprehension_scope_a", "libs_comprehension_scope_b")

    def load_node(module_name: str) -> Any:
        sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(module_name, node_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    try:
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(load_node(module_names[0]).run)
        first = first_dag.run()
        assert first.cache_hits == []
        assert first.artifacts["work"] == {"value": module_names[0]}

        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(load_node(module_names[1]).run)
        second = second_dag.run()
        assert second.cache_hits == []
        assert second.artifacts["work"] == {"value": module_names[1]}
    finally:
        for name in module_names:
            sys.modules.pop(name, None)


def test_registration_rejects_nested_class_even_when_unreached(tmp_path: Path) -> None:
    """nested class 即使不执行也不能作为节点的可分析执行边界。"""
    source = tmp_path / "src"
    source.mkdir()
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "import importlib\n\n"
        "def run(inputs, ctx):\n"
        "    def unrelated():\n"
        "        return __name__\n\n"
        "    class Box:\n"
        "        def method(self):\n"
        "            return __name__\n\n"
        "    return {'value': 'stable'}\n",
        encoding="utf-8",
    )
    node_name = "libs_nested_scope_rejected"
    try:
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.modules.pop(node_name, None)


def test_libs_hash_ignores_unexecuted_nested_helper_identity_body(
    tmp_path: Path,
) -> None:
    """未到达的普通 nested helper 仍不污染合法节点的 owner identity。"""
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "def run(inputs, ctx):\n"
        "    def unrelated():\n"
        "        return __name__\n\n"
        "    return {'value': 'stable'}\n",
        encoding="utf-8",
    )
    module_names = ("libs_nested_helper_a", "libs_nested_helper_b")
    try:
        config = KigumiConfig(project_root=tmp_path, source_dirs=[])
        first = _load_libs_runtime_module(node_path, module_names[0])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(first.run)
        assert first_dag.run().cache_hits == []

        second = _load_libs_runtime_module(node_path, module_names[1])
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(second.run)
        assert second_dag.run().cache_hits == ["work"]
    finally:
        for name in module_names:
            sys.modules.pop(name, None)


def test_libs_hash_does_not_execute_custom_receiver_during_analysis(
    tmp_path: Path,
) -> None:
    """教训 libs_safe_runtime_inspection: key analyzer 不得触发 custom receiver。"""
    source = tmp_path / "src"
    source.mkdir()
    node_name = "libs_safe_runtime_inspection_node"
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "import importlib\n\n"
        "events = []\n\n"
        "class Bomb:\n"
        "    def __getattribute__(self, name):\n"
        "        events.append(name)\n"
        "        if name == 'globals':\n"
        "            raise AssertionError('analyzer invoked custom receiver')\n"
        "        return object.__getattribute__(self, name)\n\n"
        "obj = Bomb()\n\n"
        "def run(inputs, ctx):\n"
        "    if False:\n"
        "        obj.globals\n"
        "    return {'value': 'stable'}\n",
        encoding="utf-8",
    )
    sys.modules.pop(node_name, None)
    spec = importlib.util.spec_from_file_location(node_name, node_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[node_name] = module
    try:
        spec.loader.exec_module(module)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        result = dag.run()
        assert result.cache_hits == []
        assert result.artifacts["work"] == {"value": "stable"}
        assert module.events == []
    finally:
        sys.modules.pop(node_name, None)


def _load_libs_runtime_module(path: Path, module_name: str) -> Any:
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_unrepresentable_callable_state_is_stable_but_not_cache_reusable(
    tmp_path: Path,
) -> None:
    """教训 libs_uncacheable_identity: fail closed 不能随机化 durable declaration。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_uncacheable_identity_helper"
    node_name = "libs_uncacheable_identity_node"
    (source / f"{helper_name}.py").write_text(
        "class Descriptor:\n"
        "    def __get__(self, instance, owner):\n"
        "        return 'stable'\n\n"
        "class Runner:\n"
        "    value = Descriptor()\n\n"
        "    def __call__(self):\n"
        "        return self.value\n\n"
        "runner = Runner()\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        node = dag._nodes["work"]

        assert dag._libs_hash(node) == dag._libs_hash(node)
        assert dag.run().cache_hits == []
        assert dag.run().cache_hits == []
        assert dag.plan().nodes["work"] == "miss"
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_unrepresentable_callable_state_can_resume_same_run(tmp_path: Path) -> None:
    """教训 libs_uncacheable_resume: non-reusable L3 仍须允许同声明 resume。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_uncacheable_resume_helper"
    node_name = "libs_uncacheable_resume_node"
    (source / f"{helper_name}.py").write_text(
        "class Descriptor:\n"
        "    def __get__(self, instance, owner):\n"
        "        return 'stable'\n\n"
        "class Runner:\n"
        "    value = Descriptor()\n\n"
        "    def __call__(self):\n"
        "        return self.value\n\n"
        "runner = Runner()\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import runner\n\n"
        "def run(inputs, ctx):\n"
        "    approval = ctx.checkpoint('review', {'value': runner()})\n"
        "    return {'approval': approval}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        pending = dag.run(run_id="uncacheable-resume")
        assert pending.pending_checkpoints == ["review"]
        dag.approve("uncacheable-resume", "review", {"accepted": True})
        resumed = dag.run(run_id="uncacheable-resume")
        assert resumed.artifacts["work"] == {"approval": {"accepted": True}}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_libs_hash_binds_mutable_callable_class_state(tmp_path: Path) -> None:
    """教训 libs_callable_class_state: callable class 的结果状态不能只看名称。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_callable_class_state_helper"
    node_name = "libs_callable_class_state_node"
    (source / f"{helper_name}.py").write_text(
        "class Runner:\n    result = 'A'\n\n    def __new__(cls):\n        return cls.result\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import Runner as runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": "A"}

        module.runner.result = "B"
        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_libs_hash_binds_callable_alias_topology(tmp_path: Path) -> None:
    """教训 libs_callable_aliases: 相同值图中的共享引用也可能影响结果。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_callable_aliases_helper"
    node_name = "libs_callable_aliases_node"
    (source / f"{helper_name}.py").write_text(
        "def make_runner(shared):\n"
        "    item = []\n"
        "    state = [item, item] if shared else [[], []]\n\n"
        "    def runner():\n"
        "        return state[0] is state[1]\n\n"
        "    return runner\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import make_runner\n\n"
        "runner = make_runner(True)\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": True}

        module.runner = module.make_runner(False)
        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": False}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_libs_hash_does_not_execute_partial_subclass_attributes(tmp_path: Path) -> None:
    """教训 libs_safe_partial: partial state 必须经内建 descriptor 静态读取。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_safe_partial_helper"
    node_name = "libs_safe_partial_node"
    (source / f"{helper_name}.py").write_text(
        "from functools import partial\n\n"
        "events = []\n\n"
        "def target(value):\n"
        "    return value\n\n"
        "class TracedPartial(partial):\n"
        "    def __getattribute__(self, name):\n"
        "        if name in {'func', 'args', 'keywords'}:\n"
        "            events.append(name)\n"
        "        return super().__getattribute__(name)\n\n"
        "runner = TracedPartial(target, 'stable')\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import events, runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        module.events.clear()
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        dag._libs_hash(dag._nodes["work"])
        assert module.events == []
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_libs_hash_does_not_execute_custom_metaclass_during_analysis(
    tmp_path: Path,
) -> None:
    """教训 libs_safe_metaclass: source 定位不得触发 metaclass __getattribute__。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_safe_metaclass_helper"
    node_name = "libs_safe_metaclass_node"
    (source / f"{helper_name}.py").write_text(
        "events = []\n\n"
        "class Meta(type):\n"
        "    def __getattribute__(cls, name):\n"
        "        if name == '__module__':\n"
        "            events.append(name)\n"
        "        return super().__getattribute__(name)\n\n"
        "class Runner(metaclass=Meta):\n"
        "    def __new__(cls):\n"
        "        return 'stable'\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import Runner as runner, events\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        module.events.clear()
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        dag._libs_hash(dag._nodes["work"])
        assert module.events == []
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_representable_callable_instance_state_remains_cache_reusable(
    tmp_path: Path,
) -> None:
    """教训 libs_callable_reuse: 普通 dict/slots state 不得被内建 descriptor 污染。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_callable_reuse_helper"
    node_name = "libs_callable_reuse_node"
    (source / f"{helper_name}.py").write_text(
        "class DictRunner:\n"
        "    def __init__(self):\n"
        "        self.value = 'stable'\n\n"
        "    def __call__(self):\n"
        "        return self.value\n\n"
        "class SlotsRunner:\n"
        "    __slots__ = ['value']\n\n"
        "    def __init__(self):\n"
        "        self.value = 'stable'\n\n"
        "    def __call__(self):\n"
        "        return self.value\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        helper = importlib.import_module(helper_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        for runner_name in ("DictRunner", "SlotsRunner"):
            node_path = tmp_path / f"{node_name}_{runner_name}.py"
            module_name = f"{node_name}_{runner_name}"
            node_path.write_text(
                f"import importlib\nfrom {helper_name} import {runner_name}\n\n"
                f"runner = {runner_name}()\n\n"
                "def run(inputs, ctx):\n"
                "    return {'value': runner()}\n",
                encoding="utf-8",
            )
            module = _load_libs_runtime_module(node_path, module_name)
            dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / f"llm-{runner_name}"))
            dag.node("work")(module.run)
            assert dag.run().cache_hits == []
            assert dag.run().cache_hits == ["work"]
            module.runner.value = "changed"
            changed = dag.run()
            assert changed.cache_hits == []
            assert changed.artifacts["work"] == {"value": "changed"}
            sys.modules.pop(module_name, None)
        assert helper is sys.modules[helper_name]
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(helper_name, None)


def test_safe_type_attribute_reads_every_slot_descriptor_kind() -> None:
    """教训 mro_member_descriptor: type slot 读取不得因 descriptor 种类而 fail closed。

    ``type.__mro__`` 在 3.11 是 member_descriptor、3.12+ 是 getset_descriptor。
    只接受后者会让 ``_safe_type_mro`` 在 3.11 整体返回 None，
    使所有 runtime state 判为 unrepresentable 并静默丢失缓存复用。
    """

    class Runner:
        def __call__(self) -> str:
            return "stable"

    for name in ("__mro__", "__module__", "__qualname__", "__dict__"):
        descriptor = type.__dict__.get(name)
        assert type(descriptor) in (
            types.GetSetDescriptorType,
            types.MemberDescriptorType,
        ), f"unexpected descriptor kind for type.{name}: {type(descriptor).__name__}"
        assert dag_module._safe_type_attribute(Runner, name) is not (
            dag_module._UNRESOLVED_RUNTIME_VALUE
        ), f"type.{name} must resolve on this interpreter"

    mro = dag_module._safe_type_mro(Runner)
    assert mro is not None
    assert mro[0] is Runner
    assert mro[-1] is object


def test_called_nested_body_binds_owner_module_identity(tmp_path: Path) -> None:
    """教训 libs_nested_call: 实际调用的 nested body 可观察 owner identity。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "broken.py").write_text("def broken(:\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "def run(inputs, ctx):\n"
        "    def owner_name():\n"
        "        return __name__\n\n"
        "    return {'value': owner_name()}\n",
        encoding="utf-8",
    )
    module_names = ("libs_called_nested_a", "libs_called_nested_b")
    config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
    try:
        first = _load_libs_runtime_module(node_path, module_names[0])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(first.run)
        assert first_dag.run().artifacts["work"] == {"value": module_names[0]}

        second = _load_libs_runtime_module(node_path, module_names[1])
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(second.run)
        result = second_dag.run()
        assert result.cache_hits == []
        assert result.artifacts["work"] == {"value": module_names[1]}
    finally:
        for name in module_names:
            sys.modules.pop(name, None)


def test_partial_bound_getattr_binds_owner_module_identity(tmp_path: Path) -> None:
    """教训 libs_partial_owner: partial 绑定的 lookup receiver 仍可观察 owner。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "broken.py").write_text("def broken(:\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "from functools import partial\n"
        "import sys\n\n"
        "owner_name = partial(getattr, sys.modules[__name__])\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': owner_name('__name__')}\n",
        encoding="utf-8",
    )
    module_names = ("libs_partial_owner_a", "libs_partial_owner_b")
    config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
    try:
        first = _load_libs_runtime_module(node_path, module_names[0])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(first.run)
        assert first_dag.run().artifacts["work"] == {"value": module_names[0]}

        second = _load_libs_runtime_module(node_path, module_names[1])
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(second.run)
        result = second_dag.run()
        assert result.cache_hits == []
        assert result.artifacts["work"] == {"value": module_names[1]}
    finally:
        for name in module_names:
            sys.modules.pop(name, None)


def test_callable_state_preserves_dict_order_and_wrapped_data(tmp_path: Path) -> None:
    """教训 libs_state_dict: insertion order 与普通 __wrapped__ 键都属于状态。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_state_dict_helper"
    node_name = "libs_state_dict_node"
    (source / f"{helper_name}.py").write_text(
        "state = {'first': 'A', 'second': 'B', '__wrapped__': 'X'}\n\n"
        "def runner():\n"
        "    return next(iter(state)), state['__wrapped__']\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import runner, state\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': list(runner())}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": ["first", "X"]}

        first = module.state.pop("first")
        module.state["first"] = first
        module.state["__wrapped__"] = "Y"
        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": ["second", "Y"]}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_bound_method_defaults_are_part_of_callable_state(tmp_path: Path) -> None:
    """教训 libs_bound_method_state: receiver 之外还须绑定底层 function state。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_bound_method_state_helper"
    node_name = "libs_bound_method_state_node"
    (source / f"{helper_name}.py").write_text(
        "class Runner:\n"
        "    def call(self, value='A'):\n"
        "        return value\n\n"
        "runner = Runner().call\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": "A"}

        module.runner.__func__.__defaults__ = ("B",)
        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_callable_state_preserves_aliases_across_global_roots(tmp_path: Path) -> None:
    """教训 libs_cross_root_alias: callable roots 必须共享同一 reference table。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_cross_root_alias_helper"
    node_name = "libs_cross_root_alias_node"
    (source / f"{helper_name}.py").write_text(
        "class Runner:\n"
        "    def __init__(self, state):\n"
        "        self.state = state\n\n"
        "    def __call__(self):\n"
        "        return self.state\n\n"
        "shared = []\n"
        "first = Runner(shared)\n"
        "second = Runner(shared)\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import first, second\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': first.state is second.state}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": True}

        module.second.state = []
        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": False}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_runtime_provenance_does_not_execute_pathlike_entries(tmp_path: Path) -> None:
    """教训 libs_safe_fspath: package/sys.path 取证不得调用用户 __fspath__。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_path = source / "helper.py"
    helper_path.write_text("VALUE = 1\n", encoding="utf-8")
    events: list[str] = []

    class Bomb:
        def __fspath__(self) -> str:
            events.append("fspath")
            raise AssertionError("runtime provenance invoked __fspath__")

    package_name = "libs_safe_fspath_package"
    package = types.ModuleType(package_name)
    package.__file__ = str(helper_path)
    package.__path__ = [Bomb()]
    sys.modules[package_name] = package
    try:
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))

        @dag.node("work")
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"value": "stable"}

        dag._libs_hash(dag._nodes["work"])
        assert events == []
    finally:
        sys.modules.pop(package_name, None)


def test_runtime_state_depth_limit_is_stable_and_non_reusable(tmp_path: Path) -> None:
    """教训 libs_state_bound: deep state 必须降级而不是 RecursionError/OOM。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_state_depth_helper"
    node_name = "libs_state_depth_node"
    (source / f"{helper_name}.py").write_text(
        "state = []\n"
        "for _ in range(150):\n"
        "    state = [state]\n\n"
        "def runner():\n"
        "    return 'stable'\n\n"
        "runner.state = state\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        node = dag._nodes["work"]
        assert dag._libs_hash(node) == dag._libs_hash(node)
        assert dag.run().cache_hits == []
        assert dag.run().cache_hits == []
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_non_reusable_map_plan_skips_item_cache_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """教训 libs_map_plan_off: effective cache=off 不得读取或报告 item hit。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "one"}, {"id": "two"}]}

    @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"id": item["id"]}

    dag.run()
    original = dag._libs_identities

    def non_reusable(nodes: Any) -> dict[str, Any]:
        identities = original(nodes)
        identities["work"] = replace(identities["work"], cache_reusable=False)
        return identities

    monkeypatch.setattr(dag, "_libs_identities", non_reusable)
    reads = 0
    original_read = dag_module.store.read_cache_entry

    def counting_read(*args: Any, **kwargs: Any) -> Any:
        nonlocal reads
        reads += 1
        return original_read(*args, **kwargs)

    monkeypatch.setattr(dag_module.store, "read_cache_entry", counting_read)
    plan = dag.plan()
    assert plan.nodes["work"] == "miss"
    assert plan.nodes["work@one"] == "miss"
    assert plan.nodes["work@two"] == "miss"
    assert reads == 1


def test_empty_map_never_reports_vacuous_cache_hit(tmp_path: Path) -> None:
    """教训 libs_empty_map: 无 item 时 all([]) 不能伪造 aggregate hit。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": []}

    @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"id": item["id"]}

    first = dag.run()
    second = dag.run()
    assert "work" not in first.cache_hits
    assert "work" not in second.cache_hits
    assert dag.plan().nodes["work"] == "miss"


@pytest.mark.parametrize(
    "node_source",
    [
        pytest.param(
            "import sys\n\n"
            "OWNER = sys.modules[__name__]\n\n"
            "def run(inputs, ctx):\n"
            "    return {'value': object.__getattribute__(OWNER, '__name__')}\n",
            id="unbound-object-getattribute",
        ),
        pytest.param(
            "import sys\n\n"
            "OWNER = sys.modules[__name__]\n"
            "LOOKUPS = [getattr]\n\n"
            "def run(inputs, ctx):\n"
            "    return {'value': LOOKUPS[0](OWNER, '__name__')}\n",
            id="subscript-selected-getattr",
        ),
        pytest.param(
            "import sys\n\n"
            "OWNER = sys.modules[__name__]\n\n"
            "def bind():\n"
            "    lookup = getattr\n\n"
            "    def owner_name():\n"
            "        return lookup(OWNER, '__name__')\n\n"
            "    return owner_name\n\n"
            "OWNER_NAME = bind()\n\n"
            "def run(inputs, ctx):\n"
            "    return {'value': OWNER_NAME()}\n",
            id="closure-bound-getattr",
        ),
    ],
)
def test_owner_identity_tracks_reached_reflection_forms(
    tmp_path: Path,
    node_source: str,
) -> None:
    """仍允许的静态 receiver reflection 继续覆盖 owner provenance。"""
    source = tmp_path / "src"
    source.mkdir()
    node_path = tmp_path / "node.py"
    node_path.write_text(node_source, encoding="utf-8")
    module_names = ("libs_reached_reflection_a", "libs_reached_reflection_b")
    config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
    try:
        first = _load_libs_runtime_module(node_path, module_names[0])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(first.run)
        assert first_dag.run().artifacts["work"] == {"value": module_names[0]}

        second = _load_libs_runtime_module(node_path, module_names[1])
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(second.run)
        result = second_dag.run()
        assert result.cache_hits == []
        assert result.artifacts["work"] == {"value": module_names[1]}
    finally:
        for name in module_names:
            sys.modules.pop(name, None)


def test_registration_rejects_reached_nested_class_reflection(tmp_path: Path) -> None:
    """到达的 nested class 直接触发注册期 hard cut。"""
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "def run(inputs, ctx):\n"
        "    class Box:\n"
        "        owner = __name__\n\n"
        "    return {'value': Box.owner}\n",
        encoding="utf-8",
    )
    node_name = "libs_reached_reflection_class_rejected"
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=[]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.modules.pop(node_name, None)


def test_callable_state_binds_mutable_metaclass_call_state(tmp_path: Path) -> None:
    """教训 libs_metaclass_state: custom metaclass 的执行状态属于 callable class。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_metaclass_state_helper"
    node_name = "libs_metaclass_state_node"
    (source / f"{helper_name}.py").write_text(
        "class Meta(type):\n"
        "    result = 'A'\n\n"
        "    def __call__(cls):\n"
        "        return type(cls).result\n\n"
        "class Runner(metaclass=Meta):\n"
        "    pass\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import Runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': Runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": "A"}

        type(module.Runner).result = "B"
        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_callable_state_binds_dunder_named_class_data(tmp_path: Path) -> None:
    """教训 libs_dunder_state: 用户 dunder 数据不能与结构元数据一起丢弃。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_dunder_state_helper"
    node_name = "libs_dunder_state_node"
    (source / f"{helper_name}.py").write_text(
        "class Runner:\n"
        "    __result__ = 'A'\n\n"
        "    def __call__(self):\n"
        "        return self.__result__\n\n"
        "runner = Runner()\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": "A"}

        type(module.runner).__result__ = "B"
        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_callable_state_keeps_duplicate_mro_slots_distinct(tmp_path: Path) -> None:
    """教训 libs_duplicate_slots: 同名 base/subclass slot 是两个独立 cell。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_duplicate_slots_helper"
    node_name = "libs_duplicate_slots_node"
    (source / f"{helper_name}.py").write_text(
        "class Base:\n"
        "    __slots__ = ('value',)\n\n"
        "class Runner(Base):\n"
        "    __slots__ = ('value',)\n\n"
        "    def __init__(self):\n"
        "        Base.value.__set__(self, 'base-A')\n"
        "        Runner.value.__set__(self, 'sub-A')\n\n"
        "    def __call__(self):\n"
        "        return [Base.value.__get__(self, Runner), Runner.value.__get__(self, Runner)]\n\n"
        "runner = Runner()\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import Base, Runner, runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": ["base-A", "sub-A"]}

        module.Base.value.__set__(module.runner, "base-B")
        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": ["base-B", "sub-A"]}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_slot_held_callable_provenance_changes_identity(tmp_path: Path) -> None:
    """教训 libs_slot_callable: slot 中的 configured callable 仍须追踪来源与状态。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_slot_callable_helper"
    node_name = "libs_slot_callable_node"
    (source / f"{helper_name}.py").write_text(
        "def first():\n"
        "    return 'A'\n\n"
        "def second():\n"
        "    return 'B'\n\n"
        "class Runner:\n"
        "    __slots__ = ('target',)\n\n"
        "    def __init__(self):\n"
        "        self.target = first\n\n"
        "    def __call__(self):\n"
        "        return self.target()\n\n"
        "runner = Runner()\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import runner, second\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": "A"}

        module.runner.target = module.second
        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_native_container_callable_state_is_stable_but_non_reusable(tmp_path: Path) -> None:
    """教训 libs_native_subclass: native container subclass 不猜测安全表示。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_native_subclass_helper"
    node_name = "libs_native_subclass_node"
    (source / f"{helper_name}.py").write_text(
        "class Runner(list):\n"
        "    def __call__(self):\n"
        "        return self[0]\n\n"
        "runner = Runner(['stable'])\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        node = dag._nodes["work"]
        assert dag._libs_hash(node) == dag._libs_hash(node)
        assert dag.run().cache_hits == []
        assert dag.run().cache_hits == []
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_detached_configured_native_container_provenance_is_non_reusable(
    tmp_path: Path,
) -> None:
    """摘除 owner registry 后，仍能凭稳定 module/source 识别 native subclass。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_detached_native_helper"
    node_name = "libs_detached_native_node"
    (source / f"{helper_name}.py").write_text(
        "class Runner(dict):\n    pass\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"from {helper_name} import Runner\n\n"
        "runner = Runner(value='stable')\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner['value']}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        sys.modules.pop(helper_name, None)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        identity = dag._libs_identities((dag._nodes["work"],))["work"]
        assert identity.cache_reusable is False
        assert dag.run().artifacts["work"] == {"value": "stable"}
        assert dag.run().cache_hits == []
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_registration_rejects_complete_globals_observation(tmp_path: Path) -> None:
    """任意 globals namespace 观察在注册期硬失败，不进入 L3 分析。"""
    source = tmp_path / "src"
    source.mkdir()
    node_name = "libs_all_globals_off_node"
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "import importlib\n\n"
        "VALUE = 'stable'\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': globals()['VALUE']}\n",
        encoding="utf-8",
    )
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.modules.pop(node_name, None)


def test_runtime_state_node_limit_is_stable_and_non_reusable(tmp_path: Path) -> None:
    """教训 libs_state_node_bound: wide state 超预算也须确定性降级。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_state_node_bound_helper"
    node_name = "libs_state_node_bound_node"
    (source / f"{helper_name}.py").write_text(
        "state = [[index] for index in range(5000)]\n\n"
        "def runner():\n"
        "    return 'stable'\n\n"
        "runner.state = state\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        node = dag._nodes["work"]
        assert dag._libs_hash(node) == dag._libs_hash(node)
        assert dag.run().cache_hits == []
        assert dag.run().cache_hits == []
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_runtime_provenance_member_limit_is_stable_and_non_reusable(
    tmp_path: Path,
) -> None:
    """教训 libs_member_bound: 巨型 class namespace 不得无界扫描。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_member_bound_helper"
    node_name = "libs_member_bound_node"
    (source / f"{helper_name}.py").write_text(
        "def call(self):\n"
        "    return 'stable'\n\n"
        "namespace = {f'field_{index}': index for index in range(5000)}\n"
        "namespace['__call__'] = call\n"
        "Runner = type('Runner', (), namespace)\n"
        "runner = Runner()\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"import importlib\nfrom {helper_name} import runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)
        node = dag._nodes["work"]
        assert dag._libs_hash(node) == dag._libs_hash(node)
        assert dag.run().cache_hits == []
        assert dag.run().cache_hits == []
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_empty_scan_never_reports_vacuous_cache_hit(tmp_path: Path) -> None:
    """教训 libs_empty_scan: 空 carry 链不能在 run/plan 中伪造 aggregate hit。"""
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": []}

    @dag.scan("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(
        item: dict[str, str],
        carry: Any,
        inputs: dict[str, Any],
        ctx: Any,
    ) -> dict[str, str]:
        return {"id": item["id"]}

    first = dag.run()
    second = dag.run()
    assert "work" not in first.cache_hits
    assert "work" not in second.cache_hits
    assert dag.plan().nodes["work"] == "miss"


@pytest.mark.parametrize(
    "body",
    [
        "    def inner():\n"
        "        return __name__\n\n"
        "    alias = inner\n"
        "    return {'value': alias()}\n",
        "    callback = lambda: __name__\n    return {'value': callback()}\n",
    ],
)
def test_libs_hash_binds_owner_for_reached_local_callable_forms(
    tmp_path: Path,
    body: str,
) -> None:
    """实际到达的普通局部 alias/lambda 继续覆盖 owner provenance。"""
    source = tmp_path / "src"
    source.mkdir()
    node_path = tmp_path / "node.py"
    node_path.write_text("import importlib\n\ndef run(inputs, ctx):\n" + body, encoding="utf-8")
    module_names = ("libs_reached_local_a", "libs_reached_local_b")
    config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
    try:
        first = _load_libs_runtime_module(node_path, module_names[0])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(first.run)
        assert first_dag.run().artifacts["work"] == {"value": module_names[0]}

        second = _load_libs_runtime_module(node_path, module_names[1])
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(second.run)
        result = second_dag.run()
        assert result.cache_hits == []
        assert result.artifacts["work"] == {"value": module_names[1]}
    finally:
        for name in module_names:
            sys.modules.pop(name, None)


def test_registration_rejects_reached_local_nested_class(tmp_path: Path) -> None:
    """局部 nested class 即使只为 owner identity 服务也必须拒绝。"""
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "def run(inputs, ctx):\n"
        "    class Box:\n"
        "        @staticmethod\n"
        "        def inner():\n"
        "            return __name__\n\n"
        "    return {'value': Box.inner()}\n",
        encoding="utf-8",
    )
    node_name = "libs_reached_local_class_rejected"
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=[]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.modules.pop(node_name, None)


def test_libs_hash_fails_closed_for_unresolved_attribute_call(tmp_path: Path) -> None:
    """未知 holder.callback() 不能被静态负证明成与 owner 无关。"""
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "class Holder:\n"
        "    def __getattribute__(self, name):\n"
        "        if name == 'callback':\n"
        "            return lambda: __name__\n"
        "        return object.__getattribute__(self, name)\n\n"
        "holder = Holder()\n\n"
        "import importlib\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': holder.callback()}\n",
        encoding="utf-8",
    )
    module_names = ("libs_unresolved_attr_a", "libs_unresolved_attr_b")
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    try:
        first = _load_libs_runtime_module(node_path, module_names[0])
        first_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        first_dag.node("work")(first.run)
        assert first_dag.run().artifacts["work"] == {"value": module_names[0]}
        second = _load_libs_runtime_module(node_path, module_names[1])
        second_dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        second_dag.node("work")(second.run)
        result = second_dag.run()
        assert result.cache_hits == []
        assert result.artifacts["work"] == {"value": module_names[1]}
    finally:
        for name in module_names:
            sys.modules.pop(name, None)


def test_registration_rejects_globals_in_reached_nested_function(
    tmp_path: Path,
) -> None:
    """被调用 nested function 的 globals 观察也必须注册期硬失败。"""
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "VALUE = 'A'\n"
        "LOOKUP = 'VALUE'\n"
        "UNRELATED = 'stable'\n\n"
        "import importlib\n\n"
        "def run(inputs, ctx):\n"
        "    def nested():\n"
        "        return globals()[LOOKUP]\n\n"
        "    return {'value': nested()}\n",
        encoding="utf-8",
    )
    module = _load_libs_runtime_module(node_path, "libs_nested_all_globals")
    try:
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=[]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.modules.pop("libs_nested_all_globals", None)


def test_runtime_state_preserves_direct_function_code_alias_topology(tmp_path: Path) -> None:
    """function.__code__ 是共享图节点，代码对象别名变化必须失效。"""
    helper_name = "libs_code_alias_helper"
    node_name = "libs_code_alias_node"
    source = tmp_path / "src"
    source.mkdir()
    (source / f"{helper_name}.py").write_text(
        "def first():\n    return 'stable'\n\n"
        "def second():\n    return 'stable'\n\n"
        "CALLS = (first, second)\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"from {helper_name} import CALLS\n\n"
        "import importlib\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': CALLS[0]()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=["src"]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": "stable"}
        module.CALLS = (module.CALLS[0], module.CALLS[0])
        changed = dag.run()
        assert changed.cache_hits == []
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_code_identity_distinguishes_complex_constants() -> None:
    """行为常量的 complex 值不能只序列化为类型名。"""
    template = compile("def run():\n    return 0j\n", "template.py", "exec")
    code = next(value for value in template.co_consts if isinstance(value, types.CodeType))
    first_code = code.replace(co_consts=(None, 1 + 2j))
    second_code = code.replace(co_consts=(None, 3 + 4j))
    assert dag_module._code_object_digest(first_code) != dag_module._code_object_digest(second_code)


def test_inherited_descriptor_forces_non_reusable_callable_state(tmp_path: Path) -> None:
    """descriptor 的继承 MRO 也属于不安全 runtime state。"""
    helper_name = "libs_inherited_descriptor_helper"
    node_name = "libs_inherited_descriptor_node"
    source = tmp_path / "src"
    source.mkdir()
    (source / f"{helper_name}.py").write_text(
        "class BaseDescriptor:\n"
        "    def __get__(self, instance, owner):\n"
        "        return 'stable'\n\n"
        "class Descriptor(BaseDescriptor):\n"
        "    pass\n\n"
        "class Runner:\n"
        "    value = Descriptor()\n\n"
        "    def __call__(self):\n"
        "        return self.value\n\n"
        "runner = Runner()\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"from {helper_name} import runner\n\n"
        "import importlib\n\n"
        "def run(inputs, ctx):\n    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=["src"]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        dag.node("work")(module.run)
        assert dag._libs_identities((dag._nodes["work"],))["work"].cache_reusable is False
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_classmethod_and_staticmethod_descriptor_kind_is_keyed(tmp_path: Path) -> None:
    """classmethod 与 staticmethod 即使底层 target 相同也不是同一种状态。"""
    helper_name = "libs_descriptor_kind_helper"
    node_name = "libs_descriptor_kind_node"
    source = tmp_path / "src"
    source.mkdir()
    (source / f"{helper_name}.py").write_text(
        "def target(*args):\n    return 'stable'\n\n"
        "class Runner:\n"
        "    value = staticmethod(target)\n\n"
        "runner = Runner\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"from {helper_name} import runner\n\n"
        "import importlib\n\n"
        "def run(inputs, ctx):\n    return {'value': runner.value()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=["src"]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": "stable"}
        module.runner.value = classmethod(module.runner.value)
        changed = dag.run()
        assert changed.cache_hits == []
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_exact_sets_and_non_string_dict_keys_track_nested_callables(tmp_path: Path) -> None:
    """精确集合及非字符串键 dict 中的 callable 不能逃过身份图。"""
    helper_name = "libs_unsupported_callable_container_helper"
    node_name = "libs_unsupported_callable_container_node"
    source = tmp_path / "src"
    source.mkdir()
    (source / f"{helper_name}.py").write_text(
        "def first():\n    return 'A'\n\n"
        "def second():\n    return 'B'\n\n"
        "STATE = {'A'}\n"
        "CALLS = {1: first}\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"from {helper_name} import CALLS, STATE\n\n"
        "import importlib\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': next(iter(STATE)), 'call': CALLS[1]()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=["src"]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        dag.node("work")(module.run)
        assert dag.run().artifacts["work"] == {"value": "A", "call": "A"}
        module.STATE = {"B"}
        module.CALLS = {1: module.CALLS[1].__globals__["second"]}
        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": "B", "call": "B"}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_wrapper_chain_overflow_is_non_reusable_without_recursion_error(tmp_path: Path) -> None:
    """长 wrapper/``__wrapped__`` 链必须在共享预算内稳定降级。"""
    helper_name = "libs_wrapper_budget_helper"
    node_name = "libs_wrapper_budget_node"
    source = tmp_path / "src"
    source.mkdir()
    (source / f"{helper_name}.py").write_text(
        "def target():\n    return 'stable'\n\n"
        "def wrap(previous):\n"
        "    def wrapper():\n"
        "        return previous()\n"
        "    wrapper.__wrapped__ = previous\n"
        "    return wrapper\n\n"
        "runner = target\n"
        "for index in range(1200):\n"
        "    runner = wrap(runner)\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"from {helper_name} import runner\n\n"
        "import importlib\n\n"
        "def run(inputs, ctx):\n    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        dag = Dag(
            KigumiConfig(project_root=tmp_path, source_dirs=["src"]),
            LLMCaller(FakeTransport(), tmp_path / "llm"),
        )
        dag.node("work")(module.run)
        identity = dag._libs_identities((dag._nodes["work"],))["work"]
        assert identity.cache_reusable is False
        assert identity.digest == dag._libs_identities((dag._nodes["work"],))["work"].digest
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_static_selected_mutable_callable_state_invalidates(tmp_path: Path) -> None:
    """静态闭包命中时，节点函数自身的可变 defaults 仍须参与 runtime key。"""
    source = tmp_path / "src"
    source.mkdir()
    node_name = "libs_static_mutable_callable_node"
    node_path = source / f"{node_name}.py"
    node_path.write_text(
        "def run(inputs, ctx, state=['A']):\n    return {'value': state[0]}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag.run()
        assert first.artifacts["work"] == {"value": "A"}
        assert module.run.__defaults__ is not None
        module.run.__defaults__[0][0] = "B"

        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(node_name, None)


def test_custom_container_hiding_configured_callable_is_non_reusable(tmp_path: Path) -> None:
    """配置源码自定义容器的迭代内容不能绕过 callable provenance。"""
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_custom_container_helper"
    node_name = "libs_custom_container_node"
    (source / f"{helper_name}.py").write_text(
        "def first():\n"
        "    return 'A'\n\n"
        "def second():\n"
        "    return 'B'\n\n"
        "HIDDEN = [first]\n\n"
        "class Container:\n"
        "    def __iter__(self):\n"
        "        return iter(HIDDEN)\n\n"
        "container = Container()\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"from {helper_name} import container\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': next(iter(container))()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        helper = sys.modules[helper_name]
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        identity = dag._libs_identities((dag._nodes["work"],))["work"]
        assert identity.cache_reusable is False
        assert dag.run().artifacts["work"] == {"value": "A"}

        helper.HIDDEN[0] = helper.second
        changed = dag.run()
        assert changed.cache_hits == []
        assert changed.artifacts["work"] == {"value": "B"}
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)


def test_registry_overflow_remains_non_reusable_on_repeated_identity_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """注册表扫描溢出后，复用 analyzer 也必须恢复 overflow 状态。"""
    source = tmp_path / "src"
    source.mkdir()
    (source / "helper.py").write_text("VALUE = 'stable'\n", encoding="utf-8")
    node_path = tmp_path / "node.py"
    node_path.write_text(
        "def run(inputs, ctx):\n    return {'value': 'stable'}\n",
        encoding="utf-8",
    )
    module = _load_libs_runtime_module(node_path, "libs_registry_overflow_node")
    snapshot = dag_module._capture_libs_source_snapshot([source], project_root=tmp_path)
    analyzer = dag_module._StaticLibsAnalyzer(tmp_path, [source], snapshot)
    registry = {
        f"unrelated_{index}": types.ModuleType(f"unrelated_{index}")
        for index in range(dag_module._RUNTIME_PROVENANCE_MAX_MEMBERS + 1)
    }
    monkeypatch.setattr(dag_module, "_safe_sys_modules", lambda: registry)

    first = analyzer.fallback_identity_for(module.run, "fallback")
    second = analyzer.fallback_identity_for(module.run, "fallback")

    assert first.cache_reusable is False
    assert second.cache_reusable is False
    assert first.digest == second.digest
    sys.modules.pop("libs_registry_overflow_node", None)


def test_registration_rejects_globals_in_reached_class_body(tmp_path: Path) -> None:
    """到达的 class body 同时触发 nested-class/globals 注册期硬切。"""
    node_name = "libs_reached_class_globals_node"
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "VALUE = 'A'\n\n"
        "def run(inputs, ctx):\n"
        "    class Box:\n"
        "        value = globals()['VALUE']\n\n"
        "    return {'value': Box.value}\n",
        encoding="utf-8",
    )
    module = _load_libs_runtime_module(node_path, node_name)
    try:
        config = KigumiConfig(project_root=tmp_path, source_dirs=[])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.modules.pop(node_name, None)


def test_registration_rejects_globals_in_multi_level_nested_calls(
    tmp_path: Path,
) -> None:
    """outer -> inner -> deep 的 globals 观察必须注册期硬失败。"""
    node_name = "libs_multi_level_globals_node"
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        "VALUE = 'A'\n\n"
        "def run(inputs, ctx):\n"
        "    def outer():\n"
        "        def inner():\n"
        "            def deep():\n"
        "                return globals()['VALUE']\n\n"
        "            return deep()\n\n"
        "        return inner()\n\n"
        "    return {'value': outer()}\n",
        encoding="utf-8",
    )
    module = _load_libs_runtime_module(node_path, node_name)
    try:
        config = KigumiConfig(project_root=tmp_path, source_dirs=[])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        _assert_registration_rejected(dag, module.run)
    finally:
        sys.modules.pop(node_name, None)


def test_deeply_nested_set_sorting_is_bounded_and_non_reusable(tmp_path: Path) -> None:
    """共享别名的深层 frozenset 图排序必须有界，并稳定降级为不可复用。"""
    # Keep the sorting walk deep without triggering Python 3.13's exponential
    # tuple hash for the old shared-child tuple tree.
    source = tmp_path / "src"
    source.mkdir()
    helper_name = "libs_deep_set_sort_helper"
    node_name = "libs_deep_set_sort_node"
    (source / f"{helper_name}.py").write_text(
        "anchor = frozenset({'anchor'})\n"
        "nested = frozenset({0})\n"
        "for _ in range(120):\n"
        "    nested = frozenset({nested, anchor})\n"
        "state = {nested}\n\n"
        "def runner():\n"
        "    return 'stable'\n\n"
        "runner.state = state\n",
        encoding="utf-8",
    )
    node_path = tmp_path / f"{node_name}.py"
    node_path.write_text(
        f"from {helper_name} import runner\n\n"
        "def run(inputs, ctx):\n"
        "    return {'value': runner()}\n",
        encoding="utf-8",
    )
    original_sys_path = list(sys.path)
    sys.path.insert(0, str(source))
    try:
        module = _load_libs_runtime_module(node_path, node_name)
        config = KigumiConfig(project_root=tmp_path, source_dirs=["src"])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm"))
        dag.node("work")(module.run)

        first = dag._libs_identities((dag._nodes["work"],))["work"]
        second = dag._libs_identities((dag._nodes["work"],))["work"]
        assert first.cache_reusable is False
        assert second.cache_reusable is False
        assert first.digest == second.digest
    finally:
        sys.path[:] = original_sys_path
        for name in (node_name, helper_name):
            sys.modules.pop(name, None)
