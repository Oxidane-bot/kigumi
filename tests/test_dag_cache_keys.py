from __future__ import annotations

import ast
import importlib
import sys
from dataclasses import replace
from itertools import repeat
from pathlib import Path
from typing import Any

import pytest

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
            "builtins_getattr_direct",
            "import builtins\n",
            "    helper = getattr(builtins, '__import__')('"
            "{package_name}.helper', fromlist=['VALUE'])\n",
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
            "builtins_getattr_computed_direct",
            'import builtins\nname = "__" + "import__"\n',
            "    helper = getattr(builtins, name)('{package_name}.helper', fromlist=['VALUE'])\n",
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
        (
            "attribute_target",
            "class Box:\n    pass\n\nbox = Box()\n",
            "    box.load = __import__\n"
            '    helper = box.load("{package_name}.helper", fromlist=["VALUE"])\n',
        ),
        (
            "container_target",
            "loaders = {}\n",
            '    loaders["x"] = __import__\n'
            '    helper = loaders["x"]("{package_name}.helper", fromlist=["VALUE"])\n',
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
