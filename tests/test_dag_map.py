from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from kigumi.artifacts import sha
from kigumi.calling import DryRunError, LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.testing import FakeTransport
from tests._dag_helpers import _make_dag


def test_foreach_files_fn_invalidates_only_the_changed_item(tmp_path: Path) -> None:
    """教训 item_file_cache: 逐项文件依赖不进缓存键，改 clip 就不会失效。"""
    first_file = tmp_path / "first.mp4"
    second_file = tmp_path / "second.mp4"
    first_file.write_bytes(b"first")
    second_file.write_bytes(b"second")
    executed: list[str] = []

    def run_once() -> Any:
        dag = _make_dag(tmp_path)

        @dag.foreach(
            "analyze_{name}",
            [{"name": "first", "clip": first_file}, {"name": "second", "clip": second_file}],
            files_fn=lambda item: (item["clip"],),
            params_fn=lambda item: {"name": item["name"]},
        )
        def analyze(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            executed.append(ctx.params["name"])
            return {"name": ctx.params["name"]}

        return dag.run()

    assert run_once().cache_hits == []
    assert executed == ["first", "second"]
    executed.clear()
    first_file.write_bytes(b"changed")
    result = run_once()
    assert result.cache_hits == ["analyze_second"]
    assert executed == ["first"]


def test_map_fans_out_runtime_items_and_aggregates_for_downstream(tmp_path: Path) -> None:
    """教训 map_aggregate: 运行期清单也必须以一个稳定聚合产物喂给下游。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan", params={"items": [{"id": "a", "value": 1}, {"id": "b", "value": 2}]})
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": ctx.params["items"]}

    @dag.map("process", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        return {"value": item["value"] * 2}

    @dag.node("gather", deps=("process",))
    def gather(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"received": inputs["process"]}

    result = dag.run()

    assert result.artifacts["process"] == {
        "items": {"a": {"value": 2}, "b": {"value": 4}},
        "order": ["a", "b"],
        "count": 2,
    }
    assert result.artifacts["gather"]["received"] == result.artifacts["process"]
    assert (tmp_path / "artifacts" / "runs" / result.run_id / "process@a.json").is_file()


def test_sidecar_shapes_distinguish_node_aggregate_and_item(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        del inputs, ctx
        return {"items": [{"id": "one"}]}

    @dag.map("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        del inputs, ctx
        return {"id": item["id"]}

    result = dag.run()
    run_root = tmp_path / "artifacts" / "runs" / result.run_id
    source_metadata = json.loads((run_root / "source.json.meta.json").read_text(encoding="utf-8"))
    aggregate = json.loads((run_root / "work.json.meta.json").read_text(encoding="utf-8"))
    item = json.loads((run_root / "work@one.json.meta.json").read_text(encoding="utf-8"))
    common_fields = {
        "node",
        "cache_key",
        "cache",
        "cache_policy",
        "outputs",
        "seconds",
        "calls",
        "execution_calls",
        "origin_provenance",
        "artifact_sha256",
        "prompt_sha256",
        "model",
        "params",
        "provider_response_id",
        "usage",
        "created_at",
    }

    assert isinstance(source_metadata["cache_key"], str)
    assert set(source_metadata) == common_fields | {"key_components"}
    assert isinstance(aggregate["cache_key"], list)
    assert set(aggregate) == common_fields
    assert "key_components" not in aggregate
    assert item["node"] == "work@one"
    assert set(item) == common_fields | {"key_components"}
    assert "key_components" in item
    assert all(
        metadata["origin_provenance"]["artifact_sha256"] == metadata["artifact_sha256"]
        for metadata in (source_metadata, aggregate, item)
    )


def test_map_aggregate_fn_can_narrow_downstream_artifact(tmp_path: Path) -> None:
    """教训 map_aggregate_fn: 大 item 不必复制进只消费摘要的下游聚合。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "a", "value": 2}, {"id": "b", "value": 4}]}

    @dag.map(
        "process",
        items_from=("scan", "items"),
        key_fn=lambda item: item["id"],
        aggregate_fn=lambda items, order: {
            "ids": order,
            "total": sum(items[item_id]["value"] for item_id in order),
        },
    )
    def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        return {"value": item["value"]}

    @dag.node("downstream", deps=("process",))
    def downstream(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"received": inputs["process"]}

    result = dag.run()

    assert result.artifacts["process"] == {"ids": ["a", "b"], "total": 6}
    assert result.artifacts["downstream"] == {"received": {"ids": ["a", "b"], "total": 6}}


def test_map_aggregate_fn_change_reuses_items_and_invalidates_downstream(tmp_path: Path) -> None:
    """教训 map_aggregate_fn_cache: 聚合变更重建下游，但绝不进入 item 缓存键。"""
    executed: list[str] = []

    def summary_v1(items: dict[str, dict[str, Any]], order: list[str]) -> dict[str, Any]:
        return {"count": len(order)}

    def summary_v2(items: dict[str, dict[str, Any]], order: list[str]) -> dict[str, Any]:
        return {"ids": order, "count": len(order)}

    def build(
        aggregate_fn: Callable[[dict[str, dict[str, Any]], list[str]], dict[str, Any]],
    ) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan")
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"id": "a"}, {"id": "b"}]}

        @dag.map(
            "m",
            items_from=("scan", "items"),
            key_fn=lambda item: item["id"],
            aggregate_fn=aggregate_fn,
        )
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            executed.append(item["id"])
            return {"id": item["id"]}

        @dag.node("downstream", deps=("m",))
        def downstream(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"aggregate": inputs["m"]}

        return dag

    build(summary_v1).run()
    executed.clear()
    changed = build(summary_v2)

    plan = changed.plan()
    assert plan.nodes == {
        "scan": "hit",
        "m@a": "hit",
        "m@b": "hit",
        "m": "hit",
        "downstream": "miss",
    }
    result = changed.run()

    assert executed == []
    assert result.cache_hits == ["scan", "m"]
    assert result.artifacts["m"] == {"ids": ["a", "b"], "count": 2}
    assert result.artifacts["downstream"] == {"aggregate": {"ids": ["a", "b"], "count": 2}}
    assert changed.plan().nodes == {
        "scan": "hit",
        "m@a": "hit",
        "m@b": "hit",
        "m": "hit",
        "downstream": "hit",
    }


def test_map_aggregate_fn_must_return_dict(tmp_path: Path) -> None:
    """教训 map_aggregate_fn_type: 聚合契约错误必须标出所属 map 节点。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": ["one"]}

    @dag.map("process", items_from=("scan", "items"), aggregate_fn=lambda items, order: [])
    def process(item: str, inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"item": item}

    with pytest.raises(
        TypeError, match="Map node 'process' aggregate_fn must return a dict artifact"
    ):
        dag.run()


def test_map_incrementally_recomputes_only_changed_and_added_items(tmp_path: Path) -> None:
    """教训 map_incremental: 清单变动不能把未牵连 item 的缓存一起冲掉。"""
    executed: list[str] = []

    def build(items: list[dict[str, Any]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": ctx.params["items"]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            executed.append(item["id"])
            return {"value": item["value"]}

        @dag.node("gather", deps=("m",))
        def gather(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"count": inputs["m"]["count"]}

        return dag

    assert build([{"id": "a", "value": 1}, {"id": "b", "value": 2}]).run().cache_hits == []
    assert executed == ["a", "b"]
    executed.clear()
    result = build(
        [{"id": "a", "value": 1}, {"id": "b", "value": 20}, {"id": "c", "value": 3}]
    ).run()

    assert executed == ["b", "c"]
    assert result.cache_hits == []
    assert result.artifacts["gather"] == {"count": 3}


def test_map_reorder_rebuilds_aggregate_without_reexecuting_items(tmp_path: Path) -> None:
    """教训 map_order: 顺序属于聚合语义，却不是 item 身份输入。"""
    executed: list[str] = []

    def build(items: list[dict[str, str]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": ctx.params["items"]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            executed.append(item["id"])
            return {"id": item["id"]}

        @dag.node("downstream", deps=("m",))
        def downstream(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"order": inputs["m"]["order"]}

        return dag

    build([{"id": "a"}, {"id": "b"}]).run()
    executed.clear()
    result = build([{"id": "b"}, {"id": "a"}]).run()

    assert executed == []
    assert result.cache_hits == ["m"]
    assert result.artifacts["m"]["order"] == ["b", "a"]
    assert result.artifacts["downstream"] == {"order": ["b", "a"]}


def test_map_rejects_duplicate_item_ids_without_deduplicating(tmp_path: Path) -> None:
    """教训 map_duplicate_id: 寻址冲突必须立即显形，不能静默吞 item。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "same"}, {"id": "same"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return item

    with pytest.raises(ValueError, match="duplicate item_id values: same"):
        dag.run()


def test_map_rejects_item_ids_that_are_not_single_path_components(tmp_path: Path) -> None:
    for item_id in ("../escape", "nested/item", "nested\\item", ".", ".."):
        dag = _make_dag(tmp_path / item_id.replace("/", "-"))

        @dag.node("source", params={"item_id": item_id})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"id": ctx.params["item_id"]}]}

        @dag.map("m", items_from=("source", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return item

        with pytest.raises(ValueError, match="single relative path component"):
            dag.run()


def test_map_preserves_successful_items_when_one_item_fails(tmp_path: Path) -> None:
    """教训 map_failure_isolation: 一个 item 崩溃不能浪费已完成 item 的缓存。"""
    executed: list[str] = []

    def build(items: list[dict[str, Any]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": ctx.params["items"]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            executed.append(item["id"])
            if item["bad"]:
                raise ValueError("broken")
            return {"id": item["id"]}

        return dag

    initial = [{"id": "a", "bad": False}, {"id": "b", "bad": True}, {"id": "c", "bad": False}]
    with pytest.raises(RuntimeError, match=r"b \(ValueError: broken\)"):
        build(initial).run()
    assert executed == ["a", "b", "c"]
    executed.clear()
    result = build(
        [{"id": "a", "bad": False}, {"id": "b", "bad": False}, {"id": "c", "bad": False}]
    ).run()

    assert executed == ["b"]
    assert result.artifacts["m"]["count"] == 3


def test_map_force_can_target_one_runtime_item(tmp_path: Path) -> None:
    """教训 map_force_item: 运行时 id 的 force 必须精确且拼错立即报错。"""
    dag = _make_dag(tmp_path)
    executed: list[str] = []

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "one"}, {"id": "two"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        executed.append(item["id"])
        return item

    dag.run()
    executed.clear()
    dag.run(force=["m@two"])
    assert executed == ["two"]
    with pytest.raises(ValueError, match="m@nope"):
        dag.run(force=["m@nope"])


def test_map_files_fn_invalidates_only_the_affected_item(tmp_path: Path) -> None:
    """教训 map_item_file: 逐项文件内容只应换掉对应 item 的缓存键。"""
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text("first", encoding="utf-8")
    second.write_text("second", encoding="utf-8")
    executed: list[str] = []
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "first"}, {"id": "second"}]}

    @dag.map(
        "m",
        items_from=("scan", "items"),
        key_fn=lambda item: item["id"],
        files_fn=lambda item: (first if item["id"] == "first" else second,),
    )
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        executed.append(item["id"])
        return item

    dag.run()
    executed.clear()
    first.write_text("changed", encoding="utf-8")
    dag.run()
    assert executed == ["first"]


def test_map_item_sidecars_capture_only_their_own_calls(tmp_path: Path) -> None:
    """教训 map_call_observer: 并行 item 的调用溯源不能串到同一 sidecar。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"answer": ctx.call(f"prompt {item['id']}")}

    result = dag.run(run_id="map-calls", workers=2)
    run_root = tmp_path / "artifacts" / "runs" / result.run_id
    for item_id in ("a", "b"):
        calls = json.loads((run_root / f"m@{item_id}.json.meta.json").read_text(encoding="utf-8"))[
            "calls"
        ]
        assert [call["prompt_sha"] for call in calls] == [
            sha([{"role": "user", "content": f"prompt {item_id}"}])
        ]


def test_map_key_fn_renaming_does_not_change_item_identity(tmp_path: Path) -> None:
    """教训 map_id_not_identity: 改寻址名称不能让内容相同的 item 重算。"""
    executed: list[str] = []

    def build(key_fn: Callable[[dict[str, str]], str]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan")
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"name": "first"}, {"name": "second"}]}

        @dag.map("m", items_from=("scan", "items"), key_fn=key_fn)
        def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            executed.append(item["name"])
            return item

        return dag

    build(lambda item: f"old-{item['name']}").run()
    executed.clear()
    result = build(lambda item: f"new-{item['name']}").run()

    assert executed == []
    assert result.artifacts["m"]["order"] == ["new-first", "new-second"]
    assert result.cache_hits == ["scan", "m"]


def test_map_dry_run_only_errors_for_uncached_item_calls(tmp_path: Path) -> None:
    """教训 map_dry_run: 探测本地清单后，未命中 item 仍必须阻止真实调用。"""

    def build(dry: bool) -> Dag:
        config = KigumiConfig(project_root=tmp_path, source_dirs=[])
        dag = Dag(config, LLMCaller(FakeTransport(), tmp_path / "llm", dry=dry))

        @dag.node("scan")
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"id": "a"}]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"answer": ctx.call("would call")}

        return dag

    with pytest.raises(DryRunError):
        build(dry=True).run()
    assert build(dry=False).run().cache_hits == ["scan"]
    assert build(dry=True).run().cache_hits == ["scan", "m"]


def test_map_items_from_resolves_nested_paths_even_when_dotted_top_level_exists(
    tmp_path: Path,
) -> None:
    """教训 nested_items_from: 点分路径必须一律逐段下钻，不能保留旧 artifact 歧义。"""
    received: list[str] = []
    dag = _make_dag(tmp_path)

    @dag.node("scan")
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {
            "structure": {"segments": [{"id": "nested"}]},
            "structure.segments": [{"id": "exact"}],
        }

    @dag.map("m", items_from=("scan", "structure.segments"), key_fn=lambda item: item["id"])
    def process(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        received.append(item["id"])
        return item

    result = dag.run()

    assert received == ["nested"]
    assert result.artifacts["m"]["order"] == ["nested"]


@pytest.mark.parametrize(
    ("artifact", "expected"),
    [
        ({"structure": {}}, "broke at 'segments': key is missing"),
        ({"structure": []}, "broke at 'segments': 'structure' is not a Mapping"),
    ],
)
def test_map_items_from_path_errors_name_the_full_path_and_break(
    tmp_path: Path, artifact: dict[str, Any], expected: str
) -> None:
    """教训 nested_path_diagnostics: 路径错位必须指出完整路径与断点。"""
    dag = _make_dag(tmp_path)

    @dag.node("scan", params={"artifact": artifact})
    def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return ctx.params["artifact"]

    @dag.map("m", items_from=("scan", "structure.segments"))
    def process(item: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"item": item}

    with pytest.raises(ValueError, match="Map node 'm'.*structure\\.segments") as error:
        dag.run()
    assert expected in str(error.value)


def test_nested_items_from_keeps_item_cache_granularity(tmp_path: Path) -> None:
    """教训 nested_item_cache: 取列表路径不应改变 item 内容寻址语义。"""
    executed: list[str] = []

    def build(items: list[dict[str, Any]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"structure": {"segments": ctx.params["items"]}}

        @dag.map("m", items_from=("scan", "structure.segments"), key_fn=lambda item: item["id"])
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            executed.append(item["id"])
            return {"value": item["value"]}

        return dag

    build([{"id": "a", "value": 1}, {"id": "b", "value": 2}]).run()
    assert executed == ["a", "b"]
    executed.clear()
    result = build([{"id": "a", "value": 1}, {"id": "b", "value": 20}]).run()

    assert executed == ["b"]
    assert result.map_items == {"m": {"a": "hit", "b": "miss"}}


def test_run_result_exposes_map_item_hit_miss_statuses(tmp_path: Path) -> None:
    """教训 map_result_status: 验收不该读取 sidecar 私有布局判断逐项缓存。"""
    executed: list[str] = []

    def build(items: list[dict[str, Any]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("scan", params={"items": items})
        def scan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": ctx.params["items"]}

        @dag.map("m", items_from=("scan", "items"), key_fn=lambda item: item["id"])
        def process(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            executed.append(item["id"])
            return {"value": item["value"]}

        return dag

    first = build([{"id": "a", "value": 1}, {"id": "b", "value": 2}]).run()
    second = build([{"id": "a", "value": 1}, {"id": "b", "value": 2}]).run()
    third = build([{"id": "a", "value": 1}, {"id": "b", "value": 3}]).run()

    assert first.map_items == {"m": {"a": "miss", "b": "miss"}}
    assert second.map_items == {"m": {"a": "hit", "b": "hit"}}
    assert third.map_items == {"m": {"a": "hit", "b": "miss"}}
    assert executed == ["a", "b", "b"]
