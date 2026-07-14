from __future__ import annotations

import json
from itertools import repeat
from pathlib import Path
from typing import Any

import pytest

from kigumi import Dag, Subgraph
from kigumi.calling import LLMCaller
from kigumi.config import KigumiConfig
from kigumi.testing import FakeTransport
from kigumi.transport import Response


def _make_dag(tmp_path: Path) -> Dag:
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    transport = FakeTransport(repeat(Response("model output", {"total_tokens": 1}, "stop")))
    return Dag(config, LLMCaller(transport, tmp_path / "llm"))


def _editorial() -> tuple[Subgraph, Any, Any]:
    editorial = Subgraph(inputs=("source",), outputs={"result": "publish"})

    @editorial.node("draft", deps=("source",))
    def draft(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        assert set(inputs) == {"source"}
        return {"text": f"draft:{inputs['source']['text']}"}

    @editorial.node("publish", deps=("draft",), cache="refresh", external_fingerprint="cms-v1")
    def publish(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        assert set(inputs) == {"draft"}
        return {"text": f"publish:{inputs['draft']['text']}"}

    return editorial, draft, publish


def test_two_stage_subgraph_wiring_local_keys_and_output_binding(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("outline")
    def outline(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "outline"}

    editorial, draft, _publish = _editorial()
    mounted = dag.mount(editorial, "editorial", inputs={"source": "outline"})
    result = dag.run(targets=(mounted["result"],))
    description = dag.describe()

    assert mounted == {"result": "editorial.publish"}
    assert result.artifacts["editorial.publish"] == {"text": "publish:draft:outline"}
    assert dag._nodes["editorial.draft"].function is draft
    assert description["editorial.publish"]["subgraph"] == "editorial"
    assert description["editorial.publish"]["cache"] == "refresh"
    assert description["editorial.publish"]["has_external_fingerprint"] is True
    assert description["subgraphs"] == {
        "editorial": {
            "inputs": {"source": "outline"},
            "outputs": {"result": "editorial.publish"},
            "nodes": ["editorial.draft", "editorial.publish"],
        }
    }


def test_subgraph_cache_keys_preserve_local_port_roles(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("left")
    def left(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "L"}

    @dag.node("right")
    def right(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "R"}

    graph = Subgraph(inputs=("first", "second"), outputs={"result": "combine"})

    @graph.node("combine", deps=("first", "second"))
    def combine(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": inputs["first"]["value"] + inputs["second"]["value"]}

    first = dag.mount(graph, "first", inputs={"first": "left", "second": "right"})
    second = dag.mount(graph, "second", inputs={"first": "right", "second": "left"})
    result = dag.run(targets=(first["result"], second["result"]), workers=1)

    assert result.artifacts[first["result"]] == {"value": "LR"}
    assert result.artifacts[second["result"]] == {"value": "RL"}
    assert second["result"] not in result.cache_hits


def test_subgraph_dynamic_source_alias_keeps_other_local_role_in_key(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "a"}], "shared": "context"}

    graph = Subgraph(inputs=("items", "shared"), outputs={"result": "work"})

    @graph.map(
        "work",
        items_from=("items", "items"),
        deps=("shared",),
        key_fn=lambda item: item["id"],
    )
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": inputs["shared"]["shared"]}

    mounted = dag.mount(graph, "mounted", inputs={"items": "source", "shared": "source"})
    result = dag.run(targets=(mounted["result"],))
    item_metadata = (
        tmp_path / "artifacts" / "runs" / result.run_id / "mounted.work@a.json.meta.json"
    )
    key_components = json.loads(item_metadata.read_text(encoding="utf-8"))["key_components"]

    assert "upstream:shared" in key_components
    assert "upstream:items" not in key_components


def test_frozen_subgraph_mounts_twice_and_rejects_later_mutation(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("one")
    def one(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "one"}

    @dag.node("two")
    def two(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "two"}

    editorial, _draft, _publish = _editorial()
    first = dag.mount(editorial, "first", inputs={"source": "one"})
    second = dag.mount(editorial, "second", inputs={"source": "two"})

    assert (
        dag.run(targets=(first["result"], second["result"]))
        .artifacts[second["result"]]["text"]
        .endswith("two")
    )
    with pytest.raises(RuntimeError, match="frozen"):

        @editorial.node("late")
        def late(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"text": "late"}


def test_reused_subgraph_checkpoints_are_scoped_to_mounted_nodes(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("one")
    def one(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "one"}

    @dag.node("two")
    def two(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "two"}

    review = Subgraph(inputs=("source",), outputs={"result": "review"})

    @review.node("review", deps=("source",))
    def review_node(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"approval": ctx.checkpoint("approval", {"source": inputs["source"]["text"]})}

    left = dag.mount(review, "left", inputs={"source": "one"})
    right = dag.mount(review, "right", inputs={"source": "two"})
    first = dag.run(run_id="mounted-review", workers=2)
    approvals = tmp_path / "artifacts" / "runs" / first.run_id / "approvals"

    assert first.pending_checkpoints == [
        "approval@left.review",
        "approval@right.review",
    ]
    assert sorted(path.name for path in approvals.glob("*.pending.json")) == [
        "approval@left.review.pending.json",
        "approval@right.review.pending.json",
    ]

    dag.approve(first.run_id, "approval@left.review", {"accepted": "left"})
    assert not (approvals / "approval@left.review.pending.json").exists()
    second = dag.run(run_id=first.run_id, workers=2)

    assert second.pending_checkpoints == ["approval@right.review"]
    assert dag._render_runtime(first.run_id)["pending_nodes"] == {"right.review"}
    assert second.artifacts[left["result"]] == {"approval": {"accepted": "left"}}
    assert right["result"] not in second.artifacts

    dag.approve(first.run_id, "approval@right.review", {"accepted": "right"})
    completed = dag.run(run_id=first.run_id, workers=2)

    assert completed.pending_checkpoints == []
    assert completed.artifacts[right["result"]] == {"approval": {"accepted": "right"}}


@pytest.mark.parametrize(
    "segment",
    ["", "bad.name", "bad@name", "../escape", "nested/name", "nested\\name", ".", ".."],
)
def test_subgraph_rejects_invalid_single_segments(segment: str) -> None:
    with pytest.raises(ValueError, match="single non-empty segment"):
        Subgraph(inputs=(segment,), outputs={"result": "node"})
    with pytest.raises(ValueError, match="single non-empty segment"):
        Subgraph(inputs=("source",), outputs={segment: "node"})


def test_subgraph_rejects_local_node_that_shadows_an_input_port() -> None:
    graph = Subgraph(inputs=("source",), outputs={"result": "source"})

    with pytest.raises(ValueError, match="conflicts with an input port"):

        @graph.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"text": "ambiguous"}


def test_subgraph_reused_decorator_cannot_overwrite_local_node() -> None:
    graph = Subgraph(inputs=("source",), outputs={"result": "work"})
    decorator = graph.node("work", deps=("source",))

    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "first"}

    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "second"}

    decorator(first)
    with pytest.raises(ValueError, match="already declared"):
        decorator(second)


def test_mount_rejects_bindings_refs_outputs_and_namespace_transactionally(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "source"}

    valid, _draft, _publish = _editorial()
    with pytest.raises(ValueError, match="missing.*source"):
        dag.mount(valid, "missing", inputs={})
    with pytest.raises(ValueError, match="extra"):
        dag.mount(valid, "extra", inputs={"source": "source", "extra": "source"})

    unknown_ref = Subgraph(inputs=("source",), outputs={"result": "second"})

    @unknown_ref.node("first", deps=("missing",))
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "first"}

    @unknown_ref.node("second", deps=("first",))
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "second"}

    before = list(dag._nodes)
    with pytest.raises(ValueError, match="Unknown local reference.*missing"):
        dag.mount(unknown_ref, "broken", inputs={"source": "source"})
    assert list(dag._nodes) == before

    unknown_output = Subgraph(inputs=("source",), outputs={"result": "absent"})
    with pytest.raises(ValueError, match="Unknown subgraph output target"):
        dag.mount(unknown_output, "bad-output", inputs={"source": "source"})
    assert list(dag._nodes) == before

    dag.mount(valid, "editorial", inputs={"source": "source"})
    with pytest.raises(ValueError, match="already mounted"):
        dag.mount(valid, "editorial", inputs={"source": "source"})
    with pytest.raises(ValueError, match="single non-empty segment"):
        dag.mount(valid, "bad.namespace", inputs={"source": "source"})


def test_mount_collision_does_not_partially_register(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "source"}

    @dag.node("taken.second")
    def occupied(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "occupied"}

    graph = Subgraph(inputs=("source",), outputs={"result": "second"})

    @graph.node("first", deps=("source",))
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "first"}

    @graph.node("second", deps=("first",))
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"text": "second"}

    with pytest.raises(ValueError, match="already registered"):
        dag.mount(graph, "taken", inputs={"source": "source"})

    assert "taken.first" not in dag._nodes
    assert dag._nodes["taken.second"].function is occupied


def test_subgraph_map_scan_use_existing_scheduler_and_all_views(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "a", "n": 1}, {"id": "b", "n": 2}], "carry": 0}

    stages = Subgraph(inputs=("source",), outputs={"mapped": "mapped", "result": "scanned"})

    @stages.map("mapped", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def mapped(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        assert inputs == {}
        return {"n": item["n"] * 2}

    @stages.scan(
        "scanned",
        items_from=("source", "items"),
        carry_from=("source", "carry"),
        deps=("mapped",),
        key_fn=lambda item: item["id"],
        carry_fn=lambda artifact: artifact["total"],
    )
    def scanned(
        item: dict[str, Any], carry: int, inputs: dict[str, Any], ctx: Any
    ) -> dict[str, Any]:
        assert set(inputs) == {"mapped"}
        return {"total": carry + item["n"]}

    mounted = dag.mount(stages, "stages", inputs={"source": "source"})
    before = dag.plan()
    result = dag.run(targets=(mounted["result"],))
    after = dag.plan(targets=(mounted["result"],))
    explanation = dag.explain("stages.mapped@a", result.run_id)
    description = dag.describe()

    assert before.nodes["source"] == "miss"
    assert list(mounted) == ["mapped", "result"]
    assert result.artifacts[mounted["result"]]["items"]["b"]["total"] == 3
    assert after.nodes[mounted["result"]] == "hit"
    assert explanation.status == "hit"
    assert description["stages.mapped"]["items_from"] == {"node": "source", "path": "items"}
    assert description["stages.scanned"]["carry_from"] == {"node": "source", "path": "carry"}
    assert dag._cli_check(None) == 0
    assert "stages" in dag.render_summary()
    assert "flowchart TD" in dag.render_mermaid(result.run_id)
    assert "stages.mapped" in dag.render_pipeline(result.run_id)
    assert "stages.scanned" in dag.render_pipeline_text(result.run_id)
