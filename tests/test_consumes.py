from __future__ import annotations

import json
from collections.abc import Callable
from itertools import repeat
from pathlib import Path
from typing import Any

import pytest

import kigumi.dag as dag_module
from kigumi import Dag, Subgraph
from kigumi.artifacts import sha
from kigumi.calling import LLMCaller
from kigumi.config import KigumiConfig
from kigumi.testing import FakeTransport
from kigumi.transport import Response


def _make_dag(tmp_path: Path) -> Dag:
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    transport = FakeTransport(repeat(Response("model output", {"total_tokens": 1}, "stop")))
    return Dag(config, LLMCaller(transport, tmp_path / "llm"))


def _projection(artifact: dict[str, Any]) -> dict[str, Any]:
    return {"used": artifact["used"]}


def test_consumes_ignores_unconsumed_changes_across_run_and_plan(tmp_path: Path) -> None:
    def build(ignored: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source", params={"ignored": ignored})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"used": "stable", "ignored": ctx.params["ignored"]}

        @dag.node("leaf", deps=("source",), consumes={"source": _projection})
        def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return dict(inputs["source"])

        return dag

    first = build(1)
    first.run()
    changed = build(2)
    result = changed.run()

    assert result.cache_hits == ["leaf"]
    assert result.artifacts["leaf"] == {"used": "stable"}
    assert changed.plan().nodes == {"source": "hit", "leaf": "hit"}


def test_consumes_invalidates_consumed_changes_and_explain_names_edge(tmp_path: Path) -> None:
    def build(used: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source", params={"used": used})
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"used": ctx.params["used"], "ignored": 1}

        @dag.node("leaf", deps=("source",), consumes={"source": _projection})
        def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"used": inputs["source"]["used"]}

        return dag

    first = build(1)
    original = first.run()
    changed = build(2)
    result = changed.run()
    explanation = changed.explain("leaf", original.run_id)

    assert result.cache_hits == []
    assert explanation.status == "hit"
    assert explanation.changed == ["upstream:source"]


def test_consumes_view_is_canonical_and_hides_unprojected_fields(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"z": 2, "a": 1, "hidden": "secret"}

    @dag.node(
        "leaf",
        deps=("source",),
        consumes={"source": lambda artifact: {"z": artifact["z"], "a": artifact["a"]}},
    )
    def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        with pytest.raises(KeyError, match="hidden"):
            inputs["source"]["hidden"]
        return {"keys": list(inputs["source"]), "view": inputs["source"]}

    result = dag.run()
    metadata = json.loads(
        (tmp_path / "artifacts" / "runs" / result.run_id / "leaf.json.meta.json").read_text(
            encoding="utf-8"
        )
    )

    assert result.artifacts["leaf"] == {
        "keys": ["a", "z"],
        "view": {"a": 1, "z": 2},
    }
    assert metadata["key_components"]["upstream:source"] == sha({"a": 1, "z": 2})


def test_consumes_projector_code_is_irrelevant_when_view_is_equal(tmp_path: Path) -> None:
    def first_projector(artifact: dict[str, Any]) -> dict[str, Any]:
        return {"used": artifact["used"]}

    def equivalent_projector(artifact: dict[str, Any]) -> dict[str, Any]:
        return dict(used=artifact.get("used"))

    def build(projector: Callable[[dict[str, Any]], dict[str, Any]]) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"used": "same", "ignored": True}

        @dag.node("leaf", deps=("source",), consumes={"source": projector})
        def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return dict(inputs["source"])

        return dag

    build(first_projector).run()
    equivalent = build(equivalent_projector).run()

    assert equivalent.cache_hits == ["source", "leaf"]


def test_consumes_rejects_unknown_items_and_carry_sources_at_registration(
    tmp_path: Path,
) -> None:
    def projector(artifact: dict[str, Any]) -> dict[str, Any]:
        return artifact

    dag = _make_dag(tmp_path / "node")
    with pytest.raises(ValueError, match="consumes.*missing.*declared dependency"):

        @dag.node("bad", deps=("source",), consumes={"missing": projector})
        def bad(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {}

    mapped = _make_dag(tmp_path / "map")
    with pytest.raises(ValueError, match="consumes.*source.*items_from"):

        @mapped.map("bad", items_from=("source", "items"), consumes={"source": projector})
        def bad_map(item: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {}

    scanned = _make_dag(tmp_path / "scan")
    with pytest.raises(ValueError, match="consumes.*initial.*carry_from"):

        @scanned.scan(
            "bad",
            items_from=("source", "items"),
            carry_from=("initial", "value"),
            consumes={"initial": projector},
        )
        def bad_scan(item: Any, carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {}

    foreach = _make_dag(tmp_path / "foreach")
    with pytest.raises(ValueError, match="consumes.*missing.*declared dependency"):

        @foreach.foreach("bad-{i}", [1], deps=("source",), consumes={"missing": projector})
        def bad_foreach(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {}

    graph = Subgraph(inputs=("source",), outputs={"result": "bad"})
    with pytest.raises(ValueError, match="consumes.*missing.*declared dependency"):

        @graph.node("bad", deps=("source",), consumes={"missing": projector})
        def bad_subgraph(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {}


@pytest.mark.parametrize(
    ("projector", "reason"),
    [
        (lambda artifact: [artifact["used"]], "must return a dict"),
        (lambda artifact: {"bad": object()}, "JSON serializable"),
    ],
)
def test_consumes_rejects_invalid_projection_results_with_edge_context(
    tmp_path: Path,
    projector: Callable[[dict[str, Any]], Any],
    reason: str,
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        return {"used": 1}

    @dag.node("leaf", deps=("source",), consumes={"source": projector})
    def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {}

    with pytest.raises(RuntimeError, match=rf"Node 'leaf'.*dependency 'source'.*{reason}"):
        dag.run()


def test_consumes_projector_failure_matches_in_plan_and_run(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        return {"used": 1}

    def broken(artifact: dict[str, Any]) -> dict[str, Any]:
        raise LookupError("projection broke")

    @dag.node("leaf", deps=("source",), consumes={"source": broken})
    def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {}

    dag.run(targets=("source",))
    with pytest.raises(RuntimeError) as plan_error:
        dag.plan(targets=("leaf",))
    with pytest.raises(RuntimeError) as run_error:
        dag.run(targets=("leaf",))

    assert str(plan_error.value) == str(run_error.value)
    assert "Node 'leaf' consumes dependency 'source'" in str(run_error.value)
    assert "LookupError: projection broke" in str(run_error.value)


def test_consumes_preserves_label_set_and_describe_reports_only_declarations(
    tmp_path: Path,
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        return {"used": 1}

    @dag.node("plain", deps=("source",))
    def plain(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return dict(inputs["source"])

    @dag.node("projected", deps=("source",), consumes={"source": _projection})
    def projected(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return dict(inputs["source"])

    source_artifact = {"used": 1}
    plain_components = dag._key_components(
        dag._nodes["plain"], {"source": sha(source_artifact)}, dag._libs_hash()
    )
    projected_components = dag._key_components(
        dag._nodes["projected"],
        {"source": sha(source_artifact)},
        dag._libs_hash(),
        upstream_artifacts={"source": source_artifact},
    )
    description = dag.describe()

    assert set(projected_components) == set(plain_components)
    assert description["projected"]["consumes"] == ["source"]
    assert "consumes" not in description["plain"]


def test_foreach_consumes_projects_each_registered_node(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"used": "visible", "hidden": "secret"}

    @dag.foreach(
        "work-{i}",
        [0, 1],
        deps=("source",),
        consumes={"source": _projection},
    )
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        assert inputs == {"source": {"used": "visible"}}
        return dict(inputs["source"])

    result = dag.run()

    assert result.artifacts["work-0"] == {"used": "visible"}
    assert result.artifacts["work-1"] == {"used": "visible"}


@pytest.mark.parametrize("kind", ["map", "scan"])
def test_dynamic_consumes_projects_shared_dependencies_per_item(tmp_path: Path, kind: str) -> None:
    executed: list[str] = []

    def build(ignored: int) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("source")
        def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"items": [{"id": "a"}, {"id": "b"}]}

        @dag.node("shared", params={"ignored": ignored})
        def shared(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"used": "stable", "ignored": ctx.params["ignored"]}

        decorator = getattr(dag, kind)(
            "work",
            items_from=("source", "items"),
            key_fn=lambda item: item["id"],
            deps=("shared",),
            consumes={"shared": _projection},
        )

        if kind == "map":

            @decorator
            def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
                executed.append(item["id"])
                assert inputs == {"shared": {"used": "stable"}}
                return {"id": item["id"]}

        else:

            @decorator
            def work(
                item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any
            ) -> dict[str, Any]:
                executed.append(item["id"])
                assert inputs == {"shared": {"used": "stable"}}
                return {"id": item["id"]}

        return dag

    build(1).run()
    executed.clear()
    result = build(2).run()

    assert executed == []
    assert result.map_items["work"] == {"a": "hit", "b": "hit"}


def test_subgraph_consumes_uses_local_dependency_names_after_mount(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("outer")
    def outer(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"used": 1, "hidden": 2}

    graph = Subgraph(inputs=("source",), outputs={"result": "leaf"})

    @graph.node("leaf", deps=("source",), consumes={"source": _projection})
    def leaf(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        assert inputs == {"source": {"used": 1}}
        return dict(inputs["source"])

    mounted = dag.mount(graph, "part", inputs={"source": "outer"})
    result = dag.run()
    metadata = json.loads(
        (tmp_path / "artifacts" / "runs" / result.run_id / "part.leaf.json.meta.json").read_text(
            encoding="utf-8"
        )
    )

    assert result.artifacts[mounted["result"]] == {"used": 1}
    assert metadata["key_components"]["upstream:source"] == sha({"used": 1})
    assert "upstream:outer" not in metadata["key_components"]


def test_consumes_cache_schema_is_four() -> None:
    assert dag_module.CACHE_SCHEMA == 4
