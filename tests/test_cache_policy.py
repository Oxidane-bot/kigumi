from __future__ import annotations

import json
from collections.abc import Callable
from itertools import repeat
from pathlib import Path
from typing import Any

import pytest

from kigumi import CachePolicy, Dag, Subgraph
from kigumi.artifacts import sha
from kigumi.calling import LLMCaller
from kigumi.config import KigumiConfig
from kigumi.inspect import trace_run
from kigumi.testing import FakeTransport
from kigumi.transport import Response


def _make_dag(tmp_path: Path) -> Dag:
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    transport = FakeTransport(repeat(Response("model output", {"total_tokens": 1}, "stop")))
    return Dag(config, LLMCaller(transport, tmp_path / "llm"))


@pytest.mark.parametrize(
    ("policy", "second_hits", "executions", "cache_files"),
    [
        ("auto", ["work"], 1, 1),
        ("refresh", [], 2, 1),
        ("off", [], 2, 0),
    ],
)
def test_cache_policy_repeated_runs_and_plan(
    tmp_path: Path,
    policy: CachePolicy,
    second_hits: list[str],
    executions: int,
    cache_files: int,
) -> None:
    dag = _make_dag(tmp_path)
    calls = 0

    @dag.node("work", cache=policy)
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"value": calls}

    first = dag.run()
    second = dag.run()

    assert first.cache_hits == []
    assert second.cache_hits == second_hits
    assert calls == executions
    assert dag.plan().nodes == {"work": "hit" if policy == "auto" else "miss"}
    cache_root = tmp_path / "artifacts" / "_cache" / "nodes"
    actual_cache_files = len(list(cache_root.glob("*.json"))) if cache_root.is_dir() else 0
    assert actual_cache_files == cache_files


def test_force_does_not_turn_off_cache_writes_on(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("work", cache="off")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "fresh"}

    result = dag.run(force=("work",))
    key = json.loads(
        (tmp_path / "artifacts" / "runs" / result.run_id / "work.json.meta.json").read_text(
            encoding="utf-8"
        )
    )["cache_key"]

    assert not (tmp_path / "artifacts" / "_cache" / "nodes" / f"{key}.json").exists()


@pytest.mark.parametrize("policy", ["refresh", "off"])
def test_non_auto_policy_is_miss_and_makes_downstream_unknown(
    tmp_path: Path, policy: CachePolicy
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source", cache=policy)
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "stable"}

    @dag.node("downstream", deps=("source",))
    def downstream(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": inputs["source"]["value"]}

    dag.run()

    assert dag.plan().nodes == {"source": "miss", "downstream": "unknown"}


@pytest.mark.parametrize("policy", ["refresh", "off"])
def test_map_item_cache_policy_executes_every_item_and_plan_reports_miss(
    tmp_path: Path, policy: CachePolicy
) -> None:
    dag = _make_dag(tmp_path)
    calls: list[str] = []

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.map(
        "work",
        items_from=("source", "items"),
        key_fn=lambda item: item["id"],
        cache=policy,
    )
    def work(item: dict[str, str], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        calls.append(item["id"])
        return {"id": item["id"]}

    dag.run()
    second = dag.run()

    assert calls == ["a", "b", "a", "b"]
    assert second.map_items == {"work": {"a": "miss", "b": "miss"}}
    assert dag.plan().nodes == {
        "source": "hit",
        "work@a": "miss",
        "work@b": "miss",
        "work": "miss",
    }
    for item in ("a", "b"):
        metadata = json.loads(
            (
                tmp_path / "artifacts" / "runs" / second.run_id / f"work@{item}.json.meta.json"
            ).read_text(encoding="utf-8")
        )
        assert metadata["cache_policy"] == policy
        if policy == "off":
            assert not (
                tmp_path / "artifacts" / "_cache" / "nodes" / f"{metadata['cache_key']}.json"
            ).exists()


@pytest.mark.parametrize("kind", ["map", "scan"])
@pytest.mark.parametrize("policy", ["refresh", "off"])
def test_non_auto_empty_dynamic_node_is_still_a_miss(
    tmp_path: Path, kind: str, policy: CachePolicy
) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[Any]]:
        return {"items": []}

    decorator = (
        dag.map("work", items_from=("source", "items"), cache=policy)
        if kind == "map"
        else dag.scan("work", items_from=("source", "items"), cache=policy)
    )

    @decorator
    def work(*args: Any) -> dict[str, str]:
        raise AssertionError("empty dynamic node must not execute an item")

    first = dag.run()
    second = dag.run()

    assert first.cache_hits == []
    assert second.cache_hits == ["source"]
    assert dag.plan().nodes == {"source": "hit", "work": "miss"}


def test_external_fingerprint_changes_owner_then_downstream_and_uses_exact_digest(
    tmp_path: Path,
) -> None:
    def build(fingerprint: Any) -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node("owner", external_fingerprint=fingerprint)
        def owner(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": fingerprint["revision"]}

        @dag.node("downstream", deps=("owner",))
        def downstream(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
            return {"value": inputs["owner"]["value"]}

        return dag

    first = build({"revision": 1})
    run = first.run()
    first_components = json.loads(
        (tmp_path / "artifacts" / "runs" / run.run_id / "owner.json.meta.json").read_text(
            encoding="utf-8"
        )
    )["key_components"]
    assert first_components["external"] == sha({"revision": 1})

    unchanged = build({"revision": 1})
    assert unchanged.plan().nodes == {"owner": "hit", "downstream": "hit"}

    changed = build({"revision": 2})
    assert changed.plan().nodes == {"owner": "miss", "downstream": "unknown"}
    changed_result = changed.run()
    assert changed_result.cache_hits == []


def test_external_fingerprint_is_validated_and_never_exposed_raw(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)
    with pytest.raises(ValueError, match="external_fingerprint.*JSON serializable"):
        dag.node("bad", external_fingerprint=object())

    graph = Subgraph(inputs=("source",), outputs={"result": "work"})
    with pytest.raises(ValueError, match="external_fingerprint.*JSON serializable"):
        graph.node("work", deps=("source",), external_fingerprint={1, 2})

    secret = {"credential": "must-not-leak"}

    @dag.node("safe", external_fingerprint=secret)
    def safe(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "ok"}

    result = dag.run()
    description = dag.describe()["safe"]
    metadata_text = (
        tmp_path / "artifacts" / "runs" / result.run_id / "safe.json.meta.json"
    ).read_text(encoding="utf-8")

    assert description["has_external_fingerprint"] is True
    assert "external_fingerprint" not in description
    assert "must-not-leak" not in metadata_text


def test_key_component_labels_add_only_external_when_supplied(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("plain")
    def plain(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "plain"}

    @dag.node("external", external_fingerprint="revision-7")
    def external(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "external"}

    plain_components = dag._key_components(dag._nodes["plain"], {}, dag._libs_hash())
    external_components = dag._key_components(dag._nodes["external"], {}, dag._libs_hash())

    assert set(plain_components) == {"source", "libs", "params", "kigumi"}
    assert set(external_components) == {*plain_components, "external"}
    assert external_components["external"] == sha("revision-7")


def test_scan_explain_without_initial_carry_uses_run_key_components(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("source")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, list[dict[str, str]]]:
        return {"items": [{"id": "a"}, {"id": "b"}]}

    @dag.scan("work", items_from=("source", "items"), key_fn=lambda item: item["id"])
    def work(item: dict[str, str], carry: Any, inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"id": item["id"]}

    result = dag.run()

    assert dag.explain("work@a", result.run_id).changed == []
    assert dag.explain("work@b", result.run_id).changed == []


def test_sidecar_and_trace_expose_outputs_and_cache_policy(tmp_path: Path) -> None:
    dag = _make_dag(tmp_path)

    @dag.node("work", cache="refresh")
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"files": {"z.txt": "z", "a.txt": "a"}}

    @dag.node("empty")
    def empty(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "none"}

    result = dag.run()
    work_metadata = json.loads(
        (tmp_path / "artifacts" / "runs" / result.run_id / "work.json.meta.json").read_text(
            encoding="utf-8"
        )
    )
    empty_metadata = json.loads(
        (tmp_path / "artifacts" / "runs" / result.run_id / "empty.json.meta.json").read_text(
            encoding="utf-8"
        )
    )
    traced = trace_run(tmp_path / "artifacts", tmp_path / "llm", result.run_id)
    by_name = {entry["name"]: entry for entry in traced["nodes"]}

    assert work_metadata["outputs"] == ["a.txt", "z.txt"]
    assert work_metadata["cache_policy"] == "refresh"
    assert empty_metadata["outputs"] == []
    assert empty_metadata["cache_policy"] == "auto"
    assert by_name["work"]["outputs"] == ["a.txt", "z.txt"]
    assert by_name["work"]["cache_policy"] == "refresh"


def test_cache_policy_rejects_every_non_literal_value_at_registration(tmp_path: Path) -> None:
    invalid: list[Any] = [True, False, "read", "AUTO", None, 1]
    decorators: list[Callable[[Dag, Any], Any]] = [
        lambda dag, value: dag.node("n", cache=value),
        lambda dag, value: dag.map("m", items_from=("source", "items"), cache=value),
        lambda dag, value: dag.scan("s", items_from=("source", "items"), cache=value),
        lambda dag, value: dag.foreach("f-{i}", [1], cache=value),
    ]
    for index, decorator in enumerate(decorators):
        for value in invalid:
            with pytest.raises(ValueError, match="cache must be exactly"):
                decorator(_make_dag(tmp_path / f"dag-{index}-{value!s}"), value)


@pytest.mark.parametrize("kind", ["node", "map", "scan"])
def test_subgraph_cache_policy_is_validated_at_declaration(kind: str) -> None:
    graph = Subgraph(inputs=("source",), outputs={"result": "work"})
    with pytest.raises(ValueError, match="cache must be exactly"):
        if kind == "node":
            graph.node("work", deps=("source",), cache=True)  # type: ignore[arg-type]
        elif kind == "map":
            graph.map(
                "work",
                items_from=("source", "items"),
                cache="AUTO",  # type: ignore[arg-type]
            )
        else:
            graph.scan(
                "work",
                items_from=("source", "items"),
                cache=None,  # type: ignore[arg-type]
            )
