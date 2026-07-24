from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from kigumi import (
    CarryRef,
    InputRef,
    ItemRef,
    ParamRef,
    PromptAxis,
    PromptDefinitionError,
    PromptLayer,
    PromptMaterial,
    PromptRef,
    PromptResolutionError,
    PromptSpec,
    ResolvedPrompt,
)
from kigumi._runstate import RunManifestError
from kigumi.artifacts import sha
from kigumi.calling import LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.testing import FakeTransport
from tests._dag_helpers import _make_dag


def _write_prompts(root: Path, values: dict[str, str]) -> None:
    for name, text in values.items():
        path = root / "prompts" / f"{name}.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def _item_spec() -> PromptSpec:
    return PromptSpec(
        name="process_item",
        base=PromptRef("base/task"),
        layers=(
            PromptLayer(slot="common", source=PromptRef("common/rules")),
            PromptLayer(
                slot="method",
                source=PromptAxis(
                    name="mode",
                    selector=InputRef("config", path=("mode",)),
                    variants={
                        "concise": PromptRef("variants/concise"),
                        "detailed": PromptRef("variants/detailed"),
                    },
                ),
            ),
        ),
        materials=(
            PromptMaterial(
                slot="context",
                source=InputRef("config", path=("context",)),
                title="运行上下文",
            ),
            PromptMaterial(slot="item", source=ItemRef(), title="当前项"),
        ),
    )


def test_prompt_spec_resolves_layers_axis_and_fenced_material_before_call(tmp_path: Path) -> None:
    _write_prompts(
        tmp_path,
        {
            "base/task": "A{{common}}B{{method}}C{{context}}D{{item}}E",
            "common/rules": "<common>",
            "variants/concise": "<concise>",
            "variants/detailed": "<detailed>",
        },
    )
    dag = _make_dag(tmp_path)

    @dag.node("config")
    def config(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"mode": "concise", "context": {"tone": "clear"}}

    @dag.node("plan")
    def plan(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        return {"items": [{"id": "one", "title": "Entry"}]}

    @dag.map(
        "write",
        items_from=("plan", "items"),
        deps=("config",),
        key_fn=lambda item: item["id"],
        prompt_specs=(_item_spec(),),
    )
    def write(item: dict[str, Any], inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        resolved = ctx.resolve_prompt("process_item")
        assert isinstance(resolved, ResolvedPrompt)
        assert resolved.resolution.axes[0]["selected"] == "concise"
        return {"prompt": resolved}

    result = dag.run()
    rendered = result.artifacts["write"]["items"]["one"]["prompt"]

    assert rendered.startswith("A<common>B<concise>C## 运行上下文\n\n```json\n")
    assert '"tone": "clear"' in rendered
    assert "D## 当前项\n\n```json\n" in rendered
    assert rendered.endswith("\n```\nE")


@pytest.mark.parametrize(
    "ref",
    (
        "/absolute",
        "../escape",
        "nested/../../escape",
        r"windows\escape",
        "nul\x00path",
    ),
)
def test_prompt_ref_rejects_unsafe_paths(ref: str) -> None:
    with pytest.raises(PromptDefinitionError):
        PromptRef(ref)


def test_input_and_param_refs_accept_declared_keys_that_are_not_prompt_identifiers(
    tmp_path: Path,
) -> None:
    _write_prompts(tmp_path, {"base": "{{material}}"})
    dag = _make_dag(tmp_path)
    spec = PromptSpec(
        "managed",
        PromptRef("base"),
        materials=(
            PromptMaterial(
                "material",
                InputRef("source.with-dot", ("value",)),
            ),
        ),
    )

    @dag.node("source.with-dot")
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": "accepted"}

    @dag.node("work", deps=("source.with-dot",), prompt_specs=(spec,))
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"prompt": ctx.resolve_prompt("managed")}

    result = dag.run()

    assert "accepted" in result.artifacts["work"]["prompt"]
    assert ParamRef("content-type").param == "content-type"


def test_prompt_definition_fails_before_node_side_effects(tmp_path: Path) -> None:
    _write_prompts(
        tmp_path,
        {
            "base": "{{fragment}}{{missing}}",
            "fragment": "contains {{nested}}",
        },
    )
    dag = _make_dag(tmp_path)
    executed: list[str] = []
    spec = PromptSpec(
        name="bad",
        base=PromptRef("base"),
        layers=(PromptLayer("fragment", PromptRef("fragment")),),
    )

    @dag.node("bad", prompt_specs=(spec,))
    def bad(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        executed.append("ran")
        return {"value": "no"}

    with pytest.raises(PromptDefinitionError):
        dag.run()
    assert executed == []


def test_fragment_with_nested_slot_is_rejected_before_execution(tmp_path: Path) -> None:
    _write_prompts(
        tmp_path,
        {
            "base": "{{fragment}}",
            "fragment": "contains {{nested}}",
        },
    )
    dag = _make_dag(tmp_path)
    spec = PromptSpec(
        name="bad",
        base=PromptRef("base"),
        layers=(PromptLayer("fragment", PromptRef("fragment")),),
    )

    @dag.node("bad", prompt_specs=(spec,))
    def bad(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        raise AssertionError("nested fragment validation must happen first")

    with pytest.raises(PromptDefinitionError, match="may not contain slots"):
        dag.run()


@pytest.mark.parametrize(
    ("selector", "match"),
    (
        ("missing", "missing"),
        (3, "string"),
        ("science", "unknown"),
    ),
)
def test_axis_resolution_errors_happen_before_calls(
    tmp_path: Path, selector: Any, match: str
) -> None:
    _write_prompts(
        tmp_path,
        {
            "base": "{{layer}}",
            "concise": "concise",
        },
    )
    dag = _make_dag(tmp_path)
    spec = PromptSpec(
        name="axis",
        base=PromptRef("base"),
        layers=(
            PromptLayer(
                "layer",
                PromptAxis(
                    "mode",
                    InputRef("source", path=("selector",)),
                    {"concise": PromptRef("concise")},
                ),
            ),
        ),
    )

    @dag.node("source", params={"selector": selector})
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        if ctx.params["selector"] == "missing":
            return {}
        return {"selector": ctx.params["selector"]}

    @dag.node("work", deps=("source",), prompt_specs=(spec,))
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"value": ctx.call(ctx.resolve_prompt("axis"))}

    with pytest.raises(PromptResolutionError, match=match):
        dag.run()
    assert dag.caller.calls == []


def test_selected_only_prompt_cache_ignores_inactive_variant_bytes(tmp_path: Path) -> None:
    _write_prompts(
        tmp_path,
        {
            "base": "{{layer}}",
            "concise": "active-v1",
            "detailed": "inactive-v1",
        },
    )

    def build() -> Any:
        dag = _make_dag(tmp_path)
        spec = PromptSpec(
            name="axis",
            base=PromptRef("base"),
            layers=(
                PromptLayer(
                    "layer",
                    PromptAxis(
                        "mode",
                        ParamRef("mode"),
                        {
                            "concise": PromptRef("concise"),
                            "detailed": PromptRef("detailed"),
                        },
                    ),
                ),
            ),
        )

        @dag.node("work", params={"mode": "concise"}, prompt_specs=(spec,))
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"prompt": ctx.resolve_prompt("axis")}

        return dag

    first = build().run(run_id="first")
    first_meta = json.loads(
        (tmp_path / "artifacts" / "runs" / first.run_id / "work.json.meta.json").read_text()
    )
    _write_prompts(tmp_path, {"detailed": "inactive-v2"})
    second = build().run(run_id="second")
    second_meta = json.loads(
        (tmp_path / "artifacts" / "runs" / second.run_id / "work.json.meta.json").read_text()
    )

    assert second.cache_hits == ["work"]
    assert first_meta["cache_key"] == second_meta["cache_key"]

    _write_prompts(tmp_path, {"concise": "active-v2"})
    third = build().run(run_id="third")
    assert third.cache_hits == []


def test_inactive_variant_change_rejects_resume_but_not_selected_cache(
    tmp_path: Path,
) -> None:
    _write_prompts(
        tmp_path,
        {
            "base": "{{layer}}",
            "concise": "active",
            "detailed": "inactive-v1",
        },
    )

    def build() -> Dag:
        dag = _make_dag(tmp_path)
        spec = PromptSpec(
            "managed",
            PromptRef("base"),
            layers=(
                PromptLayer(
                    "layer",
                    PromptAxis(
                        "mode",
                        ParamRef("mode"),
                        {
                            "concise": PromptRef("concise"),
                            "detailed": PromptRef("detailed"),
                        },
                    ),
                ),
            ),
        )

        @dag.node("work", params={"mode": "concise"}, prompt_specs=(spec,))
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"prompt": ctx.resolve_prompt("managed")}

        return dag

    first = build()
    first.run(run_id="bound")
    _write_prompts(tmp_path, {"detailed": "inactive-v2"})

    with pytest.raises(RunManifestError, match="declaration changed"):
        build().resume("bound")

    fresh = build().run(run_id="fresh")
    assert fresh.cache_hits == ["work"]


def test_every_declared_prompt_spec_is_a_conservative_node_input(tmp_path: Path) -> None:
    _write_prompts(
        tmp_path,
        {
            "used": "used",
            "unused": "unused-v1",
        },
    )

    def build() -> Dag:
        dag = _make_dag(tmp_path)

        @dag.node(
            "work",
            prompt_specs=(
                PromptSpec("used", PromptRef("used")),
                PromptSpec("unused", PromptRef("unused")),
            ),
        )
        def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"prompt": ctx.resolve_prompt("used")}

        return dag

    build().run(run_id="all-specs-1")
    _write_prompts(tmp_path, {"unused": "unused-v2"})
    second = build().run(run_id="all-specs-2")

    assert second.cache_hits == []


def test_resume_rejects_rehashed_but_internally_corrupt_origin_resolution(
    tmp_path: Path,
) -> None:
    _write_prompts(tmp_path, {"base": "managed"})
    dag = _make_dag(tmp_path)

    @dag.node(
        "work",
        cache="off",
        prompt_specs=(PromptSpec("managed", PromptRef("base")),),
    )
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"prompt": ctx.resolve_prompt("managed")}

    result = dag.run(run_id="corrupt-origin")
    sidecar = tmp_path / "artifacts" / "runs" / result.run_id / "work.json.meta.json"
    metadata = json.loads(sidecar.read_text())
    metadata["origin_provenance"]["prompt_resolutions"]["managed"]["resolution_digest"] = "corrupt"
    metadata["origin_provenance_digest"] = sha(metadata["origin_provenance"])
    sidecar.write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(RunManifestError, match="origin Prompt resolutions"):
        dag.resume(result.run_id)


def test_run_snapshot_prevents_mid_run_prompt_drift(tmp_path: Path) -> None:
    _write_prompts(tmp_path, {"base": "{{layer}}", "layer": "stable"})
    changed = False

    def mutate_after_first(name: str, artifact: dict[str, Any], hit: bool) -> None:
        nonlocal changed
        del artifact, hit
        if name == "first":
            _write_prompts(tmp_path, {"layer": "changed"})
            changed = True

    dag = _make_dag(tmp_path, mutate_after_first)
    spec = PromptSpec(
        name="shared",
        base=PromptRef("base"),
        layers=(PromptLayer("layer", PromptRef("layer")),),
    )

    @dag.node("first", prompt_specs=(spec,))
    def first(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"prompt": ctx.resolve_prompt("shared")}

    @dag.node("second", deps=("first",), prompt_specs=(spec,))
    def second(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        return {"prompt": ctx.resolve_prompt("shared")}

    result = dag.run()

    assert changed is True
    assert result.artifacts["first"]["prompt"] == "stable"
    assert result.artifacts["second"]["prompt"] == "stable"


def test_item_and_carry_refs_are_restricted_to_dynamic_node_kinds(tmp_path: Path) -> None:
    spec = PromptSpec(
        name="bad",
        base=PromptRef("base"),
        materials=(
            PromptMaterial("item", ItemRef()),
            PromptMaterial("carry", CarryRef()),
        ),
    )
    dag = _make_dag(tmp_path)

    with pytest.raises(PromptDefinitionError, match="ItemRef"):

        @dag.node("bad", prompt_specs=(spec,))
        def bad(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
            return {"value": "bad"}


def test_resolved_prompt_loses_lineage_on_string_operations(tmp_path: Path) -> None:
    _write_prompts(tmp_path, {"base": "managed"})
    dag = _make_dag(tmp_path)
    spec = PromptSpec(name="managed", base=PromptRef("base"))

    @dag.node("work", prompt_specs=(spec,))
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        resolved = ctx.resolve_prompt("managed")
        assert isinstance(resolved, ResolvedPrompt)
        assert not isinstance(resolved + "!", ResolvedPrompt)
        ctx.call(resolved)
        ctx.call(resolved + "!")
        return {"value": "ok"}

    result = dag.run()
    sidecar = json.loads(
        (tmp_path / "artifacts" / "runs" / result.run_id / "work.json.meta.json").read_text()
    )

    assert sidecar["calls"][0]["prompt_resolution"]["resolution_digest"]
    assert "prompt_resolution" not in sidecar["calls"][1]


def test_validated_repair_keeps_base_resolution_and_tracks_each_actual_call(
    tmp_path: Path,
) -> None:
    class Answer(BaseModel):
        value: str

    _write_prompts(tmp_path, {"base": "managed"})
    dag = Dag(
        KigumiConfig(project_root=tmp_path, source_dirs=[]),
        LLMCaller(
            FakeTransport(['{"wrong": true}', '{"value": "fixed"}']),
            tmp_path / "llm",
        ),
    )
    spec = PromptSpec(name="managed", base=PromptRef("base"))

    @dag.node("work", prompt_specs=(spec,))
    def work(inputs: dict[str, Any], ctx: Any) -> dict[str, str]:
        answer = ctx.call_validated(
            ctx.resolve_prompt("managed"),
            Answer,
            max_repairs=1,
        )
        return answer.model_dump()

    result = dag.run()
    sidecar = json.loads(
        (tmp_path / "artifacts" / "runs" / result.run_id / "work.json.meta.json").read_text()
    )
    primary, repair = [call["prompt_resolution"] for call in sidecar["calls"]]

    assert primary["base_resolution_digest"] == repair["base_resolution_digest"]
    assert (primary["phase"], primary["repair_round"]) == ("primary", 0)
    assert (repair["phase"], repair["repair_round"]) == ("repair", 1)
    assert sidecar["calls"][0]["prompt_sha"] != sidecar["calls"][1]["prompt_sha"]
