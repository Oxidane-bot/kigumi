from __future__ import annotations

import importlib.util
from collections.abc import Callable
from itertools import repeat
from pathlib import Path
from typing import Any

from kigumi.calling import LLMCaller
from kigumi.config import KigumiConfig
from kigumi.dag import Dag
from kigumi.testing import FakeTransport
from kigumi.transport import Response


def _make_dag(
    tmp_path: Path,
    post_node: Callable[[str, dict[str, Any], bool], None] | None = None,
) -> Dag:
    config = KigumiConfig(project_root=tmp_path, source_dirs=[])
    transport = FakeTransport(repeat(Response("model output", {"total_tokens": 1}, "stop")))
    return Dag(config, LLMCaller(transport, tmp_path / "llm"), post_node=post_node)


def _load_work(
    path: Path,
    docstring: str,
    value: int,
) -> Callable[[dict[str, Any], Any], dict[str, int]]:
    path.write_text(
        f'def work(inputs, ctx):\n    """{docstring}"""\n    return {{\'value\': {value}}}\n',
        encoding="utf-8",
    )
    spec = importlib.util.spec_from_file_location(f"dag_version_{path.stem}", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.work


def _build_scan_dag(
    tmp_path: Path,
    items: list[dict[str, Any]],
    *,
    initial: int = 0,
    carry_fn: Callable[[dict[str, Any]], Any] | None = None,
    fail: Callable[[str], bool] | None = None,
    attempts: dict[str, int] | None = None,
    item_files: bool = False,
) -> tuple[Dag, list[str]]:
    """Build a small carry chain whose source list may change between DAG instances."""
    dag = _make_dag(tmp_path)
    executed: list[str] = []
    effective_carry_fn = carry_fn or (lambda artifact: artifact["carry"])

    @dag.node("source", params={"items": items})
    def source(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        return {"items": ctx.params["items"]}

    @dag.node("initial", params={"initial": initial})
    def initial_node(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
        del inputs
        return {"carry": {"total": ctx.params["initial"]}}

    @dag.scan(
        "chain",
        items_from=("source", "items"),
        carry_from=("initial", "carry"),
        key_fn=lambda item: item["id"],
        carry_fn=effective_carry_fn,
        files_fn=(lambda item: (item["file"],)) if item_files else None,
    )
    def chain(
        item: dict[str, Any], carry: dict[str, int], inputs: dict[str, Any], ctx: Any
    ) -> dict[str, Any]:
        del inputs, ctx
        executed.append(item["id"])
        if fail is not None and fail(item["id"]):
            raise ValueError(f"broken {item['id']}")
        number = attempts.get(item["id"], 0) + 1 if attempts is not None else 1
        if attempts is not None:
            attempts[item["id"]] = number
        total = carry["total"] + int(item["value"])
        return {"carry": {"total": total, "attempt": number}, "id": item["id"]}

    @dag.node("after", deps=("chain",))
    def after(inputs: dict[str, Any], ctx: Any) -> dict[str, int]:
        return {"total": inputs["chain"]["items"]["c"]["carry"]["total"]}

    return dag, executed
