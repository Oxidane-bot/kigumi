"""Declarative, reusable static DAG templates.

Subgraphs contain declarations only. :class:`kigumi.dag.Dag` owns mounting,
registration guards, scheduling, caching, materialization, and execution.
"""

from __future__ import annotations

import copy
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

from ._declarations import (
    CachePolicy,
    ConsumeFunction,
    external_fingerprint_digest,
    validate_cache_policy,
    validate_consumes,
    validate_segment,
)
from .prompt import PromptSpec, validate_prompt_bindings, validate_prompt_specs

AggregateFunction = Callable[[dict[str, dict[str, Any]], list[str]], dict[str, Any]]


@dataclass(frozen=True)
class _SubgraphNode:
    name: str
    function: Callable[..., dict[str, Any]]
    deps: tuple[str, ...]
    prompts: tuple[str, ...]
    prompt_specs: tuple[PromptSpec, ...]
    files: tuple[Path, ...]
    params: dict[str, Any]
    cache: CachePolicy
    external_fingerprint_digest: str | None
    consumes: dict[str, ConsumeFunction]
    items_from: tuple[str, str] | None = None
    key_fn: Callable[[Any], str] | None = None
    files_fn: Callable[[Any], Iterable[str | Path]] | None = None
    aggregate_fn: AggregateFunction | None = None
    scan: bool = False
    carry_from: tuple[str, str] | None = None
    carry_fn: Callable[[dict[str, Any]], Any] | None = None


class Subgraph:
    """A static multi-stage template that can be mounted into one or more DAGs."""

    def __init__(self, *, inputs: Iterable[str], outputs: Mapping[str, str]) -> None:
        declared_inputs = tuple(validate_segment(name, "Subgraph input port") for name in inputs)
        if len(set(declared_inputs)) != len(declared_inputs):
            raise ValueError("Subgraph input ports must be unique")
        declared_outputs: dict[str, str] = {}
        for port, target in outputs.items():
            declared_outputs[validate_segment(port, "Subgraph output port")] = validate_segment(
                target, "Subgraph output target"
            )
        self.inputs = declared_inputs
        self.outputs = MappingProxyType(declared_outputs)
        self._nodes: dict[str, _SubgraphNode] = {}
        self._frozen = False

    def node(
        self,
        name: str,
        deps: Iterable[str] = (),
        prompts: Iterable[str] = (),
        files: Iterable[str | Path] = (),
        params: dict[str, Any] | None = None,
        *,
        prompt_specs: Iterable[PromptSpec] = (),
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint: Any | None = None,
    ) -> Callable[[Callable[..., dict[str, Any]]], Callable[..., dict[str, Any]]]:
        """Declare one ordinary local node."""
        fixed_prompts = tuple(prompts)
        return self._decorator(
            name,
            deps=tuple(deps),
            prompts=fixed_prompts,
            prompt_specs=validate_prompt_specs(
                tuple(prompt_specs),
                legacy_prompts=fixed_prompts,
                dynamic_kind="node",
            ),
            files=tuple(Path(path) for path in files),
            params=copy.deepcopy(params) if params is not None else {},
            consumes=consumes,
            cache=cache,
            external_fingerprint=external_fingerprint,
        )

    def map(
        self,
        name: str,
        *,
        items_from: tuple[str, str],
        key_fn: Callable[[Any], str] | None = None,
        deps: Iterable[str] = (),
        prompts: Iterable[str] = (),
        prompt_specs: Iterable[PromptSpec] = (),
        files: Iterable[str | Path] = (),
        files_fn: Callable[[Any], Iterable[str | Path]] | None = None,
        params: dict[str, Any] | None = None,
        aggregate_fn: AggregateFunction | None = None,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint: Any | None = None,
    ) -> Callable[[Callable[..., dict[str, Any]]], Callable[..., dict[str, Any]]]:
        """Declare a local runtime map expansion."""
        _validate_locator(items_from, "items_from")
        source, path = items_from
        fixed_prompts = tuple(prompts)
        return self._decorator(
            name,
            deps=tuple(dict.fromkeys((*deps, source))),
            prompts=fixed_prompts,
            prompt_specs=validate_prompt_specs(
                tuple(prompt_specs),
                legacy_prompts=fixed_prompts,
                dynamic_kind="map",
            ),
            files=tuple(Path(file) for file in files),
            params=copy.deepcopy(params) if params is not None else {},
            consumes=consumes,
            cache=cache,
            external_fingerprint=external_fingerprint,
            items_from=(source, path),
            key_fn=key_fn,
            files_fn=files_fn,
            aggregate_fn=aggregate_fn,
        )

    def scan(
        self,
        name: str,
        *,
        items_from: tuple[str, str],
        key_fn: Callable[[Any], str] | None = None,
        carry_from: tuple[str, str] | None = None,
        carry_fn: Callable[[dict[str, Any]], Any] | None = None,
        deps: Iterable[str] = (),
        prompts: Iterable[str] = (),
        prompt_specs: Iterable[PromptSpec] = (),
        files: Iterable[str | Path] = (),
        files_fn: Callable[[Any], Iterable[str | Path]] | None = None,
        params: dict[str, Any] | None = None,
        aggregate_fn: AggregateFunction | None = None,
        consumes: Mapping[str, ConsumeFunction] | None = None,
        cache: CachePolicy = "auto",
        external_fingerprint: Any | None = None,
    ) -> Callable[[Callable[..., dict[str, Any]]], Callable[..., dict[str, Any]]]:
        """Declare a local runtime serial scan expansion."""
        _validate_locator(items_from, "items_from")
        if carry_from is not None:
            _validate_locator(carry_from, "carry_from")
        source, path = items_from
        fixed_prompts = tuple(prompts)
        refs = (*deps, source)
        if carry_from is not None:
            refs = (*refs, carry_from[0])
        return self._decorator(
            name,
            deps=tuple(dict.fromkeys(refs)),
            prompts=fixed_prompts,
            prompt_specs=validate_prompt_specs(
                tuple(prompt_specs),
                legacy_prompts=fixed_prompts,
                dynamic_kind="scan",
            ),
            files=tuple(Path(file) for file in files),
            params=copy.deepcopy(params) if params is not None else {},
            consumes=consumes,
            cache=cache,
            external_fingerprint=external_fingerprint,
            items_from=(source, path),
            key_fn=key_fn,
            files_fn=files_fn,
            aggregate_fn=aggregate_fn,
            scan=True,
            carry_from=carry_from,
            carry_fn=carry_fn,
        )

    def _decorator(
        self,
        name: str,
        *,
        deps: tuple[str, ...],
        prompts: tuple[str, ...],
        prompt_specs: tuple[PromptSpec, ...],
        files: tuple[Path, ...],
        params: dict[str, Any],
        consumes: Mapping[str, ConsumeFunction] | None,
        cache: CachePolicy,
        external_fingerprint: Any | None,
        items_from: tuple[str, str] | None = None,
        key_fn: Callable[[Any], str] | None = None,
        files_fn: Callable[[Any], Iterable[str | Path]] | None = None,
        aggregate_fn: AggregateFunction | None = None,
        scan: bool = False,
        carry_from: tuple[str, str] | None = None,
        carry_fn: Callable[[dict[str, Any]], Any] | None = None,
    ) -> Callable[[Callable[..., dict[str, Any]]], Callable[..., dict[str, Any]]]:
        if self._frozen:
            raise RuntimeError("Subgraph is frozen after its first mount")
        local_name = validate_segment(name, "Subgraph local node name")
        if local_name in self.inputs:
            raise ValueError(f"Subgraph local node {local_name!r} conflicts with an input port")
        if local_name in self._nodes:
            raise ValueError(f"Subgraph node {local_name!r} is already declared")
        policy = validate_cache_policy(cache)
        fingerprint_digest = external_fingerprint_digest(external_fingerprint)

        def register(
            function: Callable[..., dict[str, Any]],
        ) -> Callable[..., dict[str, Any]]:
            if self._frozen:
                raise RuntimeError("Subgraph is frozen after its first mount")
            if local_name in self._nodes:
                raise ValueError(f"Subgraph node {local_name!r} is already declared")
            projections = validate_consumes(
                local_name,
                deps,
                consumes,
                items_from=items_from,
                carry_from=carry_from,
            )
            function_inputs = set(deps)
            if items_from is not None:
                function_inputs.discard(items_from[0])
            if scan and carry_from is not None:
                function_inputs.discard(carry_from[0])
            validate_prompt_bindings(
                prompt_specs,
                inputs=function_inputs,
                params=set(params),
            )
            self._nodes[local_name] = _SubgraphNode(
                local_name,
                function,
                deps,
                prompts,
                prompt_specs,
                files,
                params,
                policy,
                fingerprint_digest,
                projections,
                items_from,
                key_fn,
                files_fn,
                aggregate_fn,
                scan,
                carry_from,
                carry_fn,
            )
            return function

        return register

    def _freeze(self) -> None:
        self._frozen = True


def _validate_locator(value: tuple[str, str], name: str) -> None:
    if not (
        isinstance(value, tuple)
        and len(value) == 2
        and all(isinstance(part, str) and part for part in value)
    ):
        raise ValueError(f"{name} must be a non-empty (node_name, artifact_key) tuple")
    validate_segment(value[0], f"Subgraph {name} source")
