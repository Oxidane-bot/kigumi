"""Shared declaration validation that does not depend on the DAG scheduler."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Literal, TypeAlias

from .artifacts import canonical_json, sha

CachePolicy: TypeAlias = Literal["auto", "refresh", "off"]
ConsumeFunction: TypeAlias = Callable[[dict[str, Any]], dict[str, Any]]


def validate_cache_policy(value: Any) -> CachePolicy:
    """Accept only the three public L3 policy literals."""
    if not isinstance(value, str) or value not in {"auto", "refresh", "off"}:
        raise ValueError("cache must be exactly one of: 'auto', 'refresh', 'off'")
    return value  # type: ignore[return-value]


def external_fingerprint_digest(value: Any | None) -> str | None:
    """Validate one declaration fingerprint and retain only its digest."""
    if value is None:
        return None
    try:
        canonical_json(value)
    except (TypeError, ValueError) as error:
        raise ValueError("external_fingerprint must be JSON serializable") from error
    return sha(value)


def validate_consumes(
    node_name: str,
    deps: tuple[str, ...],
    consumes: Mapping[str, ConsumeFunction] | None,
    *,
    items_from: tuple[str, str] | None = None,
    carry_from: tuple[str, str] | None = None,
) -> dict[str, ConsumeFunction]:
    """Validate edge projections against one node's declared dependency roles."""
    if consumes is None:
        return {}
    if not isinstance(consumes, Mapping):
        raise ValueError(f"Node {node_name!r} consumes must be a mapping")
    projections: dict[str, ConsumeFunction] = {}
    for dependency, projector in consumes.items():
        if dependency not in deps:
            raise ValueError(
                f"Node {node_name!r} consumes dependency {dependency!r} "
                "is not a declared dependency"
            )
        if items_from is not None and dependency == items_from[0]:
            raise ValueError(
                f"Node {node_name!r} consumes dependency {dependency!r} "
                "cannot reference its items_from source"
            )
        if carry_from is not None and dependency == carry_from[0]:
            raise ValueError(
                f"Node {node_name!r} consumes dependency {dependency!r} "
                "cannot reference its carry_from source"
            )
        if not callable(projector):
            raise ValueError(
                f"Node {node_name!r} consumes dependency {dependency!r} must be callable"
            )
        projections[dependency] = projector
    return projections


def validate_segment(value: Any, kind: str) -> str:
    """Require a name that cannot be confused with qualification, items, or paths."""
    if (
        not isinstance(value, str)
        or not value
        or any(separator in value for separator in (".", "@", "/", "\\"))
    ):
        raise ValueError(
            f"{kind} must be a single non-empty segment without '.', '@', '/', or '\\'"
        )
    return value
