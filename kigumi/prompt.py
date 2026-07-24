"""Deterministic prompt assembly primitives."""

from __future__ import annotations

import json
import re
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType, UnionType
from typing import Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel

from .artifacts import canonical_json, sha

TITLE_DELIMITER = "## {title}\n\n"
WORDING_CLIPPED = "(已截断：原文 {original_chars} 字，保留 {kept_chars} 字)"
WORDING_JSON_ONLY = "只输出一个 JSON 对象；不要输出解释、前后缀或代码围栏。"
WORDING_REPAIR_ROUND = "这是第 {round} 轮修复。"
WORDING_REPAIR_STUCK = "检测到输出与上次完全相同；请逐项修正错误，不得原样重交。"
WORDING_REPAIR_PREAMBLE = "上次输出未通过校验。下面是错误："
WORDING_REPAIR_ECHO = "你上一轮的输出如下："

_SLOT_PATTERN = re.compile(r"{{([a-z_][a-z0-9_]*)}}")
_NAME_PATTERN = re.compile(r"[a-z_][a-z0-9_]*")
_SENTENCE_BOUNDARY = re.compile(r"[。！？.!?]")
PROMPT_RESOLUTION_SCHEMA = 1


class KigumiPromptWarning(UserWarning):
    """Warning emitted when JSON object key order looks like ordered data."""


class TemplateSlotError(ValueError):
    """Raised when a declarative template's slots do not match supplied values."""


class PromptDefinitionError(ValueError):
    """Raised when a layered Prompt declaration is unsafe or internally inconsistent."""


class PromptResolutionError(ValueError):
    """Raised when runtime facts cannot deterministically resolve a Prompt declaration."""


def _validate_prompt_name(value: Any, kind: str) -> str:
    if not isinstance(value, str) or _NAME_PATTERN.fullmatch(value) is None:
        raise PromptDefinitionError(f"{kind} must match [a-z_][a-z0-9_]*, got {value!r}")
    return value


def _validate_binding_name(value: Any, kind: str) -> str:
    if not isinstance(value, str) or not value:
        raise PromptDefinitionError(f"{kind} must be a non-empty string, got {value!r}")
    return value


def _validate_prompt_path(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\x00" in value
        or "\\" in value
        or value.startswith("/")
    ):
        raise PromptDefinitionError(f"Unsafe PromptRef path: {value!r}")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise PromptDefinitionError(f"Unsafe PromptRef path: {value!r}")
    if parts[-1].endswith(".md"):
        raise PromptDefinitionError("PromptRef uses an extension-free name; '.md' is added")
    return "/".join(parts)


def _validate_path(value: Any) -> tuple[str | int, ...]:
    if not isinstance(value, tuple):
        raise PromptDefinitionError("selector/material path must be a tuple[str | int, ...]")
    for part in value:
        if isinstance(part, bool) or not isinstance(part, str | int):
            raise PromptDefinitionError(
                "selector/material path must contain only str or int segments"
            )
        if isinstance(part, str) and not part:
            raise PromptDefinitionError("selector/material string path segments must be non-empty")
    return value


@dataclass(frozen=True)
class PromptRef:
    """A safe extension-free reference to one UTF-8 ``prompts/**/*.md`` file."""

    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _validate_prompt_path(self.name))

    def canonical(self) -> dict[str, str]:
        return {"kind": "prompt", "name": self.name}


@dataclass(frozen=True)
class InputRef:
    """Read one node function input, then follow a strict tuple path."""

    input: str
    path: tuple[str | int, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "input",
            _validate_binding_name(self.input, "InputRef input"),
        )
        object.__setattr__(self, "path", _validate_path(self.path))

    def canonical(self) -> dict[str, Any]:
        return {"kind": "input", "name": self.input, "path": list(self.path)}


@dataclass(frozen=True)
class ParamRef:
    """Read one declared node parameter, then follow a strict tuple path."""

    param: str
    path: tuple[str | int, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "param",
            _validate_binding_name(self.param, "ParamRef param"),
        )
        object.__setattr__(self, "path", _validate_path(self.path))

    def canonical(self) -> dict[str, Any]:
        return {"kind": "param", "name": self.param, "path": list(self.path)}


@dataclass(frozen=True)
class ItemRef:
    """Read the current map/scan item, then follow a strict tuple path."""

    path: tuple[str | int, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _validate_path(self.path))

    def canonical(self) -> dict[str, Any]:
        return {"kind": "item", "path": list(self.path)}


@dataclass(frozen=True)
class CarryRef:
    """Read the current scan carry, then follow a strict tuple path."""

    path: tuple[str | int, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _validate_path(self.path))

    def canonical(self) -> dict[str, Any]:
        return {"kind": "carry", "path": list(self.path)}


PromptValueRef = InputRef | ParamRef | ItemRef | CarryRef


@dataclass(frozen=True)
class PromptAxis:
    """Select exactly one Prompt fragment from a declared finite variant universe."""

    name: str
    selector: PromptValueRef
    variants: Mapping[str, PromptRef]

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _validate_prompt_name(self.name, "PromptAxis name"))
        if not isinstance(self.selector, (InputRef, ParamRef, ItemRef, CarryRef)):
            raise PromptDefinitionError(
                "PromptAxis selector must be InputRef, ParamRef, ItemRef, or CarryRef"
            )
        if not isinstance(self.variants, Mapping) or not self.variants:
            raise PromptDefinitionError("PromptAxis variants must be a non-empty mapping")
        checked: dict[str, PromptRef] = {}
        for key, reference in self.variants.items():
            if not isinstance(key, str) or not key:
                raise PromptDefinitionError("PromptAxis variant keys must be non-empty strings")
            if not isinstance(reference, PromptRef):
                raise PromptDefinitionError("PromptAxis variants must map strings to PromptRef")
            checked[key] = reference
        object.__setattr__(
            self,
            "variants",
            MappingProxyType(dict(sorted(checked.items()))),
        )

    def canonical(self) -> dict[str, Any]:
        return {
            "kind": "axis",
            "name": self.name,
            "selector": self.selector.canonical(),
            "variants": {key: reference.canonical() for key, reference in self.variants.items()},
        }


@dataclass(frozen=True)
class PromptLayer:
    """Bind one base-template slot to a fixed fragment or a selected axis fragment."""

    slot: str
    source: PromptRef | PromptAxis

    def __post_init__(self) -> None:
        object.__setattr__(self, "slot", _validate_prompt_name(self.slot, "PromptLayer slot"))
        if not isinstance(self.source, (PromptRef, PromptAxis)):
            raise PromptDefinitionError("PromptLayer source must be PromptRef or PromptAxis")

    def canonical(self) -> dict[str, Any]:
        return {"slot": self.slot, "source": self.source.canonical()}


@dataclass(frozen=True)
class PromptMaterial:
    """Bind one base-template slot to deterministically fenced runtime material."""

    slot: str
    source: PromptValueRef
    title: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "slot", _validate_prompt_name(self.slot, "PromptMaterial slot"))
        if not isinstance(self.source, (InputRef, ParamRef, ItemRef, CarryRef)):
            raise PromptDefinitionError(
                "PromptMaterial source must be InputRef, ParamRef, ItemRef, or CarryRef"
            )
        if self.title is not None and (not isinstance(self.title, str) or not self.title.strip()):
            raise PromptDefinitionError("PromptMaterial title must be non-empty when supplied")

    def canonical(self) -> dict[str, Any]:
        return {
            "slot": self.slot,
            "source": self.source.canonical(),
            "title": self.title,
        }


@dataclass(frozen=True)
class PromptSpec:
    """A fully declarative single-text Prompt composition."""

    name: str
    base: PromptRef
    layers: tuple[PromptLayer, ...] = ()
    materials: tuple[PromptMaterial, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _validate_prompt_name(self.name, "PromptSpec name"))
        if not isinstance(self.base, PromptRef):
            raise PromptDefinitionError("PromptSpec base must be PromptRef")
        if not isinstance(self.layers, tuple) or not all(
            isinstance(layer, PromptLayer) for layer in self.layers
        ):
            raise PromptDefinitionError("PromptSpec layers must be a tuple of PromptLayer")
        if not isinstance(self.materials, tuple) or not all(
            isinstance(material, PromptMaterial) for material in self.materials
        ):
            raise PromptDefinitionError("PromptSpec materials must be a tuple of PromptMaterial")
        slots = [layer.slot for layer in self.layers] + [
            material.slot for material in self.materials
        ]
        if len(set(slots)) != len(slots):
            raise PromptDefinitionError(f"PromptSpec {self.name!r} contains duplicate slots")
        axes = [layer.source.name for layer in self.layers if isinstance(layer.source, PromptAxis)]
        if len(set(axes)) != len(axes):
            raise PromptDefinitionError(f"PromptSpec {self.name!r} contains duplicate axes")

    def canonical(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "base": self.base.canonical(),
            "layers": [layer.canonical() for layer in self.layers],
            "materials": [material.canonical() for material in self.materials],
        }

    @property
    def structure_digest(self) -> str:
        """Digest declarations and bindings, but no Prompt file contents."""
        return sha(self.canonical())

    def references(self) -> tuple[PromptRef, ...]:
        references = [self.base]
        for layer in self.layers:
            if isinstance(layer.source, PromptRef):
                references.append(layer.source)
            else:
                references.extend(layer.source.variants.values())
        return tuple(dict.fromkeys(references))


def validate_prompt_specs(
    prompt_specs: Any,
    *,
    legacy_prompts: tuple[str, ...] = (),
    dynamic_kind: Literal["node", "map", "scan"] = "node",
) -> tuple[PromptSpec, ...]:
    """Freeze one node's declarations and enforce context-source restrictions."""
    if not isinstance(prompt_specs, tuple):
        try:
            prompt_specs = tuple(prompt_specs)
        except TypeError as error:
            raise PromptDefinitionError("prompt_specs must be an iterable of PromptSpec") from error
    if not all(isinstance(spec, PromptSpec) for spec in prompt_specs):
        raise PromptDefinitionError("prompt_specs must contain only PromptSpec")
    names = [spec.name for spec in prompt_specs]
    if len(set(names)) != len(names):
        raise PromptDefinitionError("PromptSpec names must be unique within one node")
    conflicts = sorted(set(names) & set(legacy_prompts))
    if conflicts:
        raise PromptDefinitionError(
            "legacy prompts and PromptSpec names conflict: " + ", ".join(conflicts)
        )
    for spec in prompt_specs:
        sources: list[PromptValueRef] = [material.source for material in spec.materials]
        sources.extend(
            layer.source.selector for layer in spec.layers if isinstance(layer.source, PromptAxis)
        )
        if dynamic_kind == "node" and any(isinstance(source, ItemRef) for source in sources):
            raise PromptDefinitionError(f"PromptSpec {spec.name!r} uses ItemRef outside map/scan")
        if dynamic_kind != "scan" and any(isinstance(source, CarryRef) for source in sources):
            raise PromptDefinitionError(f"PromptSpec {spec.name!r} uses CarryRef outside scan")
    return prompt_specs


def validate_prompt_bindings(
    prompt_specs: tuple[PromptSpec, ...],
    *,
    inputs: set[str],
    params: set[str],
) -> None:
    """Validate top-level InputRef/ParamRef names against a node's function boundary."""
    for spec in prompt_specs:
        sources: list[PromptValueRef] = [material.source for material in spec.materials]
        sources.extend(
            layer.source.selector for layer in spec.layers if isinstance(layer.source, PromptAxis)
        )
        for source in sources:
            if isinstance(source, InputRef) and source.input not in inputs:
                raise PromptDefinitionError(
                    f"PromptSpec {spec.name!r} InputRef {source.input!r} "
                    "is not an actual node function input"
                )
            if isinstance(source, ParamRef) and source.param not in params:
                raise PromptDefinitionError(
                    f"PromptSpec {spec.name!r} ParamRef {source.param!r} "
                    "is not a declared node parameter"
                )


@dataclass(frozen=True)
class _CatalogEntry:
    name: str
    text: str
    digest: str
    bytes: int

    def descriptor(self) -> dict[str, Any]:
        return {"ref": self.name, "sha256": self.digest, "bytes": self.bytes}


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_value(child) for key, child in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze_value(child) for child in value)
    return value


def _thaw_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_value(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [_thaw_value(child) for child in value]
    return value


def validate_prompt_resolution_record(value: Any) -> None:
    """Validate one persisted schema-1 resolution without reconstructing Prompt text."""
    if not isinstance(value, Mapping) or value.get("prompt_resolution_schema") != 1:
        raise PromptResolutionError("persisted Prompt resolution has invalid schema")
    keys = (
        "prompt_resolution_schema",
        "spec",
        "structure_digest",
        "base",
        "layers",
        "axes",
        "materials",
        "rendered",
    )
    body = {key: _thaw_value(value.get(key)) for key in keys}
    digest = value.get("resolution_digest")
    if not isinstance(digest, str) or digest != sha(body):
        raise PromptResolutionError("persisted Prompt resolution failed digest validation")
    if value.get("base_resolution_digest", digest) != digest:
        raise PromptResolutionError("persisted Prompt resolution has a mismatched base resolution")


@dataclass(frozen=True)
class PromptResolution:
    """Content-free immutable provenance for one rendered Prompt."""

    spec_name: str
    structure_digest: str
    base: Mapping[str, Any]
    layers: tuple[Mapping[str, Any], ...]
    axes: tuple[Mapping[str, Any], ...]
    materials: tuple[Mapping[str, Any], ...]
    rendered_sha256: str
    rendered_bytes: int
    schema: int = PROMPT_RESOLUTION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != PROMPT_RESOLUTION_SCHEMA:
            raise PromptResolutionError("unsupported prompt resolution schema")
        object.__setattr__(self, "base", _freeze_value(dict(self.base)))
        object.__setattr__(
            self,
            "layers",
            tuple(_freeze_value(dict(layer)) for layer in self.layers),
        )
        object.__setattr__(
            self,
            "axes",
            tuple(_freeze_value(dict(axis)) for axis in self.axes),
        )
        object.__setattr__(
            self,
            "materials",
            tuple(_freeze_value(dict(material)) for material in self.materials),
        )

    def _body(self) -> dict[str, Any]:
        return {
            "prompt_resolution_schema": self.schema,
            "spec": self.spec_name,
            "structure_digest": self.structure_digest,
            "base": _thaw_value(self.base),
            "layers": [_thaw_value(layer) for layer in self.layers],
            "axes": [_thaw_value(axis) for axis in self.axes],
            "materials": [_thaw_value(material) for material in self.materials],
            "rendered": {
                "sha256": self.rendered_sha256,
                "bytes": self.rendered_bytes,
            },
        }

    @property
    def digest(self) -> str:
        return sha(self._body())

    def canonical(self) -> dict[str, Any]:
        return {**self._body(), "resolution_digest": self.digest}


class ResolvedPrompt(str):
    """A ``str`` carrying Prompt resolution lineage until normal string operations erase it."""

    def __new__(cls, value: str, resolution: PromptResolution) -> ResolvedPrompt:
        if not isinstance(value, str):
            raise TypeError("ResolvedPrompt value must be a string")
        if not isinstance(resolution, PromptResolution):
            raise TypeError("ResolvedPrompt resolution must be PromptResolution")
        instance = super().__new__(cls, value)
        object.__setattr__(instance, "resolution", resolution)
        return instance

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("ResolvedPrompt is immutable")


class PromptCatalogSnapshot:
    """Immutable, run-scoped bytes and hashes for every declared Prompt file."""

    def __init__(self, root: Path, entries: Mapping[str, _CatalogEntry]) -> None:
        self.root = root
        self._entries = MappingProxyType(dict(entries))

    @classmethod
    def capture(
        cls,
        root: Path,
        *,
        prompt_specs: tuple[PromptSpec, ...] = (),
        legacy_prompts: tuple[str, ...] = (),
    ) -> PromptCatalogSnapshot:
        resolved_root = root.resolve()
        names = set(legacy_prompts)
        for spec in prompt_specs:
            names.update(reference.name for reference in spec.references())
        entries: dict[str, _CatalogEntry] = {}
        for name in sorted(names):
            checked = _validate_prompt_path(name)
            candidate = root / f"{checked}.md"
            try:
                resolved = candidate.resolve(strict=True)
                resolved.relative_to(resolved_root)
            except (FileNotFoundError, OSError, ValueError) as error:
                raise PromptDefinitionError(
                    f"PromptRef {checked!r} must resolve to a .md file under {resolved_root}"
                ) from error
            if candidate.is_symlink() or not resolved.is_file() or resolved.suffix != ".md":
                raise PromptDefinitionError(f"PromptRef {checked!r} must be a regular .md file")
            try:
                raw = resolved.read_bytes()
                text = raw.decode("utf-8")
            except (OSError, UnicodeDecodeError) as error:
                raise PromptDefinitionError(
                    f"PromptRef {checked!r} must be readable UTF-8"
                ) from error
            entries[checked] = _CatalogEntry(checked, text, sha(text), len(raw))
        snapshot = cls(resolved_root, entries)
        for spec in prompt_specs:
            snapshot.validate(spec)
        return snapshot

    def text(self, name: str) -> str:
        try:
            return self._entries[name].text
        except KeyError as error:
            raise PromptDefinitionError(f"Prompt {name!r} is not in this run snapshot") from error

    def validate(self, spec: PromptSpec) -> None:
        base = self._entries[spec.base.name]
        declared_slots = {
            *(layer.slot for layer in spec.layers),
            *(material.slot for material in spec.materials),
        }
        actual_slots = set(slot_names(base.text))
        if actual_slots != declared_slots:
            missing = sorted(actual_slots - declared_slots)
            extra = sorted(declared_slots - actual_slots)
            details: list[str] = []
            if missing:
                details.append("undeclared base slots: " + ", ".join(missing))
            if extra:
                details.append("unused declared slots: " + ", ".join(extra))
            raise PromptDefinitionError(
                f"PromptSpec {spec.name!r} slot mismatch ({'; '.join(details)})"
            )
        for reference in spec.references()[1:]:
            fragment = self._entries[reference.name]
            nested = slot_names(fragment.text)
            if nested:
                raise PromptDefinitionError(
                    f"Prompt fragment {reference.name!r} may not contain slots: "
                    + ", ".join(nested)
                )

    def declaration(self, spec: PromptSpec) -> dict[str, Any]:
        """Full candidate universe used only by graph/run identity."""
        return {
            "spec": spec.canonical(),
            "structure_digest": spec.structure_digest,
            "references": {
                reference.name: self._entries[reference.name].descriptor()
                for reference in spec.references()
            },
        }

    def resolve(
        self,
        spec: PromptSpec,
        *,
        inputs: Mapping[str, Any],
        params: Mapping[str, Any],
        item: Any = None,
        carry: Any = None,
        has_item: bool = False,
        has_carry: bool = False,
    ) -> ResolvedPrompt:
        slots: dict[str, str] = {}
        layers: list[dict[str, Any]] = []
        axes: list[dict[str, Any]] = []
        materials: list[dict[str, Any]] = []
        for layer in spec.layers:
            source = layer.source
            if isinstance(source, PromptRef):
                reference = source
                axis_record = None
            else:
                value = _resolve_value(
                    source.selector,
                    inputs=inputs,
                    params=params,
                    item=item,
                    carry=carry,
                    has_item=has_item,
                    has_carry=has_carry,
                    context=f"axis {source.name!r}",
                )
                if not isinstance(value, str):
                    raise PromptResolutionError(
                        f"Prompt axis {source.name!r} selector must resolve to a string"
                    )
                try:
                    reference = source.variants[value]
                except KeyError as error:
                    raise PromptResolutionError(
                        f"Prompt axis {source.name!r} has unknown variant {value!r}"
                    ) from error
                axis_record = {
                    "name": source.name,
                    "selector": source.selector.canonical(),
                    "selected": value,
                    "ref": reference.name,
                    "sha256": self._entries[reference.name].digest,
                }
                axes.append(axis_record)
            entry = self._entries[reference.name]
            slots[layer.slot] = entry.text
            layer_record = {
                "slot": layer.slot,
                "ref": reference.name,
                "sha256": entry.digest,
                "bytes": entry.bytes,
            }
            if axis_record is not None:
                layer_record["axis"] = source.name
                layer_record["selected"] = axis_record["selected"]
            layers.append(layer_record)
        for material in spec.materials:
            value = _resolve_value(
                material.source,
                inputs=inputs,
                params=params,
                item=item,
                carry=carry,
                has_item=has_item,
                has_carry=has_carry,
                context=f"material {material.slot!r}",
            )
            rendered_material = inject(value, title=material.title)
            encoded = rendered_material.encode("utf-8")
            slots[material.slot] = rendered_material
            materials.append(
                {
                    "slot": material.slot,
                    "source": material.source.canonical(),
                    "title": material.title,
                    "sha256": sha(rendered_material),
                    "bytes": len(encoded),
                }
            )
        base = self._entries[spec.base.name]
        rendered = render_template(base.text, slots)
        resolution = PromptResolution(
            spec_name=spec.name,
            structure_digest=spec.structure_digest,
            base=base.descriptor(),
            layers=tuple(layers),
            axes=tuple(axes),
            materials=tuple(materials),
            rendered_sha256=sha(rendered),
            rendered_bytes=len(rendered.encode("utf-8")),
        )
        return ResolvedPrompt(rendered, resolution)


def _resolve_value(
    source: PromptValueRef,
    *,
    inputs: Mapping[str, Any],
    params: Mapping[str, Any],
    item: Any,
    carry: Any,
    has_item: bool,
    has_carry: bool,
    context: str,
) -> Any:
    if isinstance(source, InputRef):
        if source.input not in inputs:
            raise PromptResolutionError(
                f"{context} input {source.input!r} is missing from projected node inputs"
            )
        value = inputs[source.input]
        path = source.path
    elif isinstance(source, ParamRef):
        if source.param not in params:
            raise PromptResolutionError(
                f"{context} param {source.param!r} is missing from declared params"
            )
        value = params[source.param]
        path = source.path
    elif isinstance(source, ItemRef):
        if not has_item:
            raise PromptResolutionError(f"{context} ItemRef is unavailable")
        value = item
        path = source.path
    else:
        if not has_carry:
            raise PromptResolutionError(f"{context} CarryRef is unavailable")
        value = carry
        path = source.path
    traversed: list[str | int] = []
    for part in path:
        try:
            if isinstance(part, int):
                if not isinstance(value, (list, tuple)):
                    raise TypeError
                value = value[part]
            else:
                if not isinstance(value, Mapping):
                    raise TypeError
                value = value[part]
        except (IndexError, KeyError, TypeError) as error:
            traversed.append(part)
            raise PromptResolutionError(
                f"{context} path {tuple(traversed)!r} is missing or has the wrong type"
            ) from error
        traversed.append(part)
    return value


@dataclass(frozen=True)
class Clipped:
    """A clip result with the sidecar event needed to disclose truncation."""

    text: str
    clipped: bool
    original_chars: int
    kept_chars: int
    event: dict[str, int | str] | None


def inject(obj: Any, *, title: str | None = None) -> str:
    """Render text or JSON-serializable material in a deterministic fenced block."""
    _warn_numeric_dict_keys(obj)
    if isinstance(obj, str):
        body, lang = obj, ""
    else:
        body, lang = canonical_json(obj), "json"
    fence = _fence_for(body)
    fenced = f"{fence}{lang}\n{body}\n{fence}\n"
    return f"{TITLE_DELIMITER.format(title=title)}{fenced}" if title is not None else fenced


def load_template(path: Path) -> str:
    """Load an explicitly supplied UTF-8 template file."""
    return path.read_text(encoding="utf-8")


def slot_names(text: str) -> list[str]:
    """Return a template's ``{{slot}}`` names in first-appearance order, deduplicated."""
    return list(dict.fromkeys(_SLOT_PATTERN.findall(text)))


def render_template(text: str, slots: dict[str, str]) -> str:
    """Render a declarative ``{{slot}}`` template with an exact slot contract."""
    required = set(slot_names(text))
    supplied = set(slots)
    missing = sorted(required - supplied)
    extra = sorted(supplied - required)
    if missing or extra:
        parts: list[str] = []
        if missing:
            parts.append(f"missing: {', '.join(missing)}")
        if extra:
            parts.append(f"extra: {', '.join(extra)}")
        raise TemplateSlotError(f"Template slots mismatch: {'; '.join(parts)}")
    return _SLOT_PATTERN.sub(lambda match: slots[match.group(1)], text)


def section(title: str, value: str | None) -> str:
    """Render a titled section only when its body has content.

    Output always ends with a newline so sections compose by plain concatenation.
    """
    if not value:
        return ""
    rendered = f"{TITLE_DELIMITER.format(title=title)}{value}"
    return rendered if rendered.endswith("\n") else f"{rendered}\n"


def schema_format_section(model_cls: type[BaseModel], *, with_example: bool = True) -> str:
    """Describe a Pydantic model and optionally include a recursive JSON skeleton."""
    field_lines = ["字段："]
    for name, field in model_cls.model_fields.items():
        required = "必填" if field.is_required() else "可选"
        description = field.description or "无描述"
        field_lines.append(
            f"- `{name}`：`{_type_label(field.annotation)}`；{required}；{description}"
        )

    body = "\n".join(field_lines)
    if with_example:
        example = {
            name: _example_value(field.annotation) for name, field in model_cls.model_fields.items()
        }
        example_json = json.dumps(example, ensure_ascii=False, indent=2)
        body = f"{body}\n\n示例：\n```json\n{example_json}\n```\n"
    return section("输出格式", body + "\n" + WORDING_JSON_ONLY)


def clip(text: str, limit: int, *, boundary: Literal["line", "sentence"] = "line") -> Clipped:
    """Clip only at an explicit safe boundary and disclose every truncation."""
    if limit < 0:
        raise ValueError("limit must be non-negative")
    if boundary not in {"line", "sentence"}:
        raise ValueError("boundary must be 'line' or 'sentence'")
    original_chars = len(text)
    if original_chars <= limit:
        return Clipped(text, False, original_chars, original_chars, None)

    prefix = text[:limit]
    # 找不到安全边界时硬切到 limit:宁可切破一行,不可把材料整段清空;标注照常披露。
    if boundary == "line":
        last_newline = prefix.rfind("\n")
        kept = prefix[: last_newline + 1] if last_newline >= 0 else prefix
    else:
        matches = list(_SENTENCE_BOUNDARY.finditer(prefix))
        kept = prefix[: matches[-1].end()] if matches else prefix
    kept_chars = len(kept)
    annotation = WORDING_CLIPPED.format(
        original_chars=original_chars,
        kept_chars=kept_chars,
    )
    separator = "" if not kept or kept.endswith("\n") else "\n"
    event: dict[str, int | str] = {
        "from": original_chars,
        "to": kept_chars,
        "boundary": boundary,
    }
    return Clipped(f"{kept}{separator}{annotation}", True, original_chars, kept_chars, event)


def render_items(items: list[Any], *, format: Literal["json", "bullets"] = "json") -> str:
    """Render a list deterministically as JSON material or indented bullet points."""
    if format == "json":
        return inject(items)
    if format == "bullets":
        return "\n".join(_bullet_item(item) for item in items)
    raise ValueError("format must be 'json' or 'bullets'")


def _fence_for(body: str) -> str:
    # 围栏必须长于材料内最长的反引号连串,否则材料自带 ``` 时边界破裂。
    longest = max((len(run.group(0)) for run in re.finditer(r"`+", body)), default=0)
    return "`" * max(3, longest + 1)


def _warn_numeric_dict_keys(obj: Any) -> None:
    if isinstance(obj, dict):
        keys = list(obj)
        if keys and all(isinstance(key, str) and key.isdigit() for key in keys):
            warnings.warn(
                "有序数据必须用 list——sort_keys 按字典序会把 1,10,11,2 排乱",
                KigumiPromptWarning,
                stacklevel=3,
            )
        for value in obj.values():
            _warn_numeric_dict_keys(value)
    elif isinstance(obj, (list, tuple)):
        for value in obj:
            _warn_numeric_dict_keys(value)


def _bullet_item(item: Any) -> str:
    return "- " + str(item).replace("\n", "\n  ")


def _type_label(annotation: Any) -> str:
    origin = get_origin(annotation)
    if annotation is Any:
        return "Any"
    if annotation is type(None):
        return "None"
    if origin is list:
        arguments = get_args(annotation)
        return f"list[{_type_label(arguments[0]) if arguments else 'Any'}]"
    if origin is dict:
        arguments = get_args(annotation)
        key = _type_label(arguments[0]) if arguments else "Any"
        value = _type_label(arguments[1]) if len(arguments) > 1 else "Any"
        return f"dict[{key}, {value}]"
    if origin in {Union, UnionType}:
        return " | ".join(_type_label(argument) for argument in get_args(annotation))
    if origin is Literal:
        return " | ".join(repr(argument) for argument in get_args(annotation))
    if isinstance(annotation, type):
        return annotation.__name__
    return str(annotation).replace("typing.", "")


def _example_value(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if annotation is Any:
        return "<value>"
    if origin is list:
        arguments = get_args(annotation)
        return [_example_value(arguments[0] if arguments else Any)]
    if origin is dict:
        return {}
    if origin in {Union, UnionType}:
        non_none = [argument for argument in get_args(annotation) if argument is not type(None)]
        return _example_value(non_none[0] if non_none else type(None))
    if origin is Literal:
        arguments = get_args(annotation)
        return arguments[0] if arguments else "<literal>"
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return {
            name: _example_value(field.annotation)
            for name, field in annotation.model_fields.items()
        }
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return next(iter(annotation)).value
    if annotation is str:
        return "<string>"
    if annotation is int:
        return 0
    if annotation is float:
        return 0.0
    if annotation is bool:
        return False
    if annotation is type(None):
        return None
    return "<value>"
