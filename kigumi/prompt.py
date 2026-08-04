"""Deterministic prompt assembly primitives."""

from __future__ import annotations

import errno
import json
import mimetypes
import os
import re
import stat
import warnings
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType, UnionType
from typing import Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel

from ._safe_io import SecureDirectory
from .artifacts import canonical_json, sha
from .config import _safe_configured_path

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
_ORIGINAL_OS_OPEN = os.open


def _read_regular_file_no_follow(root: Path, relative: str) -> bytes:
    """Read one root-relative regular file through bound no-follow descriptors.

    Prompt files are part of a run's input identity.  A path check followed by
    ``Path.read_bytes`` leaves a replacement window in which a regular file can
    become a symlink, FIFO, or another special file.  Open every directory and
    the final file relative to descriptors so a parent replacement cannot
    redirect the read, and use a non-blocking no-follow final descriptor so a
    raced FIFO fails closed instead of blocking.
    """
    supported = getattr(os, "supports_dir_fd", ())
    if (
        _ORIGINAL_OS_OPEN not in supported
        or not hasattr(os, "O_DIRECTORY")
        or not hasattr(os, "O_NOFOLLOW")
    ):
        raise OSError(errno.ENOTSUP, "Prompt snapshot requires descriptor-relative no-follow I/O")

    absolute_root = root.absolute()
    parts = tuple(part for part in relative.split("/") if part)
    if not parts or any(part in {".", ".."} for part in parts):
        raise ValueError(f"Unsafe prompt snapshot path: {relative!r}")

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    file_flags = (
        os.O_RDONLY
        | os.O_NOFOLLOW
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_BINARY", 0)
    )
    descriptors: list[int] = []
    file_descriptor = -1
    try:
        current = os.open(absolute_root.anchor, directory_flags)
        descriptors.append(current)
        for component in absolute_root.parts[1:]:
            if component in {"", "."}:
                continue
            current = os.open(component, directory_flags, dir_fd=current)
            descriptors.append(current)
        for component in parts[:-1]:
            current = os.open(component, directory_flags, dir_fd=current)
            descriptors.append(current)

        file_descriptor = os.open(parts[-1], file_flags, dir_fd=current)
        before = os.fstat(file_descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise OSError(
                getattr(errno, "EFTYPE", errno.EINVAL),
                f"Prompt snapshot is not a regular file: {relative}",
            )
        with os.fdopen(file_descriptor, "rb", closefd=True) as handle:
            file_descriptor = -1
            data = handle.read()
            after = os.fstat(handle.fileno())
        if len(data) != before.st_size or (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise OSError(errno.EAGAIN, f"Prompt snapshot changed while reading: {relative}")
        return data
    finally:
        if file_descriptor >= 0:
            with suppress(OSError):
                os.close(file_descriptor)
        for descriptor in reversed(descriptors):
            with suppress(OSError):
                os.close(descriptor)


class KigumiPromptWarning(UserWarning):
    """Warning emitted when JSON object key order looks like ordered data."""


class TemplateSlotError(ValueError):
    """Raised when a declarative template's slots do not match supplied values."""


class PromptDefinitionError(ValueError):
    """Raised when a layered Prompt declaration is unsafe or internally inconsistent."""


class PromptResolutionError(ValueError):
    """Raised when runtime facts cannot deterministically resolve a Prompt declaration."""


@dataclass(frozen=True)
class Attachment:
    """File attachment with content-addressed identity."""

    path: str
    content_hash: str
    mime_type: str
    size_bytes: int

    def __post_init__(self) -> None:
        if isinstance(self.path, Path):
            object.__setattr__(self, "path", str(self.path))
        if not isinstance(self.path, str) or not self.path:
            raise ValueError("Attachment path must be a non-empty string")
        if not isinstance(self.content_hash, str) or not self.content_hash:
            raise ValueError("Attachment content_hash must be a non-empty string")
        if not isinstance(self.mime_type, str) or not self.mime_type:
            raise ValueError("Attachment mime_type must be a non-empty string")
        if isinstance(self.size_bytes, bool) or not isinstance(self.size_bytes, int):
            raise ValueError("Attachment size_bytes must be an integer")
        if self.size_bytes < 0:
            raise ValueError("Attachment size_bytes must be non-negative")

    def canonical(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "content_hash": self.content_hash,
            "mime_type": self.mime_type,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True)
class Message:
    """Structured message with typed parts."""

    role: str
    parts: tuple[str | Mapping[str, Any], ...]

    def __post_init__(self) -> None:
        if not isinstance(self.role, str) or not self.role:
            raise ValueError("Message role must be a non-empty string")
        if not isinstance(self.parts, (list, tuple)):
            raise ValueError("Message parts must be a list or tuple")
        checked: list[str | Mapping[str, Any]] = []
        for part in self.parts:
            if not isinstance(part, (str, Mapping)):
                raise ValueError("Message parts must contain only strings or dictionaries")
            checked.append(_freeze_value(part))
        object.__setattr__(self, "parts", tuple(checked))

    def canonical(self) -> dict[str, Any]:
        return {"role": self.role, "parts": [_thaw_value(part) for part in self.parts]}


@dataclass(frozen=True)
class ResponseSpec:
    """Response format specification."""

    schema_sha256: str | None = None
    format: str = "text"

    def __post_init__(self) -> None:
        if self.schema_sha256 is not None and (
            not isinstance(self.schema_sha256, str) or not self.schema_sha256
        ):
            raise ValueError("ResponseSpec schema_sha256 must be a non-empty string or None")
        if self.format not in {"text", "json", "structured"}:
            raise ValueError("ResponseSpec format must be 'text', 'json', or 'structured'")

    def canonical(self) -> dict[str, Any]:
        return {"schema_sha256": self.schema_sha256, "format": self.format}


@dataclass(frozen=True)
class PreflightPolicy:
    """Limits checked before a request can consult cache or reach a provider."""

    max_tokens: int = 200_000
    max_attachments: int = 50
    max_attachment_bytes: int = 100 * 1024 * 1024

    def __post_init__(self) -> None:
        for name in ("max_tokens", "max_attachments", "max_attachment_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")

    @property
    def max_total_bytes(self) -> int:
        """Compatibility alias for the total attachment byte limit."""
        return self.max_attachment_bytes


_DEFAULT_PREFLIGHT_POLICY = PreflightPolicy()


@dataclass(frozen=True)
class PreflightViolation:
    """Single preflight check failure."""

    check: str
    limit: int
    actual: int
    message: str


@dataclass(frozen=True)
class PreflightReport:
    """Result of input validation."""

    violations: list[PreflightViolation]
    estimated_tokens: int
    total_bytes: int

    def is_valid(self) -> bool:
        return len(self.violations) == 0


class RequestTooLarge(ValueError):
    """Request exceeds input preflight limits."""

    def __init__(self, report: PreflightReport) -> None:
        if not isinstance(report, PreflightReport):
            raise TypeError("RequestTooLarge requires a PreflightReport")
        self.report = report
        super().__init__(f"Request too large: {report.violations}")


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
class FileRef:
    """Read file bytes supplied by the DAG from a path resolved out of runtime data."""

    path_from: PromptValueRef

    def __post_init__(self) -> None:
        if not isinstance(self.path_from, (InputRef, ParamRef, ItemRef, CarryRef)):
            raise PromptDefinitionError(
                "FileRef path_from must be InputRef, ParamRef, ItemRef, or CarryRef"
            )

    def canonical(self) -> dict[str, Any]:
        return {"kind": "file_ref", "path_from": self.path_from.canonical()}


PromptMaterialRef = PromptValueRef | FileRef


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
    source: PromptMaterialRef
    title: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "slot", _validate_prompt_name(self.slot, "PromptMaterial slot"))
        if not isinstance(self.source, (InputRef, ParamRef, ItemRef, CarryRef, FileRef)):
            raise PromptDefinitionError(
                "PromptMaterial source must be InputRef, ParamRef, ItemRef, CarryRef, or FileRef"
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
    for spec in prompt_specs:
        sources = _prompt_value_refs(spec)
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
        sources = _prompt_value_refs(spec)
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


def _prompt_value_refs(spec: PromptSpec) -> list[PromptValueRef]:
    sources: list[PromptValueRef] = []
    for material in spec.materials:
        if isinstance(material.source, FileRef):
            sources.append(material.source.path_from)
        else:
            sources.append(material.source)
    sources.extend(
        layer.source.selector for layer in spec.layers if isinstance(layer.source, PromptAxis)
    )
    return sources


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


def _record_fields(
    value: Any,
    *,
    required: set[str],
    optional: set[str] | None = None,
    label: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    if any(not isinstance(field, str) for field in value):
        raise TypeError(f"{label} keys must be strings")
    fields = set(value)
    missing = required - fields
    extra = fields - required - (optional or set())
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append(f"missing: {', '.join(sorted(missing))}")
        if extra:
            details.append(f"extra: {', '.join(sorted(extra))}")
        raise TypeError(f"{label} has invalid fields ({'; '.join(details)})")
    return value


def _record_text(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"{label} must be a non-empty string")
    return value


def _record_bytes(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TypeError(f"{label} must be a non-negative integer")
    return value


def _record_path(value: Any, *, label: str) -> None:
    if not isinstance(value, list):
        raise TypeError(f"{label} must be a list")
    for part in value:
        if isinstance(part, bool) or not isinstance(part, str | int):
            raise TypeError(f"{label} has an invalid segment")
        if isinstance(part, str) and not part:
            raise TypeError(f"{label} has an empty string segment")


def _validate_persisted_value_ref(value: Any, *, label: str, allow_file_ref: bool) -> None:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    kind = _record_text(value.get("kind"), label=f"{label}.kind")
    if kind in {"input", "param"}:
        _record_fields(value, required={"kind", "name", "path"}, label=label)
        _record_text(value["name"], label=f"{label}.name")
        _record_path(value["path"], label=f"{label}.path")
    elif kind in {"item", "carry"}:
        _record_fields(value, required={"kind", "path"}, label=label)
        _record_path(value["path"], label=f"{label}.path")
    elif kind == "file_ref" and allow_file_ref:
        _record_fields(value, required={"kind", "path_from"}, label=label)
        _validate_persisted_value_ref(
            value["path_from"],
            label=f"{label}.path_from",
            allow_file_ref=False,
        )
    else:
        raise TypeError(f"{label}.kind is not an allowed Prompt value reference")


def _validate_persisted_descriptor(value: Any, *, label: str) -> None:
    _record_fields(value, required={"ref", "sha256", "bytes"}, label=label)
    _record_text(value["ref"], label=f"{label}.ref")
    _record_text(value["sha256"], label=f"{label}.sha256")
    _record_bytes(value["bytes"], label=f"{label}.bytes")


def _validate_persisted_layer(value: Any, *, index: int) -> None:
    label = f"layers[{index}]"
    record = _record_fields(
        value,
        required={"slot", "ref", "sha256", "bytes"},
        optional={"axis", "selected"},
        label=label,
    )
    _record_text(record["slot"], label=f"{label}.slot")
    _record_text(record["ref"], label=f"{label}.ref")
    _record_text(record["sha256"], label=f"{label}.sha256")
    _record_bytes(record["bytes"], label=f"{label}.bytes")
    axis_fields = {"axis", "selected"} & set(record)
    if axis_fields and axis_fields != {"axis", "selected"}:
        raise TypeError(f"{label} axis selection fields must be complete")
    if axis_fields:
        _record_text(record["axis"], label=f"{label}.axis")
        _record_text(record["selected"], label=f"{label}.selected")


def _validate_persisted_axis(value: Any, *, index: int) -> None:
    label = f"axes[{index}]"
    record = _record_fields(
        value,
        required={"name", "selector", "selected", "ref", "sha256"},
        label=label,
    )
    _record_text(record["name"], label=f"{label}.name")
    _validate_persisted_value_ref(
        record["selector"],
        label=f"{label}.selector",
        allow_file_ref=False,
    )
    _record_text(record["selected"], label=f"{label}.selected")
    _record_text(record["ref"], label=f"{label}.ref")
    _record_text(record["sha256"], label=f"{label}.sha256")


def _validate_persisted_file_manifest(value: Any, *, label: str) -> None:
    record = _record_fields(
        value,
        required={"path", "path_from", "sha256", "bytes"},
        label=label,
    )
    _record_text(record["path"], label=f"{label}.path")
    _validate_persisted_value_ref(
        record["path_from"],
        label=f"{label}.path_from",
        allow_file_ref=False,
    )
    _record_text(record["sha256"], label=f"{label}.sha256")
    _record_bytes(record["bytes"], label=f"{label}.bytes")


def _validate_persisted_material(value: Any, *, index: int) -> None:
    label = f"materials[{index}]"
    record = _record_fields(
        value,
        required={"slot", "source", "title", "sha256", "bytes"},
        optional={"file_ref"},
        label=label,
    )
    _record_text(record["slot"], label=f"{label}.slot")
    _validate_persisted_value_ref(record["source"], label=f"{label}.source", allow_file_ref=True)
    title = record["title"]
    if title is not None and (not isinstance(title, str) or not title.strip()):
        raise TypeError(f"{label}.title must be a non-empty string or null")
    _record_text(record["sha256"], label=f"{label}.sha256")
    _record_bytes(record["bytes"], label=f"{label}.bytes")
    if "file_ref" in record:
        _validate_persisted_file_manifest(record["file_ref"], label=f"{label}.file_ref")


def validate_prompt_resolution_record(value: Any) -> None:
    """Validate one persisted schema-1 resolution without reconstructing Prompt text."""
    if not isinstance(value, Mapping):
        raise PromptResolutionError("persisted Prompt resolution has invalid schema")
    try:
        schema = value["prompt_resolution_schema"]
    except (KeyError, TypeError) as error:
        raise PromptResolutionError("persisted Prompt resolution has invalid schema") from error
    if (
        isinstance(schema, bool)
        or not isinstance(schema, int)
        or schema != PROMPT_RESOLUTION_SCHEMA
    ):
        raise PromptResolutionError("persisted Prompt resolution has invalid schema")
    if any(not isinstance(field, str) for field in value):
        raise PromptResolutionError("persisted Prompt resolution has invalid schema fields")
    required_fields = {
        "prompt_resolution_schema",
        "spec",
        "structure_digest",
        "base",
        "layers",
        "axes",
        "materials",
        "rendered",
        "messages",
        "attachments",
        "response_spec",
        "resolution_digest",
    }
    lineage_fields = {"base_resolution_digest", "phase", "repair_round"}
    missing_fields = required_fields - set(value)
    if missing_fields:
        missing = ", ".join(sorted(missing_fields))
        raise PromptResolutionError(
            f"persisted Prompt resolution has incomplete managed request fields; missing: {missing}"
        )
    extra_fields = set(value) - required_fields - lineage_fields
    if extra_fields:
        extra = ", ".join(sorted(extra_fields))
        raise PromptResolutionError(
            f"persisted Prompt resolution has invalid managed request fields; extra: {extra}"
        )

    try:
        digest = value["resolution_digest"]
        _record_text(digest, label="resolution_digest")

        spec_name = value["spec"]
        structure_digest = value["structure_digest"]
        base = value["base"]
        layers = value["layers"]
        axes = value["axes"]
        materials = value["materials"]
        rendered = value["rendered"]
        messages_value = value["messages"]
        attachments_value = value["attachments"]
        response_spec_value = value["response_spec"]

        _record_text(spec_name, label="spec")
        _record_text(structure_digest, label="structure_digest")
        _validate_persisted_descriptor(base, label="base")
        if not isinstance(layers, list):
            raise TypeError("layers must be a list")
        if not isinstance(axes, list):
            raise TypeError("axes must be a list")
        if not isinstance(materials, list):
            raise TypeError("materials must be a list")
        for index, record in enumerate(layers):
            _validate_persisted_layer(record, index=index)
        for index, record in enumerate(axes):
            _validate_persisted_axis(record, index=index)
        for index, record in enumerate(materials):
            _validate_persisted_material(record, index=index)
        rendered_record = _record_fields(
            rendered,
            required={"sha256", "bytes"},
            label="rendered",
        )
        rendered_sha256 = _record_text(rendered_record["sha256"], label="rendered.sha256")
        rendered_bytes = _record_bytes(rendered_record["bytes"], label="rendered.bytes")
        if not isinstance(messages_value, list):
            raise TypeError("messages must be a list")
        messages = []
        for record in messages_value:
            if not isinstance(record, Mapping) or set(record) != {"role", "parts"}:
                raise TypeError("message records must contain role and parts")
            messages.append(Message(role=record["role"], parts=record["parts"]))
        if not isinstance(attachments_value, list):
            raise TypeError("attachments must be a list")
        attachments = []
        for record in attachments_value:
            if not isinstance(record, Mapping) or set(record) != {
                "path",
                "content_hash",
                "mime_type",
                "size_bytes",
            }:
                raise TypeError("attachment records are incomplete")
            attachments.append(
                Attachment(
                    path=record["path"],
                    content_hash=record["content_hash"],
                    mime_type=record["mime_type"],
                    size_bytes=record["size_bytes"],
                )
            )
        if not isinstance(response_spec_value, Mapping) or set(response_spec_value) != {
            "schema_sha256",
            "format",
        }:
            raise TypeError("response_spec is incomplete")
        response_spec = ResponseSpec(
            schema_sha256=response_spec_value["schema_sha256"],
            format=response_spec_value["format"],
        )
        resolution = PromptResolution(
            spec_name=spec_name,
            structure_digest=structure_digest,
            base=base,
            layers=tuple(layers),
            axes=tuple(axes),
            materials=tuple(materials),
            rendered_sha256=rendered_sha256,
            rendered_bytes=rendered_bytes,
            schema=schema,
            messages=messages,
            attachments=attachments,
            response_spec=response_spec,
        )
        actual_digest = resolution.digest
    except PromptResolutionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise PromptResolutionError(
            f"persisted Prompt resolution has invalid managed request fields: {error}"
        ) from error
    if digest != actual_digest:
        raise PromptResolutionError("persisted Prompt resolution failed digest validation")
    present_lineage = lineage_fields & set(value)
    if present_lineage:
        if present_lineage != lineage_fields:
            missing = ", ".join(sorted(lineage_fields - present_lineage))
            raise PromptResolutionError(
                f"persisted Prompt resolution has incomplete call lineage; missing: {missing}"
            )
        base_resolution_digest = value["base_resolution_digest"]
        phase = value["phase"]
        repair_round = value["repair_round"]
        if not isinstance(base_resolution_digest, str) or not base_resolution_digest:
            raise PromptResolutionError(
                "persisted Prompt resolution has invalid call lineage base_resolution_digest"
            )
        if not isinstance(phase, str) or phase not in {"primary", "repair"}:
            raise PromptResolutionError(
                "persisted Prompt resolution has invalid call lineage phase"
            )
        if isinstance(repair_round, bool) or not isinstance(repair_round, int):
            raise PromptResolutionError(
                "persisted Prompt resolution has invalid call lineage repair_round"
            )
        if (
            repair_round < 0
            or (phase == "primary" and repair_round != 0)
            or (phase == "repair" and repair_round == 0)
        ):
            raise PromptResolutionError(
                "persisted Prompt resolution has invalid call lineage phase/repair_round"
            )
        if base_resolution_digest != digest:
            raise PromptResolutionError(
                "persisted Prompt resolution has a mismatched base resolution"
            )


@dataclass(frozen=True)
class PromptResolution:
    """Immutable provenance and managed request manifest for one rendered Prompt."""

    spec_name: str
    structure_digest: str
    base: Mapping[str, Any]
    layers: tuple[Mapping[str, Any], ...]
    axes: tuple[Mapping[str, Any], ...]
    materials: tuple[Mapping[str, Any], ...]
    rendered_sha256: str
    rendered_bytes: int
    schema: int = PROMPT_RESOLUTION_SCHEMA
    messages: tuple[Message, ...] = field(default_factory=tuple)
    attachments: tuple[Attachment, ...] = field(default_factory=tuple)
    response_spec: ResponseSpec = field(default_factory=ResponseSpec)

    def __post_init__(self) -> None:
        if type(self.schema) is not int or self.schema != PROMPT_RESOLUTION_SCHEMA:
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
        checked_messages: list[Message] = []
        for message in self.messages:
            if isinstance(message, Message):
                checked_messages.append(message)
            elif isinstance(message, Mapping):
                checked_messages.append(Message(role=message["role"], parts=list(message["parts"])))
            else:
                raise PromptResolutionError("PromptResolution messages must contain Message values")
        checked_attachments: list[Attachment] = []
        for attachment in self.attachments:
            if isinstance(attachment, Attachment):
                checked_attachments.append(attachment)
            elif isinstance(attachment, Mapping):
                checked_attachments.append(
                    Attachment(
                        path=attachment["path"],
                        content_hash=attachment["content_hash"],
                        mime_type=attachment["mime_type"],
                        size_bytes=attachment["size_bytes"],
                    )
                )
            else:
                raise PromptResolutionError(
                    "PromptResolution attachments must contain Attachment values"
                )
        if not isinstance(self.response_spec, ResponseSpec):
            if isinstance(self.response_spec, Mapping):
                response_spec = ResponseSpec(
                    schema_sha256=self.response_spec.get("schema_sha256"),
                    format=self.response_spec.get("format", "text"),
                )
            else:
                raise PromptResolutionError("PromptResolution response_spec must be ResponseSpec")
        else:
            response_spec = self.response_spec
        object.__setattr__(self, "messages", tuple(checked_messages))
        object.__setattr__(self, "attachments", tuple(checked_attachments))
        object.__setattr__(self, "response_spec", response_spec)

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
            "messages": [message.canonical() for message in self.messages],
            "attachments": [attachment.canonical() for attachment in self.attachments],
            "response_spec": self.response_spec.canonical(),
        }

    @property
    def digest(self) -> str:
        return _prompt_digest(self)

    def canonical(self) -> dict[str, Any]:
        return {**self._body(), "resolution_digest": self.digest}


def _prompt_digest(resolution: PromptResolution) -> str:
    """Return the canonical request digest without transport-only base64 expansion."""
    body = resolution._body()
    # Preserve the original resolution provenance in the digest, but never make an
    # attachment's source path part of content identity. FileRef records retain
    # their binding and content hash while dropping only the resolved path. MIME
    # and byte count remain manifest/preflight metadata; content_hash is the
    # attachment identity for Prompt lineage.
    body["attachments"] = [attachment.content_hash for attachment in resolution.attachments]
    body["materials"] = [
        {
            **material,
            "file_ref": {
                key: value for key, value in material["file_ref"].items() if key != "path"
            },
        }
        if isinstance(material.get("file_ref"), dict)
        else material
        for material in body["materials"]
    ]
    return sha(body)


def _part_bytes(part: str | dict[str, Any]) -> int:
    if isinstance(part, str):
        return len(part.encode("utf-8"))
    return len(json.dumps(_thaw_value(part), ensure_ascii=False, sort_keys=True).encode("utf-8"))


def _estimated_tokens(messages: list[Message]) -> int:
    """Estimate tokens from canonical UTF-8 request parts at roughly four bytes each."""
    total_bytes = sum(_part_bytes(part) for message in messages for part in message.parts)
    return (total_bytes + 3) // 4


def preflight(
    resolution: PromptResolution,
    policy: PreflightPolicy = _DEFAULT_PREFLIGHT_POLICY,
) -> PreflightReport:
    """Validate a managed request before cache lookup or provider work."""
    if not isinstance(resolution, PromptResolution):
        raise TypeError("preflight requires a PromptResolution")
    if not isinstance(policy, PreflightPolicy):
        raise TypeError("preflight policy must be PreflightPolicy")

    estimated_tokens = _estimated_tokens(resolution.messages)
    total_bytes = sum(attachment.size_bytes for attachment in resolution.attachments)
    violations: list[PreflightViolation] = []
    if estimated_tokens > policy.max_tokens:
        violations.append(
            PreflightViolation(
                check="token_count",
                limit=policy.max_tokens,
                actual=estimated_tokens,
                message=(
                    f"Estimated prompt tokens {estimated_tokens} exceed limit {policy.max_tokens}"
                ),
            )
        )
    if len(resolution.attachments) > policy.max_attachments:
        violations.append(
            PreflightViolation(
                check="attachment_count",
                limit=policy.max_attachments,
                actual=len(resolution.attachments),
                message=(
                    f"Attachment count {len(resolution.attachments)} exceeds limit "
                    f"{policy.max_attachments}"
                ),
            )
        )
    if total_bytes > policy.max_attachment_bytes:
        violations.append(
            PreflightViolation(
                check="byte_size",
                limit=policy.max_attachment_bytes,
                actual=total_bytes,
                message=(
                    f"Attachment bytes {total_bytes} exceed limit {policy.max_attachment_bytes}"
                ),
            )
        )
    return PreflightReport(violations, estimated_tokens, total_bytes)


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
    ) -> PromptCatalogSnapshot:
        try:
            resolved_root = _safe_configured_path(root)
        except (OSError, ValueError) as error:
            raise PromptDefinitionError(
                f"Prompt catalog root must not contain a user symlink: {root}"
            ) from error
        try:
            root_info = resolved_root.lstat()
        except FileNotFoundError:
            root_info = None
        except OSError as error:
            raise PromptDefinitionError(
                f"Prompt catalog root is not safely readable: {root}"
            ) from error
        if root_info is not None:
            if not stat.S_ISDIR(root_info.st_mode):
                raise PromptDefinitionError(
                    f"Prompt catalog root must be a regular directory: {resolved_root}"
                )
            try:
                # Bind the existing root once before reading entries.  The
                # per-file reader below still performs its own descriptor-
                # relative no-follow open and race check.
                with SecureDirectory(resolved_root, create=False):
                    pass
            except (OSError, ValueError) as error:
                raise PromptDefinitionError(
                    f"Prompt catalog root must not contain a user symlink: {root}"
                ) from error
        names: set[str] = set()
        for spec in prompt_specs:
            names.update(reference.name for reference in spec.references())
        entries: dict[str, _CatalogEntry] = {}
        for name in sorted(names):
            checked = _validate_prompt_path(name)
            try:
                raw = _read_regular_file_no_follow(resolved_root, f"{checked}.md")
                text = raw.decode("utf-8")
            except (OSError, UnicodeDecodeError) as error:
                raise PromptDefinitionError(
                    f"PromptRef {checked!r} must be a regular, readable UTF-8 .md file"
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
        file_contents: Mapping[str | Path, bytes] | None = None,
    ) -> ResolvedPrompt:
        slots: dict[str, str] = {}
        layers: list[dict[str, Any]] = []
        axes: list[dict[str, Any]] = []
        materials: list[dict[str, Any]] = []
        attachments: list[Attachment] = []
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
            file_record = None
            if isinstance(material.source, FileRef):
                value, file_record, attachment = _resolve_file_ref(
                    material.source,
                    file_contents=file_contents,
                    inputs=inputs,
                    params=params,
                    item=item,
                    carry=carry,
                    has_item=has_item,
                    has_carry=has_carry,
                    context=f"material {material.slot!r}",
                )
                attachments.append(attachment)
            else:
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
            material_record = {
                "slot": material.slot,
                "source": material.source.canonical(),
                "title": material.title,
                "sha256": sha(rendered_material),
                "bytes": len(encoded),
            }
            if file_record is not None:
                material_record["file_ref"] = file_record
            materials.append(material_record)
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
            messages=[Message(role="user", parts=[rendered])],
            attachments=attachments,
            response_spec=ResponseSpec(),
        )
        return ResolvedPrompt(rendered, resolution)


def _resolve_file_ref(
    source: FileRef,
    *,
    file_contents: Mapping[str | Path, bytes] | None,
    inputs: Mapping[str, Any],
    params: Mapping[str, Any],
    item: Any,
    carry: Any,
    has_item: bool,
    has_carry: bool,
    context: str,
) -> tuple[str, dict[str, Any], Attachment]:
    path_value = _resolve_value(
        source.path_from,
        inputs=inputs,
        params=params,
        item=item,
        carry=carry,
        has_item=has_item,
        has_carry=has_carry,
        context=f"{context} FileRef path_from",
    )
    if not isinstance(path_value, str | Path):
        raise PromptResolutionError(f"{context} FileRef path_from must resolve to a path string")
    path = str(path_value)
    if file_contents is None:
        raise PromptResolutionError(f"{context} FileRef requires injected file_contents")
    normalized = {str(key): value for key, value in file_contents.items()}
    try:
        raw = normalized[path]
    except KeyError as error:
        raise PromptResolutionError(
            f"{context} FileRef path {path!r} is missing from injected file_contents"
        ) from error
    if not isinstance(raw, bytes):
        raise PromptResolutionError(f"{context} FileRef file_contents values must be bytes")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise PromptResolutionError(f"{context} FileRef path {path!r} must be UTF-8") from error
    attachment = Attachment(
        path=path,
        content_hash=sha256(raw).hexdigest(),
        mime_type=mimetypes.guess_type(path)[0] or "application/octet-stream",
        size_bytes=len(raw),
    )
    return (
        text,
        {
            "path": path,
            "path_from": source.path_from.canonical(),
            "sha256": attachment.content_hash,
            "bytes": len(raw),
        },
        attachment,
    )


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
    for name, model_field in model_cls.model_fields.items():
        required = "必填" if model_field.is_required() else "可选"
        description = model_field.description or "无描述"
        field_lines.append(
            f"- `{name}`：`{_type_label(model_field.annotation)}`；{required}；{description}"
        )

    body = "\n".join(field_lines)
    if with_example:
        example = {
            name: _example_value(model_field.annotation)
            for name, model_field in model_cls.model_fields.items()
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
