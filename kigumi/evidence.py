"""Evidence retention policy, credential scrubbing, and content reduction."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, get_args
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .artifacts import canonical_json, sha

EvidenceMode = Literal["full", "redacted", "hash_only"]
_MODES = frozenset(get_args(EvidenceMode))
_SECRET_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "password",
    "secret",
    "token",
    "cookie",
)
_CONTENT_KEYS = frozenset(
    {
        "arguments",
        "content",
        "input",
        "inputs",
        "output",
        "outputs",
        "prompt",
        "reasoning",
        "reasoning_content",
        "text",
        "thinking",
        "thinking_content",
    }
)
_URL_RE = re.compile(r"""https?://[^\s"'<>]+""")
_BEARER_RE = re.compile(r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]+")


@dataclass(frozen=True)
class EvidencePolicy:
    """Independent retention modes for request and execution evidence."""

    request: EvidenceMode = "full"
    response: EvidenceMode = "full"
    stderr: EvidenceMode = "full"
    trajectory: EvidenceMode = "full"

    def __post_init__(self) -> None:
        for name in ("request", "response", "stderr", "trajectory"):
            if getattr(self, name) not in _MODES:
                raise ValueError(f"EvidencePolicy {name} must be full, redacted, or hash_only")

    def canonical(self) -> dict[str, EvidenceMode]:
        """Return the stable representation hashed into origin provenance."""
        return {
            "request": self.request,
            "response": self.response,
            "stderr": self.stderr,
            "trajectory": self.trajectory,
        }

    @property
    def digest(self) -> str:
        """Return the canonical policy digest without affecting content keys."""
        return sha(self.canonical())


@dataclass(frozen=True)
class CapturedEvidence:
    """A reduced evidence payload plus the descriptor bound to original bytes."""

    descriptor: dict[str, Any]
    data: bytes | None


def capture_evidence(
    data: bytes,
    *,
    media_type: str,
    mode: EvidenceMode,
    secrets: tuple[str, ...] = (),
) -> CapturedEvidence:
    """Scrub bytes first, then retain full, redacted, or hash-only evidence."""
    if not isinstance(data, bytes):
        raise TypeError("evidence data must be bytes")
    if not isinstance(media_type, str) or not media_type:
        raise ValueError("media_type must be a non-empty string")
    if mode not in _MODES:
        raise ValueError("invalid evidence mode")
    scrubbed = scrub_bytes(data, secrets=secrets)
    descriptor = {
        "sha256": hashlib.sha256(scrubbed).hexdigest(),
        "bytes": len(scrubbed),
        "media_type": media_type,
        "mode": mode,
    }
    if mode == "hash_only":
        return CapturedEvidence(descriptor, None)
    if mode == "full":
        return CapturedEvidence(descriptor, scrubbed)
    redacted = _redact_bytes_structurally(scrubbed, media_type)
    descriptor = {
        **descriptor,
        "redacted_sha256": hashlib.sha256(redacted).hexdigest(),
        "redacted_bytes": len(redacted),
    }
    return CapturedEvidence(descriptor, redacted)


def scrub_evidence(
    value: Any,
    *,
    mode: EvidenceMode,
    secrets: tuple[str, ...] = (),
) -> Any:
    """Scrub structured evidence and optionally replace content-bearing values."""
    if mode not in _MODES:
        raise ValueError("invalid evidence mode")
    scrubbed = _scrub(copy.deepcopy(value), _secret_values(secrets))
    if mode == "full":
        return scrubbed
    encoded = canonical_json(scrubbed).encode("utf-8")
    if mode == "hash_only":
        return {
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "bytes": len(encoded),
            "media_type": "application/json",
            "mode": "hash_only",
        }
    return _redact_content(scrubbed)


def scrub_bytes(data: bytes, *, secrets: tuple[str, ...] = ()) -> bytes:
    """Scrub known credentials, authorization syntax, and secret URL queries."""
    text = data.decode("utf-8", errors="replace")
    return _scrub_string(text, _secret_values(secrets)).encode("utf-8")


def _secret_values(explicit: tuple[str, ...]) -> tuple[str, ...]:
    discovered = [
        value
        for name, value in os.environ.items()
        if value and len(value) >= 4 and any(part in name.lower() for part in _SECRET_KEY_PARTS)
    ]
    return tuple(
        sorted(
            {value for value in (*explicit, *discovered) if isinstance(value, str) and value},
            key=len,
            reverse=True,
        )
    )


def _scrub(value: Any, secrets: tuple[str, ...]) -> Any:
    if isinstance(value, str):
        return _scrub_string(value, secrets)
    if isinstance(value, Mapping):
        scrubbed: dict[str, Any] = {}
        for raw_key, child in value.items():
            key = str(raw_key)
            normalized = key.lower().replace("-", "_")
            if _is_secret_key(normalized):
                scrubbed[key] = "***"
            else:
                scrubbed[key] = _scrub(child, secrets)
        return scrubbed
    if isinstance(value, list):
        return [_scrub(child, secrets) for child in value]
    if isinstance(value, tuple):
        return [_scrub(child, secrets) for child in value]
    return value


def _scrub_string(value: str, secrets: tuple[str, ...]) -> str:
    scrubbed = value
    for secret in secrets:
        scrubbed = scrubbed.replace(secret, "***")
    scrubbed = _BEARER_RE.sub("***", scrubbed)
    return _URL_RE.sub(lambda match: _scrub_url(match.group(0)), scrubbed)


def _scrub_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return value
    query = []
    for key, item in parse_qsl(parsed.query, keep_blank_values=True):
        normalized = key.lower().replace("-", "_")
        query.append((key, "***" if _is_secret_key(normalized) else item))
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query),
            parsed.fragment,
        )
    )


def _redact_content(value: Any) -> Any:
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for raw_key, child in value.items():
            key = str(raw_key)
            normalized = key.lower().replace("-", "_")
            if normalized in _CONTENT_KEYS:
                encoded = canonical_json(child).encode("utf-8")
                redacted[key] = {
                    "redacted": True,
                    "sha256": hashlib.sha256(encoded).hexdigest(),
                    "bytes": len(encoded),
                }
            else:
                redacted[key] = _redact_content(child)
        return redacted
    if isinstance(value, list):
        return [_redact_content(child) for child in value]
    return value


def _redact_bytes_structurally(data: bytes, media_type: str) -> bytes:
    text = data.decode("utf-8", errors="replace")
    if media_type in {"application/x-ndjson", "application/ndjson"}:
        lines: list[str] = []
        for line in text.splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                encoded = line.encode("utf-8")
                value = {
                    "redacted": True,
                    "sha256": hashlib.sha256(encoded).hexdigest(),
                    "bytes": len(encoded),
                }
            lines.append(canonical_json(_redact_content(value)))
        return (("\n".join(lines) + "\n") if lines else "").encode("utf-8")
    if media_type == "application/json":
        try:
            return canonical_json(_redact_content(json.loads(text))).encode("utf-8")
        except json.JSONDecodeError:
            pass
    return canonical_json(
        {
            "redacted": True,
            "sha256": hashlib.sha256(data).hexdigest(),
            "bytes": len(data),
            "media_type": media_type,
        }
    ).encode("utf-8")


def _is_secret_key(normalized: str) -> bool:
    if normalized in _SECRET_KEY_PARTS:
        return True
    return normalized.endswith(
        (
            "_api_key",
            "_apikey",
            "_authorization",
            "_credential",
            "_credentials",
            "_password",
            "_secret",
            "_token",
            "_cookie",
        )
    )
