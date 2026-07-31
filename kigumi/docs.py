"""Locate and read the documentation pages that ship inside the wheel.

The repository is the single source of truth: hatch ``force-include`` maps each page
into the wheel rather than copying it, so an agent working in a downstream project
reads the same text from site-packages. Running from a source checkout the packaged
layout does not exist, so resolution falls back to the repository path; both layouts
expose the same page names.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

__all__ = ["SHIPPED_DOCS", "ShippedDoc", "read_doc", "resolve_doc"]


@dataclass(frozen=True)
class ShippedDoc:
    """One documentation page reachable through ``kigumi docs``."""

    name: str
    source: str
    """Path in the repository, relative to the project root."""
    packaged: str
    """Path in the wheel, relative to the installed ``kigumi`` package directory."""
    summary: str


# Keep in sync with [tool.hatch.build.targets.wheel.force-include] in pyproject.toml;
# tests/test_shipped_docs.py enforces the mapping in both directions. DESIGN.md and
# CHANGELOG.md land beside the docs directory so the relative links inside adoption.md
# and contracts/README.md still resolve once installed.
SHIPPED_DOCS: tuple[ShippedDoc, ...] = (
    ShippedDoc(
        "brief",
        "docs/brief.md",
        "docs/brief.md",
        "what kigumi already owns; read before writing code",
    ),
    ShippedDoc(
        "capabilities",
        "docs/capabilities.md",
        "docs/capabilities.md",
        "need -> symbol index, one line per capability",
    ),
    ShippedDoc(
        "adoption",
        "docs/adoption.md",
        "docs/adoption.md",
        "how to adopt it, recommended shapes, troubleshooting",
    ),
    ShippedDoc(
        "api",
        "docs/api.md",
        "docs/api.md",
        "signatures, result types, exceptions and their handling",
    ),
    ShippedDoc(
        "cli",
        "docs/cli.md",
        "docs/cli.md",
        "both CLIs: every command, flag, default and exit code",
    ),
    ShippedDoc(
        "contracts",
        "docs/contracts/README.md",
        "docs/contracts/README.md",
        "index of verifiable invariants",
    ),
    ShippedDoc(
        "design",
        "DESIGN.md",
        "DESIGN.md",
        "design philosophy, boundaries and what this tool refuses to do",
    ),
    ShippedDoc(
        "changelog",
        "CHANGELOG.md",
        "CHANGELOG.md",
        "user-facing release changes, including cache family changes",
    ),
)

_DOCS_BY_NAME = {doc.name: doc for doc in SHIPPED_DOCS}


def resolve_doc(name: str) -> Path:
    """Return the readable path of a shipped page, preferring the packaged copy."""
    try:
        doc = _DOCS_BY_NAME[name]
    except KeyError:
        known = ", ".join(sorted(_DOCS_BY_NAME))
        raise KeyError(f"unknown doc {name!r}; shipped pages: {known}") from None

    packaged = Path(str(files("kigumi"))) / doc.packaged
    if packaged.is_file():
        return packaged
    checkout = Path(__file__).resolve().parent.parent / doc.source
    if checkout.is_file():
        return checkout
    raise FileNotFoundError(
        f"doc {name!r} is not readable at {packaged} or {checkout}; "
        "the installed wheel is missing its shipped documentation"
    )


def read_doc(name: str) -> str:
    """Return the text of a shipped page."""
    return resolve_doc(name).read_text(encoding="utf-8")
