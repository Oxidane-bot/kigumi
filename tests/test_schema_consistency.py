from __future__ import annotations

import re
import sys
from pathlib import Path

from kigumi._runstate import (
    ATTEMPT_RECEIPT_SCHEMA,
    FAILURE_SCHEMA,
    RUN_MANIFEST_SCHEMA,
    RUN_SIDECAR_SCHEMA,
    SUCCESS_CANDIDATE_SCHEMA,
)
from kigumi.agents import AGENT_EXECUTOR_SCHEMA, AGENT_SCHEMA
from kigumi.dag import CACHE_SCHEMA
from kigumi.optimize import _STATE_SCHEMA_VERSION
from kigumi.profile import WORKFLOW_PROFILE_SCHEMA
from kigumi.prompt import PROMPT_RESOLUTION_SCHEMA
from kigumi.store import _NODE_CACHE_ENVELOPE_SCHEMA

ROOT = Path(__file__).resolve().parents[1]
CHANGELOG_RELEASE_PATTERN = re.compile(
    r"^## \[(?P<version>\d+\.\d+\.\d+)\](?: - \d{4}-\d{2}-\d{2})?$",
    re.MULTILINE,
)

CODE_SCHEMAS: dict[str, int] = {
    "CACHE_SCHEMA": CACHE_SCHEMA,
    "AGENT_EXECUTOR_SCHEMA": AGENT_EXECUTOR_SCHEMA,
    "AGENT_SCHEMA": AGENT_SCHEMA,
    "RUN_MANIFEST_SCHEMA": RUN_MANIFEST_SCHEMA,
    "RUN_SIDECAR_SCHEMA": RUN_SIDECAR_SCHEMA,
    "FAILURE_SCHEMA": FAILURE_SCHEMA,
    "SUCCESS_CANDIDATE_SCHEMA": SUCCESS_CANDIDATE_SCHEMA,
    "ATTEMPT_RECEIPT_SCHEMA": ATTEMPT_RECEIPT_SCHEMA,
    "PROMPT_RESOLUTION_SCHEMA": PROMPT_RESOLUTION_SCHEMA,
    "WORKFLOW_PROFILE_SCHEMA": WORKFLOW_PROFILE_SCHEMA,
    "_NODE_CACHE_ENVELOPE_SCHEMA": _NODE_CACHE_ENVELOPE_SCHEMA,
    "_STATE_SCHEMA_VERSION": _STATE_SCHEMA_VERSION,
}

# The docs use both source-style constants and the lower-case field names written to
# durable JSON. Keep both spellings tied to the imported source constant.
DIRECT_SCHEMA_ALIASES: dict[str, str] = {
    **{name: name for name in CODE_SCHEMAS},
    "agent_executor_schema": "AGENT_EXECUTOR_SCHEMA",
    "agent_schema": "AGENT_SCHEMA",
    "run_manifest_schema": "RUN_MANIFEST_SCHEMA",
    "run_sidecar_schema": "RUN_SIDECAR_SCHEMA",
    "failure_schema": "FAILURE_SCHEMA",
    "success_candidate_schema": "SUCCESS_CANDIDATE_SCHEMA",
    "attempt_receipt_schema": "ATTEMPT_RECEIPT_SCHEMA",
    "prompt_resolution_schema": "PROMPT_RESOLUTION_SCHEMA",
    "workflow_profile_schema": "WORKFLOW_PROFILE_SCHEMA",
}
_DIRECT_NAMES = "|".join(
    sorted((re.escape(name) for name in DIRECT_SCHEMA_ALIASES), key=len, reverse=True)
)
DIRECT_SCHEMA_PATTERN = re.compile(
    rf"(?<![A-Za-z0-9_])(?P<name>{_DIRECT_NAMES})\s*=\s*(?P<value>\d+)"
)
NUMERIC_DAG_CITATION_PATTERN = re.compile(r"kigumi/dag\.py:[0-9]")

_MARKDOWN_HEADING_PATTERN = re.compile(r"^\s{0,3}#{1,6}(?:\s|$)")
_MARKDOWN_LIST_ITEM_PATTERN = re.compile(r"^\s{0,3}(?:[-+*]|\d+[.)])(?:\s+|$)")

# These are the prose forms actually used by the live contracts. They are deliberately
# scoped to the object named by the sentence so that unrelated capsule/report schemas are
# not mistaken for one of Kigumi's durable schemas.
PROSE_SCHEMA_PATTERNS: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    (
        "node cache envelope schema",
        "_NODE_CACHE_ENVELOPE_SCHEMA",
        re.compile(r"\bnode cache envelope schema\s+(\d+)\b", re.IGNORECASE),
    ),
    (
        "_run.json schema",
        "RUN_MANIFEST_SCHEMA",
        re.compile(r"`?_run\.json`?\s+schema\s+(\d+)\b"),
    ),
    (
        "schema _run.json",
        "RUN_MANIFEST_SCHEMA",
        re.compile(r"\bschema[- ](\d+)\s+`?_run\.json`?"),
    ),
    (
        "schema run manifest",
        "RUN_MANIFEST_SCHEMA",
        re.compile(r"\bschema[- ](\d+)\s+run manifest\b", re.IGNORECASE),
    ),
    (
        "schema manifest",
        "RUN_MANIFEST_SCHEMA",
        re.compile(r"\bschema[- ](\d+)\s+manifest\b", re.IGNORECASE),
    ),
    (
        "schema run",
        "RUN_MANIFEST_SCHEMA",
        re.compile(r"\bschema[- ](\d+)\s+run\b(?!\s+manifest)", re.IGNORECASE),
    ),
    (
        "schema WorkflowProfile",
        "WORKFLOW_PROFILE_SCHEMA",
        re.compile(r"\bschema[- ](\d+)\s+`?WorkflowProfile`?\b", re.IGNORECASE),
    ),
    (
        "schema sidecar",
        "RUN_SIDECAR_SCHEMA",
        re.compile(
            r"\bschema[- ](\d+)\s+(?:(?:cold/warm|run)\s+)?sidecar\b",
            re.IGNORECASE,
        ),
    ),
    (
        "schema Agent failure receipt",
        "FAILURE_SCHEMA",
        re.compile(r"\bschema[- ](\d+)\s+Agent failure receipt\b", re.IGNORECASE),
    ),
    (
        "schema attempt receipt",
        "ATTEMPT_RECEIPT_SCHEMA",
        re.compile(r"\bschema[- ](\d+)\s+attempt receipt\b", re.IGNORECASE),
    ),
    (
        "receipt schema",
        "ATTEMPT_RECEIPT_SCHEMA",
        re.compile(r"\breceipt\s+schema[- ]?(\d+)\b", re.IGNORECASE),
    ),
    (
        "schema candidate",
        "SUCCESS_CANDIDATE_SCHEMA",
        re.compile(
            r"\bschema[- ](\d+)\s+(?:(?:success|canonical)\s+)?candidate\b",
            re.IGNORECASE,
        ),
    ),
    (
        "schema receipt",
        "ATTEMPT_RECEIPT_SCHEMA",
        re.compile(r"\bschema[- ](\d+)\s+receipt\b", re.IGNORECASE),
    ),
)


def _live_document_paths() -> list[Path]:
    # docs/reviews/ contains dated historical records; old schema values there describe
    # the past and are intentionally excluded from the current consistency check.
    return sorted(
        [
            ROOT / "README.md",
            ROOT / "README.zh-CN.md",
            ROOT / "DESIGN.md",
            *(
                path
                for path in (ROOT / "docs").rglob("*.md")
                if path.relative_to(ROOT).parts[:2] != ("docs", "reviews")
            ),
        ]
    )


def _live_document_lines(path: Path):
    # DESIGN.md also embeds dated release notes. Keep its Unreleased entry, but do not
    # reinterpret older release values as current claims.
    in_design_history = False
    historical_design_entry = False
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if path.name == "DESIGN.md" and line.strip() == "## 修订记录":
            in_design_history = True
            historical_design_entry = False
            continue
        if path.name == "DESIGN.md" and in_design_history and line.startswith("- "):
            historical_design_entry = not line.startswith("- Unreleased：")
        if historical_design_entry:
            continue
        yield line_number, line


def test_live_docs_do_not_cite_numeric_dag_lines() -> None:
    """契约引用必须指向 dag.py 符号，避免源码增删后静默指错位置。"""
    findings: list[str] = []
    for path in _live_document_paths():
        relative_path = path.relative_to(ROOT).as_posix()
        for line_number, line in _live_document_lines(path):
            if NUMERIC_DAG_CITATION_PATTERN.search(line):
                findings.append(f"{relative_path}:{line_number}: {line}")

    assert not findings, "Numeric kigumi/dag.py citations in live docs:\n" + "\n".join(findings)


def _document_blocks(lines):
    """Group physical lines into Markdown paragraphs or individual list items."""
    block = []
    for line_number, line in lines:
        if block and line_number != block[-1][0] + 1:
            yield block
            block = []

        if not line.strip():
            if block:
                yield block
                block = []
            continue

        if _MARKDOWN_HEADING_PATTERN.match(line):
            if block:
                yield block
            yield [(line_number, line)]
            block = []
            continue

        if _MARKDOWN_LIST_ITEM_PATTERN.match(line):
            if block:
                yield block
            block = [(line_number, line)]
            continue

        block.append((line_number, line))

    if block:
        yield block


def _join_document_block(block):
    # Markdown soft-wraps lines in one block as prose; insert one separator space so
    # adjacent ASCII words cannot concatenate while keeping headings/list items apart.
    text_parts = []
    line_numbers = []
    for index, (line_number, line) in enumerate(block):
        if index:
            text_parts.append(" ")
            line_numbers.append(line_number)
        text_parts.append(line)
        line_numbers.extend([line_number] * len(line))
    return "".join(text_parts), line_numbers


def _documented_schema_claims():
    for path in _live_document_paths():
        for block in _document_blocks(_live_document_lines(path)):
            text, line_numbers = _join_document_block(block)
            matches = []
            for match in DIRECT_SCHEMA_PATTERN.finditer(text):
                alias = match.group("name")
                matches.append(
                    (
                        match.start(),
                        0,
                        alias,
                        DIRECT_SCHEMA_ALIASES[alias],
                        int(match.group("value")),
                    )
                )
            for pattern_index, (label, constant_name, pattern) in enumerate(
                PROSE_SCHEMA_PATTERNS, 1
            ):
                for match in pattern.finditer(text):
                    matches.append(
                        (
                            match.start(),
                            pattern_index,
                            label,
                            constant_name,
                            int(match.group(1)),
                        )
                    )
            for start, _, label, constant_name, documented_value in sorted(matches):
                yield (
                    path,
                    line_numbers[start],
                    label,
                    constant_name,
                    documented_value,
                )


def test_documented_schema_values_match_code() -> None:
    mismatches: list[str] = []
    for path, line_number, label, constant_name, documented_value in _documented_schema_claims():
        true_value = CODE_SCHEMAS[constant_name]
        if documented_value != true_value:
            relative_path = path.relative_to(ROOT).as_posix()
            mismatches.append(
                f"{relative_path}:{line_number}: {label} documented value "
                f"{documented_value}; true value {true_value}"
            )

    assert not mismatches, "Schema documentation drift:\n" + "\n".join(mismatches)


def test_cache_schema_eight_is_recorded_once_in_release_changelog() -> None:
    """CACHE_SCHEMA=8 belongs to 0.14.0; schema 7 stays historical."""
    assert CACHE_SCHEMA == 8

    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    headings = list(CHANGELOG_RELEASE_PATTERN.finditer(changelog))
    assert headings, "CHANGELOG.md must contain dated release sections"

    unreleased_end = headings[0].start()
    unreleased = changelog[:unreleased_end]
    assert not re.search(r"CACHE_SCHEMA[^\n]*升至\s*8\b", unreleased)

    released_sections: list[tuple[str, str]] = []
    for index, heading in enumerate(headings):
        body_end = headings[index + 1].start() if index + 1 < len(headings) else len(changelog)
        released_sections.append((heading.group("version"), changelog[heading.end() : body_end]))

    prior_rotations = [
        version
        for version, body in released_sections
        if re.search(r"CACHE_SCHEMA[^\n]*(?:升至|=)\s*7\b", body)
    ]
    assert prior_rotations, "CACHE_SCHEMA=7 must remain recorded in a dated release section"
    schema_eight_releases = [
        version
        for version, body in released_sections
        if re.search(r"CACHE_SCHEMA[^\n]*升至\s*8\b", body)
    ]
    assert schema_eight_releases == ["0.14.0"]

    stale_claims = [
        version
        for version, body in released_sections
        if re.search(r"CACHE_SCHEMA|schema\s+7", body, re.IGNORECASE)
        and re.search(r"尚未发布|未发布", body)
    ]
    assert not stale_claims, (
        "Released CHANGELOG sections must not describe CACHE_SCHEMA=7 as unreleased: "
        + ", ".join(stale_claims)
    )


def test_documented_schema_claims_join_wrapped_lines_and_report_start(
    tmp_path: Path, monkeypatch
) -> None:
    document = tmp_path / "wrapped.md"
    document.write_text(
        "A paragraph before the claim.\nThe current schema-1\nWorkflowProfile is the shared IR.\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys.modules[__name__], "_live_document_paths", lambda: [document])

    assert list(_documented_schema_claims()) == [
        (document, 2, "schema WorkflowProfile", "WORKFLOW_PROFILE_SCHEMA", 1)
    ]


def test_documented_schema_claims_do_not_cross_markdown_boundaries(
    tmp_path: Path, monkeypatch
) -> None:
    document = tmp_path / "boundaries.md"
    document.write_text(
        "schema-1\n"
        "\n"
        "WorkflowProfile is in another paragraph.\n"
        "\n"
        "# schema-1\n"
        "WorkflowProfile starts a new section.\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys.modules[__name__], "_live_document_paths", lambda: [document])

    assert list(_documented_schema_claims()) == []
