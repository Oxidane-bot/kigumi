#!/usr/bin/env python3
"""Verify the built wheel/sdist release contract using only the standard library."""

from __future__ import annotations

import argparse
import email
import sys
import tarfile
import zipfile
from pathlib import Path

EXPECTED_RUNTIME_DEPENDENCIES = frozenset({"pydantic", "litellm", "pytest", "ruff"})
EXPECTED_EXTRAS = frozenset({"dev", "litellm"})
REQUIRED_RESOURCES = frozenset(
    {
        "kigumi/_pi_bridge.ts",
        "kigumi/_pi_bridge_policy.mjs",
    }
)
# Agent-facing documentation is part of the release contract: `kigumi brief` and
# `kigumi docs` must work from the wheel alone, with no checkout present. The wheel
# gets these paths from force-include; the sdist carries the repository sources that
# force-include maps, so each target is checked against its own layout.
REQUIRED_WHEEL_DOCS = frozenset(
    {
        "kigumi/DESIGN.md",
        "kigumi/CHANGELOG.md",
        "kigumi/docs/brief.md",
        "kigumi/docs/capabilities.md",
        "kigumi/docs/adoption.md",
        "kigumi/docs/api.md",
        "kigumi/docs/cli.md",
        "kigumi/docs/recovery.md",
        "kigumi/docs/contracts/README.md",
    }
)
REQUIRED_SDIST_DOCS = frozenset(
    {
        "DESIGN.md",
        "CHANGELOG.md",
        "docs/brief.md",
        "docs/capabilities.md",
        "docs/adoption.md",
        "docs/api.md",
        "docs/cli.md",
        "docs/recovery.md",
        "docs/contracts/README.md",
    }
)


def _dependency_name(requirement: str) -> str:
    for separator in (" ", ";", "<", ">", "=", "!", "~", "["):
        requirement = requirement.split(separator, 1)[0]
    return requirement.strip().lower().replace("_", "-")


def verify(dist: Path, expected_version: str) -> None:
    wheels = sorted(dist.glob("kigumi-*.whl"))
    sdists = sorted(dist.glob("kigumi-*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise RuntimeError(_dist_layout_error(dist, expected_version, wheels, sdists))

    with zipfile.ZipFile(wheels[0]) as archive:
        wheel_names = set(archive.namelist())
        missing = (REQUIRED_RESOURCES | REQUIRED_WHEEL_DOCS) - wheel_names
        if missing:
            raise RuntimeError(f"wheel is missing resources: {sorted(missing)}")
        _reject_acp(wheel_names, "wheel")
        metadata_name = next(name for name in wheel_names if name.endswith(".dist-info/METADATA"))
        metadata = email.message_from_bytes(archive.read(metadata_name))
        if metadata["Version"] != expected_version:
            raise RuntimeError(f"wheel version {metadata['Version']!r} != {expected_version!r}")
        dependencies = {
            _dependency_name(requirement) for requirement in metadata.get_all("Requires-Dist", [])
        }
        unexpected_dependencies = dependencies - EXPECTED_RUNTIME_DEPENDENCIES
        if unexpected_dependencies:
            raise RuntimeError(f"unexpected Python dependencies: {sorted(unexpected_dependencies)}")
        extras = set(metadata.get_all("Provides-Extra", []))
        if not extras <= EXPECTED_EXTRAS or any("acp" in extra.lower() for extra in extras):
            raise RuntimeError(f"unexpected extras: {sorted(extras)}")

    with tarfile.open(sdists[0], "r:gz") as archive:
        sdist_names = {member.name for member in archive.getmembers()}
        missing = {
            resource
            for resource in REQUIRED_RESOURCES | REQUIRED_SDIST_DOCS
            if not any(name.endswith(f"/{resource}") for name in sdist_names)
        }
        if missing:
            raise RuntimeError(f"sdist is missing resources: {sorted(missing)}")
        _reject_acp(sdist_names, "sdist")


def _reject_acp(names: set[str], kind: str) -> None:
    acp = sorted(name for name in names if "acp" in Path(name).name.lower())
    if acp:
        raise RuntimeError(f"{kind} contains removed ACP files: {acp}")


def _dist_layout_error(
    dist: Path,
    expected_version: str,
    wheels: list[Path],
    sdists: list[Path],
) -> str:
    expected_wheel_prefix = f"kigumi-{expected_version}-"
    expected_sdist_name = f"kigumi-{expected_version}.tar.gz"
    stale = sorted(
        path.name
        for path in [*wheels, *sdists]
        if not (
            (path in wheels and path.name.startswith(expected_wheel_prefix))
            or (path in sdists and path.name == expected_sdist_name)
        )
    )
    stale_note = f"Stale or wrong-version artifacts were found: {stale}." if stale else ""
    return (
        f"{dist} must contain exactly one Kigumi {expected_version} wheel and one sdist; "
        f"found wheels={[path.name for path in wheels]} and "
        f"sdists={[path.name for path in sdists]}. {stale_note} "
        "Existing files are not deleted. Build into a clean temporary directory and pass "
        "it with --dist, for example:\n"
        f'  tmp_dist="$(mktemp -d)" && uv build --out-dir "$tmp_dist" && '
        "uv run python scripts/verify_dist.py "
        f'--dist "$tmp_dist" --expected-version {expected_version}'
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dist",
        type=Path,
        default=Path("dist"),
        help="artifact directory; existing stale files are never deleted",
    )
    parser.add_argument("--expected-version", required=True)
    args = parser.parse_args()
    try:
        verify(args.dist, args.expected_version)
    except RuntimeError as error:
        print(f"verify_dist: {error}", file=sys.stderr)
        return 1
    print(f"verified dist for kigumi {args.expected_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
