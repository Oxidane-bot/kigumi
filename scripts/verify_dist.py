#!/usr/bin/env python3
"""Verify the built wheel/sdist release contract using only the standard library."""

from __future__ import annotations

import argparse
import email
import re
import sys
import tarfile
import zipfile
from pathlib import Path

EXPECTED_RUNTIME_DEPENDENCIES = frozenset({"pydantic"})
EXPECTED_OPTIONAL_DEPENDENCIES = frozenset({"litellm", "pytest", "ruff"})
EXPECTED_DEPENDENCIES = EXPECTED_RUNTIME_DEPENDENCIES | EXPECTED_OPTIONAL_DEPENDENCIES
EXPECTED_EXTRAS = frozenset({"dev", "litellm"})
EXTRA_MARKER = re.compile(r"\bextra\s*(?:==|!=|in\b|not\s+in\b)", re.IGNORECASE)
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


def _is_extra_dependency(requirement: str) -> bool:
    marker = requirement.partition(";")[2]
    return bool(EXTRA_MARKER.search(marker))


def verify(dist: Path, expected_version: str) -> None:
    wheels = sorted(dist.glob("kigumi-*.whl"))
    sdists = sorted(dist.glob("kigumi-*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise RuntimeError(_dist_layout_error(dist, expected_version, wheels, sdists))
    _verify_artifact_names(wheels[0], sdists[0], expected_version)

    with zipfile.ZipFile(wheels[0]) as archive:
        wheel_names = set(archive.namelist())
        missing = (REQUIRED_RESOURCES | REQUIRED_WHEEL_DOCS) - wheel_names
        if missing:
            raise RuntimeError(f"wheel is missing resources: {sorted(missing)}")
        _reject_acp(wheel_names, "wheel")
        metadata_names = [name for name in wheel_names if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise RuntimeError(f"expected one wheel METADATA, found {len(metadata_names)}")
        metadata_name = metadata_names[0]
        metadata = email.message_from_bytes(archive.read(metadata_name))
        _verify_metadata(metadata, expected_version, "wheel")

    with tarfile.open(sdists[0], "r:gz") as archive:
        members = archive.getmembers()
        sdist_names = {member.name for member in members}
        missing = {
            resource
            for resource in REQUIRED_RESOURCES | REQUIRED_SDIST_DOCS
            if not any(name.endswith(f"/{resource}") for name in sdist_names)
        }
        if missing:
            raise RuntimeError(f"sdist is missing resources: {sorted(missing)}")
        metadata_members = [member for member in members if member.name.endswith("/PKG-INFO")]
        if len(metadata_members) != 1:
            raise RuntimeError(f"expected one sdist PKG-INFO, found {len(metadata_members)}")
        metadata_file = archive.extractfile(metadata_members[0])
        if metadata_file is None:
            raise RuntimeError("sdist PKG-INFO is not readable")
        metadata = email.message_from_binary_file(metadata_file)
        _verify_metadata(metadata, expected_version, "sdist")
        _reject_acp(sdist_names, "sdist")


def _verify_metadata(metadata: email.message.Message, expected_version: str, kind: str) -> None:
    name = metadata["Name"]
    if name != "kigumi":
        raise RuntimeError(f"{kind} metadata Name {name!r} != 'kigumi'")
    version = metadata["Version"]
    if version != expected_version:
        raise RuntimeError(f"{kind} metadata Version {version!r} != {expected_version!r}")

    requirements = metadata.get_all("Requires-Dist", [])
    runtime_dependencies = {
        _dependency_name(requirement)
        for requirement in requirements
        if not _is_extra_dependency(requirement)
    }
    if runtime_dependencies != EXPECTED_RUNTIME_DEPENDENCIES:
        raise RuntimeError(
            f"{kind} runtime dependencies {sorted(runtime_dependencies)} != "
            f"{sorted(EXPECTED_RUNTIME_DEPENDENCIES)}"
        )
    dependencies = {_dependency_name(requirement) for requirement in requirements}
    if dependencies != EXPECTED_DEPENDENCIES:
        raise RuntimeError(
            f"{kind} Python dependencies {sorted(dependencies)} != {sorted(EXPECTED_DEPENDENCIES)}"
        )

    extras = set(metadata.get_all("Provides-Extra", []))
    if extras != EXPECTED_EXTRAS:
        raise RuntimeError(f"{kind} extras {sorted(extras)} != {sorted(EXPECTED_EXTRAS)}")


def _verify_artifact_names(wheel: Path, sdist: Path, expected_version: str) -> None:
    expected_wheel_prefix = f"kigumi-{expected_version}-"
    expected_sdist_name = f"kigumi-{expected_version}.tar.gz"
    if not wheel.name.startswith(expected_wheel_prefix) or not wheel.name.endswith(".whl"):
        raise RuntimeError(
            f"unexpected wheel filename {wheel.name!r}; expected prefix {expected_wheel_prefix!r}"
        )
    if sdist.name != expected_sdist_name:
        raise RuntimeError(
            f"unexpected sdist filename {sdist.name!r}; expected {expected_sdist_name!r}"
        )


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
