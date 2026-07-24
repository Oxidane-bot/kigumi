#!/usr/bin/env python3
"""Verify the built wheel/sdist release contract using only the standard library."""

from __future__ import annotations

import argparse
import email
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


def _dependency_name(requirement: str) -> str:
    for separator in (" ", ";", "<", ">", "=", "!", "~", "["):
        requirement = requirement.split(separator, 1)[0]
    return requirement.strip().lower().replace("_", "-")


def verify(dist: Path, expected_version: str) -> None:
    wheels = sorted(dist.glob("kigumi-*.whl"))
    sdists = sorted(dist.glob("kigumi-*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise RuntimeError("dist must contain exactly one Kigumi wheel and one sdist")

    with zipfile.ZipFile(wheels[0]) as archive:
        wheel_names = set(archive.namelist())
        missing = REQUIRED_RESOURCES - wheel_names
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
            for resource in REQUIRED_RESOURCES
            if not any(name.endswith(f"/{resource}") for name in sdist_names)
        }
        if missing:
            raise RuntimeError(f"sdist is missing resources: {sorted(missing)}")
        _reject_acp(sdist_names, "sdist")


def _reject_acp(names: set[str], kind: str) -> None:
    acp = sorted(name for name in names if "acp" in Path(name).name.lower())
    if acp:
        raise RuntimeError(f"{kind} contains removed ACP files: {acp}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist", type=Path, default=Path("dist"))
    parser.add_argument("--expected-version", required=True)
    args = parser.parse_args()
    verify(args.dist, args.expected_version)
    print(f"verified dist for kigumi {args.expected_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
