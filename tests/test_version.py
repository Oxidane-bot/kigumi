from __future__ import annotations

import email
import os
import re
import subprocess
import tarfile
import tomllib
import zipfile
from pathlib import Path

import pytest

from kigumi import __version__

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_RELEASE_VERSION = "0.14.0"
EXPECTED_RELEASE_DATE = "2026-08-12"
RELEASE_HEADING_PATTERN = re.compile(
    r"^## \[(?P<version>\d+\.\d+\.\d+)\](?: - (?P<date>\d{4}-\d{2}-\d{2}))?$",
    re.MULTILINE,
)


def _release_versions() -> list[str]:
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    return [match.group("version") for match in RELEASE_HEADING_PATTERN.finditer(changelog)]


def _exact_head_tag() -> str | None:
    result = subprocess.run(
        ["git", "describe", "--tags", "--exact-match", "HEAD"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _worktree_is_dirty() -> bool:
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def test_version_has_one_package_source() -> None:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        project = tomllib.load(handle)

    version_source = (ROOT / "kigumi" / "_version.py").read_text(encoding="utf-8")
    source_match = re.search(r'^__version__ = "(\d+\.\d+\.\d+)"$', version_source, re.MULTILINE)
    assert source_match is not None
    assert source_match.group(1) == EXPECTED_RELEASE_VERSION
    assert __version__ == source_match.group(1)
    assert project["project"]["dynamic"] == ["version"]
    assert "version" not in project["project"]
    assert project["tool"]["hatch"]["version"]["path"] == "kigumi/_version.py"


def test_latest_changelog_release_matches_package_version() -> None:
    releases = _release_versions()
    assert releases, "CHANGELOG.md must contain at least one dated release"
    assert releases[0] == __version__, "the newest dated release must be the package version"


def test_release_candidate_identity_is_explicit_and_unreleased_records_changes() -> None:
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert __version__ == EXPECTED_RELEASE_VERSION
    unreleased = re.search(
        rf"^## \[Unreleased\]\n(?P<body>.*?)^## \[{re.escape(EXPECTED_RELEASE_VERSION)}\] - "
        rf"{re.escape(EXPECTED_RELEASE_DATE)}$",
        changelog,
        re.MULTILINE | re.DOTALL,
    )
    assert unreleased is not None, "the release candidate must have an explicit dated section"
    assert unreleased.group("body").strip(), "[Unreleased] must record pending user-visible changes"


def test_revised_contracts_are_indexed_for_their_latest_release() -> None:
    """Materially revised contracts identify the release that last changed them."""
    index = (ROOT / "docs" / "contracts" / "README.md").read_text(encoding="utf-8")
    expected_versions = {
        "admission.md": "0.14.0",
        "agent-node.md": "0.14.0",
        "cache-key.md": "0.14.0",
        "determinism.md": "0.14.0",
        "guards.md": "0.14.0",
        "prompt-resolution.md": "0.13.0",
        "retry-resume.md": "0.14.0",
    }
    for filename, contract_version in expected_versions.items():
        document = (ROOT / "docs" / "contracts" / filename).read_text(encoding="utf-8")
        status = re.search(r"^Status: (Active \(\d+\.\d+\.\d+\))$", document, re.MULTILINE)
        assert status is not None
        assert status.group(1) == f"Active ({contract_version})"
        assert re.search(
            rf"\]\({re.escape(filename)}\) \| Active \({re.escape(contract_version)}\) \|",
            index,
        ), f"{filename} must be indexed with its {contract_version} revision"


def test_historical_schema_boundaries_remain_explicit() -> None:
    """0.14 保留 prompt schema-1，并把已经发布的 admission 契约提升为 Active。"""
    prompt_contract = (ROOT / "docs" / "contracts" / "prompt-resolution.md").read_text(
        encoding="utf-8"
    )
    admission_contract = (ROOT / "docs" / "contracts" / "admission.md").read_text(encoding="utf-8")
    contract_index = (ROOT / "docs" / "contracts" / "README.md").read_text(encoding="utf-8")

    assert "prompt_resolution_schema=1" in prompt_contract
    assert "Status: Active (0.14.0)" in admission_contract
    assert "[执行准入契约](admission.md) | Active (0.14.0)" in contract_index


def test_exact_head_tag_matches_package_version_when_present() -> None:
    tag = _exact_head_tag()
    if tag is None:
        pytest.skip("this checkout has no exact tag on HEAD (normal for untagged PR commits)")
    if tag != f"v{__version__}" and _worktree_is_dirty():
        pytest.skip("this uncommitted release candidate intentionally has no new tag")
    assert tag == f"v{__version__}"


def test_built_artifacts_match_package_version_when_requested() -> None:
    raw_dist_dir = os.environ.get("KIGUMI_DIST_DIR")
    if not raw_dist_dir:
        pytest.skip("set KIGUMI_DIST_DIR to validate freshly built wheel and sdist metadata")

    dist_dir = Path(raw_dist_dir)
    assert dist_dir.is_dir(), f"artifact directory does not exist: {dist_dir}"
    wheels = sorted(dist_dir.glob("kigumi-*.whl"))
    sdists = sorted(dist_dir.glob("kigumi-*.tar.gz"))
    assert len(wheels) == 1, f"expected one kigumi wheel, found {wheels}"
    assert len(sdists) == 1, f"expected one kigumi sdist, found {sdists}"

    wheel_match = re.fullmatch(r"kigumi-(?P<version>\d+\.\d+\.\d+)-.+\.whl", wheels[0].name)
    assert wheel_match is not None, f"unexpected wheel filename: {wheels[0].name}"
    assert wheel_match.group("version") == __version__
    with zipfile.ZipFile(wheels[0]) as archive:
        metadata_names = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        assert len(metadata_names) == 1
        metadata = email.message_from_bytes(archive.read(metadata_names[0]))
    assert metadata["Name"] == "kigumi"
    assert metadata["Version"] == __version__

    sdist_match = re.fullmatch(r"kigumi-(?P<version>\d+\.\d+\.\d+)\.tar\.gz", sdists[0].name)
    assert sdist_match is not None, f"unexpected sdist filename: {sdists[0].name}"
    assert sdist_match.group("version") == __version__
    with tarfile.open(sdists[0], "r:gz") as archive:
        metadata_members = [
            member for member in archive.getmembers() if member.name.endswith("/PKG-INFO")
        ]
        assert len(metadata_members) == 1
        extracted = archive.extractfile(metadata_members[0])
        assert extracted is not None
        metadata = email.message_from_binary_file(extracted)
    assert metadata["Name"] == "kigumi"
    assert metadata["Version"] == __version__
