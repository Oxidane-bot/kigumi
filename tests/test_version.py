from __future__ import annotations

import tomllib
from pathlib import Path

from kigumi import __version__


def test_version_has_one_package_source() -> None:
    root = Path(__file__).resolve().parents[1]
    with (root / "pyproject.toml").open("rb") as handle:
        project = tomllib.load(handle)

    assert __version__ == "0.7.0"
    assert project["project"]["dynamic"] == ["version"]
    assert "version" not in project["project"]
    assert project["tool"]["hatch"]["version"]["path"] == "kigumi/_version.py"
