"""Tests for the package version."""

import tomllib
from pathlib import Path

import edda


def test_version_matches_pyproject():
    """Test that __version__ follows the version in pyproject.toml."""
    pyproject = Path(__file__).parent.parent / "pyproject.toml"
    project = tomllib.loads(pyproject.read_text())["project"]
    assert edda.__version__ == project["version"]
