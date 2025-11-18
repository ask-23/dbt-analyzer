"""Pytest configuration and fixtures."""

import json
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def simple_project_dir(fixtures_dir: Path) -> Path:
    """Return the path to the simple_project fixture."""
    return fixtures_dir / "simple_project"


@pytest.fixture
def simple_manifest(simple_project_dir: Path) -> dict[str, Any]:
    """Load and return the simple_project manifest."""
    with open(simple_project_dir / "manifest.json") as f:
        return json.load(f)


@pytest.fixture
def simple_run_results(simple_project_dir: Path) -> dict[str, Any]:
    """Load and return the simple_project run_results."""
    with open(simple_project_dir / "run_results.json") as f:
        return json.load(f)


@pytest.fixture
def heavy_project_dir(fixtures_dir: Path) -> Path:
    """Return the path to the heavy_non_incremental fixture."""
    return fixtures_dir / "heavy_non_incremental"


@pytest.fixture
def heavy_manifest(heavy_project_dir: Path) -> dict[str, Any]:
    """Load and return the heavy_non_incremental manifest."""
    with open(heavy_project_dir / "manifest.json") as f:
        return json.load(f)


@pytest.fixture
def heavy_run_results(heavy_project_dir: Path) -> dict[str, Any]:
    """Load and return the heavy_non_incremental run_results."""
    with open(heavy_project_dir / "run_results.json") as f:
        return json.load(f)
