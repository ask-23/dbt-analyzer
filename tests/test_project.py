"""Tests for project loading and manifest parsing."""

from pathlib import Path
from typing import Any

import pytest

from dbt_analyzer.models import MaterializationType, Model, ProjectDAG
from dbt_analyzer.project import DbtProject, load_manifest, parse_model_from_node


def test_parse_model_from_node(simple_manifest: dict[str, Any]) -> None:
    """Test parsing a single model from manifest node."""
    node_id = "model.my_project.stg_customers"
    node = simple_manifest["nodes"][node_id]

    model = parse_model_from_node(node_id, node)

    assert model.name == "stg_customers"
    assert model.unique_id == node_id
    assert model.resource_type == "model"
    assert model.path == "staging/stg_customers.sql"
    assert model.materialization == MaterializationType.VIEW
    assert model.database == "analytics"
    assert model.schema_ == "staging"
    assert model.compiled_sql == "SELECT * FROM raw.customers"


def test_parse_model_with_tags(simple_manifest: dict[str, Any]) -> None:
    """Test parsing a model with tags."""
    node_id = "model.my_project.fct_orders"
    node = simple_manifest["nodes"][node_id]

    model = parse_model_from_node(node_id, node)

    assert model.name == "fct_orders"
    assert model.materialization == MaterializationType.TABLE
    assert "marts" in model.tags


def test_load_manifest(simple_project_dir: Path) -> None:
    """Test loading a complete manifest."""
    manifest = load_manifest(simple_project_dir / "manifest.json")

    assert "metadata" in manifest
    assert "nodes" in manifest
    assert len(manifest["nodes"]) == 4


def test_dbt_project_initialization(simple_project_dir: Path) -> None:
    """Test initializing a DbtProject."""
    project = DbtProject(project_path=simple_project_dir)

    assert project.project_path == simple_project_dir
    assert project.manifest_path == simple_project_dir / "manifest.json"
    assert project.manifest is None
    assert project.dag is None


def test_dbt_project_load_manifest(simple_project_dir: Path) -> None:
    """Test loading manifest into DbtProject."""
    project = DbtProject(project_path=simple_project_dir)
    project.load()

    assert project.manifest is not None
    assert project.dag is not None
    assert len(project.dag.models) == 4

    # Check specific models exist
    stg_customers = project.dag.get_model("model.my_project.stg_customers")
    assert stg_customers is not None
    assert stg_customers.name == "stg_customers"
    assert stg_customers.materialization == MaterializationType.VIEW


def test_dbt_project_build_dag(simple_project_dir: Path) -> None:
    """Test that DAG is built correctly with relationships."""
    project = DbtProject(project_path=simple_project_dir)
    project.load()

    # Check upstream relationships
    fct_orders = project.dag.get_model("model.my_project.fct_orders")
    assert fct_orders is not None

    upstream = project.dag.get_upstream("model.my_project.fct_orders")
    assert len(upstream) == 2
    assert "model.my_project.stg_orders" in upstream
    assert "model.my_project.stg_customers" in upstream

    # Check downstream relationships
    stg_customers = project.dag.get_model("model.my_project.stg_customers")
    assert stg_customers is not None

    downstream = project.dag.get_downstream("model.my_project.stg_customers")
    assert len(downstream) == 1
    assert "model.my_project.fct_orders" in downstream


def test_dbt_project_identify_unused_models(simple_project_dir: Path) -> None:
    """Test identifying models with no downstream dependents."""
    project = DbtProject(project_path=simple_project_dir)
    project.load()

    # unused_model should have no downstream dependents
    unused = project.dag.get_model("model.my_project.unused_model")
    assert unused is not None

    downstream = project.dag.get_downstream("model.my_project.unused_model")
    assert len(downstream) == 0

    # fct_orders also has no downstream (it's a leaf)
    fct_downstream = project.dag.get_downstream("model.my_project.fct_orders")
    assert len(fct_downstream) == 0


def test_dbt_project_custom_manifest_path(simple_project_dir: Path) -> None:
    """Test loading with custom manifest path."""
    manifest_path = simple_project_dir / "manifest.json"
    project = DbtProject(
        project_path=simple_project_dir,
        manifest_path=manifest_path
    )
    project.load()

    assert project.manifest_path == manifest_path
    assert project.manifest is not None
    assert len(project.dag.models) == 4


def test_dbt_project_missing_manifest() -> None:
    """Test that loading fails gracefully with missing manifest."""
    project = DbtProject(project_path=Path("/nonexistent"))

    with pytest.raises(FileNotFoundError):
        project.load()
