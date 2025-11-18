"""Tests for run_results integration."""

from pathlib import Path
from typing import Any

import pytest

from dbt_analyzer.project import DbtProject
from dbt_analyzer.results import load_run_results, merge_run_results_into_dag


def test_load_run_results(simple_project_dir: Path) -> None:
    """Test loading run_results.json."""
    run_results = load_run_results(simple_project_dir / "run_results.json")

    assert "metadata" in run_results
    assert "results" in run_results
    assert len(run_results["results"]) == 4


def test_merge_run_results_simple(
    simple_project_dir: Path,
    simple_run_results: dict[str, Any]
) -> None:
    """Test merging run_results into DAG."""
    project = DbtProject(project_path=simple_project_dir)
    project.load()

    merge_run_results_into_dag(project.dag, simple_run_results)

    # Check that execution times were added
    stg_customers = project.dag.get_model("model.my_project.stg_customers")
    assert stg_customers is not None
    assert stg_customers.execution_time == 2.5
    assert stg_customers.rows_affected == 1000
    assert stg_customers.status == "success"


def test_merge_run_results_heavy(
    heavy_project_dir: Path,
    heavy_run_results: dict[str, Any]
) -> None:
    """Test merging run_results for heavy models."""
    project = DbtProject(project_path=heavy_project_dir)
    project.load()

    merge_run_results_into_dag(project.dag, heavy_run_results)

    # Check heavy model has performance data
    heavy_model = project.dag.get_model("model.my_project.heavy_table_model")
    assert heavy_model is not None
    assert heavy_model.execution_time == 425.3
    assert heavy_model.rows_affected == 2500000
    assert heavy_model.status == "success"

    # Check slow view model
    slow_view = project.dag.get_model("model.my_project.slow_view_model")
    assert slow_view is not None
    assert slow_view.execution_time == 387.2
    assert slow_view.rows_affected == 1800000


def test_merge_run_results_missing_file() -> None:
    """Test that loading missing run_results raises error."""
    with pytest.raises(FileNotFoundError):
        load_run_results(Path("/nonexistent/run_results.json"))


def test_merge_run_results_partial_data(simple_project_dir: Path) -> None:
    """Test merging run_results when some models don't have results."""
    project = DbtProject(project_path=simple_project_dir)
    project.load()

    # Create partial run_results with only one model
    partial_results = {
        "metadata": {"dbt_version": "1.7.0"},
        "results": [
            {
                "unique_id": "model.my_project.stg_customers",
                "status": "success",
                "execution_time": 5.0,
                "adapter_response": {"rows_affected": 500}
            }
        ]
    }

    merge_run_results_into_dag(project.dag, partial_results)

    # Check that the one model has data
    stg_customers = project.dag.get_model("model.my_project.stg_customers")
    assert stg_customers is not None
    assert stg_customers.execution_time == 5.0
    assert stg_customers.rows_affected == 500

    # Check that other models don't have performance data
    stg_orders = project.dag.get_model("model.my_project.stg_orders")
    assert stg_orders is not None
    assert stg_orders.execution_time is None
    assert stg_orders.rows_affected is None


def test_dbt_project_load_with_run_results(simple_project_dir: Path) -> None:
    """Test loading project with run_results via DbtProject."""
    project = DbtProject(
        project_path=simple_project_dir,
        run_results_path=simple_project_dir / "run_results.json"
    )
    project.load()

    # Verify performance data was loaded
    fct_orders = project.dag.get_model("model.my_project.fct_orders")
    assert fct_orders is not None
    assert fct_orders.execution_time == 125.7
    assert fct_orders.rows_affected == 50000
