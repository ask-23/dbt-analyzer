"""Run results integration for dbt projects."""

import json
from pathlib import Path
from typing import Any, Optional

from dbt_analyzer.models import ProjectDAG


def load_run_results(run_results_path: Path) -> dict[str, Any]:
    """Load a dbt run_results.json file.

    Args:
        run_results_path: Path to the run_results.json file

    Returns:
        The parsed run_results dictionary

    Raises:
        FileNotFoundError: If the run_results file doesn't exist
        json.JSONDecodeError: If the run_results is invalid JSON
    """
    if not run_results_path.exists():
        raise FileNotFoundError(f"Run results not found at {run_results_path}")

    with open(run_results_path) as f:
        return json.load(f)


def merge_run_results_into_dag(
    dag: ProjectDAG,
    run_results: dict[str, Any]
) -> None:
    """Merge run_results performance data into the DAG models.

    Args:
        dag: The ProjectDAG to update
        run_results: The run_results dictionary
    """
    results = run_results.get("results", [])

    for result in results:
        unique_id = result.get("unique_id")
        if not unique_id:
            continue

        model = dag.get_model(unique_id)
        if model is None:
            # Result for a model not in our DAG (could be test, snapshot, etc.)
            continue

        # Merge performance data
        model.execution_time = result.get("execution_time")
        model.status = result.get("status")

        # Extract rows_affected from adapter_response
        adapter_response = result.get("adapter_response", {})
        model.rows_affected = adapter_response.get("rows_affected")
