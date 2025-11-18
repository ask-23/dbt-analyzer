"""Project loading and manifest parsing for dbt projects."""

import json
from pathlib import Path
from typing import Any, Optional

from dbt_analyzer.models import MaterializationType, Model, ProjectDAG


def parse_model_from_node(node_id: str, node: dict[str, Any]) -> Model:
    """Parse a dbt manifest node into a Model object.

    Args:
        node_id: The unique_id of the node
        node: The node dictionary from the manifest

    Returns:
        A Model object
    """
    # Extract materialization from config
    config = node.get("config", {})
    materialized = config.get("materialized", "view")

    # Map to our MaterializationType enum
    try:
        materialization = MaterializationType(materialized)
    except ValueError:
        # Default to VIEW if unknown materialization
        materialization = MaterializationType.VIEW

    return Model(
        name=node.get("name", ""),
        unique_id=node_id,
        resource_type=node.get("resource_type", "model"),
        path=node.get("path", ""),
        materialization=materialization,
        database=node.get("database"),
        schema=node.get("schema"),
        tags=node.get("tags", []),
        meta=node.get("meta", {}),
        compiled_sql=node.get("compiled_sql"),
        raw_sql=node.get("raw_sql"),
    )


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    """Load a dbt manifest.json file.

    Args:
        manifest_path: Path to the manifest.json file

    Returns:
        The parsed manifest dictionary

    Raises:
        FileNotFoundError: If the manifest file doesn't exist
        json.JSONDecodeError: If the manifest is invalid JSON
    """
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found at {manifest_path}")

    with open(manifest_path) as f:
        return json.load(f)


class DbtProject:
    """Represents a dbt project with loaded manifest and DAG."""

    def __init__(
        self,
        project_path: Path,
        manifest_path: Optional[Path] = None,
        run_results_path: Optional[Path] = None,
    ) -> None:
        """Initialize a DbtProject.

        Args:
            project_path: Path to the dbt project directory
            manifest_path: Optional path to manifest.json (defaults to project_path/manifest.json)
            run_results_path: Optional path to run_results.json
        """
        self.project_path = project_path
        self.manifest_path = manifest_path or (project_path / "manifest.json")
        self.run_results_path = run_results_path
        self.manifest: Optional[dict[str, Any]] = None
        self.dag: Optional[ProjectDAG] = None

    def load(self) -> None:
        """Load the manifest and build the DAG.

        Raises:
            FileNotFoundError: If the manifest file doesn't exist
        """
        from dbt_analyzer.results import load_run_results, merge_run_results_into_dag

        self.manifest = load_manifest(self.manifest_path)
        self.dag = self._build_dag()

        # Load run results if path is provided
        if self.run_results_path and self.run_results_path.exists():
            run_results = load_run_results(self.run_results_path)
            merge_run_results_into_dag(self.dag, run_results)

    def _build_dag(self) -> ProjectDAG:
        """Build the project DAG from the loaded manifest.

        Returns:
            A ProjectDAG object with models and relationships

        Raises:
            ValueError: If manifest is not loaded
        """
        if self.manifest is None:
            raise ValueError("Manifest not loaded. Call load() first.")

        dag = ProjectDAG()

        # First pass: add all model nodes
        nodes = self.manifest.get("nodes", {})
        for node_id, node in nodes.items():
            # Only process model nodes (skip tests, snapshots, etc.)
            if node.get("resource_type") == "model":
                model = parse_model_from_node(node_id, node)
                dag.add_model(model)

        # Second pass: add dependencies
        for node_id, node in nodes.items():
            if node.get("resource_type") == "model":
                depends_on = node.get("depends_on", {})
                parent_nodes = depends_on.get("nodes", [])

                for parent_id in parent_nodes:
                    # Only add edges between models (not sources)
                    if parent_id.startswith("model."):
                        dag.add_dependency(parent_id, node_id)

        # Populate model relationships
        dag.populate_model_relationships()

        return dag

    def get_models(self) -> list[Model]:
        """Get all models in the project.

        Returns:
            List of all Model objects

        Raises:
            ValueError: If DAG is not loaded
        """
        if self.dag is None:
            raise ValueError("DAG not loaded. Call load() first.")

        return list(self.dag.models.values())

    def get_model_by_name(self, name: str) -> Optional[Model]:
        """Get a model by its name.

        Args:
            name: The model name (not unique_id)

        Returns:
            The Model if found, None otherwise

        Raises:
            ValueError: If DAG is not loaded
        """
        if self.dag is None:
            raise ValueError("DAG not loaded. Call load() first.")

        for model in self.dag.models.values():
            if model.name == name:
                return model
        return None
