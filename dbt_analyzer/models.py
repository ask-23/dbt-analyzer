"""Core data models for dbt pipeline analysis."""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional

import networkx as nx
from pydantic import BaseModel, ConfigDict, Field


class MaterializationType(str, Enum):
    """dbt materialization types."""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"
    SNAPSHOT = "snapshot"
    SEED = "seed"


class Severity(str, Enum):
    """Finding severity levels."""

    INFO = "info"
    WARN = "warn"
    ERROR = "error"


class Model(BaseModel):
    """Represents a dbt model with metadata and performance information."""

    name: str
    unique_id: str
    resource_type: str
    path: str
    materialization: MaterializationType
    database: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    tags: list[str] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)

    # Performance metrics from run_results
    execution_time: Optional[float] = None
    rows_affected: Optional[int] = None
    status: Optional[str] = None

    # DAG relationships (populated after graph construction)
    upstream_models: list[str] = Field(default_factory=list)
    downstream_models: list[str] = Field(default_factory=list)

    # Compiled SQL (optional, from manifest)
    compiled_sql: Optional[str] = None
    raw_sql: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True, use_enum_values=True)

    def __hash__(self) -> int:
        """Make Model hashable for use in sets and as dict keys."""
        return hash(self.unique_id)

    def __eq__(self, other: object) -> bool:
        """Compare models by unique_id."""
        if not isinstance(other, Model):
            return NotImplemented
        return self.unique_id == other.unique_id


class Finding(BaseModel):
    """Represents a single analysis finding/issue."""

    id: str
    severity: Severity
    model_name: str
    title: str
    description: str
    rationale: str
    suggested_action: str
    proposed_changes: Optional[dict[str, Any]] = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(use_enum_values=True)


class Recommendation(BaseModel):
    """High-level recommendation aggregating multiple findings."""

    id: str
    title: str
    description: str
    impact: str
    effort: str
    findings: list[Finding] = Field(default_factory=list)
    code_snippets: list[str] = Field(default_factory=list)
    priority: int = 1

    model_config = ConfigDict(use_enum_values=True)


class ProjectDAG:
    """Represents the dbt project as a directed acyclic graph."""

    def __init__(self) -> None:
        """Initialize an empty project DAG."""
        self.graph: nx.DiGraph = nx.DiGraph()
        self.models: dict[str, Model] = {}

    def add_model(self, model: Model) -> None:
        """Add a model to the DAG.

        Args:
            model: The model to add
        """
        self.models[model.unique_id] = model
        self.graph.add_node(model.unique_id, model=model)

    def add_dependency(self, parent_id: str, child_id: str) -> None:
        """Add a dependency edge from parent to child.

        Args:
            parent_id: The unique_id of the parent (upstream) model
            child_id: The unique_id of the child (downstream) model
        """
        self.graph.add_edge(parent_id, child_id)

    def get_model(self, unique_id: str) -> Optional[Model]:
        """Get a model by its unique_id.

        Args:
            unique_id: The unique_id of the model

        Returns:
            The model if found, None otherwise
        """
        return self.models.get(unique_id)

    def get_upstream(self, unique_id: str) -> list[str]:
        """Get all upstream (parent) model IDs for a given model.

        Args:
            unique_id: The unique_id of the model

        Returns:
            List of upstream model unique_ids
        """
        if unique_id not in self.graph:
            return []
        return list(self.graph.predecessors(unique_id))

    def get_downstream(self, unique_id: str) -> list[str]:
        """Get all downstream (child) model IDs for a given model.

        Args:
            unique_id: The unique_id of the model

        Returns:
            List of downstream model unique_ids
        """
        if unique_id not in self.graph:
            return []
        return list(self.graph.successors(unique_id))

    def get_all_upstream(self, unique_id: str) -> set[str]:
        """Get all transitive upstream dependencies for a model.

        Args:
            unique_id: The unique_id of the model

        Returns:
            Set of all upstream model unique_ids (transitive closure)
        """
        if unique_id not in self.graph:
            return set()
        return nx.ancestors(self.graph, unique_id)

    def get_all_downstream(self, unique_id: str) -> set[str]:
        """Get all transitive downstream dependents for a model.

        Args:
            unique_id: The unique_id of the model

        Returns:
            Set of all downstream model unique_ids (transitive closure)
        """
        if unique_id not in self.graph:
            return set()
        return nx.descendants(self.graph, unique_id)

    def get_path_length(self, source_id: str, target_id: str) -> Optional[int]:
        """Get the shortest path length between two models.

        Args:
            source_id: The unique_id of the source model
            target_id: The unique_id of the target model

        Returns:
            The path length if a path exists, None otherwise
        """
        try:
            return nx.shortest_path_length(self.graph, source_id, target_id)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None

    def get_longest_path_from(self, unique_id: str) -> int:
        """Get the length of the longest path from a model to any leaf.

        Args:
            unique_id: The unique_id of the model

        Returns:
            The length of the longest path
        """
        if unique_id not in self.graph:
            return 0

        descendants = self.get_all_downstream(unique_id)
        if not descendants:
            return 0

        max_length = 0
        for desc in descendants:
            try:
                path_len = nx.shortest_path_length(self.graph, unique_id, desc)
                max_length = max(max_length, path_len)
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                continue

        return max_length

    def populate_model_relationships(self) -> None:
        """Populate upstream/downstream relationships in all models."""
        for unique_id, model in self.models.items():
            model.upstream_models = self.get_upstream(unique_id)
            model.downstream_models = self.get_downstream(unique_id)
