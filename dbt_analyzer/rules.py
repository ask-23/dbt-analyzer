"""Analysis rules for dbt projects."""

from dataclasses import dataclass
from typing import Callable

from dbt_analyzer.models import Finding, MaterializationType, ProjectDAG, Severity


@dataclass
class RuleConfig:
    """Configuration for analysis rules."""

    # Thresholds for heavy model detection
    min_execution_time_seconds: float = 60.0
    min_rows_for_heavy: int = 100000

    # Thresholds for dependency analysis
    max_dependency_depth: int = 8
    min_downstream_count: int = 3

    # Whether to include leaf models in dead model detection
    include_leaf_models_as_dead: bool = True


def check_heavy_non_incremental_models(
    dag: ProjectDAG,
    config: RuleConfig
) -> list[Finding]:
    """Identify heavy models that should be incremental.

    Args:
        dag: The project DAG
        config: Rule configuration

    Returns:
        List of findings for heavy non-incremental models
    """
    findings: list[Finding] = []

    for model in dag.models.values():
        # Skip if already incremental
        if model.materialization == MaterializationType.INCREMENTAL:
            continue

        # Skip if we don't have performance data
        if model.execution_time is None and model.rows_affected is None:
            continue

        # Check if model is "heavy"
        is_slow = (
            model.execution_time is not None
            and model.execution_time >= config.min_execution_time_seconds
        )
        is_large = (
            model.rows_affected is not None
            and model.rows_affected >= config.min_rows_for_heavy
        )

        if is_slow or is_large:
            metadata = {
                "execution_time": model.execution_time,
                "rows_affected": model.rows_affected,
                "current_materialization": model.materialization,
            }

            finding = Finding(
                id="HEAVY_NON_INCREMENTAL_MODEL",
                severity=Severity.WARN,
                model_name=model.name,
                title=f"Heavy model '{model.name}' should consider incremental materialization",
                description=(
                    f"Model '{model.name}' is materialized as '{model.materialization}' "
                    f"but has significant size/execution time "
                    f"(execution: {model.execution_time}s, rows: {model.rows_affected}). "
                    f"Consider using incremental materialization."
                ),
                rationale=(
                    "Large or slow-running models benefit from incremental materialization, "
                    "which only processes new or changed records instead of rebuilding the "
                    "entire table on each run. This can significantly reduce compute costs "
                    "and runtime."
                ),
                suggested_action=(
                    f"Convert '{model.name}' to incremental materialization. "
                    "Add `config(materialized='incremental', unique_key='your_key')` "
                    "to the model and implement `is_incremental()` logic to filter for new records."
                ),
                proposed_changes={
                    "materialization": "incremental",
                    "requires_unique_key": True,
                    "requires_incremental_logic": True,
                },
                metadata=metadata,
            )
            findings.append(finding)

    return findings


def check_dead_models(dag: ProjectDAG, config: RuleConfig) -> list[Finding]:
    """Identify unused models with no downstream dependents.

    Args:
        dag: The project DAG
        config: Rule configuration

    Returns:
        List of findings for dead/unused models
    """
    findings: list[Finding] = []

    for model in dag.models.values():
        downstream = dag.get_downstream(model.unique_id)

        if len(downstream) == 0:
            # This model has no downstream dependents
            # It might be a legitimate leaf model or truly unused

            metadata = {
                "downstream_count": 0,
                "upstream_count": len(dag.get_upstream(model.unique_id)),
            }

            finding = Finding(
                id="DEAD_MODEL",
                severity=Severity.INFO,
                model_name=model.name,
                title=f"Model '{model.name}' has no downstream dependents",
                description=(
                    f"Model '{model.name}' is not referenced by any other models. "
                    f"It may be unused or a legitimate end-point (dashboard, export, etc.)."
                ),
                rationale=(
                    "Models with no downstream dependents may be: (1) legitimate end-points "
                    "consumed by BI tools or exports, (2) work-in-progress models, or "
                    "(3) truly dead code that should be removed. Review to determine which case applies."
                ),
                suggested_action=(
                    f"Review model '{model.name}' to determine if it's still needed. "
                    "If it's consumed externally (BI tool, data export), consider adding "
                    "it to an exposure. If unused, consider archiving or removing it."
                ),
                metadata=metadata,
            )
            findings.append(finding)

    return findings


def check_deep_dependency_chains(
    dag: ProjectDAG,
    config: RuleConfig
) -> list[Finding]:
    """Identify models with deep dependency chains.

    Args:
        dag: The project DAG
        config: Rule configuration

    Returns:
        List of findings for models with deep dependency chains
    """
    findings: list[Finding] = []

    for model in dag.models.values():
        # Calculate the depth of the deepest upstream chain
        all_upstream = dag.get_all_upstream(model.unique_id)
        max_depth = len(all_upstream)

        # Also check longest path from this model
        longest_downstream = dag.get_longest_path_from(model.unique_id)

        if max_depth > config.max_dependency_depth:
            metadata = {
                "upstream_depth": max_depth,
                "downstream_depth": longest_downstream,
                "total_upstream_models": len(all_upstream),
            }

            finding = Finding(
                id="DEEP_DEP_CHAIN",
                severity=Severity.WARN,
                model_name=model.name,
                title=f"Model '{model.name}' has deep dependency chain",
                description=(
                    f"Model '{model.name}' has {max_depth} upstream dependencies, "
                    f"exceeding the recommended maximum of {config.max_dependency_depth}. "
                    f"Deep dependency chains can make debugging difficult and increase fragility."
                ),
                rationale=(
                    "Deep dependency chains make it harder to understand data lineage, "
                    "debug issues, and modify models without breaking downstream dependencies. "
                    "Consider consolidating intermediate transformations or introducing "
                    "strategic materialization points."
                ),
                suggested_action=(
                    f"Review the dependency chain for '{model.name}'. Consider: "
                    "(1) consolidating some intermediate models, "
                    "(2) materializing key intermediate models as tables for better performance, "
                    "or (3) refactoring the transformation logic."
                ),
                metadata=metadata,
            )
            findings.append(finding)

    return findings


def check_fan_out_heavy_models(
    dag: ProjectDAG,
    config: RuleConfig
) -> list[Finding]:
    """Identify heavy models with many downstream dependents.

    Args:
        dag: The project DAG
        config: Rule configuration

    Returns:
        List of findings for heavy models with high fan-out
    """
    findings: list[Finding] = []

    for model in dag.models.values():
        # Skip if no execution time data
        if model.execution_time is None:
            continue

        # Check if model is heavy
        is_heavy = model.execution_time >= config.min_execution_time_seconds

        # Check downstream count
        downstream = dag.get_downstream(model.unique_id)
        downstream_count = len(downstream)

        if is_heavy and downstream_count >= config.min_downstream_count:
            metadata = {
                "execution_time": model.execution_time,
                "downstream_count": downstream_count,
                "downstream_models": downstream,
            }

            finding = Finding(
                id="FAN_OUT_HEAVY_MODEL",
                severity=Severity.ERROR,
                model_name=model.name,
                title=f"Critical bottleneck: '{model.name}' is slow with {downstream_count} dependents",
                description=(
                    f"Model '{model.name}' takes {model.execution_time}s to run and has "
                    f"{downstream_count} downstream dependents. This is a critical bottleneck "
                    f"that affects many downstream models."
                ),
                rationale=(
                    "Models that are both slow and heavily depended upon create bottlenecks "
                    "in the DAG. Optimizing these models has the highest impact on overall "
                    "pipeline performance and developer productivity."
                ),
                suggested_action=(
                    f"Prioritize optimizing '{model.name}'. Consider: "
                    "(1) adding indexes or optimizing SQL, "
                    "(2) converting to incremental materialization, "
                    "(3) pre-aggregating data, or "
                    "(4) splitting into smaller, focused models."
                ),
                metadata=metadata,
            )
            findings.append(finding)

    return findings


def run_all_rules(dag: ProjectDAG, config: RuleConfig) -> list[Finding]:
    """Run all analysis rules on a DAG.

    Args:
        dag: The project DAG
        config: Rule configuration

    Returns:
        List of all findings from all rules
    """
    all_findings: list[Finding] = []

    rules: list[Callable[[ProjectDAG, RuleConfig], list[Finding]]] = [
        check_heavy_non_incremental_models,
        check_dead_models,
        check_deep_dependency_chains,
        check_fan_out_heavy_models,
    ]

    for rule_func in rules:
        findings = rule_func(dag, config)
        all_findings.extend(findings)

    return all_findings
