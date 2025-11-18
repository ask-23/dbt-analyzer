"""Recommendations layer for dbt analysis."""

from collections import defaultdict

from dbt_analyzer.models import Finding, Recommendation, Severity


def _generate_incremental_snippet(model_name: str) -> str:
    """Generate a code snippet for converting a model to incremental.

    Args:
        model_name: The name of the model

    Returns:
        A code snippet showing how to configure incremental materialization
    """
    return f"""
-- In models/.../{ model_name}.sql
{{{{
  config(
    materialized='incremental',
    unique_key='id',  -- Replace with your actual unique key
    on_schema_change='fail'
  )
}}}}

SELECT
  *
FROM source_table
{{% if is_incremental() %}}
  -- This filter will only run on incremental runs
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{{{ this }}}})
{{% endif %}}
""".strip()


def generate_recommendations(findings: list[Finding]) -> list[Recommendation]:
    """Generate high-level recommendations from findings.

    Args:
        findings: List of findings from analysis rules

    Returns:
        List of recommendations, sorted by priority (descending)
    """
    if not findings:
        return []

    recommendations: list[Recommendation] = []

    # Group findings by type
    findings_by_type: dict[str, list[Finding]] = defaultdict(list)
    for finding in findings:
        findings_by_type[finding.id].append(finding)

    # Generate recommendations for each finding type
    if "HEAVY_NON_INCREMENTAL_MODEL" in findings_by_type:
        heavy_findings = findings_by_type["HEAVY_NON_INCREMENTAL_MODEL"]

        # Sort by execution time + rows to prioritize worst offenders
        sorted_findings = sorted(
            heavy_findings,
            key=lambda f: (
                f.metadata.get("execution_time") or 0,
                f.metadata.get("rows_affected") or 0
            ),
            reverse=True
        )

        # Generate code snippets for top models
        snippets = []
        for finding in sorted_findings[:3]:  # Top 3 models
            snippet = _generate_incremental_snippet(finding.model_name)
            snippets.append(snippet)

        rec = Recommendation(
            id="REC_INCREMENTALIZE_HEAVY_MODELS",
            title="Convert Heavy Models to Incremental Materialization",
            description=(
                f"Found {len(heavy_findings)} models that would benefit from "
                f"incremental materialization. These models are slow or process "
                f"large datasets but currently rebuild completely on each run."
            ),
            impact=(
                "HIGH - Incremental materialization can reduce run times by 80-95% "
                "for large tables that receive regular updates. This directly reduces "
                "compute costs and enables more frequent data refreshes."
            ),
            effort=(
                "MEDIUM - Requires adding incremental config and implementing "
                "is_incremental() logic to filter for new/changed records. "
                "Testing is critical to ensure data correctness."
            ),
            findings=heavy_findings,
            code_snippets=snippets,
            priority=10,  # Highest priority
        )
        recommendations.append(rec)

    if "FAN_OUT_HEAVY_MODEL" in findings_by_type:
        fanout_findings = findings_by_type["FAN_OUT_HEAVY_MODEL"]

        # Sort by downstream count * execution time
        sorted_findings = sorted(
            fanout_findings,
            key=lambda f: (
                f.metadata.get("downstream_count", 0)
                * (f.metadata.get("execution_time") or 0)
            ),
            reverse=True
        )

        top_bottlenecks = sorted_findings[:5]
        model_list = "\n".join([f"- {f.model_name}" for f in top_bottlenecks])

        rec = Recommendation(
            id="REC_OPTIMIZE_BOTTLENECK_MODELS",
            title="Optimize Critical Bottleneck Models",
            description=(
                f"Found {len(fanout_findings)} models that are both slow and heavily "
                f"depended upon. These create pipeline bottlenecks affecting many "
                f"downstream models.\n\nTop bottlenecks:\n{model_list}"
            ),
            impact=(
                "CRITICAL - These bottlenecks affect the entire pipeline. Optimizing "
                "them improves build times for all downstream models and enables "
                "parallel execution."
            ),
            effort=(
                "HIGH - Requires SQL optimization, potentially adding indexes, "
                "converting to incremental, or architectural changes."
            ),
            findings=fanout_findings,
            code_snippets=[],
            priority=9,  # Very high priority
        )
        recommendations.append(rec)

    if "DEAD_MODEL" in findings_by_type:
        dead_findings = findings_by_type["DEAD_MODEL"]

        model_list = "\n".join([f"- {f.model_name}" for f in dead_findings[:10]])

        rec = Recommendation(
            id="REC_REVIEW_UNUSED_MODELS",
            title="Review and Clean Up Unused Models",
            description=(
                f"Found {len(dead_findings)} models with no downstream dependents. "
                f"These may be unused or legitimate endpoints.\n\n"
                f"Models to review:\n{model_list}"
            ),
            impact=(
                "LOW-MEDIUM - Removing unused models reduces maintenance burden, "
                "build times, and warehouse costs. However, verify they're truly "
                "unused before removal."
            ),
            effort=(
                "LOW - Review each model to confirm it's unused, then archive or delete. "
                "Consider adding dbt exposures for models consumed by BI tools."
            ),
            findings=dead_findings,
            code_snippets=[],
            priority=3,
        )
        recommendations.append(rec)

    if "DEEP_DEP_CHAIN" in findings_by_type:
        deep_findings = findings_by_type["DEEP_DEP_CHAIN"]

        # Sort by depth
        sorted_findings = sorted(
            deep_findings,
            key=lambda f: f.metadata.get("upstream_depth", 0),
            reverse=True
        )

        top_models = sorted_findings[:5]
        model_list = "\n".join([
            f"- {f.model_name} (depth: {f.metadata.get('upstream_depth')})"
            for f in top_models
        ])

        rec = Recommendation(
            id="REC_SIMPLIFY_DEPENDENCY_CHAINS",
            title="Simplify Deep Dependency Chains",
            description=(
                f"Found {len(deep_findings)} models with deep dependency chains. "
                f"These can be hard to maintain and debug.\n\n"
                f"Deepest chains:\n{model_list}"
            ),
            impact=(
                "MEDIUM - Simplifying dependency chains improves maintainability "
                "and makes debugging easier. Can also enable better parallelization."
            ),
            effort=(
                "MEDIUM-HIGH - May require refactoring model logic or consolidating "
                "intermediate transformations."
            ),
            findings=deep_findings,
            code_snippets=[],
            priority=5,
        )
        recommendations.append(rec)

    # Sort recommendations by priority (descending)
    recommendations.sort(key=lambda r: r.priority, reverse=True)

    return recommendations
