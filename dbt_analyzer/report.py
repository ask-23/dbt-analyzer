"""Report generation for dbt analysis."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from dbt_analyzer.models import Finding, Recommendation, Severity
from dbt_analyzer.project import DbtProject


def generate_markdown_report(
    project: DbtProject,
    findings: list[Finding],
    recommendations: list[Recommendation],
    output_path: Path,
) -> None:
    """Generate a Markdown report.

    Args:
        project: The dbt project
        findings: List of findings
        recommendations: List of recommendations
        output_path: Path to write the report
    """
    lines: list[str] = []

    # Header
    lines.append("# dbt Pipeline Analysis Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**Project:** `{project.project_path}`")
    lines.append("")

    # Summary
    lines.append("## Summary")
    lines.append("")

    total_models = len(project.dag.models) if project.dag else 0
    lines.append(f"- **Total Models:** {total_models}")
    lines.append(f"- **Total Findings:** {len(findings)}")
    lines.append(f"- **Total Recommendations:** {len(recommendations)}")
    lines.append("")

    # Count findings by severity
    severity_counts = {
        Severity.ERROR: 0,
        Severity.WARN: 0,
        Severity.INFO: 0,
    }
    for finding in findings:
        severity_counts[finding.severity] += 1

    lines.append("**Findings by Severity:**")
    lines.append(f"- 🔴 ERROR: {severity_counts[Severity.ERROR]}")
    lines.append(f"- ⚠️  WARN: {severity_counts[Severity.WARN]}")
    lines.append(f"- ℹ️  INFO: {severity_counts[Severity.INFO]}")
    lines.append("")

    # Recommendations
    if recommendations:
        lines.append("## Recommendations")
        lines.append("")

        for i, rec in enumerate(recommendations, 1):
            lines.append(f"### {i}. {rec.title}")
            lines.append("")
            lines.append(f"**Priority:** {rec.priority}")
            lines.append("")
            lines.append(f"**Impact:** {rec.impact}")
            lines.append("")
            lines.append(f"**Effort:** {rec.effort}")
            lines.append("")
            lines.append(f"**Description:**")
            lines.append("")
            lines.append(rec.description)
            lines.append("")

            if rec.code_snippets:
                lines.append("**Code Examples:**")
                lines.append("")
                for snippet in rec.code_snippets:
                    lines.append("```sql")
                    lines.append(snippet)
                    lines.append("```")
                    lines.append("")

            lines.append(f"*Affects {len(rec.findings)} model(s)*")
            lines.append("")
    else:
        lines.append("## Recommendations")
        lines.append("")
        lines.append("✅ No recommendations - your dbt project looks good!")
        lines.append("")

    # Findings by Severity
    lines.append("## Findings by Severity")
    lines.append("")

    if findings:
        # Group findings by severity
        findings_by_severity: dict[Severity, list[Finding]] = {
            Severity.ERROR: [],
            Severity.WARN: [],
            Severity.INFO: [],
        }
        for finding in findings:
            findings_by_severity[finding.severity].append(finding)

        # ERROR findings
        if findings_by_severity[Severity.ERROR]:
            lines.append("### 🔴 ERROR")
            lines.append("")
            for finding in findings_by_severity[Severity.ERROR]:
                lines.append(f"#### {finding.title}")
                lines.append("")
                lines.append(f"**Model:** `{finding.model_name}`")
                lines.append("")
                lines.append(f"**Description:** {finding.description}")
                lines.append("")
                lines.append(f"**Suggested Action:** {finding.suggested_action}")
                lines.append("")

        # WARN findings
        if findings_by_severity[Severity.WARN]:
            lines.append("### ⚠️  WARN")
            lines.append("")
            for finding in findings_by_severity[Severity.WARN]:
                lines.append(f"#### {finding.title}")
                lines.append("")
                lines.append(f"**Model:** `{finding.model_name}`")
                lines.append("")
                lines.append(f"**Description:** {finding.description}")
                lines.append("")
                lines.append(f"**Suggested Action:** {finding.suggested_action}")
                lines.append("")

        # INFO findings
        if findings_by_severity[Severity.INFO]:
            lines.append("### ℹ️  INFO")
            lines.append("")
            for finding in findings_by_severity[Severity.INFO]:
                lines.append(f"#### {finding.title}")
                lines.append("")
                lines.append(f"**Model:** `{finding.model_name}`")
                lines.append("")
                lines.append(f"**Description:** {finding.description}")
                lines.append("")
    else:
        lines.append("✅ No findings - your dbt project looks good!")
        lines.append("")

    # Model Performance (if run_results available)
    if project.dag:
        models_with_perf = [
            m for m in project.dag.models.values()
            if m.execution_time is not None
        ]

        if models_with_perf:
            lines.append("## Top Models by Execution Time")
            lines.append("")

            # Sort by execution time
            sorted_models = sorted(
                models_with_perf,
                key=lambda m: m.execution_time or 0,
                reverse=True
            )[:10]

            lines.append("| Model | Materialization | Execution Time | Rows Affected |")
            lines.append("|-------|----------------|----------------|---------------|")

            for model in sorted_models:
                exec_time = f"{model.execution_time:.2f}s" if model.execution_time else "N/A"
                rows = f"{model.rows_affected:,}" if model.rows_affected else "N/A"
                lines.append(
                    f"| `{model.name}` | {model.materialization} | {exec_time} | {rows} |"
                )

            lines.append("")

    # Write to file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))


def generate_json_report(
    project: DbtProject,
    findings: list[Finding],
    recommendations: list[Recommendation],
    output_path: Path,
) -> None:
    """Generate a JSON report.

    Args:
        project: The dbt project
        findings: List of findings
        recommendations: List of recommendations
        output_path: Path to write the report
    """
    report: dict[str, Any] = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "project_path": str(project.project_path),
            "manifest_path": str(project.manifest_path),
        },
        "summary": {
            "total_models": len(project.dag.models) if project.dag else 0,
            "total_findings": len(findings),
            "total_recommendations": len(recommendations),
            "findings_by_severity": {
                "error": len([f for f in findings if f.severity == Severity.ERROR]),
                "warn": len([f for f in findings if f.severity == Severity.WARN]),
                "info": len([f for f in findings if f.severity == Severity.INFO]),
            },
        },
        "findings": [finding.model_dump() for finding in findings],
        "recommendations": [rec.model_dump() for rec in recommendations],
    }

    # Write to file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
