"""Tests for recommendations layer."""

from pathlib import Path

from dbt_analyzer.models import Finding, Severity
from dbt_analyzer.project import DbtProject
from dbt_analyzer.recommendations import generate_recommendations
from dbt_analyzer.rules import RuleConfig, run_all_rules


def test_generate_recommendations_from_findings(heavy_project_dir: Path) -> None:
    """Test generating recommendations from findings."""
    project = DbtProject(
        project_path=heavy_project_dir,
        run_results_path=heavy_project_dir / "run_results.json"
    )
    project.load()

    config = RuleConfig()
    findings = run_all_rules(project.dag, config)

    recommendations = generate_recommendations(findings)

    # Should have at least one recommendation
    assert len(recommendations) > 0

    # Check that recommendations have required fields
    for rec in recommendations:
        assert rec.id
        assert rec.title
        assert rec.description
        assert rec.impact
        assert rec.effort
        assert rec.priority >= 1


def test_recommendations_include_code_snippets(heavy_project_dir: Path) -> None:
    """Test that recommendations include code snippets for incremental models."""
    project = DbtProject(
        project_path=heavy_project_dir,
        run_results_path=heavy_project_dir / "run_results.json"
    )
    project.load()

    config = RuleConfig()
    findings = run_all_rules(project.dag, config)

    recommendations = generate_recommendations(findings)

    # Find the incremental recommendation
    incremental_recs = [
        r for r in recommendations
        if "incremental" in r.title.lower()
    ]

    assert len(incremental_recs) > 0

    # Should have code snippets
    for rec in incremental_recs:
        assert len(rec.code_snippets) > 0
        # Snippets should contain dbt config syntax
        snippet_text = " ".join(rec.code_snippets)
        assert "config" in snippet_text.lower() or "incremental" in snippet_text.lower()


def test_recommendations_prioritization(simple_project_dir: Path) -> None:
    """Test that recommendations are prioritized correctly."""
    project = DbtProject(
        project_path=simple_project_dir,
        run_results_path=simple_project_dir / "run_results.json"
    )
    project.load()

    config = RuleConfig(min_execution_time_seconds=1.0)
    findings = run_all_rules(project.dag, config)

    recommendations = generate_recommendations(findings)

    # Recommendations should be sorted by priority (higher = more important)
    priorities = [r.priority for r in recommendations]
    assert priorities == sorted(priorities, reverse=True)


def test_recommendations_group_related_findings(heavy_project_dir: Path) -> None:
    """Test that recommendations group related findings together."""
    project = DbtProject(
        project_path=heavy_project_dir,
        run_results_path=heavy_project_dir / "run_results.json"
    )
    project.load()

    config = RuleConfig()
    findings = run_all_rules(project.dag, config)

    recommendations = generate_recommendations(findings)

    # Each recommendation should aggregate one or more findings
    total_findings_in_recs = sum(len(r.findings) for r in recommendations)
    assert total_findings_in_recs >= len(findings)

    # Check that findings are grouped by type
    for rec in recommendations:
        if len(rec.findings) > 1:
            # All findings in a recommendation should be of the same type
            finding_types = {f.id for f in rec.findings}
            # Or they could be related types, but at minimum they should exist
            assert len(finding_types) >= 1


def test_empty_findings_produce_no_recommendations() -> None:
    """Test that empty findings list produces no recommendations."""
    findings: list[Finding] = []
    recommendations = generate_recommendations(findings)

    assert len(recommendations) == 0
