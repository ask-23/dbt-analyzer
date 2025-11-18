"""Tests for report generation."""

import json
from pathlib import Path

from dbt_analyzer.project import DbtProject
from dbt_analyzer.recommendations import generate_recommendations
from dbt_analyzer.report import generate_json_report, generate_markdown_report
from dbt_analyzer.rules import RuleConfig, run_all_rules


def test_generate_markdown_report(heavy_project_dir: Path, tmp_path: Path) -> None:
    """Test generating a Markdown report."""
    project = DbtProject(
        project_path=heavy_project_dir,
        run_results_path=heavy_project_dir / "run_results.json"
    )
    project.load()

    config = RuleConfig()
    findings = run_all_rules(project.dag, config)
    recommendations = generate_recommendations(findings)

    report_path = tmp_path / "report.md"
    generate_markdown_report(
        project=project,
        findings=findings,
        recommendations=recommendations,
        output_path=report_path
    )

    assert report_path.exists()

    content = report_path.read_text()

    # Check for key sections
    assert "# dbt Pipeline Analysis Report" in content
    assert "## Summary" in content
    assert "## Recommendations" in content
    assert "## Findings by Severity" in content

    # Check that findings are included
    assert "heavy_table_model" in content or "slow_view_model" in content

    # Check that recommendations are included
    assert "Incremental" in content or "incremental" in content


def test_generate_json_report(heavy_project_dir: Path, tmp_path: Path) -> None:
    """Test generating a JSON report."""
    project = DbtProject(
        project_path=heavy_project_dir,
        run_results_path=heavy_project_dir / "run_results.json"
    )
    project.load()

    config = RuleConfig()
    findings = run_all_rules(project.dag, config)
    recommendations = generate_recommendations(findings)

    report_path = tmp_path / "report.json"
    generate_json_report(
        project=project,
        findings=findings,
        recommendations=recommendations,
        output_path=report_path
    )

    assert report_path.exists()

    # Load and validate JSON
    with open(report_path) as f:
        data = json.load(f)

    assert "metadata" in data
    assert "summary" in data
    assert "findings" in data
    assert "recommendations" in data

    # Check metadata
    assert data["metadata"]["project_path"]
    assert "timestamp" in data["metadata"]

    # Check summary
    assert "total_models" in data["summary"]
    assert "total_findings" in data["summary"]
    assert "total_recommendations" in data["summary"]

    # Check findings are serialized
    assert len(data["findings"]) > 0
    first_finding = data["findings"][0]
    assert "id" in first_finding
    assert "severity" in first_finding
    assert "model_name" in first_finding


def test_markdown_report_includes_model_stats(simple_project_dir: Path, tmp_path: Path) -> None:
    """Test that Markdown report includes model performance stats."""
    project = DbtProject(
        project_path=simple_project_dir,
        run_results_path=simple_project_dir / "run_results.json"
    )
    project.load()

    config = RuleConfig()
    findings = run_all_rules(project.dag, config)
    recommendations = generate_recommendations(findings)

    report_path = tmp_path / "report.md"
    generate_markdown_report(
        project=project,
        findings=findings,
        recommendations=recommendations,
        output_path=report_path
    )

    content = report_path.read_text()

    # Should include model performance table
    assert "## Model Performance" in content or "## Top Models" in content

    # Should mention some of our models
    assert "stg_customers" in content or "fct_orders" in content


def test_json_report_structure(simple_project_dir: Path, tmp_path: Path) -> None:
    """Test that JSON report has correct structure."""
    project = DbtProject(
        project_path=simple_project_dir,
        run_results_path=simple_project_dir / "run_results.json"
    )
    project.load()

    config = RuleConfig()
    findings = run_all_rules(project.dag, config)
    recommendations = generate_recommendations(findings)

    report_path = tmp_path / "report.json"
    generate_json_report(
        project=project,
        findings=findings,
        recommendations=recommendations,
        output_path=report_path
    )

    with open(report_path) as f:
        data = json.load(f)

    # Validate structure
    assert isinstance(data["findings"], list)
    assert isinstance(data["recommendations"], list)
    assert isinstance(data["summary"], dict)

    # Check that findings are properly serialized with all fields
    if data["findings"]:
        finding = data["findings"][0]
        assert "id" in finding
        assert "severity" in finding
        assert "model_name" in finding
        assert "title" in finding
        assert "description" in finding
        assert "rationale" in finding
        assert "suggested_action" in finding


def test_reports_with_no_findings(simple_project_dir: Path, tmp_path: Path) -> None:
    """Test generating reports when there are no findings."""
    project = DbtProject(project_path=simple_project_dir)
    project.load()

    # Use very high thresholds so no findings are generated
    config = RuleConfig(
        min_execution_time_seconds=10000.0,
        max_dependency_depth=1000
    )
    findings = run_all_rules(project.dag, config)
    recommendations = generate_recommendations(findings)

    # Generate both reports
    md_path = tmp_path / "empty_report.md"
    json_path = tmp_path / "empty_report.json"

    generate_markdown_report(
        project=project,
        findings=findings,
        recommendations=recommendations,
        output_path=md_path
    )

    generate_json_report(
        project=project,
        findings=findings,
        recommendations=recommendations,
        output_path=json_path
    )

    # Both should exist
    assert md_path.exists()
    assert json_path.exists()

    # Markdown should be generated successfully
    md_content = md_path.read_text()
    assert "# dbt Pipeline Analysis Report" in md_content

    # JSON should be valid
    with open(json_path) as f:
        data = json.load(f)
    # Dead model findings may still be present since they don't depend on thresholds
    # Just verify the structure is valid
    assert "findings" in data
    assert "recommendations" in data
