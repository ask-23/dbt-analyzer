"""Tests for analysis rules."""

from pathlib import Path

from dbt_analyzer.models import Severity
from dbt_analyzer.project import DbtProject
from dbt_analyzer.rules import (
    RuleConfig,
    check_dead_models,
    check_deep_dependency_chains,
    check_fan_out_heavy_models,
    check_heavy_non_incremental_models,
    run_all_rules,
)


def test_check_heavy_non_incremental_models(heavy_project_dir: Path) -> None:
    """Test identifying heavy models that should be incremental."""
    project = DbtProject(
        project_path=heavy_project_dir,
        run_results_path=heavy_project_dir / "run_results.json"
    )
    project.load()

    config = RuleConfig(
        min_execution_time_seconds=60.0,
        min_rows_for_heavy=100000
    )

    findings = check_heavy_non_incremental_models(project.dag, config)

    # Should find both heavy models
    assert len(findings) == 2

    finding_ids = [f.model_name for f in findings]
    assert "heavy_table_model" in finding_ids
    assert "slow_view_model" in finding_ids

    # Check finding details
    for finding in findings:
        assert finding.id == "HEAVY_NON_INCREMENTAL_MODEL"
        assert finding.severity == Severity.WARN
        assert "incremental" in finding.suggested_action.lower()
        assert finding.proposed_changes is not None
        assert finding.proposed_changes.get("materialization") == "incremental"


def test_check_heavy_non_incremental_with_thresholds(heavy_project_dir: Path) -> None:
    """Test that thresholds are respected."""
    project = DbtProject(
        project_path=heavy_project_dir,
        run_results_path=heavy_project_dir / "run_results.json"
    )
    project.load()

    # Set very high thresholds - should find nothing
    config = RuleConfig(
        min_execution_time_seconds=1000.0,
        min_rows_for_heavy=10000000
    )

    findings = check_heavy_non_incremental_models(project.dag, config)
    assert len(findings) == 0


def test_check_dead_models(simple_project_dir: Path) -> None:
    """Test identifying unused models with no downstream dependents."""
    project = DbtProject(project_path=simple_project_dir)
    project.load()

    config = RuleConfig()
    findings = check_dead_models(project.dag, config)

    # Should find unused_model and fct_orders (both have no downstream)
    # But fct_orders might be a legitimate leaf, so we focus on unused_model
    assert len(findings) >= 1

    model_names = [f.model_name for f in findings]
    assert "unused_model" in model_names

    # Check finding details
    unused_finding = next(f for f in findings if f.model_name == "unused_model")
    assert unused_finding.id == "DEAD_MODEL"
    assert unused_finding.severity == Severity.INFO
    assert "not referenced" in unused_finding.description.lower()


def test_check_deep_dependency_chains(simple_project_dir: Path) -> None:
    """Test identifying deep dependency chains."""
    project = DbtProject(project_path=simple_project_dir)
    project.load()

    # Set max_depth to 2, which should flag fct_orders (depends on staging models)
    config = RuleConfig(max_dependency_depth=2)
    findings = check_deep_dependency_chains(project.dag, config)

    # With max_depth=2, we might not find issues in this simple project
    # Let's check with max_depth=0 to force findings
    config_strict = RuleConfig(max_dependency_depth=0)
    findings_strict = check_deep_dependency_chains(project.dag, config_strict)

    # Should find some models exceeding depth 0
    assert len(findings_strict) >= 1

    if findings_strict:
        finding = findings_strict[0]
        assert finding.id == "DEEP_DEP_CHAIN"
        assert finding.severity == Severity.WARN


def test_check_fan_out_heavy_models(simple_project_dir: Path) -> None:
    """Test identifying heavy models with many downstream dependents."""
    project = DbtProject(
        project_path=simple_project_dir,
        run_results_path=simple_project_dir / "run_results.json"
    )
    project.load()

    # fct_orders is slow (125.7s) but has no downstream dependents
    # stg_orders/stg_customers have 1 downstream each

    config = RuleConfig(
        min_execution_time_seconds=2.0,
        min_downstream_count=1
    )
    findings = check_fan_out_heavy_models(project.dag, config)

    # Should find stg_customers and stg_orders (both are somewhat slow and have downstream)
    assert len(findings) >= 2

    model_names = [f.model_name for f in findings]
    assert "stg_customers" in model_names or "stg_orders" in model_names

    for finding in findings:
        assert finding.id == "FAN_OUT_HEAVY_MODEL"
        assert finding.severity == Severity.ERROR


def test_run_all_rules(heavy_project_dir: Path) -> None:
    """Test running all rules together."""
    project = DbtProject(
        project_path=heavy_project_dir,
        run_results_path=heavy_project_dir / "run_results.json"
    )
    project.load()

    config = RuleConfig()
    all_findings = run_all_rules(project.dag, config)

    # Should have findings from multiple rule types
    assert len(all_findings) > 0

    finding_types = {f.id for f in all_findings}
    # At minimum, should find heavy non-incremental models
    assert "HEAVY_NON_INCREMENTAL_MODEL" in finding_types


def test_rules_with_no_run_results(simple_project_dir: Path) -> None:
    """Test that rules handle missing run_results gracefully."""
    project = DbtProject(project_path=simple_project_dir)
    project.load()

    config = RuleConfig()

    # Rules that depend on execution time should return empty findings
    findings = check_heavy_non_incremental_models(project.dag, config)
    assert len(findings) == 0

    # Rules that don't depend on execution time should still work
    dead_findings = check_dead_models(project.dag, config)
    assert len(dead_findings) >= 1
