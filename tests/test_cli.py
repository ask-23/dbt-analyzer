"""Tests for CLI."""

from pathlib import Path

from typer.testing import CliRunner

from dbt_analyzer.cli import _typer_app

runner = CliRunner()


def test_cli_analyze_basic(simple_project_dir: Path, tmp_path: Path) -> None:
    """Test basic analyze command."""
    result = runner.invoke(
        _typer_app,
        [
            str(simple_project_dir),
            "--output-path",
            str(tmp_path),
            "--format",
            "both",
        ],
    )

    assert result.exit_code == 0
    assert "dbt Pipeline Analyzer" in result.stdout
    assert "Analysis complete" in result.stdout

    # Check that reports were created
    assert (tmp_path / "analysis_report.md").exists()
    assert (tmp_path / "analysis_report.json").exists()


def test_cli_analyze_markdown_only(simple_project_dir: Path, tmp_path: Path) -> None:
    """Test analyze command with markdown format only."""
    result = runner.invoke(
        _typer_app,
        [
            str(simple_project_dir),
            "--output-path",
            str(tmp_path),
            "--format",
            "markdown",
        ],
    )

    assert result.exit_code == 0
    assert (tmp_path / "analysis_report.md").exists()
    assert not (tmp_path / "analysis_report.json").exists()


def test_cli_analyze_json_only(simple_project_dir: Path, tmp_path: Path) -> None:
    """Test analyze command with json format only."""
    result = runner.invoke(
        _typer_app,
        [
            str(simple_project_dir),
            "--output-path",
            str(tmp_path),
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0
    assert (tmp_path / "analysis_report.json").exists()
    assert not (tmp_path / "analysis_report.md").exists()


def test_cli_analyze_with_run_results(simple_project_dir: Path, tmp_path: Path) -> None:
    """Test analyze command with run_results."""
    result = runner.invoke(
        _typer_app,
        [
            str(simple_project_dir),
            "--run-results-path",
            str(simple_project_dir / "run_results.json"),
            "--output-path",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    assert "Analysis complete" in result.stdout


def test_cli_analyze_with_custom_thresholds(
    heavy_project_dir: Path, tmp_path: Path
) -> None:
    """Test analyze command with custom thresholds."""
    result = runner.invoke(
        _typer_app,
        [
            str(heavy_project_dir),
            "--run-results-path",
            str(heavy_project_dir / "run_results.json"),
            "--output-path",
            str(tmp_path),
            "--min-execution-time",
            "30.0",
            "--min-rows",
            "50000",
            "--max-depth",
            "5",
        ],
    )

    assert result.exit_code == 0
    assert "Analysis complete" in result.stdout


def test_cli_analyze_missing_manifest(tmp_path: Path) -> None:
    """Test analyze command with missing manifest."""
    # Create an empty directory
    empty_dir = tmp_path / "empty_project"
    empty_dir.mkdir()

    result = runner.invoke(
        _typer_app,
        [
            str(empty_dir),
            "--output-path",
            str(tmp_path / "output"),
        ],
    )

    assert result.exit_code == 1
    assert "Error" in result.stdout or "error" in result.stdout.lower()


def test_cli_analyze_invalid_path(tmp_path: Path) -> None:
    """Test analyze command with invalid project path."""
    result = runner.invoke(
        _typer_app,
        [
            str(tmp_path / "nonexistent"),
            "--output-path",
            str(tmp_path / "output"),
        ],
    )

    # Should fail due to validation
    assert result.exit_code != 0


def test_cli_displays_summary(heavy_project_dir: Path, tmp_path: Path) -> None:
    """Test that CLI displays summary table."""
    result = runner.invoke(
        _typer_app,
        [
            str(heavy_project_dir),
            "--run-results-path",
            str(heavy_project_dir / "run_results.json"),
            "--output-path",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    # Should display summary information
    assert "ERROR" in result.stdout or "WARN" in result.stdout or "INFO" in result.stdout
    assert "Recommendations" in result.stdout or "recommendations" in result.stdout
