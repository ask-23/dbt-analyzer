"""Command-line interface for dbt-analyzer."""

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from dbt_analyzer.project import DbtProject
from dbt_analyzer.recommendations import generate_recommendations
from dbt_analyzer.report import generate_json_report, generate_markdown_report
from dbt_analyzer.rules import RuleConfig, run_all_rules

# Create a Typer app for use in tests
_typer_app = typer.Typer()

console = Console()


@_typer_app.command(name="")
def main(
    project_path: Path = typer.Argument(
        ...,
        help="Path to the dbt project directory",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
    ),
    manifest_path: Optional[Path] = typer.Option(
        None,
        "--manifest-path",
        help="Path to manifest.json (default: <project>/manifest.json)",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    run_results_path: Optional[Path] = typer.Option(
        None,
        "--run-results-path",
        help="Path to run_results.json (optional, for performance data)",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    output_path: Path = typer.Option(
        Path("./dbt_analyzer_reports"),
        "--output-path",
        help="Directory to write reports",
    ),
    format: str = typer.Option(
        "both",
        "--format",
        help="Output format: markdown, json, or both",
    ),
    max_depth: int = typer.Option(
        8,
        "--max-depth",
        help="Maximum dependency depth before flagging",
    ),
    min_execution_time: float = typer.Option(
        60.0,
        "--min-execution-time",
        help="Minimum execution time (seconds) to flag heavy models",
    ),
    min_rows: int = typer.Option(
        100000,
        "--min-rows",
        help="Minimum rows to consider a model heavy",
    ),
) -> None:
    """Analyze a dbt project for performance and maintainability issues.

    \b
    Example:
        dbt-analyzer analyze /path/to/dbt/project --format both
    """
    console.print(f"\n[bold blue]dbt Pipeline Analyzer[/bold blue]\n")
    console.print(f"Analyzing project: [cyan]{project_path}[/cyan]\n")

    # Load project
    with console.status("[bold green]Loading dbt project..."):
        try:
            project = DbtProject(
                project_path=project_path,
                manifest_path=manifest_path,
                run_results_path=run_results_path,
            )
            project.load()
        except FileNotFoundError as e:
            console.print(f"[bold red]Error:[/bold red] {e}")
            raise typer.Exit(code=1)
        except Exception as e:
            console.print(f"[bold red]Unexpected error:[/bold red] {e}")
            raise typer.Exit(code=1)

    console.print(f"✓ Loaded {len(project.dag.models)} models\n")

    # Run analysis
    with console.status("[bold green]Running analysis rules..."):
        config = RuleConfig(
            max_dependency_depth=max_depth,
            min_execution_time_seconds=min_execution_time,
            min_rows_for_heavy=min_rows,
        )
        findings = run_all_rules(project.dag, config)

    console.print(f"✓ Found {len(findings)} issues\n")

    # Generate recommendations
    with console.status("[bold green]Generating recommendations..."):
        recommendations = generate_recommendations(findings)

    console.print(f"✓ Generated {len(recommendations)} recommendations\n")

    # Display summary table
    _display_summary(findings, recommendations)

    # Generate reports
    output_path.mkdir(parents=True, exist_ok=True)

    if format in ("markdown", "both"):
        md_path = output_path / "analysis_report.md"
        with console.status(f"[bold green]Writing Markdown report to {md_path}..."):
            generate_markdown_report(
                project=project,
                findings=findings,
                recommendations=recommendations,
                output_path=md_path,
            )
        console.print(f"✓ Markdown report: [cyan]{md_path}[/cyan]")

    if format in ("json", "both"):
        json_path = output_path / "analysis_report.json"
        with console.status(f"[bold green]Writing JSON report to {json_path}..."):
            generate_json_report(
                project=project,
                findings=findings,
                recommendations=recommendations,
                output_path=json_path,
            )
        console.print(f"✓ JSON report: [cyan]{json_path}[/cyan]")

    console.print("\n[bold green]✓ Analysis complete![/bold green]\n")


def _display_summary(findings: list, recommendations: list) -> None:
    """Display a summary table of findings and recommendations."""
    table = Table(title="Analysis Summary", show_header=True, header_style="bold magenta")

    table.add_column("Category", style="cyan", width=20)
    table.add_column("Count", justify="right", style="green")

    # Count by severity
    severity_counts = {"ERROR": 0, "WARN": 0, "INFO": 0}
    for finding in findings:
        sev = finding.severity.upper()
        if sev in severity_counts:
            severity_counts[sev] += 1

    table.add_row("🔴 ERROR", str(severity_counts["ERROR"]))
    table.add_row("⚠️  WARN", str(severity_counts["WARN"]))
    table.add_row("ℹ️  INFO", str(severity_counts["INFO"]))
    table.add_row("", "")
    table.add_row("💡 Recommendations", str(len(recommendations)))

    console.print(table)
    console.print()


def app() -> None:
    """Entry point for the CLI."""
    _typer_app()


# Expose for testing
__all__ = ["app", "_typer_app", "main"]


if __name__ == "__main__":
    app()
