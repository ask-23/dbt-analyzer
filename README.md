# dbt Pipeline Analyzer

A Python CLI tool to analyze dbt projects for performance and maintainability issues. Built with TDD principles and designed to integrate seamlessly into CI/CD pipelines.

## What It Does

**dbt Pipeline Analyzer** automatically scans your dbt project and identifies:

- **Heavy non-incremental models** - Slow or large models that should use incremental materialization
- **Dead/unused models** - Models with no downstream dependents that may be technical debt
- **Deep dependency chains** - Complex lineage that makes debugging and maintenance difficult
- **Critical bottlenecks** - Slow models with many downstream dependents that impact entire pipelines
- **Performance optimization opportunities** - Data-driven recommendations based on actual run times

## Features

- **Automated Analysis** - Parses `manifest.json` and `run_results.json` from dbt Core projects
- **Actionable Recommendations** - Provides concrete code examples and migration strategies
- **Multiple Output Formats** - Generates both Markdown and JSON reports
- **CI/CD Ready** - Simple CLI interface perfect for automated pipelines
- **Zero dbt Execution** - Analyzes artifacts without running dbt commands
- **Configurable Thresholds** - Customize what gets flagged based on your project's needs

## What It Does NOT Do

- **Does not modify your dbt project** - All analysis is read-only
- **Does not run dbt** - Works with existing manifest/run_results artifacts
- **Does not replace human judgment** - Recommendations are heuristics requiring review
- **Does not support dbt Cloud** - Designed for dbt Core projects only

## Installation

```bash
pip install -e .
```

For development:

```bash
pip install -e ".[dev]"
```

## Quick Start

1. **Run dbt to generate artifacts:**

```bash
cd your-dbt-project
dbt compile  # Generates manifest.json
dbt run      # Generates run_results.json (optional but recommended)
```

2. **Run the analyzer:**

```bash
dbt-analyzer /path/to/your-dbt-project --format both
```

3. **Review the reports:**

Reports are generated in `./dbt_analyzer_reports/`:
- `analysis_report.md` - Human-readable Markdown report
- `analysis_report.json` - Machine-readable JSON for CI integration

## Usage

### Basic Usage

```bash
dbt-analyzer /path/to/dbt/project
```

### With Custom Output Location

```bash
dbt-analyzer /path/to/dbt/project --output-path ./reports
```

### Markdown Only

```bash
dbt-analyzer /path/to/dbt/project --format markdown
```

### JSON Only (for CI)

```bash
dbt-analyzer /path/to/dbt/project --format json
```

### Custom Thresholds

```bash
dbt-analyzer /path/to/dbt/project \
  --min-execution-time 30.0 \
  --min-rows 50000 \
  --max-depth 10
```

### All Options

```bash
dbt-analyzer [PROJECT_PATH] [OPTIONS]

Arguments:
  PROJECT_PATH  Path to the dbt project directory [required]

Options:
  --manifest-path PATH           Path to manifest.json (default: PROJECT_PATH/manifest.json)
  --run-results-path PATH        Path to run_results.json (optional, enables performance analysis)
  --output-path PATH             Directory for reports (default: ./dbt_analyzer_reports)
  --format [markdown|json|both]  Output format (default: both)
  --max-depth INTEGER            Max dependency depth before flagging (default: 8)
  --min-execution-time FLOAT     Min execution time in seconds for heavy models (default: 60.0)
  --min-rows INTEGER             Min rows to consider a model heavy (default: 100000)
  --help                         Show help and exit
```

## Analysis Rules

### 1. Heavy Non-Incremental Models

**What:** Models with long execution times or large row counts that are materialized as `table` or `view`.

**Why:** Incremental materialization can reduce run times by 80-95% for large tables.

**Thresholds:**
- Execution time ≥ 60 seconds
- Row count ≥ 100,000

**Example Fix:**
```sql
{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='fail'
  )
}}

SELECT *
FROM source_table
{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

### 2. Dead/Unused Models

**What:** Models with zero downstream dependents.

**Why:** May indicate technical debt, unused code, or missing exposures for BI models.

**Action:** Review each model - archive if unused, or add to dbt exposures if consumed externally.

### 3. Deep Dependency Chains

**What:** Models with more than N upstream dependencies (default: 8).

**Why:** Makes debugging difficult and increases pipeline fragility.

**Action:** Consider consolidating intermediate transformations or refactoring logic.

### 4. Critical Bottlenecks (Fan-Out on Heavy Models)

**What:** Slow models with 3+ downstream dependents.

**Why:** These create pipeline bottlenecks - optimizing them has the highest ROI.

**Action:** Prioritize optimization (add indexes, convert to incremental, optimize SQL).

## Output Reports

### Markdown Report Structure

```
# dbt Pipeline Analysis Report

## Summary
- Total models, findings, recommendations
- Findings breakdown by severity (ERROR/WARN/INFO)

## Recommendations
- Prioritized recommendations with impact/effort analysis
- Code examples for suggested changes

## Findings by Severity
- Detailed findings grouped by severity level

## Top Models by Execution Time
- Performance table of slowest models
```

### JSON Report Structure

```json
{
  "metadata": {
    "timestamp": "2024-01-15T10:00:00",
    "project_path": "/path/to/project",
    "manifest_path": "/path/to/manifest.json"
  },
  "summary": {
    "total_models": 150,
    "total_findings": 23,
    "total_recommendations": 4,
    "findings_by_severity": {
      "error": 2,
      "warn": 15,
      "info": 6
    }
  },
  "findings": [ ... ],
  "recommendations": [ ... ]
}
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: dbt Analysis

on: [pull_request]

jobs:
  analyze:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install dbt-pipeline-analyzer
          pip install dbt-snowflake  # or your adapter

      - name: Run dbt
        run: |
          cd dbt_project
          dbt compile
          dbt run

      - name: Analyze dbt project
        run: |
          dbt-analyzer dbt_project --format json --output-path ./analysis

      - name: Upload analysis
        uses: actions/upload-artifact@v2
        with:
          name: dbt-analysis
          path: ./analysis/analysis_report.json

      - name: Comment PR (optional)
        run: |
          # Parse JSON and post findings as PR comment
          python scripts/post_analysis_to_pr.py
```

## Development

### Running Tests

```bash
pytest
```

### With Coverage

```bash
pytest --cov=dbt_analyzer --cov-report=html
```

### Linting

```bash
ruff check dbt_analyzer
```

## Supported dbt Versions

This tool supports **dbt Core 1.x**. It reads dbt's compiled artifacts (`manifest.json` v11+ and `run_results.json` v5+), so it should work with most modern dbt Core versions.

Note: This tool does NOT support dbt Cloud directly. For dbt Cloud projects, download the manifest and run_results artifacts via the dbt Cloud API, then run the analyzer against those files.

## Architecture

```
dbt_analyzer/
├── models.py           # Core data structures (Model, Finding, Recommendation)
├── project.py          # Manifest parsing and DAG construction
├── results.py          # run_results.json integration
├── rules.py            # Analysis rules (each rule is independent)
├── recommendations.py  # High-level recommendation generation
├── report.py           # Markdown and JSON report generation
└── cli.py              # Typer-based CLI
```

## Extending with Custom Rules

To add a custom rule:

1. Create a function in `rules.py`:

```python
def check_my_custom_rule(dag: ProjectDAG, config: RuleConfig) -> list[Finding]:
    findings = []

    for model in dag.models.values():
        if your_condition(model):
            finding = Finding(
                id="CUSTOM_RULE",
                severity=Severity.WARN,
                model_name=model.name,
                title="...",
                description="...",
                rationale="...",
                suggested_action="...",
            )
            findings.append(finding)

    return findings
```

2. Add it to `run_all_rules()`:

```python
def run_all_rules(dag: ProjectDAG, config: RuleConfig) -> list[Finding]:
    rules = [
        check_heavy_non_incremental_models,
        check_dead_models,
        check_deep_dependency_chains,
        check_fan_out_heavy_models,
        check_my_custom_rule,  # Add your rule here
    ]
    # ...
```

## Contributing

Contributions are welcome! Please:

1. Add tests for new features
2. Follow the existing code style
3. Update documentation
4. Run the full test suite before submitting PRs

## License

MIT License - see LICENSE file for details

## Credits

Built by ask-23 using test-driven development principles.

## Support

For issues, questions, or feature requests, please open an issue on GitHub.
