.PHONY: help install install-dev test test-cov lint format clean run-example

help:  ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install the package in editable mode
	pip install -e .

install-dev:  ## Install the package with dev dependencies
	pip install -e ".[dev]"

test:  ## Run all tests
	pytest -v

test-cov:  ## Run tests with coverage report
	pytest --cov=dbt_analyzer --cov-report=term-missing --cov-report=html

lint:  ## Run linting checks
	ruff check dbt_analyzer tests

format:  ## Format code with ruff
	ruff format dbt_analyzer tests

clean:  ## Clean up temporary files and caches
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf dbt_analyzer_reports/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete

run-example:  ## Run analyzer on simple_project fixture
	dbt-analyzer tests/fixtures/simple_project --run-results-path tests/fixtures/simple_project/run_results.json --format both

run-example-heavy:  ## Run analyzer on heavy_non_incremental fixture
	dbt-analyzer tests/fixtures/heavy_non_incremental --run-results-path tests/fixtures/heavy_non_incremental/run_results.json --format both
