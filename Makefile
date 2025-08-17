# SQLKit Makefile for development workflow

.PHONY: help install dev format lint typecheck test check clean build

help:  ## Show this help message
	@echo "SQLKit Development Commands"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## Install development environment
	@echo "🔧 Setting up development environment..."
	uv sync --all-packages
	uv pip install -e .

dev: install  ## Alias for install

format:  ## Format code with black and ruff
	@echo "🎨 Formatting code..."
	uv run black sqlkit/ examples/ scripts/
	uv run ruff format sqlkit/ examples/ scripts/

lint:  ## Lint code with ruff
	@echo "🔍 Linting code..."
	uv run ruff check sqlkit/ examples/ scripts/

typecheck:  ## Type check with mypy
	@echo "🧹 Type checking..."
	uv run mypy sqlkit/

test:  ## Run tests with pytest
	@echo "🧪 Running tests..."
	uv run pytest -v --cov=sqlkit

check: format lint typecheck test  ## Run all quality checks
	@echo "✅ All checks completed!"

clean:  ## Clean build artifacts
	@echo "🧹 Cleaning build artifacts..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

build:  ## Build package
	@echo "📦 Building package..."
	uv build

example:  ## Run usage examples
	@echo "🚀 Running usage examples..."
	uv run python examples/usage_examples.py

# Development shortcuts
run-example: example
fmt: format
check-types: typecheck