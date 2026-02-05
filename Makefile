.PHONY: install test lint typecheck clean build

install:
	uv sync --all-extras

test:
	uv run pytest -v

test-cov:
	uv run pytest --cov=datacite_data_file_dl --cov-report=term-missing

lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

format:
	uv run ruff format src/ tests/
	uv run ruff check --fix src/ tests/

typecheck:
	uv run mypy src/datacite_data_file_dl/

check: lint typecheck test

build:
	uv build

clean:
	rm -rf dist/ build/ *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
