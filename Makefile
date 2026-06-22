VENV_PY := .venv/bin/python
VENV_PIP := .venv/bin/pip
PYTEST := .venv/bin/pytest

.PHONY: setup lint format test coverage run publication manifest validate smoke verify-publication release clean-cache

setup:
	python3 -m venv .venv
	$(VENV_PIP) install -e ".[dev]"

lint:
	$(VENV_PY) -m ruff check .

format:
	$(VENV_PY) -m ruff format .

test:
	$(PYTEST) -q

coverage:
	$(VENV_PY) -m pytest -q --cov=src --cov-report=term-missing

run:
	$(VENV_PY) -m src

publication:
	$(VENV_PY) scripts/build_publication_outputs.py

manifest:
	$(VENV_PY) -m src.release_manifest_v2

validate:
	$(VENV_PY) -m src.validate_data_v2

smoke:
	$(VENV_PY) -m src.qa_smoke_v2

verify-publication:
	$(PYTEST) -q tests/test_public_artifacts.py

release: lint test run publication manifest smoke verify-publication

clean-cache:
	rm -rf .pytest_cache .mplconfig
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
