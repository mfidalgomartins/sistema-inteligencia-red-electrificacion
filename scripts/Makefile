VENV_PY := .venv/bin/python
VENV_PIP := .venv/bin/pip
PYTEST := .venv/bin/pytest

.PHONY: setup test run validate smoke

setup:
	python3 -m venv .venv
	$(VENV_PIP) install -r requirements.txt

test:
	$(PYTEST) -q

run:
	$(VENV_PY) -m src

validate:
	$(VENV_PY) -m src.validate_data_v2

smoke:
	$(VENV_PY) -m src.qa_smoke_v2
