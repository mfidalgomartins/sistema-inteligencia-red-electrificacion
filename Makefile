PYTHON ?= .venv/bin/python
PIP ?= .venv/bin/pip

.DEFAULT_GOAL := help
.PHONY: help setup deps-check lint format compile test check coverage run publication manifest validate smoke verify-publication release clean-cache

help:  ## Muestra los comandos disponibles
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[1m%-20s\033[0m %s\n", $$1, $$2}'

setup:  ## Crea el entorno virtual e instala dependencias
	python3 -m venv .venv
	$(PIP) install -e ".[dev]"

deps-check:  ## Verifica la consistencia de dependencias instaladas
	$(PYTHON) -m pip check

lint:  ## Comprueba lint y formato con Ruff
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:  ## Aplica el formato canónico de Ruff
	$(PYTHON) -m ruff format .

compile:  ## Compila módulos y pruebas para detectar errores de sintaxis
	$(PYTHON) -m compileall -q src tests scripts

test:  ## Ejecuta la suite completa de pruebas
	$(PYTHON) -m pytest

check: deps-check lint compile test  ## Gate rápido para desarrollo local

coverage:  ## Mide pruebas unitarias + pipeline y exige 80% de cobertura
	$(PYTHON) -m coverage erase
	$(PYTHON) -m coverage run -m pytest
	$(PYTHON) -m coverage run --append -m grid_intelligence --log-level WARNING
	$(PYTHON) -m coverage report --fail-under=80

run:  ## Ejecuta el pipeline analítico completo
	$(PYTHON) -m grid_intelligence

publication:  ## Genera gráficos, tablero y el informe PDF
	$(PYTHON) scripts/build_publication_outputs.py

manifest:  ## Regenera el manifiesto de release
	$(PYTHON) -m grid_intelligence.release_manifest

validate:  ## Ejecuta la validación analítica formal
	$(PYTHON) -m grid_intelligence.validation

smoke:  ## Pruebas de humo de release
	$(PYTHON) -m grid_intelligence.quality_gates

verify-publication:  ## Verifica la integridad de artefactos públicos
	$(PYTHON) -m pytest tests/test_public_artifacts.py

release: deps-check lint compile coverage publication manifest smoke verify-publication  ## Gate completo de producción

clean-cache:  ## Elimina cachés y residuos locales de build
	rm -rf .coverage .pytest_cache .ruff_cache .mplconfig build
	find . -path './.git' -prune -o -path './.venv' -prune -o -type d \( -name __pycache__ -o -name '*.egg-info' \) -prune -exec rm -rf {} +
