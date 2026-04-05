from __future__ import annotations

import json
from pathlib import Path

from .config import CONFIG, Config


def _notebook(cells: list[dict]) -> dict:
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3.11"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": text}


def _code(code: str) -> dict:
    return {
        "cell_type": "code",
        "metadata": {},
        "execution_count": None,
        "outputs": [],
        "source": code,
    }


def build_notebooks(config: Config = CONFIG) -> None:
    notebooks_dir = config.root_dir / "notebooks"
    notebooks_dir.mkdir(parents=True, exist_ok=True)

    principal = _notebook(
        [
            _md(
                "# Notebook Principal\n"
                "## Sistema de Inteligencia de Red, Flexibilidad, Resiliencia y Priorización de Inversiones\n"
                "Este notebook reproduce el flujo end-to-end: datos sintéticos, SQL, modelado, scoring, escenarios y dashboard."
            ),
            _code(
                "from pathlib import Path\n"
                "import pandas as pd\n"
                "from src.pipeline import run_pipeline\n"
                "resultados = run_pipeline()\n"
                "resultados"
            ),
            _code(
                "prioridades = pd.read_csv(Path('data/processed/investment_priorities.csv'))\n"
                "prioridades.head(15)"
            ),
            _code(
                "escenarios = pd.read_csv(Path('data/processed/scenario_summary.csv'))\n"
                "escenarios"
            ),
        ]
    )

    escenarios = _notebook(
        [
            _md(
                "# Notebook de Escenarios What-if\n"
                "Análisis comparativo de sensibilidad para EV acelerado, DG alta, flexibilidad intensiva y estrés climático."
            ),
            _code(
                "from pathlib import Path\n"
                "import pandas as pd\n"
                "scenario_results = pd.read_csv(Path('data/processed/scenario_results.csv'))\n"
                "scenario_summary = pd.read_csv(Path('data/processed/scenario_summary.csv'))\n"
                "scenario_summary"
            ),
            _code(
                "scenario_results.sort_values(['scenario','priority_rank_adj']).groupby('scenario').head(10)"
            ),
        ]
    )

    (notebooks_dir / "01_principal.ipynb").write_text(
        json.dumps(principal, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (notebooks_dir / "02_escenarios.ipynb").write_text(
        json.dumps(escenarios, ensure_ascii=False, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    build_notebooks()
