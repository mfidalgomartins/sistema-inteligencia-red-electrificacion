from __future__ import annotations

from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import pandas as pd

from .config import CONFIG, Config


def _save(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def generate_charts(
    priorities: pd.DataFrame,
    scenario_summary: pd.DataFrame,
    territory_monthly: pd.DataFrame,
    config: Config = CONFIG,
) -> Dict[str, Path]:
    outputs: Dict[str, Path] = {}

    top = priorities.nlargest(15, "congestion_rate").sort_values("congestion_rate")
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.barh(top["feeder_id"], top["congestion_rate"] * 100, color="#0f766e")
    ax.set_title("Top 15 feeders con mayor tasa de congestión")
    ax.set_xlabel("Congestión (% horas)")
    ax.set_ylabel("Feeder")
    p = config.outputs_charts / "01_top_congestion_feeders.png"
    _save(fig, p)
    outputs["top_congestion"] = p

    fig, ax = plt.subplots(figsize=(9, 6))
    sc = ax.scatter(
        priorities["execution_feasibility_score"],
        priorities["stress_score"],
        s=priorities["priority_score"] * 2.4,
        c=priorities["priority_score"],
        cmap="YlOrRd",
        alpha=0.75,
    )
    ax.set_title("Estrés técnico vs factibilidad de ejecución")
    ax.set_xlabel("Factibilidad de ejecución")
    ax.set_ylabel("Stress score")
    plt.colorbar(sc, ax=ax, label="Priority score")
    p = config.outputs_charts / "02_stress_vs_feasibility.png"
    _save(fig, p)
    outputs["stress_vs_feasibility"] = p

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(scenario_summary["scenario"], scenario_summary["total_capex_m_eur"], color="#1d4ed8")
    ax.set_title("CAPEX total estimado por escenario")
    ax.set_ylabel("CAPEX (M EUR)")
    ax.set_xlabel("Escenario")
    ax.tick_params(axis="x", rotation=20)
    p = config.outputs_charts / "03_capex_escenarios.png"
    _save(fig, p)
    outputs["capex_escenarios"] = p

    territory_pressure = (
        territory_monthly.groupby("territory_id", as_index=False)
        .agg(
            congestion_hours=("congestion_hours", "sum"),
            curtailment_mwh=("curtailment_mwh", "sum"),
            gross_demand_mwh=("gross_demand_mwh", "sum"),
        )
        .sort_values("congestion_hours", ascending=False)
        .head(15)
    )
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.bar(territory_pressure["territory_id"], territory_pressure["congestion_hours"], color="#b45309")
    ax.set_title("Territorios con mayor presión de congestión")
    ax.set_xlabel("Territorio")
    ax.set_ylabel("Horas de congestión acumuladas")
    ax.tick_params(axis="x", rotation=30)
    p = config.outputs_charts / "04_territory_pressure.png"
    _save(fig, p)
    outputs["territory_pressure"] = p

    action_counts = priorities["recommended_action"].value_counts().sort_values(ascending=True)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(action_counts.index, action_counts.values, color="#374151")
    ax.set_title("Mix de acción recomendada")
    ax.set_xlabel("Nº de feeders")
    ax.set_ylabel("Acción")
    p = config.outputs_charts / "05_action_mix.png"
    _save(fig, p)
    outputs["action_mix"] = p

    return outputs
