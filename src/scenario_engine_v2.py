from __future__ import annotations

import os
from pathlib import Path
from textwrap import dedent

os.environ.setdefault("MPLCONFIGDIR", str(Path(__file__).resolve().parents[1] / ".mplconfig"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .common_v2 import connect_v2, ensure_dirs, get_paths, write_df


SCENARIOS = {
    "crecimiento_acelerado_ev": {
        "load_factor": 1.12,
        "congestion_factor": 1.18,
        "ens_factor": 1.10,
        "curtailment_factor": 1.05,
        "flex_gap_factor": 1.16,
        "risk_cost_factor": 1.14,
        "capex_factor": 1.10,
        "priority_factor": 1.09,
    },
    "electrificacion_industrial_intensiva": {
        "load_factor": 1.10,
        "congestion_factor": 1.16,
        "ens_factor": 1.12,
        "curtailment_factor": 1.08,
        "flex_gap_factor": 1.18,
        "risk_cost_factor": 1.18,
        "capex_factor": 1.15,
        "priority_factor": 1.12,
    },
    "mayor_penetracion_gd": {
        "load_factor": 0.98,
        "congestion_factor": 1.07,
        "ens_factor": 1.02,
        "curtailment_factor": 1.25,
        "flex_gap_factor": 1.08,
        "risk_cost_factor": 1.06,
        "capex_factor": 1.07,
        "priority_factor": 1.05,
    },
    "retraso_capex": {
        "load_factor": 1.03,
        "congestion_factor": 1.20,
        "ens_factor": 1.20,
        "curtailment_factor": 1.12,
        "flex_gap_factor": 1.22,
        "risk_cost_factor": 1.26,
        "capex_factor": 1.22,
        "priority_factor": 1.18,
    },
    "despliegue_adicional_flexibilidad": {
        "load_factor": 0.99,
        "congestion_factor": 0.82,
        "ens_factor": 0.88,
        "curtailment_factor": 0.90,
        "flex_gap_factor": 0.70,
        "risk_cost_factor": 0.84,
        "capex_factor": 0.92,
        "priority_factor": 0.86,
    },
    "despliegue_adicional_storage": {
        "load_factor": 0.99,
        "congestion_factor": 0.86,
        "ens_factor": 0.90,
        "curtailment_factor": 0.75,
        "flex_gap_factor": 0.72,
        "risk_cost_factor": 0.86,
        "capex_factor": 0.96,
        "priority_factor": 0.88,
    },
    "capex_mas_flexibilidad": {
        "load_factor": 0.98,
        "congestion_factor": 0.76,
        "ens_factor": 0.82,
        "curtailment_factor": 0.80,
        "flex_gap_factor": 0.62,
        "risk_cost_factor": 0.78,
        "capex_factor": 1.04,
        "priority_factor": 0.80,
    },
    "evento_degradacion_activos": {
        "load_factor": 1.04,
        "congestion_factor": 1.24,
        "ens_factor": 1.30,
        "curtailment_factor": 1.10,
        "flex_gap_factor": 1.14,
        "risk_cost_factor": 1.28,
        "capex_factor": 1.18,
        "priority_factor": 1.20,
    },
}


def run_scenario_engine_v2() -> dict[str, pd.DataFrame]:
    paths = ensure_dirs(get_paths())
    conn = connect_v2(paths)

    base = pd.read_csv(paths.data_processed / "intervention_scoring_table.csv")
    zone_month = conn.execute(
        """
        SELECT
            zona_id,
            AVG(curtailment_mwh) AS curtailment_base,
            AVG(carga_punta_mw) AS carga_punta_base,
            AVG(horas_congestion) AS horas_congestion_base,
            AVG(ens_mwh) AS ens_base,
            AVG(gap_flex_tecnico_mwh) AS gap_flex_base,
            AVG(intensidad_capex_proxy) AS intensidad_capex_base
        FROM zone_month_features
        GROUP BY zona_id
        """
    ).df()

    base = base.merge(zone_month, on="zona_id", how="left")
    numeric_cols = base.select_dtypes(include=[np.number]).columns
    base[numeric_cols] = base[numeric_cols].fillna(0.0)
    for col in ["risk_tier", "recommended_intervention", "recommended_sequence", "main_risk_driver", "confidence_flag"]:
        if col in base.columns:
            base[col] = base[col].fillna("no_disponible")

    rows = []
    load_col = "carga_relativa_avg" if "carga_relativa_avg" in base.columns else ("carga_punta_avg" if "carga_punta_avg" in base.columns else "carga_punta_base")
    for scenario, params in SCENARIOS.items():
        sdf = base.copy()
        sdf["scenario"] = scenario
        sdf["carga_relativa_scenario"] = sdf[load_col] * params["load_factor"]
        sdf["horas_congestion_scenario"] = sdf["horas_congestion_avg"] * params["congestion_factor"]
        sdf["ens_scenario"] = sdf["ens_avg"] * params["ens_factor"]
        sdf["curtailment_scenario"] = sdf["curtailment_base"] * params["curtailment_factor"]
        sdf["flexibility_gap_scenario"] = sdf["gap_flexibilidad"] * params["flex_gap_factor"]
        sdf["coste_riesgo_scenario"] = sdf["coste_riesgo_proxy"] * params["risk_cost_factor"]
        sdf["inversion_requerida_scenario"] = (
            sdf["capex_total"] * params["capex_factor"]
            + 0.20 * sdf["intensidad_capex_base"] * max(params["flex_gap_factor"], 1.0)
        )
        sdf["investment_priority_score_scenario"] = (sdf["investment_priority_score"] * params["priority_factor"]).clip(0, 100)
        sdf = sdf.sort_values("investment_priority_score_scenario", ascending=False).reset_index(drop=True)
        sdf["priority_rank_scenario"] = np.arange(1, len(sdf) + 1)

        rows.append(
            sdf[
                [
                    "scenario",
                    "zona_id",
                    "risk_tier",
                    "recommended_intervention",
                    "carga_relativa_scenario",
                    "horas_congestion_scenario",
                    "ens_scenario",
                    "curtailment_scenario",
                    "flexibility_gap_scenario",
                    "coste_riesgo_scenario",
                    "inversion_requerida_scenario",
                    "investment_priority_score_scenario",
                    "priority_rank_scenario",
                ]
            ]
        )

    scenario_impacts = pd.concat(rows, ignore_index=True)

    summary = (
        scenario_impacts.groupby("scenario", as_index=False)
        .agg(
            carga_relativa_media=("carga_relativa_scenario", "mean"),
            congestion_total=("horas_congestion_scenario", "sum"),
            ens_total=("ens_scenario", "sum"),
            curtailment_total=("curtailment_scenario", "sum"),
            flex_gap_total=("flexibility_gap_scenario", "sum"),
            coste_riesgo_total=("coste_riesgo_scenario", "sum"),
            inversion_requerida_total=("inversion_requerida_scenario", "sum"),
            prioridad_media=("investment_priority_score_scenario", "mean"),
        )
        .sort_values("coste_riesgo_total", ascending=False)
    )

    priority = scenario_impacts.sort_values(["scenario", "priority_rank_scenario"]).groupby("scenario", as_index=False).head(15)

    # Gráfico comparativo base vs alternativos.
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(summary["scenario"], summary["coste_riesgo_total"], color="#1f78b4", alpha=0.8, label="Coste de riesgo")
    ax.plot(summary["scenario"], summary["inversion_requerida_total"], color="#e31a1c", marker="o", linewidth=2.0, label="Inversión requerida")
    ax.set_title("Escenarios: coste de riesgo vs inversión requerida")
    ax.set_xlabel("Escenario")
    ax.set_ylabel("EUR (proxy)")
    ax.tick_params(axis="x", rotation=25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(paths.outputs_charts / "13_escenarios_riesgo_vs_inversion.png", dpi=150)
    plt.close(fig)

    # Recomendaciones ejecutivas por escenario.
    best = summary.sort_values("coste_riesgo_total", ascending=True).head(3)
    worst = summary.sort_values("coste_riesgo_total", ascending=False).head(3)

    rec = dedent(
        f"""
        # Scenario Recommendations (v2)

        ## Escenarios evaluados
        {pd.DataFrame({'scenario': list(SCENARIOS.keys())}).to_markdown(index=False)}

        ## Mejores escenarios por coste de riesgo
        {best[['scenario','coste_riesgo_total','inversion_requerida_total','prioridad_media']].to_markdown(index=False)}

        ## Escenarios más exigentes
        {worst[['scenario','coste_riesgo_total','inversion_requerida_total','prioridad_media']].to_markdown(index=False)}

        ## Lectura ejecutiva
        - `capex_mas_flexibilidad`, `despliegue_adicional_flexibilidad` y `despliegue_adicional_storage` son los escenarios con mejor balance riesgo/urgencia.
        - `evento_degradacion_activos` y `retraso_capex` son los más severos y deben disparar planes de contingencia.
        - `crecimiento_acelerado_ev` y `electrificacion_industrial_intensiva` exigen acelerar preparación territorial en zonas ya estresadas.
        """
    ).strip() + "\n"

    (paths.outputs_reports / "scenario_recommendations.md").write_text(rec, encoding="utf-8")

    write_df(scenario_impacts, paths.data_processed / "scenario_impacts_v2.csv")
    write_df(summary, paths.data_processed / "scenario_summary_v2.csv")
    write_df(priority, paths.data_processed / "scenario_priority_ranking_v2.csv")

    conn.close()

    return {
        "scenario_impacts_v2": scenario_impacts,
        "scenario_summary_v2": summary,
        "scenario_priority_ranking_v2": priority,
    }


if __name__ == "__main__":
    out = run_scenario_engine_v2()
    for name, df in out.items():
        print(name, len(df))
