from __future__ import annotations

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


SCENARIO_DRIVERS = {
    "crecimiento_acelerado_ev": ("electrification_pressure_score", "congestion_risk_score"),
    "electrificacion_industrial_intensiva": ("electrification_pressure_score", "economic_priority_score"),
    "mayor_penetracion_gd": ("flexibility_gap_score", "electrification_pressure_score"),
    "retraso_capex": ("economic_priority_score", "congestion_risk_score"),
    "despliegue_adicional_flexibilidad": ("flexibility_gap_score", "congestion_risk_score"),
    "despliegue_adicional_storage": ("flexibility_gap_score", "service_impact_score"),
    "capex_mas_flexibilidad": ("congestion_risk_score", "flexibility_gap_score"),
    "evento_degradacion_activos": ("asset_exposure_score", "resilience_risk_score"),
}


def _scenario_exposure(base: pd.DataFrame, scenario: str) -> pd.Series:
    primary, secondary = SCENARIO_DRIVERS[scenario]
    return (0.65 * base[primary] + 0.35 * base[secondary]).clip(0, 100) / 100.0


def _localized_factor(base_factor: float, exposure: pd.Series) -> pd.Series:
    factor = 1.0 + (base_factor - 1.0) * (0.55 + 0.90 * exposure)
    return factor.clip(lower=0.25)


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
    for scenario, params in SCENARIOS.items():
        sdf = base.copy()
        sdf["scenario"] = scenario
        sdf["scenario_exposure"] = _scenario_exposure(sdf, scenario)
        sdf["carga_punta_scenario_mw"] = sdf["carga_punta_base"] * _localized_factor(params["load_factor"], sdf["scenario_exposure"])
        sdf["horas_congestion_scenario"] = sdf["horas_congestion_avg"] * _localized_factor(params["congestion_factor"], sdf["scenario_exposure"])
        sdf["ens_scenario"] = sdf["ens_avg"] * _localized_factor(params["ens_factor"], sdf["scenario_exposure"])
        sdf["curtailment_scenario"] = sdf["curtailment_base"] * _localized_factor(params["curtailment_factor"], sdf["scenario_exposure"])
        sdf["flexibility_gap_scenario"] = sdf["gap_flexibilidad"] * _localized_factor(params["flex_gap_factor"], sdf["scenario_exposure"])
        sdf["coste_riesgo_scenario"] = sdf["coste_riesgo_proxy"] * _localized_factor(params["risk_cost_factor"], sdf["scenario_exposure"])
        sdf["inversion_requerida_scenario"] = (
            sdf["capex_total"] * _localized_factor(params["capex_factor"], sdf["scenario_exposure"])
            + 0.20
            * sdf["intensidad_capex_base"]
            * _localized_factor(max(params["flex_gap_factor"], 1.0), sdf["scenario_exposure"])
        )
        sdf["investment_priority_score_scenario"] = (
            sdf["investment_priority_score"]
            * _localized_factor(params["priority_factor"], sdf["scenario_exposure"])
        ).clip(0, 100)
        sdf = sdf.sort_values("investment_priority_score_scenario", ascending=False).reset_index(drop=True)
        sdf["priority_rank_scenario"] = np.arange(1, len(sdf) + 1)

        rows.append(
            sdf[
                [
                    "scenario",
                    "zona_id",
                    "risk_tier",
                    "recommended_intervention",
                    "scenario_exposure",
                    "carga_punta_scenario_mw",
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
            carga_punta_media_mw=("carga_punta_scenario_mw", "mean"),
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
