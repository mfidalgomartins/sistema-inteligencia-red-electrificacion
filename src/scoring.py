from __future__ import annotations

import numpy as np
import pandas as pd


def _minmax(series: pd.Series) -> pd.Series:
    min_v = float(series.min())
    max_v = float(series.max())
    if np.isclose(min_v, max_v):
        return pd.Series(np.full(len(series), 50.0), index=series.index)
    return 100 * (series - min_v) / (max_v - min_v)


def _recommend_action(row: pd.Series) -> str:
    if row["peak_utilization"] > 1.1 and row["flex_support_ratio"] < 0.22:
        return "refuerzo_fisico"
    if row["dg_penetration_ratio"] > 0.42 and row["annual_curtailment_mwh"] > 240:
        return "almacenamiento_bateria"
    if row["outage_hours"] > 14 or row["asset_degradation_index"] > 48:
        return "automatizacion_avanzada"
    return "flexibilidad_contratada"


def build_investment_priorities(
    feeder_features: pd.DataFrame,
    territories: pd.DataFrame,
    flexibility_assets: pd.DataFrame,
    capex_catalog: pd.DataFrame,
) -> pd.DataFrame:
    df = feeder_features.copy()

    territory_fields = territories[
        [
            "territory_id",
            "climate_risk_index",
            "permitting_constraint_index",
            "digital_maturity_index",
            "industrial_index",
        ]
    ]
    df = df.merge(territory_fields, on="territory_id", how="left")
    df = df.merge(flexibility_assets, on="territory_id", how="left")

    df["demand_response_mw"] = df["demand_response_mw"].fillna(0)
    df["battery_power_mw"] = df["battery_power_mw"].fillna(0)
    df["availability_pct"] = df["availability_pct"].fillna(85)

    df["flex_support_ratio"] = (
        (df["demand_response_mw"] + 0.8 * df["battery_power_mw"] * (df["availability_pct"] / 100))
        / df["thermal_limit_mw"]
    ).clip(lower=0)

    df["stress_score"] = (
        0.35 * _minmax(df["peak_utilization"])
        + 0.30 * _minmax(df["congestion_rate"])
        + 0.20 * _minmax(df["forecast_gap_2030_mw"])
        + 0.15 * _minmax(df["anomaly_rate"])
    )

    df["resilience_score"] = (
        0.30 * _minmax(df["outage_hours"])
        + 0.30 * _minmax(df["ens_intensity"])
        + 0.25 * _minmax(df["asset_degradation_index"])
        + 0.15 * _minmax(df["climate_risk_index"])
    )

    df["integration_score"] = (
        0.35 * _minmax(df["projected_incremental_peak_mw"])
        + 0.25 * _minmax(df["hosting_gap_2030_mw"])
        + 0.20 * _minmax(df["forecast_gap_2030_mw"])
        + 0.20 * _minmax(df["dg_penetration_ratio"])
    )

    df["economic_loss_k_eur"] = 90 * df["annual_curtailment_mwh"] + 4000 * df["ens_mwh"]
    df["required_relief_mw"] = np.maximum.reduce(
        [
            np.maximum(df["hosting_gap_2030_mw"], df["forecast_gap_2030_mw"]),
            0.12 * df["congestion_rate"] * df["thermal_limit_mw"],
            np.full(len(df), 0.3),
        ]
    )

    capex_map = capex_catalog.set_index("intervention_type")["capex_unit_k_eur"].to_dict()
    df["recommended_action"] = df.apply(_recommend_action, axis=1)

    df["estimated_capex_k_eur"] = np.select(
        [
            df["recommended_action"] == "refuerzo_fisico",
            df["recommended_action"] == "almacenamiento_bateria",
            df["recommended_action"] == "flexibilidad_contratada",
            df["recommended_action"] == "automatizacion_avanzada",
        ],
        [
            capex_map.get("refuerzo_fisico", 700) * df["required_relief_mw"],
            capex_map.get("almacenamiento_bateria", 520) * df["required_relief_mw"],
            capex_map.get("flexibilidad_contratada", 90) * df["required_relief_mw"],
            capex_map.get("automatizacion_avanzada", 220),
        ],
        default=capex_map.get("refuerzo_fisico", 700) * df["required_relief_mw"],
    )

    relief_factor = np.select(
        [
            df["recommended_action"] == "refuerzo_fisico",
            df["recommended_action"] == "almacenamiento_bateria",
            df["recommended_action"] == "flexibilidad_contratada",
            df["recommended_action"] == "automatizacion_avanzada",
        ],
        [0.80, 0.68, 0.52, 0.46],
        default=0.55,
    )

    df["expected_annual_benefit_k_eur"] = (
        df["economic_loss_k_eur"] * relief_factor
        + 75 * df["congestion_hours"]
        + 18 * df["projected_incremental_peak_mw"]
    )

    df["benefit_capex_ratio"] = df["expected_annual_benefit_k_eur"] / df["estimated_capex_k_eur"].replace(0, np.nan)
    df["benefit_capex_ratio"] = df["benefit_capex_ratio"].replace([np.inf, -np.inf], np.nan).fillna(0)

    df["economic_score"] = (
        0.62 * _minmax(df["benefit_capex_ratio"])
        + 0.38 * _minmax(df["economic_loss_k_eur"])
    )

    df["execution_feasibility_score"] = (
        0.55 * (100 - 100 * df["permitting_constraint_index"])
        + 0.45 * (100 * df["digital_maturity_index"])
    ).clip(lower=0, upper=100)

    need_score = (
        0.34 * df["stress_score"]
        + 0.22 * df["resilience_score"]
        + 0.24 * df["integration_score"]
        + 0.20 * df["economic_score"]
    )

    df["priority_score"] = (0.82 * need_score + 0.18 * df["execution_feasibility_score"]).clip(0, 100)

    df["priority_tier"] = pd.cut(
        df["priority_score"],
        bins=[-np.inf, 40, 55, 70, np.inf],
        labels=["Monitorizar", "Planificar", "Alta", "Crítica"],
    )

    sort_cols = ["priority_score", "stress_score", "integration_score"]
    df = df.sort_values(sort_cols, ascending=False).reset_index(drop=True)
    df["priority_rank"] = np.arange(1, len(df) + 1)

    ordered_cols = [
        "priority_rank",
        "feeder_id",
        "territory_id",
        "priority_tier",
        "priority_score",
        "stress_score",
        "resilience_score",
        "integration_score",
        "economic_score",
        "execution_feasibility_score",
        "recommended_action",
        "required_relief_mw",
        "estimated_capex_k_eur",
        "expected_annual_benefit_k_eur",
        "benefit_capex_ratio",
        "congestion_rate",
        "congestion_hours",
        "annual_peak_net_load_mw",
        "thermal_limit_mw",
        "forecast_peak_2030_mw",
        "forecast_gap_2030_mw",
        "hosting_gap_2030_mw",
        "annual_curtailment_mwh",
        "ens_mwh",
        "anomaly_rate",
        "asset_health_score",
        "permitting_constraint_index",
        "digital_maturity_index",
    ]

    return df[ordered_cols]
