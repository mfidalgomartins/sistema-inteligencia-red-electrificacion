from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from .config import CONFIG, Config


def build_feature_table(mart_feeder_summary: pd.DataFrame) -> pd.DataFrame:
    df = mart_feeder_summary.copy()

    df["peak_utilization"] = (df["annual_peak_net_load_mw"] / df["thermal_limit_mw"]).clip(lower=0)
    df["flexibility_ratio"] = (df["flex_contract_capacity_mw"] / df["thermal_limit_mw"]).clip(lower=0)
    df["dg_penetration_ratio"] = (df["dg_capacity_mw"] / df["thermal_limit_mw"]).clip(lower=0)
    df["outage_hours"] = df["outage_duration_min"] / 60.0
    df["projected_incremental_peak_mw"] = (
        0.80 * df["ev_peak_2026_2030_mw"]
        + 0.92 * df["industrial_peak_2026_2030_mw"]
        + 0.70 * df["heat_pump_peak_2026_2030_mw"]
    )
    df["projected_peak_2030_mw"] = df["annual_peak_net_load_mw"] + df["projected_incremental_peak_mw"]
    df["hosting_gap_2030_mw"] = (df["projected_peak_2030_mw"] - df["thermal_limit_mw"]).clip(lower=0)
    df["curtailment_intensity"] = df["annual_curtailment_mwh"] / df["total_hours"].replace(0, np.nan)
    df["ens_intensity"] = df["ens_mwh"] / df["total_hours"].replace(0, np.nan)
    df["asset_degradation_index"] = (100 - df["asset_health_score"]).clip(lower=0)

    numeric_cols = [
        "peak_utilization",
        "flexibility_ratio",
        "dg_penetration_ratio",
        "outage_hours",
        "projected_incremental_peak_mw",
        "projected_peak_2030_mw",
        "hosting_gap_2030_mw",
        "curtailment_intensity",
        "ens_intensity",
        "asset_degradation_index",
    ]
    df[numeric_cols] = df[numeric_cols].fillna(0)

    return df


def run_peak_forecasting(mart_feeder_daily: pd.DataFrame) -> pd.DataFrame:
    daily = mart_feeder_daily.copy()
    daily["date"] = pd.to_datetime(daily["date"])
    daily["day_index"] = (daily["date"] - daily["date"].min()).dt.days

    records = []
    for feeder_id, group in daily.groupby("feeder_id", sort=False):
        g = group.sort_values("day_index")
        x = g["day_index"].to_numpy()
        y = g["peak_net_load_mw"].to_numpy()

        if len(x) < 30:
            slope, intercept = 0.0, float(np.mean(y) if len(y) else 0.0)
        else:
            slope, intercept = np.polyfit(x, y, 1)

        future_idx = x.max() + 5 * 365
        trend_forecast = intercept + slope * future_idx

        monthly_profile = g.assign(month=g["date"].dt.month).groupby("month")["peak_net_load_mw"].mean()
        seasonal_uplift = monthly_profile.max() / monthly_profile.mean() if monthly_profile.mean() else 1.0

        residuals = y - (intercept + slope * x)
        residual_std = float(np.std(residuals))

        forecast_peak_2030 = max(trend_forecast * seasonal_uplift + 1.28 * residual_std, 0)

        records.append(
            {
                "feeder_id": feeder_id,
                "forecast_peak_2030_mw": round(float(forecast_peak_2030), 4),
                "forecast_trend_slope": round(float(slope), 6),
                "forecast_uncertainty_mw": round(float(residual_std), 4),
            }
        )

    return pd.DataFrame(records)


def run_anomaly_detection(mart_feeder_daily: pd.DataFrame, random_state: int = 42) -> pd.DataFrame:
    daily = mart_feeder_daily.copy()
    features = daily[
        [
            "avg_demand_mw",
            "peak_demand_mw",
            "peak_net_load_mw",
            "congestion_hours",
            "curtailment_mwh",
            "avg_ev_load_mw",
            "avg_industrial_load_mw",
            "avg_temperature_c",
        ]
    ].fillna(0)

    model = IsolationForest(
        n_estimators=300,
        contamination=0.025,
        random_state=random_state,
        n_jobs=-1,
    )
    model.fit(features)

    daily["anomaly_score"] = -model.score_samples(features)
    daily["is_anomaly"] = (model.predict(features) == -1).astype(int)

    agg = (
        daily.groupby("feeder_id", as_index=False)
        .agg(
            anomaly_days=("is_anomaly", "sum"),
            anomaly_rate=("is_anomaly", "mean"),
            anomaly_score_p95=("anomaly_score", lambda x: np.quantile(x, 0.95)),
        )
        .sort_values("anomaly_rate", ascending=False)
    )

    return agg


def build_model_outputs(
    mart_feeder_summary: pd.DataFrame,
    mart_feeder_daily: pd.DataFrame,
    config: Config = CONFIG,
) -> Dict[str, pd.DataFrame]:
    features = build_feature_table(mart_feeder_summary)
    forecast = run_peak_forecasting(mart_feeder_daily)
    anomalies = run_anomaly_detection(mart_feeder_daily, random_state=config.random_seed)

    feature_enriched = (
        features.merge(forecast, on="feeder_id", how="left")
        .merge(anomalies, on="feeder_id", how="left")
        .fillna(
            {
                "forecast_peak_2030_mw": 0,
                "forecast_trend_slope": 0,
                "forecast_uncertainty_mw": 0,
                "anomaly_days": 0,
                "anomaly_rate": 0,
                "anomaly_score_p95": 0,
            }
        )
    )

    feature_enriched["forecast_gap_2030_mw"] = (
        feature_enriched["forecast_peak_2030_mw"] - feature_enriched["thermal_limit_mw"]
    ).clip(lower=0)

    return {
        "feeder_features": feature_enriched,
        "feeder_forecast": forecast,
        "feeder_anomalies": anomalies,
    }
