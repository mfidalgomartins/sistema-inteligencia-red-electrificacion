from __future__ import annotations

from typing import Dict, Tuple

import numpy as np
import pandas as pd


DEFAULT_SCENARIOS: Dict[str, Dict[str, float]] = {
    "base": {
        "load_factor": 1.00,
        "dg_factor": 1.00,
        "flex_factor": 1.00,
        "outage_factor": 1.00,
        "capex_factor": 1.00,
    },
    "ev_acelerado": {
        "load_factor": 1.16,
        "dg_factor": 1.00,
        "flex_factor": 1.00,
        "outage_factor": 1.02,
        "capex_factor": 1.04,
    },
    "dg_alta": {
        "load_factor": 1.03,
        "dg_factor": 1.28,
        "flex_factor": 1.05,
        "outage_factor": 1.01,
        "capex_factor": 1.03,
    },
    "flex_storage_push": {
        "load_factor": 1.08,
        "dg_factor": 1.10,
        "flex_factor": 1.45,
        "outage_factor": 0.92,
        "capex_factor": 0.96,
    },
    "estres_climatico": {
        "load_factor": 1.12,
        "dg_factor": 0.95,
        "flex_factor": 0.95,
        "outage_factor": 1.45,
        "capex_factor": 1.08,
    },
    "capex_restringido": {
        "load_factor": 1.06,
        "dg_factor": 1.06,
        "flex_factor": 1.15,
        "outage_factor": 1.10,
        "capex_factor": 1.15,
    },
}


def _tier_from_score(score: float) -> str:
    if score >= 70:
        return "Crítica"
    if score >= 55:
        return "Alta"
    if score >= 40:
        return "Planificar"
    return "Monitorizar"


def run_scenario_engine(
    priorities: pd.DataFrame,
    scenario_params: Dict[str, Dict[str, float]] | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    scenarios = scenario_params or DEFAULT_SCENARIOS
    base = priorities.copy()

    records = []
    for scenario_name, params in scenarios.items():
        flex_relief = 1 + 0.32 * (params["flex_factor"] - 1)
        dg_pressure = 1 + 0.22 * (params["dg_factor"] - 1)

        congestion_adj = base["congestion_rate"] * params["load_factor"] * dg_pressure / flex_relief
        ens_adj = base["ens_mwh"] * params["outage_factor"]
        curtail_adj = base["annual_curtailment_mwh"] * params["dg_factor"] / (1 + 0.28 * (params["flex_factor"] - 1))

        stress_adj = np.clip(base["stress_score"] * (0.72 + 0.45 * params["load_factor"]), 0, 100)
        resilience_adj = np.clip(base["resilience_score"] * (0.74 + 0.42 * params["outage_factor"]), 0, 100)
        integration_adj = np.clip(base["integration_score"] * (0.70 + 0.40 * params["dg_factor"] + 0.16 * params["load_factor"]), 0, 100)

        capex_adj = base["estimated_capex_k_eur"] * params["capex_factor"]
        opex_risk = 0.9 * curtail_adj + 3.4 * ens_adj + 12.5 * (congestion_adj * base["thermal_limit_mw"])
        economic_adj = np.clip(
            0.56 * (opex_risk / np.maximum(capex_adj, 1.0)).rank(pct=True) * 100
            + 0.44 * (opex_risk.rank(pct=True) * 100),
            0,
            100,
        )

        priority_adj = (
            0.34 * stress_adj
            + 0.22 * resilience_adj
            + 0.24 * integration_adj
            + 0.20 * economic_adj
        )
        priority_adj = np.clip(0.82 * priority_adj + 0.18 * base["execution_feasibility_score"], 0, 100)

        scenario_df = base[["feeder_id", "territory_id", "recommended_action"]].copy()
        scenario_df["scenario"] = scenario_name
        scenario_df["congestion_rate_adj"] = congestion_adj.clip(lower=0)
        scenario_df["ens_mwh_adj"] = ens_adj.clip(lower=0)
        scenario_df["curtailment_mwh_adj"] = curtail_adj.clip(lower=0)
        scenario_df["estimated_capex_k_eur_adj"] = capex_adj.clip(lower=0)
        scenario_df["stress_score_adj"] = stress_adj
        scenario_df["resilience_score_adj"] = resilience_adj
        scenario_df["integration_score_adj"] = integration_adj
        scenario_df["economic_score_adj"] = economic_adj
        scenario_df["priority_score_adj"] = priority_adj
        scenario_df = scenario_df.sort_values("priority_score_adj", ascending=False).reset_index(drop=True)
        scenario_df["priority_rank_adj"] = np.arange(1, len(scenario_df) + 1)
        scenario_df["priority_tier_adj"] = scenario_df["priority_score_adj"].map(_tier_from_score)

        records.append(scenario_df)

    scenario_results = pd.concat(records, ignore_index=True)

    summary = (
        scenario_results.groupby("scenario", as_index=False)
        .agg(
            avg_priority_score=("priority_score_adj", "mean"),
            critical_feeders=("priority_tier_adj", lambda x: int((x == "Crítica").sum())),
            high_or_critical=("priority_tier_adj", lambda x: int(x.isin(["Crítica", "Alta"]).sum())),
            total_capex_m_eur=("estimated_capex_k_eur_adj", lambda x: x.sum() / 1000),
            total_ens_mwh=("ens_mwh_adj", "sum"),
            total_curtailment_mwh=("curtailment_mwh_adj", "sum"),
        )
        .sort_values("avg_priority_score", ascending=False)
    )

    return scenario_results, summary
