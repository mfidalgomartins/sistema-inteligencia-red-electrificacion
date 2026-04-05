import pandas as pd

from src.scoring import build_investment_priorities


def test_priority_score_bounds_and_ranking():
    feeder_features = pd.DataFrame(
        {
            "feeder_id": ["F0001", "F0002", "F0003"],
            "territory_id": ["T001", "T001", "T002"],
            "peak_utilization": [0.92, 1.14, 1.08],
            "congestion_rate": [0.05, 0.33, 0.22],
            "forecast_gap_2030_mw": [0.5, 4.8, 2.9],
            "anomaly_rate": [0.01, 0.07, 0.04],
            "outage_hours": [6.5, 18.0, 11.2],
            "ens_intensity": [0.002, 0.015, 0.009],
            "asset_degradation_index": [22.0, 49.0, 37.0],
            "projected_incremental_peak_mw": [1.2, 5.5, 4.1],
            "hosting_gap_2030_mw": [0.0, 3.6, 2.2],
            "dg_penetration_ratio": [0.22, 0.48, 0.35],
            "annual_curtailment_mwh": [40.0, 380.0, 210.0],
            "ens_mwh": [2.5, 18.2, 9.1],
            "congestion_hours": [100, 730, 480],
            "thermal_limit_mw": [21.0, 24.0, 22.0],
            "annual_peak_net_load_mw": [19.5, 29.4, 24.8],
            "forecast_peak_2030_mw": [21.4, 31.8, 27.2],
            "asset_health_score": [78.0, 52.0, 63.0],
        }
    )

    territories = pd.DataFrame(
        {
            "territory_id": ["T001", "T002"],
            "climate_risk_index": [0.41, 0.76],
            "permitting_constraint_index": [0.32, 0.58],
            "digital_maturity_index": [0.69, 0.55],
            "industrial_index": [90.0, 108.0],
        }
    )

    flex = pd.DataFrame(
        {
            "territory_id": ["T001", "T002"],
            "demand_response_mw": [5.5, 6.2],
            "battery_power_mw": [3.4, 4.1],
            "battery_energy_mwh": [7.6, 9.4],
            "response_time_min": [10, 15],
            "availability_pct": [92.0, 89.0],
            "annual_cost_k_eur": [780.0, 920.0],
        }
    )

    capex = pd.DataFrame(
        {
            "intervention_type": [
                "refuerzo_fisico",
                "automatizacion_avanzada",
                "flexibilidad_contratada",
                "almacenamiento_bateria",
            ],
            "capex_unit_k_eur": [710, 230, 95, 530],
        }
    )

    priorities = build_investment_priorities(feeder_features, territories, flex, capex)

    assert priorities["priority_score"].between(0, 100).all()
    assert priorities["priority_rank"].is_unique
    assert priorities["recommended_action"].isin(
        {
            "refuerzo_fisico",
            "automatizacion_avanzada",
            "flexibilidad_contratada",
            "almacenamiento_bateria",
        }
    ).all()
