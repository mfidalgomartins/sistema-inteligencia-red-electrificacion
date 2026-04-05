import pandas as pd

from src.scenario_engine import run_scenario_engine


def test_scenario_engine_outputs_shape_and_non_negative_values():
    priorities = pd.DataFrame(
        {
            "feeder_id": ["F0001", "F0002", "F0003", "F0004"],
            "territory_id": ["T001", "T001", "T002", "T003"],
            "recommended_action": [
                "refuerzo_fisico",
                "flexibilidad_contratada",
                "almacenamiento_bateria",
                "automatizacion_avanzada",
            ],
            "congestion_rate": [0.12, 0.27, 0.22, 0.08],
            "ens_mwh": [8.1, 13.4, 10.0, 4.3],
            "annual_curtailment_mwh": [150.0, 260.0, 330.0, 70.0],
            "stress_score": [55.0, 78.0, 72.0, 44.0],
            "resilience_score": [48.0, 69.0, 66.0, 41.0],
            "integration_score": [57.0, 73.0, 81.0, 39.0],
            "execution_feasibility_score": [71.0, 63.0, 59.0, 76.0],
            "estimated_capex_k_eur": [1200.0, 980.0, 1400.0, 600.0],
            "thermal_limit_mw": [25.0, 22.0, 24.0, 20.0],
        }
    )

    scenario_results, scenario_summary = run_scenario_engine(priorities)

    assert {"base", "ev_acelerado", "dg_alta", "flex_storage_push", "estres_climatico", "capex_restringido"}.issubset(
        set(scenario_results["scenario"].unique())
    )
    assert (scenario_results["priority_score_adj"] >= 0).all()
    assert (scenario_results["estimated_capex_k_eur_adj"] >= 0).all()
    assert not scenario_summary.empty
    assert (scenario_summary["total_capex_m_eur"] > 0).all()
