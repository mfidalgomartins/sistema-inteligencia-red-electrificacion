from src.scenario_engine_v2 import SCENARIOS


def test_required_scenarios_exist():
    required = {
        "crecimiento_acelerado_ev",
        "electrificacion_industrial_intensiva",
        "mayor_penetracion_gd",
        "retraso_capex",
        "despliegue_adicional_flexibilidad",
        "despliegue_adicional_storage",
        "capex_mas_flexibilidad",
        "evento_degradacion_activos",
    }
    assert required.issubset(SCENARIOS.keys())
    assert len(SCENARIOS) == 8


def test_scenario_parameters_are_positive_and_complete():
    expected_keys = {
        "load_factor",
        "congestion_factor",
        "ens_factor",
        "curtailment_factor",
        "flex_gap_factor",
        "risk_cost_factor",
        "capex_factor",
        "priority_factor",
    }
    for params in SCENARIOS.values():
        assert expected_keys == set(params.keys())
        for v in params.values():
            assert v > 0
