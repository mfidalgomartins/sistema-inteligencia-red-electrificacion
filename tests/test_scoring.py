import pandas as pd

from grid_intelligence.scoring import _apply_intervention_rules, _tier, _urgency


def test_tier_boundaries():
    assert _tier(39.99) == "bajo"
    assert _tier(40.0) == "medio"
    assert _tier(60.0) == "alto"
    assert _tier(80.0) == "critico"


def test_urgency_boundaries():
    assert _urgency(54.99) == "monitorizacion"
    assert _urgency(55.0) == "planificada"
    assert _urgency(70.0) == "alta"
    assert _urgency(85.0) == "inmediata"


def test_intervention_rules_prioritize_structural_risk_and_assets():
    base = pd.DataFrame(
        [
            {
                "zona_id": "asset",
                "option": "intervencion_operativa",
                "investment_priority_score": 40,
                "asset_exposure_score": 80,
                "congestion_risk_score": 20,
                "ratio_flexibilidad_estres": 0.5,
                "flexibility_gap_score": 20,
                "service_impact_score": 20,
                "storage_efectivo": 0.1,
                "risk_tier": "medio",
            },
            {
                "zona_id": "structural",
                "option": "intervencion_operativa",
                "investment_priority_score": 70,
                "asset_exposure_score": 40,
                "congestion_risk_score": 90,
                "ratio_flexibilidad_estres": 0.1,
                "flexibility_gap_score": 60,
                "service_impact_score": 60,
                "storage_efectivo": 0.1,
                "risk_tier": "alto",
            },
            {
                "zona_id": "storage",
                "option": "flexibilidad",
                "investment_priority_score": 65,
                "asset_exposure_score": 40,
                "congestion_risk_score": 60,
                "ratio_flexibilidad_estres": 0.1,
                "flexibility_gap_score": 85,
                "service_impact_score": 70,
                "storage_efectivo": 0.01,
                "risk_tier": "alto",
            },
            {
                "zona_id": "critical",
                "option": "refuerzo_red",
                "investment_priority_score": 90,
                "asset_exposure_score": 90,
                "congestion_risk_score": 95,
                "ratio_flexibilidad_estres": 0.05,
                "flexibility_gap_score": 95,
                "service_impact_score": 90,
                "storage_efectivo": 0.01,
                "risk_tier": "critico",
            },
        ]
    )

    result = _apply_intervention_rules(base).set_index("zona_id")["recommended_intervention"]

    assert result["asset"] == "sustituir_activos"
    assert result["structural"] == "reforzar_red_local"
    assert result["storage"] == "desplegar_almacenamiento"
    assert result["critical"] == "intervencion_inmediata_prioritaria"
