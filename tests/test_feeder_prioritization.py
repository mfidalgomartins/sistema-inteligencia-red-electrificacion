import pandas as pd
import pytest

from grid_intelligence.feeder_prioritization import build_feeder_priorities


def _feeder_metrics() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "zona_id": "Z1",
                "subestacion_id": "S1",
                "alimentador_id": "A1",
                "capacidad_mw": 100.0,
                "demanda_punta_mw": 118.0,
                "carga_relativa_max": 1.18,
                "carga_relativa_media": 0.90,
                "horas_observadas": 100,
                "horas_congestion": 30,
                "exposicion_media": 55.0,
                "probabilidad_fallo_ajustada_media": 0.20,
                "ens_asociada_mwh": 8.0,
            },
            {
                "zona_id": "Z2",
                "subestacion_id": "S2",
                "alimentador_id": "A2",
                "capacidad_mw": 100.0,
                "demanda_punta_mw": 95.0,
                "carga_relativa_max": 0.95,
                "carga_relativa_media": 0.70,
                "horas_observadas": 100,
                "horas_congestion": 5,
                "exposicion_media": 90.0,
                "probabilidad_fallo_ajustada_media": 0.80,
                "ens_asociada_mwh": 12.0,
            },
            {
                "zona_id": "Z3",
                "subestacion_id": "S3",
                "alimentador_id": "A3",
                "capacidad_mw": 100.0,
                "demanda_punta_mw": 80.0,
                "carga_relativa_max": 0.80,
                "carga_relativa_media": 0.55,
                "horas_observadas": 100,
                "horas_congestion": 0,
                "exposicion_media": 20.0,
                "probabilidad_fallo_ajustada_media": 0.05,
                "ens_asociada_mwh": 0.0,
            },
        ]
    )


def test_build_feeder_priorities_is_bounded_ranked_and_explainable():
    zone_scoring = pd.DataFrame(
        {
            "zona_id": ["Z1", "Z2", "Z3"],
            "investment_priority_score": [90.0, 70.0, 25.0],
        }
    )

    result = build_feeder_priorities(_feeder_metrics(), zone_scoring).set_index("alimentador_id")

    assert result["puntuacion_prioridad"].between(0, 100).all()
    assert sorted(result["ranking_prioridad"].tolist()) == [1, 2, 3]
    assert result.loc["A1", "accion_recomendada"] == "refuerzo_selectivo"
    assert result.loc["A2", "accion_recomendada"] == "renovacion_activos"
    assert result.loc["A3", "accion_recomendada"] == "flexibilidad_local_y_monitorizacion"
    assert result.loc["A1", "alivio_requerido_mw"] == 18.0


def test_build_feeder_priorities_rejects_missing_zone_score():
    zone_scoring = pd.DataFrame({"zona_id": ["Z1"], "investment_priority_score": [90.0]})

    with pytest.raises(ValueError, match="Zonas sin puntuación"):
        build_feeder_priorities(_feeder_metrics(), zone_scoring)
