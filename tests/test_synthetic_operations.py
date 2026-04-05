import numpy as np
import pandas as pd

from src.synthetic_generator.operations import generate_interrupciones_servicio


def test_relacion_congestion_flag_has_real_overlap_when_true():
    ts = pd.date_range("2024-01-01 00:00:00", periods=24 * 10, freq="h")
    demanda_horaria = pd.DataFrame(
        {
            "timestamp": ts,
            "zona_id": ["Z001"] * len(ts),
            "subestacion_id": ["SE001"] * len(ts),
            "alimentador_id": ["AL001"] * len(ts),
            "demanda_mw": np.full(len(ts), 25.0),
        }
    )

    subestaciones = pd.DataFrame(
        {
            "subestacion_id": ["SE001"],
            "zona_id": ["Z001"],
            "capacidad_mw": [90.0],
            "antiguedad_anios": [38],
            "indice_criticidad": [0.92],
        }
    )

    zonas_red = pd.DataFrame(
        {
            "zona_id": ["Z001"],
            "densidad_demanda": [0.95],
        }
    )

    activos_red = pd.DataFrame(
        {
            "activo_id": [f"A{i:03d}" for i in range(20)],
            "subestacion_id": ["SE001"] * 20,
            "probabilidad_fallo_proxy": [0.97] * 20,
            "edad_anios": [42] * 20,
            "criticidad": [0.9] * 20,
        }
    )

    # 200 eventos para elevar la probabilidad de interrupciones relacionadas con congestión.
    starts = pd.date_range("2024-01-01 03:00:00", periods=200, freq="12h")
    eventos_congestion = pd.DataFrame(
        {
            "evento_id": [f"CG{i:05d}" for i in range(len(starts))],
            "timestamp_inicio": starts,
            "timestamp_fin": starts + pd.Timedelta(hours=2),
            "zona_id": ["Z001"] * len(starts),
            "subestacion_id": ["SE001"] * len(starts),
            "alimentador_id": ["AL001"] * len(starts),
            "severidad": ["alta"] * len(starts),
            "energia_afectada_mwh": [5.0] * len(starts),
            "carga_relativa_max": [1.2] * len(starts),
            "causa_principal": ["pico_demanda_general"] * len(starts),
            "impacto_servicio_flag": [1] * len(starts),
        }
    )

    out = generate_interrupciones_servicio(
        demanda_horaria=demanda_horaria,
        subestaciones=subestaciones,
        zonas_red=zonas_red,
        activos_red=activos_red,
        eventos_congestion=eventos_congestion,
        seed=20260328,
    )

    rel = out[out["relacion_congestion_flag"] == 1].copy()
    assert not rel.empty, "Se esperaba al menos una interrupción relacionada con congestión en este setup"

    eventos = eventos_congestion[["subestacion_id", "timestamp_inicio", "timestamp_fin"]].copy()
    for row in rel.itertuples(index=False):
        overlap = (
            (eventos["subestacion_id"] == row.subestacion_id)
            & (eventos["timestamp_inicio"] <= row.timestamp_fin)
            & (eventos["timestamp_fin"] >= row.timestamp_inicio)
        ).any()
        assert overlap
