from __future__ import annotations

import numpy as np
import pandas as pd

from .config import SyntheticDataConfig


def generate_escenario_macro(config: SyntheticDataConfig) -> pd.DataFrame:
    rng = np.random.default_rng(config.seed + 101)
    fechas = pd.date_range(config.start_timestamp, config.end_timestamp, freq="D")
    scenarios = {
        "base": {"demanda": 1.0, "ev": 1.0, "ind": 1.0, "capex": 1.0},
        "aceleracion_electrificacion": {"demanda": 1.08, "ev": 1.24, "ind": 1.18, "capex": 1.09},
        "estres_climatico": {"demanda": 1.06, "ev": 1.03, "ind": 1.04, "capex": 1.14},
        "restriccion_capex": {"demanda": 1.03, "ev": 1.07, "ind": 1.09, "capex": 1.27},
    }

    year_pos = (fechas - fechas.min()).days / 365.25

    rows = []
    for scenario, mult in scenarios.items():
        demanda_idx = (1 + 0.024 * year_pos) * mult["demanda"] + rng.normal(0, 0.008, len(fechas))
        ev_idx = (1 + 0.11 * year_pos) * mult["ev"] + rng.normal(0, 0.02, len(fechas))
        ind_idx = (1 + 0.07 * year_pos) * mult["ind"] + rng.normal(0, 0.015, len(fechas))
        capex_idx = (1 + 0.017 * year_pos) * mult["capex"] + rng.normal(0, 0.012, len(fechas))

        for i, fecha in enumerate(fechas):
            rows.append(
                {
                    "fecha": fecha,
                    "escenario": scenario,
                    "crecimiento_demanda_indice": round(float(np.clip(demanda_idx[i], 0.7, 1.8)), 5),
                    "penetracion_ev_indice": round(float(np.clip(ev_idx[i], 0.6, 2.6)), 5),
                    "electrificacion_industrial_indice": round(float(np.clip(ind_idx[i], 0.7, 2.3)), 5),
                    "presion_capex_indice": round(float(np.clip(capex_idx[i], 0.7, 2.6)), 5),
                }
            )

    return pd.DataFrame(rows)


def build_base_macro_hourly(time_features: pd.DataFrame, escenario_macro: pd.DataFrame) -> pd.DataFrame:
    base = escenario_macro[escenario_macro["escenario"] == "base"].copy()
    base["fecha"] = pd.to_datetime(base["fecha"])  # defensive

    hourly = time_features[["timestamp", "fecha"]].merge(
        base,
        on="fecha",
        how="left",
    )
    return hourly.drop(columns=["escenario"])
