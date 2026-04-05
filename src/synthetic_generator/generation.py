from __future__ import annotations

import numpy as np
import pandas as pd


def generate_generacion_distribuida(
    time_features: pd.DataFrame,
    zonas_red: pd.DataFrame,
    demanda_horaria: pd.DataFrame,
    macro_hourly: pd.DataFrame,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 223)

    timestamps = time_features["timestamp"].to_numpy()
    hora = time_features["hora"].to_numpy()
    mes = time_features["mes"].to_numpy()
    day_of_year = pd.to_datetime(timestamps).dayofyear.to_numpy()

    demand_zone_hour = (
        demanda_horaria.groupby(["zona_id", "timestamp"], as_index=False)["demanda_mw"].sum()
        .pivot(index="zona_id", columns="timestamp", values="demanda_mw")
        .reindex(index=zonas_red["zona_id"], columns=timestamps, fill_value=0)
    )

    ev_idx = macro_hourly["penetracion_ev_indice"].to_numpy()
    demand_idx = macro_hourly["crecimiento_demanda_indice"].to_numpy()

    techs = ["solar_fotovoltaica", "eolica_distribuida", "cogeneracion"]
    rows = []

    for zona in zonas_red.itertuples(index=False):
        zone_demand = demand_zone_hour.loc[zona.zona_id].to_numpy(dtype=np.float32)

        base_capacity = 22 + 110 * zona.penetracion_generacion_distribuida + 26 * zona.densidad_demanda
        cap_solar = base_capacity * rng.uniform(0.45, 0.65)
        cap_wind = base_capacity * rng.uniform(0.2, 0.38)
        cap_cogen = base_capacity * rng.uniform(0.08, 0.2) * (1.1 if zona.tipo_zona in {"industrial", "mixta"} else 0.75)

        capacities = {
            "solar_fotovoltaica": cap_solar,
            "eolica_distribuida": cap_wind,
            "cogeneracion": cap_cogen,
        }

        for tech in techs:
            cap = capacities[tech]

            if tech == "solar_fotovoltaica":
                daylight = np.clip(np.sin((hora - 6) * np.pi / 12), 0, None)
                seasonal = np.clip(0.53 + 0.37 * np.sin((day_of_year - 80) * 2 * np.pi / 365.25), 0.1, 1.0)
                cf = daylight * seasonal
            elif tech == "eolica_distribuida":
                cf = 0.32 + 0.14 * np.sin((hora + 4) * 2 * np.pi / 24) + 0.09 * np.sin((mes - 1) * 2 * np.pi / 12)
                cf = np.clip(cf + rng.normal(0, 0.07, len(hora)), 0.04, 0.93)
            else:
                base = 0.45 + 0.12 * ((hora >= 7) & (hora <= 21)).astype(float)
                industrial_adj = 1.08 if zona.tipo_zona in {"industrial", "mixta"} else 0.86
                cf = np.clip(base * industrial_adj + rng.normal(0, 0.03, len(hora)), 0.22, 0.88)

            generation = cap * cf * np.clip(0.95 + 0.08 * ev_idx, 0.7, 1.25)

            if tech == "solar_fotovoltaica":
                self_ratio = np.clip(0.42 + 0.12 * zona.densidad_demanda + 0.06 * zona.potencial_flexibilidad, 0.2, 0.78)
            elif tech == "eolica_distribuida":
                self_ratio = np.clip(0.26 + 0.10 * zona.potencial_flexibilidad, 0.12, 0.58)
            else:
                self_ratio = np.clip(0.58 + 0.10 * zona.densidad_demanda, 0.3, 0.88)

            autoconsumo = np.minimum(generation * self_ratio, zone_demand * np.clip(0.14 + 0.5 * self_ratio, 0.06, 0.85))
            vertido = np.maximum(generation - autoconsumo, 0)

            absorcion_local = zone_demand * np.clip(0.02 + 0.08 * zona.potencial_flexibilidad + 0.03 * demand_idx, 0.008, 0.22)
            headroom_red = np.clip(0.16 + 0.28 * (1 - zona.potencial_flexibilidad) + 0.22 * zona.criticidad_territorial, 0.12, 0.58)
            curtailment = np.maximum(vertido - absorcion_local * headroom_red, 0)

            df_local = pd.DataFrame(
                {
                    "timestamp": timestamps,
                    "zona_id": zona.zona_id,
                    "tecnologia": tech,
                    "capacidad_instalada_mw": round(float(cap), 5),
                    "generacion_mw": np.round(generation, 5),
                    "autoconsumo_estimado_mw": np.round(autoconsumo, 5),
                    "vertido_estimado_mw": np.round(vertido, 5),
                    "curtailment_estimado_mw": np.round(curtailment, 5),
                }
            )
            rows.append(df_local)

    return pd.concat(rows, ignore_index=True)
