from __future__ import annotations

import numpy as np
import pandas as pd

from .config import SyntheticDataConfig


def create_time_features(config: SyntheticDataConfig) -> pd.DataFrame:
    timestamps = pd.date_range(config.start_timestamp, config.end_timestamp, freq="h")
    fechas = timestamps.normalize()

    festivos = pd.to_datetime(
        [
            "2024-01-01",
            "2024-01-06",
            "2024-03-29",
            "2024-05-01",
            "2024-08-15",
            "2024-10-12",
            "2024-11-01",
            "2024-12-06",
            "2024-12-25",
            "2025-01-01",
            "2025-01-06",
            "2025-04-18",
            "2025-05-01",
            "2025-08-15",
            "2025-10-12",
            "2025-11-01",
            "2025-12-06",
            "2025-12-25",
        ]
    )

    dow = timestamps.dayofweek.to_numpy()
    is_holiday = np.isin(fechas.values.astype("datetime64[D]"), festivos.values.astype("datetime64[D]"))
    tipo_dia = np.where(is_holiday, "festivo", np.where(dow >= 5, "fin_semana", "laborable"))

    day_of_year = timestamps.dayofyear.to_numpy()
    mes = timestamps.month.to_numpy()
    hora = timestamps.hour.to_numpy()

    factor_estacional = (
        1.0
        + 0.10 * np.cos((day_of_year - 15) * 2 * np.pi / 365.25)
        + 0.07 * np.exp(-((mes - 7) / 2.0) ** 2)
    )

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "fecha": fechas,
            "mes": mes.astype(np.int16),
            "hora": hora.astype(np.int16),
            "dia_semana": dow.astype(np.int16),
            "tipo_dia": tipo_dia,
            "factor_estacional": factor_estacional.astype(np.float32),
        }
    )


def generate_demanda_ev(
    time_features: pd.DataFrame,
    zonas_red: pd.DataFrame,
    macro_hourly: pd.DataFrame,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 151)

    hora = time_features["hora"].to_numpy()
    tipo_dia = time_features["tipo_dia"].to_numpy()
    ev_idx = macro_hourly["penetracion_ev_indice"].to_numpy()

    perfiles = {
        "residencial_nocturna": np.exp(-((hora - 22) / 3.3) ** 2) + 0.25 * np.exp(-((hora - 8) / 2.7) ** 2),
        "publica_rapida": np.exp(-((hora - 14) / 3.1) ** 2) + 0.35 * np.exp(-((hora - 19) / 2.4) ** 2),
        "laboral_destino": np.exp(-((hora - 10) / 3.0) ** 2) + 0.25 * np.exp(-((hora - 17) / 2.8) ** 2),
    }
    dominantes = {
        "residencial_nocturna": "20:00-00:00",
        "publica_rapida": "12:00-16:00",
        "laboral_destino": "09:00-18:00",
    }

    rows = []
    for zona in zonas_red.itertuples(index=False):
        base_pen = np.clip(0.045 + 0.22 * zona.densidad_demanda + 0.26 * zona.tension_crecimiento_demanda + rng.normal(0, 0.015), 0.02, 0.65)
        ev_pool_mw = 1.8 + 16.5 * zona.densidad_demanda + 7.0 * zona.tension_crecimiento_demanda

        for tipo_recarga, profile in perfiles.items():
            day_adj = np.where(tipo_dia == "laborable", 1.0, np.where(tipo_dia == "fin_semana", 1.12, 0.93))
            if tipo_recarga == "laboral_destino":
                day_adj = np.where(tipo_dia == "laborable", 1.18, 0.52)
            elif tipo_recarga == "publica_rapida":
                day_adj = np.where(tipo_dia == "laborable", 1.0, 1.08)

            penetration = np.clip(base_pen * ev_idx * (1 + rng.normal(0, 0.01, len(ev_idx))), 0.01, 0.95)
            demanda = ev_pool_mw * penetration * profile * day_adj
            demanda = np.clip(demanda * rng.uniform(0.88, 1.12), 0, None)

            df_local = pd.DataFrame(
                {
                    "timestamp": time_features["timestamp"].values,
                    "zona_id": zona.zona_id,
                    "tipo_recarga": tipo_recarga,
                    "demanda_ev_mw": np.round(demanda, 5),
                    "penetracion_ev": np.round(penetration, 5),
                    "horario_recarga_dominante": dominantes[tipo_recarga],
                }
            )
            rows.append(df_local)

    demanda_ev = pd.concat(rows, ignore_index=True)
    return demanda_ev


def generate_demanda_electrificacion_industrial(
    time_features: pd.DataFrame,
    zonas_red: pd.DataFrame,
    macro_hourly: pd.DataFrame,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 167)

    hora = time_features["hora"].to_numpy()
    dow = time_features["dia_semana"].to_numpy()
    tipo_dia = time_features["tipo_dia"].to_numpy()
    ind_idx = macro_hourly["electrificacion_industrial_indice"].to_numpy()

    cluster_map = {
        "industrial": ["metalurgia", "quimica", "ceramica"],
        "urbana": ["logistica_urbana", "alimentacion"],
        "rural": ["agroindustria", "bombeo_hidrico"],
        "mixta": ["automocion", "frio_industrial", "logistica"],
    }

    perfil_map = {
        "metalurgia": "3_turnos_24x7",
        "quimica": "continuo_24x7",
        "ceramica": "doble_turno",
        "logistica_urbana": "extensivo_diurno",
        "alimentacion": "doble_turno",
        "agroindustria": "diurno_estacional",
        "bombeo_hidrico": "valle_nocturno",
        "automocion": "triple_turno",
        "frio_industrial": "continuo_24x7",
        "logistica": "extensivo_diurno",
    }

    def profile_from_name(name: str) -> np.ndarray:
        if name in {"3_turnos_24x7", "continuo_24x7", "triple_turno"}:
            base = 0.84 + 0.14 * np.sin((hora - 7) * 2 * np.pi / 24)
            weekend_adj = np.where(dow >= 5, 0.88, 1.0)
            return np.clip(base * weekend_adj, 0.55, 1.25)
        if name == "doble_turno":
            day = ((hora >= 6) & (hora <= 22)).astype(float)
            weekend_adj = np.where(dow >= 5, 0.68, 1.0)
            return np.clip((0.22 + 0.84 * day) * weekend_adj, 0.1, 1.2)
        if name == "extensivo_diurno":
            day = np.exp(-((hora - 13) / 4.8) ** 2)
            weekend_adj = np.where(dow >= 5, 0.52, 1.0)
            return np.clip((0.15 + 1.0 * day) * weekend_adj, 0.05, 1.15)
        if name == "diurno_estacional":
            day = np.exp(-((hora - 12) / 4.6) ** 2)
            seasonal = 0.84 + 0.22 * np.sin((time_features["mes"].to_numpy() - 4) * 2 * np.pi / 12)
            return np.clip((0.18 + 0.86 * day) * seasonal, 0.08, 1.25)
        if name == "valle_nocturno":
            night = np.exp(-((hora - 2) / 3.5) ** 2)
            return np.clip(0.12 + 1.05 * night, 0.07, 1.2)
        return np.ones_like(hora, dtype=float)

    rows = []
    for zona in zonas_red.itertuples(index=False):
        clusters = cluster_map[zona.tipo_zona]
        cluster = clusters[int(rng.integers(0, len(clusters)))]
        perfil = perfil_map[cluster]
        profile = profile_from_name(perfil)

        industrial_factor = 0.82 if zona.tipo_zona == "industrial" else 0.62 if zona.tipo_zona == "mixta" else 0.45
        base_power = 0.8 + 9.2 * industrial_factor + 4.5 * zona.tension_crecimiento_demanda

        holiday_adj = np.where(tipo_dia == "festivo", 0.64, 1.0)
        demanda = base_power * ind_idx * profile * holiday_adj * rng.uniform(0.9, 1.12)
        demanda = np.clip(demanda + rng.normal(0, 0.05, len(demanda)), 0, None)

        elasticidad = np.clip(0.2 + 0.55 * zona.potencial_flexibilidad + rng.normal(0, 0.05, len(demanda)), 0.05, 0.98)

        rows.append(
            pd.DataFrame(
                {
                    "timestamp": time_features["timestamp"].values,
                    "zona_id": zona.zona_id,
                    "cluster_industrial": cluster,
                    "demanda_industrial_adicional_mw": np.round(demanda, 5),
                    "perfil_operativo": perfil,
                    "elasticidad_flexibilidad_proxy": np.round(elasticidad, 5),
                }
            )
        )

    return pd.concat(rows, ignore_index=True)


def generate_demanda_horaria(
    time_features: pd.DataFrame,
    zonas_red: pd.DataFrame,
    subestaciones: pd.DataFrame,
    alimentadores: pd.DataFrame,
    demanda_ev: pd.DataFrame,
    demanda_industrial: pd.DataFrame,
    macro_hourly: pd.DataFrame,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 181)

    feeder_map = alimentadores.merge(subestaciones[["subestacion_id", "zona_id"]], on="subestacion_id", how="left")
    zone_lookup = zonas_red.set_index("zona_id")

    timestamps = time_features["timestamp"].to_numpy()
    h = len(timestamps)
    n_feeders = len(feeder_map)

    zone_ids = zonas_red["zona_id"].tolist()
    zone_index = {z: i for i, z in enumerate(zone_ids)}

    ev_zone_hour = (
        demanda_ev.groupby(["zona_id", "timestamp"], as_index=False)["demanda_ev_mw"].sum()
        .pivot(index="zona_id", columns="timestamp", values="demanda_ev_mw")
        .reindex(index=zone_ids, columns=timestamps, fill_value=0)
        .to_numpy(dtype=np.float32)
    )
    ind_zone_hour = (
        demanda_industrial.groupby(["zona_id", "timestamp"], as_index=False)["demanda_industrial_adicional_mw"].sum()
        .pivot(index="zona_id", columns="timestamp", values="demanda_industrial_adicional_mw")
        .reindex(index=zone_ids, columns=timestamps, fill_value=0)
        .to_numpy(dtype=np.float32)
    )

    feeder_map["base_weight"] = feeder_map["carga_base_esperada"] / feeder_map.groupby("zona_id")["carga_base_esperada"].transform("sum")
    ev_coef = np.where(
        feeder_map["tipo_red"].isin(["mallada_urbana", "subterranea"]),
        1.18,
        np.where(feeder_map["tipo_red"].isin(["aerea_rural", "troncal_larga"]), 0.72, 1.0),
    )
    ind_coef = np.where(
        feeder_map["tipo_red"].isin(["industrial_dedicada", "aerea_reforzada"]),
        1.35,
        np.where(feeder_map["tipo_red"].isin(["aerea_rural", "troncal_larga"]), 0.62, 0.95),
    )
    feeder_map["weight_ev"] = feeder_map["base_weight"] * ev_coef
    feeder_map["weight_ev"] = feeder_map["weight_ev"] / feeder_map.groupby("zona_id")["weight_ev"].transform("sum")
    feeder_map["weight_ind"] = feeder_map["base_weight"] * ind_coef
    feeder_map["weight_ind"] = feeder_map["weight_ind"] / feeder_map.groupby("zona_id")["weight_ind"].transform("sum")

    feeder_idx = np.repeat(np.arange(n_feeders), h)
    time_idx = np.tile(np.arange(h), n_feeders)

    feeder_zone = feeder_map["zona_id"].to_numpy()
    zone_idx_feeder = np.array([zone_index[z] for z in feeder_zone], dtype=np.int16)
    zone_idx_expanded = zone_idx_feeder[feeder_idx]

    capacity = feeder_map["capacidad_mw"].to_numpy(dtype=np.float32)
    base_load = feeder_map["carga_base_esperada"].to_numpy(dtype=np.float32)
    exposicion = feeder_map["exposicion_climatica"].to_numpy(dtype=np.float32)

    capacity_expanded = capacity[feeder_idx]
    base_load_expanded = base_load[feeder_idx]

    hora = time_features["hora"].to_numpy()[time_idx]
    dow = time_features["dia_semana"].to_numpy()[time_idx]
    tipo_dia = time_features["tipo_dia"].to_numpy()[time_idx]
    mes = time_features["mes"].to_numpy()[time_idx]

    factor_estacional = time_features["factor_estacional"].to_numpy(dtype=np.float32)[time_idx]
    growth_idx = macro_hourly["crecimiento_demanda_indice"].to_numpy(dtype=np.float32)[time_idx]

    feeder_type_expanded = feeder_map["tipo_red"].to_numpy()[feeder_idx]
    is_ind = np.isin(feeder_type_expanded, ["industrial_dedicada", "aerea_reforzada"])
    is_rural = np.isin(feeder_type_expanded, ["aerea_rural", "troncal_larga"])

    profile_urban = 0.72 + 0.20 * np.sin((hora - 7) * np.pi / 12) + 0.33 * np.exp(-((hora - 20) / 3.3) ** 2)
    profile_ind = 0.58 + 0.62 * ((hora >= 7) & (hora <= 20) & (dow < 5)).astype(float)
    profile_rural = 0.67 + 0.16 * np.exp(-((hora - 8) / 3.0) ** 2) + 0.22 * np.exp(-((hora - 21) / 3.5) ** 2)
    daily_profile = np.where(is_ind, profile_ind, np.where(is_rural, profile_rural, profile_urban))

    week_factor = np.where(dow < 5, 1.0, np.where(is_ind, 0.74, 0.9))
    festivo_factor = np.where(tipo_dia == "festivo", 0.76, 1.0)

    region_temp_params = {
        "Norte": (12.0, 8.5),
        "Noroeste": (12.5, 8.0),
        "Centro": (14.0, 10.5),
        "Nordeste": (14.5, 9.8),
        "Levante": (16.3, 8.7),
        "Sur": (17.5, 9.5),
        "Oeste": (15.2, 9.2),
        "Insular": (18.2, 6.5),
    }

    zone_temp = np.zeros((len(zone_ids), h), dtype=np.float32)
    zone_humidity = np.zeros((len(zone_ids), h), dtype=np.float32)
    doy = time_features["timestamp"].dt.dayofyear.to_numpy()

    for i, zona in enumerate(zonas_red.itertuples(index=False)):
        temp_base, amp = region_temp_params[zona.region_operativa]
        seasonal = amp * np.sin((doy - 80) * 2 * np.pi / 365.25)
        diurnal = 1.8 * np.sin((time_features["hora"].to_numpy() - 14) * 2 * np.pi / 24)
        temp = temp_base + seasonal + diurnal + rng.normal(0, 1.7, h)
        humidity = 62 + 18 * np.sin((doy + 15) * 2 * np.pi / 365.25) - 0.8 * (temp - temp_base) + rng.normal(0, 4.5, h)
        zone_temp[i] = np.clip(temp, -7, 45).astype(np.float32)
        zone_humidity[i] = np.clip(humidity, 10, 98).astype(np.float32)

    temperature = zone_temp[zone_idx_expanded, time_idx]
    humidity = zone_humidity[zone_idx_expanded, time_idx]

    zona_densidad = zonas_red.set_index("zona_id")["densidad_demanda"].to_dict()
    dens_arr = np.array([zona_densidad[z] for z in feeder_zone], dtype=np.float32)
    dens_expanded = dens_arr[feeder_idx]

    weather_sensitivity = 0.45 + 0.55 * dens_expanded + 0.18 * exposicion[feeder_idx]
    weather_component = weather_sensitivity * (0.045 * np.maximum(0, 18 - temperature) + 0.06 * np.maximum(0, temperature - 27))

    ev_component = ev_zone_hour[zone_idx_expanded, time_idx] * feeder_map["weight_ev"].to_numpy(dtype=np.float32)[feeder_idx]
    ind_component = ind_zone_hour[zone_idx_expanded, time_idx] * feeder_map["weight_ind"].to_numpy(dtype=np.float32)[feeder_idx]

    noise = rng.normal(0, 0.03, len(feeder_idx)) * capacity_expanded

    demand = (
        base_load_expanded * daily_profile * week_factor * festivo_factor * factor_estacional * growth_idx
        + ev_component
        + ind_component
        + weather_component
        + noise
    )
    demand = np.clip(demand, 0.1 * capacity_expanded, 1.42 * capacity_expanded)

    reactive = np.clip(demand * (0.22 + 0.08 * np.sin((hora - 3) * 2 * np.pi / 24) + rng.normal(0, 0.015, len(demand))), 0.01, None)

    utilization = demand / np.maximum(capacity_expanded, 1e-3)
    hora_punta = ((hora >= 19) & (hora <= 22)) | (utilization > 0.92)
    tension_proxy = np.clip(1.03 - 0.068 * utilization + rng.normal(0, 0.008, len(utilization)), 0.87, 1.08)

    df = pd.DataFrame(
        {
            "timestamp": time_features["timestamp"].values[time_idx],
            "zona_id": pd.Categorical(feeder_zone[feeder_idx]),
            "subestacion_id": pd.Categorical(feeder_map["subestacion_id"].to_numpy()[feeder_idx]),
            "alimentador_id": pd.Categorical(feeder_map["alimentador_id"].to_numpy()[feeder_idx]),
            "demanda_mw": np.round(demand, 5),
            "demanda_reactiva_proxy": np.round(reactive, 5),
            "temperatura": np.round(temperature, 4),
            "humedad": np.round(humidity, 4),
            "tipo_dia": tipo_dia,
            "mes": mes.astype(np.int16),
            "hora": hora.astype(np.int16),
            "factor_estacional": np.round(factor_estacional, 5),
            "hora_punta_flag": hora_punta.astype(np.int8),
            "tension_sistema_proxy": np.round(tension_proxy, 5),
        }
    )

    return df
