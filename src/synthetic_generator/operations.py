from __future__ import annotations

import numpy as np
import pandas as pd


def generate_recursos_flexibilidad(zonas_red: pd.DataFrame, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 271)
    tipos = ["respuesta_demanda", "agregador_ev", "gestion_cargas_termicas", "microred_industrial"]

    rows = []
    for zona in zonas_red.itertuples(index=False):
        n_resources = int(np.clip(np.round(1.5 + 2.2 * zona.potencial_flexibilidad + rng.normal(0, 0.35)), 2, 4))
        tipos_local = rng.choice(tipos, size=n_resources, replace=False)

        for tipo in tipos_local:
            cap = np.clip(
                1.8 + 22 * zona.potencial_flexibilidad + 6.8 * zona.densidad_demanda + rng.normal(0, 1.8),
                0.6,
                52,
            )
            coste = np.clip(
                58 + 85 * (1 - zona.potencial_flexibilidad) + 22 * zona.criticidad_territorial + rng.normal(0, 8),
                28,
                320,
            )
            t_resp = int(np.clip(np.round(3 + 42 * (1 - zona.potencial_flexibilidad) + rng.normal(0, 3.5)), 2, 90))
            disp = np.clip(0.78 + 0.18 * zona.potencial_flexibilidad + rng.normal(0, 0.03), 0.55, 0.98)
            fiab = np.clip(0.74 + 0.22 * zona.potencial_flexibilidad + rng.normal(0, 0.03), 0.5, 0.99)
            mad = np.clip(0.4 + 0.45 * zona.potencial_flexibilidad + 0.12 * zona.densidad_demanda + rng.normal(0, 0.05), 0.1, 0.99)

            rows.append(
                {
                    "recurso_id": f"RF{len(rows) + 1:05d}",
                    "zona_id": zona.zona_id,
                    "tipo_recurso": tipo,
                    "capacidad_flexible_mw": round(float(cap), 5),
                    "coste_activacion_eur_mwh": round(float(coste), 4),
                    "tiempo_respuesta_min": t_resp,
                    "disponibilidad_media": round(float(disp), 5),
                    "fiabilidad_activacion": round(float(fiab), 5),
                    "madurez_operativa": round(float(mad), 5),
                }
            )

    return pd.DataFrame(rows)


def generate_almacenamiento_distribuido(zonas_red: pd.DataFrame, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 283)
    rows = []

    for zona in zonas_red.itertuples(index=False):
        n_storage = int(np.clip(np.round(0.6 + 2.0 * zona.penetracion_generacion_distribuida + rng.normal(0, 0.4)), 1, 3))
        for _ in range(n_storage):
            pot = np.clip(0.8 + 10 * zona.penetracion_generacion_distribuida + 6 * zona.potencial_flexibilidad + rng.normal(0, 1.4), 0.4, 28)
            dur = np.clip(rng.normal(2.5 + 0.6 * zona.criticidad_territorial, 0.7), 1.2, 5.0)
            energy = pot * dur
            eff = np.clip(0.8 + 0.12 * zona.potencial_flexibilidad + rng.normal(0, 0.02), 0.72, 0.96)
            coste_op = np.clip(11 + 16 * (1 - zona.potencial_flexibilidad) + rng.normal(0, 2), 6, 45)
            disp = np.clip(0.84 + 0.1 * zona.potencial_flexibilidad + rng.normal(0, 0.03), 0.65, 0.99)

            rows.append(
                {
                    "storage_id": f"ST{len(rows) + 1:05d}",
                    "zona_id": zona.zona_id,
                    "capacidad_energia_mwh": round(float(energy), 5),
                    "capacidad_potencia_mw": round(float(pot), 5),
                    "eficiencia_roundtrip": round(float(eff), 5),
                    "coste_operacion_proxy": round(float(coste_op), 5),
                    "disponibilidad_media": round(float(disp), 5),
                }
            )

    return pd.DataFrame(rows)


def generate_eventos_congestion(
    demanda_horaria: pd.DataFrame,
    subestaciones: pd.DataFrame,
    alimentadores: pd.DataFrame,
    zonas_red: pd.DataFrame,
    recursos_flexibilidad: pd.DataFrame,
    almacenamiento_distribuido: pd.DataFrame,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 307)

    feeder_map = alimentadores.merge(subestaciones[["subestacion_id", "zona_id"]], on="subestacion_id", how="left")
    zone_flex = recursos_flexibilidad.groupby("zona_id", as_index=False)["capacidad_flexible_mw"].sum()
    zone_storage = almacenamiento_distribuido.groupby("zona_id", as_index=False)["capacidad_potencia_mw"].sum()

    zone_peak_demand = demanda_horaria.groupby("zona_id", as_index=False)["demanda_mw"].max().rename(columns={"demanda_mw": "peak_zona_mw"})
    flex_cov = (
        zone_peak_demand.merge(zone_flex, on="zona_id", how="left")
        .merge(zone_storage, on="zona_id", how="left")
        .fillna({"capacidad_flexible_mw": 0.0, "capacidad_potencia_mw": 0.0})
    )
    flex_cov["coverage"] = (
        flex_cov["capacidad_flexible_mw"] + 0.82 * flex_cov["capacidad_potencia_mw"]
    ) / np.maximum(flex_cov["peak_zona_mw"], 1e-6)

    df = demanda_horaria.merge(
        feeder_map[["alimentador_id", "subestacion_id", "zona_id", "capacidad_mw", "tipo_red"]],
        on=["alimentador_id", "subestacion_id", "zona_id"],
        how="left",
    ).merge(
        flex_cov[["zona_id", "coverage"]],
        on="zona_id",
        how="left",
    )

    df["coverage"] = df["coverage"].fillna(0.0)
    util = df["demanda_mw"] / np.maximum(df["capacidad_mw"], 1e-6)
    threshold = np.clip(0.91 + 0.11 * df["coverage"], 0.9, 1.03)

    overload_mw = np.maximum(df["demanda_mw"] - df["capacidad_mw"], 0) + 0.35 * np.maximum(df["demanda_mw"] - threshold * df["capacidad_mw"], 0)
    prob_burst = np.clip((util - threshold) * (1.6 - df["coverage"]) * 2.2, 0, 0.95)
    random_gate = rng.random(len(df))
    congested_flag = (util > threshold) & (random_gate < (0.55 + prob_burst))

    flagged = df.loc[congested_flag, ["timestamp", "zona_id", "subestacion_id", "alimentador_id", "tipo_red", "hora", "demanda_mw", "capacidad_mw"]].copy()
    if flagged.empty:
        return pd.DataFrame(
            columns=[
                "evento_id",
                "timestamp_inicio",
                "timestamp_fin",
                "zona_id",
                "subestacion_id",
                "alimentador_id",
                "severidad",
                "energia_afectada_mwh",
                "carga_relativa_max",
                "causa_principal",
                "impacto_servicio_flag",
            ]
        )

    flagged["util"] = flagged["demanda_mw"] / np.maximum(flagged["capacidad_mw"], 1e-6)
    flagged["overload_mw"] = np.maximum(flagged["demanda_mw"] - flagged["capacidad_mw"], 0) + 0.28 * np.maximum(flagged["demanda_mw"] - 0.95 * flagged["capacidad_mw"], 0)
    flagged = flagged.sort_values(["alimentador_id", "timestamp"]).reset_index(drop=True)

    prev_ts = flagged.groupby("alimentador_id")["timestamp"].shift(1)
    is_new = prev_ts.isna() | ((flagged["timestamp"] - prev_ts).dt.total_seconds() > 3600)
    flagged["grp"] = is_new.groupby(flagged["alimentador_id"]).cumsum()

    grouped = (
        flagged.groupby(["alimentador_id", "grp"], as_index=False)
        .agg(
            timestamp_inicio=("timestamp", "min"),
            timestamp_fin=("timestamp", "max"),
            zona_id=("zona_id", "first"),
            subestacion_id=("subestacion_id", "first"),
            tipo_red=("tipo_red", "first"),
            carga_relativa_max=("util", "max"),
            energia_afectada_mwh=("overload_mw", "sum"),
            hora_media=("hora", "mean"),
            n_horas=("timestamp", "count"),
        )
        .sort_values("timestamp_inicio")
        .reset_index(drop=True)
    )

    grouped["duracion_h"] = (grouped["timestamp_fin"] - grouped["timestamp_inicio"]).dt.total_seconds() / 3600 + 1
    grouped["energia_afectada_mwh"] = grouped["energia_afectada_mwh"] * rng.uniform(0.85, 1.2, len(grouped))

    sev_conditions = [
        (grouped["carga_relativa_max"] >= 1.28) | (grouped["duracion_h"] >= 6),
        (grouped["carga_relativa_max"] >= 1.15) | (grouped["duracion_h"] >= 3),
        (grouped["carga_relativa_max"] >= 1.05),
    ]
    grouped["severidad"] = np.select(sev_conditions, ["critica", "alta", "media"], default="baja")

    cause = []
    for row in grouped.itertuples(index=False):
        if row.hora_media >= 18 and row.hora_media <= 23:
            c = "pico_ev_residencial"
        elif row.tipo_red in {"industrial_dedicada", "aerea_reforzada"}:
            c = "electrificacion_industrial"
        elif row.carga_relativa_max > 1.24:
            c = "capacidad_termica_insuficiente"
        else:
            c = rng.choice(
                ["restriccion_topologica", "indisponibilidad_flex", "mantenimiento_diferido", "pico_demanda_general"],
                p=[0.24, 0.22, 0.18, 0.36],
            )
        cause.append(c)
    grouped["causa_principal"] = cause

    grouped["impacto_servicio_flag"] = (
        (grouped["severidad"].isin(["alta", "critica"])) | (grouped["energia_afectada_mwh"] > grouped["energia_afectada_mwh"].quantile(0.82))
    ).astype(np.int8)

    grouped["evento_id"] = [f"CG{idx:07d}" for idx in range(1, len(grouped) + 1)]
    grouped["energia_afectada_mwh"] = grouped["energia_afectada_mwh"].round(5)
    grouped["carga_relativa_max"] = grouped["carga_relativa_max"].round(5)

    return grouped[
        [
            "evento_id",
            "timestamp_inicio",
            "timestamp_fin",
            "zona_id",
            "subestacion_id",
            "alimentador_id",
            "severidad",
            "energia_afectada_mwh",
            "carga_relativa_max",
            "causa_principal",
            "impacto_servicio_flag",
        ]
    ]


def generate_interrupciones_servicio(
    demanda_horaria: pd.DataFrame,
    subestaciones: pd.DataFrame,
    zonas_red: pd.DataFrame,
    activos_red: pd.DataFrame,
    eventos_congestion: pd.DataFrame,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 337)

    time_pool = pd.Series(pd.to_datetime(demanda_horaria["timestamp"].unique())).sort_values().to_numpy()
    zone_density = zonas_red.set_index("zona_id")["densidad_demanda"].to_dict()
    sub_zone = subestaciones.set_index("subestacion_id")["zona_id"].to_dict()

    risk_sub = (
        activos_red.groupby("subestacion_id", as_index=False)
        .agg(
            prob_media=("probabilidad_fallo_proxy", "mean"),
            edad_media=("edad_anios", "mean"),
            criticidad_media=("criticidad", "mean"),
        )
        .merge(subestaciones[["subestacion_id", "capacidad_mw", "antiguedad_anios", "indice_criticidad"]], on="subestacion_id", how="left")
    )

    cong_sub = eventos_congestion.groupby("subestacion_id", as_index=False).size().rename(columns={"size": "n_congestion"})
    risk_sub = risk_sub.merge(cong_sub, on="subestacion_id", how="left").fillna({"n_congestion": 0})

    avg_load_sub = demanda_horaria.groupby("subestacion_id", as_index=False)["demanda_mw"].mean().rename(columns={"demanda_mw": "avg_load_mw"})
    risk_sub = risk_sub.merge(avg_load_sub, on="subestacion_id", how="left")

    # Mapa de ventanas de congestión por subestación.
    # Se usa para reconciliar la bandera `relacion_congestion_flag` con solape temporal real.
    if not eventos_congestion.empty:
        cong_windows = eventos_congestion[["subestacion_id", "timestamp_inicio", "timestamp_fin"]].copy()
        cong_windows["timestamp_inicio"] = pd.to_datetime(cong_windows["timestamp_inicio"])
        cong_windows["timestamp_fin"] = pd.to_datetime(cong_windows["timestamp_fin"])
        cong_by_sub = {
            sub_id: sub_df.reset_index(drop=True)
            for sub_id, sub_df in cong_windows.groupby("subestacion_id", as_index=False)
        }
    else:
        cong_by_sub = {}

    rows = []
    for row in risk_sub.itertuples(index=False):
        zona_id = sub_zone[row.subestacion_id]
        dens = zone_density[zona_id]
        windows_sub = cong_by_sub.get(row.subestacion_id, pd.DataFrame(columns=["timestamp_inicio", "timestamp_fin"]))

        intensity = (
            1.8
            + 11.5 * row.prob_media
            + 0.055 * row.n_congestion
            + 0.6 * row.indice_criticidad
            + 0.012 * row.antiguedad_anios
        )
        n_interruptions = int(np.clip(rng.poisson(max(intensity, 0.5)), 1, 42))

        for _ in range(n_interruptions):
            p_rel_cong = float(np.clip(0.14 + 0.42 * min(row.n_congestion / 200, 1) + 0.25 * row.prob_media, 0.05, 0.9))
            rel_cong = int(rng.random() < p_rel_cong)

            if rel_cong and not windows_sub.empty:
                # Si la interrupción se marca como asociada a congestión, forzamos un inicio dentro
                # de una ventana real de congestión para mantener coherencia del dataset.
                w = windows_sub.iloc[int(rng.integers(0, len(windows_sub)))]
                w_start = pd.Timestamp(w["timestamp_inicio"])
                w_end = pd.Timestamp(w["timestamp_fin"])
                w_h = max((w_end - w_start).total_seconds() / 3600.0, 0.08)
                start_offset_h = float(rng.uniform(0.0, max(w_h - 0.05, 0.02)))
                t0 = w_start + pd.Timedelta(hours=start_offset_h)
                dur_h = float(np.clip(rng.gamma(shape=2.0 + 2.2 * row.prob_media, scale=0.8), 0.12, min(11.5, w_h + 2.5)))
                t1 = t0 + pd.Timedelta(hours=dur_h)
            else:
                rel_cong = 0
                t0 = pd.Timestamp(time_pool[int(rng.integers(0, len(time_pool)))])
                dur_h = float(np.clip(rng.gamma(shape=2.0 + 2.2 * row.prob_media, scale=0.8), 0.12, 11.5))
                t1 = t0 + pd.Timedelta(hours=dur_h)

            clientes = int(
                np.clip(
                    rng.normal(680 + 4100 * dens + 4.8 * row.capacidad_mw, 240 + 950 * dens),
                    70,
                    32_000,
                )
            )
            ens = max(0.02, row.avg_load_mw * dur_h * rng.uniform(0.12, 0.48) * (1.16 if rel_cong else 1.0))

            sev = "critica" if (dur_h >= 5 or ens > 42) else "alta" if (dur_h >= 2 or ens > 14) else "media" if dur_h >= 0.7 else "baja"

            if rel_cong:
                causa = rng.choice(["sobrecarga_local", "protecciones", "desconexiones_preventivas"], p=[0.45, 0.33, 0.22])
            else:
                causa = rng.choice(["fallo_equipo", "evento_climatico", "maniobra", "terceros"], p=[0.34, 0.29, 0.22, 0.15])

            rows.append(
                {
                    "interrupcion_id": f"INT{len(rows) + 1:07d}",
                    "timestamp_inicio": t0,
                    "timestamp_fin": t1,
                    "zona_id": zona_id,
                    "subestacion_id": row.subestacion_id,
                    "clientes_afectados": clientes,
                    "energia_no_suministrada_mwh": round(float(ens), 5),
                    "causa": causa,
                    "nivel_severidad": sev,
                    "relacion_congestion_flag": rel_cong,
                }
            )

    return pd.DataFrame(rows).sort_values("timestamp_inicio").reset_index(drop=True)


def generate_intervenciones_operativas(
    zonas_red: pd.DataFrame,
    recursos_flexibilidad: pd.DataFrame,
    almacenamiento_distribuido: pd.DataFrame,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 359)

    flex_zone = recursos_flexibilidad.groupby("zona_id", as_index=False)["capacidad_flexible_mw"].sum().rename(columns={"capacidad_flexible_mw": "flex_mw"})
    storage_zone = almacenamiento_distribuido.groupby("zona_id", as_index=False)["capacidad_potencia_mw"].sum().rename(columns={"capacidad_potencia_mw": "storage_mw"})
    zone = zonas_red[["zona_id", "potencial_flexibilidad", "criticidad_territorial", "tension_crecimiento_demanda"]].merge(flex_zone, on="zona_id", how="left").merge(storage_zone, on="zona_id", how="left").fillna(0)

    tipos = [
        "reconfiguracion_topologica",
        "activacion_flexibilidad",
        "ajuste_tension_reactiva",
        "despacho_almacenamiento",
        "mantenimiento_dirigido",
    ]

    rows = []
    for z in zone.itertuples(index=False):
        for tipo in tipos:
            relief_base = {
                "reconfiguracion_topologica": 0.9,
                "activacion_flexibilidad": 0.6,
                "ajuste_tension_reactiva": 0.4,
                "despacho_almacenamiento": 0.7,
                "mantenimiento_dirigido": 0.3,
            }[tipo]

            cap_relief = (
                relief_base
                * (0.25 * z.flex_mw + 0.35 * z.storage_mw + 8 * z.potencial_flexibilidad + 4 * z.tension_crecimiento_demanda)
                * rng.uniform(0.82, 1.25)
            )
            coste = np.clip(12_000 + 58_000 * relief_base + 85_000 * z.criticidad_territorial + rng.normal(0, 8_500), 8_000, 520_000)
            tiempo = int(np.clip(np.round(3 + 28 * relief_base + 24 * z.criticidad_territorial + rng.normal(0, 3)), 2, 180))
            complejidad = np.clip(0.25 + 0.45 * relief_base + 0.3 * z.criticidad_territorial + rng.normal(0, 0.06), 0.05, 0.99)

            rows.append(
                {
                    "intervencion_id": f"IO{len(rows) + 1:06d}",
                    "zona_id": z.zona_id,
                    "tipo_intervencion": tipo,
                    "capacidad_alivio_estimado_mw": round(float(np.clip(cap_relief, 0.2, 80)), 5),
                    "coste_estimado": round(float(coste), 2),
                    "tiempo_despliegue_dias": tiempo,
                    "complejidad_operativa": round(float(complejidad), 5),
                }
            )

    return pd.DataFrame(rows)


def generate_inversiones_posibles(
    zonas_red: pd.DataFrame,
    subestaciones: pd.DataFrame,
    alimentadores: pd.DataFrame,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 383)

    sub_cap = subestaciones.groupby("zona_id", as_index=False)["capacidad_mw"].sum().rename(columns={"capacidad_mw": "capacidad_sub_mw"})
    feeders_zone = subestaciones[["subestacion_id", "zona_id"]].merge(alimentadores, on="subestacion_id", how="left")
    cap_feeder = feeders_zone.groupby("zona_id", as_index=False)["capacidad_mw"].sum().rename(columns={"capacidad_mw": "capacidad_feed_mw"})

    zone = zonas_red[["zona_id", "criticidad_territorial", "tension_crecimiento_demanda", "potencial_flexibilidad", "riesgo_climatico"]].merge(sub_cap, on="zona_id", how="left").merge(cap_feeder, on="zona_id", how="left")

    tipos = [
        "repotenciacion_subestacion",
        "nuevo_alimentador",
        "automatizacion_avanzada",
        "almacenamiento_red",
        "digitalizacion_protecciones",
        "refuerzo_selectivo_lineas",
    ]

    rows = []
    for z in zone.itertuples(index=False):
        for tipo in tipos:
            cap_factor = {
                "repotenciacion_subestacion": 0.22,
                "nuevo_alimentador": 0.16,
                "automatizacion_avanzada": 0.05,
                "almacenamiento_red": 0.11,
                "digitalizacion_protecciones": 0.03,
                "refuerzo_selectivo_lineas": 0.12,
            }[tipo]
            risk_factor = {
                "repotenciacion_subestacion": 0.28,
                "nuevo_alimentador": 0.22,
                "automatizacion_avanzada": 0.14,
                "almacenamiento_red": 0.2,
                "digitalizacion_protecciones": 0.1,
                "refuerzo_selectivo_lineas": 0.24,
            }[tipo]

            capex = (
                750_000
                + 5_200_000 * cap_factor
                + 3_300_000 * z.tension_crecimiento_demanda
                + 1_900_000 * z.criticidad_territorial
                + rng.normal(0, 250_000)
            )
            opex = np.clip(35_000 + 0.018 * capex + 24_000 * (1 - z.potencial_flexibilidad) + rng.normal(0, 8_000), 9_000, 520_000)
            red_risk = np.clip(0.12 + 0.72 * risk_factor + 0.2 * z.criticidad_territorial + rng.normal(0, 0.04), 0.05, 0.98)
            cap_gain = np.clip(cap_factor * (z.capacidad_sub_mw + 0.45 * z.capacidad_feed_mw) * rng.uniform(0.65, 1.35), 1.0, 240)
            horizon = int(np.clip(np.round(6 + 54 * cap_factor + 24 * z.riesgo_climatico + rng.normal(0, 4)), 4, 84))
            facilidad = np.clip(0.78 - 0.48 * z.criticidad_territorial - 0.18 * z.riesgo_climatico + 0.17 * z.potencial_flexibilidad + rng.normal(0, 0.05), 0.05, 0.98)
            impacto_res = np.clip(0.22 + 0.68 * risk_factor + 0.16 * z.riesgo_climatico + rng.normal(0, 0.05), 0.05, 0.99)

            rows.append(
                {
                    "inversion_id": f"INV{len(rows) + 1:06d}",
                    "zona_id": z.zona_id,
                    "tipo_inversion": tipo,
                    "capex_estimado": round(float(np.clip(capex, 250_000, 16_500_000)), 2),
                    "opex_incremental_estimado": round(float(opex), 2),
                    "reduccion_riesgo_esperada": round(float(red_risk), 5),
                    "aumento_capacidad_esperado": round(float(cap_gain), 5),
                    "horizonte_meses": horizon,
                    "facilidad_implementacion": round(float(facilidad), 5),
                    "impacto_resiliencia": round(float(impacto_res), 5),
                }
            )

    return pd.DataFrame(rows)
