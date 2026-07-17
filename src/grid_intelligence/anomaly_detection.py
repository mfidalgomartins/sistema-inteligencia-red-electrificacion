"""Detección de anomalías operativas y señales precursoras de congestión e interrupción."""

from __future__ import annotations

from textwrap import dedent

import numpy as np
import pandas as pd

from .common import connect_database, ensure_dirs, get_paths, write_df


def _severity_from_score(score: float) -> str:
    if score >= 4.0:
        return "critica"
    if score >= 3.0:
        return "alta"
    if score >= 2.0:
        return "media"
    return "baja"


def _future_window_max(series: pd.Series, window: int) -> pd.Series:
    """Máximo de las próximas observaciones, excluyendo la fila actual."""
    reversed_future = series.iloc[::-1].shift(1).rolling(window, min_periods=1).max()
    return reversed_future.iloc[::-1].fillna(0)


def run_anomaly_detection() -> dict[str, pd.DataFrame]:
    paths = ensure_dirs(get_paths())
    conn = connect_database(paths)

    node = conn.execute(
        dedent(
            """
            SELECT
                n.timestamp,
                CAST(n.timestamp AS DATE) AS fecha,
                n.zona_id,
                n.subestacion_id,
                n.alimentador_id,
                n.demanda_mw,
                n.carga_relativa,
                n.curtailment_asignado_mw,
                n.generacion_distribuida_asignada_mw,
                n.flag_congestion,
                n.hora_punta_flag,
                f.rolling_mean_24h,
                f.curtailment_ratio,
                f.crecimiento_vs_baseline,
                f.criticidad_territorial,
                f.riesgo_climatico
            FROM mart_node_hour_operational_state n
            LEFT JOIN node_hour_features f
                ON n.timestamp = f.timestamp
               AND n.zona_id = f.zona_id
               AND n.subestacion_id = f.subestacion_id
               AND n.alimentador_id = f.alimentador_id
            """
        )
    ).df()

    zone_day = conn.execute("SELECT * FROM zone_day_features").df()

    # A) Demanda inesperada (z-score de residual frente a media móvil 24h).
    demand = node[
        ["timestamp", "fecha", "zona_id", "subestacion_id", "alimentador_id", "demanda_mw", "rolling_mean_24h"]
    ].copy()
    demand["rolling_mean_24h"] = demand["rolling_mean_24h"].fillna(demand["demanda_mw"])
    demand["residual_demanda"] = demand["demanda_mw"] - demand["rolling_mean_24h"]
    residual_std = demand.groupby("alimentador_id")["residual_demanda"].transform("std").replace(0, np.nan)
    demand["z_score"] = demand["residual_demanda"] / residual_std
    demand["z_score"] = demand["z_score"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    demand_anom = demand[np.abs(demand["z_score"]) >= 3.0].copy()
    demand_anom["anomaly_type"] = "demanda_inesperada"
    demand_anom["observed_value"] = demand_anom["demanda_mw"]
    demand_anom["threshold_value"] = demand_anom["rolling_mean_24h"] + 3.0 * residual_std.loc[demand_anom.index].fillna(
        0.0
    )
    demand_anom["severity_score"] = np.abs(demand_anom["z_score"])
    demand_anom["probable_explanation"] = np.where(
        demand_anom["residual_demanda"] > 0,
        "pico_no_esperado_posible_EV_o_industrial",
        "caida_anomala_posible_reconfiguracion_o_fallo_medicion",
    )

    # B) Carga relativa anormal (percentil extremo + regla operativa).
    load = node[["timestamp", "fecha", "zona_id", "subestacion_id", "alimentador_id", "carga_relativa"]].copy()
    p99_load = load.groupby("alimentador_id")["carga_relativa"].transform(lambda s: s.quantile(0.99))
    load_anom = load[(load["carga_relativa"] >= p99_load) | (load["carga_relativa"] >= 1.15)].copy()
    load_anom["anomaly_type"] = "carga_relativa_anormal"
    load_anom["observed_value"] = load_anom["carga_relativa"]
    load_anom["threshold_value"] = np.maximum(p99_load.loc[load_anom.index], 1.15)
    load_anom["severity_score"] = 2.5 + np.maximum(load_anom["carga_relativa"] - 1.0, 0.0) * 10
    load_anom["probable_explanation"] = "saturacion_termica_o_desbalance_operativo"

    # C) Vertido anormal.
    curt = node[
        [
            "timestamp",
            "fecha",
            "zona_id",
            "subestacion_id",
            "alimentador_id",
            "curtailment_asignado_mw",
            "generacion_distribuida_asignada_mw",
            "curtailment_ratio",
        ]
    ].copy()
    curt["curtailment_ratio"] = curt["curtailment_ratio"].fillna(0.0)
    p99_curt = curt.groupby("zona_id")["curtailment_ratio"].transform(lambda s: s.quantile(0.99))
    curt_anom = curt[(curt["curtailment_ratio"] >= p99_curt) & (curt["curtailment_ratio"] >= 0.18)].copy()
    curt_anom["anomaly_type"] = "curtailment_anormal"
    curt_anom["observed_value"] = curt_anom["curtailment_ratio"]
    curt_anom["threshold_value"] = np.maximum(p99_curt.loc[curt_anom.index], 0.18)
    curt_anom["severity_score"] = 2.0 + curt_anom["curtailment_ratio"] * 8
    curt_anom["probable_explanation"] = "exceso_GD_vs_capacidad_local_o_restriccion_operativa"

    # D) Zonas con ENS atípica.
    ens = zone_day[["fecha", "zona_id", "ens", "clientes_afectados", "coste_riesgo_proxy"]].copy()
    ens["roll_mean_30d"] = ens.groupby("zona_id")["ens"].transform(lambda s: s.rolling(30, min_periods=7).mean())
    ens["roll_std_30d"] = ens.groupby("zona_id")["ens"].transform(lambda s: s.rolling(30, min_periods=7).std())
    ens["ens_threshold"] = ens["roll_mean_30d"] + 2.5 * ens["roll_std_30d"].fillna(0.0)
    ens_anom = ens[(ens["ens"] > ens["ens_threshold"]) & (ens["ens"] > 0)].copy()
    ens_anom["timestamp"] = pd.to_datetime(ens_anom["fecha"])
    ens_anom["subestacion_id"] = None
    ens_anom["alimentador_id"] = None
    ens_anom["anomaly_type"] = "ens_atipica"
    ens_anom["observed_value"] = ens_anom["ens"]
    ens_anom["threshold_value"] = ens_anom["ens_threshold"]
    ens_anom["severity_score"] = 2.0 + (ens_anom["ens"] / ens_anom["ens_threshold"].replace(0, np.nan)).fillna(0.0)
    ens_anom["probable_explanation"] = "degradacion_servicio_o_evento_meteo_con_impacto"

    # E) Subestaciones con deterioro operativo.
    sub_daily = (
        node.assign(stress=((node["carga_relativa"] >= 0.95) | (node["flag_congestion"])))
        .groupby(["fecha", "subestacion_id", "zona_id"], as_index=False)
        .agg(stress_ratio=("stress", "mean"), carga_media=("carga_relativa", "mean"))
        .sort_values(["subestacion_id", "fecha"])
    )
    sub_daily["roll7"] = sub_daily.groupby("subestacion_id")["stress_ratio"].transform(
        lambda s: s.rolling(7, min_periods=5).mean()
    )
    sub_daily["roll30"] = sub_daily.groupby("subestacion_id")["stress_ratio"].transform(
        lambda s: s.rolling(30, min_periods=10).mean()
    )
    sub_daily["roll30_std"] = sub_daily.groupby("subestacion_id")["stress_ratio"].transform(
        lambda s: s.rolling(30, min_periods=10).std()
    )
    sub_anom = sub_daily[sub_daily["roll7"] > (sub_daily["roll30"] + 1.75 * sub_daily["roll30_std"].fillna(0.0))].copy()
    sub_anom["timestamp"] = pd.to_datetime(sub_anom["fecha"])
    sub_anom["alimentador_id"] = None
    sub_anom["anomaly_type"] = "deterioro_operativo_subestacion"
    sub_anom["observed_value"] = sub_anom["roll7"]
    sub_anom["threshold_value"] = sub_anom["roll30"] + 1.75 * sub_anom["roll30_std"].fillna(0.0)
    sub_anom["severity_score"] = 1.8 + (sub_anom["roll7"] - sub_anom["roll30"]).clip(lower=0) * 10
    sub_anom["probable_explanation"] = "acumulacion_estres_y_posible_degradacion_activos"

    # F) Nodos con desviación frente a patrón esperado.
    baseline = node[
        [
            "timestamp",
            "fecha",
            "zona_id",
            "subestacion_id",
            "alimentador_id",
            "crecimiento_vs_baseline",
            "hora_punta_flag",
        ]
    ].copy()
    baseline["crecimiento_vs_baseline"] = baseline["crecimiento_vs_baseline"].fillna(0.0)
    base_anom = baseline[np.abs(baseline["crecimiento_vs_baseline"]) >= 0.35].copy()
    base_anom["anomaly_type"] = "desviacion_vs_patron"
    base_anom["observed_value"] = base_anom["crecimiento_vs_baseline"]
    base_anom["threshold_value"] = 0.35
    base_anom["severity_score"] = 1.5 + np.abs(base_anom["crecimiento_vs_baseline"]) * 4
    base_anom["probable_explanation"] = np.where(
        base_anom["hora_punta_flag"],
        "desviacion_en_hora_punta_sensible",
        "desviacion_fuera_patron_estacional",
    )

    def normalize(df: pd.DataFrame) -> pd.DataFrame:
        base_cols = [
            "timestamp",
            "fecha",
            "zona_id",
            "subestacion_id",
            "alimentador_id",
            "anomaly_type",
            "observed_value",
            "threshold_value",
            "severity_score",
            "probable_explanation",
        ]
        out = df[base_cols].copy()
        out["severity"] = out["severity_score"].map(_severity_from_score)
        return out

    anomalies = pd.concat(
        [
            normalize(demand_anom),
            normalize(load_anom),
            normalize(curt_anom),
            normalize(ens_anom),
            normalize(sub_anom),
            normalize(base_anom),
        ],
        ignore_index=True,
    )

    anomalies = anomalies.sort_values(["severity_score", "timestamp"], ascending=[False, True]).reset_index(drop=True)
    anomalies["anomaly_id"] = [f"ANOM_{i:07d}" for i in range(1, len(anomalies) + 1)]

    # Señal precursora de congestión en las próximas 48h (a nivel nodo).
    node_future = node[["timestamp", "zona_id", "subestacion_id", "alimentador_id", "flag_congestion"]].copy()
    node_future = node_future.sort_values(["zona_id", "subestacion_id", "alimentador_id", "timestamp"])
    node_future["flag_congestion_int"] = node_future["flag_congestion"].astype(int)
    node_future["precursor_congestion_48h"] = node_future.groupby(
        ["zona_id", "subestacion_id", "alimentador_id"],
        group_keys=False,
    )["flag_congestion_int"].apply(lambda s: _future_window_max(s, 48))

    anomalies = anomalies.merge(
        node_future[["timestamp", "zona_id", "subestacion_id", "alimentador_id", "precursor_congestion_48h"]],
        on=["timestamp", "zona_id", "subestacion_id", "alimentador_id"],
        how="left",
    )
    anomalies["precursor_congestion_48h"] = anomalies["precursor_congestion_48h"].fillna(0).astype(int)

    # Señal precursora de interrupción en 7 días (a nivel zona-fecha).
    interruption_daily = (
        conn.execute(
            """
            SELECT
                fecha,
                zona_id,
                CASE WHEN SUM(n_interrupciones) > 0 THEN 1 ELSE 0 END AS hubo_interrupcion
            FROM mart_zone_day_operational
            GROUP BY fecha, zona_id
            """
        )
        .df()
        .sort_values(["zona_id", "fecha"])
    )
    interruption_daily["precursor_interrupcion_7d"] = interruption_daily.groupby("zona_id", group_keys=False)[
        "hubo_interrupcion"
    ].apply(lambda s: _future_window_max(s, 7))

    anomalies = anomalies.merge(
        interruption_daily[["fecha", "zona_id", "precursor_interrupcion_7d"]],
        on=["fecha", "zona_id"],
        how="left",
    )
    anomalies["precursor_interrupcion_7d"] = anomalies["precursor_interrupcion_7d"].fillna(0).astype(int)

    # Recomendación operativa rápida por tipo.
    rec_map = {
        "demanda_inesperada": "monitorizar_intradia_y_activar_flex_rapida",
        "carga_relativa_anormal": "reconfigurar_red_y_preparar_alivio",
        "curtailment_anormal": "optimizar_despacho_GD_storage",
        "ens_atipica": "activar_plan_resiliencia_servicio",
        "deterioro_operativo_subestacion": "inspeccion_activos_y_mantenimiento_dirigido",
        "desviacion_vs_patron": "validar_medicion_y_ajustar_modelo_operativo",
    }
    anomalies["monitoring_recommendation"] = anomalies["anomaly_type"].map(rec_map)

    # Tabla resumen para puntuación.
    zone_intensity = (
        anomalies.groupby("zona_id", as_index=False)
        .agg(
            n_anomalias=("anomaly_id", "count"),
            severidad_media=("severity_score", "mean"),
            anomalias_criticas=("severity", lambda s: int((s == "critica").sum())),
            ratio_precursor_congestion=("precursor_congestion_48h", "mean"),
            ratio_precursor_interrupcion=("precursor_interrupcion_7d", "mean"),
        )
        .sort_values("severidad_media", ascending=False)
    )

    type_summary = (
        anomalies.groupby("anomaly_type", as_index=False)
        .agg(
            n_eventos=("anomaly_id", "count"),
            severidad_media=("severity_score", "mean"),
            pct_precursor_congestion=("precursor_congestion_48h", "mean"),
            pct_precursor_interrupcion=("precursor_interrupcion_7d", "mean"),
        )
        .sort_values("n_eventos", ascending=False)
    )

    write_df(anomalies, paths.data_processed / "anomalies_detected.csv")
    write_df(type_summary, paths.data_processed / "anomalies_summary_by_type.csv")
    write_df(zone_intensity, paths.data_processed / "anomaly_zone_intensity.csv")

    conn.close()

    return {
        "anomalies_detected": anomalies,
        "anomalies_summary_by_type": type_summary,
        "anomaly_zone_intensity": zone_intensity,
    }


if __name__ == "__main__":
    out = run_anomaly_detection()
    for name, df in out.items():
        print(name, len(df))
