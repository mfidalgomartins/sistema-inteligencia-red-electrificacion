"""Prioridad de intervención por alimentador a partir del modelo canónico."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .common import connect_database, ensure_dirs, get_paths, minmax, write_df

REQUIRED_FEEDER_COLUMNS = {
    "zona_id",
    "subestacion_id",
    "alimentador_id",
    "capacidad_mw",
    "demanda_punta_mw",
    "carga_relativa_max",
    "carga_relativa_media",
    "horas_observadas",
    "horas_congestion",
    "exposicion_media",
    "probabilidad_fallo_ajustada_media",
    "ens_asociada_mwh",
}


def _require_columns(df: pd.DataFrame, required: set[str], dataset: str) -> None:
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"Columnas ausentes en {dataset}: {', '.join(missing)}")


def build_feeder_priorities(
    feeder_metrics: pd.DataFrame,
    zone_scoring: pd.DataFrame,
) -> pd.DataFrame:
    """Combina estrés local y prioridad zonal en un ranking de alimentadores."""
    _require_columns(feeder_metrics, REQUIRED_FEEDER_COLUMNS, "feeder_metrics")
    _require_columns(zone_scoring, {"zona_id", "investment_priority_score"}, "zone_scoring")
    if feeder_metrics.empty:
        raise ValueError("feeder_metrics no puede estar vacío")
    if zone_scoring["zona_id"].duplicated().any():
        raise ValueError("zone_scoring debe contener una fila por zona")

    priority = feeder_metrics.copy()
    priority = priority.merge(
        zone_scoring[["zona_id", "investment_priority_score"]],
        on="zona_id",
        how="left",
        validate="many_to_one",
    )
    if priority["investment_priority_score"].isna().any():
        missing_zones = sorted(priority.loc[priority["investment_priority_score"].isna(), "zona_id"].unique())
        raise ValueError(f"Zonas sin puntuación: {', '.join(missing_zones)}")

    numeric_columns = REQUIRED_FEEDER_COLUMNS.difference({"zona_id", "subestacion_id", "alimentador_id"})
    priority[list(numeric_columns)] = priority[list(numeric_columns)].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    priority["ratio_congestion"] = (
        (priority["horas_congestion"] / priority["horas_observadas"].replace(0, np.nan)).fillna(0.0).clip(0.0, 1.0)
    )
    priority["alivio_requerido_mw"] = (priority["demanda_punta_mw"] - priority["capacidad_mw"]).clip(lower=0.0)

    priority["puntuacion_tecnica"] = (
        0.32 * minmax(priority["carga_relativa_max"])
        + 0.25 * minmax(priority["ratio_congestion"])
        + 0.23 * minmax(priority["exposicion_media"])
        + 0.12 * minmax(priority["probabilidad_fallo_ajustada_media"])
        + 0.08 * minmax(priority["ens_asociada_mwh"])
    ).clip(0.0, 100.0)
    priority["puntuacion_prioridad"] = (
        0.70 * priority["puntuacion_tecnica"] + 0.30 * priority["investment_priority_score"]
    ).clip(0.0, 100.0)

    priority["accion_recomendada"] = np.select(
        [
            priority["carga_relativa_max"] >= 1.10,
            priority["exposicion_media"] >= 75.0,
            priority["ratio_congestion"] >= 0.20,
        ],
        [
            "refuerzo_selectivo",
            "renovacion_activos",
            "reconfiguracion_y_automatizacion",
        ],
        default="flexibilidad_local_y_monitorizacion",
    )
    priority["nivel_prioridad"] = pd.cut(
        priority["puntuacion_prioridad"],
        bins=[-np.inf, 50.0, 65.0, 80.0, np.inf],
        labels=["Monitorizar", "Planificar", "Alta", "Crítica"],
        right=False,
    )

    priority = priority.sort_values(
        ["puntuacion_prioridad", "puntuacion_tecnica", "alimentador_id"],
        ascending=[False, False, True],
    ).reset_index(drop=True)
    priority.insert(0, "ranking_prioridad", np.arange(1, len(priority) + 1))

    return priority[
        [
            "ranking_prioridad",
            "alimentador_id",
            "subestacion_id",
            "zona_id",
            "nivel_prioridad",
            "puntuacion_prioridad",
            "puntuacion_tecnica",
            "accion_recomendada",
            "alivio_requerido_mw",
            "capacidad_mw",
            "demanda_punta_mw",
            "carga_relativa_max",
            "carga_relativa_media",
            "horas_congestion",
            "ratio_congestion",
            "exposicion_media",
            "probabilidad_fallo_ajustada_media",
            "ens_asociada_mwh",
            "investment_priority_score",
        ]
    ]


def run_feeder_prioritization() -> pd.DataFrame:
    """Construye y publica el ranking canónico de alimentadores."""
    paths = ensure_dirs(get_paths())
    conn = connect_database(paths, read_only=True)
    try:
        feeder_metrics = conn.execute(
            """
            WITH operational AS (
                SELECT
                    zona_id,
                    subestacion_id,
                    alimentador_id,
                    MAX(capacidad_mw) AS capacidad_mw,
                    MAX(demanda_mw) AS demanda_punta_mw,
                    MAX(carga_relativa) AS carga_relativa_max,
                    AVG(carga_relativa) AS carga_relativa_media,
                    COUNT(DISTINCT timestamp) AS horas_observadas,
                    COUNT(DISTINCT CASE WHEN flag_congestion THEN timestamp END) AS horas_congestion
                FROM mart_node_hour_operational_state
                GROUP BY zona_id, subestacion_id, alimentador_id
            ),
            assets AS (
                SELECT
                    zona_id,
                    subestacion_id,
                    alimentador_id,
                    AVG(exposicion_activo_score) AS exposicion_media,
                    AVG(probabilidad_fallo_ajustada_proxy) AS probabilidad_fallo_ajustada_media,
                    MAX(ens_subestacion_mwh) AS ens_asociada_mwh
                FROM vw_assets_exposure
                WHERE alimentador_id IS NOT NULL
                GROUP BY zona_id, subestacion_id, alimentador_id
            )
            SELECT
                o.*,
                COALESCE(a.exposicion_media, 0.0) AS exposicion_media,
                COALESCE(a.probabilidad_fallo_ajustada_media, 0.0) AS probabilidad_fallo_ajustada_media,
                COALESCE(a.ens_asociada_mwh, 0.0) AS ens_asociada_mwh
            FROM operational o
            LEFT JOIN assets a
                USING (zona_id, subestacion_id, alimentador_id)
            """
        ).df()
    finally:
        conn.close()

    scoring_path = paths.data_processed / "intervention_scoring_table.csv"
    if not scoring_path.exists():
        raise FileNotFoundError(f"Falta la puntuación zonal requerida: {scoring_path}")
    zone_scoring = pd.read_csv(scoring_path)
    priorities = build_feeder_priorities(feeder_metrics, zone_scoring)
    write_df(priorities, paths.data_processed / "prioridades_inversion_alimentadores.csv")
    return priorities


if __name__ == "__main__":
    result = run_feeder_prioritization()
    print(f"Alimentadores priorizados: {len(result)}")
