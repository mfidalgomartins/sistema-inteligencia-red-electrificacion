from __future__ import annotations

from textwrap import dedent

import pandas as pd

from .common_v2 import connect_v2, ensure_dirs, get_paths, write_df
from .sql_runner_v2 import run_sql_layer_v2


def build_features_v2(force_sql_refresh: bool = False) -> dict[str, pd.DataFrame]:
    paths = ensure_dirs(get_paths())
    conn = connect_v2(paths)

    if force_sql_refresh:
        conn.close()
        run_sql_layer_v2()
        conn = connect_v2(paths)

    # Baseline de demanda esperada por nodo-hora considerando estacionalidad operativa.
    conn.execute(
        dedent(
            """
            CREATE OR REPLACE TEMP VIEW tmp_node_hour_baseline AS
            SELECT
                n.alimentador_id,
                n.subestacion_id,
                n.zona_id,
                n.hora,
                n.tipo_dia,
                AVG(n.demanda_mw) AS baseline_demanda_mw,
                AVG(n.carga_relativa) AS baseline_carga_relativa
            FROM mart_node_hour_operational_state n
            GROUP BY
                n.alimentador_id,
                n.subestacion_id,
                n.zona_id,
                n.hora,
                n.tipo_dia
            """
        )
    )

    # Interrupciones horarias por subestación para enriquecer histórico de confiabilidad.
    conn.execute(
        dedent(
            """
            CREATE OR REPLACE TEMP VIEW tmp_interruptions_hourly AS
            WITH expanded AS (
                SELECT
                    s.subestacion_id,
                    gs.ts AS timestamp
                FROM vw_int_service_quality_events s
                CROSS JOIN LATERAL generate_series(
                    s.timestamp_inicio,
                    s.timestamp_fin,
                    INTERVAL 1 HOUR
                ) AS gs(ts)
            )
            SELECT
                e.subestacion_id,
                e.timestamp,
                COUNT(*) AS interrupciones_hora
            FROM expanded e
            GROUP BY
                e.subestacion_id,
                e.timestamp
            """
        )
    )

    conn.execute(
        dedent(
            """
            CREATE OR REPLACE TEMP VIEW tmp_fragilidad_activos AS
            SELECT
                a.subestacion_id,
                a.alimentador_id,
                AVG(a.exposicion_activo_score) / 100.0 AS fragilidad_activos_asociados,
                AVG(a.probabilidad_fallo_ajustada_proxy) AS prob_fallo_ajustada_media
            FROM vw_assets_exposure a
            GROUP BY
                a.subestacion_id,
                a.alimentador_id
            """
        )
    )

    conn.execute(
        dedent(
            """
            CREATE OR REPLACE TABLE node_hour_features AS
            WITH enriched AS (
                SELECT
                    n.timestamp,
                    CAST(n.timestamp AS DATE) AS fecha,
                    n.zona_id,
                    n.subestacion_id,
                    n.alimentador_id,
                    n.tipo_red,
                    n.hora,
                    n.mes,
                    n.tipo_dia,
                    n.demanda_mw,
                    n.capacidad_mw,
                    n.carga_relativa,
                    CASE WHEN n.carga_relativa >= 1.0 THEN 1 ELSE 0 END AS sobrecarga_flag,
                    1.0 - ABS(1.0 - n.carga_relativa) AS proximidad_a_capacidad,
                    CASE WHEN n.hora_punta_flag THEN 1 ELSE 0 END AS hora_punta_flag,
                    n.demanda_ev_asignada_mw,
                    n.demanda_industrial_asignada_mw,
                    n.generacion_distribuida_asignada_mw,
                    n.curtailment_asignado_mw,
                    n.flexibilidad_cobertura_mw,
                    n.storage_support_proxy_mw,
                    n.coste_activacion_ponderado_eur_mwh AS coste_flexibilidad_proxy,
                    n.criticidad_territorial,
                    z.riesgo_climatico,
                    b.baseline_demanda_mw,
                    b.baseline_carga_relativa,
                    COALESCE(i.interrupciones_hora, 0) AS interrupciones_hora,
                    COALESCE(fa.fragilidad_activos_asociados, 0.0) AS fragilidad_activos_asociados,
                    COALESCE(fa.prob_fallo_ajustada_media, 0.0) AS prob_fallo_ajustada_media,
                    CASE WHEN n.flag_congestion THEN 1 ELSE 0 END AS congestion_flag
                FROM mart_node_hour_operational_state n
                LEFT JOIN tmp_node_hour_baseline b
                    ON n.alimentador_id = b.alimentador_id
                   AND n.subestacion_id = b.subestacion_id
                   AND n.zona_id = b.zona_id
                   AND n.hora = b.hora
                   AND n.tipo_dia = b.tipo_dia
                LEFT JOIN tmp_interruptions_hourly i
                    ON n.subestacion_id = i.subestacion_id
                   AND n.timestamp = i.timestamp
                LEFT JOIN tmp_fragilidad_activos fa
                    ON n.subestacion_id = fa.subestacion_id
                   AND n.alimentador_id = fa.alimentador_id
                LEFT JOIN stg_zonas_red z
                    ON n.zona_id = z.zona_id
            )
            SELECT
                e.timestamp,
                e.fecha,
                e.zona_id,
                e.subestacion_id,
                e.alimentador_id,
                e.tipo_red,
                e.hora,
                e.mes,
                e.tipo_dia,
                e.demanda_mw,
                e.capacidad_mw,
                e.carga_relativa,
                e.sobrecarga_flag,
                GREATEST(LEAST(e.proximidad_a_capacidad, 1.0), 0.0) AS proximidad_a_capacidad,
                e.hora_punta_flag,
                STDDEV_SAMP(e.demanda_mw) OVER (
                    PARTITION BY e.alimentador_id
                    ORDER BY e.timestamp
                    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                ) AS volatilidad_reciente,
                AVG(e.demanda_mw) OVER (
                    PARTITION BY e.alimentador_id
                    ORDER BY e.timestamp
                    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                ) AS rolling_mean_24h,
                MAX(e.carga_relativa) OVER (
                    PARTITION BY e.alimentador_id
                    ORDER BY e.timestamp
                    ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
                ) AS rolling_max_7d,
                (e.demanda_mw - COALESCE(e.baseline_demanda_mw, e.demanda_mw))
                    / NULLIF(COALESCE(e.baseline_demanda_mw, e.demanda_mw), 0.0) AS crecimiento_vs_baseline,
                e.demanda_ev_asignada_mw / NULLIF(e.demanda_mw, 0.0) AS presion_ev,
                e.demanda_industrial_asignada_mw / NULLIF(e.demanda_mw, 0.0) AS presion_electrificacion_industrial,
                e.flexibilidad_cobertura_mw / NULLIF(e.demanda_mw, 0.0) AS cobertura_flexibilidad,
                e.storage_support_proxy_mw / NULLIF(e.demanda_mw, 0.0) AS storage_support_ratio,
                e.coste_flexibilidad_proxy,
                e.generacion_distribuida_asignada_mw / NULLIF(e.demanda_mw, 0.0) AS penetracion_generacion_distribuida,
                e.curtailment_asignado_mw / NULLIF(e.generacion_distribuida_asignada_mw, 0.0) AS curtailment_ratio,
                SUM(e.congestion_flag) OVER (
                    PARTITION BY e.alimentador_id
                    ORDER BY e.timestamp
                    ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
                ) AS historial_congestion_7d,
                SUM(e.congestion_flag) OVER (
                    PARTITION BY e.alimentador_id
                    ORDER BY e.timestamp
                    ROWS BETWEEN 719 PRECEDING AND CURRENT ROW
                ) AS historial_congestion_30d,
                SUM(e.interrupciones_hora) OVER (
                    PARTITION BY e.subestacion_id
                    ORDER BY e.timestamp
                    ROWS BETWEEN 719 PRECEDING AND CURRENT ROW
                ) AS historial_interrupciones_30d,
                e.fragilidad_activos_asociados,
                e.criticidad_territorial,
                e.riesgo_climatico
            FROM enriched e
            """
        )
    )

    conn.execute(
        dedent(
            """
            CREATE OR REPLACE TABLE zone_day_features AS
            WITH congestion_daily AS (
                SELECT
                    d.zona_id,
                    CAST(d.timestamp_inicio AS DATE) AS fecha,
                    AVG(d.severidad_score) AS severidad_media
                FROM vw_int_grid_events d
                GROUP BY
                    d.zona_id,
                    CAST(d.timestamp_inicio AS DATE)
            ),
            assets_zone AS (
                SELECT
                    a.zona_id,
                    AVG(a.exposicion_activo_score) AS exposicion_activos
                FROM vw_assets_exposure a
                GROUP BY
                    a.zona_id
            )
            SELECT
                z.fecha,
                z.zona_id,
                z.zona_nombre,
                z.tipo_zona,
                z.region_operativa,
                z.horas_congestion,
                COALESCE(c.severidad_media, 0.0) AS severidad_media,
                z.ens_mwh AS ens,
                z.clientes_afectados,
                QUANTILE_CONT(z.carga_relativa_max, 0.95) OVER (PARTITION BY z.zona_id) AS percentil_carga,
                z.gap_flex_tecnico_mwh AS gap_flexibilidad,
                COALESCE(a.exposicion_activos, 0.0) AS exposicion_activos,
                z.demanda_ev_mwh AS demanda_ev_total,
                z.demanda_industrial_mwh AS demanda_industrial_adicional_total,
                z.curtailment_mwh AS curtailment_total,
                z.ens_mwh + 0.15 * z.curtailment_mwh AS demanda_no_servida_proxy,
                (2500.0 * z.ens_mwh) + (90.0 * z.curtailment_mwh) + (45.0 * z.horas_congestion) AS coste_riesgo_proxy
            FROM mart_zone_day_operational z
            LEFT JOIN congestion_daily c
                ON z.zona_id = c.zona_id
               AND z.fecha = c.fecha
            LEFT JOIN assets_zone a
                ON z.zona_id = a.zona_id
            """
        )
    )

    conn.execute(
        dedent(
            """
            CREATE OR REPLACE TABLE zone_month_features AS
            WITH base AS (
                SELECT
                    m.mes,
                    m.zona_id,
                    m.zona_nombre,
                    m.tipo_zona,
                    m.region_operativa,
                    m.demanda_total_mwh,
                    m.horas_congestion,
                    m.ens_mwh,
                    m.carga_punta_mw,
                    m.gap_flex_tecnico_mwh,
                    m.curtailment_mwh,
                    m.demanda_ev_mwh,
                    m.demanda_industrial_mwh,
                    m.flexibilidad_cobertura_media_mw,
                    m.storage_support_medio_mw,
                    m.criticidad_territorial_media,
                    m.riesgo_climatico_medio,
                    m.tension_crecimiento_demanda_media
                FROM mart_zone_month_operational m
            )
            SELECT
                b.mes,
                b.zona_id,
                b.zona_nombre,
                b.tipo_zona,
                b.region_operativa,
                b.demanda_total_mwh,
                b.horas_congestion,
                b.ens_mwh,
                b.carga_punta_mw,
                b.gap_flex_tecnico_mwh,
                b.curtailment_mwh,
                b.demanda_ev_mwh,
                b.demanda_industrial_mwh,
                b.flexibilidad_cobertura_media_mw,
                b.storage_support_medio_mw,
                b.tension_crecimiento_demanda_media AS presion_crecimiento,
                b.demanda_total_mwh
                    - LAG(b.demanda_total_mwh) OVER (PARTITION BY b.zona_id ORDER BY b.mes) AS tendencia_demanda,
                b.demanda_total_mwh
                    / NULLIF(AVG(b.demanda_total_mwh) OVER (PARTITION BY b.zona_id), 0.0) AS cambio_estacional,
                b.horas_congestion / NULLIF(24.0 * 30.0, 0.0) AS recurrencia_congestion,
                (
                    0.28 * (100.0 * b.horas_congestion / NULLIF(MAX(b.horas_congestion) OVER (), 0.0))
                    + 0.24 * (100.0 * b.ens_mwh / NULLIF(MAX(b.ens_mwh) OVER (), 0.0))
                    + 0.20 * (100.0 * b.gap_flex_tecnico_mwh / NULLIF(MAX(b.gap_flex_tecnico_mwh) OVER (), 0.0))
                    + 0.14 * (100.0 * b.criticidad_territorial_media)
                    + 0.14 * (100.0 * b.tension_crecimiento_demanda_media)
                ) AS riesgo_operativo_agregado,
                100.0 - (
                    0.40 * (100.0 * b.ens_mwh / NULLIF(MAX(b.ens_mwh) OVER (), 0.0))
                    + 0.30 * (100.0 * b.horas_congestion / NULLIF(MAX(b.horas_congestion) OVER (), 0.0))
                    + 0.30 * (100.0 * b.riesgo_climatico_medio)
                ) AS indice_resiliencia,
                (b.carga_punta_mw * 320.0) + (b.gap_flex_tecnico_mwh * 28.0) AS intensidad_capex_proxy,
                b.flexibilidad_cobertura_media_mw / NULLIF(b.carga_punta_mw, 0.0) AS flexibilidad_efectiva,
                b.storage_support_medio_mw / NULLIF(b.carga_punta_mw, 0.0) AS storage_efectivo
            FROM base b
            """
        )
    )

    conn.execute(
        dedent(
            """
            CREATE OR REPLACE TABLE intervention_candidates_features AS
            WITH zone_driver AS (
                SELECT
                    z.zona_id,
                    AVG(z.coste_riesgo_proxy) AS coste_riesgo_proxy,
                    AVG(z.gap_flexibilidad) AS gap_flexibilidad_medio,
                    AVG(z.exposicion_activos) AS exposicion_activos_media,
                    AVG(z.demanda_ev_total / NULLIF(z.demanda_ev_total + z.demanda_industrial_adicional_total + 1e-9, 0.0)) AS peso_ev,
                    AVG(z.demanda_industrial_adicional_total / NULLIF(z.demanda_ev_total + z.demanda_industrial_adicional_total + 1e-9, 0.0)) AS peso_industrial
                FROM zone_day_features z
                GROUP BY
                    z.zona_id
            ),
            zone_month AS (
                SELECT
                    m.zona_id,
                    AVG(m.riesgo_operativo_agregado) AS riesgo_operativo_agregado,
                    AVG(m.indice_resiliencia) AS indice_resiliencia,
                    AVG(m.intensidad_capex_proxy) AS intensidad_capex_proxy,
                    AVG(m.flexibilidad_efectiva) AS flexibilidad_efectiva,
                    AVG(m.storage_efectivo) AS storage_efectivo,
                    AVG(m.presion_crecimiento) AS presion_crecimiento
                FROM zone_month_features m
                GROUP BY
                    m.zona_id
            )
            SELECT
                i.inversion_id,
                i.zona_id,
                z.zona_nombre,
                z.tipo_zona,
                i.tipo_inversion,
                i.familia_intervencion,
                i.capex_estimado,
                i.horizonte_meses,
                i.facilidad_implementacion,
                i.riesgo_operativo_score,
                i.impacto_resiliencia,
                i.aumento_capacidad_esperado,
                COALESCE(d.coste_riesgo_proxy, 0.0) AS coste_riesgo_proxy,
                COALESCE(d.gap_flexibilidad_medio, 0.0) AS gap_flexibilidad_medio,
                COALESCE(d.exposicion_activos_media, 0.0) AS exposicion_activos_media,
                COALESCE(m.riesgo_operativo_agregado, 0.0) AS riesgo_operativo_agregado,
                COALESCE(m.indice_resiliencia, 0.0) AS indice_resiliencia,
                COALESCE(m.intensidad_capex_proxy, 0.0) AS intensidad_capex_proxy,
                COALESCE(m.flexibilidad_efectiva, 0.0) AS flexibilidad_efectiva,
                COALESCE(m.storage_efectivo, 0.0) AS storage_efectivo,
                COALESCE(m.presion_crecimiento, 0.0) AS presion_crecimiento,
                CASE
                    WHEN i.riesgo_operativo_score >= 75 THEN 'congestion_estructural'
                    WHEN COALESCE(d.exposicion_activos_media, 0.0) >= 65 THEN 'degradacion_activos'
                    WHEN COALESCE(d.gap_flexibilidad_medio, 0.0) >= 25 THEN 'brecha_flexibilidad'
                    WHEN COALESCE(d.peso_ev, 0.0) >= 0.6 THEN 'presion_ev'
                    ELSE 'presion_electrificacion_industrial'
                END AS main_risk_driver,
                (
                    0.35 * i.riesgo_operativo_score
                    + 0.25 * COALESCE(m.riesgo_operativo_agregado, 0.0)
                    + 0.20 * COALESCE(d.exposicion_activos_media, 0.0)
                    + 0.20 * COALESCE(d.gap_flexibilidad_medio, 0.0)
                ) AS technical_score_inputs,
                (
                    0.45 * COALESCE(d.coste_riesgo_proxy, 0.0)
                    + 0.35 * COALESCE(m.intensidad_capex_proxy, 0.0)
                    + 0.20 * i.capex_estimado
                ) AS economic_score_inputs,
                (
                    0.45 * (100.0 * COALESCE(m.flexibilidad_efectiva, 0.0))
                    + 0.35 * (100.0 * COALESCE(m.storage_efectivo, 0.0))
                    + 0.20 * (100.0 * i.facilidad_implementacion)
                ) AS flexibility_viability_inputs,
                (
                    0.40 * (100.0 * i.facilidad_implementacion)
                    + 0.30 * (100.0 * (1.0 - i.horizonte_meses / NULLIF(MAX(i.horizonte_meses) OVER (), 0.0)))
                    + 0.30 * (100.0 * i.impacto_resiliencia / NULLIF(MAX(i.impacto_resiliencia) OVER (), 0.0))
                ) AS investment_readiness_inputs
            FROM vw_investment_candidates i
            LEFT JOIN stg_zonas_red z
                ON i.zona_id = z.zona_id
            LEFT JOIN zone_driver d
                ON i.zona_id = d.zona_id
            LEFT JOIN zone_month m
                ON i.zona_id = m.zona_id
            """
        )
    )

    outputs = {
        "node_hour_features": conn.execute("SELECT * FROM node_hour_features").df(),
        "zone_day_features": conn.execute("SELECT * FROM zone_day_features").df(),
        "zone_month_features": conn.execute("SELECT * FROM zone_month_features").df(),
        "intervention_candidates_features": conn.execute("SELECT * FROM intervention_candidates_features").df(),
    }

    write_df(outputs["node_hour_features"], paths.data_processed / "node_hour_features.parquet")
    write_df(outputs["zone_day_features"], paths.data_processed / "zone_day_features.csv")
    write_df(outputs["zone_month_features"], paths.data_processed / "zone_month_features.csv")
    write_df(outputs["intervention_candidates_features"], paths.data_processed / "intervention_candidates_features.csv")

    feature_dictionary = dedent(
        """
        # Feature Dictionary (v2)

        ## Principio de diseño
        - **Observadas**: señales directamente medidas en operación o eventos.
        - **Derivadas**: señales transformadas para modelado, interpretación o decisión.

        ## node_hour_features (granularidad nodo-hora)
        - `carga_relativa` (observada): ratio demanda/capacidad instantánea.
        - `sobrecarga_flag` (derivada): 1 si `carga_relativa >= 1.0`.
        - `proximidad_a_capacidad` (derivada): cercanía a saturación técnica.
        - `hora_punta_flag` (observada): marca de punta operativa.
        - `volatilidad_reciente` (derivada): desviación estándar móvil de 24h en demanda.
        - `rolling_mean_24h` (derivada): media móvil de demanda 24h.
        - `rolling_max_7d` (derivada): máximo de carga relativa en 7 días.
        - `crecimiento_vs_baseline` (derivada): desviación frente a baseline horario por tipo de día.
        - `presion_ev` (derivada): proporción EV en demanda del nodo.
        - `presion_electrificacion_industrial` (derivada): proporción industrial adicional en demanda.
        - `cobertura_flexibilidad` (derivada): cobertura flexible sobre demanda nodal.
        - `storage_support_ratio` (derivada): soporte de storage sobre demanda nodal.
        - `coste_flexibilidad_proxy` (observada/derivada): coste medio zonal de activación flexible.
        - `penetracion_generacion_distribuida` (derivada): GD asignada sobre demanda.
        - `curtailment_ratio` (derivada): energía recortada sobre GD asignada.
        - `historial_congestion_7d` (derivada): congestión acumulada de 7 días.
        - `historial_congestion_30d` (derivada): congestión acumulada de 30 días.
        - `historial_interrupciones_30d` (derivada): interrupciones acumuladas por subestación en 30 días.
        - `fragilidad_activos_asociados` (derivada): exposición media de activos asociados.
        - `criticidad_territorial` (observada): criticidad estructural de la zona.
        - `riesgo_climatico` (observada): vulnerabilidad climática territorial.

        ## zone_day_features (granularidad zona-día)
        - `horas_congestion` (observada agregada): total de horas con congestión.
        - `severidad_media` (derivada): severidad media diaria de eventos.
        - `ens` (observada agregada): energía no suministrada diaria.
        - `clientes_afectados` (observada agregada): afectados diarios.
        - `percentil_carga` (derivada): percentil 95 de carga relativa diaria por zona.
        - `gap_flexibilidad` (derivada): brecha técnica flexible diaria.
        - `exposicion_activos` (derivada): exposición media de activos en la zona.
        - `demanda_ev_total` (observada agregada): energía EV diaria.
        - `demanda_industrial_adicional_total` (observada agregada): energía industrial adicional diaria.
        - `curtailment_total` (observada agregada): energía recortada diaria.
        - `demanda_no_servida_proxy` (derivada): ENS + componente de curtailment.
        - `coste_riesgo_proxy` (derivada): proxy económico de riesgo diario.

        ## zone_month_features (granularidad zona-mes)
        - `tendencia_demanda` (derivada): cambio mensual de demanda total.
        - `cambio_estacional` (derivada): demanda mensual respecto a media anual zonal.
        - `recurrencia_congestion` (derivada): frecuencia relativa mensual de congestión.
        - `riesgo_operativo_agregado` (derivada): índice compuesto de riesgo mensual.
        - `indice_resiliencia` (derivada): score inverso de fragilidad mensual.
        - `intensidad_capex_proxy` (derivada): presión de inversión por carga y gap flexible.
        - `flexibilidad_efectiva` (derivada): cobertura flexible media / carga punta.
        - `storage_efectivo` (derivada): soporte storage medio / carga punta.
        - `presion_crecimiento` (observada agregada): tensión estructural de crecimiento.

        ## intervention_candidates_features (granularidad candidato-zona)
        - `main_risk_driver` (derivada): driver dominante de riesgo para intervención.
        - `technical_score_inputs` (derivada): inputs técnicos combinados de riesgo.
        - `economic_score_inputs` (derivada): inputs económicos y CAPEX.
        - `flexibility_viability_inputs` (derivada): viabilidad operativa de flex/storage.
        - `investment_readiness_inputs` (derivada): madurez de despliegue e impacto.

        ## Utilidad para utility
        Estas señales permiten separar saturación puntual vs. estructural, conectar calidad de servicio con exposición de activos, y traducir estrés operativo en decisiones de refuerzo, flexibilidad, storage o secuenciación CAPEX.
        """
    ).strip() + "\n"

    (paths.docs / "feature_dictionary.md").write_text(feature_dictionary, encoding="utf-8")

    conn.close()
    return outputs


if __name__ == "__main__":
    dfs = build_features_v2(force_sql_refresh=False)
    for k, v in dfs.items():
        print(k, len(v))
