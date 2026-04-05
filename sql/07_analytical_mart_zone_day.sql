-- Dialecto: DuckDB SQL
-- Nivel: analytical marts
-- Objetivo: consolidar riesgo operativo diario por zona y exposición de activos.

CREATE OR REPLACE TABLE mart_zone_day_operational AS
WITH node_day AS (
    SELECT
        nh.fecha,
        nh.zona_id,
        SUM(nh.demanda_mw) AS demanda_total_mwh,
        SUM(nh.net_load_mw) AS net_load_total_mwh,
        MAX(nh.demanda_mw) AS carga_punta_mw,
        MAX(nh.carga_relativa) AS carga_relativa_max,
        SUM(CASE WHEN nh.flag_carga_alta THEN 1 ELSE 0 END) AS horas_carga_alta,
        SUM(CASE WHEN nh.flag_congestion THEN 1 ELSE 0 END) AS horas_congestion,
        SUM(CASE WHEN nh.flag_estres_operativo THEN 1 ELSE 0 END) AS horas_estres_operativo,
        SUM(nh.energia_afectada_hora_mwh) AS energia_afectada_congestion_mwh,
        SUM(nh.curtailment_asignado_mw) AS curtailment_mwh,
        SUM(nh.demanda_ev_asignada_mw) AS demanda_ev_mwh,
        SUM(nh.demanda_industrial_asignada_mw) AS demanda_industrial_mwh,
        AVG(nh.flexibilidad_cobertura_mw) AS flexibilidad_cobertura_media_mw,
        AVG(nh.storage_support_proxy_mw) AS storage_support_medio_mw,
        SUM(GREATEST(nh.demanda_critica_mw - nh.flexibilidad_cobertura_mw, 0.0)) AS gap_flex_tecnico_mwh,
        AVG(nh.coste_activacion_ponderado_eur_mwh) AS coste_flex_medio_eur_mwh
    FROM mart_node_hour_operational_state nh
    GROUP BY
        nh.fecha,
        nh.zona_id
),
service_day AS (
    SELECT
        CAST(s.timestamp_inicio AS DATE) AS fecha,
        s.zona_id,
        COUNT(s.interrupcion_id) AS n_interrupciones,
        SUM(s.clientes_afectados) AS clientes_afectados,
        SUM(s.energia_no_suministrada_mwh) AS ens_mwh,
        AVG(s.duracion_minutos) AS duracion_media_interrupcion_min,
        SUM(CASE WHEN s.congestion_overlap_flag THEN 1 ELSE 0 END) AS interrupciones_con_solape_congestion
    FROM vw_int_service_quality_enriched s
    GROUP BY
        CAST(s.timestamp_inicio AS DATE),
        s.zona_id
)
SELECT
    nd.fecha,
    nd.zona_id,
    z.zona_nombre,
    z.comunidad_autonoma,
    z.provincia,
    z.tipo_zona,
    z.region_operativa,
    z.criticidad_territorial,
    z.riesgo_climatico,
    z.tension_crecimiento_demanda,
    nd.demanda_total_mwh,
    nd.net_load_total_mwh,
    nd.carga_punta_mw,
    nd.carga_relativa_max,
    nd.horas_carga_alta,
    nd.horas_congestion,
    nd.horas_estres_operativo,
    nd.energia_afectada_congestion_mwh,
    nd.curtailment_mwh,
    nd.demanda_ev_mwh,
    nd.demanda_industrial_mwh,
    nd.flexibilidad_cobertura_media_mw,
    nd.storage_support_medio_mw,
    nd.gap_flex_tecnico_mwh,
    nd.coste_flex_medio_eur_mwh,
    COALESCE(sd.n_interrupciones, 0) AS n_interrupciones,
    COALESCE(sd.clientes_afectados, 0) AS clientes_afectados,
    COALESCE(sd.ens_mwh, 0.0) AS ens_mwh,
    COALESCE(sd.duracion_media_interrupcion_min, 0.0) AS duracion_media_interrupcion_min,
    COALESCE(sd.interrupciones_con_solape_congestion, 0) AS interrupciones_con_solape_congestion
FROM node_day nd
INNER JOIN stg_zonas_red z
    ON nd.zona_id = z.zona_id
LEFT JOIN service_day sd
    ON nd.fecha = sd.fecha
   AND nd.zona_id = sd.zona_id;

CREATE OR REPLACE VIEW vw_zone_operational_risk AS
WITH zone_rollup AS (
    SELECT
        zd.zona_id,
        MAX(zd.zona_nombre) AS zona_nombre,
        MAX(zd.tipo_zona) AS tipo_zona,
        MAX(zd.region_operativa) AS region_operativa,
        MAX(zd.criticidad_territorial) AS criticidad_territorial,
        MAX(zd.tension_crecimiento_demanda) AS tension_crecimiento_demanda,
        SUM(zd.horas_congestion) AS horas_congestion,
        SUM(zd.energia_afectada_congestion_mwh) AS energia_afectada_congestion_mwh,
        SUM(zd.ens_mwh) AS ens_total_mwh,
        SUM(zd.clientes_afectados) AS clientes_afectados_total,
        MAX(zd.carga_punta_mw) AS carga_punta_mw,
        AVG(zd.carga_relativa_max) AS carga_relativa_max_media,
        AVG((zd.demanda_ev_mwh + zd.demanda_industrial_mwh) / NULLIF(zd.demanda_total_mwh, 0.0)) AS presion_electrificacion_media,
        AVG(zd.gap_flex_tecnico_mwh / NULLIF(zd.demanda_total_mwh, 0.0)) AS brecha_flex_media
    FROM mart_zone_day_operational zd
    GROUP BY
        zd.zona_id
),
zone_severity AS (
    SELECT
        e.zona_id,
        AVG(e.severidad_score) AS severidad_media_congestion,
        MAX(e.severidad_score) AS severidad_max_congestion,
        SUM(e.energia_afectada_mwh) AS energia_total_eventos_mwh
    FROM vw_int_grid_events e
    GROUP BY
        e.zona_id
),
base AS (
    SELECT
        zr.zona_id,
        zr.zona_nombre,
        zr.tipo_zona,
        zr.region_operativa,
        zr.horas_congestion,
        COALESCE(zs.severidad_media_congestion, 0.0) AS severidad_media_congestion,
        COALESCE(zs.severidad_max_congestion, 0) AS severidad_max_congestion,
        zr.energia_afectada_congestion_mwh,
        zr.ens_total_mwh,
        zr.clientes_afectados_total,
        zr.carga_punta_mw,
        zr.carga_relativa_max_media,
        zr.criticidad_territorial,
        zr.tension_crecimiento_demanda,
        COALESCE(zr.presion_electrificacion_media, 0.0) AS presion_electrificacion_media,
        COALESCE(zr.brecha_flex_media, 0.0) AS brecha_flex_media
    FROM zone_rollup zr
    LEFT JOIN zone_severity zs
        ON zr.zona_id = zs.zona_id
)
SELECT
    b.zona_id,
    b.zona_nombre,
    b.tipo_zona,
    b.region_operativa,
    b.horas_congestion,
    b.severidad_media_congestion,
    b.severidad_max_congestion,
    b.energia_afectada_congestion_mwh,
    b.ens_total_mwh,
    b.clientes_afectados_total,
    b.carga_punta_mw,
    b.carga_relativa_max_media,
    b.criticidad_territorial,
    b.tension_crecimiento_demanda,
    b.presion_electrificacion_media,
    b.brecha_flex_media,
    (
        0.24 * (100.0 * (b.horas_congestion / NULLIF(MAX(b.horas_congestion) OVER (), 0.0)))
        + 0.16 * (100.0 * (b.severidad_media_congestion / NULLIF(MAX(b.severidad_media_congestion) OVER (), 0.0)))
        + 0.18 * (100.0 * (b.ens_total_mwh / NULLIF(MAX(b.ens_total_mwh) OVER (), 0.0)))
        + 0.14 * (100.0 * (b.clientes_afectados_total / NULLIF(MAX(b.clientes_afectados_total) OVER (), 0.0)))
        + 0.10 * (100.0 * (b.carga_relativa_max_media / NULLIF(MAX(b.carga_relativa_max_media) OVER (), 0.0)))
        + 0.09 * (100.0 * b.criticidad_territorial)
        + 0.09 * (100.0 * b.tension_crecimiento_demanda)
    ) AS riesgo_operativo_score
FROM base b;

CREATE OR REPLACE VIEW vw_assets_exposure AS
WITH node_stress_exposure AS (
    SELECT
        nh.subestacion_id,
        nh.alimentador_id,
        COUNT(*) AS horas_observadas,
        SUM(CASE WHEN nh.flag_estres_operativo THEN 1 ELSE 0 END) AS horas_estres_operativo,
        SUM(CASE WHEN nh.flag_congestion THEN 1 ELSE 0 END) AS horas_congestion,
        AVG(nh.carga_relativa) AS carga_relativa_media,
        MAX(nh.carga_relativa) AS carga_relativa_max,
        SUM(nh.energia_afectada_hora_mwh) AS energia_congestion_expuesta_mwh
    FROM mart_node_hour_operational_state nh
    GROUP BY
        nh.subestacion_id,
        nh.alimentador_id
),
substation_service_exposure AS (
    SELECT
        s.subestacion_id,
        SUM(s.energia_no_suministrada_mwh) AS ens_subestacion_mwh,
        SUM(s.clientes_afectados) AS clientes_afectados_subestacion,
        COUNT(s.interrupcion_id) AS interrupciones_subestacion
    FROM vw_int_service_quality_events s
    GROUP BY
        s.subestacion_id
)
SELECT
    a.activo_id,
    a.tipo_activo,
    s.zona_id,
    a.subestacion_id,
    a.alimentador_id,
    a.edad_anios,
    a.estado_salud,
    a.probabilidad_fallo_proxy,
    a.criticidad,
    a.capex_reposicion_estimado,
    a.opex_mantenimiento_estimado,
    COALESCE(nse.horas_observadas, 0) AS horas_observadas,
    COALESCE(nse.horas_estres_operativo, 0) AS horas_estres_operativo,
    COALESCE(nse.horas_congestion, 0) AS horas_congestion,
    COALESCE(nse.carga_relativa_media, 0.0) AS carga_relativa_media,
    COALESCE(nse.carga_relativa_max, 0.0) AS carga_relativa_max,
    COALESCE(nse.energia_congestion_expuesta_mwh, 0.0) AS energia_congestion_expuesta_mwh,
    COALESCE(sse.ens_subestacion_mwh, 0.0) AS ens_subestacion_mwh,
    COALESCE(sse.clientes_afectados_subestacion, 0) AS clientes_afectados_subestacion,
    COALESCE(sse.interrupciones_subestacion, 0) AS interrupciones_subestacion,
    (
        0.26 * (100.0 * (a.edad_anios / NULLIF(MAX(a.edad_anios) OVER (), 0.0)))
        + 0.18 * (100.0 * (1.0 - a.estado_salud))
        + 0.16 * (100.0 * a.criticidad)
        + 0.20 * (100.0 * (COALESCE(nse.horas_estres_operativo, 0) / NULLIF(COALESCE(nse.horas_observadas, 0), 0.0)))
        + 0.20 * (100.0 * a.probabilidad_fallo_proxy)
    ) AS exposicion_activo_score,
    LEAST(
        1.0,
        a.probabilidad_fallo_proxy * (
            1.0 + 0.35 * (COALESCE(nse.horas_estres_operativo, 0) / NULLIF(COALESCE(nse.horas_observadas, 0), 0.0))
        )
    ) AS probabilidad_fallo_ajustada_proxy
FROM stg_activos_red a
LEFT JOIN stg_subestaciones s
    ON a.subestacion_id = s.subestacion_id
LEFT JOIN node_stress_exposure nse
    ON a.subestacion_id = nse.subestacion_id
   AND COALESCE(a.alimentador_id, '__SIN_ALIMENTADOR__') = COALESCE(nse.alimentador_id, '__SIN_ALIMENTADOR__')
LEFT JOIN substation_service_exposure sse
    ON a.subestacion_id = sse.subestacion_id;
