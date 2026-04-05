-- Dialecto: DuckDB SQL
-- Nivel: analytical marts
-- Objetivo: sintetizar desempeño mensual y generar vistas para brecha de flexibilidad y candidatos de inversión.

CREATE OR REPLACE TABLE mart_zone_month_operational AS
SELECT
    DATE_TRUNC('month', zd.fecha) AS mes,
    zd.zona_id,
    MAX(zd.zona_nombre) AS zona_nombre,
    MAX(zd.tipo_zona) AS tipo_zona,
    MAX(zd.region_operativa) AS region_operativa,
    SUM(zd.demanda_total_mwh) AS demanda_total_mwh,
    SUM(zd.net_load_total_mwh) AS net_load_total_mwh,
    MAX(zd.carga_punta_mw) AS carga_punta_mw,
    AVG(zd.carga_relativa_max) AS carga_relativa_max_media,
    SUM(zd.horas_carga_alta) AS horas_carga_alta,
    SUM(zd.horas_congestion) AS horas_congestion,
    SUM(zd.horas_estres_operativo) AS horas_estres_operativo,
    SUM(zd.energia_afectada_congestion_mwh) AS energia_afectada_congestion_mwh,
    SUM(zd.curtailment_mwh) AS curtailment_mwh,
    SUM(zd.demanda_ev_mwh) AS demanda_ev_mwh,
    SUM(zd.demanda_industrial_mwh) AS demanda_industrial_mwh,
    AVG(zd.flexibilidad_cobertura_media_mw) AS flexibilidad_cobertura_media_mw,
    AVG(zd.storage_support_medio_mw) AS storage_support_medio_mw,
    SUM(zd.gap_flex_tecnico_mwh) AS gap_flex_tecnico_mwh,
    AVG(zd.coste_flex_medio_eur_mwh) AS coste_flex_medio_eur_mwh,
    SUM(zd.n_interrupciones) AS n_interrupciones,
    SUM(zd.clientes_afectados) AS clientes_afectados,
    SUM(zd.ens_mwh) AS ens_mwh,
    AVG(zd.duracion_media_interrupcion_min) AS duracion_media_interrupcion_min,
    SUM(zd.interrupciones_con_solape_congestion) AS interrupciones_con_solape_congestion,
    AVG(zd.criticidad_territorial) AS criticidad_territorial_media,
    AVG(zd.riesgo_climatico) AS riesgo_climatico_medio,
    AVG(zd.tension_crecimiento_demanda) AS tension_crecimiento_demanda_media
FROM mart_zone_day_operational zd
GROUP BY
    DATE_TRUNC('month', zd.fecha),
    zd.zona_id;

CREATE OR REPLACE VIEW vw_flexibility_gap AS
WITH stress_reference AS (
    SELECT
        zr.zona_id,
        zr.riesgo_operativo_score,
        zr.horas_congestion,
        zr.carga_punta_mw,
        zr.presion_electrificacion_media
    FROM vw_zone_operational_risk zr
),
zone_month AS (
    SELECT
        zm.zona_id,
        AVG(zm.carga_punta_mw) AS carga_punta_media_mw,
        MAX(zm.carga_punta_mw) AS carga_punta_max_mw,
        SUM(zm.horas_estres_operativo) AS horas_estres_operativo_total,
        AVG(zm.coste_flex_medio_eur_mwh) AS coste_flex_medio_eur_mwh,
        AVG(zm.gap_flex_tecnico_mwh) AS gap_flex_tecnico_mwh_medio,
        AVG(zm.demanda_total_mwh) AS demanda_mensual_media_mwh
    FROM mart_zone_month_operational zm
    GROUP BY
        zm.zona_id
)
SELECT
    zf.zona_id,
    zf.zona_nombre,
    zf.tipo_zona,
    zf.region_operativa,
    COALESCE(sr.riesgo_operativo_score, 0.0) AS riesgo_operativo_score,
    COALESCE(zm.carga_punta_max_mw, 0.0) AS demanda_critica_mw,
    zf.capacidad_flexible_mw,
    zf.storage_potencia_total_mw,
    zf.storage_energia_total_mwh,
    zf.storage_disponibilidad_media,
    zf.soporte_flex_storage_mw AS cobertura_flexible_total_mw,
    zf.coste_activacion_ponderado_eur_mwh AS coste_activacion_flex_eur_mwh,
    GREATEST(COALESCE(zm.carga_punta_max_mw, 0.0) - COALESCE(zf.soporte_flex_storage_mw, 0.0), 0.0) AS gap_tecnico_mw,
    COALESCE(zm.gap_flex_tecnico_mwh_medio, 0.0) AS gap_tecnico_mwh_medio,
    (
        GREATEST(COALESCE(zm.carga_punta_max_mw, 0.0) - COALESCE(zf.soporte_flex_storage_mw, 0.0), 0.0)
        * COALESCE(zf.coste_activacion_ponderado_eur_mwh, 0.0)
        * (COALESCE(zm.horas_estres_operativo_total, 0.0) / NULLIF(COUNT(*) OVER (), 0.0))
    ) AS gap_economico_proxy_eur,
    COALESCE(zf.soporte_flex_storage_mw, 0.0) / NULLIF(COALESCE(zm.carga_punta_max_mw, 0.0), 0.0) AS ratio_flexibilidad_estres,
    COALESCE(sr.horas_congestion, 0) AS horas_congestion_acumuladas,
    COALESCE(sr.presion_electrificacion_media, 0.0) AS presion_electrificacion_media
FROM vw_int_flexibility_assets_zone zf
LEFT JOIN stress_reference sr
    ON zf.zona_id = sr.zona_id
LEFT JOIN zone_month zm
    ON zf.zona_id = zm.zona_id;

CREATE OR REPLACE VIEW vw_investment_candidates AS
WITH inv_base AS (
    SELECT
        i.inversion_id,
        i.zona_id,
        i.tipo_inversion,
        i.capex_estimado,
        i.opex_incremental_estimado,
        i.reduccion_riesgo_esperada,
        i.aumento_capacidad_esperado,
        i.horizonte_meses,
        i.facilidad_implementacion,
        i.impacto_resiliencia,
        CASE
            WHEN i.tipo_inversion IN ('nuevo_alimentador', 'refuerzo_selectivo_lineas', 'repotenciacion_subestacion') THEN 'refuerzo_fisico'
            WHEN i.tipo_inversion IN ('almacenamiento_red') THEN 'storage_distribuido'
            WHEN i.tipo_inversion IN ('automatizacion_avanzada', 'digitalizacion_protecciones') THEN 'operacion_avanzada'
            ELSE 'flexibilidad_operativa'
        END AS familia_intervencion
    FROM stg_inversiones_posibles i
),
zone_context AS (
    SELECT
        r.zona_id,
        r.riesgo_operativo_score,
        r.horas_congestion,
        r.ens_total_mwh,
        r.carga_punta_mw,
        f.gap_tecnico_mw,
        f.ratio_flexibilidad_estres,
        f.coste_activacion_flex_eur_mwh
    FROM vw_zone_operational_risk r
    LEFT JOIN vw_flexibility_gap f
        ON r.zona_id = f.zona_id
),
candidate_scored AS (
    SELECT
        ib.inversion_id,
        ib.zona_id,
        ib.tipo_inversion,
        ib.familia_intervencion,
        ib.capex_estimado,
        ib.opex_incremental_estimado,
        ib.reduccion_riesgo_esperada,
        ib.aumento_capacidad_esperado,
        ib.horizonte_meses,
        ib.facilidad_implementacion,
        ib.impacto_resiliencia,
        COALESCE(zc.riesgo_operativo_score, 0.0) AS riesgo_operativo_score,
        COALESCE(zc.horas_congestion, 0) AS horas_congestion,
        COALESCE(zc.ens_total_mwh, 0.0) AS ens_total_mwh,
        COALESCE(zc.carga_punta_mw, 0.0) AS carga_punta_mw,
        COALESCE(zc.gap_tecnico_mw, 0.0) AS gap_tecnico_mw,
        COALESCE(zc.ratio_flexibilidad_estres, 0.0) AS ratio_flexibilidad_estres,
        COALESCE(zc.coste_activacion_flex_eur_mwh, 0.0) AS coste_activacion_flex_eur_mwh,
        (ib.reduccion_riesgo_esperada * COALESCE(zc.riesgo_operativo_score, 0.0)) AS impacto_riesgo_proxy,
        (ib.aumento_capacidad_esperado * COALESCE(zc.carga_punta_mw, 0.0) / NULLIF(COALESCE(zc.gap_tecnico_mw, 0.0), 0.0)) AS impacto_capacidad_relativo,
        (ib.reduccion_riesgo_esperada * COALESCE(zc.riesgo_operativo_score, 0.0) + 15.0 * ib.impacto_resiliencia)
            / NULLIF(ib.capex_estimado, 0.0) AS eficiencia_capex_riesgo
    FROM inv_base ib
    LEFT JOIN zone_context zc
        ON ib.zona_id = zc.zona_id
)
SELECT
    cs.inversion_id,
    cs.zona_id,
    z.zona_nombre,
    z.tipo_zona,
    z.region_operativa,
    cs.tipo_inversion,
    cs.familia_intervencion,
    cs.riesgo_operativo_score,
    cs.horas_congestion,
    cs.ens_total_mwh,
    cs.gap_tecnico_mw,
    cs.ratio_flexibilidad_estres,
    cs.capex_estimado,
    cs.opex_incremental_estimado,
    cs.horizonte_meses,
    cs.facilidad_implementacion,
    cs.reduccion_riesgo_esperada,
    cs.aumento_capacidad_esperado,
    cs.impacto_resiliencia,
    cs.impacto_riesgo_proxy,
    cs.impacto_capacidad_relativo,
    cs.eficiencia_capex_riesgo,
    CASE
        WHEN cs.familia_intervencion = 'refuerzo_fisico' THEN 'refuerzo'
        WHEN cs.familia_intervencion IN ('storage_distribuido', 'flexibilidad_operativa') THEN 'flexibilidad'
        ELSE 'operacion'
    END AS estrategia_flexibilidad_vs_refuerzo,
    (
        0.28 * (100.0 * cs.reduccion_riesgo_esperada)
        + 0.22 * (100.0 * (cs.impacto_resiliencia / NULLIF(MAX(cs.impacto_resiliencia) OVER (), 0.0)))
        + 0.18 * (100.0 * (cs.aumento_capacidad_esperado / NULLIF(MAX(cs.aumento_capacidad_esperado) OVER (), 0.0)))
        + 0.12 * (100.0 * (cs.eficiencia_capex_riesgo / NULLIF(MAX(cs.eficiencia_capex_riesgo) OVER (), 0.0)))
        + 0.10 * (100.0 * cs.facilidad_implementacion)
        + 0.10 * (100.0 * (1.0 - cs.horizonte_meses / NULLIF(MAX(cs.horizonte_meses) OVER (), 0.0)))
    ) AS prioridad_inicial_score
FROM candidate_scored cs
LEFT JOIN stg_zonas_red z
    ON cs.zona_id = z.zona_id;
