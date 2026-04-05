-- Dialecto: DuckDB SQL
-- Nivel: KPI queries
-- Objetivo: publicar consultas ejecutivas para priorización territorial y técnica.

-- 1) Top zonas por riesgo operativo.
CREATE OR REPLACE VIEW kpi_top_zonas_riesgo_operativo AS
SELECT
    r.zona_id,
    r.zona_nombre,
    r.tipo_zona,
    r.region_operativa,
    ROUND(r.riesgo_operativo_score, 2) AS riesgo_operativo_score,
    r.horas_congestion,
    ROUND(r.ens_total_mwh, 2) AS ens_total_mwh,
    r.clientes_afectados_total,
    ROUND(r.presion_electrificacion_media, 4) AS presion_electrificacion_media,
    ROUND(r.brecha_flex_media, 4) AS brecha_flex_media
FROM vw_zone_operational_risk r
ORDER BY
    r.riesgo_operativo_score DESC,
    r.horas_congestion DESC
LIMIT 10;

-- 2) Top subestaciones por congestión acumulada.
CREATE OR REPLACE VIEW kpi_top_subestaciones_congestion_acumulada AS
SELECT
    nh.zona_id,
    nh.subestacion_id,
    COUNT(*) AS horas_observadas,
    SUM(CASE WHEN nh.flag_congestion THEN 1 ELSE 0 END) AS horas_congestion,
    ROUND(SUM(nh.energia_afectada_hora_mwh), 2) AS energia_afectada_total_mwh,
    ROUND(MAX(nh.carga_relativa), 4) AS carga_relativa_max,
    ROUND(AVG(nh.carga_relativa), 4) AS carga_relativa_media,
    ROUND(100.0 * SUM(CASE WHEN nh.flag_congestion THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_horas_congestion
FROM mart_node_hour_operational_state nh
GROUP BY
    nh.zona_id,
    nh.subestacion_id
ORDER BY
    horas_congestion DESC,
    energia_afectada_total_mwh DESC
LIMIT 20;

-- 3) Top alimentadores por exposición.
CREATE OR REPLACE VIEW kpi_top_alimentadores_exposicion AS
SELECT
    a.zona_id,
    a.subestacion_id,
    a.alimentador_id,
    COUNT(a.activo_id) AS activos_en_alimentador,
    ROUND(AVG(a.exposicion_activo_score), 2) AS exposicion_media_activos,
    ROUND(MAX(a.exposicion_activo_score), 2) AS exposicion_max_activo,
    ROUND(AVG(a.probabilidad_fallo_ajustada_proxy), 4) AS probabilidad_fallo_ajustada_media,
    ROUND(SUM(a.ens_subestacion_mwh), 2) AS ens_asociada_mwh
FROM vw_assets_exposure a
GROUP BY
    a.zona_id,
    a.subestacion_id,
    a.alimentador_id
ORDER BY
    exposicion_media_activos DESC,
    probabilidad_fallo_ajustada_media DESC
LIMIT 20;

-- 4) Zonas con mayor ENS.
CREATE OR REPLACE VIEW kpi_zonas_mayor_ens AS
SELECT
    r.zona_id,
    r.zona_nombre,
    r.tipo_zona,
    r.region_operativa,
    ROUND(r.ens_total_mwh, 2) AS ens_total_mwh,
    r.clientes_afectados_total,
    r.horas_congestion,
    ROUND(r.riesgo_operativo_score, 2) AS riesgo_operativo_score
FROM vw_zone_operational_risk r
ORDER BY
    r.ens_total_mwh DESC,
    r.clientes_afectados_total DESC
LIMIT 10;

-- 5) Zonas con peor ratio flexibilidad/estrés.
CREATE OR REPLACE VIEW kpi_zonas_peor_ratio_flex_estres AS
SELECT
    f.zona_id,
    f.zona_nombre,
    f.tipo_zona,
    f.region_operativa,
    ROUND(f.ratio_flexibilidad_estres, 4) AS ratio_flexibilidad_estres,
    ROUND(f.gap_tecnico_mw, 3) AS gap_tecnico_mw,
    ROUND(f.gap_economico_proxy_eur, 2) AS gap_economico_proxy_eur,
    ROUND(f.riesgo_operativo_score, 2) AS riesgo_operativo_score,
    f.horas_congestion_acumuladas
FROM vw_flexibility_gap f
ORDER BY
    f.ratio_flexibilidad_estres ASC,
    f.gap_tecnico_mw DESC
LIMIT 10;

-- 6) Zonas con mayor potencial de CAPEX diferible.
CREATE OR REPLACE VIEW kpi_zonas_potencial_capex_diferible AS
WITH zone_capex AS (
    SELECT
        c.zona_id,
        MAX(c.zona_nombre) AS zona_nombre,
        MAX(c.tipo_zona) AS tipo_zona,
        MAX(c.region_operativa) AS region_operativa,
        SUM(CASE WHEN c.estrategia_flexibilidad_vs_refuerzo = 'refuerzo' THEN c.capex_estimado ELSE 0.0 END) AS capex_refuerzo_eur,
        SUM(CASE WHEN c.estrategia_flexibilidad_vs_refuerzo = 'flexibilidad' THEN c.capex_estimado ELSE 0.0 END) AS capex_flexibilidad_eur,
        AVG(c.prioridad_inicial_score) AS prioridad_media_cartera
    FROM vw_investment_candidates c
    GROUP BY
        c.zona_id
)
SELECT
    zc.zona_id,
    zc.zona_nombre,
    zc.tipo_zona,
    zc.region_operativa,
    ROUND(zc.capex_refuerzo_eur, 2) AS capex_refuerzo_eur,
    ROUND(zc.capex_flexibilidad_eur, 2) AS capex_flexibilidad_eur,
    ROUND(GREATEST(zc.capex_refuerzo_eur - zc.capex_flexibilidad_eur, 0.0), 2) AS capex_diferible_proxy_eur,
    ROUND(zc.prioridad_media_cartera, 2) AS prioridad_media_cartera
FROM zone_capex zc
ORDER BY
    capex_diferible_proxy_eur DESC,
    prioridad_media_cartera DESC
LIMIT 10;

-- 7) Activos más expuestos.
CREATE OR REPLACE VIEW kpi_activos_mas_expuestos AS
SELECT
    a.activo_id,
    a.tipo_activo,
    a.zona_id,
    a.subestacion_id,
    a.alimentador_id,
    a.edad_anios,
    ROUND(a.estado_salud, 4) AS estado_salud,
    ROUND(a.criticidad, 4) AS criticidad,
    ROUND(a.probabilidad_fallo_proxy, 4) AS probabilidad_fallo_proxy,
    ROUND(a.probabilidad_fallo_ajustada_proxy, 4) AS probabilidad_fallo_ajustada_proxy,
    ROUND(a.exposicion_activo_score, 2) AS exposicion_activo_score,
    a.horas_estres_operativo,
    ROUND(a.energia_congestion_expuesta_mwh, 2) AS energia_congestion_expuesta_mwh
FROM vw_assets_exposure a
ORDER BY
    a.exposicion_activo_score DESC,
    a.probabilidad_fallo_ajustada_proxy DESC
LIMIT 25;

-- 8) Zonas más afectadas por EV y electrificación industrial.
CREATE OR REPLACE VIEW kpi_zonas_afectadas_ev_industrial AS
WITH zone_load AS (
    SELECT
        zm.zona_id,
        MAX(zm.zona_nombre) AS zona_nombre,
        MAX(zm.tipo_zona) AS tipo_zona,
        MAX(zm.region_operativa) AS region_operativa,
        SUM(zm.demanda_total_mwh) AS demanda_total_mwh,
        SUM(zm.demanda_ev_mwh) AS demanda_ev_mwh,
        SUM(zm.demanda_industrial_mwh) AS demanda_industrial_mwh,
        AVG(zm.carga_relativa_max_media) AS carga_relativa_max_media,
        SUM(zm.horas_congestion) AS horas_congestion
    FROM mart_zone_month_operational zm
    GROUP BY
        zm.zona_id
)
SELECT
    zl.zona_id,
    zl.zona_nombre,
    zl.tipo_zona,
    zl.region_operativa,
    ROUND(zl.demanda_ev_mwh, 2) AS demanda_ev_mwh,
    ROUND(zl.demanda_industrial_mwh, 2) AS demanda_industrial_mwh,
    ROUND(zl.demanda_ev_mwh + zl.demanda_industrial_mwh, 2) AS demanda_nueva_total_mwh,
    ROUND((zl.demanda_ev_mwh + zl.demanda_industrial_mwh) / NULLIF(zl.demanda_total_mwh, 0.0), 4) AS ratio_demanda_nueva,
    ROUND(zl.carga_relativa_max_media, 4) AS carga_relativa_max_media,
    zl.horas_congestion
FROM zone_load zl
ORDER BY
    ratio_demanda_nueva DESC,
    demanda_nueva_total_mwh DESC
LIMIT 10;
