-- Dialecto: DuckDB SQL
-- Nivel: integration
-- Objetivo: integrar carga horaria por nodo con componentes EV/industrial/GD y contexto de capacidad.

CREATE OR REPLACE VIEW vw_int_zone_hour_components AS
WITH zone_hour_demand AS (
    SELECT
        d.timestamp,
        d.zona_id,
        SUM(d.demanda_mw) AS demanda_total_zona_mw,
        SUM(d.demanda_reactiva_proxy) AS demanda_reactiva_total_zona_mvar,
        MAX(d.temperatura) AS temperatura_max_zona,
        AVG(d.temperatura) AS temperatura_media_zona,
        AVG(d.humedad) AS humedad_media_zona
    FROM stg_demanda_horaria d
    GROUP BY d.timestamp, d.zona_id
),
zone_hour_ev AS (
    SELECT
        e.timestamp,
        e.zona_id,
        SUM(e.demanda_ev_mw) AS demanda_ev_total_zona_mw,
        AVG(e.penetracion_ev) AS penetracion_ev_media_zona
    FROM stg_demanda_ev e
    GROUP BY e.timestamp, e.zona_id
),
zone_hour_ind AS (
    SELECT
        i.timestamp,
        i.zona_id,
        SUM(i.demanda_industrial_adicional_mw) AS demanda_industrial_total_zona_mw
    FROM stg_demanda_electrificacion_industrial i
    GROUP BY i.timestamp, i.zona_id
),
zone_hour_gd AS (
    SELECT
        g.timestamp,
        g.zona_id,
        SUM(g.generacion_mw) AS generacion_distribuida_total_zona_mw,
        SUM(g.autoconsumo_estimado_mw) AS autoconsumo_total_zona_mw,
        SUM(g.vertido_estimado_mw) AS vertido_total_zona_mw,
        SUM(g.curtailment_estimado_mw) AS curtailment_total_zona_mw
    FROM stg_generacion_distribuida g
    GROUP BY g.timestamp, g.zona_id
),
macro_base AS (
    SELECT
        m.fecha,
        m.crecimiento_demanda_indice,
        m.penetracion_ev_indice,
        m.electrificacion_industrial_indice,
        m.presion_capex_indice
    FROM stg_escenario_macro m
    WHERE m.escenario = 'base'
)
SELECT
    zhd.timestamp,
    zhd.zona_id,
    zhd.demanda_total_zona_mw,
    zhd.demanda_reactiva_total_zona_mvar,
    COALESCE(zhe.demanda_ev_total_zona_mw, 0.0) AS demanda_ev_total_zona_mw,
    COALESCE(zhi.demanda_industrial_total_zona_mw, 0.0) AS demanda_industrial_total_zona_mw,
    COALESCE(zhg.generacion_distribuida_total_zona_mw, 0.0) AS generacion_distribuida_total_zona_mw,
    COALESCE(zhg.autoconsumo_total_zona_mw, 0.0) AS autoconsumo_total_zona_mw,
    COALESCE(zhg.vertido_total_zona_mw, 0.0) AS vertido_total_zona_mw,
    COALESCE(zhg.curtailment_total_zona_mw, 0.0) AS curtailment_total_zona_mw,
    zhd.temperatura_max_zona,
    zhd.temperatura_media_zona,
    zhd.humedad_media_zona,
    COALESCE(zhe.penetracion_ev_media_zona, 0.0) AS penetracion_ev_media_zona,
    COALESCE(mb.crecimiento_demanda_indice, 1.0) AS crecimiento_demanda_indice,
    COALESCE(mb.penetracion_ev_indice, 1.0) AS penetracion_ev_indice,
    COALESCE(mb.electrificacion_industrial_indice, 1.0) AS electrificacion_industrial_indice,
    COALESCE(mb.presion_capex_indice, 1.0) AS presion_capex_indice
FROM zone_hour_demand zhd
LEFT JOIN zone_hour_ev zhe
    ON zhd.timestamp = zhe.timestamp
   AND zhd.zona_id = zhe.zona_id
LEFT JOIN zone_hour_ind zhi
    ON zhd.timestamp = zhi.timestamp
   AND zhd.zona_id = zhi.zona_id
LEFT JOIN zone_hour_gd zhg
    ON zhd.timestamp = zhg.timestamp
   AND zhd.zona_id = zhg.zona_id
LEFT JOIN macro_base mb
    ON CAST(zhd.timestamp AS DATE) = mb.fecha;


CREATE OR REPLACE VIEW vw_int_network_load_hour AS
WITH feeder_capacity_share AS (
    SELECT
        z.zona_id,
        a.alimentador_id,
        a.capacidad_mw,
        a.carga_base_esperada,
        a.nivel_perdidas_estimado,
        a.exposicion_climatica,
        a.criticidad_operativa,
        a.tipo_red,
        a.subestacion_id,
        a.capacidad_mw / NULLIF(SUM(a.capacidad_mw) OVER (PARTITION BY z.zona_id), 0) AS feeder_capacity_share_zona
    FROM stg_alimentadores a
    INNER JOIN stg_subestaciones s
        ON a.subestacion_id = s.subestacion_id
    INNER JOIN stg_zonas_red z
        ON s.zona_id = z.zona_id
),
node_base AS (
    SELECT
        d.timestamp,
        d.zona_id,
        d.subestacion_id,
        d.alimentador_id,
        d.demanda_mw,
        d.demanda_reactiva_proxy,
        d.temperatura,
        d.humedad,
        d.tipo_dia,
        d.mes,
        d.hora,
        d.factor_estacional,
        d.hora_punta_flag,
        d.tension_sistema_proxy,
        f.capacidad_mw,
        f.carga_base_esperada,
        f.nivel_perdidas_estimado,
        f.exposicion_climatica,
        f.criticidad_operativa,
        f.tipo_red,
        f.feeder_capacity_share_zona,
        SUM(d.demanda_mw) OVER (PARTITION BY d.timestamp, d.zona_id) AS demanda_total_zona_mw
    FROM stg_demanda_horaria d
    INNER JOIN feeder_capacity_share f
        ON d.alimentador_id = f.alimentador_id
       AND d.subestacion_id = f.subestacion_id
       AND d.zona_id = f.zona_id
),
node_share AS (
    SELECT
        nb.timestamp,
        nb.zona_id,
        nb.subestacion_id,
        nb.alimentador_id,
        nb.demanda_mw,
        nb.demanda_reactiva_proxy,
        nb.temperatura,
        nb.humedad,
        nb.tipo_dia,
        nb.mes,
        nb.hora,
        nb.factor_estacional,
        nb.hora_punta_flag,
        nb.tension_sistema_proxy,
        nb.capacidad_mw,
        nb.carga_base_esperada,
        nb.nivel_perdidas_estimado,
        nb.exposicion_climatica,
        nb.criticidad_operativa,
        nb.tipo_red,
        nb.feeder_capacity_share_zona,
        nb.demanda_total_zona_mw,
        nb.demanda_mw / NULLIF(nb.demanda_total_zona_mw, 0) AS feeder_demand_share_zona
    FROM node_base nb
)
SELECT
    ns.timestamp,
    ns.zona_id,
    ns.subestacion_id,
    ns.alimentador_id,
    ns.tipo_red,
    ns.demanda_mw,
    ns.demanda_reactiva_proxy,
    ns.capacidad_mw,
    ns.demanda_mw / NULLIF(ns.capacidad_mw, 0) AS carga_relativa,
    COALESCE(zh.demanda_ev_total_zona_mw, 0.0) * ns.feeder_demand_share_zona AS demanda_ev_asignada_mw,
    COALESCE(zh.demanda_industrial_total_zona_mw, 0.0) * ns.feeder_demand_share_zona AS demanda_industrial_asignada_mw,
    COALESCE(zh.generacion_distribuida_total_zona_mw, 0.0) * ns.feeder_capacity_share_zona AS generacion_distribuida_asignada_mw,
    COALESCE(zh.curtailment_total_zona_mw, 0.0) * ns.feeder_capacity_share_zona AS curtailment_asignado_mw,
    ns.demanda_mw - (COALESCE(zh.generacion_distribuida_total_zona_mw, 0.0) * ns.feeder_capacity_share_zona) AS net_load_mw,
    GREATEST(ns.demanda_mw - ns.capacidad_mw, 0.0) AS overload_mw,
    ns.demanda_total_zona_mw,
    COALESCE(zh.demanda_ev_total_zona_mw, 0.0) AS demanda_ev_total_zona_mw,
    COALESCE(zh.demanda_industrial_total_zona_mw, 0.0) AS demanda_industrial_total_zona_mw,
    COALESCE(zh.generacion_distribuida_total_zona_mw, 0.0) AS generacion_distribuida_total_zona_mw,
    COALESCE(zh.curtailment_total_zona_mw, 0.0) AS curtailment_total_zona_mw,
    ns.carga_base_esperada,
    ns.nivel_perdidas_estimado,
    ns.exposicion_climatica,
    ns.criticidad_operativa,
    ns.temperatura,
    ns.humedad,
    ns.tipo_dia,
    ns.mes,
    ns.hora,
    ns.factor_estacional,
    ns.hora_punta_flag,
    ns.tension_sistema_proxy,
    COALESCE(zh.crecimiento_demanda_indice, 1.0) AS crecimiento_demanda_indice,
    COALESCE(zh.penetracion_ev_indice, 1.0) AS penetracion_ev_indice,
    COALESCE(zh.electrificacion_industrial_indice, 1.0) AS electrificacion_industrial_indice,
    COALESCE(zh.presion_capex_indice, 1.0) AS presion_capex_indice
FROM node_share ns
LEFT JOIN vw_int_zone_hour_components zh
    ON ns.timestamp = zh.timestamp
   AND ns.zona_id = zh.zona_id;
