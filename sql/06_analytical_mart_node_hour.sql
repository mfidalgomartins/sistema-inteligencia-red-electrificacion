-- Dialecto: DuckDB SQL
-- Nivel: analytical marts
-- Objetivo: construir el estado operativo horario por nodo para análisis técnico y de resiliencia.

CREATE OR REPLACE VIEW vw_node_hour_operational_state AS
WITH zone_flex AS (
    SELECT
        f.zona_id,
        f.capacidad_flexible_mw,
        f.coste_activacion_ponderado_eur_mwh,
        f.storage_potencia_total_mw,
        f.storage_disponibilidad_media,
        f.storage_energia_total_mwh,
        f.soporte_flex_storage_mw,
        f.flex_coverage_ratio,
        f.peak_demand_zona_mw,
        f.tension_crecimiento_demanda,
        f.criticidad_territorial
    FROM vw_int_flexibility_assets_zone f
),
congestion_hour AS (
    SELECT
        g.timestamp_hour AS timestamp,
        g.zona_id,
        g.subestacion_id,
        g.alimentador_id,
        COUNT(g.evento_id) AS eventos_congestion_hora,
        MAX(g.severidad_score) AS max_severidad_congestion_hora,
        SUM(g.energia_afectada_hora_mwh) AS energia_afectada_hora_mwh
    FROM vw_int_grid_events_hourly g
    GROUP BY
        g.timestamp_hour,
        g.zona_id,
        g.subestacion_id,
        g.alimentador_id
),
node_base AS (
    SELECT
        n.timestamp,
        n.zona_id,
        n.subestacion_id,
        n.alimentador_id,
        n.tipo_red,
        n.demanda_mw,
        n.demanda_reactiva_proxy,
        n.capacidad_mw,
        n.carga_relativa,
        n.demanda_ev_asignada_mw,
        n.demanda_industrial_asignada_mw,
        n.generacion_distribuida_asignada_mw,
        n.curtailment_asignado_mw,
        n.net_load_mw,
        n.overload_mw,
        n.demanda_total_zona_mw,
        n.demanda_ev_total_zona_mw,
        n.demanda_industrial_total_zona_mw,
        n.generacion_distribuida_total_zona_mw,
        n.curtailment_total_zona_mw,
        n.nivel_perdidas_estimado,
        n.exposicion_climatica,
        n.criticidad_operativa,
        n.temperatura,
        n.humedad,
        n.tipo_dia,
        n.mes,
        n.hora,
        n.factor_estacional,
        n.hora_punta_flag,
        n.tension_sistema_proxy,
        n.crecimiento_demanda_indice,
        n.penetracion_ev_indice,
        n.electrificacion_industrial_indice,
        n.presion_capex_indice,
        zf.capacidad_flexible_mw,
        zf.coste_activacion_ponderado_eur_mwh,
        zf.storage_potencia_total_mw,
        zf.storage_disponibilidad_media,
        zf.storage_energia_total_mwh,
        zf.soporte_flex_storage_mw,
        zf.flex_coverage_ratio,
        zf.peak_demand_zona_mw,
        zf.tension_crecimiento_demanda,
        zf.criticidad_territorial,
        COALESCE(ch.eventos_congestion_hora, 0) AS eventos_congestion_hora,
        COALESCE(ch.max_severidad_congestion_hora, 0) AS max_severidad_congestion_hora,
        COALESCE(ch.energia_afectada_hora_mwh, 0.0) AS energia_afectada_hora_mwh
    FROM vw_int_network_load_hour n
    LEFT JOIN zone_flex zf
        ON n.zona_id = zf.zona_id
    LEFT JOIN congestion_hour ch
        ON n.timestamp = ch.timestamp
       AND n.zona_id = ch.zona_id
       AND n.subestacion_id = ch.subestacion_id
       AND n.alimentador_id = ch.alimentador_id
)
SELECT
    nb.timestamp,
    CAST(nb.timestamp AS DATE) AS fecha,
    nb.zona_id,
    nb.subestacion_id,
    nb.alimentador_id,
    nb.tipo_red,
    nb.demanda_mw,
    nb.demanda_reactiva_proxy,
    nb.capacidad_mw,
    nb.net_load_mw,
    nb.carga_relativa,
    nb.net_load_mw / NULLIF(nb.capacidad_mw, 0) AS carga_relativa_neta,
    nb.overload_mw,
    nb.demanda_ev_asignada_mw,
    nb.demanda_industrial_asignada_mw,
    nb.generacion_distribuida_asignada_mw,
    nb.curtailment_asignado_mw,
    nb.demanda_total_zona_mw,
    nb.demanda_ev_total_zona_mw,
    nb.demanda_industrial_total_zona_mw,
    nb.generacion_distribuida_total_zona_mw,
    nb.curtailment_total_zona_mw,
    nb.capacidad_flexible_mw,
    nb.storage_potencia_total_mw,
    nb.storage_energia_total_mwh,
    nb.storage_disponibilidad_media,
    nb.soporte_flex_storage_mw,
    nb.flex_coverage_ratio,
    nb.coste_activacion_ponderado_eur_mwh,
    COALESCE(nb.soporte_flex_storage_mw, 0.0) * (nb.demanda_mw / NULLIF(nb.demanda_total_zona_mw, 0.0)) AS flexibilidad_cobertura_mw,
    COALESCE(nb.storage_potencia_total_mw, 0.0)
        * COALESCE(nb.storage_disponibilidad_media, 0.0)
        * (nb.demanda_mw / NULLIF(nb.demanda_total_zona_mw, 0.0)) AS storage_support_proxy_mw,
    nb.demanda_mw + nb.demanda_ev_asignada_mw + nb.demanda_industrial_asignada_mw AS demanda_critica_mw,
    nb.eventos_congestion_hora,
    nb.max_severidad_congestion_hora,
    nb.energia_afectada_hora_mwh,
    nb.nivel_perdidas_estimado,
    nb.exposicion_climatica,
    nb.criticidad_operativa,
    nb.temperatura,
    nb.humedad,
    nb.tipo_dia,
    nb.mes,
    nb.hora,
    nb.factor_estacional,
    nb.hora_punta_flag,
    nb.tension_sistema_proxy,
    nb.crecimiento_demanda_indice,
    nb.penetracion_ev_indice,
    nb.electrificacion_industrial_indice,
    nb.presion_capex_indice,
    nb.tension_crecimiento_demanda,
    nb.criticidad_territorial,
    CASE WHEN nb.carga_relativa >= 0.85 THEN TRUE ELSE FALSE END AS flag_carga_alta,
    CASE WHEN nb.carga_relativa >= 1.00 OR nb.overload_mw > 0 OR nb.eventos_congestion_hora > 0 THEN TRUE ELSE FALSE END AS flag_congestion,
    CASE WHEN nb.temperatura >= 33 OR nb.exposicion_climatica >= 0.70 THEN TRUE ELSE FALSE END AS flag_estres_climatico,
    CASE WHEN nb.tension_sistema_proxy < 0.94 OR nb.tension_sistema_proxy > 1.06 THEN TRUE ELSE FALSE END AS flag_estres_tension,
    CASE
        WHEN nb.carga_relativa >= 1.00
          OR nb.overload_mw > 0
          OR nb.eventos_congestion_hora > 0
          OR nb.curtailment_asignado_mw > 0
          OR nb.tension_sistema_proxy < 0.94
          OR nb.tension_sistema_proxy > 1.06
        THEN TRUE
        ELSE FALSE
    END AS flag_estres_operativo
FROM node_base nb;

CREATE OR REPLACE TABLE mart_node_hour_operational_state AS
SELECT
    timestamp,
    fecha,
    zona_id,
    subestacion_id,
    alimentador_id,
    tipo_red,
    demanda_mw,
    demanda_reactiva_proxy,
    capacidad_mw,
    net_load_mw,
    carga_relativa,
    carga_relativa_neta,
    overload_mw,
    demanda_ev_asignada_mw,
    demanda_industrial_asignada_mw,
    generacion_distribuida_asignada_mw,
    curtailment_asignado_mw,
    demanda_total_zona_mw,
    demanda_ev_total_zona_mw,
    demanda_industrial_total_zona_mw,
    generacion_distribuida_total_zona_mw,
    curtailment_total_zona_mw,
    capacidad_flexible_mw,
    storage_potencia_total_mw,
    storage_energia_total_mwh,
    storage_disponibilidad_media,
    soporte_flex_storage_mw,
    flex_coverage_ratio,
    coste_activacion_ponderado_eur_mwh,
    flexibilidad_cobertura_mw,
    storage_support_proxy_mw,
    demanda_critica_mw,
    eventos_congestion_hora,
    max_severidad_congestion_hora,
    energia_afectada_hora_mwh,
    nivel_perdidas_estimado,
    exposicion_climatica,
    criticidad_operativa,
    temperatura,
    humedad,
    tipo_dia,
    mes,
    hora,
    factor_estacional,
    hora_punta_flag,
    tension_sistema_proxy,
    crecimiento_demanda_indice,
    penetracion_ev_indice,
    electrificacion_industrial_indice,
    presion_capex_indice,
    tension_crecimiento_demanda,
    criticidad_territorial,
    flag_carga_alta,
    flag_congestion,
    flag_estres_climatico,
    flag_estres_tension,
    flag_estres_operativo
FROM vw_node_hour_operational_state;
