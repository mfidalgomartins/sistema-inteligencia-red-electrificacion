-- Dialecto: DuckDB SQL
-- Nivel: integration
-- Objetivo: consolidar continuidad de servicio e integrar relación con congestión.

CREATE OR REPLACE VIEW vw_int_service_quality_events AS
SELECT
    i.interrupcion_id,
    i.timestamp_inicio,
    i.timestamp_fin,
    i.zona_id,
    i.subestacion_id,
    i.clientes_afectados,
    i.energia_no_suministrada_mwh,
    i.causa,
    i.nivel_severidad,
    CASE
        WHEN i.nivel_severidad = 'critica' THEN 4
        WHEN i.nivel_severidad = 'alta' THEN 3
        WHEN i.nivel_severidad = 'media' THEN 2
        ELSE 1
    END AS nivel_severidad_score,
    i.relacion_congestion_flag,
    GREATEST(DATE_DIFF('minute', i.timestamp_inicio, i.timestamp_fin), 1) AS duracion_minutos,
    GREATEST(DATE_DIFF('hour', i.timestamp_inicio, i.timestamp_fin) + 1, 1) AS duracion_horas
FROM stg_interrupciones_servicio i;


CREATE OR REPLACE VIEW vw_int_service_quality_enriched AS
WITH overlap AS (
    SELECT
        s.interrupcion_id,
        COUNT(DISTINCT g.evento_id) AS n_eventos_congestion_solapados,
        MAX(g.severidad_score) AS max_severidad_congestion_solapada,
        SUM(g.energia_afectada_mwh) AS energia_congestion_solapada_mwh
    FROM vw_int_service_quality_events s
    LEFT JOIN vw_int_grid_events g
        ON s.subestacion_id = g.subestacion_id
       -- Los eventos de congestión se modelan en bucket horario.
       -- Para evitar falsos negativos en eventos de 1h (inicio=fin), extendemos el final +1h.
       AND g.timestamp_inicio <= s.timestamp_fin
       AND (g.timestamp_fin + INTERVAL 1 HOUR) >= s.timestamp_inicio
    GROUP BY s.interrupcion_id
)
SELECT
    s.interrupcion_id,
    s.timestamp_inicio,
    s.timestamp_fin,
    s.zona_id,
    s.subestacion_id,
    s.clientes_afectados,
    s.energia_no_suministrada_mwh,
    s.causa,
    s.nivel_severidad,
    s.nivel_severidad_score,
    s.relacion_congestion_flag,
    s.duracion_minutos,
    s.duracion_horas,
    COALESCE(o.n_eventos_congestion_solapados, 0) AS n_eventos_congestion_solapados,
    COALESCE(o.max_severidad_congestion_solapada, 0) AS max_severidad_congestion_solapada,
    COALESCE(o.energia_congestion_solapada_mwh, 0.0) AS energia_congestion_solapada_mwh,
    CASE
        WHEN COALESCE(o.n_eventos_congestion_solapados, 0) > 0 THEN TRUE
        ELSE FALSE
    END AS congestion_overlap_flag
FROM vw_int_service_quality_events s
LEFT JOIN overlap o
    ON s.interrupcion_id = o.interrupcion_id;
