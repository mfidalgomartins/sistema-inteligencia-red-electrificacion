-- Dialecto: DuckDB SQL
-- Nivel: integration
-- Objetivo: normalizar eventos de congestión y habilitar análisis temporal por hora.

CREATE OR REPLACE VIEW vw_int_grid_events AS
SELECT
    e.evento_id,
    e.timestamp_inicio,
    e.timestamp_fin,
    e.zona_id,
    e.subestacion_id,
    e.alimentador_id,
    e.severidad,
    CASE
        WHEN e.severidad = 'critica' THEN 4
        WHEN e.severidad = 'alta' THEN 3
        WHEN e.severidad = 'media' THEN 2
        ELSE 1
    END AS severidad_score,
    e.energia_afectada_mwh,
    e.carga_relativa_max,
    e.causa_principal,
    e.impacto_servicio_flag,
    GREATEST(DATE_DIFF('hour', e.timestamp_inicio, e.timestamp_fin) + 1, 1) AS duracion_horas,
    e.energia_afectada_mwh / NULLIF(GREATEST(DATE_DIFF('hour', e.timestamp_inicio, e.timestamp_fin) + 1, 1), 0) AS energia_media_por_hora_mwh
FROM stg_eventos_congestion e;


CREATE OR REPLACE VIEW vw_int_grid_events_hourly AS
WITH expanded AS (
    SELECT
        e.evento_id,
        e.zona_id,
        e.subestacion_id,
        e.alimentador_id,
        e.severidad,
        e.severidad_score,
        e.energia_afectada_mwh,
        e.carga_relativa_max,
        e.causa_principal,
        e.impacto_servicio_flag,
        e.duracion_horas,
        gs.event_hour AS timestamp_hour
    FROM vw_int_grid_events e
    CROSS JOIN LATERAL generate_series(e.timestamp_inicio, e.timestamp_fin, INTERVAL 1 HOUR) AS gs(event_hour)
)
SELECT
    x.evento_id,
    x.timestamp_hour,
    x.zona_id,
    x.subestacion_id,
    x.alimentador_id,
    x.severidad,
    x.severidad_score,
    x.causa_principal,
    x.carga_relativa_max,
    x.impacto_servicio_flag,
    x.energia_afectada_mwh / NULLIF(x.duracion_horas, 0) AS energia_afectada_hora_mwh
FROM expanded x;
