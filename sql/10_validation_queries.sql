-- Dialecto: DuckDB SQL
-- Nivel: validation queries
-- Objetivo: controles de calidad y consistencia para staging, integración y marts analíticos.

CREATE OR REPLACE TABLE validation_checks AS
WITH checks AS (
    SELECT
        'staging_pk' AS check_group,
        'pk_zonas_red_unica' AS check_name,
        CASE WHEN COUNT(*) = COUNT(DISTINCT zona_id) THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) - COUNT(DISTINCT zona_id) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'alta' AS severity,
        'zonas_red debe tener zona_id único' AS details
    FROM stg_zonas_red

    UNION ALL

    SELECT
        'staging_pk' AS check_group,
        'pk_subestaciones_unica' AS check_name,
        CASE WHEN COUNT(*) = COUNT(DISTINCT subestacion_id) THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) - COUNT(DISTINCT subestacion_id) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'alta' AS severity,
        'subestaciones debe tener subestacion_id único' AS details
    FROM stg_subestaciones

    UNION ALL

    SELECT
        'staging_pk' AS check_group,
        'pk_alimentadores_unica' AS check_name,
        CASE WHEN COUNT(*) = COUNT(DISTINCT alimentador_id) THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) - COUNT(DISTINCT alimentador_id) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'alta' AS severity,
        'alimentadores debe tener alimentador_id único' AS details
    FROM stg_alimentadores

    UNION ALL

    SELECT
        'staging_fk' AS check_group,
        'fk_subestaciones_zona_valida' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'alta' AS severity,
        'todas las subestaciones deben mapear a una zona existente' AS details
    FROM stg_subestaciones s
    LEFT JOIN stg_zonas_red z
        ON s.zona_id = z.zona_id
    WHERE z.zona_id IS NULL

    UNION ALL

    SELECT
        'staging_fk' AS check_group,
        'fk_alimentadores_subestacion_valida' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'alta' AS severity,
        'todos los alimentadores deben mapear a una subestación existente' AS details
    FROM stg_alimentadores a
    LEFT JOIN stg_subestaciones s
        ON a.subestacion_id = s.subestacion_id
    WHERE s.subestacion_id IS NULL

    UNION ALL

    SELECT
        'staging_domain' AS check_group,
        'demanda_horaria_no_negativa' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'alta' AS severity,
        'demanda_mw no debe tener valores negativos' AS details
    FROM stg_demanda_horaria
    WHERE demanda_mw < 0

    UNION ALL

    SELECT
        'staging_domain' AS check_group,
        'capacidades_alimentador_positivas' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'alta' AS severity,
        'capacidad_mw de alimentadores debe ser > 0' AS details
    FROM stg_alimentadores
    WHERE capacidad_mw <= 0

    UNION ALL

    SELECT
        'integration_quality' AS check_group,
        'node_hour_duplicados_clave' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'alta' AS severity,
        'clave (timestamp,zona,subestacion,alimentador) debe ser única en mart_node_hour_operational_state' AS details
    FROM (
        SELECT
            timestamp,
            zona_id,
            subestacion_id,
            alimentador_id,
            COUNT(*) AS n_registros
        FROM mart_node_hour_operational_state
        GROUP BY
            timestamp,
            zona_id,
            subestacion_id,
            alimentador_id
        HAVING COUNT(*) > 1
    ) dups

    UNION ALL

    SELECT
        'integration_domain' AS check_group,
        'carga_relativa_rango_razonable' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'media' AS severity,
        'carga_relativa debe estar en rango [0, 2.5]' AS details
    FROM mart_node_hour_operational_state
    WHERE carga_relativa < 0 OR carga_relativa > 2.5

    UNION ALL

    SELECT
        'integration_domain' AS check_group,
        'gap_tecnico_no_negativo' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'media' AS severity,
        'gap_tecnico_mw no debe ser negativo' AS details
    FROM vw_flexibility_gap
    WHERE gap_tecnico_mw < 0

    UNION ALL

    SELECT
        'consistency_temporal' AS check_group,
        'eventos_congestion_inicio_fin_validos' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'alta' AS severity,
        'timestamp_inicio debe ser menor o igual a timestamp_fin en eventos de congestión' AS details
    FROM stg_eventos_congestion
    WHERE timestamp_fin < timestamp_inicio

    UNION ALL

    SELECT
        'consistency_temporal' AS check_group,
        'interrupciones_inicio_fin_validos' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'alta' AS severity,
        'timestamp_inicio debe ser menor o igual a timestamp_fin en interrupciones' AS details
    FROM stg_interrupciones_servicio
    WHERE timestamp_fin < timestamp_inicio

    UNION ALL

    SELECT
        'cross_domain' AS check_group,
        'interrupciones_congestion_sin_solape' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        CAST(COUNT(*) AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'media' AS severity,
        'interrupciones marcadas con relacion_congestion_flag no deberían carecer de solape real' AS details
    FROM vw_int_service_quality_enriched
    WHERE relacion_congestion_flag = TRUE
      AND congestion_overlap_flag = FALSE

    UNION ALL

    SELECT
        'mart_coverage' AS check_group,
        'cobertura_zonas_en_risk_view' AS check_name,
        CASE WHEN n_zonas_stage = n_zonas_risk THEN 1 ELSE 0 END AS passed,
        CAST(n_zonas_stage - n_zonas_risk AS DOUBLE) AS observed_value,
        0.0 AS threshold_value,
        'media' AS severity,
        'todas las zonas del staging deben aparecer en vw_zone_operational_risk' AS details
    FROM (
        SELECT
            (SELECT COUNT(DISTINCT zona_id) FROM stg_zonas_red) AS n_zonas_stage,
            (SELECT COUNT(DISTINCT zona_id) FROM vw_zone_operational_risk) AS n_zonas_risk
    ) coverage
)
SELECT
    check_group,
    check_name,
    passed,
    observed_value,
    threshold_value,
    severity,
    details
FROM checks;

CREATE OR REPLACE VIEW vw_validation_failures AS
SELECT
    v.check_group,
    v.check_name,
    v.passed,
    v.observed_value,
    v.threshold_value,
    v.severity,
    v.details
FROM validation_checks v
WHERE v.passed = 0
ORDER BY
    CASE v.severity
        WHEN 'alta' THEN 1
        WHEN 'media' THEN 2
        ELSE 3
    END,
    v.check_group,
    v.check_name;
