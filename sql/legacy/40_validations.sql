CREATE OR REPLACE TABLE validation_checks AS
WITH checks AS (
    SELECT
        'raw_feeders_unique' AS check_name,
        CASE WHEN COUNT(*) = COUNT(DISTINCT feeder_id) THEN 1 ELSE 0 END AS passed,
        COUNT(*)::DOUBLE AS observed_value,
        COUNT(DISTINCT feeder_id)::DOUBLE AS expected_or_reference
    FROM feeders

    UNION ALL

    SELECT
        'hourly_no_null_key' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        COUNT(*)::DOUBLE AS observed_value,
        0.0 AS expected_or_reference
    FROM stg_hourly_feeder
    WHERE feeder_id IS NULL OR timestamp IS NULL

    UNION ALL

    SELECT
        'hourly_non_negative_demand' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        COUNT(*)::DOUBLE AS observed_value,
        0.0 AS expected_or_reference
    FROM stg_hourly_feeder
    WHERE demand_mw < 0

    UNION ALL

    SELECT
        'mart_feeder_count_alignment' AS check_name,
        CASE WHEN raw_cnt = mart_cnt THEN 1 ELSE 0 END AS passed,
        mart_cnt::DOUBLE AS observed_value,
        raw_cnt::DOUBLE AS expected_or_reference
    FROM (
        SELECT
            (SELECT COUNT(*) FROM feeders) AS raw_cnt,
            (SELECT COUNT(*) FROM mart_feeder_summary) AS mart_cnt
    )

    UNION ALL

    SELECT
        'congestion_rate_bounds' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS passed,
        COUNT(*)::DOUBLE AS observed_value,
        0.0 AS expected_or_reference
    FROM mart_feeder_summary
    WHERE congestion_rate < 0 OR congestion_rate > 1
)
SELECT * FROM checks;
