CREATE OR REPLACE TABLE stg_hourly_feeder AS
SELECT
    d.timestamp,
    d.feeder_id,
    f.substation_id,
    f.territory_id,
    t.territory_name,
    t.territory_type,
    f.feeder_type,
    f.thermal_limit_mw,
    f.flex_contract_capacity_mw,
    f.dg_capacity_mw,
    f.asset_health_score,
    f.age_years,
    d.demand_mw,
    d.ev_load_mw,
    d.industrial_load_mw,
    d.temperature_c,
    g.pv_generation_mw,
    g.wind_generation_mw,
    g.dg_total_mw,
    d.demand_mw - g.dg_total_mw AS net_load_mw,
    GREATEST(d.demand_mw - g.dg_total_mw - f.thermal_limit_mw, 0) AS congestion_mw,
    CASE WHEN d.demand_mw - g.dg_total_mw > f.thermal_limit_mw THEN 1 ELSE 0 END AS congestion_flag,
    -- Curtailment estimado en horas de baja demanda con saturación de capacidad de absorción local
    CASE
        WHEN d.demand_mw < 0.85 * f.thermal_limit_mw
        THEN GREATEST(g.dg_total_mw - (0.12 * f.thermal_limit_mw + 0.06 * d.demand_mw), 0)
        ELSE 0
    END AS estimated_curtailment_mw
FROM feeder_demand_hourly d
INNER JOIN feeder_generation_hourly g
    ON d.timestamp = g.timestamp
    AND d.feeder_id = g.feeder_id
INNER JOIN feeders f
    ON d.feeder_id = f.feeder_id
INNER JOIN territories t
    ON f.territory_id = t.territory_id;

CREATE OR REPLACE TABLE stg_outage_feeder AS
SELECT
    f.feeder_id,
    COUNT(*) AS outage_events,
    SUM(o.duration_min) AS outage_duration_min,
    AVG(o.duration_min) AS avg_outage_duration_min,
    SUM(o.customers_affected) AS customers_affected_total,
    SUM(o.energy_not_served_mwh) AS ens_mwh
FROM feeders f
LEFT JOIN outage_events o
    ON f.feeder_id = o.feeder_id
GROUP BY 1;

CREATE OR REPLACE TABLE stg_electrification_2030 AS
SELECT
    territory_id,
    SUM(ev_peak_additional_mw) FILTER (WHERE year BETWEEN 2026 AND 2030) AS ev_peak_2026_2030_mw,
    SUM(industrial_peak_additional_mw) FILTER (WHERE year BETWEEN 2026 AND 2030) AS industrial_peak_2026_2030_mw,
    SUM(heat_pump_peak_additional_mw) FILTER (WHERE year BETWEEN 2026 AND 2030) AS heat_pump_peak_2026_2030_mw,
    AVG(base_demand_growth_pct) FILTER (WHERE year BETWEEN 2026 AND 2030) AS avg_growth_pct_2026_2030
FROM electrification_forecast
GROUP BY 1;
