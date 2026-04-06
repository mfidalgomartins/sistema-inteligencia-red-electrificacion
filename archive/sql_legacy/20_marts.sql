CREATE OR REPLACE TABLE mart_feeder_daily AS
SELECT
    DATE(timestamp) AS date,
    feeder_id,
    territory_id,
    AVG(demand_mw) AS avg_demand_mw,
    MAX(demand_mw) AS peak_demand_mw,
    AVG(net_load_mw) AS avg_net_load_mw,
    MAX(net_load_mw) AS peak_net_load_mw,
    SUM(congestion_flag) AS congestion_hours,
    AVG(congestion_mw) AS avg_congestion_mw,
    SUM(estimated_curtailment_mw) AS curtailment_mwh,
    AVG(ev_load_mw) AS avg_ev_load_mw,
    AVG(industrial_load_mw) AS avg_industrial_load_mw,
    AVG(temperature_c) AS avg_temperature_c
FROM stg_hourly_feeder
GROUP BY 1, 2, 3;

CREATE OR REPLACE TABLE mart_feeder_summary AS
SELECT
    h.feeder_id,
    h.territory_id,
    MAX(h.thermal_limit_mw) AS thermal_limit_mw,
    AVG(h.asset_health_score) AS asset_health_score,
    AVG(h.age_years) AS age_years,
    AVG(h.flex_contract_capacity_mw) AS flex_contract_capacity_mw,
    AVG(h.dg_capacity_mw) AS dg_capacity_mw,
    COUNT(*) AS total_hours,
    SUM(h.congestion_flag) AS congestion_hours,
    SUM(h.congestion_flag) * 1.0 / COUNT(*) AS congestion_rate,
    MAX(h.net_load_mw) AS annual_peak_net_load_mw,
    AVG(h.net_load_mw) AS annual_avg_net_load_mw,
    SUM(h.estimated_curtailment_mw) AS annual_curtailment_mwh,
    AVG(h.ev_load_mw) AS avg_ev_load_mw,
    AVG(h.industrial_load_mw) AS avg_industrial_load_mw,
    COALESCE(o.outage_events, 0) AS outage_events,
    COALESCE(o.outage_duration_min, 0) AS outage_duration_min,
    COALESCE(o.avg_outage_duration_min, 0) AS avg_outage_duration_min,
    COALESCE(o.customers_affected_total, 0) AS customers_affected_total,
    COALESCE(o.ens_mwh, 0) AS ens_mwh,
    COALESCE(e.ev_peak_2026_2030_mw, 0) AS ev_peak_2026_2030_mw,
    COALESCE(e.industrial_peak_2026_2030_mw, 0) AS industrial_peak_2026_2030_mw,
    COALESCE(e.heat_pump_peak_2026_2030_mw, 0) AS heat_pump_peak_2026_2030_mw,
    COALESCE(e.avg_growth_pct_2026_2030, 0) AS avg_growth_pct_2026_2030
FROM stg_hourly_feeder h
LEFT JOIN stg_outage_feeder o
    ON h.feeder_id = o.feeder_id
LEFT JOIN stg_electrification_2030 e
    ON h.territory_id = e.territory_id
GROUP BY
    h.feeder_id,
    h.territory_id,
    o.outage_events,
    o.outage_duration_min,
    o.avg_outage_duration_min,
    o.customers_affected_total,
    o.ens_mwh,
    e.ev_peak_2026_2030_mw,
    e.industrial_peak_2026_2030_mw,
    e.heat_pump_peak_2026_2030_mw,
    e.avg_growth_pct_2026_2030;

CREATE OR REPLACE TABLE mart_territory_monthly AS
SELECT
    DATE_TRUNC('month', timestamp) AS month,
    territory_id,
    territory_name,
    territory_type,
    SUM(demand_mw) AS gross_demand_mwh,
    SUM(net_load_mw) AS net_demand_mwh,
    SUM(congestion_flag) AS congestion_hours,
    AVG(congestion_mw) AS avg_congestion_mw,
    SUM(estimated_curtailment_mw) AS curtailment_mwh,
    AVG(ev_load_mw) AS avg_ev_load_mw,
    AVG(industrial_load_mw) AS avg_industrial_load_mw
FROM stg_hourly_feeder
GROUP BY 1, 2, 3, 4;
