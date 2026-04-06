CREATE OR REPLACE VIEW kpi_network_overview AS
SELECT
    COUNT(DISTINCT feeder_id) AS feeders,
    SUM(congestion_hours) AS total_congestion_hours,
    AVG(congestion_rate) AS avg_congestion_rate,
    SUM(annual_curtailment_mwh) AS total_curtailment_mwh,
    SUM(ens_mwh) AS total_ens_mwh,
    AVG(asset_health_score) AS avg_asset_health_score,
    SUM(ev_peak_2026_2030_mw + industrial_peak_2026_2030_mw + heat_pump_peak_2026_2030_mw) AS incremental_peak_2030_mw
FROM mart_feeder_summary;

CREATE OR REPLACE VIEW kpi_top_feeders_stress AS
SELECT
    feeder_id,
    territory_id,
    congestion_hours,
    congestion_rate,
    annual_peak_net_load_mw,
    thermal_limit_mw,
    annual_curtailment_mwh,
    ens_mwh,
    avg_growth_pct_2026_2030
FROM mart_feeder_summary
ORDER BY congestion_rate DESC, congestion_hours DESC
LIMIT 25;

CREATE OR REPLACE VIEW kpi_territorial_pressure AS
SELECT
    territory_id,
    SUM(congestion_hours) AS congestion_hours,
    SUM(curtailment_mwh) AS curtailment_mwh,
    AVG(avg_ev_load_mw) AS avg_ev_load_mw,
    AVG(avg_industrial_load_mw) AS avg_industrial_load_mw,
    COUNT(DISTINCT month) AS observed_months
FROM mart_territory_monthly
GROUP BY 1
ORDER BY congestion_hours DESC;
