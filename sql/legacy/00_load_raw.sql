CREATE OR REPLACE TABLE territories AS
SELECT * FROM read_csv_auto('{raw_path}/territories.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE substations AS
SELECT * FROM read_csv_auto('{raw_path}/substations.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE feeders AS
SELECT * FROM read_csv_auto('{raw_path}/feeders.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE feeder_demand_hourly AS
SELECT * FROM read_csv_auto('{raw_path}/feeder_demand_hourly.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE feeder_generation_hourly AS
SELECT * FROM read_csv_auto('{raw_path}/feeder_generation_hourly.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE outage_events AS
SELECT * FROM read_csv_auto('{raw_path}/outage_events.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE flexibility_assets AS
SELECT * FROM read_csv_auto('{raw_path}/flexibility_assets.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE electrification_forecast AS
SELECT * FROM read_csv_auto('{raw_path}/electrification_forecast.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE capex_catalog AS
SELECT * FROM read_csv_auto('{raw_path}/capex_catalog.csv', HEADER = TRUE);
