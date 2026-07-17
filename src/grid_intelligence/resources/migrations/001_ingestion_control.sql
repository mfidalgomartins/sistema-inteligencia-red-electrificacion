CREATE TABLE IF NOT EXISTS ingestion_batches (
    contract_name VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    source_checksum VARCHAR NOT NULL,
    source_path VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    min_event_time TIMESTAMP,
    max_event_time TIMESTAMP,
    status VARCHAR NOT NULL CHECK (status IN ('committed', 'rejected')),
    committed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (contract_name, batch_id)
);

CREATE TABLE IF NOT EXISTS ingestion_partitions (
    partition_id VARCHAR PRIMARY KEY,
    contract_name VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    partition_value VARCHAR NOT NULL,
    relative_path VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    file_checksum VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    UNIQUE (contract_name, batch_id, relative_path)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_partitions_contract_date
    ON ingestion_partitions (contract_name, partition_value);

CREATE TABLE IF NOT EXISTS incremental_runs (
    run_id VARCHAR PRIMARY KEY,
    process_date DATE NOT NULL,
    input_watermark VARCHAR NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'skipped')),
    feeder_rows BIGINT NOT NULL DEFAULT 0,
    zone_rows BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    error_message VARCHAR,
    UNIQUE (process_date, input_watermark)
);

