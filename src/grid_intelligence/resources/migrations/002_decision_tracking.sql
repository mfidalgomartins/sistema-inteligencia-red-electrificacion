CREATE TABLE IF NOT EXISTS decisions (
    decision_id VARCHAR PRIMARY KEY,
    idempotency_key VARCHAR NOT NULL UNIQUE,
    request_hash VARCHAR NOT NULL,
    zona_id VARCHAR NOT NULL,
    alimentador_id VARCHAR,
    recomendacion VARCHAR NOT NULL,
    owner VARCHAR NOT NULL,
    status VARCHAR NOT NULL CHECK (
        status IN ('proposed', 'under_review', 'approved', 'rejected', 'in_execution', 'implemented', 'verified', 'cancelled')
    ),
    currency VARCHAR NOT NULL DEFAULT 'EUR',
    expected_capex_eur DOUBLE,
    expected_annual_benefit_eur DOUBLE,
    expected_risk_reduction_pct DOUBLE,
    actual_capex_eur DOUBLE,
    actual_annual_benefit_eur DOUBLE,
    actual_risk_reduction_pct DOUBLE,
    verification_due_date DATE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    approved_at TIMESTAMP,
    execution_started_at TIMESTAMP,
    implemented_at TIMESTAMP,
    verified_at TIMESTAMP,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_decisions_status_zone
    ON decisions (status, zona_id);

CREATE TABLE IF NOT EXISTS decision_events (
    event_id VARCHAR PRIMARY KEY,
    decision_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    from_status VARCHAR,
    to_status VARCHAR NOT NULL,
    actor VARCHAR NOT NULL,
    notes VARCHAR,
    payload_json VARCHAR NOT NULL,
    occurred_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_decision_events_decision_time
    ON decision_events (decision_id, occurred_at);

