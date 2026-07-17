import duckdb
import pandas as pd
from fastapi.testclient import TestClient

from grid_intelligence.api.app import ApiSettings, create_app
from grid_intelligence.common import ProjectPaths, ensure_dirs


def _client(tmp_path) -> TestClient:
    paths = ensure_dirs(ProjectPaths(tmp_path))
    pd.DataFrame(
        [
            {
                "zona_id": "Z001",
                "priority_rank": 1,
                "investment_priority_score": 88.0,
                "risk_tier": "critico",
                "urgency_tier": "inmediata",
                "recommended_intervention": "reforzar_red_local",
                "recommended_sequence": "0-6 meses",
                "main_risk_driver": "congestion_risk_score",
                "confidence_flag": "alta",
                "congestion_risk_score": 90.0,
                "service_impact_score": 80.0,
                "flexibility_gap_score": 70.0,
                "asset_exposure_score": 60.0,
                "electrification_pressure_score": 75.0,
                "economic_priority_score": 85.0,
                "capex_total": 1_000_000.0,
                "coste_riesgo_proxy": 50_000.0,
            }
        ]
    ).to_csv(paths.data_processed / "intervention_scoring_table.csv", index=False)
    pd.DataFrame(
        [
            {
                "ranking_prioridad": 1,
                "alimentador_id": "A001",
                "subestacion_id": "S001",
                "zona_id": "Z001",
                "nivel_prioridad": "Crítica",
                "puntuacion_prioridad": 90.0,
                "puntuacion_tecnica": 91.0,
                "accion_recomendada": "refuerzo_selectivo",
                "alivio_requerido_mw": 5.0,
            }
        ]
    ).to_csv(paths.data_processed / "prioridades_inversion_alimentadores.csv", index=False)
    pd.DataFrame([{"scenario": "retraso_capex", "coste_riesgo_total": 120_000.0, "prioridad_media": 70.0}]).to_csv(
        paths.data_processed / "scenario_summary.csv", index=False
    )
    pd.DataFrame(
        [
            {
                "zona_id": "Z001",
                "zona_nombre": "Zona uno",
                "tipo_zona": "urbana",
                "region_operativa": "Centro",
            }
        ]
    ).to_csv(paths.data_processed / "mart_zone_month_operational.csv", index=False)
    pd.DataFrame(
        [
            {
                "best_model": "seasonal_naive",
                "latest_fold": 4,
                "historical_mae": 10.0,
                "latest_mae": 11.0,
                "mae_drift_ratio": 1.1,
                "coverage_95": 0.95,
                "monitoring_status": "stable",
            }
        ]
    ).to_csv(paths.data_processed / "forecast_monitoring_status.csv", index=False)
    conn = duckdb.connect(str(paths.database))
    try:
        conn.execute(
            """
            CREATE TABLE mart_zone_day_operational AS
            SELECT DATE '2025-01-01' AS fecha, 'Z001' AS zona_id, 120.0 AS demanda_total_mwh;
            CREATE TABLE mart_zone_month_operational AS
            SELECT DATE '2025-01-01' AS mes, 'Z001' AS zona_id, 3600.0 AS demanda_total_mwh;
            """
        )
    finally:
        conn.close()
    return TestClient(create_app(paths=paths, settings=ApiSettings(mutation_api_key="secret-test-key")))


def _decision_payload() -> dict:
    return {
        "idempotency_key": "api-case-001",
        "zona_id": "Z001",
        "alimentador_id": "A001",
        "recomendacion": "refuerzo_selectivo",
        "owner": "Planificación",
        "expected_capex_eur": 1_000_000,
        "expected_annual_benefit_eur": 200_000,
        "expected_risk_reduction_pct": 30,
    }


def test_analytical_endpoints_are_paginated_and_healthy(tmp_path):
    client = _client(tmp_path)

    health = client.get("/health")
    zones = client.get("/v1/zones?limit=1")
    mart = client.get("/v1/marts/zone-day?start_date=2025-01-01&end_date=2025-01-31")

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert health.headers["X-Request-ID"]
    assert zones.json()["total"] == 1
    assert zones.json()["items"][0]["zona_id"] == "Z001"
    assert mart.json()["items"][0]["demanda_total_mwh"] == 120.0


def test_decision_mutations_require_key_and_support_idempotency(tmp_path):
    client = _client(tmp_path)
    payload = _decision_payload()

    assert client.post("/v1/decisions", json=payload).status_code == 401
    created = client.post("/v1/decisions", json=payload, headers={"X-API-Key": "secret-test-key"})
    replay = client.post("/v1/decisions", json=payload, headers={"X-API-Key": "secret-test-key"})

    assert created.status_code == 201
    assert replay.status_code == 200
    assert replay.json()["decision_id"] == created.json()["decision_id"]

    decision_id = created.json()["decision_id"]
    reviewed = client.post(
        f"/v1/decisions/{decision_id}/transitions",
        json={"to_status": "under_review", "expected_version": 1, "actor": "Comité"},
        headers={"X-API-Key": "secret-test-key"},
    )
    conflict = client.post(
        f"/v1/decisions/{decision_id}/transitions",
        json={"to_status": "approved", "expected_version": 1, "actor": "Comité"},
        headers={"X-API-Key": "secret-test-key"},
    )

    assert reviewed.status_code == 200
    assert reviewed.json()["version"] == 2
    assert conflict.status_code == 409
    assert client.get(f"/v1/decisions/{decision_id}/events").json()[0]["event_type"] == "created"
