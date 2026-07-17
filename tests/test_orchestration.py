import pytest

from grid_intelligence import orchestration


def test_release_pipeline_runs_feeder_priority_before_scenarios(monkeypatch):
    calls: list[str] = []

    def stage(name, result=None):
        def run(*args, **kwargs):
            calls.append(name)
            return result if result is not None else {}

        return run

    monkeypatch.setattr(orchestration, "ensure_dirs", stage("ensure_dirs"))
    monkeypatch.setattr(orchestration, "get_paths", lambda: object())
    monkeypatch.setattr(orchestration, "generate_synthetic_ecosystem", stage("synthetic_data"))
    monkeypatch.setattr(orchestration, "run_sql_layer", stage("sql_layer"))
    monkeypatch.setattr(orchestration, "build_features", stage("feature_engineering"))
    monkeypatch.setattr(orchestration, "run_forecasting", stage("forecasting"))
    monkeypatch.setattr(orchestration, "run_anomaly_detection", stage("anomaly_detection"))
    monkeypatch.setattr(orchestration, "run_scoring", stage("zone_scoring"))
    monkeypatch.setattr(orchestration, "run_feeder_prioritization", stage("feeder_prioritization"))
    monkeypatch.setattr(orchestration, "run_scenario_engine", stage("scenario_engine"))
    monkeypatch.setattr(orchestration, "run_advanced_analysis", stage("advanced_analysis"))
    monkeypatch.setattr(orchestration, "run_visualization", stage("visualization"))
    monkeypatch.setattr(orchestration, "build_dashboard", stage("dashboard", "dashboard.html"))
    monkeypatch.setattr(orchestration, "run_validation", stage("validation"))
    monkeypatch.setattr(orchestration, "build_release_docs", stage("release_docs", {"release_brief": "brief.md"}))
    monkeypatch.setattr(
        orchestration,
        "build_release_manifest",
        stage("release_manifest", {"release_readiness": {"publish_state": "publish-with-caveats"}}),
    )
    monkeypatch.setattr(orchestration, "run_quality_gates", stage("quality_gates", {"status": "ok"}))

    result = orchestration.run_release_pipeline()

    assert calls.index("feeder_prioritization") < calls.index("scenario_engine")
    assert result == {
        "source_mode": "synthetic",
        "dashboard": "dashboard.html",
        "release_brief": "brief.md",
        "smoke_status": "ok",
        "publish_state": "publish-with-caveats",
    }


def test_external_mode_validates_contracts_instead_of_generating_synthetic_data(monkeypatch):
    validated: list[str] = []

    class FakeIngestionService:
        def __init__(self, paths):
            self.paths = paths

        def validate_external_raw_inputs(self):
            validated.append("external")
            raise RuntimeError("stop-after-source-check")

    monkeypatch.setattr(orchestration, "ensure_dirs", lambda paths: paths)
    monkeypatch.setattr(orchestration, "get_paths", lambda: object())
    monkeypatch.setattr(orchestration, "IngestionService", FakeIngestionService)
    monkeypatch.setattr(
        orchestration,
        "generate_synthetic_ecosystem",
        lambda: pytest.fail("El generador no debe ejecutarse en modo externo"),
    )

    with pytest.raises(RuntimeError, match="stop-after-source-check"):
        orchestration.run_release_pipeline(source_mode="external")

    assert validated == ["external"]
