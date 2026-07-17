import json

import pandas as pd

from grid_intelligence.common import ProjectPaths
from grid_intelligence.ingestion.contracts import load_contract_catalog
from grid_intelligence.quality_gates import run_quality_gates


def _write_minimal_fixture(root):
    paths = ProjectPaths(root=root)
    paths.data_processed.mkdir(parents=True, exist_ok=True)
    paths.data_raw.mkdir(parents=True, exist_ok=True)
    paths.outputs_reports.mkdir(parents=True, exist_ok=True)
    paths.outputs_dashboard.mkdir(parents=True, exist_ok=True)

    base_df = pd.DataFrame([{"x": 1}])
    for file_name in [
        "intervention_scoring_table.csv",
        "prioridades_inversion_alimentadores.csv",
        "scenario_summary.csv",
        "forecast_model_benchmark.csv",
        "forecast_calibration_summary.csv",
        "forecast_monitoring_status.csv",
        "anomalies_detected.csv",
    ]:
        base_df.to_csv(paths.data_processed / file_name, index=False)

    pd.DataFrame(
        [
            {
                "interval_level": 0.8,
                "nominal_coverage": 0.8,
                "empirical_coverage": 0.9,
                "mean_interval_width": 2.0,
            },
            {
                "interval_level": 0.95,
                "nominal_coverage": 0.95,
                "empirical_coverage": 0.95,
                "mean_interval_width": 4.0,
            },
        ]
    ).to_csv(paths.data_processed / "forecast_calibration_summary.csv", index=False)
    pd.DataFrame(
        [
            {
                "actual": 10.0,
                "pred": 10.0,
                "lower_80": 9.0,
                "upper_80": 11.0,
                "covered_80": True,
                "lower_95": 8.0,
                "upper_95": 12.0,
                "covered_95": True,
            }
        ]
    ).to_csv(paths.data_processed / "forecast_uncertainty_intervals.csv", index=False)
    pd.DataFrame(
        [
            {
                "mae_drift_ratio": 1.0,
                "mae_drift_zscore": 0.0,
                "coverage_95": 0.95,
                "monitoring_status": "stable",
                "n_entities": 1,
                "n_evaluation": 1,
            }
        ]
    ).to_csv(paths.data_processed / "forecast_monitoring_status.csv", index=False)

    raw_df = pd.DataFrame([{"x": 1}])
    for target in {contract.target_table for contract in load_contract_catalog().contracts.values()}:
        file_name = f"{target}.csv"
        raw_df.to_csv(paths.data_raw / file_name, index=False)

    (paths.outputs_reports / "validation_report.md").write_text("ok\n", encoding="utf-8")
    (paths.outputs_reports / "release_brief.md").write_text("ok\n", encoding="utf-8")
    (paths.outputs_reports / "validation_summary.json").write_text(
        json.dumps(
            {
                "overall_status": "WARN",
                "release_readiness": {
                    "publish_state": "publish-with-caveats",
                    "decision_state": "decision-support only",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (paths.outputs_reports / "release_manifest.json").write_text(
        json.dumps(
            {
                "validation_status": "WARN",
                "release_readiness": {
                    "publish_state": "publish-with-caveats",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    (paths.outputs_dashboard / "grid-electrification-command-center.html").write_text(
        "<html><body>ok</body></html>",
        encoding="utf-8",
    )

    return paths


def test_quality_gates_ok_with_governed_artifacts(monkeypatch, tmp_path):
    paths = _write_minimal_fixture(tmp_path)
    monkeypatch.setattr("grid_intelligence.quality_gates.get_paths", lambda: paths)

    out = run_quality_gates()

    assert out["status"] == "ok"
    assert out["validation_status"] == "WARN"
    assert out["publish_state"] == "publish-with-caveats"
    assert out["errors"] == ""


def test_quality_gates_fail_on_unresolved_dashboard_placeholders(monkeypatch, tmp_path):
    paths = _write_minimal_fixture(tmp_path)
    monkeypatch.setattr("grid_intelligence.quality_gates.get_paths", lambda: paths)
    (paths.outputs_dashboard / "grid-electrification-command-center.html").write_text(
        "<script>const DATA = __PAYLOAD__;</script>",
        encoding="utf-8",
    )

    out = run_quality_gates()

    assert out["status"] == "error"
    assert "dashboard_unresolved_placeholders" in out["errors"]


def test_quality_gates_fail_on_manifest_summary_mismatch(monkeypatch, tmp_path):
    paths = _write_minimal_fixture(tmp_path)
    monkeypatch.setattr("grid_intelligence.quality_gates.get_paths", lambda: paths)
    (paths.outputs_reports / "release_manifest.json").write_text(
        json.dumps(
            {
                "validation_status": "PASS",
                "release_readiness": {"publish_state": "publish-ready"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    out = run_quality_gates()

    assert out["status"] == "error"
    assert "manifest_validation_status_mismatch" in out["errors"]


def test_quality_gates_fail_on_manifest_hash_mismatch(monkeypatch, tmp_path):
    paths = _write_minimal_fixture(tmp_path)
    monkeypatch.setattr("grid_intelligence.quality_gates.get_paths", lambda: paths)
    (paths.outputs_reports / "release_manifest.json").write_text(
        json.dumps(
            {
                "validation_status": "WARN",
                "release_readiness": {"publish_state": "publish-with-caveats"},
                "artifacts": {
                    "dashboard": {
                        "exists": True,
                        "path": "outputs/dashboard/grid-electrification-command-center.html",
                        "sha256": "incorrect",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    out = run_quality_gates()

    assert out["status"] == "error"
    assert "manifest_hash_mismatch:dashboard" in out["errors"]


def test_quality_gates_report_malformed_json(monkeypatch, tmp_path):
    paths = _write_minimal_fixture(tmp_path)
    monkeypatch.setattr("grid_intelligence.quality_gates.get_paths", lambda: paths)
    (paths.outputs_reports / "validation_summary.json").write_text("{broken", encoding="utf-8")

    out = run_quality_gates()

    assert out["status"] == "error"
    assert "invalid_json:validation_summary.json" in out["errors"]
