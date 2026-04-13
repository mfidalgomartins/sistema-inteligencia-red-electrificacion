import json

import pandas as pd

from src.common_v2 import V2Paths
from src.qa_smoke_v2 import run_smoke_checks_v2


def _write_minimal_fixture(root):
    paths = V2Paths(root=root)
    paths.data_processed.mkdir(parents=True, exist_ok=True)
    paths.data_raw.mkdir(parents=True, exist_ok=True)
    paths.outputs_reports.mkdir(parents=True, exist_ok=True)
    paths.outputs_dashboard.mkdir(parents=True, exist_ok=True)

    base_df = pd.DataFrame([{"x": 1}])
    for file_name in [
        "intervention_scoring_table.csv",
        "scenario_summary_v2.csv",
        "forecast_model_benchmark.csv",
        "anomalies_detected.csv",
    ]:
        base_df.to_csv(paths.data_processed / file_name, index=False)

    raw_df = pd.DataFrame([{"x": 1}])
    for file_name in [
        "zonas_red.csv",
        "subestaciones.csv",
        "alimentadores.csv",
        "demanda_horaria.csv",
        "eventos_congestion.csv",
        "interrupciones_servicio.csv",
        "activos_red.csv",
    ]:
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


def test_smoke_ok_with_governed_artifacts(monkeypatch, tmp_path):
    paths = _write_minimal_fixture(tmp_path)
    monkeypatch.setattr("src.qa_smoke_v2.get_paths", lambda: paths)

    out = run_smoke_checks_v2()

    assert out["status"] == "ok"
    assert out["validation_status"] == "WARN"
    assert out["publish_state"] == "publish-with-caveats"
    assert out["errors"] == ""


def test_smoke_fails_on_unresolved_dashboard_placeholders(monkeypatch, tmp_path):
    paths = _write_minimal_fixture(tmp_path)
    monkeypatch.setattr("src.qa_smoke_v2.get_paths", lambda: paths)
    (paths.outputs_dashboard / "grid-electrification-command-center.html").write_text(
        "<script>const DATA = __PAYLOAD__;</script>",
        encoding="utf-8",
    )

    out = run_smoke_checks_v2()

    assert out["status"] == "error"
    assert "dashboard_unresolved_placeholders" in out["errors"]


def test_smoke_fails_on_manifest_summary_mismatch(monkeypatch, tmp_path):
    paths = _write_minimal_fixture(tmp_path)
    monkeypatch.setattr("src.qa_smoke_v2.get_paths", lambda: paths)
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

    out = run_smoke_checks_v2()

    assert out["status"] == "error"
    assert "manifest_validation_status_mismatch" in out["errors"]
