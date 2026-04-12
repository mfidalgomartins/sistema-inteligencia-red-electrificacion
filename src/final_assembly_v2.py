from __future__ import annotations

import subprocess
from pathlib import Path
from textwrap import dedent

import pandas as pd

from .analysis_v2 import run_advanced_analysis_v2
from .anomaly_detection_v2 import run_anomaly_detection_v2
from .common_v2 import ensure_dirs, get_paths
from .dashboard_v2 import build_dashboard_v2
from .feature_engineering_v2 import build_features_v2
from .final_docs_v2 import build_final_docs_v2
from .forecasting_v2 import run_forecasting_v2
from .qa_smoke_v2 import run_smoke_checks_v2
from .release_manifest_v2 import build_release_manifest_v2
from .scenario_engine_v2 import run_scenario_engine_v2
from .scoring_v2 import run_scoring_v2
from .sql_runner_v2 import run_sql_layer_v2
from .synthetic_generator.pipeline import generate_synthetic_ecosystem
from .validate_data_v2 import run_validate_data_v2
from .visualization_v2 import run_visualization_v2


def _run_pytest(paths) -> str:
    cmd = [str(paths.root / ".venv" / "bin" / "pytest"), "-q"]
    proc = subprocess.run(cmd, cwd=str(paths.root), capture_output=True, text=True, check=False)
    return (proc.stdout + "\n" + proc.stderr).strip()


def run_final_assembly_v2() -> dict[str, str]:
    paths = ensure_dirs(get_paths())

    # 0) Regeneración determinista del ecosistema raw para evitar drift de artefactos.
    generate_synthetic_ecosystem()

    run_sql_layer_v2()
    build_features_v2(force_sql_refresh=False)
    run_forecasting_v2()
    run_anomaly_detection_v2()
    run_scoring_v2()
    run_scenario_engine_v2()
    run_advanced_analysis_v2()
    run_visualization_v2()
    dashboard_path = build_dashboard_v2()
    run_validate_data_v2()
    release_manifest = build_release_manifest_v2()
    smoke = run_smoke_checks_v2()
    docs_out = build_final_docs_v2()

    pytest_out = _run_pytest(paths)

    # Resumen final solicitado.
    processed_files = sorted([p.name for p in (paths.data_processed).glob("*")])
    chart_files = sorted([p.name for p in (paths.outputs_charts).glob("*.png")])
    report_files = sorted([p.name for p in (paths.outputs_reports).glob("*")])

    scoring = pd.read_csv(paths.data_processed / "intervention_ranking_final.csv")
    top5 = scoring.head(5)

    issues = pd.read_csv(paths.outputs_reports / "issues_found.csv") if (paths.outputs_reports / "issues_found.csv").exists() else pd.DataFrame()
    validation_summary = (
        pd.read_json(paths.outputs_reports / "validation_summary.json", typ="series").to_dict()
        if (paths.outputs_reports / "validation_summary.json").exists()
        else {}
    )
    n_high = int((issues["severity"] == "alta").sum()) if "severity" in issues.columns else 0
    n_med = int((issues["severity"] == "media").sum()) if "severity" in issues.columns else 0

    processed_table = pd.DataFrame({"file": processed_files}).to_markdown(index=False)
    top5_table = top5.to_markdown(index=False)

    summary = "\n".join(
        [
            "# Ensamblado Final del Proyecto (v2)",
            "",
            "## 1) Estructura final del repositorio",
            "- data/raw",
            "- data/processed",
            "- sql",
            "- src",
            "- notebooks",
            "- outputs/charts",
            "- outputs/dashboard",
            "- outputs/reports",
            "- docs",
            "",
            "## 2) Lista de archivos creados/relevantes",
            "- Módulos v2 en src: sql_runner_v2, feature_engineering_v2, forecasting_v2, anomaly_detection_v2, scoring_v2, scenario_engine_v2, analysis_v2, visualization_v2, dashboard_v2, validate_data_v2, final_docs_v2, final_assembly_v2.",
            "- SQL multicapa: 01 a 10.",
            "- Documentación: feature_dictionary, sql_architecture, sql_metric_definitions, scoring_framework, dashboard_architecture.",
            "",
            "## 3) Scripts ejecutados",
            "- generate_synthetic_ecosystem",
            "- run_sql_layer_v2",
            "- build_features_v2",
            "- run_forecasting_v2",
            "- run_anomaly_detection_v2",
            "- run_scoring_v2",
            "- run_scenario_engine_v2",
            "- run_advanced_analysis_v2",
            "- run_visualization_v2",
            "- build_dashboard_v2",
            "- run_validate_data_v2",
            "- build_release_manifest_v2",
            "- build_final_docs_v2",
            "",
            "## 4) Datos generados (processed)",
            processed_table,
            "",
            "## 5) Tablas analíticas creadas",
            "- mart_node_hour_operational_state",
            "- mart_zone_day_operational",
            "- mart_zone_month_operational",
            "- node_hour_features",
            "- zone_day_features",
            "- zone_month_features",
            "- intervention_candidates_features",
            "- intervention_scoring_table",
            "- scenario_impacts_v2",
            "",
            "## 6) Outputs generados",
            f"- Charts: {len(chart_files)} archivos PNG.",
            f"- Reports: {len(report_files)} archivos en outputs/reports.",
            "",
            "## 7) Dashboard HTML final",
            f"- {dashboard_path}",
            "",
            "## 8) Resumen ejecutivo final",
            "- Se consolidó un sistema de decisión para red que integra riesgo técnico, resiliencia, presión de electrificación y criterios económicos.",
            "",
            "## 9) Hallazgos principales",
            top5_table,
            "",
            "## 10) Recomendaciones",
            "- Ejecutar intervención inmediata en zonas top con tier crítico.",
            "- Priorizar flexibilidad/operación en zonas con CAPEX diferible.",
            "- Programar refuerzo estructural en zonas con presión persistente y baja cobertura flexible.",
            "",
            "## 11) Resumen de validación",
            f"- Issues alta severidad: {n_high}",
            f"- Issues media severidad: {n_med}",
            f"- Estado global validación: {validation_summary.get('overall_status', 'N/A')}",
            f"- Nivel de confianza: {validation_summary.get('confidence_level', 'N/A')}",
            "- Ver detalle en outputs/reports/validation_report.md",
            "",
            "## 12) Limitaciones",
            "- Datos sintéticos y proxies económicos; requiere calibración real.",
            "",
            "## 13) Próximos pasos",
            "- Calibración con datos operativos reales.",
            "- Integración en ciclo de planificación trimestral.",
            "- Endpoints para refresh y gobierno de métricas.",
            "",
            "## 14) Sugerencias exactas para publicarlo en GitHub",
            "1. Crear repo con nombre: sistema-inteligencia-red-electrificacion.",
            "2. Subir estructura completa manteniendo outputs clave (charts, dashboard, reports).",
            "3. Añadir release v1.0 con dashboard HTML y memo ejecutivo como assets.",
            "4. Incluir capturas del dashboard en README.",
            "5. Añadir sección de reproducibilidad con comando: python -m src.",
            "",
            "## Consistencia global y naming",
            "- Naming unificado en español por dominio analítico.",
            "- Capa v2 desacoplada del pipeline legacy para evitar regresiones.",
            "",
            "## Resultado tests",
            "```",
            pytest_out,
            "```",
            "",
            "## Smoke checks",
            f"- status: {smoke.get('status', 'unknown')}",
            f"- validation_status: {smoke.get('validation_status', 'unknown')}",
            f"- publish_state: {smoke.get('publish_state', 'unknown')}",
            f"- warnings: {smoke.get('warnings', '') or 'none'}",
            f"- errors: {smoke.get('errors', '') or 'none'}",
            "",
            "## Release manifest",
            f"- manifest_version: {release_manifest.get('manifest_version', 'N/A')}",
            f"- top_zone_by_priority: {release_manifest.get('top_zone_by_priority', 'N/A')}",
            f"- dashboard_sha256: {release_manifest.get('artifacts', {}).get('dashboard', {}).get('sha256', 'N/A')}",
            "",
        ]
    )

    summary_path = paths.outputs_reports / "final_assembly_summary.md"
    summary_path.write_text(summary, encoding="utf-8")

    return {
        "dashboard": dashboard_path,
        "project_snapshot": docs_out.get("project_snapshot", "N/A"),
        "final_summary": str(summary_path),
    }


if __name__ == "__main__":
    result = run_final_assembly_v2()
    for k, v in result.items():
        print(k, v)
