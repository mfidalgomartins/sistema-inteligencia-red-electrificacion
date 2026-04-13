from __future__ import annotations

import subprocess

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

    return {
        "dashboard": dashboard_path,
        "release_brief": docs_out.get("release_brief", "N/A"),
        "pytest_output": pytest_out,
        "smoke_status": str(smoke.get("status", "unknown")),
        "publish_state": str(release_manifest.get("release_readiness", {}).get("publish_state", "N/A")),
    }


if __name__ == "__main__":
    result = run_final_assembly_v2()
    for k, v in result.items():
        print(k, v)
