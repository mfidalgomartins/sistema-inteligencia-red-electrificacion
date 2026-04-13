from __future__ import annotations

import json
from pathlib import Path
import re

import pandas as pd

from .common_v2 import ensure_dirs, get_paths


REQUIRED_PROCESSED = [
    "intervention_scoring_table.csv",
    "scenario_summary_v2.csv",
    "forecast_model_benchmark.csv",
    "anomalies_detected.csv",
]

REQUIRED_REPORTS = [
    "validation_report.md",
    "validation_summary.json",
    "release_manifest.json",
    "release_brief.md",
]

REQUIRED_RAW = [
    "zonas_red.csv",
    "subestaciones.csv",
    "alimentadores.csv",
    "demanda_horaria.csv",
    "eventos_congestion.csv",
    "interrupciones_servicio.csv",
    "activos_red.csv",
]

ALLOWED_OVERALL = {"PASS", "WARN", "FAIL"}
ALLOWED_PUBLISH = {"publish-ready", "publish-with-caveats", "publish-blocked"}


def run_smoke_checks_v2() -> dict[str, str]:
    paths = ensure_dirs(get_paths())
    errors: list[str] = []
    warnings: list[str] = []

    for file_name in REQUIRED_PROCESSED:
        p = paths.data_processed / file_name
        if not p.exists():
            errors.append(f"missing_processed:{file_name}")
            continue
        df = pd.read_csv(p)
        if df.empty:
            errors.append(f"empty_processed:{file_name}")

    for file_name in REQUIRED_REPORTS:
        p = paths.outputs_reports / file_name
        if not p.exists():
            errors.append(f"missing_report:{file_name}")

    for file_name in REQUIRED_RAW:
        p = paths.data_raw / file_name
        if not p.exists():
            errors.append(f"missing_raw:{file_name}")
            continue
        df = pd.read_csv(p, nrows=5)
        if df.empty:
            errors.append(f"empty_raw:{file_name}")

    dashboard_official = paths.outputs_dashboard / "grid-electrification-command-center.html"
    dashboard_legacy = paths.outputs_dashboard / "dashboard_inteligencia_red_premium.html"
    if not dashboard_official.exists():
        errors.append("missing_dashboard_official")
    if dashboard_legacy.exists():
        warnings.append("legacy_dashboard_duplicate_present")
    if dashboard_official.exists():
        html = dashboard_official.read_text(encoding="utf-8")
        unresolved = re.findall(r"__[A-Z0-9_]+__", html)
        if unresolved:
            errors.append(f"dashboard_unresolved_placeholders:{','.join(sorted(set(unresolved)))}")

    summary_path = paths.outputs_reports / "validation_summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if summary.get("overall_status") not in ALLOWED_OVERALL:
            errors.append("invalid_validation_status")
        publish_state = (
            summary.get("release_readiness", {}).get("publish_state")
            if isinstance(summary.get("release_readiness"), dict)
            else None
        )
        if publish_state not in ALLOWED_PUBLISH:
            errors.append("invalid_publish_state")
    else:
        summary = {}

    manifest_path = paths.outputs_reports / "release_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("validation_status") != summary.get("overall_status"):
            errors.append("manifest_validation_status_mismatch")
        if manifest.get("release_readiness", {}).get("publish_state") != summary.get("release_readiness", {}).get("publish_state"):
            errors.append("manifest_publish_state_mismatch")
    else:
        manifest = {}

    status = "ok" if not errors else "error"
    out = {
        "status": status,
        "errors": ";".join(errors) if errors else "",
        "warnings": ";".join(warnings) if warnings else "",
        "validation_status": str(summary.get("overall_status", "N/A")),
        "publish_state": str(summary.get("release_readiness", {}).get("publish_state", "N/A")),
    }
    return out


def main() -> None:
    out = run_smoke_checks_v2()
    print(f"status: {out['status']}")
    print(f"validation_status: {out['validation_status']}")
    print(f"publish_state: {out['publish_state']}")
    if out["warnings"]:
        print(f"warnings: {out['warnings']}")
    if out["errors"]:
        print(f"errors: {out['errors']}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
