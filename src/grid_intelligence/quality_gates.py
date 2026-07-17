"""Pruebas de humo de release: artefactos, estados y umbrales mínimos."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pandas as pd

from .common import ensure_dirs, get_paths
from .forecast_validation import validate_forecast_artifacts
from .ingestion.contracts import load_contract_catalog

REQUIRED_PROCESSED = [
    "intervention_scoring_table.csv",
    "prioridades_inversion_alimentadores.csv",
    "scenario_summary.csv",
    "forecast_model_benchmark.csv",
    "forecast_calibration_summary.csv",
    "forecast_monitoring_status.csv",
    "anomalies_detected.csv",
]

REQUIRED_REPORTS = [
    "validation_report.md",
    "validation_summary.json",
    "release_manifest.json",
    "release_brief.md",
]

ALLOWED_OVERALL = {"PASS", "WARN", "FAIL"}
ALLOWED_PUBLISH = {"publish-ready", "publish-with-caveats", "publish-blocked"}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json_object(path: Path, artifact: str, errors: list[str]) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        errors.append(f"invalid_json:{artifact}")
        return {}
    if not isinstance(payload, dict):
        errors.append(f"invalid_json_root:{artifact}")
        return {}
    return payload


def _read_csv(path: Path, artifact: str, errors: list[str], *, nrows: int | None = None) -> pd.DataFrame:
    try:
        return pd.read_csv(path, nrows=nrows)
    except (OSError, UnicodeError, pd.errors.ParserError):
        errors.append(f"invalid_csv:{artifact}")
        return pd.DataFrame()


def run_quality_gates() -> dict[str, str]:
    paths = ensure_dirs(get_paths())
    errors: list[str] = []
    warnings: list[str] = []

    for file_name in REQUIRED_PROCESSED:
        p = paths.data_processed / file_name
        if not p.exists():
            errors.append(f"missing_processed:{file_name}")
            continue
        df = _read_csv(p, file_name, errors)
        if df.empty:
            errors.append(f"empty_processed:{file_name}")

    for file_name in REQUIRED_REPORTS:
        p = paths.outputs_reports / file_name
        if not p.exists():
            errors.append(f"missing_report:{file_name}")

    try:
        source_catalog = load_contract_catalog()
        if len(source_catalog.contracts) != 15:
            errors.append(f"invalid_source_contract_count:{len(source_catalog.contracts)}")
        target_tables = [contract.target_table for contract in source_catalog.contracts.values()]
        if len(target_tables) != len(set(target_tables)):
            errors.append("duplicate_source_contract_target")
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
        errors.append("invalid_source_contract_catalog")
        target_tables = []

    for file_name in [f"{target}.csv" for target in target_tables]:
        p = paths.data_raw / file_name
        if not p.exists():
            errors.append(f"missing_raw:{file_name}")
            continue
        df = _read_csv(p, file_name, errors, nrows=5)
        if df.empty:
            errors.append(f"empty_raw:{file_name}")

    for forecast_error in validate_forecast_artifacts(paths):
        errors.append(f"forecast_governance:{forecast_error}")

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
        summary = _read_json_object(summary_path, "validation_summary.json", errors)
        if summary.get("overall_status") not in ALLOWED_OVERALL:
            errors.append("invalid_validation_status")
        publish_state = (
            summary.get("release_readiness", {}).get("publish_state")
            if isinstance(summary.get("release_readiness"), dict)
            else None
        )
        if publish_state not in ALLOWED_PUBLISH:
            errors.append("invalid_publish_state")
        if publish_state == "publish-blocked":
            errors.append("release_publish_blocked")
    else:
        summary = {}

    manifest_path = paths.outputs_reports / "release_manifest.json"
    if manifest_path.exists():
        manifest = _read_json_object(manifest_path, "release_manifest.json", errors)
        if manifest.get("validation_status") != summary.get("overall_status"):
            errors.append("manifest_validation_status_mismatch")
        if manifest.get("release_readiness", {}).get("publish_state") != summary.get("release_readiness", {}).get(
            "publish_state"
        ):
            errors.append("manifest_publish_state_mismatch")
        for artifact_name, metadata in manifest.get("artifacts", {}).items():
            if not metadata.get("exists"):
                errors.append(f"manifest_missing_artifact:{artifact_name}")
                continue
            artifact_path = paths.root / metadata.get("path", "")
            if not artifact_path.exists():
                errors.append(f"manifest_path_missing:{artifact_name}")
            elif metadata.get("sha256") != _sha256(artifact_path):
                errors.append(f"manifest_hash_mismatch:{artifact_name}")
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
    out = run_quality_gates()
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
