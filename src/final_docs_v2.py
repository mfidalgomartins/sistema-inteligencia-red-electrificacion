from __future__ import annotations

import json
from datetime import datetime, timezone
from textwrap import dedent

import pandas as pd

from .common_v2 import ensure_dirs, get_paths


def _safe_read_csv(path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _safe_read_json(path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_final_docs_v2() -> dict[str, str]:
    """
    Genera solo snapshot técnico de release.
    No reescribe README ni memo ejecutivo para preservar edición humana.
    """
    paths = ensure_dirs(get_paths())

    scoring = _safe_read_csv(paths.data_processed / "intervention_scoring_table.csv")
    zone_risk = _safe_read_csv(paths.data_processed / "vw_zone_operational_risk.csv")
    scenario_summary = _safe_read_csv(paths.data_processed / "scenario_summary_v2.csv")
    validation_summary = _safe_read_json(paths.outputs_reports / "validation_summary.json")
    manifest = _safe_read_json(paths.outputs_reports / "release_manifest.json")

    n_zonas = int(zone_risk["zona_id"].nunique()) if "zona_id" in zone_risk.columns else 0
    horas_cong = float(zone_risk["horas_congestion"].sum()) if "horas_congestion" in zone_risk.columns else 0.0
    ens_total = float(zone_risk["ens_total_mwh"].sum()) if "ens_total_mwh" in zone_risk.columns else 0.0
    top_zone = (
        scoring.sort_values("investment_priority_score", ascending=False).iloc[0]["zona_id"]
        if not scoring.empty and "investment_priority_score" in scoring.columns and "zona_id" in scoring.columns
        else "N/A"
    )
    top_scenario = (
        scenario_summary.sort_values("coste_riesgo_total", ascending=True).iloc[0]["scenario"]
        if not scenario_summary.empty and "coste_riesgo_total" in scenario_summary.columns and "scenario" in scenario_summary.columns
        else "N/A"
    )

    snapshot = dedent(
        f"""
        # Snapshot de Release (v2)

        Fecha UTC: {datetime.now(timezone.utc).isoformat()}

        ## Estado
        - validation_status: {validation_summary.get('overall_status', 'N/A')}
        - confidence_level: {validation_summary.get('confidence_level', 'N/A')}
        - publish_state: {validation_summary.get('release_readiness', {}).get('publish_state', 'N/A')}
        - decision_state: {validation_summary.get('release_readiness', {}).get('decision_state', 'N/A')}

        ## Señales clave
        - zonas_analizadas: {n_zonas}
        - horas_congestion_total: {horas_cong:,.0f}
        - ens_total_mwh: {ens_total:,.2f}
        - top_zone_priority: {top_zone}
        - escenario_menor_coste_riesgo: {top_scenario}

        ## Integridad de artefactos
        - dashboard_sha256: {manifest.get('artifacts', {}).get('dashboard', {}).get('sha256', 'N/A')}
        - scoring_rows: {manifest.get('n_scoring_rows', 'N/A')}
        """
    ).strip() + "\n"

    out_path = paths.outputs_reports / "project_snapshot.md"
    out_path.write_text(snapshot, encoding="utf-8")
    return {"project_snapshot": str(out_path)}


if __name__ == "__main__":
    result = build_final_docs_v2()
    for k, v in result.items():
        print(f"{k}: {v}")
