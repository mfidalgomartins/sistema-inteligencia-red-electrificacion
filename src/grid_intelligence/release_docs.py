"""Documentos finales de publicación: brief de release y resúmenes técnicos."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from textwrap import dedent

import pandas as pd

from .common import ensure_dirs, get_paths
from .ingestion.contracts import load_contract_catalog

PUBLICATION_LABELS = {
    "decision-support ready": "apto como soporte de decisión",
    "decision-support only": "solo soporte de decisión",
    "screening-grade only": "solo cribado preliminar",
    "publish-ready": "publicable",
    "publish-with-caveats": "publicable con matices",
    "publish-blocked": "publicación bloqueada",
    "capex_mas_flexibilidad": "CAPEX y flexibilidad",
    "despliegue_adicional_storage": "almacenamiento adicional",
    "despliegue_adicional_flexibilidad": "flexibilidad adicional",
    "retraso_capex": "retraso de CAPEX",
}


def _publication_label(value: object) -> str:
    text = str(value)
    return PUBLICATION_LABELS.get(text, text.replace("_", " "))


def _read_csv(path: Path) -> pd.DataFrame:
    """Lee un artefacto requerido y conserva el error original si está corrupto."""
    if not path.exists():
        raise FileNotFoundError(f"Falta el artefacto requerido: {path}")
    return pd.read_csv(path)


def _read_json(path: Path) -> dict:
    """Lee un JSON requerido con validación de tipo raíz."""
    if not path.exists():
        raise FileNotFoundError(f"Falta el artefacto requerido: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"El JSON debe contener un objeto en la raíz: {path}")
    return payload


def _sha256(path: Path) -> str:
    if not path.exists():
        return "N/A"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fmt_es(value: float, decimals: int = 0) -> str:
    return f"{value:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def build_release_docs() -> dict[str, str]:
    """
    Genera solo instantánea técnica de publicación.
    No reescribe README ni memo ejecutivo para preservar edición humana.
    """
    paths = ensure_dirs(get_paths())

    scoring = _read_csv(paths.data_processed / "intervention_scoring_table.csv")
    zone_risk = _read_csv(paths.data_processed / "vw_zone_operational_risk.csv")
    scenario_summary = _read_csv(paths.data_processed / "scenario_summary.csv")
    calibration = _read_csv(paths.data_processed / "forecast_calibration_summary.csv")
    monitoring = _read_csv(paths.data_processed / "forecast_monitoring_status.csv")
    validation_summary = _read_json(paths.outputs_reports / "validation_summary.json")

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
        if not scenario_summary.empty
        and "coste_riesgo_total" in scenario_summary.columns
        and "scenario" in scenario_summary.columns
        else "N/A"
    )
    monitoring_row = monitoring.iloc[0] if not monitoring.empty else pd.Series(dtype=object)
    coverage_95_rows = calibration[calibration["interval_level"].round(2) == 0.95]
    coverage_95 = float(coverage_95_rows.iloc[0]["empirical_coverage"]) if not coverage_95_rows.empty else float("nan")
    contract_count = len(load_contract_catalog().contracts)

    brief = (
        dedent(
            f"""
        # Resumen de publicación

        ## Estado de publicación
        - Validación: {validation_summary.get("overall_status", "N/A")}
        - Confianza: {validation_summary.get("confidence_level", "N/A")}
        - Modo de fuente: {validation_summary.get("source_mode", "N/A")}
        - Publicación: {_publication_label(validation_summary.get("release_readiness", {}).get("publish_state", "N/A"))}
        - Estado de decisión: {_publication_label(validation_summary.get("release_readiness", {}).get("decision_state", "N/A"))}

        ## Señales clave
        - Zonas analizadas: {n_zonas}
        - Horas de congestión acumuladas: {_fmt_es(horas_cong)}
        - ENS total (MWh): {_fmt_es(ens_total, 2)}
        - Zona con mayor prioridad: {top_zone}
        - Escenario con menor coste de riesgo: {_publication_label(top_scenario)}

        ## Operación y monitorización
        - Contratos de fuentes gobernados: {contract_count}
        - Modelo de pronóstico monitorizado: {monitoring_row.get("best_model", "N/A")}
        - Estado del pronóstico: {monitoring_row.get("monitoring_status", "N/A")}
        - Deriva MAE: {_fmt_es(float(monitoring_row.get("mae_drift_ratio", float("nan"))), 3)}x
        - Cobertura empírica 95%: {_fmt_es(coverage_95 * 100, 1)}%
        - API analítica: `/v1/zones`, `/v1/feeders`, `/v1/marts/*`, `/v1/decisions`

        ## Integridad de artefactos
        - sha256_tablero: {_sha256(paths.outputs_dashboard / "grid-electrification-command-center.html")}
        - filas_puntuación: {len(scoring)}
        """
        ).strip()
        + "\n"
    )

    out_path = paths.outputs_reports / "release_brief.md"
    out_path.write_text(brief, encoding="utf-8")
    return {"release_brief": str(out_path)}


if __name__ == "__main__":
    result = build_release_docs()
    for k, v in result.items():
        print(f"{k}: {v}")
