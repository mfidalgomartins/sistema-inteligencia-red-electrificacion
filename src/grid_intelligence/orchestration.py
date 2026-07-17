"""Orquestación observable del pipeline analítico canónico."""

from __future__ import annotations

import logging
from collections.abc import Callable
from time import perf_counter
from typing import Literal

from .analysis import run_advanced_analysis
from .anomaly_detection import run_anomaly_detection
from .common import ensure_dirs, get_paths
from .dashboard import build_dashboard
from .feature_engineering import build_features
from .feeder_prioritization import run_feeder_prioritization
from .forecasting import run_forecasting
from .ingestion import IngestionService
from .quality_gates import run_quality_gates
from .release_docs import build_release_docs
from .release_manifest import build_release_manifest
from .scenario_engine import run_scenario_engine
from .scoring import run_scoring
from .sql_runner import run_sql_layer
from .synthetic_generator.pipeline import generate_synthetic_ecosystem
from .validation import run_validation
from .visualization import run_visualization

LOGGER = logging.getLogger(__name__)


def _run_stage[T](name: str, action: Callable[[], T]) -> T:
    started = perf_counter()
    LOGGER.info("stage_started stage=%s", name)
    try:
        result = action()
    except Exception:
        LOGGER.exception("stage_failed stage=%s", name)
        raise
    LOGGER.info("stage_completed stage=%s duration_seconds=%.2f", name, perf_counter() - started)
    return result


def run_release_pipeline(source_mode: Literal["synthetic", "external"] = "synthetic") -> dict[str, str]:
    """Regenera datos, modelos, validaciones y artefactos analíticos."""
    paths = ensure_dirs(get_paths())

    if source_mode == "synthetic":
        _run_stage("synthetic_data", generate_synthetic_ecosystem)
    elif source_mode == "external":
        _run_stage("external_contracts", lambda: IngestionService(paths).validate_external_raw_inputs())
    else:
        raise ValueError(f"source_mode no soportado: {source_mode}")
    _run_stage("sql_layer", run_sql_layer)
    _run_stage("feature_engineering", lambda: build_features(force_sql_refresh=False))
    _run_stage("forecasting", run_forecasting)
    _run_stage("anomaly_detection", run_anomaly_detection)
    _run_stage("zone_scoring", run_scoring)
    _run_stage("feeder_prioritization", run_feeder_prioritization)
    _run_stage("scenario_engine", run_scenario_engine)
    _run_stage("advanced_analysis", run_advanced_analysis)
    _run_stage("visualization", run_visualization)
    dashboard_path = _run_stage("dashboard", build_dashboard)
    _run_stage("validation", lambda: run_validation(source_mode=source_mode))
    docs_out = _run_stage("release_docs", build_release_docs)
    release_manifest = _run_stage("release_manifest", build_release_manifest)
    smoke = _run_stage("quality_gates", run_quality_gates)
    if smoke.get("status") != "ok":
        raise RuntimeError(f"Pruebas de humo fallidas: {smoke.get('errors', 'error desconocido')}")

    return {
        "source_mode": source_mode,
        "dashboard": dashboard_path,
        "release_brief": docs_out.get("release_brief", "N/A"),
        "smoke_status": str(smoke.get("status", "unknown")),
        "publish_state": str(release_manifest.get("release_readiness", {}).get("publish_state", "N/A")),
    }


if __name__ == "__main__":
    result = run_release_pipeline()
    for k, v in result.items():
        print(k, v)
