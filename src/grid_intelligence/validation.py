"""Validación analítica formal: controles de datos, reglas y preparación de release."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Literal

import pandas as pd

from .common import connect_database, ensure_dirs, get_paths, project_relative
from .forecast_validation import validate_forecast_artifacts
from .ingestion.contracts import load_contract_catalog

RELEASE_LABELS = {
    "not technically valid": "no válido técnicamente",
    "technically valid": "válido técnicamente",
    "not analytically acceptable": "no aceptable analíticamente",
    "analytically acceptable": "aceptable analíticamente",
    "screening-grade only": "solo cribado preliminar",
    "decision-support only": "solo soporte de decisión",
    "decision-support ready": "apto como soporte de decisión",
    "committee-grade": "apto para comité",
    "not committee-grade": "no apto para comité",
    "publish-ready": "publicable",
    "publish-with-caveats": "publicable con matices",
    "publish-blocked": "publicación bloqueada",
}

COUNT_LABELS = {
    "n_zonas": "zonas",
    "n_subestaciones": "subestaciones",
    "n_alimentadores": "alimentadores",
    "n_demanda_horaria": "registros de demanda horaria",
    "n_node_features": "variables nodo-hora",
    "n_zone_day_features": "variables zona-día",
    "n_zone_month_features": "variables zona-mes",
    "n_scoring": "filas de puntuación",
}

CHECKLIST_LABELS = {
    "generacion_datos": "generación de datos",
    "relaciones_tablas": "relaciones entre tablas",
    "perfilado": "perfilado",
    "sql": "SQL",
    "variables_analiticas": "variables analíticas",
    "pronostico": "pronóstico",
    "deteccion_anomalias": "detección de anomalías",
    "puntuacion": "puntuación",
    "motor_escenarios": "motor de escenarios",
    "impacto_economico": "impacto económico",
    "visualizaciones": "visualizaciones",
    "tablero": "tablero",
    "narrativa_final": "narrativa final",
}

STATUS_LABELS = {
    "ok": "correcto",
    "warning": "aviso",
}

AREA_LABELS = {
    "generacion": "generación",
    "profiling": "perfilado",
    "data": "datos",
    "metricas": "métricas",
    "puntuacion": "puntuación",
    "economico": "económico",
    "decision": "decisión",
    "temporal": "temporal",
    "consistencia": "consistencia",
    "sql": "SQL",
    "global": "global",
    "tablero": "tablero",
}

CONTROL_LABELS = {
    "sin_incidencias_criticas": "sin incidencias críticas",
}

GATE_LABELS = {
    "tablero_oficial_existe": "tablero oficial existe",
    "tablero_oficial_unico": "tablero oficial único",
    "archivos_puntuacion_existen": "archivos de puntuación existen",
    "archivos_escenario_existen": "archivos de escenario existen",
    "archivos_anomalias_existen": "archivos de anomalías existen",
    "comparativa_pronostico_existe": "comparativa de pronóstico existe",
    "sensibilidad_existe": "sensibilidad existe",
    "ranking_coincide_lider_puntuacion": "clasificación coincide con líder de puntuación",
    "consistencia_coste_escenario": "consistencia de coste por escenario",
    "rankings_escenario_distintos": "clasificaciones de escenario distintas",
    "consistencia_resumen_anomalias": "consistencia del resumen de anomalías",
    "estabilidad_puntuacion_cambio_ranking": "estabilidad de puntuación ante cambios de clasificación",
    "tareas_pronostico_cubiertas": "tareas de pronóstico cubiertas",
    "gobierno_pronostico_continuo": "backtesting e incertidumbre validados",
    "contratos_fuente_gobernados": "contratos de fuentes gobernados",
}


def _release_label(value: object) -> str:
    text = str(value)
    return RELEASE_LABELS.get(text, text)


def _display_token(value: object) -> str:
    return str(value).replace("_", " ")


def compute_validation_assessment(issues_df: pd.DataFrame) -> dict[str, int | str]:
    """Calcula estado global de calidad con reglas conservadoras."""
    if issues_df.empty:
        return {
            "n_high": 0,
            "n_med": 0,
            "n_low": 0,
            "overall_status": "PASS",
            "confidence_level": "alta",
        }

    n_high = int((issues_df["severity"] == "alta").sum())
    n_med = int((issues_df["severity"] == "media").sum())
    n_low = int((issues_df["severity"] == "baja").sum())

    if n_high > 0:
        return {
            "n_high": n_high,
            "n_med": n_med,
            "n_low": n_low,
            "overall_status": "FAIL",
            "confidence_level": "baja",
        }
    if n_med > 0:
        return {
            "n_high": n_high,
            "n_med": n_med,
            "n_low": n_low,
            "overall_status": "WARN",
            "confidence_level": "media",
        }
    return {
        "n_high": n_high,
        "n_med": n_med,
        "n_low": n_low,
        "overall_status": "PASS",
        "confidence_level": "alta",
    }


def classify_release_readiness(
    assessment: dict[str, int | str],
    gate_checks: pd.DataFrame,
) -> dict[str, str]:
    """Clasifica preparación técnica/analítica/ejecutiva con reglas explícitas."""
    n_high = int(assessment.get("n_high", 0))
    n_med = int(assessment.get("n_med", 0))
    blocker_col = "bloqueante" if "bloqueante" in gate_checks.columns else "is_blocker"
    passed_col = "superado" if "superado" in gate_checks.columns else "passed"
    blocked = bool((gate_checks[blocker_col] & (~gate_checks[passed_col])).any()) if not gate_checks.empty else False

    if n_high > 0 or blocked:
        return {
            "technical_state": "not technically valid",
            "analytical_state": "not analytically acceptable",
            "decision_state": "screening-grade only",
            "committee_state": "not committee-grade",
            "publish_state": "publish-blocked",
        }
    if n_med >= 2:
        return {
            "technical_state": "technically valid",
            "analytical_state": "analytically acceptable",
            "decision_state": "screening-grade only",
            "committee_state": "not committee-grade",
            "publish_state": "publish-blocked",
        }
    if n_med == 1:
        return {
            "technical_state": "technically valid",
            "analytical_state": "analytically acceptable",
            "decision_state": "decision-support only",
            "committee_state": "not committee-grade",
            "publish_state": "publish-with-caveats",
        }
    return {
        "technical_state": "technically valid",
        "analytical_state": "analytically acceptable",
        "decision_state": "decision-support ready",
        "committee_state": "not committee-grade",
        "publish_state": "publish-with-caveats",
    }


def run_validation(source_mode: Literal["synthetic", "external"] = "synthetic") -> dict[str, pd.DataFrame]:
    if source_mode not in {"synthetic", "external"}:
        raise ValueError(f"source_mode no soportado: {source_mode}")
    paths = ensure_dirs(get_paths())
    conn = connect_database(paths)

    issues: list[dict] = []

    def add_issue(area: str, check: str, severity: str, observed: float | str, expected: str, fix: str):
        issues.append(
            {
                "area": area,
                "check": check,
                "severity": severity,
                "observed": observed,
                "expected": expected,
                "fix_applied_or_recommended": fix,
            }
        )

    sql_failures = conn.execute("SELECT * FROM vw_validation_failures").df()
    for row in sql_failures.itertuples(index=False):
        add_issue(
            "sql",
            str(row.check_name),
            str(row.severity),
            float(row.observed_value),
            f"<= {row.threshold_value}",
            str(row.details),
        )

    # 1) Conteos de filas razonables.
    counts = (
        conn.execute(
            """
        SELECT
            (SELECT COUNT(*) FROM stg_zonas_red) AS n_zonas,
            (SELECT COUNT(*) FROM stg_subestaciones) AS n_subestaciones,
            (SELECT COUNT(*) FROM stg_alimentadores) AS n_alimentadores,
            (SELECT COUNT(*) FROM stg_demanda_horaria) AS n_demanda_horaria,
            (SELECT COUNT(*) FROM node_hour_features) AS n_node_features,
            (SELECT COUNT(*) FROM zone_day_features) AS n_zone_day_features,
            (SELECT COUNT(*) FROM zone_month_features) AS n_zone_month_features,
            (SELECT COUNT(*) FROM intervention_scoring_table) AS n_scoring
        """
        )
        .df()
        .iloc[0]
        .to_dict()
    )

    if counts["n_demanda_horaria"] < 1_000_000:
        add_issue(
            "generacion",
            "conteo_demanda_horaria",
            "alta",
            counts["n_demanda_horaria"],
            ">=1,000,000",
            "revisar parámetros del generador",
        )

    # 2) Duplicados inesperados.
    dups = conn.execute(
        """
        SELECT COUNT(*) AS n_dups
        FROM (
            SELECT timestamp, zona_id, subestacion_id, alimentador_id, COUNT(*) AS n
            FROM mart_node_hour_operational_state
            GROUP BY 1,2,3,4
            HAVING COUNT(*) > 1
        ) t
        """
    ).fetchone()[0]
    if dups > 0:
        add_issue("sql", "duplicados_clave_nodo_hora", "alta", dups, "0", "forzar clave primaria lógica en mart")

    # 3) Nulls problemáticos.
    nulls = (
        conn.execute(
            """
        SELECT
            SUM(CASE WHEN zona_id IS NULL THEN 1 ELSE 0 END) AS null_zona,
            SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp,
            SUM(CASE WHEN demanda_mw IS NULL THEN 1 ELSE 0 END) AS null_demanda
        FROM stg_demanda_horaria
        """
        )
        .df()
        .iloc[0]
    )
    for col in ["null_zona", "null_timestamp", "null_demanda"]:
        if int(nulls[col]) > 0:
            add_issue("profiling", f"nulos_{col}", "alta", int(nulls[col]), "0", "aplicar contrato NOT NULL en staging")

    # 4) Valores imposibles.
    neg = (
        conn.execute(
            """
        SELECT
            SUM(CASE WHEN demanda_mw < 0 THEN 1 ELSE 0 END) AS demanda_neg,
            SUM(CASE WHEN capacidad_mw <= 0 THEN 1 ELSE 0 END) AS capacidad_no_positiva
        FROM vw_int_network_load_hour
        """
        )
        .df()
        .iloc[0]
    )
    if int(neg["demanda_neg"]) > 0:
        add_issue(
            "data", "demanda_negativa", "alta", int(neg["demanda_neg"]), "0", "corregir generador y validación previa"
        )
    if int(neg["capacidad_no_positiva"]) > 0:
        add_issue(
            "data",
            "capacidad_no_positiva",
            "alta",
            int(neg["capacidad_no_positiva"]),
            "0",
            "limpiar metadatos de activos",
        )

    # 5) Lógica temporal.
    temporal = (
        conn.execute(
            """
        SELECT
            SUM(CASE WHEN timestamp_fin < timestamp_inicio THEN 1 ELSE 0 END) AS eventos_bad,
            (SELECT SUM(CASE WHEN timestamp_fin < timestamp_inicio THEN 1 ELSE 0 END) FROM stg_interrupciones_servicio) AS interrupciones_bad
        FROM stg_eventos_congestion
        """
        )
        .df()
        .iloc[0]
    )
    if int(temporal["eventos_bad"]) > 0 or int(temporal["interrupciones_bad"]) > 0:
        add_issue(
            "temporal",
            "timestamp_inicio_fin_invalido",
            "alta",
            int(temporal["eventos_bad"] + temporal["interrupciones_bad"]),
            "0",
            "normalizar eventos con inicio<=fin",
        )

    # 6) Consistencia demanda-capacidad.
    overload = conn.execute(
        """
        SELECT
            AVG(CASE WHEN carga_relativa > 1.0 THEN 1 ELSE 0 END) AS pct_overload
        FROM mart_node_hour_operational_state
        """
    ).fetchone()[0]
    if float(overload) > 0.45:
        add_issue(
            "consistencia",
            "sobrecarga_excesiva",
            "media",
            round(float(overload), 4),
            "<=0.45",
            "revisar calibración de capacidad/demanda",
        )

    # 7) Consistencia congestión vs interrupciones.
    cong_vs_int = conn.execute(
        """
        SELECT COUNT(*)
        FROM vw_int_service_quality_enriched
        WHERE relacion_congestion_flag = TRUE AND congestion_overlap_flag = FALSE
        """
    ).fetchone()[0]
    if int(cong_vs_int) > 0:
        add_issue(
            "consistencia",
            "interrupciones_congestion_sin_solape",
            "media",
            int(cong_vs_int),
            "0",
            "alinear lógica de bandera y solape temporal",
        )

    # 8) Consistencia GD y vertido.
    gd_curt = conn.execute(
        """
        SELECT COUNT(*)
        FROM stg_generacion_distribuida
        WHERE curtailment_estimado_mw > generacion_mw
        """
    ).fetchone()[0]
    if int(gd_curt) > 0:
        add_issue(
            "consistencia",
            "vertido_mayor_generacion",
            "alta",
            int(gd_curt),
            "0",
            "limitar vertido al máximo de generación",
        )

    # 9) Consistencia de variables analíticas y puntuaciones.
    no_score = conn.execute(
        """
        SELECT COUNT(*)
        FROM zone_month_features z
        LEFT JOIN intervention_scoring_table s ON z.zona_id = s.zona_id
        WHERE s.zona_id IS NULL
        """
    ).fetchone()[0]
    if int(no_score) > 0:
        add_issue(
            "puntuacion", "zonas_sin_puntuacion", "alta", int(no_score), "0", "forzar cobertura completa en puntuación"
        )

    # 10) Coherencia de salidas del tablero.
    dashboard_file = paths.outputs_dashboard / "grid-electrification-command-center.html"
    if not dashboard_file.exists():
        add_issue("tablero", "tablero_faltante", "alta", "sin_archivo", "archivo_existe", "ejecutar build_dashboard")

    # 11) Denominadores correctos (aproximación).
    bad_den = conn.execute(
        """
        SELECT COUNT(*)
        FROM node_hour_features
        WHERE (presion_ev IS NOT NULL AND presion_ev > 5)
           OR (storage_support_ratio IS NOT NULL AND storage_support_ratio > 5)
        """
    ).fetchone()[0]
    if int(bad_den) > 0:
        add_issue("metricas", "ratios_fuera_rango", "media", int(bad_den), "0", "acotar ratios y revisar denominadores")

    # 12) Reglas de puntuaciones y niveles.
    score_bounds = conn.execute(
        """
        SELECT COUNT(*) AS n_bad
        FROM intervention_scoring_table
        WHERE congestion_risk_score NOT BETWEEN 0 AND 100
           OR resilience_risk_score NOT BETWEEN 0 AND 100
           OR service_impact_score NOT BETWEEN 0 AND 100
           OR flexibility_gap_score NOT BETWEEN 0 AND 100
           OR asset_exposure_score NOT BETWEEN 0 AND 100
           OR electrification_pressure_score NOT BETWEEN 0 AND 100
           OR economic_priority_score NOT BETWEEN 0 AND 100
           OR investment_priority_score NOT BETWEEN 0 AND 100
        """
    ).fetchone()[0]
    if int(score_bounds) > 0:
        add_issue(
            "puntuacion",
            "puntuaciones_fuera_rango_0_100",
            "alta",
            int(score_bounds),
            "0",
            "normalizar y acotar puntuaciones",
        )

    asset_score_bounds = conn.execute(
        """
        SELECT COUNT(*)
        FROM vw_assets_exposure
        WHERE exposicion_activo_score NOT BETWEEN 0 AND 100
        """
    ).fetchone()[0]
    if int(asset_score_bounds) > 0:
        add_issue(
            "puntuacion",
            "exposicion_activo_fuera_rango_0_100",
            "alta",
            int(asset_score_bounds),
            "0",
            "normalizar estado_salud antes de calcular exposición",
        )

    invalid_zone_hours = conn.execute(
        """
        SELECT COUNT(*)
        FROM mart_zone_day_operational
        WHERE horas_congestion NOT BETWEEN 0 AND 24
           OR horas_carga_alta NOT BETWEEN 0 AND 24
           OR horas_estres_operativo NOT BETWEEN 0 AND 24
        """
    ).fetchone()[0]
    if int(invalid_zone_hours) > 0:
        add_issue(
            "metricas",
            "horas_zona_dia_fuera_rango",
            "alta",
            int(invalid_zone_hours),
            "0",
            "agregar flags a granularidad zona-hora antes de sumar",
        )

    peak_mismatch = conn.execute(
        """
        SELECT COUNT(*)
        FROM mart_zone_day_operational z
        JOIN (
            SELECT zona_id, fecha, MAX(demanda_total_zona_mw) AS carga_punta_esperada
            FROM mart_node_hour_operational_state
            GROUP BY zona_id, fecha
        ) n USING (zona_id, fecha)
        WHERE ABS(z.carga_punta_mw - n.carga_punta_esperada) > 1e-6
        """
    ).fetchone()[0]
    if int(peak_mismatch) > 0:
        add_issue(
            "metricas",
            "carga_punta_zonal_inconsistente",
            "alta",
            int(peak_mismatch),
            "0",
            "usar pico de demanda agregada da zona",
        )

    tier_mismatch = conn.execute(
        """
        SELECT COUNT(*) AS n_bad
        FROM intervention_scoring_table
        WHERE (investment_priority_score >= 80 AND risk_tier <> 'critico')
           OR (investment_priority_score >= 60 AND investment_priority_score < 80 AND risk_tier <> 'alto')
           OR (investment_priority_score >= 40 AND investment_priority_score < 60 AND risk_tier <> 'medio')
           OR (investment_priority_score < 40 AND risk_tier <> 'bajo')
        """
    ).fetchone()[0]
    if int(tier_mismatch) > 0:
        add_issue(
            "puntuacion",
            "desajuste_nivel_vs_puntuacion",
            "alta",
            int(tier_mismatch),
            "0",
            "alinear reglas de nivel con umbrales oficiales",
        )

    # 13) Lógica financiera/decisión.
    capex_incoherente = conn.execute(
        """
        SELECT COUNT(*)
        FROM kpi_zonas_potencial_capex_diferible
        WHERE capex_diferible_proxy_eur > capex_refuerzo_eur
        """
    ).fetchone()[0]
    if int(capex_incoherente) > 0:
        add_issue(
            "economico",
            "capex_diferible_mayor_refuerzo",
            "media",
            int(capex_incoherente),
            "0",
            "recalibrar proxy de capex diferible",
        )

    decision_critica_sin_accion = conn.execute(
        """
        SELECT COUNT(*)
        FROM intervention_scoring_table
        WHERE risk_tier = 'critico'
          AND recommended_intervention <> 'intervencion_inmediata_prioritaria'
        """
    ).fetchone()[0]
    if int(decision_critica_sin_accion) > 0:
        add_issue(
            "decision",
            "tier_critico_sin_intervencion_inmediata",
            "alta",
            int(decision_critica_sin_accion),
            "0",
            "alinear acción recomendada con tier crítico",
        )

    structural_mismatch = conn.execute(
        """
        SELECT COUNT(*)
        FROM intervention_scoring_table
        WHERE congestion_risk_score >= 80
          AND ratio_flexibilidad_estres < 0.15
          AND recommended_intervention NOT IN ('reforzar_red_local', 'intervencion_inmediata_prioritaria')
        """
    ).fetchone()[0]
    if int(structural_mismatch) > 0:
        add_issue(
            "decision",
            "estres_estructural_sin_refuerzo",
            "alta",
            int(structural_mismatch),
            "0",
            "forzar refuerzo en congestión estructural con baja cobertura flexible",
        )

    asset_mismatch = conn.execute(
        """
        SELECT COUNT(*)
        FROM intervention_scoring_table
        WHERE asset_exposure_score >= 75
          AND recommended_intervention NOT IN ('sustituir_activos', 'intervencion_inmediata_prioritaria')
        """
    ).fetchone()[0]
    if int(asset_mismatch) > 0:
        add_issue(
            "decision",
            "exposicion_activos_sin_sustitucion",
            "alta",
            int(asset_mismatch),
            "0",
            "priorizar sustitución de activos expuestos",
        )

    issues_df = pd.DataFrame(issues)
    if issues_df.empty:
        issues_df = pd.DataFrame(
            [
                {
                    "area": "global",
                    "check": "sin_incidencias_criticas",
                    "severity": "info",
                    "observed": 0,
                    "expected": "0",
                    "fix_applied_or_recommended": "No aplica",
                }
            ]
        )

    severity_order = {"alta": 1, "media": 2, "baja": 3, "info": 4}
    issues_df["severity_rank"] = issues_df["severity"].map(severity_order).fillna(5)
    issues_df = issues_df.sort_values(["severity_rank", "area", "check"]).drop(columns=["severity_rank"])

    controls = [
        "Integridad de claves, relaciones y dominios técnicos.",
        "Reconciliación de agregados entre datos brutos, marts y salidas.",
        "Rangos y reglas de decisión para puntuación.",
        "Consistencia de ranking, escenarios, anomalías, pronóstico y manifiesto de publicación.",
        "Contratos de fuentes gobernados.",
    ]

    caveats = [
        (
            "Los datos de demostración son sintéticos; no sustituyen calibración con telemetría real SCADA/AMI."
            if source_mode == "synthetic"
            else "Los inputs externos cumplen el contrato técnico; su aptitud de ingeniería requiere validación del propietario."
        ),
        "Las aproximaciones económicas no reemplazan valoración regulatoria ni WACC real.",
        "La causalidad entre anomalías, congestión y ENS requiere validación en histórico real.",
        "El tablero usa simplificaciones para garantizar portabilidad HTML única.",
        "Los resultados de pronóstico dependen de estabilidad estructural de patrones de carga.",
    ]

    checklist = pd.DataFrame(
        [
            {"item": "generacion_datos", "status": "ok"},
            {"item": "relaciones_tablas", "status": "ok"},
            {"item": "perfilado", "status": "ok"},
            {"item": "sql", "status": "ok"},
            {"item": "variables_analiticas", "status": "ok"},
            {"item": "pronostico", "status": "ok"},
            {"item": "deteccion_anomalias", "status": "ok"},
            {"item": "puntuacion", "status": "ok"},
            {"item": "motor_escenarios", "status": "ok"},
            {"item": "impacto_economico", "status": "ok"},
            {"item": "visualizaciones", "status": "ok"},
            {"item": "tablero", "status": "ok" if dashboard_file.exists() else "warning"},
            {"item": "narrativa_final", "status": "ok"},
        ]
    )

    assessment = compute_validation_assessment(issues_df)
    n_high = int(assessment["n_high"])
    n_med = int(assessment["n_med"])
    confidence = str(assessment["confidence_level"])
    overall_status = str(assessment["overall_status"])

    # 14) Consistencia entre salidas (fuera de SQL, usando artefactos persistidos).
    gate_rows: list[dict] = []

    def add_gate(name: str, passed: bool, is_blocker: bool, detail: str):
        gate_rows.append(
            {
                "control_puerta": name,
                "superado": bool(passed),
                "bloqueante": bool(is_blocker),
                "detalle": detail,
            }
        )

    score_path = paths.data_processed / "intervention_scoring_table.csv"
    ranking_path = paths.data_processed / "intervention_ranking_final.csv"
    feeder_priority_path = paths.data_processed / "prioridades_inversion_alimentadores.csv"
    scenario_impacts_path = paths.data_processed / "scenario_impacts.csv"
    scenario_summary_path = paths.data_processed / "scenario_summary.csv"
    anomalies_path = paths.data_processed / "anomalies_detected.csv"
    anomalies_summary_path = paths.data_processed / "anomalies_summary_by_type.csv"
    sensitivity_path = paths.data_processed / "scoring_sensitivity_analysis.csv"
    forecast_benchmark_path = paths.data_processed / "forecast_model_benchmark.csv"

    add_gate("tablero_oficial_existe", dashboard_file.exists(), True, project_relative(dashboard_file, paths))
    add_gate(
        "tablero_oficial_unico",
        not (paths.outputs_dashboard / "dashboard_inteligencia_red_premium.html").exists(),
        False,
        "Solo grid-electrification-command-center.html debe ser oficial",
    )
    add_gate(
        "archivos_puntuacion_existen",
        score_path.exists() and ranking_path.exists(),
        True,
        "tabla de puntuación + ranking final",
    )
    add_gate(
        "prioridad_alimentadores_existe",
        feeder_priority_path.exists(),
        True,
        "prioridades_inversion_alimentadores.csv",
    )
    add_gate(
        "archivos_escenario_existen",
        scenario_impacts_path.exists() and scenario_summary_path.exists(),
        True,
        "impactos y resumen de escenarios",
    )
    add_gate(
        "archivos_anomalias_existen",
        anomalies_path.exists() and anomalies_summary_path.exists(),
        False,
        "anomalías detectadas + resumen por tipo",
    )
    add_gate("comparativa_pronostico_existe", forecast_benchmark_path.exists(), False, "forecast_model_benchmark.csv")
    add_gate("sensibilidad_existe", sensitivity_path.exists(), False, "scoring_sensitivity_analysis.csv")

    forecast_errors = validate_forecast_artifacts(paths)
    add_gate(
        "gobierno_pronostico_continuo",
        not forecast_errors,
        True,
        "correcto" if not forecast_errors else "; ".join(forecast_errors),
    )

    try:
        source_catalog = load_contract_catalog()
        source_targets = [contract.target_table for contract in source_catalog.contracts.values()]
        source_contract_errors = []
        if len(source_catalog.contracts) != 15:
            source_contract_errors.append(f"contratos={len(source_catalog.contracts)}")
        if len(source_targets) != len(set(source_targets)):
            source_contract_errors.append("targets_duplicados")
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        source_contract_errors = [f"catálogo_inválido:{exc}"]
    add_gate(
        "contratos_fuente_gobernados",
        not source_contract_errors,
        True,
        "15 contratos válidos" if not source_contract_errors else "; ".join(source_contract_errors),
    )

    if score_path.exists() and ranking_path.exists():
        score_df = pd.read_csv(score_path)
        ranking_df = pd.read_csv(ranking_path)
        if not score_df.empty and not ranking_df.empty:
            top_score = score_df.sort_values("investment_priority_score", ascending=False).iloc[0]["zona_id"]
            top_rank = ranking_df.sort_values("priority_rank").iloc[0]["zona_id"]
            add_gate(
                "ranking_coincide_lider_puntuacion",
                bool(top_score == top_rank),
                True,
                f"zona_puntuación={top_score}, zona_clasificación={top_rank}",
            )

    if scenario_impacts_path.exists() and scenario_summary_path.exists():
        impacts = pd.read_csv(scenario_impacts_path)
        summary = pd.read_csv(scenario_summary_path)
        if not impacts.empty and not summary.empty:
            agg = impacts.groupby("scenario", as_index=False)["coste_riesgo_scenario"].sum()
            merged = summary.merge(agg, on="scenario", how="inner")
            if not merged.empty:
                max_abs_diff = (merged["coste_riesgo_total"] - merged["coste_riesgo_scenario"]).abs().max()
                add_gate(
                    "consistencia_coste_escenario",
                    bool(max_abs_diff <= 1e-6),
                    True,
                    f"diferencia_max_abs={max_abs_diff}",
                )
            ranking_variants = (
                impacts.sort_values(["scenario", "priority_rank_scenario"])
                .groupby("scenario")["zona_id"]
                .apply(tuple)
                .nunique()
            )
            add_gate(
                "rankings_escenario_distintos",
                bool(ranking_variants >= 2),
                True,
                f"clasificaciones_distintas={ranking_variants}",
            )

    if anomalies_path.exists() and anomalies_summary_path.exists():
        anom = pd.read_csv(anomalies_path)
        anom_sum = pd.read_csv(anomalies_summary_path)
        if not anom.empty and not anom_sum.empty:
            cnt = anom.groupby("anomaly_type").size().rename("n_eventos").reset_index()
            chk = anom_sum.merge(cnt, on="anomaly_type", how="left", suffixes=("_summary", "_detected"))
            if not chk.empty and "n_eventos_summary" in chk.columns and "n_eventos_detected" in chk.columns:
                mismatch = (chk["n_eventos_summary"] != chk["n_eventos_detected"]).sum()
                add_gate(
                    "consistencia_resumen_anomalias", bool(mismatch == 0), False, f"filas_desajustadas={int(mismatch)}"
                )

    if sensitivity_path.exists() and score_path.exists():
        sens = pd.read_csv(sensitivity_path)
        score_df = pd.read_csv(score_path)
        if not sens.empty and not score_df.empty:
            piv = sens.pivot_table(index="zona_id", columns="factor", values="rank_alt", aggfunc="mean").reset_index()
            if 0.9 in piv.columns and 1.1 in piv.columns and "priority_rank" in score_df.columns:
                merged = score_df[["zona_id", "priority_rank"]].merge(piv, on="zona_id", how="left")
                merged["dev_09"] = (merged["priority_rank"] - merged[0.9]).abs()
                merged["dev_11"] = (merged["priority_rank"] - merged[1.1]).abs()
                max_dev = float(max(merged["dev_09"].max(), merged["dev_11"].max()))
                add_gate(
                    "estabilidad_puntuacion_cambio_ranking",
                    bool(max_dev <= 3.0),
                    False,
                    f"cambio_máximo_clasificación={max_dev}",
                )

    if forecast_benchmark_path.exists():
        bench = pd.read_csv(forecast_benchmark_path)
        required_tasks = {
            "demanda_zona",
            "demanda_subestacion",
            "carga_relativa_zona",
            "demanda_ev_zona",
            "demanda_industrial_zona",
        }
        present_tasks = set(bench["task"].unique()) if "task" in bench.columns else set()
        add_gate(
            "tareas_pronostico_cubiertas",
            required_tasks.issubset(present_tasks),
            False,
            f"presentes={sorted(present_tasks)}",
        )

    gate_checks = pd.DataFrame(gate_rows)
    if gate_checks.empty:
        gate_checks = pd.DataFrame(columns=["control_puerta", "superado", "bloqueante", "detalle"])

    release = classify_release_readiness(assessment, gate_checks)

    # Endurecer checklist global según severidad consolidada.
    if overall_status != "PASS":
        checklist.loc[checklist["item"].isin(["relaciones_tablas", "narrativa_final"]), "status"] = "warning"
    if overall_status == "FAIL":
        checklist.loc[:, "status"] = checklist["status"].replace({"ok": "warning"})

    counts_display = pd.DataFrame(
        [{"métrica": COUNT_LABELS.get(k, _display_token(k)), "filas": v} for k, v in counts.items()]
    )
    issues_display = issues_df.rename(
        columns={
            "area": "área",
            "check": "control",
            "severity": "severidad",
            "observed": "observado",
            "expected": "esperado",
            "fix_applied_or_recommended": "acción recomendada",
        }
    ).copy()
    issues_display["área"] = issues_display["área"].map(lambda x: AREA_LABELS.get(str(x), _display_token(x)))
    issues_display["control"] = issues_display["control"].map(lambda x: CONTROL_LABELS.get(str(x), _display_token(x)))

    checklist_display = checklist.rename(columns={"item": "elemento", "status": "estado"}).copy()
    checklist_display["elemento"] = checklist_display["elemento"].map(
        lambda x: CHECKLIST_LABELS.get(str(x), _display_token(x))
    )
    checklist_display["estado"] = checklist_display["estado"].map(lambda x: STATUS_LABELS.get(str(x), str(x)))

    gate_checks_display = gate_checks.rename(columns={"control_puerta": "control"}).copy()
    if "control" in gate_checks_display.columns:
        gate_checks_display["control"] = gate_checks_display["control"].map(
            lambda x: GATE_LABELS.get(str(x), _display_token(x))
        )
    for bool_col in ["superado", "bloqueante"]:
        if bool_col in gate_checks_display.columns:
            gate_checks_display[bool_col] = gate_checks_display[bool_col].map(lambda x: "Sí" if bool(x) else "No")

    report = (
        "\n\n".join(
            [
                "# Informe de validación",
                "## Objetivo\nValidar coherencia de extremo a extremo del proyecto: datos, SQL, variables analíticas, pronóstico, anomalías, puntuación, escenarios, visuales y tablero.",
                f"## Conteos de filas clave\n{counts_display.to_markdown(index=False)}",
                f"## Incidencias encontradas\n{issues_display.to_markdown(index=False)}",
                f"## Controles ejecutados\n{pd.DataFrame({'control': controls}).to_markdown(index=False)}",
                f"## Matices obligatorios\n{pd.DataFrame({'matiz': caveats}).to_markdown(index=False)}",
                "\n".join(
                    [
                        "## Evaluación global de confianza",
                        f"- Estado global: {overall_status}",
                        f"- Nivel: {confidence}",
                        f"- Incidencias de severidad alta: {n_high}",
                        f"- Incidencias de severidad media: {n_med}",
                    ]
                ),
                "\n".join(
                    [
                        "## Clasificación de preparación para publicación",
                        f"- Técnica: {_release_label(release['technical_state'])}",
                        f"- Analítica: {_release_label(release['analytical_state'])}",
                        f"- Decisión: {_release_label(release['decision_state'])}",
                        f"- Comité: {_release_label(release['committee_state'])}",
                        f"- Publicación: {_release_label(release['publish_state'])}",
                    ]
                ),
                f"## Checklist final\n{checklist_display.to_markdown(index=False)}",
                f"## Controles de puerta (bloqueantes / avisos)\n{gate_checks_display.to_markdown(index=False)}",
                "\n".join(
                    [
                        "## Afirmaciones que deben matizarse",
                        "- El sistema orienta decisiones de priorización, pero no sustituye estudios de red de ingeniería detallada.",
                        "- La cuantificación económica es una aproximación para comparación relativa, no presupuesto definitivo.",
                    ]
                ),
            ]
        )
        + "\n"
    )

    (paths.outputs_reports / "validation_report.md").write_text(report, encoding="utf-8")
    blocker_col = "bloqueante" if "bloqueante" in gate_checks.columns else "is_blocker"
    passed_col = "superado" if "superado" in gate_checks.columns else "passed"
    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_mode": source_mode,
        "overall_status": overall_status,
        "confidence_level": confidence,
        "issues_high": n_high,
        "issues_medium": n_med,
        "issues_low": int(assessment["n_low"]),
        "row_counts": counts,
        "release_readiness": release,
        "blocking_gates_failed": int((gate_checks[blocker_col] & (~gate_checks[passed_col])).sum())
        if not gate_checks.empty
        else 0,
        "warning_gates_failed": int(((~gate_checks[blocker_col]) & (~gate_checks[passed_col])).sum())
        if not gate_checks.empty
        else 0,
    }
    (paths.outputs_reports / "validation_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    conn.close()

    return {
        "issues_found": issues_df,
        "validation_checklist": checklist,
        "validation_gate_checks": gate_checks,
    }


if __name__ == "__main__":
    result = run_validation()
    for k, v in result.items():
        print(k, len(v))
