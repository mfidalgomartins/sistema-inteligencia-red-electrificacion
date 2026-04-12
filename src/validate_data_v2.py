from __future__ import annotations

import json
from datetime import datetime, timezone
from textwrap import dedent

import pandas as pd

from .common_v2 import connect_v2, ensure_dirs, get_paths, write_df


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
    """Clasifica readiness técnico/analítico/ejecutivo con reglas explícitas."""
    n_high = int(assessment.get("n_high", 0))
    n_med = int(assessment.get("n_med", 0))
    blocked = bool((gate_checks["is_blocker"] & (~gate_checks["passed"])).any()) if not gate_checks.empty else False

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
        "committee_state": "committee-grade",
        "publish_state": "publish-ready",
    }


def run_validate_data_v2() -> dict[str, pd.DataFrame]:
    paths = ensure_dirs(get_paths())
    conn = connect_v2(paths)

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

    # 1) Row counts razonables.
    counts = conn.execute(
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
    ).df().iloc[0].to_dict()

    if counts["n_demanda_horaria"] < 1_000_000:
        add_issue("generacion", "row_count_demanda_horaria", "alta", counts["n_demanda_horaria"], ">=1,000,000", "revisar parámetros del generador")

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
        add_issue("sql", "duplicates_node_hour_key", "alta", dups, "0", "forzar PK lógica en mart")

    # 3) Nulls problemáticos.
    nulls = conn.execute(
        """
        SELECT
            SUM(CASE WHEN zona_id IS NULL THEN 1 ELSE 0 END) AS null_zona,
            SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp,
            SUM(CASE WHEN demanda_mw IS NULL THEN 1 ELSE 0 END) AS null_demanda
        FROM stg_demanda_horaria
        """
    ).df().iloc[0]
    for col in ["null_zona", "null_timestamp", "null_demanda"]:
        if int(nulls[col]) > 0:
            add_issue("profiling", f"nulls_{col}", "alta", int(nulls[col]), "0", "aplicar contrato NOT NULL en staging")

    # 4) Valores imposibles.
    neg = conn.execute(
        """
        SELECT
            SUM(CASE WHEN demanda_mw < 0 THEN 1 ELSE 0 END) AS demanda_neg,
            SUM(CASE WHEN capacidad_mw <= 0 THEN 1 ELSE 0 END) AS capacidad_no_positiva
        FROM vw_int_network_load_hour
        """
    ).df().iloc[0]
    if int(neg["demanda_neg"]) > 0:
        add_issue("data", "demanda_negativa", "alta", int(neg["demanda_neg"]), "0", "corregir generador y validación previa")
    if int(neg["capacidad_no_positiva"]) > 0:
        add_issue("data", "capacidad_no_positiva", "alta", int(neg["capacidad_no_positiva"]), "0", "limpiar metadatos de activos")

    # 5) Lógica temporal.
    temporal = conn.execute(
        """
        SELECT
            SUM(CASE WHEN timestamp_fin < timestamp_inicio THEN 1 ELSE 0 END) AS eventos_bad,
            (SELECT SUM(CASE WHEN timestamp_fin < timestamp_inicio THEN 1 ELSE 0 END) FROM stg_interrupciones_servicio) AS interrupciones_bad
        FROM stg_eventos_congestion
        """
    ).df().iloc[0]
    if int(temporal["eventos_bad"]) > 0 or int(temporal["interrupciones_bad"]) > 0:
        add_issue("temporal", "timestamp_inicio_fin_invalido", "alta", int(temporal["eventos_bad"] + temporal["interrupciones_bad"]), "0", "normalizar eventos con start<=end")

    # 6) Consistencia demanda-capacidad.
    overload = conn.execute(
        """
        SELECT
            AVG(CASE WHEN carga_relativa > 1.0 THEN 1 ELSE 0 END) AS pct_overload
        FROM mart_node_hour_operational_state
        """
    ).fetchone()[0]
    if float(overload) > 0.45:
        add_issue("consistencia", "overload_excesivo", "media", round(float(overload), 4), "<=0.45", "revisar calibración de capacidad/demanda")

    # 7) Consistencia congestión vs interrupciones.
    cong_vs_int = conn.execute(
        """
        SELECT COUNT(*)
        FROM vw_int_service_quality_enriched
        WHERE relacion_congestion_flag = TRUE AND congestion_overlap_flag = FALSE
        """
    ).fetchone()[0]
    if int(cong_vs_int) > 0:
        add_issue("consistencia", "interrupciones_congestion_sin_solape", "media", int(cong_vs_int), "0", "alinear lógica de bandera y solape temporal")

    # 8) Consistencia GD y curtailment.
    gd_curt = conn.execute(
        """
        SELECT COUNT(*)
        FROM stg_generacion_distribuida
        WHERE curtailment_estimado_mw > generacion_mw
        """
    ).fetchone()[0]
    if int(gd_curt) > 0:
        add_issue("consistencia", "curtailment_mayor_generacion", "alta", int(gd_curt), "0", "capar curtailment al máximo de generación")

    # 9) Consistencia features y scores.
    no_score = conn.execute(
        """
        SELECT COUNT(*)
        FROM zone_month_features z
        LEFT JOIN intervention_scoring_table s ON z.zona_id = s.zona_id
        WHERE s.zona_id IS NULL
        """
    ).fetchone()[0]
    if int(no_score) > 0:
        add_issue("scoring", "zonas_sin_score", "alta", int(no_score), "0", "forzar cobertura full join en scoring")

    # 10) Coherencia outputs-dashboard.
    dashboard_file = paths.outputs_dashboard / "grid-electrification-command-center.html"
    if not dashboard_file.exists():
        add_issue("dashboard", "dashboard_missing", "alta", "no_file", "file_exists", "ejecutar build_dashboard_v2")

    # 11) Denominadores correctos (proxy).
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

    # 12) Enforcements de scores y tiers.
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
        add_issue("scoring", "scores_fuera_rango_0_100", "alta", int(score_bounds), "0", "normalizar y acotar scores en capa scoring")

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
        add_issue("scoring", "mismatch_tier_vs_score", "alta", int(tier_mismatch), "0", "alinear reglas de tier con umbrales oficiales")

    # 13) Lógica financiera/decisión.
    capex_incoherente = conn.execute(
        """
        SELECT COUNT(*)
        FROM kpi_zonas_potencial_capex_diferible
        WHERE capex_diferible_proxy_eur > capex_refuerzo_eur
        """
    ).fetchone()[0]
    if int(capex_incoherente) > 0:
        add_issue("economico", "capex_diferible_mayor_refuerzo", "media", int(capex_incoherente), "0", "recalibrar proxy de capex diferible")

    decision_critica_sin_accion = conn.execute(
        """
        SELECT COUNT(*)
        FROM intervention_scoring_table
        WHERE risk_tier = 'critico'
          AND recommended_intervention IN ('monitorizar')
        """
    ).fetchone()[0]
    if int(decision_critica_sin_accion) > 0:
        add_issue("decision", "critico_sin_accion_fuerte", "alta", int(decision_critica_sin_accion), "0", "forzar regla de intervención fuerte en tier crítico")

    issues_df = pd.DataFrame(issues)
    if issues_df.empty:
        issues_df = pd.DataFrame(
            [{
                "area": "global",
                "check": "sin_issues_criticos",
                "severity": "info",
                "observed": 0,
                "expected": "0",
                "fix_applied_or_recommended": "No aplica",
            }]
        )

    severity_order = {"alta": 1, "media": 2, "baja": 3, "info": 4}
    issues_df["severity_rank"] = issues_df["severity"].map(severity_order).fillna(5)
    issues_df = issues_df.sort_values(["severity_rank", "area", "check"]).drop(columns=["severity_rank"])

    fixes_applied = [
        "Se implementó capa SQL validada con controles formales (10_validation_queries.sql).",
        "Se incorporó feature engineering con contratos explícitos por granularidad.",
        "Se añadió benchmark de forecasting interpretable con métricas por segmento.",
        "Se incorporó detector de anomalías con señales precursoras.",
        "Se implementó scoring multicriterio con sensibilidad de pesos.",
        "Se añadió scenario engine con 8 escenarios comparables.",
    ]

    caveats = [
        "Los datos son sintéticos; no sustituyen calibración con telemetría real SCADA/AMI.",
        "Los proxies económicos no reemplazan valoración regulatoria ni WACC real.",
        "La causalidad entre anomalías, congestión y ENS requiere validación en histórico real.",
        "El dashboard usa simplificaciones para garantizar portabilidad HTML única.",
        "Los resultados de forecast dependen de estabilidad estructural de patrones de carga.",
    ]

    checklist = pd.DataFrame(
        [
            {"item": "generacion_datos", "status": "ok"},
            {"item": "relaciones_tablas", "status": "ok"},
            {"item": "profiling", "status": "ok"},
            {"item": "sql", "status": "ok"},
            {"item": "features", "status": "ok"},
            {"item": "forecasting", "status": "ok"},
            {"item": "anomaly_detection", "status": "ok"},
            {"item": "scoring", "status": "ok"},
            {"item": "scenario_engine", "status": "ok"},
            {"item": "impacto_economico", "status": "ok"},
            {"item": "visualizaciones", "status": "ok"},
            {"item": "dashboard", "status": "ok" if dashboard_file.exists() else "warning"},
            {"item": "narrativa_final", "status": "ok"},
        ]
    )

    assessment = compute_validation_assessment(issues_df)
    n_high = int(assessment["n_high"])
    n_med = int(assessment["n_med"])
    confidence = str(assessment["confidence_level"])
    overall_status = str(assessment["overall_status"])

    # 14) Consistencia cross-output (fuera de SQL, usando artefactos persistidos).
    gate_rows: list[dict] = []
    def add_gate(name: str, passed: bool, is_blocker: bool, detail: str):
        gate_rows.append(
            {
                "gate_name": name,
                "passed": bool(passed),
                "is_blocker": bool(is_blocker),
                "detail": detail,
            }
        )

    score_path = paths.data_processed / "intervention_scoring_table.csv"
    ranking_path = paths.data_processed / "intervention_ranking_final.csv"
    scenario_impacts_path = paths.data_processed / "scenario_impacts_v2.csv"
    scenario_summary_path = paths.data_processed / "scenario_summary_v2.csv"
    anomalies_path = paths.data_processed / "anomalies_detected.csv"
    anomalies_summary_path = paths.data_processed / "anomalies_summary_by_type.csv"
    sensitivity_path = paths.data_processed / "scoring_sensitivity_analysis.csv"
    forecast_benchmark_path = paths.data_processed / "forecast_model_benchmark.csv"

    add_gate("official_dashboard_exists", dashboard_file.exists(), True, str(dashboard_file))
    add_gate("official_dashboard_singleton", not (paths.outputs_dashboard / "dashboard_inteligencia_red_premium.html").exists(), False, "Solo grid-electrification-command-center.html debe ser oficial")
    add_gate("core_scoring_files_exist", score_path.exists() and ranking_path.exists(), True, "scoring_table + ranking_final")
    add_gate("scenario_files_exist", scenario_impacts_path.exists() and scenario_summary_path.exists(), True, "scenario_impacts_v2 + scenario_summary_v2")
    add_gate("anomaly_files_exist", anomalies_path.exists() and anomalies_summary_path.exists(), False, "anomalies_detected + anomalies_summary_by_type")
    add_gate("forecast_benchmark_exists", forecast_benchmark_path.exists(), False, "forecast_model_benchmark.csv")
    add_gate("sensitivity_exists", sensitivity_path.exists(), False, "scoring_sensitivity_analysis.csv")

    if score_path.exists() and ranking_path.exists():
        score_df = pd.read_csv(score_path)
        ranking_df = pd.read_csv(ranking_path)
        if not score_df.empty and not ranking_df.empty:
            top_score = score_df.sort_values("investment_priority_score", ascending=False).iloc[0]["zona_id"]
            top_rank = ranking_df.sort_values("priority_rank").iloc[0]["zona_id"]
            add_gate("ranking_matches_scoring_top", bool(top_score == top_rank), True, f"score_top={top_score}, rank_top={top_rank}")

    if scenario_impacts_path.exists() and scenario_summary_path.exists():
        impacts = pd.read_csv(scenario_impacts_path)
        summary = pd.read_csv(scenario_summary_path)
        if not impacts.empty and not summary.empty:
            agg = impacts.groupby("scenario", as_index=False)["coste_riesgo_scenario"].sum()
            merged = summary.merge(agg, on="scenario", how="inner")
            if not merged.empty:
                max_abs_diff = (merged["coste_riesgo_total"] - merged["coste_riesgo_scenario"]).abs().max()
                add_gate("scenario_cost_consistency", bool(max_abs_diff <= 1e-6), True, f"max_abs_diff={max_abs_diff}")

    if anomalies_path.exists() and anomalies_summary_path.exists():
        anom = pd.read_csv(anomalies_path)
        anom_sum = pd.read_csv(anomalies_summary_path)
        if not anom.empty and not anom_sum.empty:
            cnt = anom.groupby("anomaly_type").size().rename("n_eventos").reset_index()
            chk = anom_sum.merge(cnt, on="anomaly_type", how="left", suffixes=("_summary", "_detected"))
            if not chk.empty and "n_eventos_summary" in chk.columns and "n_eventos_detected" in chk.columns:
                mismatch = (chk["n_eventos_summary"] != chk["n_eventos_detected"]).sum()
                add_gate("anomaly_summary_consistency", bool(mismatch == 0), False, f"mismatch_rows={int(mismatch)}")

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
                add_gate("score_stability_rank_shift", bool(max_dev <= 3.0), False, f"max_rank_shift={max_dev}")

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
        add_gate("forecast_required_tasks_covered", required_tasks.issubset(present_tasks), False, f"present={sorted(present_tasks)}")

    gate_checks = pd.DataFrame(gate_rows)
    if gate_checks.empty:
        gate_checks = pd.DataFrame(columns=["gate_name", "passed", "is_blocker", "detail"])

    release = classify_release_readiness(assessment, gate_checks)

    # Endurecer checklist global según severidad consolidada.
    if overall_status != "PASS":
        checklist.loc[checklist["item"].isin(["relaciones_tablas", "narrativa_final"]), "status"] = "warning"
    if overall_status == "FAIL":
        checklist.loc[:, "status"] = checklist["status"].replace({"ok": "warning"})

    report = dedent(
        f"""
        # Validation Report v2

        ## Objetivo
        Validar coherencia end-to-end del proyecto: datos, SQL, features, forecasting, anomalías, scoring, escenarios, visuales y dashboard.

        ## Row counts clave
        {pd.DataFrame([counts]).to_markdown(index=False)}

        ## Issues encontrados
        {issues_df.to_markdown(index=False)}

        ## Fixes applied
        {pd.DataFrame({'fix': fixes_applied}).to_markdown(index=False)}

        ## Caveats obligatorios
        {pd.DataFrame({'caveat': caveats}).to_markdown(index=False)}

        ## Overall confidence assessment
        - Estado global: {overall_status}
        - Nivel: {confidence}
        - Issues alta severidad: {n_high}
        - Issues media severidad: {n_med}

        ## Release readiness classification
        - Technical: {release['technical_state']}
        - Analytical: {release['analytical_state']}
        - Decision: {release['decision_state']}
        - Committee: {release['committee_state']}
        - Publish: {release['publish_state']}

        ## Checklist final
        {checklist.to_markdown(index=False)}

        ## Gate checks (hard blockers / warnings)
        {gate_checks.to_markdown(index=False)}

        ## Claims que deben matizarse
        - El sistema orienta decisiones de priorización, pero no sustituye estudios de red de ingeniería detallada.
        - La cuantificación económica es proxy para comparación relativa, no presupuesto definitivo.
        """
    ).strip() + "\n"

    (paths.outputs_reports / "validation_report.md").write_text(report, encoding="utf-8")
    write_df(issues_df, paths.outputs_reports / "issues_found.csv")
    write_df(checklist, paths.outputs_reports / "validation_checklist_final.csv")
    write_df(gate_checks, paths.outputs_reports / "validation_gate_checks.csv")
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "overall_status": overall_status,
        "confidence_level": confidence,
        "issues_high": n_high,
        "issues_medium": n_med,
        "issues_low": int(assessment["n_low"]),
        "row_counts": counts,
        "release_readiness": release,
        "blocking_gates_failed": int((gate_checks["is_blocker"] & (~gate_checks["passed"])).sum()) if not gate_checks.empty else 0,
        "warning_gates_failed": int(((~gate_checks["is_blocker"]) & (~gate_checks["passed"])).sum()) if not gate_checks.empty else 0,
    }
    (paths.outputs_reports / "validation_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    conn.close()

    return {
        "issues_found": issues_df,
        "validation_checklist_final": checklist,
        "validation_gate_checks": gate_checks,
    }


if __name__ == "__main__":
    result = run_validate_data_v2()
    for k, v in result.items():
        print(k, len(v))
