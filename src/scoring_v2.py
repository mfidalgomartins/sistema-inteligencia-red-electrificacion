from __future__ import annotations

from textwrap import dedent

import numpy as np
import pandas as pd

from .common_v2 import connect_v2, ensure_dirs, get_paths, minmax, write_df


def _tier(score: float, cuts: tuple[float, float, float] = (40, 60, 80)) -> str:
    if score >= cuts[2]:
        return "critico"
    if score >= cuts[1]:
        return "alto"
    if score >= cuts[0]:
        return "medio"
    return "bajo"


def _urgency(score: float) -> str:
    if score >= 85:
        return "inmediata"
    if score >= 70:
        return "alta"
    if score >= 55:
        return "planificada"
    return "monitorizacion"


def run_scoring_v2() -> dict[str, pd.DataFrame]:
    paths = ensure_dirs(get_paths())
    conn = connect_v2(paths)

    zone_month = conn.execute(
        """
        SELECT
            zona_id,
            AVG(horas_congestion) AS horas_congestion_avg,
            AVG(carga_punta_mw) AS carga_punta_avg,
            AVG(riesgo_operativo_agregado) AS riesgo_operativo_agregado,
            AVG(indice_resiliencia) AS indice_resiliencia,
            AVG(intensidad_capex_proxy) AS intensidad_capex_proxy,
            AVG(flexibilidad_efectiva) AS flexibilidad_efectiva,
            AVG(storage_efectivo) AS storage_efectivo,
            AVG(presion_crecimiento) AS presion_crecimiento,
            AVG(demanda_ev_mwh + demanda_industrial_mwh) AS nueva_demanda_mwh
        FROM zone_month_features
        GROUP BY zona_id
        """
    ).df()

    zone_day = conn.execute(
        """
        SELECT
            zona_id,
            AVG(ens) AS ens_avg,
            AVG(clientes_afectados) AS clientes_afectados_avg,
            AVG(coste_riesgo_proxy) AS coste_riesgo_proxy,
            AVG(demanda_no_servida_proxy) AS demanda_no_servida_proxy,
            AVG(gap_flexibilidad) AS gap_flexibilidad,
            AVG(percentil_carga) AS percentil_carga
        FROM zone_day_features
        GROUP BY zona_id
        """
    ).df()

    flex_gap = conn.execute(
        """
        SELECT
            zona_id,
            gap_tecnico_mw,
            gap_economico_proxy_eur,
            ratio_flexibilidad_estres,
            horas_congestion_acumuladas,
            riesgo_operativo_score
        FROM vw_flexibility_gap
        """
    ).df()

    assets = conn.execute(
        """
        SELECT
            zona_id,
            AVG(exposicion_activo_score) AS exposicion_activos,
            AVG(probabilidad_fallo_ajustada_proxy) AS prob_fallo_ajustada,
            SUM(ens_subestacion_mwh) AS ens_expuesta
        FROM vw_assets_exposure
        GROUP BY zona_id
        """
    ).df()

    candidates = conn.execute(
        """
        SELECT
            zona_id,
            AVG(capex_estimado) AS capex_medio,
            SUM(capex_estimado) AS capex_total,
            AVG(horizonte_meses) AS horizonte_medio,
            AVG(facilidad_implementacion) AS facilidad_media,
            AVG(impacto_resiliencia) AS impacto_resiliencia_medio,
            AVG(prioridad_inicial_score) AS prioridad_inicial_media
        FROM vw_investment_candidates
        GROUP BY zona_id
        """
    ).df()

    anomaly = pd.read_csv(paths.data_processed / "anomaly_zone_intensity.csv") if (paths.data_processed / "anomaly_zone_intensity.csv").exists() else pd.DataFrame()
    forecast = pd.read_csv(paths.data_processed / "forecast_predictability_pressure.csv") if (paths.data_processed / "forecast_predictability_pressure.csv").exists() else pd.DataFrame()

    base = zone_month.merge(zone_day, on="zona_id", how="left")
    base = base.merge(flex_gap, on="zona_id", how="left")
    base = base.merge(assets, on="zona_id", how="left")
    base = base.merge(candidates, on="zona_id", how="left")

    if not anomaly.empty:
        base = base.merge(anomaly, on="zona_id", how="left")
    else:
        base["n_anomalias"] = 0.0
        base["severidad_media"] = 0.0
        base["anomalias_criticas"] = 0.0
        base["ratio_precursor_congestion"] = 0.0
        base["ratio_precursor_interrupcion"] = 0.0

    if not forecast.empty:
        base = base.merge(forecast[["zona_id", "mae", "ratio_nueva_demanda", "decision_forecast"]], on="zona_id", how="left")
    else:
        base["mae"] = 0.0
        base["ratio_nueva_demanda"] = 0.0
        base["decision_forecast"] = "forecast_no_disponible"

    numeric_cols = base.select_dtypes(include=[np.number]).columns
    base[numeric_cols] = base[numeric_cols].fillna(0.0)
    if "decision_forecast" in base.columns:
        base["decision_forecast"] = base["decision_forecast"].fillna("forecast_no_disponible")

    # Scores obligatorios.
    base["congestion_risk_score"] = (
        0.40 * minmax(base["horas_congestion_avg"])
        + 0.30 * minmax(base["carga_punta_avg"])
        + 0.20 * minmax(base["recurrencia_congestion" if "recurrencia_congestion" in base.columns else "horas_congestion_acumuladas"])
        + 0.10 * minmax(base["ratio_precursor_congestion"])
    )

    base["resilience_risk_score"] = (
        0.38 * minmax(base["ens_avg"])
        + 0.22 * minmax(base["demanda_no_servida_proxy"])
        + 0.20 * (100 - minmax(base["indice_resiliencia"]))
        + 0.20 * minmax(base["severidad_media"])
    )

    base["service_impact_score"] = (
        0.40 * minmax(base["clientes_afectados_avg"])
        + 0.25 * minmax(base["ens_avg"])
        + 0.20 * minmax(base["anomalias_criticas"])
        + 0.15 * minmax(base["ratio_precursor_interrupcion"])
    )

    base["flexibility_gap_score"] = (
        0.42 * minmax(base["gap_flexibilidad"])
        + 0.28 * (100 - minmax(base["ratio_flexibilidad_estres"]))
        + 0.20 * minmax(base["gap_tecnico_mw"])
        + 0.10 * minmax(base["gap_economico_proxy_eur"])
    )

    base["asset_exposure_score"] = (
        0.50 * minmax(base["exposicion_activos"])
        + 0.30 * minmax(base["prob_fallo_ajustada"])
        + 0.20 * minmax(base["ens_expuesta"])
    )

    base["electrification_pressure_score"] = (
        0.40 * minmax(base["ratio_nueva_demanda"])
        + 0.30 * minmax(base["presion_crecimiento"])
        + 0.20 * minmax(base["nueva_demanda_mwh"])
        + 0.10 * minmax(base["mae"])
    )

    base["economic_priority_score"] = (
        0.45 * minmax(base["coste_riesgo_proxy"])
        + 0.30 * minmax(base["intensidad_capex_proxy"])
        + 0.15 * minmax(base["capex_total"])
        + 0.10 * minmax(base["capex_medio"])
    )

    urgency_base = (
        0.22 * base["congestion_risk_score"]
        + 0.16 * base["resilience_risk_score"]
        + 0.14 * base["service_impact_score"]
        + 0.14 * base["flexibility_gap_score"]
        + 0.12 * base["asset_exposure_score"]
        + 0.10 * base["electrification_pressure_score"]
        + 0.12 * base["economic_priority_score"]
    )

    # Algoritmo multicriterio por tipo de intervención.
    option_rows: list[dict] = []
    for _, row in base.iterrows():
        options = {
            "refuerzo_red": {
                "impact": 0.55 * row["congestion_risk_score"] + 0.25 * row["electrification_pressure_score"] + 0.20 * row["service_impact_score"],
                "cost": row["capex_total"],
                "time": row["horizonte_medio"],
                "robustez": 0.90,
            },
            "flexibilidad": {
                "impact": 0.45 * row["flexibility_gap_score"] + 0.25 * row["congestion_risk_score"] + 0.30 * row["economic_priority_score"],
                "cost": 0.35 * row["capex_total"],
                "time": 0.55 * row["horizonte_medio"],
                "robustez": 0.68,
            },
            "almacenamiento": {
                "impact": 0.40 * row["flexibility_gap_score"] + 0.35 * row["service_impact_score"] + 0.25 * row["electrification_pressure_score"],
                "cost": 0.55 * row["capex_total"],
                "time": 0.70 * row["horizonte_medio"],
                "robustez": 0.76,
            },
            "intervencion_operativa": {
                "impact": 0.35 * row["congestion_risk_score"] + 0.30 * row["asset_exposure_score"] + 0.35 * row["resilience_risk_score"],
                "cost": 0.18 * row["capex_total"],
                "time": 0.35 * row["horizonte_medio"],
                "robustez": 0.60,
            },
        }

        # Normalización local para comparar opciones de la misma zona.
        cost_vals = np.array([v["cost"] for v in options.values()], dtype=float)
        time_vals = np.array([v["time"] for v in options.values()], dtype=float)
        cost_min, cost_max = cost_vals.min(), cost_vals.max()
        time_min, time_max = time_vals.min(), time_vals.max()

        for opt, v in options.items():
            cost_norm = 100.0 * (v["cost"] - cost_min) / (cost_max - cost_min + 1e-9)
            time_norm = 100.0 * (v["time"] - time_min) / (time_max - time_min + 1e-9)
            urgency = urgency_base.loc[row.name]
            option_score = (
                0.40 * v["impact"]
                + 0.20 * (100.0 - cost_norm)
                + 0.15 * (100.0 - time_norm)
                + 0.15 * urgency
                + 0.10 * (100.0 * v["robustez"])
            )
            option_rows.append(
                {
                    "zona_id": row["zona_id"],
                    "option": opt,
                    "impact": v["impact"],
                    "cost_proxy": v["cost"],
                    "time_proxy": v["time"],
                    "robustez": v["robustez"],
                    "option_score": option_score,
                }
            )

    options_df = pd.DataFrame(option_rows)
    best_option = options_df.sort_values(["zona_id", "option_score"], ascending=[True, False]).groupby("zona_id", as_index=False).first()

    base = base.merge(best_option[["zona_id", "option", "option_score"]], on="zona_id", how="left")

    base["investment_priority_score"] = (
        0.74 * urgency_base + 0.26 * base["option_score"]
    ).clip(0, 100)

    base["risk_tier"] = base["investment_priority_score"].map(_tier)
    base["urgency_tier"] = base["investment_priority_score"].map(_urgency)

    intervention_map = {
        "refuerzo_red": "reforzar_red_local",
        "flexibilidad": "activar_flexibilidad",
        "almacenamiento": "desplegar_almacenamiento",
        "intervencion_operativa": "optimizar_operacion",
    }
    base["recommended_intervention"] = base["option"].map(intervention_map)

    base.loc[base["asset_exposure_score"] >= 75, "recommended_intervention"] = "sustituir_activos"
    base.loc[base["investment_priority_score"] >= 90, "recommended_intervention"] = "intervencion_inmediata_prioritaria"
    base.loc[base["investment_priority_score"] < 45, "recommended_intervention"] = "monitorizar"

    seq_map = {
        "intervencion_inmediata_prioritaria": "0-6m",
        "sustituir_activos": "0-12m",
        "reforzar_red_local": "6-24m",
        "desplegar_almacenamiento": "3-12m",
        "activar_flexibilidad": "0-6m",
        "optimizar_operacion": "0-3m",
        "monitorizar": "revision_trimestral",
    }
    base["recommended_sequence"] = base["recommended_intervention"].map(seq_map)

    base["main_risk_driver"] = base[
        [
            "congestion_risk_score",
            "resilience_risk_score",
            "service_impact_score",
            "flexibility_gap_score",
            "asset_exposure_score",
            "electrification_pressure_score",
            "economic_priority_score",
        ]
    ].idxmax(axis=1)

    base["confidence_flag"] = np.where(
        base["mae"] <= base["mae"].median(),
        "alta_confianza",
        "media_confianza_requiere_seguimiento",
    )

    # Sensibilidad de score: perturbación simple de pesos +/-10%.
    sens = []
    for factor in [0.9, 1.1]:
        alt = (
            0.22 * factor * base["congestion_risk_score"]
            + 0.16 * factor * base["resilience_risk_score"]
            + 0.14 * base["service_impact_score"]
            + 0.14 * base["flexibility_gap_score"]
            + 0.12 * base["asset_exposure_score"]
            + 0.10 * base["electrification_pressure_score"]
            + 0.12 * base["economic_priority_score"]
        )
        rank = alt.rank(ascending=False, method="first")
        sens.append(pd.DataFrame({"zona_id": base["zona_id"], "factor": factor, "rank_alt": rank}))
    sens_df = pd.concat(sens, ignore_index=True)

    ranking = base.sort_values("investment_priority_score", ascending=False).reset_index(drop=True)
    ranking["priority_rank"] = np.arange(1, len(ranking) + 1)

    # Persistencia canónica en DuckDB para consumo de validación/dashboard.
    conn.register("tmp_intervention_scoring_table", ranking)
    conn.register("tmp_intervention_multicriteria_options", options_df)
    conn.register("tmp_scoring_sensitivity_analysis", sens_df)
    conn.execute("CREATE OR REPLACE TABLE intervention_scoring_table AS SELECT * FROM tmp_intervention_scoring_table")
    conn.execute("CREATE OR REPLACE TABLE intervention_multicriteria_options AS SELECT * FROM tmp_intervention_multicriteria_options")
    conn.execute("CREATE OR REPLACE TABLE scoring_sensitivity_analysis AS SELECT * FROM tmp_scoring_sensitivity_analysis")
    conn.unregister("tmp_intervention_scoring_table")
    conn.unregister("tmp_intervention_multicriteria_options")
    conn.unregister("tmp_scoring_sensitivity_analysis")

    write_df(options_df, paths.data_processed / "intervention_multicriteria_options.csv")
    write_df(ranking, paths.data_processed / "intervention_scoring_table.csv")
    write_df(ranking[[
        "priority_rank",
        "zona_id",
        "investment_priority_score",
        "risk_tier",
        "urgency_tier",
        "main_risk_driver",
        "recommended_intervention",
        "recommended_sequence",
        "confidence_flag",
    ]], paths.data_processed / "intervention_ranking_final.csv")
    write_df(sens_df, paths.data_processed / "scoring_sensitivity_analysis.csv")

    framework_doc = dedent(
        """
        # Scoring Framework v2

        ## Scores obligatorios
        1. congestion_risk_score
        2. resilience_risk_score
        3. service_impact_score
        4. flexibility_gap_score
        5. asset_exposure_score
        6. electrification_pressure_score
        7. economic_priority_score
        8. investment_priority_score

        ## Principio
        Framework interpretable, sin black box, combinando criterios técnicos, de servicio, activos, electrificación y economía.

        ## Fórmulas (resumen)
        - Cada score parcial se construye con combinación lineal ponderada de señales normalizadas (0-100).
        - `investment_priority_score = 0.74 * urgency_base + 0.26 * option_score_multicriterio`.

        ## Algoritmo multicriterio de alternativas
        Alternativas comparadas por zona:
        - refuerzo_red
        - flexibilidad
        - almacenamiento
        - intervencion_operativa

        Criterios:
        - impacto esperado (40%)
        - coste proxy (20%, inverso)
        - tiempo de despliegue (15%, inverso)
        - urgencia (15%)
        - robustez de solución (10%)

        ## Trade-offs
        - Refuerzo mejora robustez estructural pero penaliza coste/tiempo.
        - Flexibilidad y operación mejoran rapidez y diferibilidad de CAPEX.
        - Storage equilibra impacto técnico y resiliencia en zonas con alta variabilidad.

        ## Tiers y reglas
        - risk_tier: bajo / medio / alto / critico.
        - urgency_tier: monitorizacion / planificada / alta / inmediata.
        - confidence_flag depende de error de forecasting por zona.

        ## Tipos de intervención finales
        - monitorizar
        - optimizar_operacion
        - activar_flexibilidad
        - desplegar_almacenamiento
        - reforzar_red_local
        - sustituir_activos
        - intervencion_inmediata_prioritaria
        """
    ).strip() + "\n"

    (paths.docs / "scoring_framework.md").write_text(framework_doc, encoding="utf-8")

    conn.close()

    return {
        "intervention_scoring_table": ranking,
        "intervention_multicriteria_options": options_df,
        "scoring_sensitivity_analysis": sens_df,
    }


if __name__ == "__main__":
    out = run_scoring_v2()
    for name, df in out.items():
        print(name, len(df))
