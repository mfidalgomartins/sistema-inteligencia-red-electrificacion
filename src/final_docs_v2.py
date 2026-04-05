from __future__ import annotations

import json
from datetime import datetime
from textwrap import dedent

import pandas as pd

from .common_v2 import ensure_dirs, get_paths


def _safe_read(path, default=None):
    try:
        return pd.read_csv(path)
    except Exception:
        return default if default is not None else pd.DataFrame()


def _safe_read_json(path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default if default is not None else {}


def build_final_docs_v2() -> dict[str, str]:
    paths = ensure_dirs(get_paths())

    scoring = _safe_read(paths.data_processed / "intervention_scoring_table.csv")
    zone_risk = _safe_read(paths.data_processed / "vw_zone_operational_risk.csv")
    forecast_bench = _safe_read(paths.data_processed / "forecast_model_benchmark.csv")
    scenario_summary = _safe_read(paths.data_processed / "scenario_summary_v2.csv")
    issues = _safe_read(paths.outputs_reports / "issues_found.csv")
    validation_summary = _safe_read_json(paths.outputs_reports / "validation_summary.json", default={})

    zonas = int(zone_risk["zona_id"].nunique()) if "zona_id" in zone_risk.columns else 0
    horas_cong = float(zone_risk["horas_congestion"].sum()) if "horas_congestion" in zone_risk.columns else 0.0
    ens_total = float(zone_risk["ens_total_mwh"].sum()) if "ens_total_mwh" in zone_risk.columns else 0.0
    top_intervention = (
        scoring["recommended_intervention"].mode().iloc[0]
        if (not scoring.empty and "recommended_intervention" in scoring.columns)
        else "n/a"
    )
    top_driver = (
        scoring["main_risk_driver"].mode().iloc[0]
        if (not scoring.empty and "main_risk_driver" in scoring.columns)
        else "n/a"
    )

    best_forecast = (
        forecast_bench.sort_values("mae").iloc[0][["task", "model", "mae"]].to_dict()
        if not forecast_bench.empty
        else {"task": "n/a", "model": "n/a", "mae": "n/a"}
    )

    best_scenario = (
        scenario_summary.sort_values("coste_riesgo_total").iloc[0]["scenario"]
        if (not scenario_summary.empty and "coste_riesgo_total" in scenario_summary.columns)
        else "n/a"
    )
    validation_status = str(validation_summary.get("overall_status", "N/A"))
    validation_confidence = str(validation_summary.get("confidence_level", "N/A"))
    release_readiness = validation_summary.get("release_readiness", {})
    decision_state = str(release_readiness.get("decision_state", "N/A"))
    publish_state = str(release_readiness.get("publish_state", "N/A"))
    issues_high = int(validation_summary.get("issues_high", (issues["severity"] == "alta").sum() if "severity" in issues.columns else 0))
    issues_medium = int(
        validation_summary.get("issues_medium", (issues["severity"] == "media").sum() if "severity" in issues.columns else 0)
    )

    readme = dedent(
        f"""
        # Sistema de Inteligencia de Red, Flexibilidad, Resiliencia y Priorización de Inversiones para la Electrificación Territorial

        ## Executive Overview
        Plataforma analítica end-to-end para una utility de distribución orientada a electrificación territorial. El sistema integra planificación de red, operación, calidad de servicio, flexibilidad, almacenamiento y estrategia de inversión para responder dónde intervenir primero y con qué palanca.

        Cobertura actual del proyecto:
        - Zonas analizadas: **{zonas}**
        - Horas de congestión acumuladas: **{horas_cong:,.0f}**
        - ENS total estimada: **{ens_total:,.2f} MWh**

        ## Business Problem
        ¿Dónde está la red perdiendo capacidad operativa, resiliencia y eficiencia económica por congestión, crecimiento de demanda, electrificación, generación distribuida, activos envejecidos y restricciones territoriales, y cómo priorizar entre refuerzo, flexibilidad, almacenamiento y operación avanzada?

        ## Why this matters for utilities and electrification
        - La electrificación traslada presión a redes de distribución con ventanas punta más exigentes.
        - El CAPEX no puede crecer de forma uniforme; requiere priorización multicriterio.
        - La combinación de inteligencia operativa + inversión selectiva puede reducir coste de riesgo y ENS.

        ## Project Architecture
        - `data/raw`: ecosistema sintético realista multi-zona y multi-nodo.
        - `sql`: capa SQL profesional por niveles (`staging`, `integration`, `marts`, `kpis`, `validation`).
        - `src/*_v2.py`: módulos analíticos avanzados (features, forecasting, anomalías, scoring, escenarios, visualización, dashboard, validación).
        - `data/processed`: tablas analíticas y outputs modelados.
        - `outputs/charts`, `outputs/dashboard`, `outputs/reports`: artefactos ejecutivos.
        - `docs/repository_structure.md`: mapa canónico de estructura y separación entre capa oficial y legacy.

        ## Synthetic Data Model
        Modelo con granularidad horaria de 2 años, múltiples regiones/zonas/subestaciones/alimentadores, eventos de congestión/interrupciones, EV, electrificación industrial, GD, curtailment, flexibilidad, storage, activos e inversiones.

        ## SQL Layer
        Dialecto: **DuckDB SQL**.

        Secuencia principal:
        1. `01_staging_core_tables.sql`
        2. `02_integrated_network_load.sql`
        3. `03_integrated_grid_events.sql`
        4. `04_integrated_service_quality.sql`
        5. `05_integrated_flexibility_assets.sql`
        6. `06_analytical_mart_node_hour.sql`
        7. `07_analytical_mart_zone_day.sql`
        8. `08_analytical_mart_zone_month.sql`
        9. `09_kpi_queries.sql`
        10. `10_validation_queries.sql`

        ## Feature Engineering
        Tablas creadas:
        - `node_hour_features`
        - `zone_day_features`
        - `zone_month_features`
        - `intervention_candidates_features`

        Documentación: `docs/feature_dictionary.md`

        ## Forecasting
        Benchmark interpretable con:
        - naive
        - seasonal naive
        - moving average
        - linear trend
        - exponential smoothing

        Cobertura mínima:
        - demanda por zona
        - demanda por subestación
        - carga relativa por zona
        - demanda EV
        - demanda industrial

        Mejor combinación actual (MAE): **{best_forecast['task']} / {best_forecast['model']} / {best_forecast['mae']}**.

        ## Anomaly Detection
        Detección operativa con métodos interpretables:
        - z-score
        - residual rolling
        - percentiles extremos
        - baseline estacional
        - reglas operativas

        Salida clave: `data/processed/anomalies_detected.csv` con severidad, explicación probable y señal precursora.

        ## Scoring Framework
        Scores obligatorios implementados:
        - congestion_risk_score
        - resilience_risk_score
        - service_impact_score
        - flexibility_gap_score
        - asset_exposure_score
        - electrification_pressure_score
        - economic_priority_score
        - investment_priority_score

        Extras:
        - risk_tier
        - urgency_tier
        - main_risk_driver
        - recommended_intervention
        - recommended_sequence
        - confidence_flag

        Driver principal dominante: **{top_driver}**.
        Intervención más frecuente: **{top_intervention}**.

        ## Scenario Engine
        Escenarios cubiertos:
        1. crecimiento acelerado EV
        2. electrificación industrial intensiva
        3. mayor penetración GD
        4. retraso CAPEX
        5. despliegue adicional flexibilidad
        6. despliegue adicional storage
        7. CAPEX + flexibilidad
        8. degradación de activos

        Escenario con menor coste de riesgo (actual): **{best_scenario}**.

        ## Dashboard Overview
        Archivo oficial único: `outputs/dashboard/dashboard_inteligencia_red.html`

        Incluye:
        - header ejecutivo
        - KPI cards
        - salud operativa
        - resiliencia y servicio
        - flexibilidad y storage
        - electrificación
        - inversión y priorización
        - comparación de escenarios
        - tabla interactiva final con filtros
        - sección de decisión ejecutiva

        ## Key Findings
        - El riesgo no es homogéneo: la congestión se concentra en nodos concretos.
        - ENS y fragilidad de activos se acoplan en zonas de mayor criticidad.
        - La flexibilidad cubre parcialmente la presión; el gap técnico persiste en zonas específicas.
        - EV e industrial incrementan presión y reducen previsibilidad en segmentos concretos.

        ## Recommendations
        - Priorizar intervención focalizada por nodo/zona, no refuerzo uniforme.
        - Usar flexibilidad y operación avanzada para diferir CAPEX donde la previsibilidad es robusta.
        - Reservar refuerzo estructural para zonas críticas con riesgo persistente y baja cobertura flexible.

        ## What this project demonstrates
        - Traducción de problema regulado/operativo a arquitectura analítica ejecutable.
        - Construcción de capa SQL y data products defendibles.
        - Diseño de sistema de decisión multicriterio interpretable.

        ## Business skills demonstrated
        - Framing de decisiones de inversión en red.
        - Priorización técnico-económica.
        - Comunicación ejecutiva y narrativa para dirección.

        ## Technical skills demonstrated
        - Data modeling y synthetic data engineering.
        - SQL analítico de nivel producción.
        - Feature engineering para operación y planificación.
        - Forecasting interpretable y evaluación segmentada.
        - Anomaly detection operacional.
        - Dashboard engineering HTML + Chart.js.

        ## Questions this system helps answer
        - Dónde está el mayor riesgo operativo y por qué.
        - Qué intervención conviene por zona y en qué secuencia.
        - Qué parte del CAPEX puede diferirse sin elevar riesgo inaceptable.
        - Cómo cambia la cartera ante escenarios de electrificación.

        ## Why this is relevant for a company like Iberdrola
        - Integra operación diaria, resiliencia y estrategia de capital en un único marco analítico.
        - Mejora trazabilidad de decisiones frente a regulación, dirección territorial y planificación.
        - Permite comparar palancas (refuerzo/flex/storage/operación) con criterios transparentes.

        ## Repository Structure
        ```
        data/
          raw/
          processed/
        docs/
        notebooks/
        outputs/
          charts/
          dashboard/
          reports/
        sql/
        src/
        tests/
        README.md
        requirements.txt
        ```

        ## How to Run
        ```bash
        python3 -m venv .venv
        source .venv/bin/activate
        pip install -r requirements.txt
        python -m src
        ```
        Compatibilidad legacy (solo referencia histórica):
        ```bash
        python -m src --legacy
        ```

        ## Validation Approach
        - Controles SQL + validación transversal en `src/validate_data_v2.py`.
        - Reportes de calidad: `outputs/reports/validation_report.md` y `outputs/reports/issues_found.csv`.
        - Estado actual de validación: **{validation_status}** (confianza: **{validation_confidence}**, issues alta/media: **{issues_high}/{issues_medium}**).
        - Release readiness: **{decision_state}** / **{publish_state}**.

        ## Governance and Quality Controls
        - Contrato canónico v2 y política de deprecación: `docs/governance_framework.md`.
        - Diccionario de métricas activo: `docs/metric_dictionary.md`.
        - Definiciones SQL oficiales: `docs/sql_metric_definitions.md`.
        - Manifest de release con hashes de artefactos: `outputs/reports/release_manifest.json`.
        - Smoke checks post-ejecución: `python -m src.qa_smoke_v2`.

        ## Limitations
        - Dataset sintético: útil para diseño y comparación, no sustituto de operación real.
        - Proxies económicos: orientan prioridades relativas, no presupuesto regulatorio final.
        - Causalidad limitada: requiere calibración con eventos históricos reales.

        ## Next Steps
        1. Calibración con telemetría y histórico real de incidentes.
        2. Integración de restricciones eléctricas más detalladas en red.
        3. Ajuste de costes con supuestos regulatorios y financieros reales.
        4. Integración API para refresh operativo continuo.

        ## Decision Rules (explicit)
        - **Cuándo reforzar red**: cuando coinciden congestion_risk alto, electrification_pressure alto y gap flexible persistente.
        - **Cuándo activar flexibilidad**: cuando el riesgo es alto pero la ventana de despliegue exige respuesta rápida y CAPEX diferible.
        - **Cuándo desplegar almacenamiento**: cuando curtailment y variabilidad neta elevan coste de riesgo y la flexibilidad contratada no basta.
        - **Qué decisiones pueden diferirse**: zonas con score medio/bajo, forecast estable y cobertura flexible suficiente.
        """
    ).strip() + "\n"

    readme_path = paths.root / "README.md"
    readme_path.write_text(readme, encoding="utf-8")

    memo = dedent(
        f"""
        # Memo Ejecutivo
        **Sistema de Inteligencia de Red, Flexibilidad, Resiliencia y Priorización de Inversiones para la Electrificación Territorial**

        Fecha: {datetime.now().strftime('%Y-%m-%d')}

        ## 1. Contexto
        La red de distribución afronta presión creciente por electrificación, variabilidad operativa y limitaciones de inversión.

        ## 2. Problema
        Se requiere priorizar intervenciones con criterio técnico-económico para minimizar congestión, ENS y coste de riesgo.

        ## 3. Enfoque metodológico
        Capa SQL multicapa, feature engineering por granularidad, forecasting interpretable, detección de anomalías, scoring multicriterio y scenario engine comparativo.

        ## 4. Hallazgos principales
        - La congestión está concentrada territorialmente.
        - ENS y exposición de activos muestran patrones alineados con estrés de red.
        - EV e industria elevan presión y reducen previsibilidad en zonas concretas.

        ## 5. Implicaciones operativas
        - Priorización de nodos críticos para intervención temprana.
        - Activación preventiva de flexibilidad en ventanas de alto riesgo.
        - Refuerzo de monitoreo en subestaciones con deterioro operativo.

        ## 6. Implicaciones económicas
        - Existe CAPEX diferible en zonas con buena previsibilidad y cobertura flexible.
        - El coste de no actuar aumenta de forma no lineal en escenarios de retraso CAPEX.

        ## 7. Trade-offs principales
        - Refuerzo físico: robustez alta, coste y tiempo altos.
        - Flexibilidad/operación: rapidez alta, robustez media.
        - Storage: equilibrio entre resiliencia y control de curtailment.

        ## 8. Prioridades de intervención
        Se recomienda secuencia diferenciada por urgencia tier y driver principal de riesgo.

        ## 9. Decisiones que pueden diferirse
        Intervenciones estructurales en zonas con score medio/bajo y confianza alta de forecast.

        ## 10. Limitaciones
        Datos sintéticos y proxies económicos; requiere calibración con datos reales de red y regulación.

        ## 11. Próximos pasos
        - Calibración real SCADA/AMI.
        - Validación de causalidad de eventos.
        - Industrialización de refresh analítico y gobierno de métricas.
        """
    ).strip() + "\n"

    memo_path = paths.outputs_reports / "memo_ejecutivo_es.md"
    memo_path.write_text(memo, encoding="utf-8")

    return {
        "readme": str(readme_path),
        "memo": str(memo_path),
    }


if __name__ == "__main__":
    out = build_final_docs_v2()
    for k, v in out.items():
        print(k, v)
