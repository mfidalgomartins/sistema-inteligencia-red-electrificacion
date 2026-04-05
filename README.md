# Sistema de Inteligencia de Red, Flexibilidad, Resiliencia y Priorización de Inversiones para la Electrificación Territorial

## Executive Overview
Plataforma analítica end-to-end para una utility de distribución orientada a electrificación territorial. El sistema integra planificación de red, operación, calidad de servicio, flexibilidad, almacenamiento y estrategia de inversión para responder dónde intervenir primero y con qué palanca.

Cobertura actual del proyecto:
- Zonas analizadas: **24**
- Horas de congestión acumuladas: **274,623**
- ENS total estimada: **44,676.08 MWh**

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

Mejor combinación actual (MAE): **carga_relativa_zona / seasonal_naive / 0.0171433698770502**.

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

Driver principal dominante: **flexibility_gap_score**.
Intervención más frecuente: **monitorizar**.

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

Escenario con menor coste de riesgo (actual): **capex_mas_flexibilidad**.

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
- Estado actual de validación: **PASS** (confianza: **alta**, issues alta/media: **0/0**).
- Release readiness: **decision-support ready** / **publish-ready**.

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
