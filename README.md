# Sistema de Inteligencia de Red para Electrificación Territorial

Plataforma analítica orientada a utility para decidir **dónde** y **cómo** intervenir primero ante congestión, pérdida de resiliencia, presión de electrificación y restricciones de CAPEX.

## Problema de negocio
La pregunta central del sistema es:

**¿Qué zonas de red están perdiendo capacidad operativa y flexibilidad, cuál es el impacto técnico-económico y qué palanca conviene priorizar entre refuerzo, flexibilidad, almacenamiento y operación avanzada?**

## Qué demuestra este proyecto
- Diseño de arquitectura analítica por capas (raw, SQL, features, scoring, escenarios, dashboard).
- SQL profesional sobre DuckDB para integración operativa, marts y validaciones.
- Framework interpretable de priorización multicriterio para decisiones de inversión.
- Dashboard ejecutivo autocontenido para lectura rápida y decisión territorial.
- Disciplina de validación, smoke checks y release manifest.

## Arquitectura resumida
- `data/raw`: ecosistema sintético multi-zona/multi-nodo.
- `sql`: 10 scripts por capas (`staging` → `integration` → `marts` → `kpi` → `validation`).
- `src`: pipeline v2 de features, forecasting, anomalías, scoring, escenarios, visualización y QA.
- `data/processed`: tablas analíticas y outputs de decisión.
- `outputs/dashboard`: dashboard oficial único.
- `outputs/reports`: validación, memo ejecutivo y snapshot de release.

## Outputs oficiales para revisión
- Dashboard: `outputs/dashboard/dashboard_inteligencia_red.html`
- Memo ejecutivo: `outputs/reports/memo_ejecutivo_es.md`
- Validación: `outputs/reports/validation_report.md`
- Estado machine-readable: `outputs/reports/validation_summary.json`
- Manifest de release: `outputs/reports/release_manifest.json`

## Cómo ejecutar
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src
```

## Decisiones que habilita
- Dónde reforzar red local por riesgo estructural persistente.
- Dónde activar flexibilidad para diferir CAPEX sin degradar servicio.
- Dónde desplegar almacenamiento por brecha flexible y curtailment.
- Dónde mantener monitorización reforzada antes de invertir.

## Límites del modelo
- El dataset es sintético: válido para diseño de decisión, no sustituto de telemetría real.
- La capa económica usa proxies comparativos, no valoración financiera regulatoria final.
- El motor de escenarios es de estrés/priorización, no optimización eléctrica de detalle.

## Stack
Python, SQL, DuckDB, pandas, matplotlib, Chart.js
