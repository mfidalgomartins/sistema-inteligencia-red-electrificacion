# Sistema de Inteligencia de Red para Electrificación Territorial

Sistema analítico de apoyo a decisión para priorizar intervenciones de red entre refuerzo, flexibilidad, almacenamiento y operación.

## Problema de negocio
La red de distribución pierde capacidad operativa en territorios concretos por congestión, electrificación de demanda, activos envejecidos y límites locales. La decisión crítica no es solo *dónde* intervenir, sino *con qué palanca* y *en qué secuencia*.

## Qué hace el sistema
- Genera un ecosistema sintético realista multi-zona y multi-nodo.
- Integra datos y KPIs con una capa SQL por niveles.
- Construye features, forecasting interpretable, detección de anomalías y scoring multicriterio.
- Ejecuta escenarios what-if para comparar impacto técnico y económico.
- Entrega dashboard ejecutivo y reportes de validación para comité.

## Decisiones que soporta
- Qué zonas requieren refuerzo estructural inmediato.
- Dónde conviene activar flexibilidad para diferir CAPEX.
- Dónde el almacenamiento aporta mayor reducción de riesgo.
- Qué focos deben quedar en monitorización reforzada antes de invertir.

## Arquitectura del proyecto
- `data/raw`: datos sintéticos base.
- `sql`: staging, integración, marts, KPIs y validaciones.
- `src`: pipeline analítico v2 (features, modelos, scoring, escenarios, dashboard, QA).
- `data/processed`: tablas analíticas y salidas de decisión.
- `outputs`: charts, dashboard y reportes finales.
- `tests`: checks de contrato y smoke tests.

## Estructura del repositorio
```text
src/
sql/
data/
outputs/
docs/
tests/
notebooks/
scripts/
```

## Outputs clave
- Dashboard final: `outputs/dashboard/dashboard_inteligencia_red.html`
- Memo ejecutivo: `outputs/reports/memo_ejecutivo_es.md`
- Validación integral: `outputs/reports/validation_report.md`
- Estado de release: `outputs/reports/release_manifest.json`

## Por qué este proyecto es fuerte
- Está orientado a decisión operativa e inversión, no a visualización decorativa.
- Mantiene trazabilidad de extremo a extremo: dato → métrica → score → recomendación.
- Usa métodos interpretables y gobierno explícito de calidad/validación.
- Incluye una capa ejecutiva utilizable en contexto real de utility.

## Cómo ejecutar
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src
```

## Limitaciones
- Datos sintéticos: sirven para diseño y comparación de decisiones, no sustituyen operación real.
- Capa económica basada en proxies: no reemplaza evaluación financiera regulatoria.
- Escenarios de priorización: no equivalen a ingeniería eléctrica de detalle.

## Herramientas
Python, SQL, DuckDB, pandas, scikit-learn, matplotlib, Chart.js.
