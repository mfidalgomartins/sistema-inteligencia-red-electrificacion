# Sistema de Inteligencia de Red para Electrificación Territorial

Este proyecto construye un sistema de apoyo a decisión para planificación y operación de red en contexto de electrificación acelerada. El foco no está en “mostrar métricas”, sino en decidir con criterio dónde intervenir primero y con qué combinación de palancas: refuerzo físico, flexibilidad, almacenamiento u operación avanzada.

## Contexto de negocio
La presión sobre la red no se reparte de forma homogénea: hay territorios donde coinciden congestión, crecimiento de demanda, activos más expuestos y menor cobertura flexible. En ese contexto, decidir mal el orden de intervención tiene coste operativo, coste económico y coste de resiliencia.

## Qué resuelve el sistema
Integra un flujo completo de trabajo: generación de datos sintéticos plausibles, capa SQL por niveles, features analíticas, forecasting interpretable, detección de anomalías, scoring multicriterio y simulación de escenarios. El resultado es una salida ejecutiva utilizable para priorizar inversión y operación con trazabilidad técnica.

## Decisiones que habilita
- Dónde escalar refuerzo estructural por riesgo persistente.
- Dónde activar flexibilidad para contener riesgo y diferir CAPEX.
- Dónde desplegar almacenamiento por brecha técnica y curtailment.
- Dónde mantener monitorización reforzada antes de comprometer inversión.

## Arquitectura, en una vista
- `data/raw` y `data/processed`: base sintética y tablas analíticas.
- `sql`: staging, integración, marts, KPIs y validaciones.
- `src`: pipeline v2 de análisis, scoring, escenarios, dashboard y QA.
- `outputs`: pack de visuales, dashboard final y reportes de validación.
- `tests`: checks de contrato y smoke tests.

## Estructura principal
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

## Entregables que importan
- Dashboard ejecutivo: `outputs/dashboard/dashboard_inteligencia_red.html`
- Memo para dirección: `outputs/reports/memo_ejecutivo_es.md`
- Validación integral: `outputs/reports/validation_report.md`
- Manifest de release: `outputs/reports/release_manifest.json`

## Por qué este trabajo está por encima del portfolio medio
Porque está diseñado como sistema de decisión, no como ejercicio de visualización: combina gobierno de métricas, validación explícita, métodos interpretables y una narrativa operativa coherente con contexto utility.

## Ejecución
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src
```

## Límites
Los datos son sintéticos, la capa económica usa proxies comparativos y los escenarios son de priorización (no sustituyen ingeniería eléctrica de detalle ni valoración financiera regulatoria final).

## Herramientas
Python, SQL, DuckDB, pandas, scikit-learn, matplotlib, Chart.js.
