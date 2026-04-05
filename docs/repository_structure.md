# Estructura del Repositorio (Canónica)

## Objetivo
Separar con claridad código fuente, contratos oficiales, artefactos generados y componentes legacy.

## 1) Código fuente
- `src/`:
  - `*_v2.py`: capa canónica de producción analítica.
  - módulos sin sufijo (`pipeline.py`, `sql_runner.py`, etc.): compatibilidad legacy.
- `src/synthetic_generator/`: generador sintético modular por dominio.

## 2) Contratos y lógica oficial
- `sql/01_...10_*.sql`: capa SQL oficial v2 (staging → integration → marts → kpis → validaciones).
- `docs/sql_architecture.md`, `docs/sql_metric_definitions.md`, `docs/metric_dictionary.md`: contrato de métricas y consultas.
- `docs/governance_framework.md`: reglas de release y estados de readiness.

## 3) Legacy aislado
- `sql/legacy/`: scripts SQL históricos no canónicos.

## 4) Datos y salidas generadas
- `data/raw/`: datos sintéticos de entrada.
- `data/processed/`: tablas analíticas y artefactos intermedios/finales.
- `outputs/charts/`: gráficos exportados.
- `outputs/dashboard/dashboard_inteligencia_red.html`: dashboard oficial único.
- `outputs/reports/`: auditorías, validación, memo e informes.
  - incluye `release_manifest.json` como contrato de release verificable.

## 5) Artefactos de trabajo
- `notebooks/`: notebooks de exploración/comunicación.
- `tests/`: contratos y pruebas automatizadas.

## 6) Política de revisión rápida
1. Verificar `outputs/reports/validation_summary.json`.
2. Confirmar `outputs/dashboard/dashboard_inteligencia_red.html` como único dashboard oficial.
3. Revisar `outputs/reports/validation_gate_checks.csv` antes de publicación.
4. Confirmar que `python -m src` regenera `data/raw` y mantiene consistencia de outputs sin intervención manual.
