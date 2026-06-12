# Governance & Quality Framework

## Propósito
Establecer un contrato operativo único para ejecución, validación y publicación del sistema analítico.

## 1) Fuente de verdad canónica
- **Pipeline oficial**: `python -m src` (ejecuta `run_pipeline`).
- **Reproducibilidad raw**: el pipeline oficial regenera `data/raw` de forma determinista (`generate_synthetic_ecosystem`) antes de SQL/feature/modelado.
- **SQL oficial**: secuencia `01_staging_core_tables.sql` a `10_validation_queries.sql`.
- **Tablas canónicas**:
  - `mart_node_hour_operational_state`
  - `mart_zone_day_operational`
  - `mart_zone_month_operational`
  - `node_hour_features`
  - `zone_day_features`
  - `zone_month_features`
  - `intervention_scoring_table`
  - `scenario_impacts_v2`

## 2) Política de calidad
- `validation_summary.json` es el estado machine-readable oficial.
- Reglas de estado:
  - `PASS`: sin issues alta/media.
  - `WARN`: >=1 issue media y 0 alta.
  - `FAIL`: >=1 issue alta.
- La confianza del proyecto no puede declararse como "alta" con estado `WARN` o `FAIL`.
- En datos sintéticos, `committee_state` no debe declararse `committee-grade` por defecto.

### Clasificación de readiness (obligatoria en release)
- `technical_state`: `technically valid` / `not technically valid`
- `analytical_state`: `analytically acceptable` / `not analytically acceptable`
- `decision_state`: `decision-support ready` / `decision-support only` / `screening-grade only`
- `committee_state`: `committee-grade` / `not committee-grade`
- `publish_state`: `publish-ready` / `publish-with-caveats` / `publish-blocked`

Regla operativa:
- Si el conjunto es sintético o proxy, el release estándar es `publish-with-caveats` y `not committee-grade`.

Los estados se publican en:
- `outputs/reports/validation_report.md`
- `outputs/reports/validation_summary.json`

Estos diagnósticos son regenerables y no se versionan. Los artefactos públicos versionados son:
- `outputs/dashboard/grid-electrification-command-center.html`
- `outputs/reports/informe_analitico_red_electrificacion.pdf`
- `outputs/graphs/*.png`

## 3) Política de métricas
- Definiciones oficiales en:
  - `docs/sql_metric_definitions.md`
  - `docs/scoring_framework.md`
- Cualquier métrica no incluida en estas fuentes se considera no gobernada.

## 3.1) Release manifest
- Artefacto obligatorio: `outputs/reports/release_manifest.json`.
- Debe incluir hash (`sha256`) de dashboard oficial y estado de readiness.
- `validation_summary.json` y `release_manifest.json` deben ser consistentes en:
  - `overall_status` ↔ `validation_status`
  - `release_readiness.publish_state`

## 4) Política de testing mínima
- Cobertura mínima por dominio:
  - entrypoint canónico
  - contratos de scoring (tiers/urgencia)
  - contrato de escenarios
  - contrato de secuencia SQL canónica
  - evaluación de severidad en validación
- Cualquier PR o release sin `pytest` verde queda bloqueado.

## 5) Hard gates de release
El release queda bloqueado si falla cualquier gate bloqueante:
1. dashboard oficial no existe.
2. artefactos core (`intervention_scoring_table`, `intervention_ranking_final`, escenarios) no existen.
3. inconsistencia entre ranking final y score top.
4. inconsistencia de agregación de coste entre `scenario_impacts_v2` y `scenario_summary_v2`.
5. todos los escenarios producen el mismo ranking.
6. issues `alta` > 0.

## 6) Criterio de publicación
Antes de publicar:
1. Ejecutar `make release`.
2. Revisar `outputs/reports/validation_report.md`.
3. Confirmar que `publish_state` no sea `publish-blocked`.
4. Confirmar consistencia narrativa entre README, dashboard e informe analítico.
