# Governance & Quality Framework (v2)

## Propósito
Establecer un contrato operativo único para evitar ambigüedad entre artefactos legacy y la arquitectura v2 canónica.

## 1) Fuente de verdad canónica
- **Pipeline oficial**: `python -m src` (ejecuta `run_final_assembly_v2`).
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

## 2) Artefactos legacy (solo compatibilidad)
Los siguientes componentes permanecen para trazabilidad histórica y no deben usarse como fuente principal:
- `src/pipeline.py`, `src/sql_runner.py`, `src/scoring.py`, `src/scenario_engine.py`
- SQL legacy: `archive/sql_legacy/00_load_raw.sql`, `archive/sql_legacy/10_staging.sql`, `archive/sql_legacy/20_marts.sql`, `archive/sql_legacy/30_kpis.sql`, `archive/sql_legacy/40_validations.sql`
- Datasets legacy en `data/raw` con naming inglés (`territories`, `feeders`, etc.)

## 3) Política de calidad
- `validation_summary.json` es el estado machine-readable oficial.
- Reglas de estado:
  - `PASS`: sin issues alta/media.
  - `WARN`: >=1 issue media y 0 alta.
  - `FAIL`: >=1 issue alta.
- La confianza del proyecto no puede declararse como "alta" con estado `WARN` o `FAIL`.

### Clasificación de readiness (obligatoria en release)
- `technical_state`: `technically valid` / `not technically valid`
- `analytical_state`: `analytically acceptable` / `not analytically acceptable`
- `decision_state`: `decision-support ready` / `decision-support only` / `screening-grade only`
- `committee_state`: `committee-grade` / `not committee-grade`
- `publish_state`: `publish-ready` / `publish-with-caveats` / `publish-blocked`

Los estados se publican en:
- `outputs/reports/validation_report.md`
- `outputs/reports/validation_summary.json`
- `outputs/reports/validation_gate_checks.csv`

## 4) Política de métricas
- Definiciones oficiales en:
  - `docs/sql_metric_definitions.md`
  - `docs/scoring_framework_v2.md`
- Cualquier métrica no incluida en estas fuentes se considera no gobernada.

## 4.1) Release manifest
- Artefacto obligatorio: `outputs/reports/release_manifest.json`.
- Debe incluir hash (`sha256`) de dashboard oficial y estado de readiness.
- `validation_summary.json` y `release_manifest.json` deben ser consistentes en:
  - `overall_status` ↔ `validation_status`
  - `release_readiness.publish_state`

## 5) Política de testing mínima
- Cobertura mínima por dominio:
  - entrypoint canónico
  - contratos de scoring (tiers/urgencia)
  - contrato de escenarios
  - contrato de secuencia SQL v2
  - evaluación de severidad en validación
- Cualquier PR o release sin `pytest` verde queda bloqueado.

## 6) Hard gates de release
El release queda bloqueado si falla cualquier gate bloqueante:
1. dashboard oficial no existe.
2. artefactos core (`intervention_scoring_table`, `intervention_ranking_final`, escenarios) no existen.
3. inconsistencia entre ranking final y score top.
4. inconsistencia de agregación de coste entre `scenario_impacts_v2` y `scenario_summary_v2`.
5. issues `alta` > 0.

## 7) Criterio de publicación
Antes de publicar:
1. Ejecutar `python -m src`.
2. Ejecutar `pytest -q`.
3. Revisar `outputs/reports/validation_report.md`.
4. Confirmar que `publish_state` no sea `publish-blocked`.
5. Confirmar consistencia narrativa en README + memo ejecutivo.
