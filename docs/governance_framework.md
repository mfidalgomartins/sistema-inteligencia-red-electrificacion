# Marco de gobernanza y calidad

## Propósito
Establecer un contrato operativo único para ejecución, validación y publicación del sistema analítico.

## 1) Fuente de verdad canónica
- **Flujo oficial**: `python -m grid_intelligence --source-mode synthetic|external`.
- **Reproducibilidad de datos brutos**: el flujo oficial regenera `data/raw` de forma determinista (`generate_synthetic_ecosystem`) antes de SQL, variables analíticas y modelado.
- **SQL oficial**: secuencia `01_staging_core_tables.sql` a `10_validation_queries.sql`.
- **Contratos de fuente**: `src/grid_intelligence/resources/source_contracts.json`.
- **Control operativo**: migraciones versionadas sobre `data/operational/operations.duckdb`.
- **Tablas canónicas**:
  - `mart_node_hour_operational_state`
  - `mart_zone_day_operational`
  - `mart_zone_month_operational`
  - `node_hour_features`
  - `zone_day_features`
  - `zone_month_features`
  - `intervention_scoring_table`
  - `prioridades_inversion_alimentadores`
  - `scenario_impacts`

## 2) Política de calidad
- `validation_summary.json` es el estado oficial legible por máquina.
- Reglas de estado:
  - `PASS`: sin incidencias altas ni medias.
  - `WARN`: al menos una incidencia media y ninguna alta.
  - `FAIL`: al menos una incidencia alta.
- La confianza del proyecto no puede declararse como "alta" con estado `WARN` o `FAIL`.
- En datos sintéticos, `committee_state` no debe declararse apto para comité por defecto.

### Clasificación de preparación (obligatoria en publicación)
Los campos siguientes son contratos técnicos legibles por máquina. En informes y tablero se muestran con etiqueta española.
- `technical_state`: válido técnicamente / no válido técnicamente.
- `analytical_state`: aceptable analíticamente / no aceptable analíticamente.
- `decision_state`: apto como soporte de decisión / solo soporte de decisión / solo cribado preliminar.
- `committee_state`: apto para comité / no apto para comité.
- `publish_state`: publicable / publicable con matices / publicación bloqueada.

Regla operativa:
- Si el conjunto es sintético o aproximado, el estado estándar de publicación es publicable con matices y no apto para comité.

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

## 3.1) Política de ingestión y linaje

- La identidad de un lote es `contract_name + batch_id`; reutilizarla con contenido distinto es un conflicto.
- Cada lote conserva SHA-256 del origen, rango temporal, filas, particiones, checksums y fecha de confirmación.
- Una promoción raw usa la clave primaria del contrato y conserva la versión más reciente por `_ingested_at`.
- El refresco incremental usa `process_date + input_watermark` como identidad idempotente.
- Un replay verifica que las particiones registradas existen y no fueron alteradas.

## 3.2) Pronóstico y monitorización

- El backtest rolling-origin mantiene `cutoff_date < date` para toda predicción evaluada.
- La selección de modelo usa MAE y RMSE sobre múltiples folds.
- Los intervalos al 80% y 95% se calibran con residuos anteriores al fold evaluado.
- `forecast_monitoring_status.csv` clasifica deriva y cobertura como `stable`, `watch` o `action_required`; la severidad combina ratio MAE, desviación entre folds y cobertura 95%.

## 3.3) Manifiesto de publicación
- Artefacto obligatorio: `outputs/reports/release_manifest.json`.
- Debe incluir hash (`sha256`) del tablero, rankings, calibración y contratos de fuente.
- `validation_summary.json` y `release_manifest.json` deben ser consistentes en:
  - `overall_status` ↔ `validation_status`
  - `release_readiness.publish_state`

## 4) Política mínima de pruebas
- Cobertura mínima por dominio:
  - punto de entrada canónico
  - contratos de puntuación (niveles/urgencia)
  - contrato de escenarios
  - contrato de secuencia SQL canónica
  - evaluación de severidad en validación
  - priorización de alimentadores
- El gate de release combina pruebas unitarias y pipeline completo, con cobertura mínima del 80% sobre `src/`.
- Cualquier PR sin lint, formato, compilación, cobertura y puertas de publicación en verde queda bloqueado.

## 5) Puertas bloqueantes de publicación
La publicación queda bloqueada si falla cualquier puerta bloqueante:
1. tablero oficial no existe.
2. artefactos centrales (`intervention_scoring_table`, `intervention_ranking_final`, `prioridades_inversion_alimentadores`, escenarios) no existen.
3. inconsistencia entre ranking final y puntuación líder.
4. inconsistencia de agregación de coste entre `scenario_impacts` y `scenario_summary`.
5. todos los escenarios producen el mismo ranking.
6. incidencias `alta` > 0.
7. contratos de fuente ausentes, duplicados o inválidos.
8. intervalos de pronóstico incoherentes o estado de monitorización inconsistente.

## 6) Criterio de publicación
Antes de publicar:
1. Ejecutar `make release`.
2. Revisar `outputs/reports/validation_report.md`.
3. Confirmar que `publish_state` no indique publicación bloqueada.
4. Confirmar consistencia narrativa entre README, tablero e informe analítico.
