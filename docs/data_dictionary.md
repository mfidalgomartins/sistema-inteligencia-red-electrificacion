# Diccionario de Datos

## Estado de documento
- **Activo**: sí
- **Modelo oficial**: esquema en español de `zonas_red` a `escenario_macro`.

## Raw (`data/raw`)

Cada tabla raw tiene un contrato versionado con sistema origen, estrategia `snapshot|upsert`, clave primaria, columna temporal, nulabilidad y límites numéricos. En landing se añaden `_batch_id`, `_source_contract`, `_source_checksum` y `_ingested_at`; estas columnas de auditoría no forman parte de la interfaz raw promovida.

### `zonas_red.csv`
- `zona_id`: identificador de zona.
- `zona_nombre`: nombre de zona.
- `tipo_zona`, `region_operativa`: segmentación territorial.
- `criticidad_territorial`, `riesgo_climatico`, `tension_crecimiento_demanda`: contexto de riesgo.

### `subestaciones.csv`
- `subestacion_id`, `zona_id`
- `capacidad_mw`, `capacidad_firme_mw`
- `antiguedad_anios`, `indice_criticidad`, `digitalizacion_nivel`, `redundancia_nivel`

### `alimentadores.csv`
- `alimentador_id`, `subestacion_id`
- `tipo_red`, `capacidad_mw`, `longitud_km`
- `nivel_perdidas_estimado`, `exposicion_climatica`, `carga_base_esperada`, `criticidad_operativa`

### `demanda_horaria.csv`
- Grain: `timestamp + zona_id + subestacion_id + alimentador_id`
- `demanda_mw`, `demanda_reactiva_proxy`
- `temperatura`, `humedad`
- `tipo_dia`, `mes`, `hora`, `factor_estacional`
- `hora_punta_flag`, `tension_sistema_proxy`

### `generacion_distribuida.csv`
- Grain: `timestamp + zona_id + tecnologia`
- `capacidad_instalada_mw`, `generacion_mw`
- `autoconsumo_estimado_mw`, `vertido_estimado_mw`, `curtailment_estimado_mw`

### `demanda_ev.csv`
- Grain: `timestamp + zona_id + tipo_recarga`
- `demanda_ev_mw`, `penetracion_ev`, `horario_recarga_dominante`

### `demanda_electrificacion_industrial.csv`
- Grain: `timestamp + zona_id + cluster_industrial`
- `demanda_industrial_adicional_mw`, `perfil_operativo`, `elasticidad_flexibilidad_proxy`

### `eventos_congestion.csv`
- `evento_id`, `timestamp_inicio`, `timestamp_fin`
- `zona_id`, `subestacion_id`, `alimentador_id`
- `severidad`, `energia_afectada_mwh`, `carga_relativa_max`, `impacto_servicio_flag`

### `interrupciones_servicio.csv`
- `interrupcion_id`, `timestamp_inicio`, `timestamp_fin`
- `zona_id`, `subestacion_id`
- `clientes_afectados`, `energia_no_suministrada_mwh`, `causa`, `nivel_severidad`
- `relacion_congestion_flag`

### `activos_red.csv`
- `activo_id`, `tipo_activo`
- `subestacion_id`, `alimentador_id`
- `edad_anios`, `estado_salud`, `probabilidad_fallo_proxy`, `criticidad`
- `capex_reposicion_estimado`, `opex_mantenimiento_estimado`

### `recursos_flexibilidad.csv`
- `recurso_id`, `zona_id`, `tipo_recurso`
- `capacidad_flexible_mw`, `coste_activacion_eur_mwh`
- `tiempo_respuesta_min`, `disponibilidad_media`, `fiabilidad_activacion`, `madurez_operativa`

### `almacenamiento_distribuido.csv`
- `storage_id`, `zona_id`
- `capacidad_energia_mwh`, `capacidad_potencia_mw`
- `eficiencia_roundtrip`, `coste_operacion_proxy`, `disponibilidad_media`

### `intervenciones_operativas.csv`
- `intervencion_id`, `zona_id`
- `tipo_intervencion`, `capacidad_alivio_estimado_mw`
- `coste_estimado`, `tiempo_despliegue_dias`, `complejidad_operativa`

### `inversiones_posibles.csv`
- `inversion_id`, `zona_id`, `tipo_inversion`
- `capex_estimado`, `opex_incremental_estimado`
- `reduccion_riesgo_esperada`, `aumento_capacidad_esperado`
- `horizonte_meses`, `facilidad_implementacion`, `impacto_resiliencia`

### `escenario_macro.csv`
- Grain: `fecha + escenario`
- `crecimiento_demanda_indice`, `penetracion_ev_indice`
- `electrificacion_industrial_indice`, `presion_capex_indice`

## Processed (`data/processed`) - tablas clave
- `mart_node_hour_operational_state.parquet`
- `mart_zone_day_operational.csv`
- `mart_zone_month_operational.csv`
- `node_hour_features.parquet`
- `zone_day_features.csv`
- `zone_month_features.csv`
- `intervention_scoring_table.csv`
- `prioridades_inversion_alimentadores.csv`
- `scenario_impacts.csv` (salida técnica de escenarios)
- `scenario_summary.csv`
- `scenario_priority_ranking.csv`

### `prioridades_inversion_alimentadores.csv`
- Grain: `alimentador_id`.
- Claves de contexto: `zona_id`, `subestacion_id`.
- Señales: carga relativa, ratio de congestión, exposición de activos, probabilidad de fallo y ENS asociada.
- Decisión: `puntuacion_prioridad`, `nivel_prioridad`, `accion_recomendada` y `alivio_requerido_mw`.

### Pronóstico gobernado

- `forecast_rolling_backtest.csv`: modelo, entidad, fold, fecha, cutoff, real, predicción y error.
- `forecast_model_selection_rolling.csv`: ranking por MAE/RMSE, sesgo y observaciones.
- `forecast_uncertainty_intervals.csv`: predicción, límites y cobertura al 80/95% para el fold de evaluación.
- `forecast_calibration_summary.csv`: cobertura nominal/empírica, ancho medio y muestras de calibración/evaluación.
- `forecast_monitoring_status.csv`: modelo elegido, ratio y z-score de deriva MAE, dispersión entre folds, cobertura 95%, ventana y estado operativo.

## Incremental (`data/incremental`)

- `mart_feeder_day/fecha=YYYY-MM-DD/part-00000.parquet`: una fila por alimentador y día.
- `mart_zone_day/fecha=YYYY-MM-DD/part-00000.parquet`: una fila por zona y día.
- Ambas tablas incluyen demanda total, punta, media, horas observadas y completitud.

## Operacional (`data/operational/operations.duckdb`)

- `ingestion_batches`: estado y linaje de cada lote.
- `ingestion_partitions`: ruta, fecha, filas y checksum de cada partición.
- `incremental_runs`: watermark, estado, filas, tiempos y error del refresco diario.
- `decisions`: recomendación, owner, estado, valores esperados/reales, hitos y versión.
- `decision_events`: historial append-only de creación y transición.
