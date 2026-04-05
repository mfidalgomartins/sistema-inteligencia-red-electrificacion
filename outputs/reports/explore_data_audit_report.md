# Explore-Data Audit | Sistema de Inteligencia de Red

## Resumen Ejecutivo
- Datasets auditados: **15**
- Filas totales auditadas: **7,850,237**
- Issues priorizados: **4**

## Tabla Resumen por Dataset
| table                              | grain                                        | pk_candidate                                       | expected_fks                                                                                                                    |    rows |   columns | period_start        | period_end          |   key_null_rows |   duplicate_rows_on_key |   null_rate_max_column |   n_numeric_cols |   n_categorical_cols |
|:-----------------------------------|:---------------------------------------------|:---------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------|--------:|----------:|:--------------------|:--------------------|----------------:|------------------------:|-----------------------:|-----------------:|---------------------:|
| activos_red                        | 1 fila por activo                            | activo_id                                          | subestacion_id->subestaciones.subestacion_id; alimentador_id->alimentadores.alimentador_id (nullable en activos de subestacion) |    1053 |        10 | NaT                 | NaT                 |               0 |                       0 |               0.219373 |                6 |                    4 |
| alimentadores                      | 1 fila por alimentador                       | alimentador_id                                     | subestacion_id->subestaciones.subestacion_id                                                                                    |     274 |         9 | NaT                 | NaT                 |               0 |                       0 |               0        |                6 |                    3 |
| almacenamiento_distribuido         | 1 fila por sistema de almacenamiento         | storage_id                                         | zona_id->zonas_red.zona_id                                                                                                      |      31 |         7 | NaT                 | NaT                 |               0 |                       0 |               0        |                5 |                    2 |
| demanda_electrificacion_industrial | 1 fila por hora-zona-cluster_industrial      | timestamp, zona_id, cluster_industrial             | zona_id->zonas_red.zona_id                                                                                                      |  421056 |         6 | 2024-01-01 00:00:00 | 2025-12-31 23:00:00 |               0 |                       0 |               0        |                2 |                    3 |
| demanda_ev                         | 1 fila por hora-zona-tipo_recarga            | timestamp, zona_id, tipo_recarga                   | zona_id->zonas_red.zona_id                                                                                                      | 1263168 |         6 | 2024-01-01 00:00:00 | 2025-12-31 23:00:00 |               0 |                       0 |               0        |                2 |                    3 |
| demanda_horaria                    | 1 fila por hora-zona-subestacion-alimentador | timestamp, zona_id, subestacion_id, alimentador_id | zona_id->zonas_red.zona_id; subestacion_id->subestaciones.subestacion_id; alimentador_id->alimentadores.alimentador_id          | 4807056 |        14 | 2024-01-01 00:00:00 | 2025-12-31 23:00:00 |               0 |                       0 |               0        |                9 |                    4 |
| escenario_macro                    | 1 fila por fecha-escenario                   | fecha, escenario                                   | -                                                                                                                               |    2924 |         6 | 2024-01-01          | 2025-12-31          |               0 |                       0 |               0        |                4 |                    1 |
| eventos_congestion                 | 1 fila por evento de congestion              | evento_id                                          | zona_id->zonas_red.zona_id; subestacion_id->subestaciones.subestacion_id; alimentador_id->alimentadores.alimentador_id          |   88855 |        11 | 2024-01-01 07:00:00 | 2025-12-31 21:00:00 |               0 |                       0 |               0        |                3 |                    6 |
| generacion_distribuida             | 1 fila por hora-zona-tecnologia              | timestamp, zona_id, tecnologia                     | zona_id->zonas_red.zona_id                                                                                                      | 1263168 |         8 | 2024-01-01 00:00:00 | 2025-12-31 23:00:00 |               0 |                       0 |               0        |                5 |                    2 |
| interrupciones_servicio            | 1 fila por interrupcion de servicio          | interrupcion_id                                    | zona_id->zonas_red.zona_id; subestacion_id->subestaciones.subestacion_id                                                        |    2222 |        10 | 2024-01-01 07:00:00 | 2025-12-31 02:00:00 |               0 |                       0 |               0        |                3 |                    5 |
| intervenciones_operativas          | 1 fila por intervencion operativa catalogada | intervencion_id                                    | zona_id->zonas_red.zona_id                                                                                                      |     120 |         7 | NaT                 | NaT                 |               0 |                       0 |               0        |                4 |                    3 |
| inversiones_posibles               | 1 fila por opcion de inversion               | inversion_id                                       | zona_id->zonas_red.zona_id                                                                                                      |     144 |        10 | NaT                 | NaT                 |               0 |                       0 |               0        |                7 |                    3 |
| recursos_flexibilidad              | 1 fila por recurso de flexibilidad           | recurso_id                                         | zona_id->zonas_red.zona_id                                                                                                      |      65 |         9 | NaT                 | NaT                 |               0 |                       0 |               0        |                6 |                    3 |
| subestaciones                      | 1 fila por subestacion                       | subestacion_id                                     | zona_id->zonas_red.zona_id                                                                                                      |      77 |         9 | NaT                 | NaT                 |               0 |                       0 |               0        |                6 |                    3 |
| zonas_red                          | 1 fila por zona de red                       | zona_id                                            | -                                                                                                                               |      24 |        12 | NaT                 | NaT                 |               0 |                       0 |               0        |                6 |                    6 |

## Hallazgos Cross-Dataset
- Over-capacity rate demanda vs capacidad: **4.5388%**
- Max load ratio demanda/capacidad: **1.420x**
- Interrupciones marcadas con congestión sin solape: **1080** (76.60%)
- Correlación congestión vs ENS por subestación: **0.4918**

## Issues Priorizados
| severity   | issue_type                   | table_scope                                  | description                                                                                      | impact                                                                      | recommendation                                                                    | observed_value   |
|:-----------|:-----------------------------|:---------------------------------------------|:-------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------|:----------------------------------------------------------------------------------|:-----------------|
| P2         | congestion_vs_interrupciones | eventos_congestion + interrupciones_servicio | Interrupciones marcadas como relacionadas con congestión sin solape temporal suficiente.         | Debilita trazabilidad causal en análisis diagnóstico.                       | Ajustar regla de relacion_congestion_flag y ventana temporal oficial.             | 0.765957         |
| P2         | inconsistencia_logica        | eventos_congestion                           | Check fallido: energia_o_carga_relativa_invalidas                                                | Puede romper series temporales o reglas de negocio.                         | Corregir en generador/staging y añadir test de regresión.                         | 28788            |
| P3         | redundancia_derivada         | demanda_horaria                              | Columnas `mes`, `hora`, `tipo_dia` y `factor_estacional` son derivables de timestamp/calendario. | Riesgo de inconsistencias si se recalculan distinto entre capas.            | Tratar como campos derivados en marts; usar timestamp como única verdad temporal. | aplica           |
| P3         | riesgo_modelado              | escenario_macro                              | Serie macro diaria requiere expansión horaria controlada para forecasting horario.               | Si se mezcla granularidad diaria/horaria sin cuidado, hay leakage temporal. | Crear feature store con reglas explícitas de forward-fill y lag temporal.         | aplica           |

## Recomendaciones para Transformación Analítica
| priority   | topic                       | recommendation                                                                                            | impact                                                   |
|:-----------|:----------------------------|:----------------------------------------------------------------------------------------------------------|:---------------------------------------------------------|
| Alta       | Normalizacion temporal      | Construir dim_calendario y recalcular mes/hora/tipo_dia en staging para evitar inconsistencias derivadas. | Mejora calidad para forecasting y dashboards temporales. |
| Alta       | Contratos de datos          | Definir contratos de llaves (NOT NULL/UNIQUE) por tabla en SQL staging.                                   | Reduce riesgo de joins incompletos en scoring.           |
| Alta       | Reglas de integridad evento | Validar siempre timestamp_fin >= timestamp_inicio y relaciones de causalidad congestion-interrupcion.     | Fortalece análisis de resiliencia y continuidad.         |
| Media      | Gestión de outliers         | Aplicar winsorization/flags para extremos en demanda_mw, energia_afectada_mwh y ENS antes de modelado.    | Mayor robustez en anomaly detection.                     |
| Media      | Feature governance          | Separar features de estado actual vs derivadas de escenario para evitar leakage en modelos predictivos.   | Mejora validez de forecast y escenario analysis.         |
| Media      | Semántica de métricas       | Documentar definición oficial de curtailment, vertido y energía afectada para coherencia de KPI.          | Evita interpretaciones divergentes en comité ejecutivo.  |

## Propuesta de Joins Oficiales
| join_name                              | left_table                    | right_table                                                                    | join_keys                                                     | join_type   | purpose                                           |
|:---------------------------------------|:------------------------------|:-------------------------------------------------------------------------------|:--------------------------------------------------------------|:------------|:--------------------------------------------------|
| topologia_zona_subestacion_alimentador | subestaciones                 | zonas_red                                                                      | subestaciones.zona_id = zonas_red.zona_id                     | INNER       | Contexto territorial y operativo de nodos         |
| demanda_con_capacidad_alimentador      | demanda_horaria               | alimentadores                                                                  | demanda_horaria.alimentador_id = alimentadores.alimentador_id | INNER       | Calcular utilizacion, sobrecargas y perdidas      |
| demanda_gd_por_zona_hora               | demanda_horaria_agg_zona_hora | generacion_distribuida_agg_zona_hora                                           | zona_id + timestamp                                           | INNER       | Balance neto, vertido y curtailment               |
| demanda_componentes_ev_industrial      | demanda_horaria_agg_zona_hora | demanda_ev_agg + demanda_electrificacion_industrial_agg                        | zona_id + timestamp                                           | LEFT        | Descomponer drivers de pico                       |
| congestion_con_interrupciones          | eventos_congestion            | interrupciones_servicio                                                        | subestacion_id + solape temporal                              | LEFT        | Analisis impacto continuidad asociado a estres    |
| riesgo_activos_con_eventos             | activos_red                   | eventos_congestion/interrupciones_servicio                                     | subestacion_id y opcionalmente alimentador_id                 | LEFT        | Modelos de riesgo y priorizacion de mantenimiento |
| capacidad_alivio_operativo             | zonas_red                     | recursos_flexibilidad + almacenamiento_distribuido + intervenciones_operativas | zona_id                                                       | LEFT        | Analizar mitigacion no-fisica de congestion       |
| roadmap_inversiones                    | inversiones_posibles          | zonas_red + escenario_macro                                                    | zona_id y fecha/escenario para stress testing                 | LEFT        | Priorizacion tecnico-economica de cartera         |

## Tablas Candidatas para Marts Analíticos
| mart_name                     | grain                      | sources                                                                                    | core_metrics                                                               | use_cases                                            |
|:------------------------------|:---------------------------|:-------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------|:-----------------------------------------------------|
| mart_operacion_horaria_feeder | timestamp + alimentador_id | demanda_horaria + alimentadores + subestaciones + zonas_red                                | demanda_mw, demanda_reactiva_proxy, utilization, tension_sistema_proxy     | monitoreo operativo, anomalias, forecasting intradia |
| mart_balance_zona_horario     | timestamp + zona_id        | demanda_horaria agg + generacion_distribuida agg + demanda_ev agg + demanda_industrial agg | demanda_total, gd_total, net_load, curtailment, share_ev, share_industrial | planeamiento de capacidad y hosting                  |
| mart_congestion_eventos       | evento_id                  | eventos_congestion + topologia + capacidad flex/storage por zona                           | severidad, energia_afectada_mwh, carga_relativa_max, impacto_servicio_flag | diagnostico de estres y mitigacion                   |
| mart_continuidad_servicio     | interrupcion_id            | interrupciones_servicio + eventos_congestion + activos_red                                 | duracion_h, ens_mwh, clientes_afectados, relacion_congestion_flag          | SAIDI/SAIFI proxy y resiliencia                      |
| mart_riesgo_activos           | activo_id                  | activos_red + topologia + eventos/interrupciones agregados                                 | edad, estado_salud, probabilidad_fallo_proxy, exposicion_eventos           | priorizacion de reemplazo y mantenimiento            |
| mart_portafolio_inversion     | zona_id + tipo_inversion   | inversiones_posibles + intervenciones_operativas + escenario_macro + riesgos agregados     | capex, opex, reduccion_riesgo, facilidad, impacto_resiliencia              | scoring multicriterio y comite de inversiones        |

## Clasificación de Columnas
| table                              | classification   |   n_columnas |
|:-----------------------------------|:-----------------|-------------:|
| activos_red                        | estructurales    |            1 |
| activos_red                        | identificadores  |            3 |
| activos_red                        | metricas         |            6 |
| alimentadores                      | estructurales    |            1 |
| alimentadores                      | identificadores  |            2 |
| alimentadores                      | metricas         |            6 |
| almacenamiento_distribuido         | identificadores  |            2 |
| almacenamiento_distribuido         | metricas         |            5 |
| demanda_electrificacion_industrial | estructurales    |            2 |
| demanda_electrificacion_industrial | identificadores  |            1 |
| demanda_electrificacion_industrial | metricas         |            2 |
| demanda_electrificacion_industrial | temporales       |            1 |
| demanda_ev                         | estructurales    |            2 |
| demanda_ev                         | identificadores  |            1 |
| demanda_ev                         | metricas         |            2 |
| demanda_ev                         | temporales       |            1 |
| demanda_horaria                    | booleanas        |            1 |
| demanda_horaria                    | estructurales    |            1 |
| demanda_horaria                    | identificadores  |            3 |
| demanda_horaria                    | metricas         |            6 |
| demanda_horaria                    | temporales       |            3 |
| escenario_macro                    | dimensiones      |            1 |
| escenario_macro                    | metricas         |            4 |
| escenario_macro                    | temporales       |            1 |
| eventos_congestion                 | booleanas        |            1 |
| eventos_congestion                 | estructurales    |            2 |
| eventos_congestion                 | identificadores  |            4 |
| eventos_congestion                 | metricas         |            2 |
| eventos_congestion                 | temporales       |            2 |
| generacion_distribuida             | estructurales    |            1 |
| generacion_distribuida             | identificadores  |            1 |
| generacion_distribuida             | metricas         |            5 |
| generacion_distribuida             | temporales       |            1 |
| interrupciones_servicio            | booleanas        |            1 |
| interrupciones_servicio            | estructurales    |            2 |
| interrupciones_servicio            | identificadores  |            3 |
| interrupciones_servicio            | metricas         |            2 |
| interrupciones_servicio            | temporales       |            2 |
| intervenciones_operativas          | estructurales    |            1 |
| intervenciones_operativas          | identificadores  |            2 |
| intervenciones_operativas          | metricas         |            4 |
| inversiones_posibles               | estructurales    |            1 |
| inversiones_posibles               | identificadores  |            2 |
| inversiones_posibles               | metricas         |            7 |
| recursos_flexibilidad              | estructurales    |            1 |
| recursos_flexibilidad              | identificadores  |            2 |
| recursos_flexibilidad              | metricas         |            6 |
| subestaciones                      | estructurales    |            1 |
| subestaciones                      | identificadores  |            2 |
| subestaciones                      | metricas         |            6 |
| zonas_red                          | estructurales    |            5 |
| zonas_red                          | identificadores  |            1 |
| zonas_red                          | metricas         |            6 |

## Perfil Detallado por Tabla
### zonas_red
- Grain: `1 fila por zona de red`
- Candidate key: `zona_id`
- Foreign keys esperadas: `-`
- Filas/Columnas: `24` / `12`
- Cobertura temporal: `NaT` -> `NaT`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                             |   null_count |   null_rate |
|:-----------------------------------|-------------:|------------:|
| zona_id                            |            0 |           0 |
| zona_nombre                        |            0 |           0 |
| comunidad_autonoma                 |            0 |           0 |
| provincia                          |            0 |           0 |
| tipo_zona                          |            0 |           0 |
| region_operativa                   |            0 |           0 |
| densidad_demanda                   |            0 |           0 |
| penetracion_generacion_distribuida |            0 |           0 |
- Cardinalidad categórica (muestra):
| column             |   n_unique | top_values                                                               |
|:-------------------|-----------:|:-------------------------------------------------------------------------|
| zona_id            |         24 | Z013:1; Z015:1; Z014:1                                                   |
| zona_nombre        |         24 | Zona Valencia urbana:1; Zona Bizkaia rural:1; Zona Asturias industrial:1 |
| comunidad_autonoma |         16 | Andalucia:3; Castilla y Leon:2; Castilla-La Mancha:2                     |
| provincia          |         24 | Bizkaia:1; Pontevedra:1; Cantabria:1                                     |
| tipo_zona          |          4 | industrial:8; urbana:8; rural:5                                          |
| region_operativa   |          8 | Norte:8; Centro:3; Sur:3                                                 |
- Distribuciones numéricas (muestra):
| column                             |    min |     p50 |     p95 |    max |    mean |
|:-----------------------------------|-------:|--------:|--------:|-------:|--------:|
| densidad_demanda                   | 0.2178 | 0.69255 | 0.96822 | 0.99   | 0.64382 |
| penetracion_generacion_distribuida | 0.163  | 0.3859  | 0.61734 | 0.6458 | 0.38407 |
| criticidad_territorial             | 0.6128 | 0.80635 | 0.99061 | 1      | 0.79963 |
| potencial_flexibilidad             | 0.341  | 0.6473  | 0.77812 | 0.7961 | 0.63448 |
| riesgo_climatico                   | 0.4274 | 0.58745 | 0.87046 | 0.8854 | 0.61772 |
| tension_crecimiento_demanda        | 0.355  | 0.7355  | 0.86429 | 0.9473 | 0.71151 |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### subestaciones
- Grain: `1 fila por subestacion`
- Candidate key: `subestacion_id`
- Foreign keys esperadas: `zona_id->zonas_red.zona_id`
- Filas/Columnas: `77` / `9`
- Cobertura temporal: `NaT` -> `NaT`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column               |   null_count |   null_rate |
|:---------------------|-------------:|------------:|
| subestacion_id       |            0 |           0 |
| zona_id              |            0 |           0 |
| nombre_subestacion   |            0 |           0 |
| capacidad_mw         |            0 |           0 |
| capacidad_firme_mw   |            0 |           0 |
| antiguedad_anios     |            0 |           0 |
| indice_criticidad    |            0 |           0 |
| digitalizacion_nivel |            0 |           0 |
- Cardinalidad categórica (muestra):
| column             |   n_unique | top_values                                         |
|:-------------------|-----------:|:---------------------------------------------------|
| subestacion_id     |         77 | S0006:1; S0018:1; S0070:1                          |
| zona_id            |         24 | Z007:4; Z017:4; Z014:4                             |
| nombre_subestacion |         77 | SE_SEVILLA_02:1; SE_MALAGA_03:1; SE_BARCELONA_02:1 |
- Distribuciones numéricas (muestra):
| column               |     min |      p50 |       p95 |      max |      mean |
|:---------------------|--------:|---------:|----------:|---------:|----------:|
| capacidad_mw         | 35      | 165.394  | 279.04    | 322.484  | 158.144   |
| capacidad_firme_mw   | 24.904  | 128.573  | 230.451   | 259.541  | 125.66    |
| antiguedad_anios     | 12      |  33      |  51.2     |  58      |  33.5325  |
| indice_criticidad    |  0.6096 |   0.7692 |   0.88328 |   0.9793 |   0.77296 |
| digitalizacion_nivel |  0.4355 |   0.7001 |   0.82336 |   0.951  |   0.68607 |
| redundancia_nivel    |  0.2849 |   0.5966 |   0.78148 |   0.8347 |   0.57964 |
- Coherencia lógica (checks):
| check                           |   failed_rows |
|:--------------------------------|--------------:|
| capacidad_firme_mayor_capacidad |             0 |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### alimentadores
- Grain: `1 fila por alimentador`
- Candidate key: `alimentador_id`
- Foreign keys esperadas: `subestacion_id->subestaciones.subestacion_id`
- Filas/Columnas: `274` / `9`
- Cobertura temporal: `NaT` -> `NaT`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                  |   null_count |   null_rate |
|:------------------------|-------------:|------------:|
| alimentador_id          |            0 |           0 |
| subestacion_id          |            0 |           0 |
| tipo_red                |            0 |           0 |
| capacidad_mw            |            0 |           0 |
| longitud_km             |            0 |           0 |
| nivel_perdidas_estimado |            0 |           0 |
| exposicion_climatica    |            0 |           0 |
| carga_base_esperada     |            0 |           0 |
- Cardinalidad categórica (muestra):
| column         |   n_unique | top_values                                      |
|:---------------|-----------:|:------------------------------------------------|
| alimentador_id |        274 | A00004:1; A00012:1; A00017:1                    |
| subestacion_id |         77 | S0022:4; S0039:4; S0018:4                       |
| tipo_red       |          7 | mixta:88; mallada_urbana:49; aerea_reforzada:49 |
- Distribuciones numéricas (muestra):
| column                  |     min |      p50 |       p95 |       max |     mean |
|:------------------------|--------:|---------:|----------:|----------:|---------:|
| capacidad_mw            | 6.5     | 32.362   | 110.093   | 162.749   | 43.006   |
| longitud_km             | 2.82    | 13.602   |  32.7311  |  55.824   | 15.3512  |
| nivel_perdidas_estimado | 0.02818 |  0.05932 |   0.1023  |   0.15307 |  0.06419 |
| exposicion_climatica    | 0.4283  |  0.65395 |   0.88184 |   0.99    |  0.66722 |
| carga_base_esperada     | 3.441   | 23.677   |  82.9365  | 123.23    | 31.1303  |
| criticidad_operativa    | 0.6662  |  0.807   |   0.91694 |   0.9845  |  0.81026 |
- Coherencia lógica (checks):
| check                            |   failed_rows |
|:---------------------------------|--------------:|
| carga_base_excesiva_vs_capacidad |             0 |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### demanda_horaria
- Grain: `1 fila por hora-zona-subestacion-alimentador`
- Candidate key: `timestamp, zona_id, subestacion_id, alimentador_id`
- Foreign keys esperadas: `zona_id->zonas_red.zona_id; subestacion_id->subestaciones.subestacion_id; alimentador_id->alimentadores.alimentador_id`
- Filas/Columnas: `4,807,056` / `14`
- Cobertura temporal: `2024-01-01 00:00:00` -> `2025-12-31 23:00:00`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                 |   null_count |   null_rate |
|:-----------------------|-------------:|------------:|
| timestamp              |            0 |           0 |
| zona_id                |            0 |           0 |
| subestacion_id         |            0 |           0 |
| alimentador_id         |            0 |           0 |
| demanda_mw             |            0 |           0 |
| demanda_reactiva_proxy |            0 |           0 |
| temperatura            |            0 |           0 |
| humedad                |            0 |           0 |
- Cardinalidad categórica (muestra):
| column         |   n_unique | top_values                                            |
|:---------------|-----------:|:------------------------------------------------------|
| zona_id        |         24 | Z007:280704; Z024:280704; Z001:280704                 |
| subestacion_id |         77 | S0038:70176; S0058:70176; S0052:70176                 |
| alimentador_id |        274 | A00121:17544; A00123:17544; A00202:17544              |
| tipo_dia       |          3 | laborable:3353760; fin_semana:1334928; festivo:118368 |
- Distribuciones numéricas (muestra):
| column                 |      min |      p50 |      p95 |       max |     mean |
|:-----------------------|---------:|---------:|---------:|----------:|---------:|
| demanda_mw             |  1.19147 | 18.628   | 75.6554  | 191.183   | 26.4903  |
| demanda_reactiva_proxy |  0.16784 |  3.8599  | 17.822   |  58.9967  |  5.86364 |
| temperatura            | -4.2425  | 14.3467  | 25.388   |  34.3399  | 14.3074  |
| humedad                | 22.3198  | 62.0217  | 84.2202  |  98       | 62.0004  |
| mes                    |  1       |  7       | 12       |  12       |  6.51984 |
| hora                   |  0       | 11.5     | 22       |  23       | 11.5     |
| factor_estacional      |  0.955   |  1.00749 |  1.09881 |   1.10001 |  1.02087 |
| hora_punta_flag        |  0       |  0       |  1       |   1       |  0.2474  |
| tension_sistema_proxy  |  0.91624 |  0.98897 |  1.0119  |   1.04584 |  0.98767 |
- Coherencia lógica (checks):
| check                          |   failed_rows |
|:-------------------------------|--------------:|
| hora_no_coincide_con_timestamp |             0 |
| mes_no_coincide_con_timestamp  |             0 |
| metricas_negativas_imposibles  |             0 |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### generacion_distribuida
- Grain: `1 fila por hora-zona-tecnologia`
- Candidate key: `timestamp, zona_id, tecnologia`
- Foreign keys esperadas: `zona_id->zonas_red.zona_id`
- Filas/Columnas: `1,263,168` / `8`
- Cobertura temporal: `2024-01-01 00:00:00` -> `2025-12-31 23:00:00`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                  |   null_count |   null_rate |
|:------------------------|-------------:|------------:|
| timestamp               |            0 |           0 |
| zona_id                 |            0 |           0 |
| tecnologia              |            0 |           0 |
| capacidad_instalada_mw  |            0 |           0 |
| generacion_mw           |            0 |           0 |
| autoconsumo_estimado_mw |            0 |           0 |
| vertido_estimado_mw     |            0 |           0 |
| curtailment_estimado_mw |            0 |           0 |
- Cardinalidad categórica (muestra):
| column     |   n_unique | top_values                                                                |
|:-----------|-----------:|:--------------------------------------------------------------------------|
| zona_id    |         24 | Z007:52632; Z017:52632; Z006:52632                                        |
| tecnologia |          3 | eolica_distribuida:421056; cogeneracion:421056; solar_fotovoltaica:421056 |
- Distribuciones numéricas (muestra):
| column                  |     min |      p50 |      p95 |     max |     mean |
|:------------------------|--------:|---------:|---------:|--------:|---------:|
| capacidad_instalada_mw  | 5.64896 | 21.3647  | 51.6619  | 60.6176 | 26.065   |
| generacion_mw           | 0       |  5.76234 | 20.9373  | 57.1026 |  7.0214  |
| autoconsumo_estimado_mw | 0       |  2.74523 | 11.0324  | 30.155  |  3.39998 |
| vertido_estimado_mw     | 0       |  2.43368 | 11.1534  | 29.8105 |  3.62142 |
| curtailment_estimado_mw | 0       |  0       |  2.28704 | 25.1866 |  0.36302 |
- Coherencia lógica (checks):
| check                                   |   failed_rows |
|:----------------------------------------|--------------:|
| autoconsumo_mayor_generacion            |             0 |
| curtailment_mayor_que_vertido           |             0 |
| vertido_negativo_o_curtailment_negativo |             0 |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### demanda_ev
- Grain: `1 fila por hora-zona-tipo_recarga`
- Candidate key: `timestamp, zona_id, tipo_recarga`
- Foreign keys esperadas: `zona_id->zonas_red.zona_id`
- Filas/Columnas: `1,263,168` / `6`
- Cobertura temporal: `2024-01-01 00:00:00` -> `2025-12-31 23:00:00`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                    |   null_count |   null_rate |
|:--------------------------|-------------:|------------:|
| timestamp                 |            0 |           0 |
| zona_id                   |            0 |           0 |
| tipo_recarga              |            0 |           0 |
| demanda_ev_mw             |            0 |           0 |
| penetracion_ev            |            0 |           0 |
| horario_recarga_dominante |            0 |           0 |
- Cardinalidad categórica (muestra):
| column                    |   n_unique | top_values                                                                 |
|:--------------------------|-----------:|:---------------------------------------------------------------------------|
| zona_id                   |         24 | Z016:52632; Z018:52632; Z022:52632                                         |
| tipo_recarga              |          3 | publica_rapida:421056; laboral_destino:421056; residencial_nocturna:421056 |
| horario_recarga_dominante |          3 | 09:00-18:00:421056; 20:00-00:00:421056; 12:00-16:00:421056                 |
- Distribuciones numéricas (muestra):
| column         |     min |     p50 |     p95 |      max |    mean |
|:---------------|--------:|--------:|--------:|---------:|--------:|
| demanda_ev_mw  | 0       | 0.71597 | 8.73144 | 16.6215  | 2.01522 |
| penetracion_ev | 0.16702 | 0.43435 | 0.52923 |  0.60583 | 0.4078  |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### demanda_electrificacion_industrial
- Grain: `1 fila por hora-zona-cluster_industrial`
- Candidate key: `timestamp, zona_id, cluster_industrial`
- Foreign keys esperadas: `zona_id->zonas_red.zona_id`
- Filas/Columnas: `421,056` / `6`
- Cobertura temporal: `2024-01-01 00:00:00` -> `2025-12-31 23:00:00`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                          |   null_count |   null_rate |
|:--------------------------------|-------------:|------------:|
| timestamp                       |            0 |           0 |
| zona_id                         |            0 |           0 |
| cluster_industrial              |            0 |           0 |
| demanda_industrial_adicional_mw |            0 |           0 |
| perfil_operativo                |            0 |           0 |
| elasticidad_flexibilidad_proxy  |            0 |           0 |
- Cardinalidad categórica (muestra):
| column             |   n_unique | top_values                                                       |
|:-------------------|-----------:|:-----------------------------------------------------------------|
| zona_id            |         24 | Z015:17544; Z013:17544; Z014:17544                               |
| cluster_industrial |          8 | alimentacion:87720; metalurgia:70176; logistica:52632            |
| perfil_operativo   |          6 | doble_turno:122808; extensivo_diurno:105264; 3_turnos_24x7:70176 |
- Distribuciones numéricas (muestra):
| column                          |     min |     p50 |      p95 |      max |    mean |
|:--------------------------------|--------:|--------:|---------:|---------:|--------:|
| demanda_industrial_adicional_mw | 0.35765 | 6.83766 | 13.1811  | 15.9942  | 6.44557 |
| elasticidad_flexibilidad_proxy  | 0.17459 | 0.55349 |  0.66528 |  0.83633 | 0.54893 |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### eventos_congestion
- Grain: `1 fila por evento de congestion`
- Candidate key: `evento_id`
- Foreign keys esperadas: `zona_id->zonas_red.zona_id; subestacion_id->subestaciones.subestacion_id; alimentador_id->alimentadores.alimentador_id`
- Filas/Columnas: `88,855` / `11`
- Cobertura temporal: `2024-01-01 07:00:00` -> `2025-12-31 21:00:00`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column               |   null_count |   null_rate |
|:---------------------|-------------:|------------:|
| evento_id            |            0 |           0 |
| timestamp_inicio     |            0 |           0 |
| timestamp_fin        |            0 |           0 |
| zona_id              |            0 |           0 |
| subestacion_id       |            0 |           0 |
| alimentador_id       |            0 |           0 |
| severidad            |            0 |           0 |
| energia_afectada_mwh |            0 |           0 |
- Cardinalidad categórica (muestra):
| column          |   n_unique | top_values                                                                             |
|:----------------|-----------:|:---------------------------------------------------------------------------------------|
| evento_id       |      88855 | CG0068787:1; CG0068818:1; CG0068828:1                                                  |
| zona_id         |         18 | Z013:16727; Z021:14035; Z020:9109                                                      |
| subestacion_id  |         60 | S0037:5403; S0049:4822; S0066:4184                                                     |
| alimentador_id  |        176 | A00157:1580; A00231:1546; A00159:1541                                                  |
| severidad       |          4 | baja:51831; alta:17589; media:10645                                                    |
| causa_principal |          6 | electrificacion_industrial:60554; pico_ev_residencial:24150; pico_demanda_general:1471 |
- Distribuciones numéricas (muestra):
| column                |     min |     p50 |      p95 |     max |    mean |
|:----------------------|--------:|--------:|---------:|--------:|--------:|
| energia_afectada_mwh  | 0.00162 | 1.69756 | 37.7567  | 402.141 | 8.2892  |
| carga_relativa_max    | 0.95065 | 1.02227 |  1.14305 |   1.42  | 1.03452 |
| impacto_servicio_flag | 0       | 0       |  1       |   1     | 0.32525 |
- Coherencia lógica (checks):
| check                              |   failed_rows |
|:-----------------------------------|--------------:|
| energia_o_carga_relativa_invalidas |         28788 |
| fin_antes_de_inicio                |             0 |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### interrupciones_servicio
- Grain: `1 fila por interrupcion de servicio`
- Candidate key: `interrupcion_id`
- Foreign keys esperadas: `zona_id->zonas_red.zona_id; subestacion_id->subestaciones.subestacion_id`
- Filas/Columnas: `2,222` / `10`
- Cobertura temporal: `2024-01-01 07:00:00` -> `2025-12-31 02:00:00`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                      |   null_count |   null_rate |
|:----------------------------|-------------:|------------:|
| interrupcion_id             |            0 |           0 |
| timestamp_inicio            |            0 |           0 |
| timestamp_fin               |            0 |           0 |
| zona_id                     |            0 |           0 |
| subestacion_id              |            0 |           0 |
| clientes_afectados          |            0 |           0 |
| energia_no_suministrada_mwh |            0 |           0 |
| causa                       |            0 |           0 |
- Cardinalidad categórica (muestra):
| column          |   n_unique | top_values                                                            |
|:----------------|-----------:|:----------------------------------------------------------------------|
| interrupcion_id |       2222 | INT0000168:1; INT0001515:1; INT0000292:1                              |
| zona_id         |         24 | Z021:168; Z020:168; Z013:168                                          |
| subestacion_id  |         77 | S0038:42; S0058:42; S0052:42                                          |
| causa           |          7 | sobrecarga_local:652; protecciones:427; desconexiones_preventivas:331 |
| nivel_severidad |          4 | alta:1201; media:561; critica:351                                     |
- Distribuciones numéricas (muestra):
| column                      |       min |       p50 |       p95 |      max |       mean |
|:----------------------------|----------:|----------:|----------:|---------:|-----------:|
| clientes_afectados          | 410       | 4433      | 6588.9    | 8569     | 4464.27    |
| energia_no_suministrada_mwh |   0.31082 |   16.0057 |   64.4099 |  296.687 |   22.3152  |
| relacion_congestion_flag    |   0       |    1      |    1      |    1     |    0.63456 |
- Coherencia lógica (checks):
| check                    |   failed_rows |
|:-------------------------|--------------:|
| clientes_o_ens_invalidos |             0 |
| fin_antes_de_inicio      |             0 |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### activos_red
- Grain: `1 fila por activo`
- Candidate key: `activo_id`
- Foreign keys esperadas: `subestacion_id->subestaciones.subestacion_id; alimentador_id->alimentadores.alimentador_id (nullable en activos de subestacion)`
- Filas/Columnas: `1,053` / `10`
- Cobertura temporal: `NaT` -> `NaT`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                   |   null_count |   null_rate |
|:-------------------------|-------------:|------------:|
| alimentador_id           |          231 |    0.219373 |
| activo_id                |            0 |    0        |
| tipo_activo              |            0 |    0        |
| subestacion_id           |            0 |    0        |
| edad_anios               |            0 |    0        |
| estado_salud             |            0 |    0        |
| probabilidad_fallo_proxy |            0 |    0        |
| criticidad               |            0 |    0        |
- Cardinalidad categórica (muestra):
| column         |   n_unique | top_values                                           |
|:---------------|-----------:|:-----------------------------------------------------|
| activo_id      |       1053 | ACT000040:1; ACT000090:1; ACT000092:1                |
| tipo_activo    |          6 | linea_mt:274; reconectador:274; sensor_corriente:274 |
| subestacion_id |         77 | S0018:15; S0022:15; S0039:15                         |
| alimentador_id |        274 | nan:231; A00026:3; A00042:3                          |
- Distribuciones numéricas (muestra):
| column                      |        min |          p50 |             p95 |             max |         mean |
|:----------------------------|-----------:|-------------:|----------------:|----------------:|-------------:|
| edad_anios                  |     3      |     34       |    52           |    68           |     34.5128  |
| estado_salud                |    13.745  |     56.849   |    78.6808      |    98           |     57.1163  |
| probabilidad_fallo_proxy    |     0.1423 |      0.51082 |     0.68697     |     0.87757     |      0.51101 |
| criticidad                  |     0.6419 |      0.8591  |     0.99        |     0.99        |      0.86111 |
| capex_reposicion_estimado   | 20422.4    | 166786       |     1.21131e+06 |     1.57411e+06 | 327398       |
| opex_mantenimiento_estimado |  1959.26   |  10784.5     | 38016.4         | 51223.2         |  13269.4     |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### recursos_flexibilidad
- Grain: `1 fila por recurso de flexibilidad`
- Candidate key: `recurso_id`
- Foreign keys esperadas: `zona_id->zonas_red.zona_id`
- Filas/Columnas: `65` / `9`
- Cobertura temporal: `NaT` -> `NaT`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                   |   null_count |   null_rate |
|:-------------------------|-------------:|------------:|
| recurso_id               |            0 |           0 |
| zona_id                  |            0 |           0 |
| tipo_recurso             |            0 |           0 |
| capacidad_flexible_mw    |            0 |           0 |
| coste_activacion_eur_mwh |            0 |           0 |
| tiempo_respuesta_min     |            0 |           0 |
| disponibilidad_media     |            0 |           0 |
| fiabilidad_activacion    |            0 |           0 |
- Cardinalidad categórica (muestra):
| column       |   n_unique | top_values                                                          |
|:-------------|-----------:|:--------------------------------------------------------------------|
| recurso_id   |         65 | RF00020:1; RF00032:1; RF00038:1                                     |
| zona_id      |         24 | Z014:3; Z019:3; Z013:3                                              |
| tipo_recurso |          4 | microred_industrial:20; gestion_cargas_termicas:17; agregador_ev:14 |
- Distribuciones numéricas (muestra):
| column                   |      min |       p50 |       p95 |       max |      mean |
|:-------------------------|---------:|----------:|----------:|----------:|----------:|
| capacidad_flexible_mw    | 10.6674  |  20.9479  |  25.6398  |  27.1033  |  20.2299  |
| coste_activacion_eur_mwh | 85.7409  | 105.73    | 124.97    | 146.944   | 106.243   |
| tiempo_respuesta_min     |  9       |  19       |  24       |  32       |  18.0769  |
| disponibilidad_media     |  0.81792 |   0.90397 |   0.94602 |   0.96561 |   0.89801 |
| fiabilidad_activacion    |  0.80086 |   0.88449 |   0.93058 |   0.95237 |   0.88032 |
| madurez_operativa        |  0.53574 |   0.76267 |   0.88675 |   0.92482 |   0.765   |
- Coherencia lógica (checks):
| check                      |   failed_rows |
|:---------------------------|--------------:|
| indices_fuera_de_rango_0_1 |             0 |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### almacenamiento_distribuido
- Grain: `1 fila por sistema de almacenamiento`
- Candidate key: `storage_id`
- Foreign keys esperadas: `zona_id->zonas_red.zona_id`
- Filas/Columnas: `31` / `7`
- Cobertura temporal: `NaT` -> `NaT`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                |   null_count |   null_rate |
|:----------------------|-------------:|------------:|
| storage_id            |            0 |           0 |
| zona_id               |            0 |           0 |
| capacidad_energia_mwh |            0 |           0 |
| capacidad_potencia_mw |            0 |           0 |
| eficiencia_roundtrip  |            0 |           0 |
| coste_operacion_proxy |            0 |           0 |
| disponibilidad_media  |            0 |           0 |
- Cardinalidad categórica (muestra):
| column     |   n_unique | top_values                      |
|:-----------|-----------:|:--------------------------------|
| storage_id |         31 | ST00007:1; ST00027:1; ST00017:1 |
| zona_id    |         24 | Z004:2; Z019:2; Z006:2          |
- Distribuciones numéricas (muestra):
| column                |      min |      p50 |      p95 |      max |     mean |
|:----------------------|---------:|---------:|---------:|---------:|---------:|
| capacidad_energia_mwh |  9.16729 | 21.3806  | 38.0103  | 47.9423  | 23.3024  |
| capacidad_potencia_mw |  5.0226  |  8.50728 | 10.8257  | 11.8517  |  8.34447 |
| eficiencia_roundtrip  |  0.79697 |  0.87177 |  0.9109  |  0.92124 |  0.87316 |
| coste_operacion_proxy | 13.187   | 17.241   | 20.2537  | 21.9484  | 16.9627  |
| disponibilidad_media  |  0.84779 |  0.90596 |  0.95678 |  0.96959 |  0.90636 |
- Coherencia lógica (checks):
| check                      |   failed_rows |
|:---------------------------|--------------:|
| indices_fuera_de_rango_0_1 |             0 |
- Utilidad analítica estimada: **Alta** para forecasting/scoring/dashboard.

### intervenciones_operativas
- Grain: `1 fila por intervencion operativa catalogada`
- Candidate key: `intervencion_id`
- Foreign keys esperadas: `zona_id->zonas_red.zona_id`
- Filas/Columnas: `120` / `7`
- Cobertura temporal: `NaT` -> `NaT`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                       |   null_count |   null_rate |
|:-----------------------------|-------------:|------------:|
| intervencion_id              |            0 |           0 |
| zona_id                      |            0 |           0 |
| tipo_intervencion            |            0 |           0 |
| capacidad_alivio_estimado_mw |            0 |           0 |
| coste_estimado               |            0 |           0 |
| tiempo_despliegue_dias       |            0 |           0 |
| complejidad_operativa        |            0 |           0 |
- Cardinalidad categórica (muestra):
| column            |   n_unique | top_values                                                                            |
|:------------------|-----------:|:--------------------------------------------------------------------------------------|
| intervencion_id   |        120 | IO000001:1; IO000005:1; IO000014:1                                                    |
| zona_id           |         24 | Z006:5; Z016:5; Z018:5                                                                |
| tipo_intervencion |          5 | activacion_flexibilidad:24; reconfiguracion_topologica:24; ajuste_tension_reactiva:24 |
- Distribuciones numéricas (muestra):
| column                       |         min |          p50 |          p95 |         max |         mean |
|:-----------------------------|------------:|-------------:|-------------:|------------:|-------------:|
| capacidad_alivio_estimado_mw |     5.18662 |     15.4557  |     27.4103  |     32.1215 |     15.375   |
| coste_estimado               | 74348.9     | 113643       | 143819       | 154519      | 114127       |
| tiempo_despliegue_dias       |    22       |     38.5     |     50.05    |     56      |     38.4083  |
| complejidad_operativa        |     0.47397 |      0.75575 |      0.93078 |      0.99   |      0.74411 |
- Utilidad analítica estimada: **Media-Alta** para forecasting/scoring/dashboard.

### inversiones_posibles
- Grain: `1 fila por opcion de inversion`
- Candidate key: `inversion_id`
- Foreign keys esperadas: `zona_id->zonas_red.zona_id`
- Filas/Columnas: `144` / `10`
- Cobertura temporal: `NaT` -> `NaT`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                     |   null_count |   null_rate |
|:---------------------------|-------------:|------------:|
| inversion_id               |            0 |           0 |
| zona_id                    |            0 |           0 |
| tipo_inversion             |            0 |           0 |
| capex_estimado             |            0 |           0 |
| opex_incremental_estimado  |            0 |           0 |
| reduccion_riesgo_esperada  |            0 |           0 |
| aumento_capacidad_esperado |            0 |           0 |
| horizonte_meses            |            0 |           0 |
- Cardinalidad categórica (muestra):
| column         |   n_unique | top_values                                                                      |
|:---------------|-----------:|:--------------------------------------------------------------------------------|
| inversion_id   |        144 | INV000005:1; INV000036:1; INV000055:1                                           |
| zona_id        |         24 | Z006:6; Z016:6; Z018:6                                                          |
| tipo_inversion |          6 | nuevo_alimentador:24; repotenciacion_subestacion:24; automatizacion_avanzada:24 |
- Distribuciones numéricas (muestra):
| column                     |             min |              p50 |              p95 |              max |             mean |
|:---------------------------|----------------:|-----------------:|-----------------:|-----------------:|-----------------:|
| capex_estimado             |     2.78673e+06 |      5.27572e+06 |      6.42745e+06 |      6.95917e+06 |      5.19686e+06 |
| opex_incremental_estimado  | 85031           | 138711           | 160646           | 181446           | 137572           |
| reduccion_riesgo_esperada  |     0.25438     |      0.42404     |      0.51606     |      0.58151     |      0.42321     |
| aumento_capacidad_esperado |    10.9531      |     72.358       |    196.154       |    240           |     84.6415      |
| horizonte_meses            |     8           |     28           |     36.85        |     44           |     27.0903      |
| facilidad_implementacion   |     0.23615     |      0.39368     |      0.50381     |      0.58992     |      0.39572     |
| impacto_resiliencia        |     0.27081     |      0.45761     |      0.54438     |      0.60931     |      0.45086     |
- Coherencia lógica (checks):
| check                      |   failed_rows |
|:---------------------------|--------------:|
| indices_fuera_de_rango_0_1 |             0 |
- Utilidad analítica estimada: **Media-Alta** para forecasting/scoring/dashboard.

### escenario_macro
- Grain: `1 fila por fecha-escenario`
- Candidate key: `fecha, escenario`
- Foreign keys esperadas: `-`
- Filas/Columnas: `2,924` / `6`
- Cobertura temporal: `2024-01-01` -> `2025-12-31`
- Key null rows: `0`
- Duplicados sobre key: `0`
- Null rates (top 8):
| column                            |   null_count |   null_rate |
|:----------------------------------|-------------:|------------:|
| fecha                             |            0 |           0 |
| escenario                         |            0 |           0 |
| crecimiento_demanda_indice        |            0 |           0 |
| penetracion_ev_indice             |            0 |           0 |
| electrificacion_industrial_indice |            0 |           0 |
| presion_capex_indice              |            0 |           0 |
- Cardinalidad categórica (muestra):
| column    |   n_unique | top_values                                                      |
|:----------|-----------:|:----------------------------------------------------------------|
| escenario |          4 | aceleracion_electrificacion:731; estres_climatico:731; base:731 |
- Distribuciones numéricas (muestra):
| column                            |     min |     p50 |     p95 |     max |    mean |
|:----------------------------------|--------:|--------:|--------:|--------:|--------:|
| crecimiento_demanda_indice        | 0.98337 | 1.0694  | 1.12215 | 1.14547 | 1.06756 |
| penetracion_ev_indice             | 0.95342 | 1.18226 | 1.45896 | 1.54991 | 1.2041  |
| electrificacion_industrial_indice | 0.96011 | 1.14198 | 1.30945 | 1.37669 | 1.15236 |
| presion_capex_indice              | 0.95702 | 1.13301 | 1.30666 | 1.33888 | 1.1441  |
- Utilidad analítica estimada: **Media-Alta** para forecasting/scoring/dashboard.
