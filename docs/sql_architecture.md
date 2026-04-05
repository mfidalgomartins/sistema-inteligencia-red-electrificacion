# Arquitectura SQL - Sistema de Inteligencia de Red

## Dialecto elegido
Se utiliza **DuckDB SQL** por tres motivos operativos:
1. Lectura directa de `CSV` con `read_csv_auto`, ideal para portfolio reproducible sin servidor.
2. Buen rendimiento analítico para tablas horarias de millones de filas.
3. Sintaxis compatible con patrones de modelado SQL profesional (CTEs, ventanas, vistas, tablas analíticas).

## Objetivo de la capa SQL
Construir una capa visible, auditable y defendible en entrevista para:
- normalización de datos (`staging`)
- integración técnico-operativa de red (`integration`)
- construcción de marts analíticos (`analytical marts`)
- exposición de KPIs ejecutivos (`kpi queries`)
- controles formales de calidad (`validation queries`)

## Estructura por niveles

### 1) Staging
Archivo: `sql/01_staging_core_tables.sql`

Función:
- Tipado explícito y contrato de columnas por tabla base.
- Estandarización de claves y fechas.
- Punto de entrada oficial de datos raw.

Tablas/vistas creadas:
- `stg_zonas_red`
- `stg_subestaciones`
- `stg_alimentadores`
- `stg_demanda_horaria`
- `stg_generacion_distribuida`
- `stg_demanda_ev`
- `stg_demanda_electrificacion_industrial`
- `stg_eventos_congestion`
- `stg_interrupciones_servicio`
- `stg_activos_red`
- `stg_recursos_flexibilidad`
- `stg_almacenamiento_distribuido`
- `stg_intervenciones_operativas`
- `stg_inversiones_posibles`
- `stg_escenario_macro`

### 2) Integration
Archivos:
- `sql/02_integrated_network_load.sql`
- `sql/03_integrated_grid_events.sql`
- `sql/04_integrated_service_quality.sql`
- `sql/05_integrated_flexibility_assets.sql`

Función:
- Integrar carga horaria de nodo con EV, electrificación industrial y GD.
- Normalizar eventos de congestión e interrupciones con duración/severidad.
- Consolidar recursos de flexibilidad, almacenamiento e intervención por zona.

Vistas clave:
- `vw_int_zone_hour_components`
- `vw_int_network_load_hour`
- `vw_int_grid_events`
- `vw_int_grid_events_hourly`
- `vw_int_service_quality_events`
- `vw_int_service_quality_enriched`
- `vw_int_flexibility_assets_zone`

### 3) Analytical Marts
Archivos:
- `sql/06_analytical_mart_node_hour.sql`
- `sql/07_analytical_mart_zone_day.sql`
- `sql/08_analytical_mart_zone_month.sql`

Función:
- Crear granularidades analíticas para operación y planificación.
- Materializar vistas de riesgo y exposición.
- Preparar base de scoring y priorización de inversiones.

Marts:
- `mart_node_hour_operational_state`
- `mart_zone_day_operational`
- `mart_zone_month_operational`

Vistas obligatorias implementadas:
- `vw_node_hour_operational_state`
- `vw_zone_operational_risk`
- `vw_assets_exposure`
- `vw_flexibility_gap`
- `vw_investment_candidates`

### 4) KPI Queries
Archivo: `sql/09_kpi_queries.sql`

Vistas KPI implementadas:
- `kpi_top_zonas_riesgo_operativo`
- `kpi_top_subestaciones_congestion_acumulada`
- `kpi_top_alimentadores_exposicion`
- `kpi_zonas_mayor_ens`
- `kpi_zonas_peor_ratio_flex_estres`
- `kpi_zonas_potencial_capex_diferible`
- `kpi_activos_mas_expuestos`
- `kpi_zonas_afectadas_ev_industrial`

### 5) Validation Queries
Archivo: `sql/10_validation_queries.sql`

Activos de calidad:
- `validation_checks` (tabla de controles)
- `vw_validation_failures` (fallos priorizados)

Familias de control:
- unicidad de PK candidatas
- integridad referencial esperada
- dominios técnicos (no negativos, rangos)
- consistencia temporal
- coherencia de integración y cobertura de marts

## Orden de ejecución
1. `sql/01_staging_core_tables.sql`
2. `sql/02_integrated_network_load.sql`
3. `sql/03_integrated_grid_events.sql`
4. `sql/04_integrated_service_quality.sql`
5. `sql/05_integrated_flexibility_assets.sql`
6. `sql/06_analytical_mart_node_hour.sql`
7. `sql/07_analytical_mart_zone_day.sql`
8. `sql/08_analytical_mart_zone_month.sql`
9. `sql/09_kpi_queries.sql`
10. `sql/10_validation_queries.sql`

## Dependencias lógicas de joins
- Nodo horario: `stg_demanda_horaria` + `stg_alimentadores` + `stg_subestaciones`.
- Eventos: `vw_int_grid_events_hourly` se acopla por `(timestamp, zona_id, subestacion_id, alimentador_id)`.
- Calidad de servicio: enriquecimiento de interrupciones mediante solape temporal con congestión.
- Flexibilidad: consolidación zonal para cobertura técnica y económica.
- Activos: exposición derivada de estrés operativo por nodo + continuidad de servicio por subestación.

## Convenciones de modelado
- Sin `SELECT *`.
- CTEs con responsabilidad única.
- Nombres de columnas en español de negocio técnico.
- Flags booleanos para trazabilidad (`flag_congestion`, `flag_estres_operativo`, etc.).
- Métricas normalizadas con funciones ventana para scores comparables entre zonas/candidatos.

## Cómo ejecutar rápidamente con DuckDB
Ejemplo de ejecución secuencial desde shell:

```bash
.venv/bin/python - <<'PY'
from pathlib import Path
import duckdb

root = Path('/Users/miguelfidalgo/Documents/sistema-inteligencia-red-electrificacion')
raw_path = str(root / 'data' / 'raw').replace("'", "''")
conn = duckdb.connect()

sequence = [
    '01_staging_core_tables.sql',
    '02_integrated_network_load.sql',
    '03_integrated_grid_events.sql',
    '04_integrated_service_quality.sql',
    '05_integrated_flexibility_assets.sql',
    '06_analytical_mart_node_hour.sql',
    '07_analytical_mart_zone_day.sql',
    '08_analytical_mart_zone_month.sql',
    '09_kpi_queries.sql',
    '10_validation_queries.sql',
]

for sql_name in sequence:
    sql_text = (root / 'sql' / sql_name).read_text(encoding='utf-8').format(raw_path=raw_path)
    conn.execute(sql_text)

print(conn.execute('SELECT COUNT(*) AS n_checks, SUM(1-passed) AS checks_fallidos FROM validation_checks').fetchdf())
PY
```

