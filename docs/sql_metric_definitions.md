# Definiciones de Métricas SQL

## Principios
- Todas las métricas se calculan en la capa SQL para trazabilidad.
- Las métricas de score se normalizan por máximo observado cuando procede.
- Los agregados horarios usan `demanda_mw` como aproximación de `MWh` (paso horario = 1h).

## Métricas de estado operativo de nodo (`vw_node_hour_operational_state`)

### Carga y capacidad
- `carga_relativa` = `demanda_mw / capacidad_mw`
- `carga_relativa_neta` = `net_load_mw / capacidad_mw`
- `overload_mw` = `max(demanda_mw - capacidad_mw, 0)`
- `demanda_critica_mw` = `demanda_mw + demanda_ev_asignada_mw + demanda_industrial_asignada_mw`

### Integración de electrificación y generación distribuida
- `demanda_ev_asignada_mw`: carga EV asignada al nodo por share de demanda zonal.
- `demanda_industrial_asignada_mw`: carga industrial asignada al nodo por share de demanda zonal.
- `generacion_distribuida_asignada_mw`: GD asignada al nodo por share de capacidad zonal.
- `curtailment_asignado_mw`: curtailment asignado al nodo por share de capacidad zonal.

### Flexibilidad y almacenamiento
- `flexibilidad_cobertura_mw` = `soporte_flex_storage_mw * (demanda_mw / demanda_total_zona_mw)`
- `storage_support_proxy_mw` = `storage_potencia_total_mw * storage_disponibilidad_media * (demanda_mw / demanda_total_zona_mw)`
- `flex_coverage_ratio` (zonal) = `soporte_flex_storage_mw / peak_demand_zona_mw`

### Flags operativos
- `flag_carga_alta`: `carga_relativa >= 0.85`
- `flag_congestion`: `carga_relativa >= 1.00` o `overload_mw > 0` o eventos de congestión en la hora
- `flag_estres_climatico`: `temperatura >= 33` o `exposicion_climatica >= 0.70`
- `flag_estres_tension`: `tension_sistema_proxy < 0.94` o `> 1.06`
- `flag_estres_operativo`: combinación lógica de sobrecarga, congestión, curtailment o estrés de tensión

## Métricas de riesgo por zona (`vw_zone_operational_risk`)

### Métricas base
- `horas_congestion`: suma de horas con congestión en el horizonte analizado.
- `severidad_media_congestion`: media de `severidad_score` de eventos de congestión.
- `energia_afectada_congestion_mwh`: energía afectada agregada en horas de congestión.
- `ens_total_mwh`: energía no suministrada acumulada.
- `clientes_afectados_total`: clientes afectados acumulados.
- `carga_punta_mw`: máximo de carga diaria observada en zona.
- `presion_electrificacion_media` = media de `(demanda_ev_mwh + demanda_industrial_mwh) / demanda_total_mwh`.
- `brecha_flex_media` = media de `gap_flex_tecnico_mwh / demanda_total_mwh`.

### Score de riesgo operativo
`riesgo_operativo_score` combina siete señales:
1. presión por congestión (`horas_congestion`)
2. severidad media de congestión
3. ENS total
4. clientes afectados
5. carga relativa máxima media
6. criticidad territorial
7. tensión de crecimiento de demanda

Ponderaciones usadas:
- 24% congestión
- 16% severidad
- 18% ENS
- 14% clientes
- 10% carga relativa
- 9% criticidad territorial
- 9% tensión de demanda

## Métricas de exposición de activos (`vw_assets_exposure`)
- `exposicion_activo_score`: score compuesto por edad, salud, criticidad, horas de estrés y probabilidad de fallo base.
- `probabilidad_fallo_ajustada_proxy`: ajuste multiplicativo de `probabilidad_fallo_proxy` por exposición operativa observada.
- `energia_congestion_expuesta_mwh`: energía de congestión en el nodo donde opera el activo.

## Métricas de brecha de flexibilidad (`vw_flexibility_gap`)
- `demanda_critica_mw`: aproximada por `carga_punta_max_mw` zonal.
- `cobertura_flexible_total_mw`: `soporte_flex_storage_mw` zonal.
- `gap_tecnico_mw` = `max(demanda_critica_mw - cobertura_flexible_total_mw, 0)`.
- `gap_tecnico_mwh_medio`: media mensual de brecha técnica energética.
- `gap_economico_proxy_eur`: proxy de coste por brecha técnica, coste activación y horas de estrés.
- `ratio_flexibilidad_estres` = `cobertura_flexible_total_mw / demanda_critica_mw`.

## Métricas de priorización de inversión (`vw_investment_candidates`)
- `impacto_riesgo_proxy` = `reduccion_riesgo_esperada * riesgo_operativo_score`.
- `impacto_capacidad_relativo` = `aumento_capacidad_esperado * carga_punta_mw / gap_tecnico_mw`.
- `eficiencia_capex_riesgo` = `(reduccion_riesgo_esperada * riesgo_operativo_score + 15 * impacto_resiliencia) / capex_estimado`.
- `estrategia_flexibilidad_vs_refuerzo`: clasificación en `refuerzo`, `flexibilidad` u `operacion`.

### Score de prioridad inicial
`prioridad_inicial_score` agrega:
- 28% reducción de riesgo esperada
- 22% impacto en resiliencia
- 18% aumento de capacidad
- 12% eficiencia CAPEX-riesgo
- 10% facilidad de implementación
- 10% rapidez de despliegue (inverso de horizonte)

## KPIs ejecutivos (archivo `09_kpi_queries.sql`)
1. `kpi_top_zonas_riesgo_operativo`
2. `kpi_top_subestaciones_congestion_acumulada`
3. `kpi_top_alimentadores_exposicion`
4. `kpi_zonas_mayor_ens`
5. `kpi_zonas_peor_ratio_flex_estres`
6. `kpi_zonas_potencial_capex_diferible`
7. `kpi_activos_mas_expuestos`
8. `kpi_zonas_afectadas_ev_industrial`

## Validaciones clave (archivo `10_validation_queries.sql`)
- Unicidad de llaves candidatas en entidades maestras.
- Integridad de FKs `zona -> subestacion -> alimentador`.
- Dominios técnicos (`demanda_mw >= 0`, `capacidad_mw > 0`, `carga_relativa` en rango).
- Coherencia temporal (`timestamp_inicio <= timestamp_fin`).
- Cobertura de zonas en vista de riesgo.
- Consistencia congestión/interrupciones (bandera declarativa vs solape real).
