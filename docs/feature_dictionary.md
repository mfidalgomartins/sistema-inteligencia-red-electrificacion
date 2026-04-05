# Feature Dictionary (v2)

## Principio de diseño
- **Observadas**: señales directamente medidas en operación o eventos.
- **Derivadas**: señales transformadas para modelado, interpretación o decisión.

## node_hour_features (granularidad nodo-hora)
- `carga_relativa` (observada): ratio demanda/capacidad instantánea.
- `sobrecarga_flag` (derivada): 1 si `carga_relativa >= 1.0`.
- `proximidad_a_capacidad` (derivada): cercanía a saturación técnica.
- `hora_punta_flag` (observada): marca de punta operativa.
- `volatilidad_reciente` (derivada): desviación estándar móvil de 24h en demanda.
- `rolling_mean_24h` (derivada): media móvil de demanda 24h.
- `rolling_max_7d` (derivada): máximo de carga relativa en 7 días.
- `crecimiento_vs_baseline` (derivada): desviación frente a baseline horario por tipo de día.
- `presion_ev` (derivada): proporción EV en demanda del nodo.
- `presion_electrificacion_industrial` (derivada): proporción industrial adicional en demanda.
- `cobertura_flexibilidad` (derivada): cobertura flexible sobre demanda nodal.
- `storage_support_ratio` (derivada): soporte de storage sobre demanda nodal.
- `coste_flexibilidad_proxy` (observada/derivada): coste medio zonal de activación flexible.
- `penetracion_generacion_distribuida` (derivada): GD asignada sobre demanda.
- `curtailment_ratio` (derivada): energía recortada sobre GD asignada.
- `historial_congestion_7d` (derivada): congestión acumulada de 7 días.
- `historial_congestion_30d` (derivada): congestión acumulada de 30 días.
- `historial_interrupciones_30d` (derivada): interrupciones acumuladas por subestación en 30 días.
- `fragilidad_activos_asociados` (derivada): exposición media de activos asociados.
- `criticidad_territorial` (observada): criticidad estructural de la zona.
- `riesgo_climatico` (observada): vulnerabilidad climática territorial.

## zone_day_features (granularidad zona-día)
- `horas_congestion` (observada agregada): total de horas con congestión.
- `severidad_media` (derivada): severidad media diaria de eventos.
- `ens` (observada agregada): energía no suministrada diaria.
- `clientes_afectados` (observada agregada): afectados diarios.
- `percentil_carga` (derivada): percentil 95 de carga relativa diaria por zona.
- `gap_flexibilidad` (derivada): brecha técnica flexible diaria.
- `exposicion_activos` (derivada): exposición media de activos en la zona.
- `demanda_ev_total` (observada agregada): energía EV diaria.
- `demanda_industrial_adicional_total` (observada agregada): energía industrial adicional diaria.
- `curtailment_total` (observada agregada): energía recortada diaria.
- `demanda_no_servida_proxy` (derivada): ENS + componente de curtailment.
- `coste_riesgo_proxy` (derivada): proxy económico de riesgo diario.

## zone_month_features (granularidad zona-mes)
- `tendencia_demanda` (derivada): cambio mensual de demanda total.
- `cambio_estacional` (derivada): demanda mensual respecto a media anual zonal.
- `recurrencia_congestion` (derivada): frecuencia relativa mensual de congestión.
- `riesgo_operativo_agregado` (derivada): índice compuesto de riesgo mensual.
- `indice_resiliencia` (derivada): score inverso de fragilidad mensual.
- `intensidad_capex_proxy` (derivada): presión de inversión por carga y gap flexible.
- `flexibilidad_efectiva` (derivada): cobertura flexible media / carga punta.
- `storage_efectivo` (derivada): soporte storage medio / carga punta.
- `presion_crecimiento` (observada agregada): tensión estructural de crecimiento.

## intervention_candidates_features (granularidad candidato-zona)
- `main_risk_driver` (derivada): driver dominante de riesgo para intervención.
- `technical_score_inputs` (derivada): inputs técnicos combinados de riesgo.
- `economic_score_inputs` (derivada): inputs económicos y CAPEX.
- `flexibility_viability_inputs` (derivada): viabilidad operativa de flex/storage.
- `investment_readiness_inputs` (derivada): madurez de despliegue e impacto.

## Utilidad para utility
Estas señales permiten separar saturación puntual vs. estructural, conectar calidad de servicio con exposición de activos, y traducir estrés operativo en decisiones de refuerzo, flexibilidad, storage o secuenciación CAPEX.
