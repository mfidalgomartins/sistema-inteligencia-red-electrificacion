# Diccionario de Métricas

## Estado de documento
- **Activo**: sí
- **Ámbito**: capa SQL, puntuación y validación canónica
- **Referencia técnica principal**: `docs/sql_metric_definitions.md`

## 1) Métricas operativas de red
- `carga_relativa`: demanda/capacidad por nodo-hora.
- `flag_congestion`: congestión por regla técnica u ocurrencia de evento.
- `horas_congestion`: acumulado de horas distintas con al menos un nodo congestionado en la entidad agregada.
- `energia_afectada_congestion_mwh`: energía afectada por eventos de congestión.
- `carga_punta_mw`: pico horario de demanda agregada observado por zona.

## 2) Métricas de calidad de servicio y resiliencia
- `ens_total_mwh`: energía no suministrada agregada.
- `clientes_afectados_total`: clientes impactados por interrupciones.
- `indice_resiliencia`: índice inverso de fragilidad mensual.
- `resilience_risk_score`: puntuación de riesgo de resiliencia en la capa de priorización.

## 3) Métricas de electrificación y GD
- `demanda_ev_mwh`: carga agregada de vehículos eléctricos.
- `demanda_industrial_mwh`: carga adicional por electrificación industrial.
- `ratio_nueva_demanda`: `(VE + industrial) / demanda total`.
- `curtailment_mwh`: energía de GD no absorbida.

## 4) Métricas de flexibilidad y almacenamiento
- `cobertura_flexible_total_mw`: capacidad flexible + soporte de almacenamiento disponible.
- `gap_tecnico_mw`: demanda crítica no cubierta por flexibilidad.
- `ratio_flexibilidad_estres`: cobertura flexible respecto a demanda crítica.
- `flexibility_gap_score`: puntuación de brecha técnico-económica.

## 5) Métricas económicas y de inversión
- `coste_riesgo_proxy`: aproximación de coste por ENS, vertido y congestión.
- `intensidad_capex_proxy`: presión de inversión asociada a carga y brecha.
- `capex_total`: suma de CAPEX en cartera por zona.
- `economic_priority_score`: puntuación económica de priorización.
- `investment_priority_score`: puntuación final multicriterio de inversión.

## 6) Métricas de gobernanza de decisión
- `risk_tier`: bajo / medio / alto / critico.
- `urgency_tier`: monitorizacion / planificada / alta / inmediata.
- `main_risk_driver`: factor dominante de la puntuación.
- `recommended_intervention`: intervención recomendada final.
- `confidence_flag`: confianza alta cuando el NMAE de demanda zonal es `<= 3,5%`.
