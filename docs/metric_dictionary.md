# Diccionario de Métricas (Canónico v2)

## Estado de documento
- **Activo**: sí
- **Ámbito**: capa SQL v2 + scoring v2 + validación v2
- **Referencia técnica principal**: `docs/sql_metric_definitions.md`

## 1) Métricas operativas de red
- `carga_relativa`: demanda/capacidad por nodo-hora.
- `flag_congestion`: congestión por regla técnica u ocurrencia de evento.
- `horas_congestion`: acumulado temporal de horas con congestión.
- `energia_afectada_congestion_mwh`: energía afectada por eventos de congestión.
- `carga_punta_mw`: pico de carga observado por zona.

## 2) Métricas de calidad de servicio y resiliencia
- `ens_total_mwh`: energía no suministrada agregada.
- `clientes_afectados_total`: clientes impactados por interrupciones.
- `indice_resiliencia`: score inverso de fragilidad mensual.
- `resilience_risk_score`: score de riesgo de resiliencia en la capa de priorización.

## 3) Métricas de electrificación y GD
- `demanda_ev_mwh`: carga EV agregada.
- `demanda_industrial_mwh`: carga adicional por electrificación industrial.
- `ratio_nueva_demanda`: `(EV + industrial) / demanda total`.
- `curtailment_mwh`: energía de GD no absorbida.

## 4) Métricas de flexibilidad y almacenamiento
- `cobertura_flexible_total_mw`: capacidad flexible + soporte storage disponible.
- `gap_tecnico_mw`: demanda crítica no cubierta por flexibilidad.
- `ratio_flexibilidad_estres`: cobertura flexible respecto a demanda crítica.
- `flexibility_gap_score`: score de brecha técnico-económica.

## 5) Métricas económicas y de inversión
- `coste_riesgo_proxy`: proxy de coste por ENS, curtailment y congestión.
- `intensidad_capex_proxy`: presión de inversión asociada a carga y brecha.
- `capex_total`: suma de CAPEX en cartera por zona.
- `economic_priority_score`: score económico de priorización.
- `investment_priority_score`: score final multicriterio de inversión.

## 6) Métricas de gobernanza de decisión
- `risk_tier`: bajo / medio / alto / critico.
- `urgency_tier`: monitorizacion / planificada / alta / inmediata.
- `main_risk_driver`: driver dominante del score.
- `recommended_intervention`: intervención recomendada final.
- `confidence_flag`: confianza de recomendación condicionada por forecast.

## Nota de compatibilidad
El diccionario legacy basado en `feeder/territory` queda deprecado para análisis principal.
