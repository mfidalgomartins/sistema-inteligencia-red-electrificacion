# Scoring Framework

## Scores obligatorios
1. congestion_risk_score
2. resilience_risk_score
3. service_impact_score
4. flexibility_gap_score
5. asset_exposure_score
6. electrification_pressure_score
7. economic_priority_score
8. investment_priority_score

## Principio
Scoring lineal interpretable que combina criterios técnicos, de servicio, activos, electrificación y economía.

## Fórmulas (resumen)
- Cada score parcial se construye con combinación lineal ponderada de señales normalizadas (0-100).
- `investment_priority_score = 0.74 * urgency_base + 0.26 * option_score_multicriterio`.

## Algoritmo multicriterio de alternativas
Alternativas comparadas por zona:
- refuerzo_red
- flexibilidad
- almacenamiento
- intervencion_operativa

Criterios:
- impacto esperado (40%)
- coste proxy (20%, inverso)
- tiempo de despliegue (15%, inverso)
- urgencia (15%)
- robustez de solución (10%)

## Trade-offs
- Refuerzo mejora robustez estructural pero penaliza coste/tiempo.
- Flexibilidad y operación mejoran rapidez y diferibilidad de CAPEX.
- Storage equilibra impacto técnico y resiliencia en zonas con alta variabilidad.

## Tiers y reglas
- risk_tier: bajo / medio / alto / critico.
- urgency_tier: monitorizacion / planificada / alta / inmediata.
- `risk_tier = critico` fuerza `intervencion_inmediata_prioritaria`.
- `congestion_risk_score >= 80` y cobertura flexible `< 0.15` fuerza refuerzo local.
- brecha de flexibilidad `>= 80`, impacto de servicio `>= 65` y storage efectivo `< 0.03` fuerza almacenamiento.
- `asset_exposure_score >= 75` fuerza sustitución de activos salvo intervención crítica inmediata.
- `confidence_flag` es alta cuando `NMAE <= 3.5%`.

## Escenarios
Los factores de escenario se localizan por zona según los drivers de score relevantes.
La validación bloquea el release si todos los escenarios producen el mismo ranking.

## Tipos de intervención finales
- monitorizar
- optimizar_operacion
- activar_flexibilidad
- desplegar_almacenamiento
- reforzar_red_local
- sustituir_activos
- intervencion_inmediata_prioritaria
