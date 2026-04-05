# Diseno del Generador Sintetico de Red

## Objetivo
Crear un ecosistema de datos realista para analitica avanzada de utility: SQL complejo, forecast, deteccion de anomalias, escenarios, scoring y dashboard.

## Principios de simulacion
- Reproducibilidad total con seed global fija.
- Horizonte minimo de 2 anios con granularidad horaria.
- Coherencia jerarquica: zona -> subestacion -> alimentador.
- Correlaciones estructurales entre demanda, EV, electrificacion industrial, congestion, estado de activos e interrupciones.
- Diferenciacion territorial por tipo de zona y region operativa.

## Dominios modulares
- `entities.py`: topologia y activos.
- `macro.py`: escenario macro y drivers de crecimiento.
- `demand.py`: demanda horaria, EV e industrial.
- `generation.py`: GD por tecnologia, autoconsumo, vertido y curtailment.
- `operations.py`: congestion, interrupciones, flexibilidad, almacenamiento, intervenciones e inversiones.
- `validation.py`: checks de plausibilidad y cardinalidades.

## Salidas
- 15 tablas obligatorias en `data/raw/`.
- `validaciones_plausibilidad.csv`.
- `resumen_cardinalidades.csv`.
- `resumen_logica_generador.md`.
