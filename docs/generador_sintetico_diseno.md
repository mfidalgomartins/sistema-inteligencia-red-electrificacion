# Diseño del Generador Sintético de Red

## Objetivo
Crear datos coherentes y reproducibles para probar la capa SQL, forecasting, anomalías, escenarios, scoring y dashboard.

## Principios de simulación
- Reproducibilidad total con seed global fija.
- Horizonte de dos años con granularidad horaria.
- Coherencia jerárquica: zona -> subestación -> alimentador.
- Correlaciones estructurales entre demanda, EV, electrificación industrial, congestión, estado de activos e interrupciones.
- Diferenciación territorial por tipo de zona y región operativa.

## Dominios modulares
- `entities.py`: topología y activos.
- `macro.py`: escenario macro y drivers de crecimiento.
- `demand.py`: demanda horaria, EV e industrial.
- `generation.py`: GD por tecnología, autoconsumo, vertido y curtailment.
- `operations.py`: congestión, interrupciones, flexibilidad, almacenamiento, intervenciones e inversiones.
- `validation.py`: checks de plausibilidad y cardinalidades.

## Salidas
- 15 tablas obligatorias en `data/raw/`.
- `validaciones_plausibilidad.csv`.
- `resumen_cardinalidades.csv`.
- `resumen_logica_generador.md`.
