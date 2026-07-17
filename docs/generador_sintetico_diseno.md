# Diseño del generador sintético de red

## Objetivo
Crear datos coherentes y reproducibles para probar la capa SQL, pronóstico, anomalías, escenarios, puntuación y tablero.

## Principios de simulación
- Reproducibilidad total con semilla global fija.
- Horizonte de dos años con granularidad horaria.
- Coherencia jerárquica: zona -> subestación -> alimentador.
- Correlaciones estructurales entre demanda, vehículos eléctricos, electrificación industrial, congestión, estado de activos e interrupciones.
- Diferenciación territorial por tipo de zona y región operativa.

## Dominios modulares
- `entities.py`: topología y activos.
- `macro.py`: escenario macro y factores de crecimiento.
- `demand.py`: demanda horaria, vehículos eléctricos e industrial.
- `generation.py`: GD por tecnología, autoconsumo y vertido.
- `operations.py`: congestión, interrupciones, flexibilidad, almacenamiento, intervenciones e inversiones.
- `validation.py`: controles de plausibilidad y cardinalidades.

## Salidas
- 15 tablas obligatorias en `data/raw/`.
- `validaciones_plausibilidad.csv`.
- `resumen_cardinalidades.csv`.
- `resumen_logica_generador.md`.
