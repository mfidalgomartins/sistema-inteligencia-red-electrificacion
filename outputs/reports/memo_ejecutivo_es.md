# Memo Ejecutivo

## Contexto
El sistema analítico identifica pérdidas de capacidad operativa y eficiencia económica en red de distribución por congestión, envejecimiento de activos, crecimiento de demanda y electrificación territorial.

## Hallazgos clave
- Feeders monitorizados: **160**
- Horas de congestión agregadas: **185**
- ENS anual agregada: **17736.29 MWh**
- Curtailment anual agregado: **618044.57 MWh**
- Incremento de pico esperado acumulado a 2030: **3894.77 MW**

## Recomendación de estrategia de inversión
1. Priorizar feeders de categoría **Crítica** y **Alta** con mayor brecha entre pico previsto y límite térmico.
2. Aplicar combinación de refuerzo físico y flexibilidad en zonas con baja factibilidad de permisos.
3. Acelerar automatización en activos con alta ENS y degradación.
4. Mantener portafolio de almacenamiento donde el curtailment es estructuralmente elevado.

## Top 10 prioridades
|   priority_rank | feeder_id   | territory_id   | priority_tier   |   priority_score | recommended_action      |   estimated_capex_k_eur |
|----------------:|:------------|:---------------|:----------------|-----------------:|:------------------------|------------------------:|
|               1 | F0034       | T010           | Alta            |          58.5399 | almacenamiento_bateria  |                14112.5  |
|               2 | F0023       | T019           | Alta            |          55.6359 | automatizacion_avanzada |                  230    |
|               3 | F0044       | T013           | Planificar      |          54.4802 | flexibilidad_contratada |                 2458.03 |
|               4 | F0142       | T019           | Planificar      |          53.2391 | flexibilidad_contratada |                 2752.67 |
|               5 | F0051       | T019           | Planificar      |          51.329  | automatizacion_avanzada |                  230    |
|               6 | F0050       | T019           | Planificar      |          50.9474 | almacenamiento_bateria  |                11610.9  |
|               7 | F0106       | T009           | Planificar      |          49.4637 | flexibilidad_contratada |                 2346.37 |
|               8 | F0029       | T019           | Planificar      |          46.8116 | almacenamiento_bateria  |                14070.6  |
|               9 | F0108       | T002           | Planificar      |          46.6721 | flexibilidad_contratada |                 1756.61 |
|              10 | F0033       | T019           | Planificar      |          46.3565 | almacenamiento_bateria  |                11685.9  |

## Comparativa de escenarios
| scenario          |   avg_priority_score |   critical_feeders |   high_or_critical |   total_capex_m_eur |   total_ens_mwh |   total_curtailment_mwh |
|:------------------|---------------------:|-------------------:|-------------------:|--------------------:|----------------:|------------------------:|
| estres_climatico  |              45.0301 |                  1 |                 24 |             626.982 |         25717.6 |                  595479 |
| dg_alta           |              44.4077 |                  1 |                 21 |             597.955 |         17913.7 |                  780175 |
| capex_restringido |              44.1107 |                  1 |                 19 |             667.619 |         19509.9 |                  628721 |
| ev_acelerado      |              44.024  |                  1 |                 19 |             603.76  |         18091   |                  618045 |
| flex_storage_push |              43.7493 |                  1 |                 19 |             557.317 |         16317.4 |                  603774 |
| base              |              43.3959 |                  1 |                 18 |             580.539 |         17736.3 |                  618045 |
