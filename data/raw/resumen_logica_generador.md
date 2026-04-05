# Resumen de Logica del Generador Sintetico

## Configuracion principal
- Seed global: 20260328
- Periodo: 2024-01-01 00:00:00 a 2025-12-31 23:00:00
- Horas unicas simuladas: 17544
- Zonas: 24
- Subestaciones: 77
- Alimentadores: 274

## Logica sintetica aplicada
1. Topologia coherente jerarquica (zona -> subestacion -> alimentador).
2. Estacionalidad diaria, semanal y anual en demanda.
3. Perfiles diferenciales por tipo de zona y tipo de red.
4. Componentes EV e industrial correlacionados con crecimiento estructural.
5. Generacion distribuida tecnologica (solar/eolica/cogeneracion) con autoconsumo, vertido y curtailment.
6. Congestion modelada sobre utilizacion, cobertura de flexibilidad y eventos de punta.
7. Interrupciones correlacionadas con envejecimiento de activos y estres por congestion.
8. Recursos de flexibilidad y almacenamiento con cobertura desigual entre zonas.
9. Catalogos de intervenciones e inversiones con trade-offs tecnico-economicos.

## Magnitudes resultantes
- Demanda_horaria filas: 4,807,056
- Generacion_distribuida filas: 1,263,168
- Eventos de congestion: 88,855
- Interrupciones de servicio: 2,213
- Curtailment total estimado (MWh proxy): 458,556.61

## Cardinalidades por tabla
| tabla                              |   filas |   columnas | periodo_inicio      | periodo_fin                   |   n_unicos_clave_referencia |
|:-----------------------------------|--------:|-----------:|:--------------------|:------------------------------|----------------------------:|
| activos_red                        |    1053 |         10 | NaT                 | NaT                           |                        1053 |
| alimentadores                      |     274 |          9 | NaT                 | NaT                           |                         274 |
| almacenamiento_distribuido         |      31 |          7 | NaT                 | NaT                           |                          31 |
| demanda_electrificacion_industrial |  421056 |          6 | 2024-01-01 00:00:00 | 2025-12-31 23:00:00           |                           8 |
| demanda_ev                         | 1263168 |          6 | 2024-01-01 00:00:00 | 2025-12-31 23:00:00           |                           3 |
| demanda_horaria                    | 4807056 |         14 | 2024-01-01 00:00:00 | 2025-12-31 23:00:00           |                         274 |
| escenario_macro                    |    2924 |          6 | 2024-01-01 00:00:00 | 2025-12-31 00:00:00           |                           4 |
| eventos_congestion                 |   88855 |         11 | 2024-01-01 07:00:00 | 2025-12-31 21:00:00           |                       88855 |
| generacion_distribuida             | 1263168 |          8 | 2024-01-01 00:00:00 | 2025-12-31 23:00:00           |                           3 |
| interrupciones_servicio            |    2213 |         10 | 2024-01-01 00:00:00 | 2025-12-31 20:00:20.332967335 |                        2213 |
| intervenciones_operativas          |     120 |          7 | NaT                 | NaT                           |                         120 |
| inversiones_posibles               |     144 |         10 | NaT                 | NaT                           |                         144 |
| recursos_flexibilidad              |      65 |          9 | NaT                 | NaT                           |                          65 |
| subestaciones                      |      77 |          9 | NaT                 | NaT                           |                          77 |
| zonas_red                          |      24 |         12 | NaT                 | NaT                           |                          24 |
