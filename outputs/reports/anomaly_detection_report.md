# Anomaly Detection Report (v2)

        ## Cobertura
        - demanda inesperada
        - carga relativa anormal
        - curtailment anormal
        - ENS atípica por zona
        - deterioro operativo por subestación
        - desviaciones frente a patrón esperado

        ## Resumen por tipo
        | anomaly_type           |   n_eventos |   severidad_media |   pct_precursor_congestion |   pct_precursor_interrupcion |
|:-----------------------|------------:|------------------:|---------------------------:|-----------------------------:|
| carga_relativa_anormal |       60956 |           3.17985 |                  0.602631  |                     0.735399 |
| curtailment_anormal    |        8361 |           4.66696 |                  0.0101662 |                     0.199498 |
| desviacion_vs_patron   |        7075 |           3.44244 |                  0.710813  |                     0.750106 |
| demanda_inesperada     |         833 |           3.33596 |                  0.0060024 |                     0.310924 |
| ens_atipica            |         618 |           3.56398 |                  0         |                     1        |

        ## Utilidad operativa
        - Las anomalías con `precursor_congestion_48h=1` priorizan monitorización y alivio preventivo.
        - Las anomalías `ens_atipica` y `deterioro_operativo_subestacion` elevan prioridad de inspección y resiliencia.

        ## Recomendación de monitorización
        - Activar vigilancia intradía en zonas con mayor `severidad_media` y `ratio_precursor_congestion`.
        - Integrar `n_anomalias` y `anomalias_criticas` en el scoring de prioridad de inversión.
