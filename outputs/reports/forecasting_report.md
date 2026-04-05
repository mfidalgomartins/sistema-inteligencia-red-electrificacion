# Forecasting Report (v2)

        ## Enfoque
        - Split temporal holdout estricto: últimos 90 días para series diarias y últimas 3 semanas para series horarias.
        - Backtesting walk-forward con modelos interpretables: naive, seasonal naive, moving average, linear trend y exponential smoothing.

        ## Cobertura de forecasting
        - Demanda por zona.
        - Demanda por subestación.
        - Carga relativa por zona.
        - Demanda EV por zona.
        - Demanda de electrificación industrial por zona.

        ## Modelo ganador por objetivo (MAE)
        | task                    | model          |         mae |        rmse |   smape |         bias |   n_obs |
|:------------------------|:---------------|------------:|------------:|--------:|-------------:|--------:|
| carga_relativa_zona     | seasonal_naive |   0.0171434 |   0.0368978 | 3.12884 |  -0.00418692 |    2160 |
| demanda_ev_zona         | seasonal_naive |   4.40999   |   7.11461   | 2.85699 |   0.113327   |    2160 |
| demanda_industrial_zona | seasonal_naive |   6.17204   |  16.3837    | 4.94599 |   0.687694   |    2160 |
| demanda_subestacion     | seasonal_naive |  65.7659    | 153.728     | 3.25979 | -13.4805     |    6930 |
| demanda_zona            | seasonal_naive | 203.856     | 462.758     | 3.17937 | -43.25       |    2160 |

        ## Error por tipo de zona
        | tipo_zona   |      mae |    rmse |     bias |   n_obs |
|:------------|---------:|--------:|---------:|--------:|
| urbana      | 264.178  | 589.532 | -55.5516 |     720 |
| industrial  | 219.714  | 463.124 | -48.7091 |     720 |
| mixta       | 174.57   | 371.954 | -37.5468 |     270 |
| rural       |  99.5385 | 213.622 | -18.2547 |     450 |

        ## Error en horas punta (demanda horaria por zona)
        | model          |   mae_peak_hours |   rmse_peak_hours |   smape_peak_hours |   bias_peak_hours |   mae_all_hours |   rmse_all_hours |
|:---------------|-----------------:|------------------:|-------------------:|------------------:|----------------:|-----------------:|
| naive          |          24.4682 |           55.8255 |            5.97801 |          0.656248 |         19.0607 |          40.6396 |
| exp_smoothing  |          47.9115 |           76.1896 |           11.4177  |        -16.9333   |         40.3092 |          63.8235 |
| seasonal_naive |          48.2352 |           95.2911 |           12.0944  |        -24.5898   |         39.4826 |          81.3708 |
| moving_average |          88.4509 |          103.849  |           20.7318  |        -74.9101   |         75.7357 |          97.6581 |
| linear_trend   |         107.996  |          130.114  |           25.9767  |        -87.8489   |         87.554  |         113.523  |

        ## EV e industrial vs previsibilidad
        Las zonas con mayor ratio de nueva demanda (EV + industrial) muestran mayor MAE en demanda agregada.

        Zonas con peor previsibilidad:
        | zona_id   |     mae |   ratio_nueva_demanda | decision_forecast                              |
|:----------|--------:|----------------------:|:-----------------------------------------------|
| Z013      | 315.202 |             0.0404114 | forecast_insuficiente_requiere_refuerzo_o_flex |
| Z001      | 311.161 |             0.0395493 | forecast_insuficiente_requiere_refuerzo_o_flex |
| Z024      | 302.893 |             0.0362922 | forecast_insuficiente_requiere_refuerzo_o_flex |
| Z007      | 282.879 |             0.04524   | forecast_insuficiente_requiere_refuerzo_o_flex |
| Z010      | 261.109 |             0.035108  | forecast_insuficiente_requiere_refuerzo_o_flex |

        Zonas con previsibilidad suficiente para diferir inversión:
        | zona_id   |      mae |   ratio_nueva_demanda | decision_forecast                      |
|:----------|---------:|----------------------:|:---------------------------------------|
| Z004      |  79.2037 |             0.0381042 | forecast_suficiente_para_diferir_capex |
| Z002      |  90.8724 |             0.0373188 | forecast_suficiente_para_diferir_capex |
| Z005      |  98.7148 |             0.0360178 | forecast_suficiente_para_diferir_capex |
| Z009      | 102.08   |             0.0379227 | forecast_suficiente_para_diferir_capex |
| Z012      | 126.821  |             0.0274983 | forecast_suficiente_para_diferir_capex |

        ## Lectura operativa
        - Donde el error en horas punta es persistentemente alto, conviene reforzar margen operativo y monitorización intradía.
        - Donde el error es bajo y estable, existe fundamento para diferir CAPEX con gestión activa y flexibilidad.

        ## Lectura de planificación
        - La electrificación acelera la incertidumbre en zonas con alta presión EV+industrial.
        - En esos nodos, la decisión de diferir inversión requiere banderas de confianza más conservadoras.
