# Validation Report v2

        ## Objetivo
        Validar coherencia end-to-end del proyecto: datos, SQL, features, forecasting, anomalías, scoring, escenarios, visuales y dashboard.

        ## Row counts clave
        |   n_zonas |   n_subestaciones |   n_alimentadores |   n_demanda_horaria |   n_node_features |   n_zone_day_features |   n_zone_month_features |   n_scoring |
|----------:|------------------:|------------------:|--------------------:|------------------:|----------------------:|------------------------:|------------:|
|        24 |                77 |               274 |             4807056 |           4807056 |                 17544 |                     576 |          24 |

        ## Issues encontrados
        | area   | check               | severity   |   observed |   expected | fix_applied_or_recommended   |
|:-------|:--------------------|:-----------|-----------:|-----------:|:-----------------------------|
| global | sin_issues_criticos | info       |          0 |          0 | No aplica                    |

        ## Fixes applied
        | fix                                                                                 |
|:------------------------------------------------------------------------------------|
| Se implementó capa SQL validada con controles formales (10_validation_queries.sql). |
| Se incorporó feature engineering con contratos explícitos por granularidad.         |
| Se añadió benchmark de forecasting interpretable con métricas por segmento.         |
| Se incorporó detector de anomalías con señales precursoras.                         |
| Se implementó scoring multicriterio con sensibilidad de pesos.                      |
| Se añadió scenario engine con 8 escenarios comparables.                             |

        ## Caveats obligatorios
        | caveat                                                                                 |
|:---------------------------------------------------------------------------------------|
| Los datos son sintéticos; no sustituyen calibración con telemetría real SCADA/AMI.     |
| Los proxies económicos no reemplazan valoración regulatoria ni WACC real.              |
| La causalidad entre anomalías, congestión y ENS requiere validación en histórico real. |
| El dashboard usa simplificaciones para garantizar portabilidad HTML única.             |
| Los resultados de forecast dependen de estabilidad estructural de patrones de carga.   |

        ## Overall confidence assessment
        - Estado global: PASS
        - Nivel: alta
        - Issues alta severidad: 0
        - Issues media severidad: 0

        ## Release readiness classification
        - Technical: technically valid
        - Analytical: analytically acceptable
        - Decision: decision-support ready
        - Committee: not committee-grade
        - Publish: publish-with-caveats

        ## Checklist final
        | item              | status   |
|:------------------|:---------|
| generacion_datos  | ok       |
| relaciones_tablas | ok       |
| profiling         | ok       |
| sql               | ok       |
| features          | ok       |
| forecasting       | ok       |
| anomaly_detection | ok       |
| scoring           | ok       |
| scenario_engine   | ok       |
| impacto_economico | ok       |
| visualizaciones   | ok       |
| dashboard         | ok       |
| narrativa_final   | ok       |

        ## Gate checks (hard blockers / warnings)
        | gate_name                       | passed   | is_blocker   | detail                                                                                                                             |
|:--------------------------------|:---------|:-------------|:-----------------------------------------------------------------------------------------------------------------------------------|
| official_dashboard_exists       | True     | True         | /Users/miguelfidalgo/Documents/sistema-inteligencia-red-electrificacion/outputs/dashboard/grid-electrification-command-center.html |
| official_dashboard_singleton    | True     | False        | Solo grid-electrification-command-center.html debe ser oficial                                                                     |
| core_scoring_files_exist        | True     | True         | scoring_table + ranking_final                                                                                                      |
| scenario_files_exist            | True     | True         | scenario_impacts_v2 + scenario_summary_v2                                                                                          |
| anomaly_files_exist             | True     | False        | anomalies_detected + anomalies_summary_by_type                                                                                     |
| forecast_benchmark_exists       | True     | False        | forecast_model_benchmark.csv                                                                                                       |
| sensitivity_exists              | True     | False        | scoring_sensitivity_analysis.csv                                                                                                   |
| ranking_matches_scoring_top     | True     | True         | score_top=Z013, rank_top=Z013                                                                                                      |
| scenario_cost_consistency       | True     | True         | max_abs_diff=5.820766091346741e-11                                                                                                 |
| anomaly_summary_consistency     | True     | False        | mismatch_rows=0                                                                                                                    |
| score_stability_rank_shift      | True     | False        | max_rank_shift=1.0                                                                                                                 |
| forecast_required_tasks_covered | True     | False        | present=['carga_relativa_zona', 'demanda_ev_zona', 'demanda_industrial_zona', 'demanda_subestacion', 'demanda_zona']               |

        ## Claims que deben matizarse
        - El sistema orienta decisiones de priorización, pero no sustituye estudios de red de ingeniería detallada.
        - La cuantificación económica es proxy para comparación relativa, no presupuesto definitivo.
