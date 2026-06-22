from __future__ import annotations

import json

import numpy as np
import pandas as pd

from .common_v2 import connect_v2, ensure_dirs, get_paths, write_df


def run_advanced_analysis_v2() -> dict[str, pd.DataFrame]:
    paths = ensure_dirs(get_paths())
    conn = connect_v2(paths)

    node_hour = conn.execute(
        """
        SELECT
            timestamp,
            zona_id,
            subestacion_id,
            alimentador_id,
            carga_relativa,
            flag_congestion,
            flag_estres_operativo,
            demanda_ev_asignada_mw,
            demanda_industrial_asignada_mw,
            curtailment_asignado_mw,
            hora_punta_flag
        FROM mart_node_hour_operational_state
        """
    ).df()
    zone_day = conn.execute("SELECT * FROM zone_day_features").df()
    flex_gap = conn.execute("SELECT * FROM vw_flexibility_gap").df()
    scoring = pd.read_csv(paths.data_processed / "intervention_scoring_table.csv")

    # Bloque 1: salud operativa general.
    b1_temporal = (
        node_hour.assign(month=pd.to_datetime(node_hour["timestamp"]).dt.to_period("M").astype(str))
        .groupby("month", as_index=False)
        .agg(
            carga_relativa_media=("carga_relativa", "mean"),
            horas_congestion=("flag_congestion", "sum"),
            horas_estres=("flag_estres_operativo", "sum"),
        )
    )
    write_df(b1_temporal, paths.data_processed / "support_salud_operativa_temporal.csv")

    # Bloque 2: congestión y capacidad.
    b2_nodes = (
        node_hour.groupby(["zona_id", "subestacion_id", "alimentador_id"], as_index=False)
        .agg(
            horas_congestion=("flag_congestion", "sum"),
            carga_relativa_max=("carga_relativa", "max"),
            carga_relativa_media=("carga_relativa", "mean"),
        )
        .sort_values("horas_congestion", ascending=False)
    )
    write_df(b2_nodes, paths.data_processed / "support_congestion_nodos.csv")

    # Bloque 3: calidad de servicio y resiliencia.
    b3_service = (
        zone_day.groupby("zona_id", as_index=False)
        .agg(
            ens_total=("ens", "sum"),
            clientes_afectados_total=("clientes_afectados", "sum"),
            coste_riesgo_total=("coste_riesgo_proxy", "sum"),
            horas_congestion=("horas_congestion", "sum"),
        )
        .sort_values("ens_total", ascending=False)
    )
    write_df(b3_service, paths.data_processed / "support_servicio_resiliencia.csv")

    # Bloque 4: flexibilidad y almacenamiento.
    b4_flex = flex_gap[[
        "zona_id",
        "demanda_critica_mw",
        "cobertura_flexible_total_mw",
        "gap_tecnico_mw",
        "coste_activacion_flex_eur_mwh",
        "ratio_flexibilidad_estres",
        "riesgo_operativo_score",
    ]].sort_values("gap_tecnico_mw", ascending=False)
    write_df(b4_flex, paths.data_processed / "support_flexibilidad_storage.csv")

    # Bloque 5: electrificación.
    b5_electr = (
        zone_day.groupby("zona_id", as_index=False)
        .agg(
            demanda_total=("demanda_total_mwh", "sum"),
            demanda_ev_total=("demanda_ev_total", "sum"),
            demanda_industrial_total=("demanda_industrial_adicional_total", "sum"),
            curtailment_total=("curtailment_total", "sum"),
            horas_congestion=("horas_congestion", "sum"),
        )
    )
    b5_electr["ratio_nueva_demanda"] = (
        b5_electr["demanda_ev_total"] + b5_electr["demanda_industrial_total"]
    ) / b5_electr["demanda_total"].replace(0, np.nan)
    b5_electr["ratio_nueva_demanda"] = b5_electr["ratio_nueva_demanda"].fillna(0.0)
    b5_electr = b5_electr.sort_values("ratio_nueva_demanda", ascending=False)
    write_df(b5_electr, paths.data_processed / "support_electrificacion_presion.csv")

    # Bloque 6: implicaciones económicas.
    b6_econ = scoring[[
        "zona_id",
        "investment_priority_score",
        "economic_priority_score",
        "recommended_intervention",
        "recommended_sequence",
        "urgency_tier",
        "risk_tier",
    ]].sort_values("investment_priority_score", ascending=False)
    write_df(b6_econ, paths.data_processed / "support_implicaciones_economicas.csv")

    # Hallazgos priorizados.
    findings = pd.DataFrame(
        [
            {
                "prioridad": 1,
                "hallazgo": "La congestión y el estrés se concentran en un subconjunto reducido de nodos.",
                "evidencia": f"Top 10 nodos concentran {b2_nodes.head(10)['horas_congestion'].sum():,.0f} horas de congestión.",
                "implicacion": "Priorizar intervención focalizada reduce riesgo sin sobredimensionar CAPEX global.",
            },
            {
                "prioridad": 2,
                "hallazgo": "La ENS y clientes afectados siguen patrón territorial asimétrico.",
                "evidencia": f"Zona líder en ENS: {b3_service.iloc[0]['zona_id']} con {b3_service.iloc[0]['ens_total']:.2f} MWh.",
                "implicacion": "Necesario combinar resiliencia operativa con renovación selectiva de activos.",
            },
            {
                "prioridad": 3,
                "hallazgo": "La brecha de flexibilidad no es homogénea entre zonas.",
                "evidencia": f"Gap técnico máximo: {b4_flex.iloc[0]['gap_tecnico_mw']:.2f} MW en {b4_flex.iloc[0]['zona_id']}.",
                "implicacion": "Hay espacio para CAPEX diferible donde ratio flex/estrés es alto.",
            },
            {
                "prioridad": 4,
                "hallazgo": "EV + electrificación industrial incrementan presión de absorción.",
                "evidencia": f"Zona con mayor ratio nueva demanda: {b5_electr.iloc[0]['zona_id']}.",
                "implicacion": "Reforzar forecasting y flexibilidad antes de escalada estructural de CAPEX.",
            },
            {
                "prioridad": 5,
                "hallazgo": "Existe cartera con secuencia diferenciada por urgencia y madurez.",
                "evidencia": f"Intervención recomendada dominante: {b6_econ['recommended_intervention'].mode().iloc[0]}.",
                "implicacion": "Secuenciar decisiones evita inversiones prematuras y reduce coste de no actuar.",
            },
        ]
    )
    write_df(findings, paths.data_processed / "priority_findings.csv")

    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# Notebook Principal - Inteligencia de Red\\n",
                    "Lectura reproducible de riesgo, flexibilidad, electrificación y priorización.\\n",
                ],
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## Carga de artefactos principales\\n",
                    "Se cargan las tablas procesadas por el pipeline canónico.\\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "import pandas as pd\\n",
                    "from pathlib import Path\\n",
                    "root = Path.cwd()\\n",
                    "if not (root / 'data').exists():\\n",
                    "    root = root.parent\\n",
                    "findings = pd.read_csv(root / 'data' / 'processed' / 'priority_findings.csv')\\n",
                    "zone_risk = pd.read_csv(root / 'data' / 'processed' / 'vw_zone_operational_risk.csv')\\n",
                    "scoring = pd.read_csv(root / 'data' / 'processed' / 'intervention_scoring_table.csv')\\n",
                    "scenario_summary = pd.read_csv(root / 'data' / 'processed' / 'scenario_summary_v2.csv')\\n",
                    "findings.head(10)\\n",
                ],
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## Top zonas por riesgo operativo\\n",
                    "Orden relativo de riesgo operativo territorial.\\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "zone_risk.sort_values('riesgo_operativo_score', ascending=False)[['zona_id','riesgo_operativo_score','horas_congestion','ens_total_mwh']].head(10)\\n",
                ],
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## Ranking final de intervención\\n",
                    "Tabla base para secuenciación 0-24 meses.\\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "scoring[['priority_rank','zona_id','investment_priority_score','risk_tier','urgency_tier','recommended_intervention','main_risk_driver']].head(15)\\n",
                ],
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## Escenarios comparados\\n",
                    "Lectura rápida de trade-offs entre riesgo residual e inversión requerida.\\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "scenario_summary.sort_values('coste_riesgo_total')[['scenario','coste_riesgo_total','inversion_requerida_total','prioridad_media']]\\n",
                ],
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## Dashboard\\n",
                    "Abrir `/outputs/dashboard/grid-electrification-command-center.html` para explorar filtros y escenarios.\\n",
                ],
            },
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3.x"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }

    (paths.root / "notebooks" / "proyecto_principal.ipynb").write_text(json.dumps(notebook, ensure_ascii=False, indent=2), encoding="utf-8")

    conn.close()

    return {
        "support_salud_operativa_temporal": b1_temporal,
        "support_congestion_nodos": b2_nodes,
        "support_servicio_resiliencia": b3_service,
        "support_flexibilidad_storage": b4_flex,
        "support_electrificacion_presion": b5_electr,
        "support_implicaciones_economicas": b6_econ,
        "priority_findings": findings,
    }


if __name__ == "__main__":
    out = run_advanced_analysis_v2()
    for name, df in out.items():
        print(name, len(df))
