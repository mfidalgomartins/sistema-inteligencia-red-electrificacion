from __future__ import annotations

import os
from pathlib import Path
from textwrap import dedent

os.environ.setdefault("MPLCONFIGDIR", str(Path(__file__).resolve().parents[1] / ".mplconfig"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .common_v2 import connect_v2, ensure_dirs, get_paths


def run_visualization_v2() -> list[str]:
    paths = ensure_dirs(get_paths())
    conn = connect_v2(paths)

    node = conn.execute(
        """
        SELECT
            timestamp,
            zona_id,
            subestacion_id,
            alimentador_id,
            carga_relativa,
            flag_congestion,
            hora_punta_flag,
            demanda_ev_asignada_mw,
            demanda_industrial_asignada_mw,
            curtailment_asignado_mw,
            criticidad_operativa
        FROM mart_node_hour_operational_state
        """
    ).df()
    zone_risk = conn.execute("SELECT * FROM vw_zone_operational_risk").df()
    flex_gap = conn.execute("SELECT * FROM vw_flexibility_gap").df()
    zone_day = conn.execute("SELECT * FROM zone_day_features").df()

    scoring = pd.read_csv(paths.data_processed / "intervention_scoring_table.csv")
    scenario_summary = pd.read_csv(paths.data_processed / "scenario_summary_v2.csv") if (paths.data_processed / "scenario_summary_v2.csv").exists() else pd.DataFrame()

    chart_paths: list[str] = []
    plt.style.use("seaborn-v0_8-whitegrid")

    # 1) Tendencia temporal de carga relativa.
    c1 = node.assign(month=pd.to_datetime(node["timestamp"]).dt.to_period("M").astype(str)).groupby("month", as_index=False)["carga_relativa"].mean()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(c1["month"], c1["carga_relativa"], marker="o", color="#1b9e77")
    ax.set_title("Tendencia temporal de carga relativa media")
    ax.set_xlabel("Mes")
    ax.set_ylabel("Carga relativa")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    p = paths.outputs_charts / "v2_01_tendencia_carga_relativa.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 2) Horas de congestión por zona.
    c2 = zone_risk.sort_values("horas_congestion", ascending=False)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(c2["zona_id"], c2["horas_congestion"], color="#d95f02")
    ax.set_title("Horas de congestión por zona")
    ax.set_xlabel("Zona")
    ax.set_ylabel("Horas")
    ax.tick_params(axis="x", rotation=60)
    fig.tight_layout()
    p = paths.outputs_charts / "v2_02_horas_congestion_zona.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 3) Heatmap de congestión por hora y territorio.
    c3 = node.assign(hour=pd.to_datetime(node["timestamp"]).dt.hour).groupby(["zona_id", "hour"], as_index=False)["flag_congestion"].sum()
    heat = c3.pivot(index="zona_id", columns="hour", values="flag_congestion").fillna(0.0)
    fig, ax = plt.subplots(figsize=(12, 6))
    im = ax.imshow(heat.values, aspect="auto", cmap="Reds")
    ax.set_title("Heatmap de congestión por hora y zona")
    ax.set_xlabel("Hora del día")
    ax.set_ylabel("Zona")
    ax.set_xticks(range(0, 24, 2))
    ax.set_yticks(range(len(heat.index)))
    ax.set_yticklabels(heat.index)
    fig.colorbar(im, ax=ax, label="Horas de congestión")
    fig.tight_layout()
    p = paths.outputs_charts / "v2_03_heatmap_congestion_hora_territorio.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 4) Top subestaciones por exposición.
    c4 = (
        node.groupby("subestacion_id", as_index=False)
        .agg(horas_congestion=("flag_congestion", "sum"), carga_relativa_max=("carga_relativa", "max"))
        .sort_values("horas_congestion", ascending=False)
        .head(15)
    )
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.barh(c4["subestacion_id"], c4["horas_congestion"], color="#7570b3")
    ax.set_title("Top subestaciones por exposición a congestión")
    ax.set_xlabel("Horas de congestión")
    ax.set_ylabel("Subestación")
    fig.tight_layout()
    p = paths.outputs_charts / "v2_04_top_subestaciones_exposicion.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 5) Top alimentadores por criticidad.
    c5 = (
        node.groupby("alimentador_id", as_index=False)
        .agg(criticidad_media=("criticidad_operativa", "mean"), horas_congestion=("flag_congestion", "sum"), carga_relativa_max=("carga_relativa", "max"))
        .sort_values(["criticidad_media", "horas_congestion"], ascending=False)
        .head(15)
    )
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(c5["alimentador_id"], c5["criticidad_media"], color="#66a61e")
    ax.set_title("Top alimentadores por criticidad operativa")
    ax.set_xlabel("Alimentador")
    ax.set_ylabel("Criticidad media")
    ax.tick_params(axis="x", rotation=70)
    fig.tight_layout()
    p = paths.outputs_charts / "v2_05_top_alimentadores_criticidad.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 6) ENS por zona.
    c6 = zone_day.groupby("zona_id", as_index=False)["ens"].sum().sort_values("ens", ascending=False)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(c6["zona_id"], c6["ens"], color="#e7298a")
    ax.set_title("ENS acumulada por zona")
    ax.set_xlabel("Zona")
    ax.set_ylabel("ENS (MWh)")
    ax.tick_params(axis="x", rotation=60)
    fig.tight_layout()
    p = paths.outputs_charts / "v2_06_ens_por_zona.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 7) Flexibilidad disponible vs necesidad.
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(flex_gap["cobertura_flexible_total_mw"], flex_gap["demanda_critica_mw"], c=flex_gap["riesgo_operativo_score"], cmap="viridis", s=70)
    for _, row in flex_gap.nlargest(10, "gap_tecnico_mw").iterrows():
        ax.annotate(row["zona_id"], (row["cobertura_flexible_total_mw"], row["demanda_critica_mw"]), fontsize=8)
    ax.set_title("Flexibilidad disponible vs demanda crítica")
    ax.set_xlabel("Cobertura flexible total (MW)")
    ax.set_ylabel("Demanda crítica (MW)")
    fig.tight_layout()
    p = paths.outputs_charts / "v2_07_flexibilidad_vs_necesidad.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 8) Storage capability vs stress.
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(flex_gap["storage_potencia_total_mw"], flex_gap["riesgo_operativo_score"], s=70, color="#1f78b4")
    for _, row in flex_gap.nlargest(10, "riesgo_operativo_score").iterrows():
        ax.annotate(row["zona_id"], (row["storage_potencia_total_mw"], row["riesgo_operativo_score"]), fontsize=8)
    ax.set_title("Capacidad de storage vs estrés operativo")
    ax.set_xlabel("Storage potencia total (MW)")
    ax.set_ylabel("Riesgo operativo score")
    fig.tight_layout()
    p = paths.outputs_charts / "v2_08_storage_vs_stress.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 9) Impacto EV sobre carga crítica.
    c9 = zone_day.groupby("zona_id", as_index=False).agg(demanda_ev_total=("demanda_ev_total", "sum"), percentil_carga=("percentil_carga", "mean"))
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(c9["demanda_ev_total"], c9["percentil_carga"], color="#a6761d")
    for _, row in c9.nlargest(8, "demanda_ev_total").iterrows():
        ax.annotate(row["zona_id"], (row["demanda_ev_total"], row["percentil_carga"]), fontsize=8)
    ax.set_title("Impacto EV sobre carga crítica (percentil carga)")
    ax.set_xlabel("Demanda EV total")
    ax.set_ylabel("Percentil de carga")
    fig.tight_layout()
    p = paths.outputs_charts / "v2_09_impacto_ev_carga_critica.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 10) Impacto electrificación industrial.
    c10 = zone_day.groupby("zona_id", as_index=False).agg(demanda_industrial=("demanda_industrial_adicional_total", "sum"), horas_congestion=("horas_congestion", "sum"))
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(c10["demanda_industrial"], c10["horas_congestion"], color="#e31a1c")
    for _, row in c10.nlargest(8, "demanda_industrial").iterrows():
        ax.annotate(row["zona_id"], (row["demanda_industrial"], row["horas_congestion"]), fontsize=8)
    ax.set_title("Impacto de electrificación industrial en congestión")
    ax.set_xlabel("Demanda industrial adicional total")
    ax.set_ylabel("Horas de congestión")
    fig.tight_layout()
    p = paths.outputs_charts / "v2_10_impacto_industrial.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 11) Curtailment por territorio.
    c11 = zone_day.groupby("zona_id", as_index=False)["curtailment_total"].sum().sort_values("curtailment_total", ascending=False)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(c11["zona_id"], c11["curtailment_total"], color="#fb9a99")
    ax.set_title("Curtailment acumulado por territorio")
    ax.set_xlabel("Zona")
    ax.set_ylabel("Curtailment total")
    ax.tick_params(axis="x", rotation=60)
    fig.tight_layout()
    p = paths.outputs_charts / "v2_11_curtailment_territorio.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 12) Riesgo técnico vs impacto económico.
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(scoring["congestion_risk_score"], scoring["economic_priority_score"], s=70, color="#6a3d9a")
    for _, row in scoring.nlargest(10, "investment_priority_score").iterrows():
        ax.annotate(row["zona_id"], (row["congestion_risk_score"], row["economic_priority_score"]), fontsize=8)
    ax.set_title("Riesgo técnico vs impacto económico")
    ax.set_xlabel("Congestion risk score")
    ax.set_ylabel("Economic priority score")
    fig.tight_layout()
    p = paths.outputs_charts / "v2_12_riesgo_tecnico_vs_economico.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 13) Ranking de prioridades.
    c13 = scoring.sort_values("investment_priority_score", ascending=False).head(20)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(c13["zona_id"], c13["investment_priority_score"], color="#33a02c")
    ax.set_title("Ranking de prioridades de intervención")
    ax.set_xlabel("Zona")
    ax.set_ylabel("Investment priority score")
    ax.tick_params(axis="x", rotation=60)
    fig.tight_layout()
    p = paths.outputs_charts / "v2_13_ranking_prioridades.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 14) Comparación entre escenarios.
    if scenario_summary.empty:
        scenario_summary = pd.DataFrame(
            {
                "scenario": ["sin_datos"],
                "coste_riesgo_total": [0.0],
                "inversion_requerida_total": [0.0],
            }
        )
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(scenario_summary["scenario"], scenario_summary["coste_riesgo_total"], marker="o", linewidth=2.0, label="Coste riesgo")
    ax.plot(scenario_summary["scenario"], scenario_summary["inversion_requerida_total"], marker="s", linewidth=2.0, label="Inversión")
    ax.set_title("Comparación de escenarios: riesgo e inversión")
    ax.set_xlabel("Escenario")
    ax.set_ylabel("EUR proxy")
    ax.tick_params(axis="x", rotation=25)
    ax.legend()
    fig.tight_layout()
    p = paths.outputs_charts / "v2_14_comparacion_escenarios.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    # 15) Drivers de score por zona.
    c15 = scoring.sort_values("investment_priority_score", ascending=False).head(12)
    drivers = [
        "congestion_risk_score",
        "resilience_risk_score",
        "service_impact_score",
        "flexibility_gap_score",
        "asset_exposure_score",
        "electrification_pressure_score",
        "economic_priority_score",
    ]
    fig, ax = plt.subplots(figsize=(13, 6))
    x = np.arange(len(c15))
    bottom = np.zeros(len(c15))
    colors = ["#1b9e77", "#d95f02", "#7570b3", "#e7298a", "#66a61e", "#e6ab02", "#a6761d"]
    for d, c in zip(drivers, colors):
        vals = c15[d].to_numpy(dtype=float)
        ax.bar(x, vals, bottom=bottom, label=d, color=c, alpha=0.85)
        bottom += vals
    ax.set_title("Drivers de score por zona (top prioridades)")
    ax.set_xlabel("Zona")
    ax.set_ylabel("Contribución acumulada")
    ax.set_xticks(x)
    ax.set_xticklabels(c15["zona_id"], rotation=60)
    ax.legend(ncol=2, fontsize=8)
    fig.tight_layout()
    p = paths.outputs_charts / "v2_15_drivers_score_por_zona.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    chart_paths.append(str(p))

    index_lines = ["# Índice de Visualizaciones (v2)", ""]
    explanations = [
        "1. Tendencia temporal de carga relativa: evolución mensual de saturación operativa.",
        "2. Horas de congestión por zona: concentración territorial de presión.",
        "3. Heatmap hora-territorio: patrón intradía de congestión.",
        "4. Top subestaciones por exposición: focos críticos de operación.",
        "5. Top alimentadores por criticidad: activos lineales con mayor riesgo.",
        "6. ENS por zona: impacto de continuidad de servicio.",
        "7. Flexibilidad disponible vs necesidad: brecha técnica de cobertura.",
        "8. Storage capability vs stress: alineación entre soporte y riesgo.",
        "9. Impacto EV sobre carga crítica: presión EV vs percentil de carga.",
        "10. Impacto industrial: electrificación industrial frente a congestión.",
        "11. Curtailment por territorio: zonas con mayor recorte de GD.",
        "12. Riesgo técnico vs impacto económico: balance para priorización.",
        "13. Ranking de prioridades: visión ejecutiva de intervención.",
        "14. Comparación entre escenarios: sensibilidad del sistema.",
        "15. Drivers de score por zona: composición del riesgo agregado.",
    ]

    for exp, path in zip(explanations, chart_paths):
        index_lines.append(f"- {exp} -> `{path}`")

    (paths.outputs_charts / "index_visualizaciones_v2.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")

    conn.close()
    return chart_paths


if __name__ == "__main__":
    charts = run_visualization_v2()
    print(f"Charts generated: {len(charts)}")
