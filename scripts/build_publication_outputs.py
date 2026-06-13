from __future__ import annotations

import html
import os
import re
import shutil
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/grid_publication_matplotlib")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import FuncFormatter, PercentFormatter
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    HRFlowable,
    Image,
    KeepTogether,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.tableofcontents import TableOfContents


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed"
OUTPUTS = ROOT / "outputs"
GRAPHS = OUTPUTS / "graphs"
DASHBOARD = OUTPUTS / "dashboard"
REPORTS = OUTPUTS / "reports"

INK = "#17212B"
MUTED = "#63717E"
LIGHT = "#E8EDF1"
GRID = "#D9E0E5"
ACCENT = "#0B6E75"
ACCENT_LIGHT = "#9CC8CA"
DARK = "#263746"
RISK = "#B44A3A"
WHITE = "#FFFFFF"
NEUTRALS = ["#DDE4E8", "#C5D0D6", "#AAB9C1", "#84959F", "#637681", "#435761"]


def read(name: str) -> pd.DataFrame:
    return pd.read_csv(DATA / name)


def slug_label(value: str) -> str:
    return str(value).replace("_", " ").capitalize()


def fmt_int(value: float) -> str:
    return f"{value:,.0f}".replace(",", ".")


def fmt_dec(value: float, decimals: int = 1) -> str:
    return f"{value:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_m(value: float) -> str:
    return f"{value / 1_000_000:.1f} M€".replace(".", ",")


def prepare_outputs() -> None:
    OUTPUTS.mkdir(exist_ok=True)
    if (OUTPUTS / "charts").exists():
        shutil.rmtree(OUTPUTS / "charts")
    for path in OUTPUTS.iterdir():
        if path.name not in {"graphs", "dashboard", "reports"}:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
    for folder in (GRAPHS, DASHBOARD, REPORTS):
        folder.mkdir(parents=True, exist_ok=True)
    for path in GRAPHS.iterdir():
        if path.is_file():
            path.unlink()
    public_report = REPORTS / "informe_analitico_red_electrificacion.pdf"
    if public_report.exists():
        public_report.unlink()


def style_axes(ax: plt.Axes, title: str, subtitle: str = "") -> None:
    ax.set_title(title, loc="left", fontsize=15, fontweight="bold", color=INK, pad=20)
    if subtitle:
        ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=9.5, color=MUTED, va="bottom")
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.7, alpha=0.8)
    ax.set_axisbelow(True)


def save_chart(fig: plt.Figure, filename: str) -> Path:
    path = GRAPHS / filename
    fig.savefig(path, dpi=220, bbox_inches="tight", facecolor=WHITE)
    plt.close(fig)
    return path


def add_source(fig: plt.Figure, source: str) -> None:
    fig.text(0.01, 0.005, "Fuente: modelo analítico de red; datos procesados del proyecto.", fontsize=7.5, color=MUTED)


def generate_charts() -> list[Path]:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "figure.facecolor": WHITE,
            "axes.facecolor": WHITE,
            "text.color": INK,
            "axes.labelcolor": MUTED,
            "axes.titlesize": 15,
            "axes.titleweight": "bold",
        }
    )
    zone_risk = read("vw_zone_operational_risk.csv")
    scoring = read("intervention_scoring_table.csv")
    monthly = read("mart_zone_month_operational.csv")
    flex = read("vw_flexibility_gap.csv")
    scenarios = read("scenario_summary_v2.csv")
    anomalies = read("anomaly_zone_intensity.csv")
    anomaly_types = read("anomalies_summary_by_type.csv")
    forecast = read("forecast_error_by_zone.csv")
    sensitivity = read("scoring_sensitivity_analysis.csv")
    service = read("support_servicio_resiliencia.csv")
    electrification = read("support_electrificacion_presion.csv")
    feeder_priorities = read("investment_priorities.csv")

    paths: list[Path] = []

    # 01 Trend: demand and net load.
    trend = monthly.assign(mes=pd.to_datetime(monthly["mes"])).groupby("mes", as_index=False).agg(
        demanda=("demanda_total_mwh", "sum"), net_load=("net_load_total_mwh", "sum")
    )
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    style_axes(ax, "La carga neta mantiene una trayectoria elevada durante todo el horizonte",
               "Demanda bruta y carga neta mensuales, GWh")
    ax.plot(trend["mes"], trend["demanda"] / 1000, color=ACCENT, lw=2.5, label="Demanda bruta")
    ax.plot(trend["mes"], trend["net_load"] / 1000, color=DARK, lw=2, label="Carga neta")
    ax.fill_between(trend["mes"], trend["net_load"] / 1000, trend["demanda"] / 1000, color=ACCENT_LIGHT, alpha=0.35)
    ax.legend(frameon=False, ncol=2, loc="upper left")
    ax.set_ylabel("GWh")
    add_source(fig, "Fuente: mart_zone_month_operational.csv")
    paths.append(save_chart(fig, "01_tendencia_demanda_carga_neta.png"))

    # 02 Trend: congestion and operational stress.
    trend2 = monthly.assign(mes=pd.to_datetime(monthly["mes"])).groupby("mes", as_index=False).agg(
        congestion=("horas_congestion", "sum"), stress=("horas_estres_operativo", "sum")
    )
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    style_axes(ax, "El estrés operativo permanece por encima de la congestión confirmada",
               "Horas-zona mensuales; la distancia identifica presión previa a congestión")
    ax.plot(trend2["mes"], trend2["stress"], color=DARK, lw=2.2, label="Estrés operativo")
    ax.plot(trend2["mes"], trend2["congestion"], color=ACCENT, lw=2.5, label="Congestión")
    ax.fill_between(trend2["mes"], trend2["congestion"], trend2["stress"], color=ACCENT_LIGHT, alpha=0.35)
    ax.legend(frameon=False, ncol=2, loc="upper left")
    ax.set_ylabel("Horas-zona")
    add_source(fig, "Fuente: mart_zone_month_operational.csv")
    paths.append(save_chart(fig, "02_evolucion_congestion_estres.png"))

    # 03 Concentration: Pareto.
    pareto = zone_risk.sort_values("horas_congestion", ascending=False).reset_index(drop=True)
    pareto["share_acum"] = pareto["horas_congestion"].cumsum() / pareto["horas_congestion"].sum()
    fig, ax = plt.subplots(figsize=(11.5, 6))
    style_axes(ax, "La congestión está concentrada, pero no limitada a una única zona",
               "Horas acumuladas y participación acumulada por zona")
    cols = [ACCENT if i < 5 else NEUTRALS[1] for i in range(len(pareto))]
    ax.bar(pareto["zona_id"], pareto["horas_congestion"], color=cols)
    ax.set_ylabel("Horas de congestión")
    ax.tick_params(axis="x", rotation=55)
    ax2 = ax.twinx()
    ax2.plot(pareto["zona_id"], pareto["share_acum"], color=RISK, lw=2, marker="o", ms=3)
    ax2.yaxis.set_major_formatter(PercentFormatter(1))
    ax2.set_ylim(0, 1.05)
    ax2.spines[["top", "left"]].set_visible(False)
    ax2.tick_params(colors=MUTED, length=0)
    add_source(fig, "Fuente: vw_zone_operational_risk.csv")
    paths.append(save_chart(fig, "03_concentracion_congestion_pareto.png"))

    # 04 Ranking.
    rank = scoring.nsmallest(12, "priority_rank").sort_values("investment_priority_score")
    fig, ax = plt.subplots(figsize=(11.5, 6.4))
    style_axes(ax, "Z013 exige una decisión inmediata y separa claramente del resto de la cartera",
               "12 zonas con mayor índice de prioridad de inversión")
    colors_rank = [ACCENT if z == "Z013" else DARK for z in rank["zona_id"]]
    bars = ax.barh(rank["zona_id"], rank["investment_priority_score"], color=colors_rank)
    ax.bar_label(bars, fmt="%.1f", padding=4, color=MUTED, fontsize=9)
    ax.set_xlabel("Índice 0-100")
    ax.set_xlim(0, 100)
    add_source(fig, "Fuente: intervention_scoring_table.csv")
    paths.append(save_chart(fig, "04_ranking_prioridad_zonas.png"))

    # 05 Composition.
    comp = scoring["recommended_intervention"].value_counts().sort_values()
    fig, ax = plt.subplots(figsize=(11.5, 6))
    style_axes(ax, "La cartera combina mitigación rápida con cinco refuerzos estructurales",
               "Número de zonas por intervención recomendada")
    bars = ax.barh([slug_label(v) for v in comp.index], comp.values, color=[NEUTRALS[2]] * (len(comp) - 1) + [ACCENT])
    ax.bar_label(bars, padding=4, color=MUTED)
    ax.set_xlabel("Zonas")
    add_source(fig, "Fuente: intervention_scoring_table.csv")
    paths.append(save_chart(fig, "05_composicion_cartera_intervenciones.png"))

    # 06 Funnel.
    high_risk = scoring[scoring["risk_tier"].isin(["alto", "critico"])]
    urgent_high_risk = high_risk[high_risk["urgency_tier"].isin(["alta", "inmediata"])]
    short_term_urgent = urgent_high_risk[urgent_high_risk["recommended_sequence"].isin(["0-3m", "0-6m"])]
    funnel_labels = ["Zonas analizadas", "Riesgo alto o crítico", "Urgencia alta o inmediata", "Decisión 0-6 meses"]
    funnel_values = [
        len(scoring),
        len(high_risk),
        len(urgent_high_risk),
        len(short_term_urgent),
    ]
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    style_axes(ax, "La evaluación reduce 24 zonas a una decisión urgente de corto plazo",
               "Embudo de priorización y secuenciación")
    y = np.arange(len(funnel_labels))
    widths = np.array(funnel_values)
    left = (widths.max() - widths) / 2
    bars = ax.barh(y, widths, left=left, color=[NEUTRALS[1], NEUTRALS[2], DARK, ACCENT], height=0.65)
    ax.set_yticks(y, funnel_labels)
    ax.invert_yaxis()
    ax.set_xticks([])
    ax.grid(False)
    for bar, val in zip(bars, funnel_values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_y() + bar.get_height() / 2, str(val),
                ha="center", va="center", color=WHITE if val < 15 else INK, fontweight="bold", fontsize=12)
    add_source(fig, "Fuente: intervention_scoring_table.csv")
    paths.append(save_chart(fig, "06_funnel_priorizacion.png"))

    # 07 Distribution.
    order = scoring.groupby("risk_tier")["investment_priority_score"].median().sort_values().index
    groups = [scoring.loc[scoring["risk_tier"] == tier, "investment_priority_score"] for tier in order]
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    style_axes(ax, "Los tiers de riesgo separan de forma consistente la intensidad de decisión",
               "Distribución del score de prioridad por tier de riesgo")
    box = ax.boxplot(groups, tick_labels=[slug_label(v) for v in order], patch_artist=True, widths=0.55)
    for patch, color in zip(box["boxes"], [NEUTRALS[1], NEUTRALS[2], DARK, ACCENT][-len(groups):]):
        patch.set_facecolor(color)
    for median in box["medians"]:
        median.set_color(WHITE)
        median.set_linewidth(2)
    ax.set_ylabel("Score de prioridad")
    add_source(fig, "Fuente: intervention_scoring_table.csv")
    paths.append(save_chart(fig, "07_distribucion_score_por_riesgo.png"))

    # 08 Correlation.
    drivers = [
        "congestion_risk_score", "resilience_risk_score", "service_impact_score",
        "flexibility_gap_score", "asset_exposure_score", "electrification_pressure_score",
        "economic_priority_score", "investment_priority_score",
    ]
    corr = scoring[drivers].corr()
    labels = ["Congestión", "Resiliencia", "Servicio", "Flexibilidad", "Activos", "Electrificación", "Economía", "Prioridad"]
    fig, ax = plt.subplots(figsize=(9, 7.2))
    cmap = LinearSegmentedColormap.from_list("neutral_accent", ["#EEF2F4", ACCENT])
    im = ax.imshow(corr, vmin=-1, vmax=1, cmap=cmap)
    ax.set_xticks(range(len(labels)), labels, rotation=45, ha="right")
    ax.set_yticks(range(len(labels)), labels)
    ax.set_title("La prioridad final está más asociada a congestión, servicio y brecha flexible", loc="left", fontsize=14, pad=18)
    for i in range(len(labels)):
        for j in range(len(labels)):
            ax.text(j, i, f"{corr.iloc[i, j]:.2f}", ha="center", va="center", fontsize=8,
                    color=WHITE if corr.iloc[i, j] > 0.65 else INK)
    fig.colorbar(im, ax=ax, fraction=0.035, pad=0.03)
    add_source(fig, "Fuente: intervention_scoring_table.csv")
    paths.append(save_chart(fig, "08_correlacion_drivers_prioridad.png"))

    # 09 Risk matrix.
    fig, ax = plt.subplots(figsize=(11.5, 6.3))
    style_axes(ax, "El mayor riesgo coincide con coberturas flexibles insuficientes",
               "Riesgo operativo frente a ratio flexibilidad/estrés; tamaño = gap técnico")
    sizes = 50 + 350 * (flex["gap_tecnico_mw"] / flex["gap_tecnico_mw"].max())
    ax.scatter(flex["ratio_flexibilidad_estres"], flex["riesgo_operativo_score"], s=sizes,
               c=[ACCENT if z in scoring.nsmallest(6, "priority_rank")["zona_id"].tolist() else NEUTRALS[2] for z in flex["zona_id"]],
               alpha=0.85, edgecolor=WHITE, linewidth=0.8)
    for _, row in flex.nlargest(8, "riesgo_operativo_score").iterrows():
        ax.annotate(row["zona_id"], (row["ratio_flexibilidad_estres"], row["riesgo_operativo_score"]),
                    xytext=(4, 4), textcoords="offset points", fontsize=8, color=INK)
    ax.axvline(0.15, color=RISK, ls="--", lw=1.2)
    ax.set_xlabel("Ratio flexibilidad / estrés")
    ax.set_ylabel("Riesgo operativo")
    add_source(fig, "Fuente: vw_flexibility_gap.csv")
    paths.append(save_chart(fig, "09_matriz_riesgo_flexibilidad.png"))

    # 10 Service resilience.
    top_service = service.nlargest(12, "ens_total").sort_values("ens_total")
    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    style_axes(ax, "La ENS confirma una exposición de servicio distinta de la congestión pura",
               "12 zonas con mayor energía no suministrada acumulada")
    bars = ax.barh(top_service["zona_id"], top_service["ens_total"], color=[ACCENT if z == "Z013" else DARK for z in top_service["zona_id"]])
    ax.bar_label(bars, labels=[fmt_int(v) for v in top_service["ens_total"]], padding=4, fontsize=8, color=MUTED)
    ax.set_xlabel("ENS (MWh)")
    add_source(fig, "Fuente: support_servicio_resiliencia.csv")
    paths.append(save_chart(fig, "10_ranking_ens_servicio.png"))

    # 11 Geography / operational regions.
    region = zone_risk.groupby("region_operativa", as_index=False).agg(
        riesgo=("riesgo_operativo_score", "mean"), congestion=("horas_congestion", "sum"),
        ens=("ens_total_mwh", "sum"), zonas=("zona_id", "nunique")
    ).sort_values("riesgo")
    top_regions = region.sort_values("riesgo", ascending=False)["region_operativa"].head(2).tolist()
    fig, ax = plt.subplots(figsize=(11.5, 6))
    style_axes(ax, f"{top_regions[0]} y {top_regions[1]} concentran la mayor presión operativa media",
               "Riesgo operativo medio por región operativa")
    region_colors = [ACCENT if r in top_regions else NEUTRALS[2] for r in region["region_operativa"]]
    bars = ax.barh(region["region_operativa"], region["riesgo"], color=region_colors)
    ax.bar_label(bars, fmt="%.1f", padding=4, color=MUTED)
    ax.set_xlabel("Riesgo operativo medio")
    add_source(fig, "Fuente: vw_zone_operational_risk.csv")
    paths.append(save_chart(fig, "11_geografia_riesgo_regiones.png"))

    # 12 Grupos territoriales.
    cohort = zone_risk.groupby("tipo_zona", as_index=False).agg(
        riesgo=("riesgo_operativo_score", "mean"), congestion=("horas_congestion", "mean"),
        ens=("ens_total_mwh", "mean"), carga=("carga_punta_mw", "mean")
    )
    for col in ["riesgo", "congestion", "ens", "carga"]:
        cohort[col] = 100 * cohort[col] / cohort[col].max()
    matrix = cohort.set_index("tipo_zona")[["riesgo", "congestion", "ens", "carga"]]
    fig, ax = plt.subplots(figsize=(10, 5.8))
    im = ax.imshow(matrix, cmap=LinearSegmentedColormap.from_list("c", ["#EEF2F4", ACCENT]), vmin=0, vmax=100)
    ax.set_title("Los grupos territoriales presentan perfiles de presión materialmente distintos", loc="left", fontsize=14, pad=18)
    ax.set_xticks(range(4), ["Riesgo", "Congestión", "ENS", "Carga punta"])
    ax.set_yticks(range(len(matrix)), [slug_label(v) for v in matrix.index])
    for i in range(len(matrix)):
        for j in range(4):
            ax.text(j, i, f"{matrix.iloc[i, j]:.0f}", ha="center", va="center",
                    color=WHITE if matrix.iloc[i, j] > 62 else INK, fontweight="bold")
    fig.colorbar(im, ax=ax, label="Índice relativo, máximo = 100", fraction=0.035, pad=0.03)
    add_source(fig, "Fuente: vw_zone_operational_risk.csv")
    paths.append(save_chart(fig, "12_grupos_tipo_zona.png"))

    # 13 Electrification correlation.
    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    style_axes(ax, "La nueva demanda explica presión adicional, pero no determina sola la congestión",
               "Ratio de nueva demanda frente a horas de congestión")
    ax.scatter(electrification["ratio_nueva_demanda"], electrification["horas_congestion"],
               s=90, color=ACCENT, alpha=0.8, edgecolor=WHITE)
    for _, row in electrification.nlargest(7, "ratio_nueva_demanda").iterrows():
        ax.annotate(row["zona_id"], (row["ratio_nueva_demanda"], row["horas_congestion"]),
                    xytext=(4, 4), textcoords="offset points", fontsize=8)
    coef = np.polyfit(electrification["ratio_nueva_demanda"], electrification["horas_congestion"], 1)
    xx = np.linspace(electrification["ratio_nueva_demanda"].min(), electrification["ratio_nueva_demanda"].max(), 50)
    ax.plot(xx, coef[0] * xx + coef[1], color=DARK, lw=1.5)
    ax.xaxis.set_major_formatter(PercentFormatter(1))
    ax.set_xlabel("Nueva demanda / demanda total")
    ax.set_ylabel("Horas de congestión")
    add_source(fig, "Fuente: support_electrificacion_presion.csv")
    paths.append(save_chart(fig, "13_electrificacion_vs_congestion.png"))

    # 14 Anomalies composition and precursor.
    at = anomaly_types.sort_values("n_eventos")
    fig, ax = plt.subplots(figsize=(11.5, 6.1))
    style_axes(ax, "Las anomalías de carga dominan el volumen y actúan como señal precursora",
               "Eventos por tipo; etiqueta indica porcentaje precursor de congestión")
    bars = ax.barh([slug_label(v) for v in at["anomaly_type"]], at["n_eventos"], color=[NEUTRALS[2], DARK, ACCENT, NEUTRALS[1], NEUTRALS[3]][:len(at)])
    labels_anom = [f"{fmt_int(n)} | {p:.0%} precursor" for n, p in zip(at["n_eventos"], at["pct_precursor_congestion"])]
    ax.bar_label(bars, labels=labels_anom, padding=4, fontsize=8, color=MUTED)
    ax.set_xlabel("Eventos")
    add_source(fig, "Fuente: anomalies_summary_by_type.csv")
    paths.append(save_chart(fig, "14_anomalias_y_senal_precursora.png"))

    # 15 Forecast accuracy.
    fc = forecast.sort_values("nmae")
    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    style_axes(ax, "La precisión del pronóstico es estable entre zonas",
               "Error normalizado por zona; intervalo total 2,39%-3,10%")
    ax.bar(fc["zona_id"], fc["nmae"], color=[ACCENT if v == fc["nmae"].max() else NEUTRALS[2] for v in fc["nmae"]])
    ax.axhline(0.035, color=RISK, ls="--", lw=1.2, label="Umbral de confianza: 3,5%")
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    ax.tick_params(axis="x", rotation=55)
    ax.set_ylabel("NMAE")
    ax.legend(frameon=False, loc="upper left")
    add_source(fig, "Fuente: forecast_error_by_zone.csv")
    paths.append(save_chart(fig, "15_precision_forecast_por_zona.png"))

    # 16 Scenarios.
    sc = scenarios.sort_values("coste_riesgo_total", ascending=False)
    fig, ax = plt.subplots(figsize=(11.5, 6.4))
    style_axes(ax, "CAPEX más flexibilidad reduce el coste de riesgo proxy frente al retraso",
               "Coste de riesgo por escenario; M€ de proxy relativo")
    colors_sc = [RISK if s == "retraso_capex" else ACCENT if s == "capex_mas_flexibilidad" else NEUTRALS[2] for s in sc["scenario"]]
    bars = ax.barh([slug_label(v) for v in sc["scenario"]], sc["coste_riesgo_total"] / 1_000_000, color=colors_sc)
    ax.bar_label(bars, labels=[f"{v/1_000_000:.2f}" for v in sc["coste_riesgo_total"]], padding=4, fontsize=8, color=MUTED)
    ax.set_xlabel("M€ proxy")
    add_source(fig, "Fuente: scenario_summary_v2.csv")
    paths.append(save_chart(fig, "16_comparacion_escenarios_riesgo.png"))

    # 17 Before vs after proxy.
    compare_names = ["Retraso de CAPEX", "Almacenamiento adicional", "Flexibilidad adicional", "CAPEX + flexibilidad"]
    lookup = scenarios.set_index("scenario")
    compare_keys = ["retraso_capex", "despliegue_adicional_storage", "despliegue_adicional_flexibilidad", "capex_mas_flexibilidad"]
    worst = float(lookup.loc["retraso_capex", "coste_riesgo_total"])
    reduction = [(worst - float(lookup.loc[k, "coste_riesgo_total"])) / worst for k in compare_keys]
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    style_axes(ax, "Las palancas combinadas capturan la mayor reducción de riesgo",
               "Reducción frente al escenario de retraso de CAPEX")
    bars = ax.bar(compare_names, reduction, color=[NEUTRALS[2], DARK, DARK, ACCENT])
    ax.bar_label(bars, labels=[f"{v:.0%}" for v in reduction], padding=4, color=MUTED)
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    ax.set_ylim(0, max(reduction) * 1.2)
    ax.tick_params(axis="x", rotation=15)
    add_source(fig, "Fuente: scenario_summary_v2.csv")
    paths.append(save_chart(fig, "17_antes_despues_reduccion_riesgo.png"))

    # 18 Variance / sensitivity.
    base_rank = scoring.set_index("zona_id")["priority_rank"]
    sens = sensitivity.pivot(index="zona_id", columns="factor", values="rank_alt")
    sens["amplitude"] = sens.max(axis=1) - sens.min(axis=1)
    sens["base"] = base_rank
    top_sens = sens.nlargest(12, "amplitude").sort_values("amplitude")
    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    style_axes(ax, "La sensibilidad de ranking está contenida en las zonas prioritarias",
               "Amplitud de posición bajo factores alternativos")
    bars = ax.barh(top_sens.index, top_sens["amplitude"], color=[ACCENT if int(top_sens.loc[z, "base"]) <= 6 else DARK for z in top_sens.index])
    ax.bar_label(bars, fmt="%.0f", padding=4, color=MUTED)
    ax.set_xlabel("Posiciones de variación")
    add_source(fig, "Fuente: scoring_sensitivity_analysis.csv")
    paths.append(save_chart(fig, "18_variacion_sensibilidad_ranking.png"))

    # 19 Feeder investment ranking.
    feeders = feeder_priorities.head(15).sort_values("priority_score")
    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    style_axes(ax, "La priorización zonal se traduce en una lista concreta de alimentadores",
               "15 alimentadores con mayor prioridad")
    bars = ax.barh(feeders["feeder_id"], feeders["priority_score"], color=[ACCENT if t == "Alta" else DARK for t in feeders["priority_tier"]])
    ax.bar_label(bars, fmt="%.1f", padding=4, color=MUTED, fontsize=8)
    ax.set_xlabel("Score de prioridad")
    add_source(fig, "Fuente: investment_priorities.csv")
    paths.append(save_chart(fig, "19_ranking_alimentadores.png"))

    return paths


def make_dashboard_standalone() -> Path:
    dashboard_path = DASHBOARD / "grid-electrification-command-center.html"
    if not dashboard_path.exists():
        raise FileNotFoundError(f"Dashboard no encontrado: {dashboard_path}")
    text = dashboard_path.read_text(encoding="utf-8")
    chart_js = (ROOT / "src" / "assets" / "chart.umd.min.js").read_text(encoding="utf-8")
    chart_js = chart_js.replace("</script", "<\\/script")
    text = re.sub(r"\s*<link rel=\"preconnect\"[^>]+/>\s*", "\n", text)
    text = re.sub(r"\s*<link href=\"https://fonts\.googleapis\.com[^\"]+\"[^>]+/>\s*", "\n", text)
    text = re.sub(
        r'<script src="../../src/assets/chart\.umd\.min\.js" defer></script>',
        lambda _: f"<script>{chart_js}</script>",
        text,
    )
    dashboard_path.write_text(text, encoding="utf-8")
    return dashboard_path


class ReportDocTemplate(BaseDocTemplate):
    def __init__(self, filename: str, **kwargs):
        super().__init__(filename, **kwargs)
        frame = Frame(self.leftMargin, self.bottomMargin, self.width, self.height, id="normal")
        self.addPageTemplates(PageTemplate(id="main", frames=frame, onPage=self._header_footer))

    def _header_footer(self, canvas, doc):
        if doc.page == 1:
            return
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor(LIGHT))
        canvas.setLineWidth(0.5)
        canvas.line(self.leftMargin, A4[1] - 1.15 * cm, A4[0] - self.rightMargin, A4[1] - 1.15 * cm)
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(colors.HexColor(MUTED))
        canvas.drawString(self.leftMargin, 0.75 * cm, "Sistema de Inteligencia de Red para Electrificación Territorial")
        canvas.drawRightString(A4[0] - self.rightMargin, 0.75 * cm, f"{doc.page}")
        canvas.restoreState()

    def afterFlowable(self, flowable):
        if isinstance(flowable, Paragraph) and flowable.style.name == "H1":
            level = 0
            text = flowable.getPlainText()
            key = f"h{level}-{self.page}-{abs(hash(text))}"
            self.canv.bookmarkPage(key)
            self.canv.addOutlineEntry(text, key, level=level, closed=False)
            self.notify("TOCEntry", (level, text, self.page, key))


def report_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name="CoverKicker", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=9,
        leading=12, textColor=colors.HexColor(ACCENT), alignment=TA_CENTER, spaceAfter=14,
    ))
    styles.add(ParagraphStyle(
        name="CoverTitle", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=29,
        leading=34, textColor=colors.HexColor(INK), alignment=TA_CENTER, spaceAfter=16,
    ))
    styles.add(ParagraphStyle(
        name="CoverSubtitle", parent=styles["Normal"], fontName="Helvetica", fontSize=13,
        leading=19, textColor=colors.HexColor(MUTED), alignment=TA_CENTER, spaceAfter=18,
    ))
    styles.add(ParagraphStyle(
        name="H1", parent=styles["Heading1"], fontName="Helvetica-Bold", fontSize=19,
        leading=23, textColor=colors.HexColor(INK), spaceBefore=4, spaceAfter=12,
    ))
    styles.add(ParagraphStyle(
        name="H2", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=13.5,
        leading=17, textColor=colors.HexColor(ACCENT), spaceBefore=8, spaceAfter=8,
    ))
    styles.add(ParagraphStyle(
        name="Body", parent=styles["BodyText"], fontName="Helvetica", fontSize=9.4,
        leading=13.2, textColor=colors.HexColor(INK), alignment=TA_LEFT, spaceAfter=8,
    ))
    styles.add(ParagraphStyle(
        name="Lead", parent=styles["BodyText"], fontName="Helvetica", fontSize=11,
        leading=15, textColor=colors.HexColor(DARK), spaceAfter=12,
    ))
    styles.add(ParagraphStyle(
        name="Small", parent=styles["BodyText"], fontName="Helvetica", fontSize=7.5,
        leading=10, textColor=colors.HexColor(MUTED), spaceAfter=4,
    ))
    styles.add(ParagraphStyle(
        name="Callout", parent=styles["BodyText"], fontName="Helvetica-Bold", fontSize=11,
        leading=15, textColor=colors.HexColor(ACCENT), borderColor=colors.HexColor(ACCENT_LIGHT),
        borderWidth=0.8, borderPadding=10, backColor=colors.HexColor("#EFF7F7"), spaceAfter=12,
    ))
    styles.add(ParagraphStyle(
        name="TableHead", parent=styles["BodyText"], fontName="Helvetica-Bold", fontSize=7.5,
        leading=9, textColor=colors.white,
    ))
    styles.add(ParagraphStyle(
        name="TableBody", parent=styles["BodyText"], fontName="Helvetica", fontSize=7.2,
        leading=9, textColor=colors.HexColor(INK),
    ))
    return styles


def p(text: str, styles, style: str = "Body") -> Paragraph:
    return Paragraph(html.escape(text).replace("\n", "<br/>"), styles[style])


def chart(path: Path, width: float = 17.2 * cm) -> Image:
    img = Image(str(path))
    img._restrictSize(width, 13.0 * cm)
    img.hAlign = "CENTER"
    return img


def data_table(rows: list[list[str]], widths: list[float], styles) -> Table:
    converted = []
    for ridx, row in enumerate(rows):
        converted.append([Paragraph(html.escape(str(cell)), styles["TableHead" if ridx == 0 else "TableBody"]) for cell in row])
    table = Table(converted, colWidths=widths, repeatRows=1, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(DARK)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("BACKGROUND", (0, 1), (-1, -1), colors.white),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F5F7F8")]),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor(GRID)),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    return table


def page_section(story, styles, title: str, lead: str, paragraphs: list[str],
                 visual: Path | None = None, table: Table | None = None, callout: str | None = None) -> None:
    story.append(Paragraph(title, styles["H1"]))
    story.append(p(lead, styles, "Lead"))
    if callout:
        story.append(p(callout, styles, "Callout"))
    for paragraph in paragraphs:
        story.append(p(paragraph, styles))
    if visual:
        story.append(Spacer(1, 4))
        story.append(chart(visual))
    if table:
        story.append(Spacer(1, 6))
        story.append(table)
    story.append(PageBreak())


def build_report() -> Path:
    styles = report_styles()
    zone_risk = read("vw_zone_operational_risk.csv")
    scoring = read("intervention_scoring_table.csv").sort_values("priority_rank")
    scenarios = read("scenario_summary_v2.csv")
    forecast = read("forecast_error_by_zone.csv")
    anomaly_types = read("anomalies_summary_by_type.csv")
    flex = read("vw_flexibility_gap.csv")
    feeder_priorities = read("investment_priorities.csv")
    checks = read("validation_checks_sql_v2.csv")
    monthly = read("mart_zone_month_operational.csv")

    total_congestion = zone_risk["horas_congestion"].sum()
    total_ens = zone_risk["ens_total_mwh"].sum()
    top = scoring.iloc[0]
    top5_share = zone_risk.nlargest(5, "horas_congestion")["horas_congestion"].sum() / total_congestion
    best_scenario = scenarios.loc[scenarios["coste_riesgo_total"].idxmin()]
    worst_scenario = scenarios.loc[scenarios["coste_riesgo_total"].idxmax()]
    scenario_delta = worst_scenario["coste_riesgo_total"] / best_scenario["coste_riesgo_total"] - 1
    charts = {path.name.split("_", 1)[0]: path for path in sorted(GRAPHS.glob("*.png"))}

    pdf_path = REPORTS / "informe_analitico_red_electrificacion.pdf"
    doc = ReportDocTemplate(
        str(pdf_path), pagesize=A4, rightMargin=1.7 * cm, leftMargin=1.7 * cm,
        topMargin=1.65 * cm, bottomMargin=1.35 * cm, title="Informe analítico de red y electrificación",
        author="Sistema de Inteligencia de Red",
    )
    story = []

    # Cover.
    story.extend([
        Spacer(1, 4.2 * cm),
        Paragraph("INFORME ANALÍTICO", styles["CoverKicker"]),
        Paragraph("Sistema de Inteligencia de Red para Electrificación Territorial", styles["CoverTitle"]),
        Paragraph("Priorización de riesgo, flexibilidad e inversión bajo presión de electrificación", styles["CoverSubtitle"]),
        Spacer(1, 1.2 * cm),
        p("Horizonte analizado: 24 meses | Cobertura: 24 zonas | Versión de publicación: junio de 2026", styles, "Small"),
        Spacer(1, 4.0 * cm),
        p("Documento de soporte a decisión. Los datos y costes son sintéticos y los importes económicos son proxies relativos. El informe no sustituye estudios eléctricos detallados ni aprobación regulatoria de inversión.", styles, "Small"),
        PageBreak(),
    ])

    # TOC.
    story.append(Paragraph("Índice", styles["H1"]))
    toc = TableOfContents()
    toc.levelStyles = [
        ParagraphStyle(name="TOC1", fontName="Helvetica-Bold", fontSize=9.5, leading=15, leftIndent=0, textColor=colors.HexColor(INK)),
        ParagraphStyle(name="TOC2", fontName="Helvetica", fontSize=8.5, leading=13, leftIndent=14, textColor=colors.HexColor(MUTED)),
    ]
    story.append(toc)
    story.append(PageBreak())

    page_section(
        story, styles, "1. Resumen ejecutivo",
        f"La red acumula {fmt_int(total_congestion)} horas-zona de congestión y {fmt_dec(total_ens, 0)} MWh de energía no suministrada en el horizonte analizado.",
        [
            f"La presión no es uniforme. Las cinco zonas con mayor congestión concentran {top5_share:.1%} del total, mientras Z013 lidera la cartera con un score de prioridad de {fmt_dec(top['investment_priority_score'])}, riesgo crítico y secuencia inmediata de 0 a 6 meses.",
            "La cartera recomendada no responde con una única tecnología. Combina una intervención inmediata, cinco refuerzos locales, dos despliegues de almacenamiento, cuatro activaciones de flexibilidad, cinco optimizaciones operativas, una sustitución de activos y seis zonas en monitorización.",
            f"Los escenarios confirman el coste de posponer decisiones. El escenario de retraso de CAPEX presenta un coste de riesgo proxy {scenario_delta:.1%} superior al escenario de menor riesgo. La combinación de CAPEX y flexibilidad ofrece la menor exposición residual, aunque exige validar la secuencia física y contractual por zona.",
        ],
        callout="Decisión central: abrir diagnóstico técnico inmediato en Z013 y lanzar en paralelo estudios de refuerzo para Z021, Z020, Z016, Z015 y Z019.",
    )

    page_section(
        story, styles, "1.1 Decisiones prioritarias",
        "La prioridad inmediata consiste en separar medidas reversibles de corto plazo de compromisos estructurales de capital.",
        [
            "Z013 debe pasar de screening analítico a diagnóstico técnico detallado. Su combinación de congestión, impacto de servicio, brecha flexible y anomalías críticas justifica una revisión específica de topología, protecciones, contingencias y alternativas de alivio.",
            "Las cinco zonas recomendadas para refuerzo local muestran congestión estructural y cobertura flexible insuficiente. En estas zonas, flexibilidad y operación avanzada deben reducir exposición durante el desarrollo del refuerzo, no sustituir la solución estructural.",
            "Z001 y Z024 forman un bloque distinto. Su brecha de flexibilidad, impacto de servicio y soporte de almacenamiento limitado justifican validar dimensionamiento, ubicación, duración y régimen operativo de almacenamiento antes de comprometer CAPEX.",
        ],
        visual=charts["06"],
    )

    page_section(
        story, styles, "2. Contexto y objetivos",
        "La electrificación territorial aumenta carga, modifica perfiles horarios y amplifica el valor de una priorización que combine red, servicio y economía.",
        [
            "El sistema responde a una pregunta operativa concreta: dónde está la red perdiendo capacidad, resiliencia y eficiencia económica, y qué combinación de refuerzo, flexibilidad, almacenamiento u operación debe activarse primero.",
            "La unidad de decisión principal es la zona. El análisis conserva trazabilidad hacia subestaciones y alimentadores, pero consolida la priorización a un nivel territorial capaz de integrar congestión, continuidad de suministro, activos, nueva demanda, flexibilidad y factibilidad.",
            "El objetivo no es producir un presupuesto definitivo. El objetivo es reducir el espacio de decisión, distinguir urgencias y ordenar los estudios técnicos y comerciales que deben preceder a una aprobación de inversión.",
        ],
    )

    page_section(
        story, styles, "2.1 Preguntas de decisión",
        "Cada visual y cada métrica del informe responde a una pregunta explícita de gestión.",
        [
            "Primero, se evalúa si el estrés es transitorio o persistente. Segundo, se identifica dónde se concentra la exposición. Tercero, se determina si la respuesta adecuada es estructural, flexible u operativa. Cuarto, se compara la robustez de la cartera bajo escenarios alternativos.",
            "El marco evita interpretar congestión como única señal de riesgo. Una zona puede requerir intervención por ENS elevada, activos expuestos, crecimiento de demanda, brecha flexible o una combinación de factores que no aparece en un ranking unidimensional.",
            "La salida final es una secuencia 0 a 24 meses, con acciones diferenciadas y un nivel de confianza explícito. Esta estructura permite asignar responsables, lanzar estudios y revisar decisiones sin perder trazabilidad analítica.",
        ],
    )

    page_section(
        story, styles, "3. Datos y metodología",
        "El análisis utiliza marts procesados y gobernados que cubren demanda, congestión, servicio, activos, flexibilidad, forecast, anomalías, escenarios e inversión.",
        [
            f"La capa procesada incluye {fmt_int(len(monthly))} observaciones zona-mes, {fmt_int(len(feeder_priorities))} alimentadores priorizados y {fmt_int(len(scoring))} zonas con scoring final. La granularidad operativa de origen alcanza millones de observaciones horarias.",
            "Las métricas de congestión zonal cuentan horas distintas con al menos un nodo congestionado. La demanda horaria ya incorpora EV e industrial, por lo que estos componentes no se suman de nuevo. Los scores publicados están acotados entre 0 y 100.",
            "La calidad se controla mediante validaciones de claves, no negatividad, alineación de cardinalidades, límites de scores, consistencia de rankings y diferenciación de escenarios.",
        ],
    )

    page_section(
        story, styles, "3.1 Marco de scoring",
        "La prioridad combina urgencia técnica con una evaluación multicriterio de alternativas.",
        [
            "El score final integra riesgo de congestión, resiliencia, impacto de servicio, brecha de flexibilidad, exposición de activos, presión de electrificación y prioridad económica. La ponderación favorece urgencia, pero preserva una comparación explícita entre opciones.",
            "Las alternativas consideradas son refuerzo de red, flexibilidad, almacenamiento e intervención operativa. Cada alternativa se valora por impacto esperado, coste proxy, tiempo de despliegue, urgencia y robustez.",
            "Reglas de negocio refuerzan la interpretación. Riesgo crítico fuerza intervención inmediata; congestión elevada con cobertura flexible baja fuerza refuerzo local; brecha flexible alta con impacto de servicio y poco almacenamiento fuerza despliegue de almacenamiento.",
        ],
        visual=charts["08"],
    )

    page_section(
        story, styles, "3.2 Forecast y confianza",
        "La incertidumbre de demanda está contenida y permite usar el forecast como soporte de secuenciación, no como garantía de resultado.",
        [
            f"El NMAE por zona varía entre {forecast['nmae'].min():.2%} y {forecast['nmae'].max():.2%}. Todas las zonas se mantienen por debajo del umbral de confianza de 3,5% definido por el modelo.",
            "Esta estabilidad reduce el riesgo de que el ranking sea impulsado por diferencias extremas de previsibilidad entre territorios. No elimina, sin embargo, la necesidad de actualizar previsiones con datos SCADA/AMI reales e hipótesis de adopción de EV e industrial.",
            "La confianza publicada significa que el forecast es suficiente para orientar el timing relativo de las decisiones. No significa que pueda sustituir un estudio de capacidad, un flujo de carga o un análisis de contingencias.",
        ],
        visual=charts["15"],
    )

    page_section(
        story, styles, "3.3 Anomalías y señales precursoras",
        "La detección de anomalías añade una señal temprana que complementa la congestión confirmada y las interrupciones observadas.",
        [
            f"El sistema identifica {fmt_int(anomaly_types['n_eventos'].sum())} eventos anómalos. Las anomalías de carga relativa representan la mayor parte del volumen y muestran una asociación relevante con congestión posterior.",
            "La señal debe interpretarse como indicador de priorización, no como prueba causal. La asociación puede reflejar estacionalidad, condiciones operativas, indisponibilidades o cambios de demanda no capturados por el modelo base.",
            "En zonas prioritarias, especialmente Z013, la elevada severidad y el número de anomalías críticas justifican revisar curvas de carga, alarmas y eventos operativos antes de seleccionar la medida final.",
        ],
        visual=charts["14"],
    )

    page_section(
        story, styles, "4. Marco analítico",
        "La lectura se organiza desde salud general de red hasta una cartera accionable y resistente a escenarios.",
        [
            "El primer bloque analiza tendencia y persistencia. El segundo mide concentración territorial y exposición de servicio. El tercero evalúa brecha flexible, electrificación, activos y anomalías. El cuarto convierte señales en intervenciones y secuencias.",
            "Esta progresión evita decisiones basadas en una fotografía aislada. Una zona entra en prioridad cuando el patrón aparece de forma coherente en varias dimensiones o cuando una señal crítica supera un umbral de negocio explícito.",
            "La robustez se verifica comparando escenarios y sensibilidad de ranking. Las zonas que mantienen prioridad bajo variaciones de factores merecen avanzar antes a diagnóstico, mientras posiciones volátiles requieren más evidencia antes de comprometer capital.",
        ],
    )

    page_section(
        story, styles, "5. Salud operativa de la red",
        "La demanda y la carga neta permanecen elevadas durante el horizonte, sin una reversión estructural que alivie por sí sola la presión.",
        [
            "La diferencia entre demanda bruta y carga neta representa el efecto agregado de generación distribuida y otros ajustes operativos. Ese diferencial reduce carga, pero no elimina picos ni exposición territorial.",
            "El patrón mensual sugiere que la planificación debe considerar persistencia y estacionalidad. Medidas diseñadas únicamente para un pico anual pueden no resolver periodos repetidos de carga alta y congestión.",
            "La evolución también refuerza la necesidad de seguimiento continuo. Decisiones de monitorización deben incluir umbrales claros de escalada cuando carga, congestión o ENS superen el rango esperado.",
        ],
        visual=charts["01"],
    )

    page_section(
        story, styles, "5.1 Estrés antes de congestión",
        "Las horas de estrés operativo superan de forma consistente las horas de congestión confirmada.",
        [
            "La distancia entre ambas curvas funciona como indicador de presión latente. Cuando aumenta, existe más tiempo de operación cerca de límites aunque la condición formal de congestión todavía no se active.",
            "Este patrón es relevante para medidas preventivas. Flexibilidad, reconfiguración operativa y mantenimiento dirigido pueden actuar durante el periodo de estrés y evitar que una proporción de esas horas evolucione hacia congestión.",
            "La gestión debe acompanhar ambas métricas. Reducir congestión sin reducir estrés puede desplazar el problema en el tiempo y mantener una exposición elevada a eventos adversos.",
        ],
        visual=charts["02"],
    )

    page_section(
        story, styles, "6. Concentración territorial",
        f"Las cinco zonas con mayor congestión concentran {top5_share:.1%} de las horas-zona, lo que favorece una intervención focalizada.",
        [
            "La curva de concentración no muestra un único punto de fallo dominante. Existe un bloque de zonas con exposición elevada seguido de una cola larga con necesidad de respuesta diferenciada.",
            "Este perfil desaconseja una estrategia uniforme. Las primeras posiciones justifican estudios y mitigación inmediata, mientras la cola debe gestionarse mediante monitorización, operación y gatillos de inversión.",
            "La concentración permite asignar capacidad técnica limitada donde el riesgo marginal es mayor, sin perder visibilidad sobre zonas que aún no superan umbrales de intervención.",
        ],
        visual=charts["03"],
    )

    page_section(
        story, styles, "6.1 Geografía operativa",
        "Las diferencias entre regiones operativas reflejan combinaciones distintas de carga, topología, continuidad de suministro y flexibilidad.",
        [
            "La agregación regional sirve para organizar recursos y comparar presión media, pero no debe sustituir el ranking zonal. Una región moderada puede contener una zona crítica, y una región de riesgo alto puede incluir zonas adecuadas para monitorización.",
            "Centro y Levante requieren especial atención por la combinación de riesgo operativo y exposición de servicio. La respuesta regional debe coordinar diagnósticos, contratos de flexibilidad y planificación de refuerzos.",
            "La gobernanza recomendada combina un foro regional de cartera con responsables zonales. El foro gestiona dependencias y recursos; el responsable zonal mantiene la trazabilidad técnica y la secuencia de decisión.",
        ],
        visual=charts["11"],
    )

    page_section(
        story, styles, "6.2 Grupos territoriales",
        "Los tipos de zona no presentan el mismo patrón de presión y, por tanto, no deben compartir umbrales operativos idénticos.",
        [
            "Las zonas industriales tienden a concentrar carga punta y sensibilidad a incrementos discretos de demanda. Las zonas urbanas y mixtas combinan presión de carga con mayor exposición de clientes. Las zonas rurales pueden presentar menor congestión, pero vulnerabilidad de servicio y activos más dispersos.",
            "El análisis por cohort ayuda a calibrar políticas, pero las recomendaciones continúan siendo específicas por zona. El tipo territorial explica contexto; no determina automáticamente la intervención.",
            "La próxima calibración con datos reales debe revisar umbrales por cohort y contrastar si los costes de ENS, tiempos de despliegue y restricciones de permisos difieren materialmente.",
        ],
        visual=charts["12"],
    )

    page_section(
        story, styles, "7. Prioridad de intervención",
        f"Z013 lidera con {fmt_dec(top['investment_priority_score'])} puntos y es la única zona clasificada como crítica.",
        [
            "La separación de Z013 frente al resto es coherente con su congestión, impacto de servicio, brecha flexible y señal anómala. Esta convergencia reduce el riesgo de que la posición sea resultado de un único indicador.",
            "El bloque siguiente contiene zonas altas que requieren acciones diferenciadas. Z021, Z020, Z016, Z015 y Z019 se orientan a refuerzo local; otras posiciones altas se gestionan con almacenamiento, flexibilidad u operación.",
            "El ranking debe utilizarse para ordenar diagnósticos y recursos, no para aprobar automáticamente el tipo o importe de inversión. La intervención final depende de estudios técnicos, costes reales y restricciones de ejecución.",
        ],
        visual=charts["04"],
    )

    page_section(
        story, styles, "7.1 Distribución de riesgo",
        "Los tiers de riesgo separan la cartera de forma consistente y ofrecen una base clara para gobernanza.",
        [
            "La cartera incluye una zona crítica, nueve altas, nueve medias y cinco bajas. La distribución evita una clasificación excesivamente concentrada en una única categoría y permite asignar cadencias de revisión distintas.",
            "Las zonas críticas y altas deben revisarse mensualmente hasta cerrar diagnóstico y mitigación. Las zonas medias deben revisarse trimestralmente con gatillos definidos. Las zonas bajas pueden permanecer en monitorización semestral, salvo deterioro de indicadores.",
            "La dispersión dentro de cada tier recuerda que los límites son administrativos. Una zona cerca del umbral superior puede requerir más atención que otra dentro del mismo grupo.",
        ],
        visual=charts["07"],
    )

    page_section(
        story, styles, "7.2 Cartera de intervenciones",
        "La cartera evita concentrar toda la respuesta en refuerzo físico y reserva CAPEX estructural para los casos donde la evidencia es más fuerte.",
        [
            "Cinco zonas requieren refuerzo local y una requiere intervención inmediata prioritaria. En paralelo, once zonas reciben medidas flexibles u operativas que pueden ejecutarse con menor plazo y preservar opciones.",
            "El almacenamiento se concentra en dos zonas donde la combinación de brecha flexible, impacto de servicio y soporte existente insuficiente crea un caso específico. La flexibilidad contratada se reserva para zonas donde puede reducir exposición sin ocultar una necesidad estructural.",
            "Seis zonas permanecen en monitorización. Esta decisión no equivale a inacción: exige umbrales, propietario, periodicidad y un mecanismo claro de escalada.",
        ],
        visual=charts["05"],
    )

    page_section(
        story, styles, "8. Flexibilidad y riesgo",
        "Las zonas de mayor riesgo tienden a operar con ratios de flexibilidad frente a estrés insuficientes.",
        [
            "El umbral de 0,15 funciona como señal útil para separar zonas donde la cobertura flexible es demasiado baja para absorber presión relevante. Varias zonas prioritarias se sitúan por debajo de ese nivel.",
            "La brecha técnica debe interpretarse junto con duración, disponibilidad y localización. Una capacidad nominal elevada puede no resolver el periodo crítico si no está disponible en la hora, nodo o duración necesarios.",
            "Antes de contratar flexibilidad o aprobar almacenamiento, debe validarse el perfil horario de necesidad, la capacidad efectiva, la activación esperada y el coste total durante el horizonte de decisión.",
        ],
        visual=charts["09"],
    )

    flex_rows = [["Zona", "Riesgo", "Gap técnico MW", "Ratio flex/estrés", "Horas congestión"]]
    for _, row in flex.sort_values("gap_tecnico_mw", ascending=False).head(10).iterrows():
        flex_rows.append([row["zona_id"], fmt_dec(row["riesgo_operativo_score"]), fmt_dec(row["gap_tecnico_mw"]),
                          f"{row['ratio_flexibilidad_estres']:.3f}", fmt_int(row["horas_congestion_acumuladas"])])
    page_section(
        story, styles, "8.1 Zonas con mayor brecha flexible",
        "La brecha flexible es amplia en varias zonas, pero el orden de intervención depende también de servicio, congestión y opciones disponibles.",
        [
            "Z013 presenta el mayor gap técnico y combina esa brecha con riesgo crítico. Otras zonas con gaps elevados pueden requerir refuerzo, almacenamiento o mitigación operativa según la persistencia y el perfil del problema.",
            "La tabla debe utilizarse como lista de investigación para dimensionamiento. El gap agregado no equivale a potencia contratada o capacidad de batería recomendada, porque no incorpora todas las restricciones de duración, red y operación.",
        ],
        table=data_table(flex_rows, [2.2 * cm, 2.4 * cm, 3.0 * cm, 3.2 * cm, 3.0 * cm], styles),
    )

    page_section(
        story, styles, "9. Calidad de servicio y resiliencia",
        f"La red registra {fmt_dec(total_ens, 0)} MWh de ENS y una exposición territorial que no coincide exactamente con el ranking de congestión.",
        [
            "La ENS incorpora una dimensión de impacto que la congestión por sí sola no captura. Zonas con menos horas congestionadas pueden justificar una respuesta prioritaria si cada evento afecta más energía o más clientes.",
            "Z013 lidera la ENS acumulada y refuerza el caso para diagnóstico inmediato. El resto del ranking muestra una dispersión suficiente para exigir planes específicos de resiliencia y mantenimiento.",
            "Las decisiones de refuerzo deben cuantificar la reducción esperada de ENS y clientes afectados. Sin esa conexión, existe riesgo de resolver capacidad térmica sin capturar el mayor beneficio de continuidad de suministro.",
        ],
        visual=charts["10"],
    )

    page_section(
        story, styles, "10. Electrificación y crecimiento",
        "La nueva demanda aumenta presión, pero la relación con congestión depende de capacidad existente, perfil horario y recursos flexibles.",
        [
            "El ratio de nueva demanda explica parte de la exposición, pero no produce una relación determinista con horas de congestión. Zonas con ratios similares presentan resultados operativos distintos.",
            "Esta dispersión confirma que las previsiones de EV e industrial deben integrarse con información de topología, activos y flexibilidad. Una política basada solo en crecimiento de demanda puede sobredimensionar unas zonas y reaccionar tarde en otras.",
            "La gestión debe mantener escenarios específicos para grandes conexiones industriales, adopción acelerada de EV y retrasos de infraestructura. Los gatillos deben estar asociados a solicitudes, capacidad firme y evolución observada.",
        ],
        visual=charts["13"],
    )

    page_section(
        story, styles, "11. Escenarios",
        f"El coste de riesgo proxy varía {scenario_delta:.1%} entre el escenario de mayor y menor exposición.",
        [
            "El retraso de CAPEX produce la mayor exposición. La degradación de activos y la electrificación intensiva también elevan riesgo, aunque por mecanismos diferentes.",
            "El despliegue adicional de flexibilidad y almacenamiento reduce riesgo, y la combinación de CAPEX con flexibilidad obtiene el menor coste de riesgo proxy. El resultado favorece una cartera mixta en lugar de una respuesta exclusivamente física o contractual.",
            "Los importes son comparables de forma relativa, no presupuestos regulatorios. La utilidad del escenario reside en ordenar direcciones de cambio y comprobar si la cartera mantiene coherencia bajo condiciones adversas.",
        ],
        visual=charts["16"],
    )

    page_section(
        story, styles, "11.1 Efecto antes y después",
        "Las palancas flexibles reducen exposición frente al retraso de CAPEX, pero la combinación con inversión estructural produce el mayor efecto.",
        [
            "El almacenamiento adicional y la flexibilidad adicional generan reducciones relevantes frente al escenario de retraso. El escenario combinado captura la mayor reducción porque actúa sobre necesidad estructural y capacidad de respuesta.",
            "Esta comparación no implica ejecutar todas las medidas simultáneamente. Implica construir una secuencia en la que mitigación temprana reduzca riesgo mientras se desarrollan estudios, permisos y obras.",
            "La cartera debe medir reducción de riesgo conseguida después de cada etapa. Si la mitigación supera el efecto esperado, parte del CAPEX puede diferirse; si queda por debajo, la urgencia del refuerzo aumenta.",
        ],
        visual=charts["17"],
    )

    scenario_rows = [["Escenario", "Riesgo proxy", "Inversión requerida", "Prioridad media"]]
    for _, row in scenarios.sort_values("coste_riesgo_total").iterrows():
        scenario_rows.append([slug_label(row["scenario"]), fmt_int(row["coste_riesgo_total"]),
                              fmt_m(row["inversion_requerida_total"]), fmt_dec(row["prioridad_media"])])
    page_section(
        story, styles, "11.2 Comparación detallada de escenarios",
        "La cartera debe utilizar escenarios como mecanismo de stress test y no como previsión única.",
        [
            "Los escenarios con menor riesgo no son necesariamente los de menor inversión. La decisión debe equilibrar exposición residual, capacidad de ejecución y valor de preservar opciones.",
            "La revisión trimestral debe actualizar factores de escenario y registrar si cambios reales aproximan la red a un caso adverso. Retrasos de CAPEX y deterioro de activos merecen indicadores tempranos específicos.",
        ],
        table=data_table(scenario_rows, [6.4 * cm, 3.0 * cm, 3.4 * cm, 2.6 * cm], styles),
    )

    page_section(
        story, styles, "12. Sensibilidad y robustez",
        "La sensibilidad de ranking es contenida en las primeras posiciones, lo que refuerza la prioridad de diagnóstico de la cartera principal.",
        [
            "Las zonas con gran amplitud de posición requieren cautela, especialmente cuando están cerca de umbrales de intervención. Una pequeña variación de ponderación puede alterar su secuencia relativa.",
            "Las primeras decisiones deben combinar posición, tier, driver principal y estabilidad. Una zona alta y estable puede avanzar directamente a diagnóstico; una zona média y volátil debe acumular más evidencia.",
            "La sensibilidad debe repetirse cuando se sustituyan proxies por costes reales o se recalibren umbrales. Cambios metodológicos sin teste de estabilidad pueden alterar prioridades por razones administrativas, no operativas.",
        ],
        visual=charts["18"],
    )

    page_section(
        story, styles, "13. Traducción a activos",
        "La priorización territorial se convierte en una lista concreta de alimentadores para investigación y ejecución.",
        [
            "El ranking de alimentadores permite pasar de la cartera zonal a unidades técnicas de análisis. El primer alimentador, F0034 en T010, combina estrés elevado, integración de nueva demanda y necessidade de alivio.",
            "La priorización de activos debe permanecer subordinada al diagnóstico de red. Un alimentador alto puede requerir almacenamiento, automatización, flexibilidad o refuerzo dependiendo de perfil horario, topología y contingencias.",
            "El siguiente paso técnico es validar los top alimentadores con curvas de carga, capacidade firme, eventos, ativos asociados y alternativas de conexión.",
        ],
        visual=charts["19"],
    )

    action_rows = [["Prioridad", "Zona", "Riesgo", "Intervención", "Secuencia", "Driver"]]
    for _, row in scoring.head(12).iterrows():
        action_rows.append([str(int(row["priority_rank"])), row["zona_id"], slug_label(row["risk_tier"]),
                            slug_label(row["recommended_intervention"]), slug_label(row["recommended_sequence"]),
                            slug_label(row["main_risk_driver"])])
    page_section(
        story, styles, "14. Hoja de ruta priorizada",
        "La secuencia recomendada convierte el ranking en un programa de trabajo de 0 a 24 meses.",
        [
            "La primera ola cubre Z013 y las medidas 0 a 6 meses. Debe producir diagnósticos, mitigación inmediata, responsables y decisiones de avance. La segunda ola desarrolla almacenamiento y refuerzos con horizontes de 3 a 24 meses.",
            "Cada acción debe tener un expediente de decisión con problema, evidencia, alternativas, coste real, reducción de riesgo esperada, dependencias y fecha de revisión. Sin este expediente, el ranking corre el riesgo de convertirse en una lista sin ejecución.",
        ],
        table=data_table(action_rows, [1.3 * cm, 1.6 * cm, 1.8 * cm, 4.2 * cm, 2.1 * cm, 4.1 * cm], styles),
    )

    page_section(
        story, styles, "14.1 Prioridad 0 a 6 meses",
        "El corto plazo debe reducir exposición mientras aumenta la calidad de evidencia para decisiones estructurales.",
        [
            "Para Z013, el objetivo es cerrar un diagnóstico técnico y seleccionar una combinación de medidas con fecha, responsable y reducción de riesgo esperada. La mitigación no debe retrasar el estudio estructural.",
            "Para zonas con flexibilidad u operación, el objetivo es convertir recomendaciones en contratos, procedimientos o cambios de configuración con métricas antes y después.",
            "Para zonas de refuerzo local, el objetivo es definir alcance, alternativa técnica, coste, permisos y dependencias. La secuencia debe identificar decisiones irreversibles y mantener opciones mientras la evidencia madura.",
        ],
        callout="Gate de avance: ninguna inversión estructural debe pasar a aprobación sin validar topología, contingencias, coste real, reducción de ENS y alternativa flexible.",
    )

    page_section(
        story, styles, "14.2 Prioridad 6 a 24 meses",
        "El medio plazo debe convertir mitigaciones en una arquitectura de red más robusta y medible.",
        [
            "Los refuerzos deben programarse según riesgo residual, madurez de proyecto y capacidade de ejecución. La cartera no debe ordenarse exclusivamente por score si existen dependencias de permisos, obra o indisponibilidad.",
            "Los contratos de flexibilidad y activos de almacenamiento deben incluir medición de disponibilidad, activación, desempeño y contribución real a la reducción de picos. La renovación debe depender de resultados observados.",
            "La monitorización debe permanecer activa durante obra y despliegue. Cambios de demanda, activos o conexión pueden alterar alcance y beneficio antes de la puesta en servicio.",
        ],
    )

    page_section(
        story, styles, "15. Riesgos, limitaciones y cautelas",
        "La principal limitación es que datos, costes y varios indicadores económicos son sintéticos o proxies relativos.",
        [
            "El sistema demuestra arquitectura analítica, coherencia de métricas y capacidad de priorización. No representa un modelo eléctrico completo ni una base suficiente para aprobación de inversión.",
            "No se incluyen flujo de carga AC, protecciones, contingencias N-1, restricciones topológicas detalladas, costes licitados, WACC, tratamiento regulatorio ni impactos de obra. Estos elementos pueden alterar la alternativa preferida.",
            "La asociación entre anomalías, congestión e interrupciones requiere calibración con histórico real. Los factores de coste por ENS, curtailment y congestión deben sustituirse por valores de negocio gobernados antes de tomar decisiones económicas.",
        ],
    )

    page_section(
        story, styles, "15.1 Riesgos de interpretación",
        "Un ranking preciso puede transmitir una falsa sensación de certeza si se separa de sus supuestos y gates.",
        [
            "Los scores son relativos y dependen de normalización, ponderaciones y reglas. Una diferencia pequeña entre posiciones no representa necesariamente una diferencia material de valor.",
            "Los escenarios no tienen probabilidades asociadas. Deben utilizarse para stress test, no para calcular valor esperado sin una capa adicional de probabilidad y coste real.",
            "La recomendación de intervención representa la mejor opción dentro del marco disponible. Un estudio técnico puede concluir que una combinación distinta produce mayor reducción de riesgo o menor coste total.",
        ],
    )

    validation_rows = [["Grupo", "Check", "Estado", "Severidad"]]
    for _, row in checks.iterrows():
        validation_rows.append([str(row["check_group"]), str(row["check_name"]),
                                "PASS" if bool(row["passed"]) else "FAIL", str(row["severity"])])
    page_section(
        story, styles, "16. Calidad y gobernanza",
        "Los controles analíticos publicados pasan, pero el estado de decisión permanece condicionado por la naturaleza sintética de los datos.",
        [
            "La gobernanza separa validez técnica, aceptabilidad analítica, preparación para decisión y calidad para comité. Esta distinción evita que un pipeline válido sea interpretado como autorización de inversión.",
            "La publicación debe mantener cautelas y no considerarse apta para comité hasta sustituir datos y proxies por fuentes reales, completar estudios eléctricos y validar costes.",
        ],
        table=data_table(validation_rows[:15], [3.0 * cm, 7.0 * cm, 2.0 * cm, 2.5 * cm], styles),
    )

    page_section(
        story, styles, "17. Recomendaciones",
        "La recomendación principal es ejecutar una cartera escalonada con gates técnicos y medición explícita de reducción de riesgo.",
        [
            "Primero, abrir de inmediato el diagnóstico de Z013 y asignar un responsable único. Segundo, iniciar estudios de refuerzo en las cinco zonas estructurales y activar mitigación transitoria. Tercero, validar storage en Z001 y Z024 con perfiles horarios y alternativas.",
            "Cuarto, formalizar contratos y procedimientos para las medidas de flexibilidad y operación. Quinto, establecer cadencias de revisión por tier con gatillos medibles. Sexto, sustituir proxies económicos y datos sintéticos antes de cualquier aprobación de CAPEX.",
            "La ejecución debe reportar reducción de horas de estrés, congestión, ENS, clientes afectados y gap técnico. Sin medición antes y después, no será posible distinguir medidas efectivas de simples cambios de clasificación.",
        ],
        callout="Prioridad de gestión: transformar el ranking en expedientes de decisión con propietario, gate, plazo y resultado medible.",
    )

    rec_rows = [
        ["Horizonte", "Acción", "Owner sugerido", "Gate"],
        ["0-30 días", "Diagnóstico técnico Z013", "Planificación de red", "Alternativas y mitigación aprobadas"],
        ["0-90 días", "Estudios de refuerzo en cinco zonas", "Ingeniería + regiones", "Alcance, coste y permisos"],
        ["0-90 días", "Validar storage Z001 y Z024", "Flexibilidad + operación", "Perfil y dimensionamiento"],
        ["0-6 meses", "Ejecutar medidas tácticas", "Operación regional", "Reducción observada de riesgo"],
        ["Trimestral", "Recalibrar cartera", "Gobernanza analítica", "Datos, costes y escenarios actualizados"],
    ]
    page_section(
        story, styles, "17.1 Plan de acción",
        "El plan de acción prioriza decisiones que reducen exposición y aumentan calidad de evidencia.",
        [
            "Los owners sugeridos deben confirmarse según el modelo operativo de la organización. Cada gate debe producir una decisión documentada: avanzar, modificar, diferir o cancelar.",
            "La gobernanza debe registrar cambios de ranking y justificar cualquier desviación. Una desviación puede ser correcta por restricciones de ejecución, pero no debe quedar implícita.",
        ],
        table=data_table(rec_rows, [2.4 * cm, 5.4 * cm, 3.7 * cm, 4.0 * cm], styles),
    )

    page_section(
        story, styles, "17.2 Métricas de seguimiento",
        "La cartera debe medirse por reducción de exposición, velocidad de decisión y calidad de ejecución.",
        [
            "Las métricas operativas recomendadas son horas de estrés, horas de congestión, ENS, clientes afectados, gap técnico, disponibilidad flexible y desempeño de storage. Las métricas de cartera incluyen tiempo hasta diagnóstico, tiempo hasta gate, desviación de coste y reducción de riesgo conseguida.",
            "Las métricas deben reportarse por zona y agregarse por región y tipo de intervención. El agregado permite gobernanza; el detalle permite responsabilización y aprendizaje.",
            "Cada medida debe tener un baseline y una ventana de evaluación. Sin baseline, una mejora no puede atribuirse razonablemente a la intervención.",
        ],
    )

    page_section(
        story, styles, "Apéndice A. Diccionario de decisión",
        "Las métricas siguientes deben acompañar cualquier uso ejecutivo del ranking.",
        [
            "Horas de congestión: horas distintas con al menos un nodo congestionado dentro de la zona. ENS: energía no suministrada acumulada. Gap técnico: demanda crítica menos cobertura flexible total. Ratio flexibilidad/estrés: cobertura flexible relativa a la necesidad de estrés.",
            "Riesgo operativo: score compuesto de señales operativas. Prioridad de inversión: combinación de urgencia y score multicriterio de alternativa. Coste de riesgo proxy: valoración relativa basada en ENS, curtailment y congestión.",
            "Estas definições devem permanecer estáveis entre releases. Qualquer alteração exige documentação, comparação de impacto e recalibração de thresholds.",
        ],
    )

    top_zone_rows = [["Rank", "Zona", "Score", "Tier", "Intervención", "Secuencia"]]
    for _, row in scoring.iterrows():
        top_zone_rows.append([str(int(row["priority_rank"])), row["zona_id"], fmt_dec(row["investment_priority_score"]),
                              slug_label(row["risk_tier"]), slug_label(row["recommended_intervention"]), slug_label(row["recommended_sequence"])])
    page_section(
        story, styles, "Apéndice B. Ranking completo de zonas",
        "El ranking completo mantiene trazabilidad entre score, tier, intervención y secuencia.",
        ["Las posiciones deben interpretarse junto con el driver principal, la estabilidad y las limitaciones metodológicas descritas en el informe."],
        table=data_table(top_zone_rows, [1.2 * cm, 1.4 * cm, 1.6 * cm, 1.8 * cm, 5.0 * cm, 2.4 * cm], styles),
    )

    feeder_rows = [["Rank", "Feeder", "Territorio", "Score", "Acción", "Alivio MW"]]
    for _, row in feeder_priorities.head(25).iterrows():
        feeder_rows.append([str(int(row["priority_rank"])), row["feeder_id"], row["territory_id"],
                            fmt_dec(row["priority_score"]), slug_label(row["recommended_action"]), fmt_dec(row["required_relief_mw"])])
    page_section(
        story, styles, "Apéndice C. Top alimentadores",
        "La lista de alimentadores orienta el siguiente nivel de diagnóstico técnico.",
        ["El ranking de alimentadores no sustituye análisis de topología, capacidad firme, contingencias y activos asociados."],
        table=data_table(feeder_rows, [1.2 * cm, 1.8 * cm, 1.9 * cm, 1.6 * cm, 5.4 * cm, 2.0 * cm], styles),
    )

    page_section(
        story, styles, "Apéndice D. Supuestos económicos",
        "Los factores económicos se utilizan para comparación relativa y deben sustituirse por valores gobernados antes de aprobación.",
        [
            "El modelo aplica 2.500 EUR/MWh de ENS, 90 EUR/MWh de curtailment y 45 EUR por hora de congestión como proxies. Estos valores permiten ordenar exposición, pero no representan necessariamente o custo regulatório ou social final.",
            "El CAPEX y los costes de activación deben validarse con catálogos, licitaciones y condiciones contractuales reales. La comparación de alternativas debe incorporar WACC, vida útil, degradación, operación, mantenimiento y valor residual.",
            "El análisis económico final debe documentar incertidumbre, rango de costes y sensibilidad. Una única estimación puntual no es suficiente para decisiones de capital de largo plazo.",
        ],
    )

    page_section(
        story, styles, "Apéndice E. Criterios de uso",
        "El informe está diseñado para screening y priorización, con una transición explícita hacia estudios técnicos y decisiones gobernadas.",
        [
            "Uso permitido: ordenar diagnósticos, comparar señales, estructurar escenarios, seleccionar zonas para análisis detallado y comunicar una cartera relativa.",
            "Uso no permitido: aprobar CAPEX, dimensionar storage final, contratar flexibilidad sin validación, afirmar causalidad o presentar los proxies como costes reales.",
            "El sistema debe actualizarse cuando existan nuevos datos, cambios de red, conexiones relevantes, deterioro de activos o resultados de intervención. La utilidad depende de mantener la cartera viva.",
        ],
    )

    doc.multiBuild(story)
    return pdf_path


def report_styles_big4():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name="CoverLabelB4", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=9,
        leading=11, textColor=colors.HexColor(ACCENT), spaceAfter=18, uppercase=True,
    ))
    styles.add(ParagraphStyle(
        name="CoverTitleB4", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=31,
        leading=35, textColor=colors.HexColor(INK), alignment=TA_LEFT, spaceAfter=12,
    ))
    styles.add(ParagraphStyle(
        name="CoverSubtitleB4", parent=styles["Normal"], fontName="Helvetica", fontSize=14,
        leading=19, textColor=colors.HexColor(DARK), alignment=TA_LEFT, spaceAfter=30,
    ))
    styles.add(ParagraphStyle(
        name="H1", parent=styles["Heading1"], fontName="Helvetica-Bold", fontSize=18,
        leading=22, textColor=colors.HexColor(INK), spaceBefore=0, spaceAfter=12,
    ))
    styles.add(ParagraphStyle(
        name="PageTitleB4", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=16,
        leading=20, textColor=colors.HexColor(INK), spaceBefore=0, spaceAfter=7,
    ))
    styles.add(ParagraphStyle(
        name="KickerB4", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=7.5,
        leading=9, textColor=colors.HexColor(ACCENT), spaceAfter=5,
    ))
    styles.add(ParagraphStyle(
        name="StandfirstB4", parent=styles["BodyText"], fontName="Helvetica", fontSize=11,
        leading=15, textColor=colors.HexColor(DARK), spaceAfter=10,
    ))
    styles.add(ParagraphStyle(
        name="BodyB4", parent=styles["BodyText"], fontName="Helvetica", fontSize=9.6,
        leading=13.5, textColor=colors.HexColor(INK), spaceAfter=7,
    ))
    styles.add(ParagraphStyle(
        name="SmallB4", parent=styles["BodyText"], fontName="Helvetica", fontSize=7.5,
        leading=10, textColor=colors.HexColor(MUTED), spaceAfter=4,
    ))
    styles.add(ParagraphStyle(
        name="MetricB4", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=18,
        leading=21, textColor=colors.HexColor(INK), alignment=TA_LEFT, spaceAfter=2,
    ))
    styles.add(ParagraphStyle(
        name="MetricLabelB4", parent=styles["Normal"], fontName="Helvetica", fontSize=7.4,
        leading=9, textColor=colors.HexColor(MUTED), alignment=TA_LEFT,
    ))
    styles.add(ParagraphStyle(
        name="DecisionTitleB4", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=9.2,
        leading=11, textColor=colors.HexColor(INK), spaceAfter=2,
    ))
    styles.add(ParagraphStyle(
        name="DecisionBodyB4", parent=styles["Normal"], fontName="Helvetica", fontSize=8.2,
        leading=10.5, textColor=colors.HexColor(DARK),
    ))
    styles.add(ParagraphStyle(
        name="TableHeadB4", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=7.2,
        leading=8.5, textColor=colors.white,
    ))
    styles.add(ParagraphStyle(
        name="TableBodyB4", parent=styles["Normal"], fontName="Helvetica", fontSize=7.2,
        leading=9, textColor=colors.HexColor(INK),
    ))
    styles.add(ParagraphStyle(
        name="TableNumB4", parent=styles["Normal"], fontName="Helvetica", fontSize=7.2,
        leading=9, textColor=colors.HexColor(INK), alignment=TA_RIGHT,
    ))
    return styles


PUBLICATION_LABELS = {
    "critico": "Crítico", "alto": "Alto", "medio": "Medio", "bajo": "Bajo",
    "inmediata": "Inmediata", "alta": "Alta", "planificada": "Planificada", "monitorizacion": "Monitorización",
    "intervencion_inmediata_prioritaria": "Intervención inmediata",
    "reforzar_red_local": "Refuerzo local de red",
    "desplegar_almacenamiento": "Despliegue de almacenamiento",
    "activar_flexibilidad": "Activación de flexibilidad",
    "optimizar_operacion": "Optimización operativa",
    "sustituir_activos": "Sustitución de activos",
    "monitorizar": "Monitorización",
    "congestion_risk_score": "Riesgo de congestión",
    "resilience_risk_score": "Riesgo de resiliencia",
    "service_impact_score": "Impacto de servicio",
    "flexibility_gap_score": "Brecha de flexibilidad",
    "asset_exposure_score": "Exposición de activos",
    "electrification_pressure_score": "Presión de electrificación",
    "economic_priority_score": "Prioridad económica",
    "revision_trimestral": "Revisión trimestral",
    "0-3m": "0-3 meses", "0-6m": "0-6 meses", "0-12m": "0-12 meses",
    "3-12m": "3-12 meses", "6-24m": "6-24 meses",
    "retraso_capex": "Retraso de CAPEX",
    "evento_degradacion_activos": "Degradación de activos",
    "electrificacion_industrial_intensiva": "Electrificación industrial intensiva",
    "crecimiento_acelerado_ev": "Crecimiento acelerado de vehículos eléctricos",
    "mayor_penetracion_gd": "Mayor penetración de generación distribuida",
    "despliegue_adicional_storage": "Almacenamiento adicional",
    "despliegue_adicional_flexibilidad": "Flexibilidad adicional",
    "capex_mas_flexibilidad": "CAPEX y flexibilidad",
}


def pub_label(value: object) -> str:
    text = str(value)
    return PUBLICATION_LABELS.get(text, text.replace("_", " ").capitalize())


def metric_strip(items: list[tuple[str, str]], styles) -> Table:
    values = []
    labels = []
    for value, label in items:
        values.append(Paragraph(html.escape(value), styles["MetricB4"]))
        labels.append(Paragraph(html.escape(label), styles["MetricLabelB4"]))
    table = Table([values, labels], colWidths=[(17.6 * cm) / len(items)] * len(items), rowHeights=[1.05 * cm, 0.9 * cm])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F2F5F6")),
        ("LINEABOVE", (0, 0), (-1, 0), 2.2, colors.HexColor(ACCENT)),
        ("LINEBEFORE", (1, 0), (-1, -1), 0.5, colors.HexColor(GRID)),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 9),
        ("RIGHTPADDING", (0, 0), (-1, -1), 9),
        ("TOPPADDING", (0, 0), (-1, 0), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    return table


def decision_list(items: list[tuple[str, str, str]], styles) -> Table:
    rows = []
    for idx, (title, evidence, mandate) in enumerate(items, start=1):
        rows.append([
            Paragraph(f"<b>{idx}</b>", styles["DecisionTitleB4"]),
            Paragraph(f"<b>{html.escape(title)}</b><br/><font color='{MUTED}'>{html.escape(evidence)}</font>", styles["DecisionBodyB4"]),
            Paragraph(html.escape(mandate), styles["DecisionBodyB4"]),
        ])
    table = Table(rows, colWidths=[0.8 * cm, 8.7 * cm, 8.1 * cm], hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor(ACCENT)),
        ("TEXTCOLOR", (0, 0), (0, -1), colors.white),
        ("ROWBACKGROUNDS", (1, 0), (-1, -1), [colors.white, colors.HexColor("#F6F8F9")]),
        ("LINEBELOW", (0, 0), (-1, -2), 0.45, colors.HexColor(GRID)),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
    ]))
    return table


def implication_strip(action: str, owner: str, timing: str, styles) -> Table:
    values = [("ACCIÓN", action), ("RESPONSABLE", owner), ("PLAZO", timing)]
    cells = []
    for label, value in values:
        cells.append(Paragraph(
            f"<font size='6.5' color='{ACCENT}'><b>{label}</b></font><br/><font size='8.5' color='{INK}'>{html.escape(value)}</font>",
            styles["BodyB4"],
        ))
    table = Table([cells], colWidths=[7.5 * cm, 5.4 * cm, 4.7 * cm], hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#EDF5F5")),
        ("LINEABOVE", (0, 0), (-1, 0), 1.5, colors.HexColor(ACCENT)),
        ("LINEBEFORE", (1, 0), (-1, -1), 0.5, colors.HexColor(ACCENT_LIGHT)),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    return table


def big4_table(rows: list[list[object]], widths: list[float], styles,
               numeric_cols: list[int] | None = None, badge_col: int | None = None) -> Table:
    numeric_cols = numeric_cols or []
    converted = []
    for ridx, row in enumerate(rows):
        converted_row = []
        for cidx, cell in enumerate(row):
            style = styles["TableHeadB4"] if ridx == 0 else styles["TableNumB4"] if cidx in numeric_cols else styles["TableBodyB4"]
            converted_row.append(Paragraph(html.escape(str(cell)), style))
        converted.append(converted_row)
    table = Table(converted, colWidths=widths, repeatRows=1, hAlign="LEFT")
    commands = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(DARK)),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F3F6F7")]),
        ("LINEBELOW", (0, 0), (-1, 0), 1.0, colors.HexColor(DARK)),
        ("LINEBELOW", (0, 1), (-1, -1), 0.35, colors.HexColor(GRID)),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]
    for cidx in numeric_cols:
        commands.append(("ALIGN", (cidx, 1), (cidx, -1), "RIGHT"))
    if badge_col is not None:
        badge_colors = {
            "Crítico": "#9E3D33", "Alto": "#C46B32", "Medio": "#D9B84D", "Bajo": "#8CA0A9",
            "Inmediata": "#9E3D33", "Alta": "#C46B32", "Planificada": "#547B87", "Monitorización": "#8CA0A9",
        }
        for ridx, row in enumerate(rows[1:], start=1):
            value = str(row[badge_col])
            if value in badge_colors:
                commands.extend([
                    ("BACKGROUND", (badge_col, ridx), (badge_col, ridx), colors.HexColor(badge_colors[value])),
                    ("TEXTCOLOR", (badge_col, ridx), (badge_col, ridx), colors.white),
                    ("ALIGN", (badge_col, ridx), (badge_col, ridx), "CENTER"),
                ])
    table.setStyle(TableStyle(commands))
    return table


def chart_pair(path_a: Path, path_b: Path) -> Table:
    images = []
    for path in (path_a, path_b):
        image = Image(str(path))
        image._restrictSize(15.2 * cm, 6.0 * cm)
        images.append(image)
    table = Table([[images[0]], [images[1]]], colWidths=[17.6 * cm], hAlign="CENTER")
    table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    return table


def add_page_header(story, styles, kicker: str, title: str, standfirst: str, major: bool = False) -> None:
    story.append(Paragraph(html.escape(kicker.upper()), styles["KickerB4"]))
    story.append(Paragraph(html.escape(title), styles["H1" if major else "PageTitleB4"]))
    story.append(Paragraph(html.escape(standfirst), styles["StandfirstB4"]))


def add_analysis_page(
    story,
    styles,
    kicker: str,
    title: str,
    standfirst: str,
    *,
    major: bool = False,
    metrics: list[tuple[str, str]] | None = None,
    paragraphs: list[str] | None = None,
    visual: Path | None = None,
    visual_pair: tuple[Path, Path] | None = None,
    table: Table | None = None,
    implication: tuple[str, str, str] | None = None,
) -> None:
    add_page_header(story, styles, kicker, title, standfirst, major)
    if metrics:
        story.append(metric_strip(metrics, styles))
        story.append(Spacer(1, 8))
    for text in paragraphs or []:
        story.append(Paragraph(html.escape(text), styles["BodyB4"]))
    if visual_pair:
        story.append(Spacer(1, 4))
        story.append(chart_pair(*visual_pair))
    elif visual:
        story.append(Spacer(1, 4))
        story.append(chart(visual, 17.6 * cm))
    if table:
        story.append(Spacer(1, 6))
        story.append(table)
    if implication:
        story.append(Spacer(1, 8))
        story.append(implication_strip(*implication, styles))
    story.append(PageBreak())


def build_report_big4() -> Path:
    styles = report_styles_big4()
    zone_risk = read("vw_zone_operational_risk.csv")
    scoring = read("intervention_scoring_table.csv").sort_values("priority_rank")
    scenarios = read("scenario_summary_v2.csv")
    forecast = read("forecast_error_by_zone.csv")
    anomaly_types = read("anomalies_summary_by_type.csv")
    flex = read("vw_flexibility_gap.csv")
    feeder_priorities = read("investment_priorities.csv")
    checks = read("validation_checks_sql_v2.csv")
    monthly = read("mart_zone_month_operational.csv")
    nodes = read("support_congestion_nodos.csv")
    charts = {path.name.split("_", 1)[0]: path for path in sorted(GRAPHS.glob("*.png"))}

    total_congestion = float(zone_risk["horas_congestion"].sum())
    total_ens = float(zone_risk["ens_total_mwh"].sum())
    top = scoring.iloc[0]
    top5_share = float(zone_risk.nlargest(5, "horas_congestion")["horas_congestion"].sum() / total_congestion)
    best_scenario = scenarios.loc[scenarios["coste_riesgo_total"].idxmin()]
    worst_scenario = scenarios.loc[scenarios["coste_riesgo_total"].idxmax()]
    scenario_delta = float(worst_scenario["coste_riesgo_total"] / best_scenario["coste_riesgo_total"] - 1)
    critical_anomalies = int(read("anomaly_zone_intensity.csv")["anomalias_criticas"].sum())

    pdf_path = REPORTS / "informe_analitico_red_electrificacion.pdf"
    doc = ReportDocTemplate(
        str(pdf_path), pagesize=A4, rightMargin=1.7 * cm, leftMargin=1.7 * cm,
        topMargin=1.55 * cm, bottomMargin=1.35 * cm,
        title="Cartera priorizada de red y electrificación",
        subject="Soporte a decisión para priorización de intervenciones de red",
        author="Sistema de Inteligencia de Red",
    )
    story = []

    # 1. Cover.
    story.append(Table([[""]], colWidths=[17.6 * cm], rowHeights=[0.28 * cm], style=TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(ACCENT)),
    ])))
    story.extend([
        Spacer(1, 2.2 * cm),
        Paragraph("INFORME PARA DECISIÓN DE INVERSIÓN", styles["CoverLabelB4"]),
        Paragraph("Cartera priorizada de red y electrificación", styles["CoverTitleB4"]),
        Paragraph("Riesgo operativo, flexibilidad y secuencia de intervención a 24 meses", styles["CoverSubtitleB4"]),
        HRFlowable(width="100%", thickness=0.8, color=colors.HexColor(GRID), spaceBefore=4, spaceAfter=18),
    ])
    story.extend([
        Spacer(1, 0.6 * cm),
        Paragraph(f"24 zonas · {len(nodes)} alimentadores · secuencia 0-24 meses", styles["SmallB4"]),
        Spacer(1, 5.6 * cm),
        Paragraph("Autorizar la primera vaga de diagnóstico y mitigación, y financiar los estudios que determinan qué refuerzos avanzan a aprobación.", styles["StandfirstB4"]),
        Spacer(1, 0.8 * cm),
        Paragraph("Los importes económicos son referencias relativas y requieren validación antes de cualquier compromiso de capital.", styles["SmallB4"]),
        PageBreak(),
    ])

    # 2. Executive summary.
    add_page_header(story, styles, "Resumen ejecutivo", "Cinco decisiones concentran el valor de la cartera", "La recomendación separa mitigación inmediata, estudios estructurales y condiciones previas a la aprobación de capital.")
    story.append(metric_strip([
        ("Z013", "única zona crítica; prioridad 86,2"),
        (fmt_int(total_congestion), "horas-zona de congestión"),
        (fmt_dec(total_ens, 0), "MWh de energía no suministrada"),
        (f"{scenario_delta:.0%}", "sobrecoste de riesgo por retrasar CAPEX"),
    ], styles))
    story.append(Spacer(1, 10))
    story.append(decision_list([
        ("Autorizar diagnóstico inmediato de Z013", "Combina riesgo crítico, 7.225 horas de congestión y 4.152 MWh de energía no suministrada.", "Planificación de red | 30 días"),
        ("Lanzar cinco estudios de refuerzo local", "Z021, Z020, Z016, Z015 y Z019 presentan congestión estructural y cobertura flexible insuficiente.", "Ingeniería y regiones | 90 días"),
        ("Validar almacenamiento en Z001 y Z024", "La brecha flexible y el impacto de servicio justifican dimensionamiento específico antes de comprometer capital.", "Flexibilidad y operación | 90 días"),
        ("Ejecutar nueve medidas tácticas", "Cuatro activaciones de flexibilidad y cinco optimizaciones operativas reducen exposición durante los estudios.", "Operación regional | 6 meses"),
        ("Mantener bloqueada la aprobación de CAPEX", "Datos sintéticos, costes de referencia y ausencia de flujo de carga impiden una decisión final de inversión.", "Comité de inversión | antes de aprobar"),
    ], styles))
    story.append(Spacer(1, 8))
    story.append(Paragraph("Recomendación al comité: aprobar la primera vaga de trabajo y sus estudios; no aprobar todavía inversiones estructurales.", styles["StandfirstB4"]))
    story.append(big4_table([
        ["APROBAR AHORA", "MANTENER CONDICIONADO", "RETORNO AL COMITÉ"],
        ["Diagnóstico, estudios y mitigación", "CAPEX estructural y contratos de largo plazo", "Expedientes con ingeniería, economía y riesgo residual"],
    ], [5.7 * cm, 5.7 * cm, 6.2 * cm], styles))
    story.append(PageBreak())

    # 3. TOC.
    story.append(Paragraph("Índice", styles["H1"]))
    story.append(Paragraph("El cuerpo principal está organizado por decisión; los detalles metodológicos y rankings completos quedan en apéndice.", styles["StandfirstB4"]))
    toc = TableOfContents()
    toc.levelStyles = [
        ParagraphStyle(name="TOCB4", fontName="Helvetica-Bold", fontSize=10, leading=23, leftIndent=0,
                       textColor=colors.HexColor(INK), rightIndent=12),
    ]
    story.append(toc)
    story.append(PageBreak())

    # 4-6. Mandate and assurance.
    add_analysis_page(
        story, styles, "1 | Mandato y base analítica",
        "La decisión es financiar la primera vaga de trabajo, no aprobar una cartera cerrada de CAPEX",
        "El modelo reduce el universo técnico a intervenciones que deben validarse con estudios, costes reales y restricciones de ejecución.",
        major=True,
        metrics=[("24", "zonas evaluadas"), ("10", "zonas de riesgo alto o crítico"), ("6", "tipos de respuesta"), ("0-24", "meses de secuenciación")],
        paragraphs=[
            "La unidad de decisión es la zona, con trazabilidad a subestaciones y alimentadores. El índice final combina congestión, continuidad de suministro, brecha flexible, activos, electrificación y economía relativa.",
            "El informe asigna una respuesta y un plazo, pero mantiene un umbral explícito entre priorización y autorización de inversión.",
        ],
        table=big4_table([
            ["ETAPA", "PREGUNTA", "SALIDA"],
            ["Priorizar", "¿Dónde se concentra el riesgo?", "Ranking y plazo"],
            ["Diagnosticar", "¿Qué alternativa reduce mejor la exposición?", "Expediente técnico y económico"],
            ["Autorizar", "¿La solución supera los umbrales?", "Decisión de inversión"],
        ], [3.0 * cm, 7.3 * cm, 6.3 * cm], styles),
        implication=("Aprobar alcance, responsables y presupuesto de estudios de la primera vaga.", "Comité de inversión y dirección de red", "En la próxima sesión de cartera"),
    )
    add_analysis_page(
        story, styles, "1 | Mandato y base analítica",
        "El modelo es adecuado para ordenar decisiones; aún no es suficiente para comprometer capital",
        "La calidad analítica es consistente, pero la base sintética y los costes de referencia limitan el uso financiero.",
        metrics=[(fmt_int(len(monthly)), "observaciones zona-mes"), (fmt_int(len(feeder_priorities)), "alimentadores priorizados"),
                 (f"{forecast['nmae'].min():.2%}-{forecast['nmae'].max():.2%}", "rango de error normalizado"), ("14/14", "controles SQL superados")],
        visual=charts["15"],
        paragraphs=[
            "La estabilidad del pronóstico reduce el riesgo de que el ranking dependa de diferencias extremas de previsibilidad entre zonas.",
            "La validación pendiente es material: topología, contingencias, protecciones, costes licitados y restricciones regulatorias pueden cambiar la alternativa final.",
        ],
        implication=("Sustituir datos sintéticos y referencias económicas por fuentes gobernadas antes de aprobar CAPEX.", "Gobernanza de datos y finanzas", "Antes de la decisión de inversión"),
    )
    validation_rows = [["Ámbito", "Control", "Resultado", "Severidad"]]
    for _, row in checks.head(10).iterrows():
        validation_rows.append([pub_label(row["check_group"]), pub_label(row["check_name"]),
                                "Superado" if bool(row["passed"]) else "No superado", pub_label(row["severity"])])
    add_analysis_page(
        story, styles, "1 | Mandato y base analítica",
        "Los controles protegen la coherencia del ranking, no la validez eléctrica de cada solución",
        "Los controles publicados verifican datos, agregaciones y reglas de decisión; no sustituyen estudios de ingeniería.",
        metrics=[("14", "controles analíticos"), ("0", "controles no superados"), ("1", "fuente de verdad"), ("Separado", "umbral de aprobación")],
        table=big4_table(validation_rows, [3.2 * cm, 8.0 * cm, 2.8 * cm, 3.0 * cm], styles),
        paragraphs=["La gobernanza debe mantener esta separación para evitar que un resultado reproducible se interprete como aprobación técnica o financiera."],
        implication=("Incorporar un control de aprobación técnica y financiera fuera del modelo analítico.", "Riesgo técnico y control de inversiones", "Antes de elevar cada expediente"),
    )

    # 7-16. Diagnosis.
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "La presión es persistente: el estrés operativo precede y supera a la congestión confirmada",
        "La red no muestra un alivio estructural durante el horizonte; la gestión preventiva debe actuar antes de que el estrés se convierta en congestión.",
        major=True, visual_pair=(charts["01"], charts["02"]),
        paragraphs=["La distancia entre estrés y congestión identifica horas en las que flexibilidad, reconfiguración y mantenimiento dirigido pueden reducir exposición sin esperar a un evento formal."],
        implication=("Activar umbrales mensuales de estrés y congestión para anticipar medidas operativas.", "Centro de operación de red", "Desde el próximo ciclo mensual"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        f"Cinco zonas concentran {top5_share:.0%} de la congestión, pero el riesgo exige coordinación regional",
        "La concentración favorece una primera vaga focalizada; la agregación regional organiza recursos sin sustituir el diagnóstico zonal.",
        visual_pair=(charts["03"], charts["11"]),
        implication=("Reservar capacidad de ingeniería para las cinco zonas líderes y coordinar dependencias por región.", "Planificación de red y direcciones regionales", "Próximos 90 días"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "Los perfiles territoriales justifican umbrales distintos; una política uniforme asignaría mal el capital",
        "Zonas industriales, urbanas, mixtas y rurales combinan de forma diferente carga punta, congestión y continuidad de suministro.",
        visual_pair=(charts["07"], charts["12"]),
        paragraphs=["El tipo territorial explica contexto, pero no determina la intervención. Los umbrales deben calibrarse por perfil y confirmarse a nivel de zona."],
        implication=("Recalibrar umbrales por tipo de zona antes del siguiente ciclo de priorización.", "Gobernanza analítica y planificación", "Próxima revisión trimestral"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "Z013 se separa del resto y requiere diagnóstico inmediato",
        "La posición de Z013 no depende de un único indicador: congestión, servicio, brecha flexible y anomalías convergen en la misma conclusión.",
        metrics=[("86,2", "índice de prioridad"), ("7.225", "horas de congestión"), ("4.152", "MWh no suministrados"), ("759", "MW de brecha técnica")],
        visual=charts["04"],
        implication=("Abrir diagnóstico completo de Z013 y definir mitigación transitoria.", "Planificación de red con operación regional", "30 días"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "La cartera equilibra respuesta estructural y mitigación rápida",
        "La selección evita tratar todo el riesgo con refuerzo físico y reserva las medidas reversibles para ganar tiempo y evidencia.",
        visual_pair=(charts["05"], charts["06"]),
        paragraphs=["Una intervención inmediata y cinco refuerzos conviven con once medidas flexibles u operativas y seis zonas en monitorización."],
        implication=("Asignar presupuesto y responsable por familia de intervención, no solo por zona.", "Dirección de red y finanzas", "En la planificación anual"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "Congestión, servicio y brecha flexible explican la prioridad; la cobertura flexible sigue siendo insuficiente",
        "El índice final responde a varios factores, pero las zonas más expuestas tienden a operar por debajo del umbral de cobertura flexible.",
        visual_pair=(charts["08"], charts["09"]),
        implication=("Validar perfil horario, duración y ubicación de la flexibilidad antes de contratar capacidad.", "Flexibilidad y operación de red", "90 días"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "La energía no suministrada cambia el orden de atención respecto a la congestión pura",
        "El impacto de servicio exige proteger zonas donde cada evento afecta más energía o clientes, aunque no lideren las horas congestionadas.",
        visual_pair=(charts["10"], charts["13"]),
        paragraphs=["La nueva demanda aumenta presión, pero la dispersión confirma que capacidad existente y perfil horario siguen siendo determinantes."],
        implication=("Incluir reducción esperada de energía no suministrada en cada estudio de alternativa.", "Resiliencia de red y finanzas", "Durante los estudios de 90 días"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "Las anomalías aportan alerta temprana; el pronóstico permite secuenciar con confianza controlada",
        "Las señales precursoras ayudan a priorizar investigación, pero no prueban causalidad ni sustituyen el análisis operativo.",
        metrics=[(fmt_int(anomaly_types["n_eventos"].sum()), "eventos anómalos"), (fmt_int(critical_anomalies), "anomalías críticas"),
                 (f"{forecast['nmae'].max():.2%}", "error normalizado máximo"), ("3,5%", "umbral de confianza")],
        visual_pair=(charts["14"], charts["15"]),
        implication=("Revisar alarmas, curvas de carga y eventos de Z013 antes de seleccionar la solución.", "Operación y analítica de red", "30 días"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        f"Retrasar CAPEX eleva el coste de riesgo relativo en {scenario_delta:.0%}; combinar inversión y flexibilidad reduce más exposición",
        "Los escenarios respaldan una secuencia mixta: mitigación temprana mientras maduran los refuerzos estructurales.",
        visual_pair=(charts["16"], charts["17"]),
        implication=("Usar el escenario combinado como referencia para diseñar la secuencia, no como presupuesto aprobado.", "Planificación estratégica y finanzas", "En la revisión de cartera"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "Las primeras decisiones son estables y ya se traducen en una lista concreta de alimentadores",
        "La sensibilidad contenida en las posiciones prioritarias permite avanzar a diagnóstico sin esperar a una recalibración completa.",
        visual_pair=(charts["18"], charts["19"]),
        implication=("Validar los 15 alimentadores prioritarios con curvas, capacidad firme y contingencias.", "Ingeniería de red", "90 días"),
    )

    # 17-22. Portfolio.
    priority_rows = [["Prioridad", "Zona", "Riesgo", "Índice", "Intervención", "Plazo"]]
    for _, row in scoring.head(12).iterrows():
        priority_rows.append([int(row["priority_rank"]), row["zona_id"], pub_label(row["risk_tier"]),
                              fmt_dec(row["investment_priority_score"]), pub_label(row["recommended_intervention"]),
                              pub_label(row["recommended_sequence"])])
    add_analysis_page(
        story, styles, "3 | Cartera priorizada",
        "Doce zonas concentran la primera cartera de decisión",
        "La tabla separa prioridad, nivel de riesgo, intervención y plazo para facilitar asignación de responsables y seguimiento.",
        major=True,
        table=big4_table(priority_rows, [1.4 * cm, 1.3 * cm, 1.7 * cm, 1.4 * cm, 7.0 * cm, 2.6 * cm], styles,
                         numeric_cols=[0, 3], badge_col=2),
        implication=("Confirmar responsable y expediente para cada una de las 12 primeras zonas.", "Oficina de cartera de red", "30 días"),
    )
    z13 = scoring[scoring["zona_id"] == "Z013"].iloc[0]
    z13_rows = [["Indicador", "Z013", "Mediana de cartera", "Lectura"]]
    for column, label in [
        ("congestion_risk_score", "Riesgo de congestión"),
        ("service_impact_score", "Impacto de servicio"),
        ("flexibility_gap_score", "Brecha de flexibilidad"),
        ("economic_priority_score", "Prioridad económica"),
    ]:
        z13_rows.append([label, fmt_dec(z13[column]), fmt_dec(scoring[column].median()),
                         "Exposición materialmente superior" if z13[column] > scoring[column].median() * 1.25 else "Exposición superior"])
    add_analysis_page(
        story, styles, "3 | Cartera priorizada",
        "Z013 requiere una intervención inmediata, pero la solución final sigue abierta",
        "La convergencia de riesgo técnico y de servicio justifica actuar; la ausencia de estudios eléctricos impide elegir todavía entre refuerzo, operación y flexibilidad.",
        metrics=[(fmt_dec(z13["congestion_risk_score"]), "riesgo de congestión"), (fmt_dec(z13["service_impact_score"]), "impacto de servicio"),
                 (fmt_dec(z13["flexibility_gap_score"]), "brecha de flexibilidad"), (fmt_dec(z13["economic_priority_score"]), "prioridad económica")],
        paragraphs=[
            "La mitigación debe reducir exposición durante el diagnóstico sin crear dependencia de una medida temporal.",
            "El expediente debe comparar alternativas, coste real, reducción de energía no suministrada y plazo de ejecución.",
        ],
        table=big4_table(z13_rows, [4.3 * cm, 2.2 * cm, 3.4 * cm, 6.0 * cm], styles, numeric_cols=[1, 2]),
        implication=("Seleccionar alternativa preferida y mitigación temporal para Z013.", "Director de planificación de red", "30 días"),
    )
    reinforcement = scoring[scoring["recommended_intervention"] == "reforzar_red_local"]
    reinforcement_rows = [["Zona", "Prioridad", "Congestión h", "Brecha MW", "Plazo"]]
    for _, row in reinforcement.sort_values("priority_rank").iterrows():
        reinforcement_rows.append([row["zona_id"], fmt_dec(row["investment_priority_score"]),
                                   fmt_int(row["horas_congestion_acumuladas"]), fmt_dec(row["gap_tecnico_mw"]),
                                   pub_label(row["recommended_sequence"])])
    add_analysis_page(
        story, styles, "3 | Cartera priorizada",
        "Cinco refuerzos locales deben avanzar como estudios, no como obras preaprobadas",
        "La congestión estructural y la baja cobertura flexible justifican madurar alternativas físicas mientras se mantiene mitigación operativa.",
        metrics=[(str(len(reinforcement)), "zonas de refuerzo"), (fmt_int(reinforcement["horas_congestion_acumuladas"].sum()), "horas acumuladas"),
                 (fmt_dec(reinforcement["gap_tecnico_mw"].sum(), 0), "MW de brecha agregada"), ("6-24", "meses de horizonte")],
        paragraphs=["Los estudios deben cubrir topología, contingencias, permisos, coste, reducción de riesgo y posibilidad de diferir inversión mediante flexibilidad."],
        table=big4_table(reinforcement_rows, [2.0 * cm, 2.5 * cm, 3.2 * cm, 3.0 * cm, 3.0 * cm], styles, numeric_cols=[1, 2, 3]),
        implication=("Lanzar estudios de Z021, Z020, Z016, Z015 y Z019 con términos de referencia comunes.", "Ingeniería y direcciones regionales", "90 días"),
    )
    flex_rows = [["Zona", "Riesgo", "Brecha MW", "Cobertura relativa", "Congestión h"]]
    for _, row in flex.sort_values("gap_tecnico_mw", ascending=False).head(10).iterrows():
        flex_rows.append([row["zona_id"], fmt_dec(row["riesgo_operativo_score"]), fmt_dec(row["gap_tecnico_mw"]),
                          f"{row['ratio_flexibilidad_estres']:.3f}", fmt_int(row["horas_congestion_acumuladas"])])
    add_analysis_page(
        story, styles, "3 | Cartera priorizada",
        "La brecha flexible exige dimensionamiento específico; la capacidad nominal no basta",
        "Duración, disponibilidad y ubicación determinan si almacenamiento o flexibilidad contractual reducen el riesgo en la hora crítica.",
        table=big4_table(flex_rows, [2.0 * cm, 2.4 * cm, 2.8 * cm, 3.4 * cm, 3.1 * cm], styles, numeric_cols=[1, 2, 3, 4]),
        implication=("Dimensionar almacenamiento en Z001 y Z024 y validar contratos en las zonas tácticas.", "Flexibilidad, operación y compras", "90 días"),
    )
    tactical = scoring[scoring["recommended_intervention"].isin(["activar_flexibilidad", "optimizar_operacion", "monitorizar"])]
    add_analysis_page(
        story, styles, "3 | Cartera priorizada",
        "Las medidas tácticas necesitan umbrales de éxito y escalada; monitorizar no significa esperar",
        "La cartera contiene medidas reversibles que deben demostrar reducción de riesgo o activar una respuesta estructural.",
        metrics=[("4", "activaciones de flexibilidad"), ("5", "optimizaciones operativas"), ("6", "zonas en monitorización"), (str(len(tactical)), "zonas con respuesta táctica")],
        paragraphs=[
            "Cada medida debe tener una línea base, un objetivo de reducción y una fecha de revisión.",
            "Las zonas monitorizadas deben escalar si superan umbrales de estrés, congestión, energía no suministrada o brecha flexible.",
        ],
        table=big4_table([
            ["RESPUESTA", "OBJETIVO DE GESTIÓN", "UMBRAL DE ESCALADA"],
            ["Flexibilidad", "Reducir presión en horas críticas", "Disponibilidad o reducción inferior al objetivo"],
            ["Optimización operativa", "Reducir estrés sin obra", "Congestión persistente tras dos revisiones"],
            ["Monitorización", "Preservar opción sin inversión", "Deterioro de servicio o brecha flexible"],
        ], [4.0 * cm, 6.4 * cm, 6.2 * cm], styles),
        implication=("Publicar umbrales, responsables y cadencia para las 15 zonas tácticas.", "Operación regional y oficina de cartera", "60 días"),
    )
    scenario_rows = [["Escenario", "Riesgo relativo", "Inversión requerida", "Prioridad media"]]
    for _, row in scenarios.sort_values("coste_riesgo_total").iterrows():
        scenario_rows.append([pub_label(row["scenario"]), fmt_int(row["coste_riesgo_total"]), fmt_m(row["inversion_requerida_total"]),
                              fmt_dec(row["prioridad_media"])])
    add_analysis_page(
        story, styles, "3 | Cartera priorizada",
        "La cartera combinada ofrece la menor exposición relativa, pero requiere una secuencia disciplinada",
        "La comparación de escenarios orienta dirección y orden; no representa presupuesto ni valor esperado.",
        table=big4_table(scenario_rows, [7.1 * cm, 3.1 * cm, 3.5 * cm, 2.8 * cm], styles, numeric_cols=[1, 2, 3]),
        implication=("Revisar trimestralmente si la red se aproxima a retraso de CAPEX o degradación de activos.", "Planificación estratégica y riesgo", "Trimestral"),
    )

    # 23-28. Execution and recommendation.
    roadmap_rows = [
        ["Periodo", "Decisión", "Entregable", "Responsable"],
        ["0-30 días", "Abrir Z013", "Alternativas y mitigación", "Planificación de red"],
        ["0-90 días", "Madurar cinco refuerzos", "Alcance, coste y permisos", "Ingeniería y regiones"],
        ["0-90 días", "Validar almacenamiento", "Perfil, ubicación y dimensión", "Flexibilidad y operación"],
        ["0-6 meses", "Ejecutar medidas tácticas", "Reducción observada", "Operación regional"],
        ["Trimestral", "Recalibrar cartera", "Ranking y escenarios actualizados", "Gobernanza analítica"],
    ]
    add_analysis_page(
        story, styles, "4 | Ejecución y gobernanza",
        "Los primeros 90 días deben convertir el ranking en expedientes de decisión",
        "La prioridad es aumentar calidad de evidencia y reducir exposición, no iniciar todas las obras.",
        major=True,
        table=big4_table(roadmap_rows, [2.2 * cm, 4.7 * cm, 5.5 * cm, 4.2 * cm], styles),
        implication=("Aprobar la hoja de ruta y nombrar responsables antes de liberar presupuesto de estudios.", "Comité de inversión", "Próxima sesión"),
    )
    add_analysis_page(
        story, styles, "4 | Ejecución y gobernanza",
        "La primera vaga debe cerrar cuatro umbrales antes de avanzar",
        "Cada expediente debe demostrar necesidad, alternativa preferida, economía validada y capacidad de ejecución.",
        metrics=[("1", "necesidad técnica confirmada"), ("2", "alternativas comparadas"), ("3", "coste y beneficio validados"), ("4", "permiso para avanzar")],
        paragraphs=[
            "La decisión de avanzar debe documentar reducción esperada de congestión y energía no suministrada, coste total, dependencias y riesgo residual.",
            "Una medida que no supere el umbral debe modificarse, diferirse o cancelarse; el ranking no debe utilizarse como justificación automática.",
        ],
        table=big4_table([
            ["UMBRAL", "EVIDENCIA EXIGIDA", "DECISIÓN POSIBLE"],
            ["Necesidad", "Topología, contingencias y línea base", "Continuar o cerrar"],
            ["Alternativas", "Comparación técnica y operativa", "Seleccionar o rediseñar"],
            ["Economía", "Coste real, beneficio y riesgo residual", "Diferir o elevar"],
            ["Ejecución", "Permisos, recursos y secuencia", "Autorizar o reprogramar"],
        ], [3.0 * cm, 8.2 * cm, 5.4 * cm], styles),
        implication=("Implantar un expediente estándar y una decisión formal en cada umbral.", "Control de inversiones y riesgo técnico", "Antes de liberar CAPEX"),
    )
    add_analysis_page(
        story, styles, "4 | Ejecución y gobernanza",
        "La gestión debe medir reducción de riesgo, velocidad de decisión y disciplina de ejecución",
        "Los indicadores de cartera deben distinguir mejora operativa de avance administrativo.",
        metrics=[("Mensual", "estrés, congestión y servicio"), ("Trimestral", "ranking y escenarios"), ("Por expediente", "coste, plazo y riesgo residual"), ("Antes/después", "efecto de cada medida")],
        paragraphs=["La línea base debe fijarse antes de ejecutar; sin ella no es posible atribuir una mejora a la intervención ni decidir si debe renovarse."],
        table=big4_table([
            ["ÁMBITO", "INDICADOR", "USO DE GESTIÓN"],
            ["Operación", "Estrés, congestión, energía no suministrada", "Activar o retirar mitigación"],
            ["Cartera", "Tiempo hasta diagnóstico y umbral", "Eliminar bloqueos"],
            ["Finanzas", "Coste, beneficio y desviación", "Controlar valor"],
            ["Resultado", "Reducción antes/después", "Renovar, escalar o cerrar"],
        ], [3.0 * cm, 7.1 * cm, 6.5 * cm], styles),
        implication=("Publicar un cuadro de seguimiento con línea base, objetivo y resultado por zona.", "Oficina de cartera y analítica", "60 días"),
    )
    add_analysis_page(
        story, styles, "4 | Ejecución y gobernanza",
        "Cinco limitaciones impiden tratar el informe como aprobación de inversión",
        "La transparencia sobre límites protege la calidad de la decisión y evita una falsa precisión financiera.",
        metrics=[("Sintética", "base de datos"), ("Referencial", "valoración económica"), ("No incluido", "flujo de carga y N-1"), ("Pendiente", "coste, permisos y regulación")],
        paragraphs=[
            "La causalidad entre anomalías, congestión e interrupciones requiere histórico real. La recomendación de intervención puede cambiar al incorporar topología y costes licitados.",
            "Los escenarios no tienen probabilidades asociadas y no deben utilizarse para calcular valor esperado sin una capa adicional.",
        ],
        table=big4_table([
            ["LIMITACIÓN", "RIESGO DE DECISIÓN", "RESPUESTA EXIGIDA"],
            ["Datos sintéticos", "Prioridad no calibrada con operación real", "Sustituir por SCADA/AMI gobernado"],
            ["Costes de referencia", "Caso financiero distorsionado", "Validar coste total y beneficio"],
            ["Sin flujo de carga ni N-1", "Solución técnica incompleta", "Completar estudio eléctrico"],
            ["Sin probabilidades de escenario", "No existe valor esperado", "Usar escenarios solo como prueba de robustez"],
        ], [4.0 * cm, 6.3 * cm, 6.3 * cm], styles),
        implication=("Mantener visible la condición de soporte a decisión en cada expediente y presentación.", "Riesgo técnico y finanzas", "Hasta completar validaciones"),
    )
    board_rows = [
        ["Decisión solicitada", "Recomendación", "Condición"],
        ["Diagnóstico de Z013", "Aprobar", "Alternativas y mitigación en 30 días"],
        ["Cinco estudios de refuerzo", "Aprobar", "Alcance, coste, permisos y riesgo residual"],
        ["Validación de almacenamiento", "Aprobar", "Perfil horario y dimensionamiento"],
        ["Nueve medidas tácticas", "Aprobar", "Línea base y objetivo de reducción"],
        ["CAPEX estructural", "No aprobar todavía", "Ingeniería y economía validadas"],
    ]
    add_analysis_page(
        story, styles, "5 | Recomendación al comité",
        "Aprobar la primera vaga de diagnóstico y mitigación; mantener el CAPEX estructural condicionado",
        "La cartera es suficientemente robusta para movilizar equipos y estudios, pero no para autorizar obras.",
        major=True,
        metrics=[("30 días", "diagnóstico Z013"), ("90 días", "refuerzos y almacenamiento"), ("6 meses", "medidas tácticas"), ("Trimestral", "revisión de cartera")],
        paragraphs=[
            "La aprobación solicitada cubre diagnóstico, estudios, mitigación transitoria y gobernanza de cartera.",
            "La aprobación no cubre inversiones estructurales hasta completar ingeniería, costes reales, valoración financiera y permisos.",
        ],
        table=big4_table(board_rows, [6.0 * cm, 3.7 * cm, 7.0 * cm], styles),
        implication=("Autorizar la primera vaga y exigir retorno al comité con expedientes completos.", "CFO y comité de inversión", "Próxima sesión"),
    )

    # Appendices.
    method_rows = [
        ["Componente", "Uso en la decisión", "Limitación principal"],
        ["Riesgo de congestión", "Identifica presión estructural", "No incorpora flujo de carga AC"],
        ["Impacto de servicio", "Prioriza continuidad de suministro", "Coste de ENS de referencia"],
        ["Brecha de flexibilidad", "Orienta medidas reversibles", "Requiere perfil horario y ubicación"],
        ["Exposición de activos", "Identifica riesgo de fallo", "Necesita inspección y mantenimiento real"],
        ["Escenarios", "Prueba robustez de cartera", "Sin probabilidades asociadas"],
    ]
    add_analysis_page(
        story, styles, "Apéndice",
        "Metodología: el índice ordena prioridades y las reglas traducen señales en respuestas",
        "La metodología es interpretable y reproducible; cada componente tiene un uso y una limitación explícitos.",
        major=True,
        table=big4_table(method_rows, [4.0 * cm, 6.3 * cm, 6.3 * cm], styles),
    )
    full_rows = [["Prioridad", "Zona", "Riesgo", "Índice", "Intervención", "Plazo"]]
    for _, row in scoring.iterrows():
        full_rows.append([int(row["priority_rank"]), row["zona_id"], pub_label(row["risk_tier"]),
                          fmt_dec(row["investment_priority_score"]), pub_label(row["recommended_intervention"]),
                          pub_label(row["recommended_sequence"])])
    add_analysis_page(
        story, styles, "Apéndice",
        "Ranking completo de zonas",
        "La clasificación completa mantiene trazabilidad entre prioridad, riesgo, intervención y plazo.",
        table=big4_table(full_rows, [1.4 * cm, 1.3 * cm, 1.7 * cm, 1.4 * cm, 7.0 * cm, 2.6 * cm], styles,
                         numeric_cols=[0, 3], badge_col=2),
    )
    feeder_rows = [["Prioridad", "Alimentador", "Territorio", "Índice", "Acción", "Alivio MW"]]
    for _, row in feeder_priorities.head(25).iterrows():
        feeder_rows.append([int(row["priority_rank"]), row["feeder_id"], row["territory_id"], fmt_dec(row["priority_score"]),
                            pub_label(row["recommended_action"]), fmt_dec(row["required_relief_mw"])])
    add_analysis_page(
        story, styles, "Apéndice",
        "Alimentadores prioritarios para validación técnica",
        "La lista traduce la prioridad territorial a unidades concretas de investigación.",
        table=big4_table(feeder_rows, [1.4 * cm, 2.2 * cm, 2.0 * cm, 1.5 * cm, 6.5 * cm, 2.2 * cm], styles,
                         numeric_cols=[0, 3, 5]),
    )
    assumptions_rows = [
        ["Referencia", "Valor", "Uso"],
        ["Energía no suministrada", "2.500 EUR/MWh", "Comparación relativa de impacto de servicio"],
        ["Vertido de generación", "90 EUR/MWh", "Comparación relativa de energía desaprovechada"],
        ["Congestión", "45 EUR/hora", "Comparación relativa de presión operativa"],
        ["Horizonte", "0-24 meses", "Secuenciación de intervención"],
        ["Uso permitido", "Priorización", "Ordenar diagnósticos, estudios y mitigación"],
        ["Uso condicionado", "Diseño preliminar", "Requiere ingeniería y datos reales"],
        ["Uso no permitido", "Aprobación de CAPEX", "No comprometer inversión con este informe"],
        ["Revisión", "Trimestral", "Actualizar ranking, escenarios y resultados"],
    ]
    add_analysis_page(
        story, styles, "Apéndice",
        "Definiciones y supuestos económicos de referencia",
        "Los valores permiten comparar exposición; deben sustituirse por datos gobernados antes de una decisión financiera.",
        table=big4_table(assumptions_rows, [5.0 * cm, 3.2 * cm, 8.4 * cm], styles),
        paragraphs=["La valoración final debe incorporar coste licitado, vida útil, operación, mantenimiento, regulación, coste de capital y valor residual."],
    )

    doc.multiBuild(story)
    return pdf_path


def main() -> None:
    prepare_outputs()
    charts = generate_charts()
    dashboard = make_dashboard_standalone()
    report = build_report_big4()
    print(f"graphs={len(charts)}")
    print(f"dashboard={dashboard}")
    print(f"report={report}")


if __name__ == "__main__":
    main()
