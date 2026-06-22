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
from matplotlib.ticker import PercentFormatter
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    HRFlowable,
    Image,
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
    ax.set_title(title, loc="left", fontsize=16.5, fontweight="bold", color=INK, pad=20)
    if subtitle:
        ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=11, color=MUTED, va="bottom")
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=10.5, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.7, alpha=0.8)
    ax.set_axisbelow(True)


def save_chart(fig: plt.Figure, filename: str) -> Path:
    path = GRAPHS / filename
    fig.savefig(path, dpi=220, bbox_inches="tight", facecolor=WHITE)
    plt.close(fig)
    return path


def add_source(fig: plt.Figure, source: str) -> None:
    fig.text(0.01, 0.005, "Fuente: modelo analítico de red; datos procesados del proyecto.", fontsize=9, color=MUTED)


def generate_charts() -> list[Path]:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "figure.facecolor": WHITE,
            "axes.facecolor": WHITE,
            "text.color": INK,
            "axes.labelcolor": MUTED,
            "axes.titlesize": 16.5,
            "axes.titleweight": "bold",
            "axes.labelsize": 11,
            "legend.fontsize": 10.5,
        }
    )
    zone_risk = read("vw_zone_operational_risk.csv")
    scoring = read("intervention_scoring_table.csv")
    monthly = read("mart_zone_month_operational.csv")
    flex = read("vw_flexibility_gap.csv")
    scenarios = read("scenario_summary_v2.csv")
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
    for bar, val in zip(bars, funnel_values, strict=False):
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
    for patch, color in zip(box["boxes"], [NEUTRALS[1], NEUTRALS[2], DARK, ACCENT][-len(groups):], strict=False):
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
    ax.set_title("La prioridad final está más asociada a congestión, servicio y brecha flexible", loc="left", fontsize=16, fontweight="bold", color=INK, pad=18)
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
    ax.set_title("Los grupos territoriales presentan perfiles de presión materialmente distintos", loc="left", fontsize=16, fontweight="bold", color=INK, pad=18)
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
    labels_anom = [f"{fmt_int(n)} | {p:.0%} precursor" for n, p in zip(at["n_eventos"], at["pct_precursor_congestion"], strict=False)]
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
    (ROOT / "index.html").write_text(text, encoding="utf-8")
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


def chart(path: Path, width: float = 17.2 * cm) -> Image:
    img = Image(str(path))
    img._restrictSize(width, 13.0 * cm)
    img.hAlign = "CENTER"
    return img


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
    n = len(items)
    col_w = (17.6 * cm) / n
    avail = col_w - 18  # 9pt of horizontal padding on each side
    # Choose one uniform font size so every value sits on a single line; word-based
    # KPIs (e.g. "Por expediente") shrink instead of wrapping into the label row.
    size = 18.0
    for value, _ in items:
        while size > 11.0 and stringWidth(value, "Helvetica-Bold", size) > avail:
            size -= 0.5
    value_style = ParagraphStyle(
        "MetricValueAuto", parent=styles["MetricB4"], fontSize=size, leading=size * 1.16,
    )
    values = []
    labels = []
    for value, label in items:
        values.append(Paragraph(html.escape(value), value_style))
        labels.append(Paragraph(html.escape(label), styles["MetricLabelB4"]))
    table = Table([values, labels], colWidths=[col_w] * n, rowHeights=[1.05 * cm, 0.9 * cm])
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
        image._restrictSize(17.6 * cm, 6.55 * cm)
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
    top5_share = float(zone_risk.nlargest(5, "horas_congestion")["horas_congestion"].sum() / total_congestion)
    best_scenario = scenarios.loc[scenarios["coste_riesgo_total"].idxmin()]
    worst_scenario = scenarios.loc[scenarios["coste_riesgo_total"].idxmax()]
    scenario_delta = float(worst_scenario["coste_riesgo_total"] / best_scenario["coste_riesgo_total"] - 1)
    critical_anomalies = int(read("anomaly_zone_intensity.csv")["anomalias_criticas"].sum())
    risk_counts = scoring["risk_tier"].value_counts()
    intervention_counts = scoring["recommended_intervention"].value_counts()
    high_or_critical = int(risk_counts.get("alto", 0) + risk_counts.get("critico", 0))
    tactical_count = int(
        intervention_counts.get("activar_flexibilidad", 0)
        + intervention_counts.get("optimizar_operacion", 0)
        + intervention_counts.get("monitorizar", 0)
    )

    pdf_path = REPORTS / "informe_analitico_red_electrificacion.pdf"
    doc = ReportDocTemplate(
        str(pdf_path), pagesize=A4, rightMargin=1.7 * cm, leftMargin=1.7 * cm,
        topMargin=1.55 * cm, bottomMargin=1.35 * cm,
        title="Cartera priorizada de red y electrificación",
        subject="Soporte a decisión para priorización de intervenciones de red",
        author="Sistema de Inteligencia de Red",
    )
    story = []

    # Deterministic edition stamp derived from the data horizon (not wall-clock).
    meses_es = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
                "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
    max_month = pd.to_datetime(monthly["mes"]).max()
    edition_label = f"Edición analítica · datos hasta {meses_es[max_month.month - 1]} de {max_month.year}"

    # 1. Cover.
    story.append(Table([[""]], colWidths=[17.6 * cm], rowHeights=[0.28 * cm], style=TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(ACCENT)),
    ])))
    story.extend([
        Spacer(1, 2.0 * cm),
        Paragraph("INFORME PARA DECISIÓN DE INVERSIÓN", styles["CoverLabelB4"]),
        Paragraph("Cartera priorizada de red y electrificación", styles["CoverTitleB4"]),
        Paragraph("Riesgo operativo, flexibilidad y secuencia de intervención a 24 meses", styles["CoverSubtitleB4"]),
        HRFlowable(width="100%", thickness=0.8, color=colors.HexColor(GRID), spaceBefore=4, spaceAfter=14),
        Paragraph(f"24 zonas · {len(nodes)} alimentadores · horizonte de secuenciación 0-24 meses", styles["SmallB4"]),
        Spacer(1, 3.0 * cm),
    ])
    # Bottom-line recommendation, anchored with a left accent rule so the lower
    # third reads as an intentional block rather than floating whitespace.
    bottom_line = Table(
        [[
            "",
            Paragraph(
                f"<font size='7.5' color='{ACCENT}'><b>RECOMENDACIÓN PRINCIPAL</b></font><br/><br/>"
                f"<font size='12.5' color='{INK}'>Autorizar la primera oleada de diagnóstico y mitigación, "
                "financiando sólo los estudios y medidas reversibles que convierten el ranking en "
                "expedientes de inversión defendibles.</font>",
                styles["StandfirstB4"],
            ),
        ]],
        colWidths=[0.45 * cm, 17.15 * cm],
        style=TableStyle([
            ("BACKGROUND", (0, 0), (0, 0), colors.HexColor(ACCENT)),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (1, 0), (1, 0), 12),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("LEFTPADDING", (0, 0), (0, 0), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ]),
    )
    story.extend([
        bottom_line,
        Spacer(1, 0.7 * cm),
        Paragraph("Los importes económicos son referencias relativas. Cualquier compromiso de capital exige ingeniería, costes reales, permisos, caso financiero y control de riesgo residual.", styles["SmallB4"]),
        Spacer(1, 3.0 * cm),
        HRFlowable(width="100%", thickness=0.6, color=colors.HexColor(GRID), spaceBefore=0, spaceAfter=8),
        Paragraph(f"{edition_label} · Documento de soporte a la decisión — no constituye aprobación de inversión", styles["SmallB4"]),
        PageBreak(),
    ])

    # 2. Executive summary.
    add_page_header(story, styles, "Resumen ejecutivo", "Cinco decisiones concentran el valor de la cartera", "La recomendación separa qué debe moverse ahora, qué debe madurar como expediente y qué sigue bloqueado hasta validar evidencia técnica y financiera.")
    story.append(metric_strip([
        ("Z013", "única zona crítica; prioridad 86,2"),
        (fmt_int(total_congestion), "horas-zona de congestión"),
        (fmt_dec(total_ens, 0), "MWh de energía no suministrada"),
        (f"{scenario_delta:.0%}", "sobrecoste de riesgo por retrasar CAPEX"),
    ], styles))
    story.append(Spacer(1, 10))
    story.append(decision_list([
        ("Autorizar diagnóstico inmediato de Z013", "Combina riesgo crítico, 7.225 horas de congestión, 4.152 MWh de ENS y brecha técnica de 759 MW; requiere mitigación transitoria mientras se define la alternativa final.", "Planificación de red | 30 días"),
        ("Lanzar cinco estudios de refuerzo local", "Z021, Z020, Z016, Z015 y Z019 presentan congestión estructural y cobertura flexible insuficiente; el mandato es madurar expedientes, no preaprobar obras.", "Ingeniería y regiones | 90 días"),
        ("Validar almacenamiento en Z001 y Z024", "La brecha flexible y el impacto de servicio justifican dimensionamiento por MW, MWh, ubicación, duración y efecto sobre curtailment antes de comprometer capital.", "Flexibilidad y operación | 90 días"),
        ("Ejecutar medidas tácticas con umbral", f"{tactical_count} zonas quedan en flexibilidad, optimización o monitorización; cada una necesita línea base, objetivo y gatillo de escalada.", "Operación regional | 6 meses"),
        ("Mantener bloqueada la aprobación de CAPEX", "Datos sintéticos, costes de referencia, escenarios sin probabilidad y ausencia de flujo de carga/N-1 impiden una decisión final de inversión.", "Comité de inversión | antes de aprobar"),
    ], styles))
    story.append(Spacer(1, 8))
    story.append(Paragraph("Recomendación al comité: aprobar la primera oleada de trabajo, estudios y mitigaciones reversibles; no aprobar todavía inversiones estructurales ni contratos de largo plazo.", styles["StandfirstB4"]))
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
        "La decisión es financiar la primera oleada de trabajo, no aprobar una cartera cerrada de CAPEX",
        "El modelo reduce el universo técnico a un conjunto defendible de expedientes que deben validarse con estudios, costes reales, restricciones de ejecución y riesgo residual.",
        major=True,
        metrics=[("24", "zonas evaluadas"), (str(high_or_critical), "zonas de riesgo alto o crítico"), ("6", "tipos de respuesta"), ("0-24", "meses de secuenciación")],
        paragraphs=[
            "La unidad de decisión es la zona, con trazabilidad a subestaciones y alimentadores. El índice final combina congestión, continuidad de suministro, brecha flexible, exposición de activos, electrificación y economía relativa; por tanto, el ranking debe leerse como una agenda de decisión, no como una lista de compras.",
            "El informe asigna una respuesta y un plazo, pero mantiene un umbral explícito entre priorización, diagnóstico y autorización. Esa separación es crítica: una zona puede requerir acción inmediata sin que la obra final esté definida.",
            "El mandato recomendado es movilizar equipos, cerrar incertidumbres y reducir exposición en el corto plazo. La aprobación de capital debe esperar a que cada expediente demuestre necesidad técnica, alternativa preferida, coste validado, beneficio esperado y factibilidad de ejecución.",
        ],
        table=big4_table([
            ["ETAPA", "PREGUNTA", "SALIDA"],
            ["Priorizar", "¿Dónde se concentra el riesgo?", "Ranking y plazo"],
            ["Diagnosticar", "¿Qué alternativa reduce mejor la exposición?", "Expediente técnico y económico"],
            ["Autorizar", "¿La solución supera los umbrales?", "Decisión de inversión"],
        ], [3.0 * cm, 7.3 * cm, 6.3 * cm], styles),
        implication=("Aprobar alcance, responsables y presupuesto de estudios de la primera oleada.", "Comité de inversión y dirección de red", "En la próxima sesión de cartera"),
    )
    add_analysis_page(
        story, styles, "1 | Mandato y base analítica",
        "El modelo es adecuado para ordenar decisiones; aún no es suficiente para comprometer capital",
        "La calidad analítica es consistente para priorización relativa, pero la base sintética y los costes de referencia limitan cualquier uso financiero o regulatorio.",
        metrics=[(fmt_int(len(monthly)), "observaciones zona-mes"), (fmt_int(len(feeder_priorities)), "alimentadores priorizados"),
                 (f"{forecast['nmae'].min():.2%}-{forecast['nmae'].max():.2%}", "rango de error normalizado"), ("14/14", "controles SQL superados")],
        visual=charts["15"],
        paragraphs=[
            "La estabilidad del pronóstico reduce el riesgo de que el ranking dependa de diferencias extremas de previsibilidad entre zonas. Aun así, la confianza predictiva no valida por sí sola la alternativa técnica: sólo indica que la señal temporal es utilizable para secuenciar.",
            "La validación pendiente es material: topología, contingencias, protecciones, costes licitados, permisos, ventanas de obra y restricciones regulatorias pueden cambiar la alternativa final o su orden de ejecución.",
            "El uso correcto del informe es decision-support ready: suficiente para abrir expedientes, comparar palancas y definir responsables; insuficiente para firmar CAPEX o contratos de capacidad de largo plazo.",
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
        paragraphs=[
            "La gobernanza debe mantener esta separación para evitar que un resultado reproducible se interprete como aprobación técnica o financiera.",
            "La trazabilidad analítica cubre consistencia de tablas, reglas, escenarios y ranking. La trazabilidad de inversión debe añadirse fuera del modelo: evidencia eléctrica, coste total, beneficio, permisos, dependencias y decisión formal por umbral.",
        ],
        implication=("Incorporar un control de aprobación técnica y financiera fuera del modelo analítico.", "Riesgo técnico y control de inversiones", "Antes de elevar cada expediente"),
    )

    # 7-16. Diagnosis.
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "La presión es persistente: el estrés operativo precede y supera a la congestión confirmada",
        "La red no muestra un alivio estructural durante el horizonte; la gestión preventiva debe actuar antes de que el estrés se convierta en congestión.",
        major=True, visual_pair=(charts["01"], charts["02"]),
        paragraphs=[
            "La distancia entre estrés y congestión identifica horas en las que flexibilidad, reconfiguración y mantenimiento dirigido pueden reducir exposición sin esperar a un evento formal.",
            "Para la dirección de red, esta diferencia es accionable: el estrés operativo debe activar medidas preventivas y no tratarse como ruido. Si el indicador se mantiene elevado durante dos ciclos de revisión, la zona debe pasar de monitorización a expediente de mitigación.",
        ],
        implication=("Activar umbrales mensuales de estrés y congestión para anticipar medidas operativas.", "Centro de operación de red", "Desde el próximo ciclo mensual"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        f"Cinco zonas concentran {top5_share:.0%} de la congestión, pero el riesgo exige coordinación regional",
        "La concentración favorece una primera oleada focalizada; la agregación regional organiza recursos sin sustituir el diagnóstico zonal.",
        visual_pair=(charts["03"], charts["11"]),
        paragraphs=[
            "El Pareto permite proteger recursos escasos de ingeniería: las primeras zonas explican una parte relevante de la congestión y deben recibir prioridad de análisis. Sin embargo, el mapa territorial evita una lectura excesivamente estrecha: dependencias regionales, subestaciones compartidas y ventanas de operación pueden cambiar la secuencia real.",
            "La decisión recomendada es doble: foco técnico en las zonas líderes y coordinación regional para no desplazar riesgo a territorios adyacentes.",
        ],
        implication=("Reservar capacidad de ingeniería para las cinco zonas líderes y coordinar dependencias por región.", "Planificación de red y direcciones regionales", "Próximos 90 días"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "Los perfiles territoriales justifican umbrales distintos; una política uniforme asignaría mal el capital",
        "Zonas industriales, urbanas, mixtas y rurales combinan de forma diferente carga punta, congestión y continuidad de suministro.",
        visual_pair=(charts["07"], charts["12"]),
        paragraphs=[
            "El tipo territorial explica contexto, pero no determina la intervención. Los umbrales deben calibrarse por perfil y confirmarse a nivel de zona.",
            "Una zona industrial puede justificar intervención por demanda nueva y continuidad de suministro; una zona urbana puede requerir otro umbral por densidad de clientes; una zona rural puede priorizarse por resiliencia aunque el volumen absoluto sea menor.",
        ],
        implication=("Recalibrar umbrales por tipo de zona antes del siguiente ciclo de priorización.", "Gobernanza analítica y planificación", "Próxima revisión trimestral"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "Z013 se separa del resto y requiere diagnóstico inmediato",
        "La posición de Z013 no depende de un único indicador: congestión, servicio, brecha flexible y anomalías convergen en la misma conclusión.",
        metrics=[("86,2", "índice de prioridad"), ("7.225", "horas de congestión"), ("4.152", "MWh no suministrados"), ("759", "MW de brecha técnica")],
        visual=charts["04"],
        paragraphs=[
            "Z013 no debe leerse como una recomendación automática de obra. Debe leerse como un caso donde el coste de esperar a tener toda la evidencia es superior al coste de abrir un diagnóstico inmediato con mitigación transitoria.",
            "La primera decisión debe fijar una línea base, separar síntomas de causa raíz y comparar refuerzo, operación, flexibilidad y almacenamiento bajo el mismo marco de beneficio esperado.",
        ],
        implication=("Abrir diagnóstico completo de Z013 y definir mitigación transitoria.", "Planificación de red con operación regional", "30 días"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "La cartera equilibra respuesta estructural y mitigación rápida",
        "La selección evita tratar todo el riesgo con refuerzo físico y reserva las medidas reversibles para ganar tiempo y evidencia.",
        visual_pair=(charts["05"], charts["06"]),
        paragraphs=[
            "Una intervención inmediata y cinco refuerzos conviven con once medidas flexibles u operativas y seis zonas en monitorización.",
            "Esta composición es saludable si se gobierna como cartera: los refuerzos maduran como expedientes; las medidas reversibles compran tiempo y reducen exposición; la monitorización preserva capital sólo si tiene gatillos claros de escalada.",
        ],
        implication=("Asignar presupuesto y responsable por familia de intervención, no solo por zona.", "Dirección de red y finanzas", "En la planificación anual"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "Congestión, servicio y brecha flexible explican la prioridad; la cobertura flexible sigue siendo insuficiente",
        "El índice final responde a varios factores, pero las zonas más expuestas tienden a operar por debajo del umbral de cobertura flexible.",
        visual_pair=(charts["08"], charts["09"]),
        paragraphs=[
            "La matriz evita una lectura unidimensional: una zona puede puntuar alto por congestión, otra por servicio y otra por flexibilidad. Por eso el driver dominante debe convertirse en el criterio principal del expediente.",
            "La cobertura flexible baja no invalida la flexibilidad como palanca, pero exige validación horaria y local. Contratar capacidad que no aparece en la hora crítica no reduce riesgo real.",
        ],
        implication=("Validar perfil horario, duración y ubicación de la flexibilidad antes de contratar capacidad.", "Flexibilidad y operación de red", "90 días"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "La energía no suministrada cambia el orden de atención respecto a la congestión pura",
        "El impacto de servicio exige proteger zonas donde cada evento afecta más energía o clientes, aunque no lideren las horas congestionadas.",
        visual_pair=(charts["10"], charts["13"]),
        paragraphs=[
            "La nueva demanda aumenta presión, pero la dispersión confirma que capacidad existente y perfil horario siguen siendo determinantes.",
            "En términos de comité, ENS es el puente entre ingeniería y negocio: permite defender que la prioridad no se basa sólo en carga, sino en continuidad, cliente y riesgo regulatorio.",
        ],
        implication=("Incluir reducción esperada de energía no suministrada en cada estudio de alternativa.", "Resiliencia de red y finanzas", "Durante los estudios de 90 días"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "Las anomalías aportan alerta temprana; el pronóstico permite secuenciar con confianza controlada",
        "Las señales precursoras ayudan a priorizar investigación, pero no prueban causalidad ni sustituyen el análisis operativo.",
        metrics=[(fmt_int(anomaly_types["n_eventos"].sum()), "eventos anómalos"), (fmt_int(critical_anomalies), "anomalías críticas"),
                 (f"{forecast['nmae'].max():.2%}", "error normalizado máximo"), ("3,5%", "umbral de confianza")],
        visual_pair=(charts["14"], charts["15"]),
        paragraphs=[
            "Las anomalías sirven como sistema de alerta y no como sentencia causal. Su valor está en acelerar investigación sobre zonas donde el ranking ya muestra presión técnica o de servicio.",
            "La calidad de forecast permite secuenciar con disciplina, pero cada decisión diferida debe tener fecha de relectura y variables de vigilancia; de lo contrario, diferir se convierte en aceptación pasiva de riesgo.",
        ],
        implication=("Revisar alarmas, curvas de carga y eventos de Z013 antes de seleccionar la solución.", "Operación y analítica de red", "30 días"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        f"Retrasar CAPEX eleva el coste de riesgo relativo en {scenario_delta:.0%}; combinar inversión y flexibilidad reduce más exposición",
        "Los escenarios respaldan una secuencia mixta: mitigación temprana mientras maduran los refuerzos estructurales.",
        visual_pair=(charts["16"], charts["17"]),
        paragraphs=[
            "El escenario combinado no debe transformarse en presupuesto automático. Su función es mostrar dirección: la exposición cae más cuando CAPEX, flexibilidad y almacenamiento se secuencian de forma coordinada.",
            "El riesgo de retrasar CAPEX justifica acelerar estudios, no saltarse controles. El comité debe exigir que cada expediente demuestre qué parte del riesgo reduce y qué riesgo residual queda.",
        ],
        implication=("Usar el escenario combinado como referencia para diseñar la secuencia, no como presupuesto aprobado.", "Planificación estratégica y finanzas", "En la revisión de cartera"),
    )
    add_analysis_page(
        story, styles, "2 | Diagnóstico de red",
        "Las primeras decisiones son estables y ya se traducen en una lista concreta de alimentadores",
        "La sensibilidad contenida en las posiciones prioritarias permite avanzar a diagnóstico sin esperar a una recalibración completa.",
        visual_pair=(charts["18"], charts["19"]),
        paragraphs=[
            "La sensibilidad es especialmente importante para evitar falsa precisión. Si las primeras posiciones se mantienen bajo cambios razonables de ponderación, la organización puede actuar sobre diagnóstico sin esperar a perfeccionar el modelo.",
            "La traducción a alimentadores convierte la cartera en trabajo técnico concreto: inspección, curvas de carga, capacidad firme, contingencias y restricciones de operación.",
        ],
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
        paragraphs=[
            "La primera cartera no debe gestionarse como una cola simple. Las zonas de refuerzo requieren ingeniería; las de flexibilidad requieren diseño contractual y disponibilidad horaria; las de operación requieren línea base y medición de efecto; las monitorizadas requieren gatillos de escalada.",
            "La oficina de cartera debe revisar semanalmente las urgencias y mensualmente las zonas planificadas, manteniendo trazabilidad entre score, driver, intervención, responsable y próximo hito.",
        ],
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
            "El criterio de éxito de 30 días no es tener obra aprobada; es tener causa raíz, opciones comparables, mitigación de corto plazo y decisión sobre el siguiente umbral.",
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
        paragraphs=[
            "Los estudios deben cubrir topología, contingencias, permisos, coste, reducción de riesgo y posibilidad de diferir inversión mediante flexibilidad.",
            "Para evitar sesgo de solución, cada expediente de refuerzo debe incluir una alternativa no-wire, una medida transitoria y una estimación de riesgo residual si se aplaza la obra.",
        ],
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
        paragraphs=[
            "La decisión de flexibilidad debe moverse de MW nominales a capacidad útil en ventana crítica. La misma capacidad puede tener valor muy distinto si aparece fuera del nodo, de la hora o de la duración que genera estrés.",
            "Storage debe justificarse cuando reduce simultáneamente punta, curtailment o resiliencia; si sólo mejora un indicador aislado, debe competir contra operación avanzada y contratos de flexibilidad.",
        ],
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
            "La monitorización reforzada debe tener dueño operativo. Sin propietario, cadencia y gatillos, la decisión deja de ser diferimiento disciplinado y pasa a ser riesgo aceptado sin gobierno.",
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
        paragraphs=[
            "El escenario de menor coste de riesgo confirma que la cartera debe combinar palancas, pero el informe no asigna probabilidades a escenarios. Por tanto, no existe valor esperado financiero en esta versión.",
            "La revisión trimestral debe comprobar si la realidad se acerca a un escenario adverso - retraso de CAPEX, degradación de activos o electrificación acelerada - y ajustar la secuencia antes de que el riesgo sea irreversible.",
        ],
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
        paragraphs=[
            "El valor de los primeros 90 días está en eliminar ambigüedad. Cada zona priorizada debe salir del ciclo con una de tres decisiones: avanzar a autorización, mantener mitigación con evidencia suficiente o retirar/reprogramar por falta de beneficio.",
            "El comité debe pedir un único formato de expediente para evitar discusiones incomparables entre regiones: línea base, causa raíz, alternativas, coste, beneficio, riesgo residual, dependencias y decisión solicitada.",
        ],
        table=big4_table(roadmap_rows, [2.2 * cm, 4.7 * cm, 5.5 * cm, 4.2 * cm], styles),
        implication=("Aprobar la hoja de ruta y nombrar responsables antes de liberar presupuesto de estudios.", "Comité de inversión", "Próxima sesión"),
    )
    add_analysis_page(
        story, styles, "4 | Ejecución y gobernanza",
        "La primera oleada debe cerrar cuatro umbrales antes de avanzar",
        "Cada expediente debe demostrar necesidad, alternativa preferida, economía validada y capacidad de ejecución.",
        metrics=[("1", "necesidad técnica confirmada"), ("2", "alternativas comparadas"), ("3", "coste y beneficio validados"), ("4", "permiso para avanzar")],
        paragraphs=[
            "La decisión de avanzar debe documentar reducción esperada de congestión y energía no suministrada, coste total, dependencias y riesgo residual.",
            "Una medida que no supere el umbral debe modificarse, diferirse o cancelarse; el ranking no debe utilizarse como justificación automática.",
            "Los cuatro umbrales deben quedar registrados en acta de cartera. Esto protege a la organización de dos errores habituales: sobreejecutar CAPEX donde una medida reversible bastaba, o diferir inversión estructural donde el riesgo ya no es aceptable.",
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
        paragraphs=[
            "La línea base debe fijarse antes de ejecutar; sin ella no es posible atribuir una mejora a la intervención ni decidir si debe renovarse.",
            "El seguimiento debe separar indicadores de resultado - reducción de estrés, congestión y ENS - de indicadores de proceso, como expedientes abiertos o estudios completados. Lo primero mide valor; lo segundo mide velocidad.",
        ],
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
            "La precisión aparente de scores y costes debe comunicarse con cuidado. El número sirve para comparar y ordenar; la decisión final requiere rangos, sensibilidad y criterios de aceptación.",
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
        "Aprobar la primera oleada de diagnóstico y mitigación; mantener el CAPEX estructural condicionado",
        "La cartera es suficientemente robusta para movilizar equipos y estudios, pero no para autorizar obras.",
        major=True,
        metrics=[("30 días", "diagnóstico Z013"), ("90 días", "refuerzos y almacenamiento"), ("6 meses", "medidas tácticas"), ("Trimestral", "revisión de cartera")],
        paragraphs=[
            "La aprobación solicitada cubre diagnóstico, estudios, mitigación transitoria y gobernanza de cartera.",
            "La aprobación no cubre inversiones estructurales hasta completar ingeniería, costes reales, valoración financiera y permisos.",
            "El retorno al comité debe traer expedientes comparables, no sólo una actualización del ranking. La decisión esperada será autorizar, rediseñar, diferir con umbral o cerrar cada caso.",
        ],
        table=big4_table(board_rows, [6.0 * cm, 3.7 * cm, 7.0 * cm], styles),
        implication=("Autorizar la primera oleada y exigir retorno al comité con expedientes completos.", "CFO y comité de inversión", "Próxima sesión"),
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
