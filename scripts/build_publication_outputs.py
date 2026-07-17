from __future__ import annotations

import os
import re
from importlib.resources import files
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/grid_publication_matplotlib")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import font_manager
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import PercentFormatter

try:
    from scripts.build_pdf_report import build_pdf_report
except ModuleNotFoundError:  # Ejecución directa: python scripts/build_publication_outputs.py
    from build_pdf_report import build_pdf_report

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed"
OUTPUTS = ROOT / "outputs"
GRAPHS = OUTPUTS / "graphs"
DASHBOARD = OUTPUTS / "dashboard"
REPORTS = OUTPUTS / "reports"

INK = "#1D1D1B"
MUTED = "#6B6F76"
LIGHT = "#EEF0EC"
GRID = "#D8D8D3"
ACCENT = "#1F3B2D"
ACCENT_LIGHT = "#B9C8BD"
DARK = "#2D5A42"
RISK = "#9C2B1B"
WHITE = "#FFFFFF"
AMBER = "#B07D2B"
NEUTRALS = ["#EEF0EC", "#D8D8D3", "#BAC1B9", "#929A93", "#6B6F76", "#465248"]
FONTS = ROOT / "assets" / "fonts"


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
    """Crea destinos sin borrar una publicación válida antes de reconstruirla."""
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    for folder in (GRAPHS, DASHBOARD, REPORTS):
        folder.mkdir(parents=True, exist_ok=True)


def prune_stale_graphs(generated: list[Path]) -> None:
    """Elimina PNG obsoletos solo después de generar la colección completa."""
    expected = {path.resolve() for path in generated}
    for path in GRAPHS.glob("*.png"):
        if path.resolve() not in expected:
            path.unlink()


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
    source_label = re.sub(r"^Fuente:\s*", "", source.strip(), flags=re.IGNORECASE).rstrip(".")
    fig.text(0.01, 0.005, f"Fuente: {source_label}. Elaboración propia.", fontsize=9, color=MUTED)


def generate_charts() -> list[Path]:
    for filename in ("Inter-Regular.ttf", "Inter-SemiBold.ttf"):
        font_manager.fontManager.addfont(FONTS / filename)
    plt.rcParams.update(
        {
            "font.family": "Inter",
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
    scenarios = read("scenario_summary.csv")
    anomaly_types = read("anomalies_summary_by_type.csv")
    forecast = read("forecast_error_by_zone.csv")
    forecast_calibration = read("forecast_calibration_summary.csv")
    sensitivity = read("scoring_sensitivity_analysis.csv")
    service = read("support_servicio_resiliencia.csv")
    electrification = read("support_electrificacion_presion.csv")
    feeder_priorities = read("prioridades_inversion_alimentadores.csv")

    paths: list[Path] = []

    # 01 Tendencia: demanda y carga neta.
    trend = (
        monthly.assign(mes=pd.to_datetime(monthly["mes"]))
        .groupby("mes", as_index=False)
        .agg(demanda=("demanda_total_mwh", "sum"), net_load=("net_load_total_mwh", "sum"))
    )
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    style_axes(
        ax,
        "La carga neta mantiene una trayectoria elevada durante todo el horizonte",
        "Demanda bruta y carga neta mensuales, GWh",
    )
    ax.plot(trend["mes"], trend["demanda"] / 1000, color=ACCENT, lw=2.5, label="Demanda bruta")
    ax.plot(trend["mes"], trend["net_load"] / 1000, color=DARK, lw=2, label="Carga neta")
    ax.fill_between(trend["mes"], trend["net_load"] / 1000, trend["demanda"] / 1000, color=ACCENT_LIGHT, alpha=0.35)
    ax.legend(frameon=False, ncol=2, loc="upper left")
    ax.set_ylabel("GWh")
    add_source(fig, "Fuente: serie mensual operativa por zona")
    paths.append(save_chart(fig, "01_tendencia_demanda_carga_neta.png"))

    # 02 Tendencia: congestión y estrés operativo.
    trend2 = (
        monthly.assign(mes=pd.to_datetime(monthly["mes"]))
        .groupby("mes", as_index=False)
        .agg(congestion=("horas_congestion", "sum"), stress=("horas_estres_operativo", "sum"))
    )
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    style_axes(
        ax,
        "El estrés operativo permanece por encima de la congestión confirmada",
        "Horas-zona mensuales; la distancia identifica presión previa a congestión",
    )
    ax.plot(trend2["mes"], trend2["stress"], color=DARK, lw=2.2, label="Estrés operativo")
    ax.plot(trend2["mes"], trend2["congestion"], color=ACCENT, lw=2.5, label="Congestión")
    ax.fill_between(trend2["mes"], trend2["congestion"], trend2["stress"], color=ACCENT_LIGHT, alpha=0.35)
    ax.legend(frameon=False, ncol=2, loc="upper left")
    ax.set_ylabel("Horas-zona")
    add_source(fig, "Fuente: serie mensual operativa por zona")
    paths.append(save_chart(fig, "02_evolucion_congestion_estres.png"))

    # 03 Concentration: Pareto.
    pareto = zone_risk.sort_values("horas_congestion", ascending=False).reset_index(drop=True)
    pareto["share_acum"] = pareto["horas_congestion"].cumsum() / pareto["horas_congestion"].sum()
    fig, ax = plt.subplots(figsize=(11.5, 6))
    style_axes(
        ax,
        "La congestión está concentrada, pero no limitada a una única zona",
        "Horas acumuladas y participación acumulada por zona",
    )
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
    add_source(fig, "Fuente: vista de riesgo operativo por zona")
    paths.append(save_chart(fig, "03_concentracion_congestion_pareto.png"))

    # 04 Ranking.
    rank = scoring.nsmallest(12, "priority_rank").sort_values("investment_priority_score")
    fig, ax = plt.subplots(figsize=(11.5, 6.4))
    style_axes(
        ax,
        "Z013 exige una decisión inmediata y separa claramente del resto de la cartera",
        "12 zonas con mayor índice de prioridad de inversión",
    )
    colors_rank = [ACCENT if z == "Z013" else DARK for z in rank["zona_id"]]
    bars = ax.barh(rank["zona_id"], rank["investment_priority_score"], color=colors_rank)
    ax.bar_label(bars, fmt="%.1f", padding=4, color=MUTED, fontsize=9)
    ax.set_xlabel("Índice 0-100")
    ax.set_xlim(0, 100)
    add_source(fig, "Fuente: tabla de puntuación de intervenciones")
    paths.append(save_chart(fig, "04_ranking_prioridad_zonas.png"))

    # 05 Composición. Tamaño y márgenes se alinean con el gráfico 06.
    PAIR_FIGSIZE = (11.5, 6.0)
    PAIR_MARGINS = {"left": 0.30, "right": 0.96, "top": 0.78, "bottom": 0.16}
    comp = scoring["recommended_intervention"].value_counts().sort_values()
    fig, ax = plt.subplots(figsize=PAIR_FIGSIZE)
    fig.subplots_adjust(**PAIR_MARGINS)
    style_axes(
        ax,
        "La cartera combina mitigación rápida con cinco refuerzos estructurales",
        "Número de zonas por intervención recomendada",
    )
    bars = ax.barh([slug_label(v) for v in comp.index], comp.values, color=[NEUTRALS[2]] * (len(comp) - 1) + [ACCENT])
    ax.bar_label(bars, padding=4, color=MUTED)
    ax.set_xlabel("Zonas")
    add_source(fig, "Fuente: tabla de puntuación de intervenciones")
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
    fig, ax = plt.subplots(figsize=PAIR_FIGSIZE)
    fig.subplots_adjust(**PAIR_MARGINS)
    style_axes(
        ax,
        "La evaluación reduce 24 zonas a una decisión urgente de corto plazo",
        "Embudo de priorización y secuenciación",
    )
    y = np.arange(len(funnel_labels))
    widths = np.array(funnel_values)
    left = (widths.max() - widths) / 2
    bars = ax.barh(y, widths, left=left, color=[NEUTRALS[1], NEUTRALS[2], DARK, ACCENT], height=0.65)
    ax.set_yticks(y, funnel_labels)
    ax.invert_yaxis()
    ax.set_xticks([])
    ax.grid(False)
    for bar, val in zip(bars, funnel_values, strict=False):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_y() + bar.get_height() / 2,
            str(val),
            ha="center",
            va="center",
            color=WHITE if val < 15 else INK,
            fontweight="bold",
            fontsize=12,
        )
    add_source(fig, "Fuente: tabla de puntuación de intervenciones")
    paths.append(save_chart(fig, "06_funnel_priorizacion.png"))

    # 07 Distribución.
    order = scoring.groupby("risk_tier")["investment_priority_score"].median().sort_values().index
    groups = [scoring.loc[scoring["risk_tier"] == tier, "investment_priority_score"] for tier in order]
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    style_axes(
        ax,
        "Los niveles de riesgo separan de forma consistente la intensidad de decisión",
        "Distribución de la puntuación de prioridad por nivel de riesgo",
    )
    tier_colors = [NEUTRALS[1], NEUTRALS[2], DARK, ACCENT][-len(groups) :]
    # Un boxplot con una sola zona queda ilegible; se muestra marcador y etiqueta directa.
    box_positions = [i + 1 for i, g in enumerate(groups) if len(g) > 1]
    box_groups = [g for g in groups if len(g) > 1]
    box_colors = [c for g, c in zip(groups, tier_colors, strict=False) if len(g) > 1]
    if box_groups:
        box = ax.boxplot(box_groups, positions=box_positions, tick_labels=None, patch_artist=True, widths=0.55)
        for patch, color in zip(box["boxes"], box_colors, strict=False):
            patch.set_facecolor(color)
        for median in box["medians"]:
            median.set_color(WHITE)
            median.set_linewidth(2)
    for i, (_tier, g, color) in enumerate(zip(order, groups, tier_colors, strict=False)):
        if len(g) == 1:
            x, y = i + 1, float(g.iloc[0])
            ax.scatter([x], [y], marker="D", s=70, color=color, edgecolor=WHITE, linewidth=1.2, zorder=5)
            ax.annotate(
                f"{scoring.loc[g.index[0], 'zona_id']} · única zona",
                (x, y),
                xytext=(10, 0),
                textcoords="offset points",
                va="center",
                fontsize=8.5,
                color=MUTED,
            )
    ax.set_xticks(range(1, len(order) + 1), [slug_label(v) for v in order])
    ax.set_xlim(0.4, len(order) + 0.6)
    ax.set_ylabel("Puntuación de prioridad")
    add_source(fig, "Fuente: tabla de puntuación de intervenciones")
    paths.append(save_chart(fig, "07_distribucion_puntuacion_por_riesgo.png"))

    # 08 Correlación.
    drivers = [
        "congestion_risk_score",
        "resilience_risk_score",
        "service_impact_score",
        "flexibility_gap_score",
        "asset_exposure_score",
        "electrification_pressure_score",
        "economic_priority_score",
        "investment_priority_score",
    ]
    corr = scoring[drivers].corr()
    labels = [
        "Congestión",
        "Resiliencia",
        "Servicio",
        "Flexibilidad",
        "Activos",
        "Electrificación",
        "Economía",
        "Prioridad",
    ]
    fig, ax = plt.subplots(figsize=(9, 7.2))
    cmap = LinearSegmentedColormap.from_list("neutral_accent", ["#EEF2F4", ACCENT])
    im = ax.imshow(corr, vmin=-1, vmax=1, cmap=cmap)
    ax.set_xticks(range(len(labels)), labels, rotation=45, ha="right")
    ax.set_yticks(range(len(labels)), labels)
    ax.set_title(
        "La prioridad final está más asociada a congestión, servicio y brecha flexible",
        loc="left",
        fontsize=16,
        fontweight="bold",
        color=INK,
        pad=18,
    )
    for i in range(len(labels)):
        for j in range(len(labels)):
            ax.text(
                j,
                i,
                f"{corr.iloc[i, j]:.2f}",
                ha="center",
                va="center",
                fontsize=8,
                color=WHITE if corr.iloc[i, j] > 0.65 else INK,
            )
    fig.colorbar(im, ax=ax, fraction=0.035, pad=0.03)
    add_source(fig, "Fuente: tabla de puntuación de intervenciones")
    paths.append(save_chart(fig, "08_correlacion_factores_prioridad.png"))

    # 09 Matriz de riesgo.
    fig, ax = plt.subplots(figsize=(11.5, 6.3))
    style_axes(
        ax,
        "El mayor riesgo coincide con coberturas flexibles insuficientes",
        "Riesgo operativo frente a relación flexibilidad/estrés; tamaño = brecha técnica",
    )
    sizes = 50 + 350 * (flex["gap_tecnico_mw"] / flex["gap_tecnico_mw"].max())
    ax.scatter(
        flex["ratio_flexibilidad_estres"],
        flex["riesgo_operativo_score"],
        s=sizes,
        c=[
            ACCENT if z in scoring.nsmallest(6, "priority_rank")["zona_id"].tolist() else NEUTRALS[2]
            for z in flex["zona_id"]
        ],
        alpha=0.85,
        edgecolor=WHITE,
        linewidth=0.8,
    )
    for _, row in flex.nlargest(8, "riesgo_operativo_score").iterrows():
        ax.annotate(
            row["zona_id"],
            (row["ratio_flexibilidad_estres"], row["riesgo_operativo_score"]),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=8,
            color=INK,
        )
    ax.axvline(0.15, color=RISK, ls="--", lw=1.2)
    ax.set_xlabel("Relación flexibilidad / estrés")
    ax.set_ylabel("Riesgo operativo")
    add_source(fig, "Fuente: brecha de flexibilidad por zona")
    paths.append(save_chart(fig, "09_matriz_riesgo_flexibilidad.png"))

    # 10 Resiliencia de servicio.
    top_service = service.nlargest(12, "ens_total").sort_values("ens_total")
    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    style_axes(
        ax,
        "La ENS confirma una exposición de servicio distinta de la congestión pura",
        "12 zonas con mayor energía no suministrada acumulada",
    )
    bars = ax.barh(
        top_service["zona_id"],
        top_service["ens_total"],
        color=[ACCENT if z == "Z013" else DARK for z in top_service["zona_id"]],
    )
    ax.bar_label(bars, labels=[fmt_int(v) for v in top_service["ens_total"]], padding=4, fontsize=8, color=MUTED)
    ax.set_xlabel("ENS (MWh)")
    add_source(fig, "Fuente: soporte de servicio y resiliencia")
    paths.append(save_chart(fig, "10_ranking_ens_servicio.png"))

    # 11 Geografía / regiones operativas.
    region = (
        zone_risk.groupby("region_operativa", as_index=False)
        .agg(
            riesgo=("riesgo_operativo_score", "mean"),
            congestion=("horas_congestion", "sum"),
            ens=("ens_total_mwh", "sum"),
            zonas=("zona_id", "nunique"),
        )
        .sort_values("riesgo")
    )
    top_regions = region.sort_values("riesgo", ascending=False)["region_operativa"].head(2).tolist()
    fig, ax = plt.subplots(figsize=(11.5, 6))
    style_axes(
        ax,
        f"{top_regions[0]} y {top_regions[1]} concentran la mayor presión operativa media",
        "Riesgo operativo medio por región operativa",
    )
    region_colors = [ACCENT if r in top_regions else NEUTRALS[2] for r in region["region_operativa"]]
    bars = ax.barh(region["region_operativa"], region["riesgo"], color=region_colors)
    ax.bar_label(bars, fmt="%.1f", padding=4, color=MUTED)
    ax.set_xlabel("Riesgo operativo medio")
    add_source(fig, "Fuente: vista de riesgo operativo por zona")
    paths.append(save_chart(fig, "11_geografia_riesgo_regiones.png"))

    # 12 Grupos territoriales.
    cohort = zone_risk.groupby("tipo_zona", as_index=False).agg(
        riesgo=("riesgo_operativo_score", "mean"),
        congestion=("horas_congestion", "mean"),
        ens=("ens_total_mwh", "mean"),
        carga=("carga_punta_mw", "mean"),
    )
    for col in ["riesgo", "congestion", "ens", "carga"]:
        cohort[col] = 100 * cohort[col] / cohort[col].max()
    matrix = cohort.set_index("tipo_zona")[["riesgo", "congestion", "ens", "carga"]]
    fig, ax = plt.subplots(figsize=(10, 5.8))
    im = ax.imshow(matrix, cmap=LinearSegmentedColormap.from_list("c", ["#EEF2F4", ACCENT]), vmin=0, vmax=100)
    ax.set_title(
        "Los grupos territoriales presentan perfiles de presión materialmente distintos",
        loc="left",
        fontsize=16,
        fontweight="bold",
        color=INK,
        pad=18,
    )
    ax.set_xticks(range(4), ["Riesgo", "Congestión", "ENS", "Carga punta"])
    ax.set_yticks(range(len(matrix)), [slug_label(v) for v in matrix.index])
    for i in range(len(matrix)):
        for j in range(4):
            ax.text(
                j,
                i,
                f"{matrix.iloc[i, j]:.0f}",
                ha="center",
                va="center",
                color=WHITE if matrix.iloc[i, j] > 62 else INK,
                fontweight="bold",
            )
    fig.colorbar(im, ax=ax, label="Índice relativo, máximo = 100", fraction=0.035, pad=0.03)
    add_source(fig, "Fuente: vista de riesgo operativo por zona")
    paths.append(save_chart(fig, "12_grupos_tipo_zona.png"))

    # 13 Correlación de electrificación.
    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    style_axes(
        ax,
        "La nueva demanda explica presión adicional, pero no determina sola la congestión",
        "Ratio de nueva demanda frente a horas de congestión",
    )
    ax.scatter(
        electrification["ratio_nueva_demanda"],
        electrification["horas_congestion"],
        s=90,
        color=ACCENT,
        alpha=0.8,
        edgecolor=WHITE,
    )
    for _, row in electrification.nlargest(7, "ratio_nueva_demanda").iterrows():
        ax.annotate(
            row["zona_id"],
            (row["ratio_nueva_demanda"], row["horas_congestion"]),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=8,
        )
    coef = np.polyfit(electrification["ratio_nueva_demanda"], electrification["horas_congestion"], 1)
    xx = np.linspace(electrification["ratio_nueva_demanda"].min(), electrification["ratio_nueva_demanda"].max(), 50)
    ax.plot(xx, coef[0] * xx + coef[1], color=DARK, lw=1.5)
    ax.xaxis.set_major_formatter(PercentFormatter(1))
    ax.set_xlabel("Nueva demanda / demanda total")
    ax.set_ylabel("Horas de congestión")
    add_source(fig, "Fuente: soporte de presión de electrificación")
    paths.append(save_chart(fig, "13_electrificacion_vs_congestion.png"))

    # 14 Composición de anomalías y señal precursora.
    at = anomaly_types.sort_values("n_eventos")
    fig, ax = plt.subplots(figsize=(11.5, 6.1))
    style_axes(
        ax,
        "Las anomalías de carga dominan el volumen y actúan como señal precursora",
        "Eventos por tipo; etiqueta indica porcentaje precursor de congestión",
    )
    bars = ax.barh(
        [slug_label(v) for v in at["anomaly_type"]],
        at["n_eventos"],
        color=[NEUTRALS[2], DARK, ACCENT, NEUTRALS[1], NEUTRALS[3]][: len(at)],
    )
    labels_anom = [
        f"{fmt_int(n)} | {p:.0%} precursor"
        for n, p in zip(at["n_eventos"], at["pct_precursor_congestion"], strict=False)
    ]
    ax.bar_label(bars, labels=labels_anom, padding=4, fontsize=8, color=MUTED)
    ax.set_xlabel("Eventos")
    add_source(fig, "Fuente: resumen de anomalías por tipo")
    paths.append(save_chart(fig, "14_anomalias_y_senal_precursora.png"))

    # 15 Precisión y calibración del pronóstico.
    fc = forecast.sort_values("nmae", ascending=True)
    calibration = forecast_calibration.sort_values("interval_level")
    fig, (ax_error, ax_coverage) = plt.subplots(
        1,
        2,
        figsize=(11.5, 6.35),
        gridspec_kw={"width_ratios": [1.65, 1.0]},
    )
    fig.suptitle(
        "Error zonal y calibración del pronóstico",
        x=0.04,
        y=0.975,
        ha="left",
        fontsize=16.5,
        fontweight="bold",
        color=INK,
    )
    fig.text(
        0.04,
        0.925,
        "NMAE por zona y cobertura empírica de los intervalos en el último fold",
        fontsize=10.5,
        color=MUTED,
    )

    error_colors = [RISK if value == fc["nmae"].max() else NEUTRALS[2] for value in fc["nmae"]]
    ax_error.barh(fc["zona_id"], fc["nmae"], color=error_colors, height=0.7)
    ax_error.axvline(0.035, color=RISK, ls="--", lw=1.1, label="Referencia 3,5%")
    ax_error.xaxis.set_major_formatter(PercentFormatter(1))
    ax_error.set_xlim(0, 0.04)
    ax_error.set_title("Error normalizado por zona", loc="left", fontsize=11.5, fontweight="bold", pad=10)
    ax_error.tick_params(axis="y", labelsize=7.5)
    ax_error.tick_params(axis="x", labelsize=8.5)
    ax_error.spines[["top", "right", "left"]].set_visible(False)
    ax_error.spines["bottom"].set_color(GRID)
    ax_error.grid(axis="x", color=GRID, linewidth=0.65)
    ax_error.set_axisbelow(True)
    ax_error.legend(frameon=False, loc="lower right", fontsize=8.5)

    y = np.arange(len(calibration))
    h = 0.32
    ax_coverage.barh(y + h / 2, calibration["nominal_coverage"], height=h, color=NEUTRALS[1], label="Nominal")
    ax_coverage.barh(
        y - h / 2,
        calibration["empirical_coverage"],
        height=h,
        color=[
            ACCENT if value >= nominal else RISK
            for value, nominal in zip(calibration["empirical_coverage"], calibration["nominal_coverage"], strict=False)
        ],
        label="Empírica",
    )
    ax_coverage.set_yticks(y, [f"{level:.0%}" for level in calibration["interval_level"]])
    ax_coverage.set_xlim(0.72, 1.0)
    ax_coverage.xaxis.set_major_formatter(PercentFormatter(1))
    ax_coverage.set_title("Cobertura de intervalos", loc="left", fontsize=11.5, fontweight="bold", pad=10)
    ax_coverage.tick_params(labelsize=8.5, length=0, colors=MUTED)
    ax_coverage.spines[["top", "right", "left"]].set_visible(False)
    ax_coverage.spines["bottom"].set_color(GRID)
    ax_coverage.grid(axis="x", color=GRID, linewidth=0.65)
    ax_coverage.set_axisbelow(True)
    ax_coverage.legend(frameon=False, loc="lower right", fontsize=8.5)
    for idx, value in enumerate(calibration["empirical_coverage"]):
        ax_coverage.text(value - 0.005, idx - h / 2, f"{value:.1%}", ha="right", va="center", fontsize=8, color=WHITE)

    fig.tight_layout(rect=[0.03, 0.075, 0.99, 0.89], w_pad=2.5)
    add_source(fig, "error por zona, calibración conformal y monitor de pronóstico")
    paths.append(save_chart(fig, "15_precision_y_calibracion_pronostico.png"))

    # 16 Scenarios.
    sc = scenarios.sort_values("coste_riesgo_total", ascending=False)
    fig, ax = plt.subplots(figsize=(11.5, 6.4))
    style_axes(
        ax,
        "CAPEX más flexibilidad reduce el coste de riesgo relativo frente al retraso",
        "Coste de riesgo por escenario; M€ de referencia relativa",
    )
    colors_sc = [
        RISK if s == "retraso_capex" else ACCENT if s == "capex_mas_flexibilidad" else NEUTRALS[2]
        for s in sc["scenario"]
    ]
    bars = ax.barh([slug_label(v) for v in sc["scenario"]], sc["coste_riesgo_total"] / 1_000_000, color=colors_sc)
    ax.bar_label(
        bars, labels=[f"{v / 1_000_000:.2f}" for v in sc["coste_riesgo_total"]], padding=4, fontsize=8, color=MUTED
    )
    ax.set_xlabel("M€ de referencia")
    add_source(fig, "Fuente: resumen de escenarios")
    paths.append(save_chart(fig, "16_comparacion_escenarios_riesgo.png"))

    # 17 Antes y después.
    compare_names = ["Retraso de CAPEX", "Almacenamiento adicional", "Flexibilidad adicional", "CAPEX + flexibilidad"]
    lookup = scenarios.set_index("scenario")
    compare_keys = [
        "retraso_capex",
        "despliegue_adicional_storage",
        "despliegue_adicional_flexibilidad",
        "capex_mas_flexibilidad",
    ]
    worst = float(lookup.loc["retraso_capex", "coste_riesgo_total"])
    reduction = [(worst - float(lookup.loc[k, "coste_riesgo_total"])) / worst for k in compare_keys]
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    style_axes(
        ax,
        "Las palancas combinadas capturan la mayor reducción de riesgo",
        "Reducción frente al escenario de retraso de CAPEX",
    )
    bars = ax.bar(compare_names, reduction, color=[NEUTRALS[2], DARK, DARK, ACCENT])
    top = max(reduction)
    ax.set_ylim(0, top * 1.25)
    # La barra de retraso de CAPEX es la línea base de reducción cero; se separa su etiqueta.
    for bar, value in zip(bars, reduction, strict=False):
        offset_pts = 14 if value == 0 else 4
        ax.annotate(
            f"{value:.0%}",
            (bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, offset_pts),
            textcoords="offset points",
            ha="center",
            color=MUTED,
            fontsize=9,
        )
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    ax.tick_params(axis="x", rotation=20)
    fig.subplots_adjust(bottom=0.26)  # espacio para que las etiquetas rotadas no invadan la fuente
    add_source(fig, "Fuente: resumen de escenarios")
    paths.append(save_chart(fig, "17_antes_despues_reduccion_riesgo.png"))

    # 18 Variance / sensitivity.
    base_rank = scoring.set_index("zona_id")["priority_rank"]
    sens = sensitivity.pivot(index="zona_id", columns="factor", values="rank_alt")
    sens["amplitude"] = sens.max(axis=1) - sens.min(axis=1)
    sens["base"] = base_rank
    top_sens = sens.nlargest(12, "amplitude").sort_values("amplitude")
    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    style_axes(
        ax,
        "La sensibilidad de ranking está contenida en las zonas prioritarias",
        "Amplitud de posición bajo factores alternativos",
    )
    bars = ax.barh(
        top_sens.index,
        top_sens["amplitude"],
        color=[ACCENT if int(top_sens.loc[z, "base"]) <= 6 else DARK for z in top_sens.index],
    )
    ax.bar_label(bars, fmt="%.0f", padding=4, color=MUTED)
    ax.set_xlabel("Posiciones de variación")
    add_source(fig, "Fuente: análisis de sensibilidad de la puntuación")
    paths.append(save_chart(fig, "18_variacion_sensibilidad_ranking.png"))

    # 19 Ranking de inversión por alimentador.
    feeders = feeder_priorities.head(15).sort_values("puntuacion_prioridad")
    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    style_axes(
        ax,
        "La priorización zonal se traduce en una lista concreta de alimentadores",
        "15 alimentadores con mayor prioridad",
    )
    bars = ax.barh(
        feeders["alimentador_id"],
        feeders["puntuacion_prioridad"],
        color=[ACCENT if t in {"Alta", "Crítica"} else DARK for t in feeders["nivel_prioridad"]],
    )
    ax.bar_label(bars, fmt="%.1f", padding=4, color=MUTED, fontsize=8)
    ax.set_xlabel("Puntuación de prioridad")
    add_source(fig, "Fuente: prioridades de inversión")
    paths.append(save_chart(fig, "19_ranking_alimentadores.png"))

    return paths


def make_dashboard_standalone() -> Path:
    dashboard_path = DASHBOARD / "grid-electrification-command-center.html"
    if not dashboard_path.exists():
        raise FileNotFoundError(f"Tablero no encontrado: {dashboard_path}")
    text = dashboard_path.read_text(encoding="utf-8")
    chart_js = files("grid_intelligence").joinpath("assets/chart.umd.min.js").read_text(encoding="utf-8")
    chart_js = chart_js.replace("</script", "<\\/script")
    text = re.sub(r"\s*<link rel=\"preconnect\"[^>]+/>\s*", "\n", text)
    text = re.sub(r"\s*<link href=\"https://fonts\.googleapis\.com[^\"]+\"[^>]+/>\s*", "\n", text)
    text = re.sub(
        r'<script src="(?:\.\./\.\./src/(?:grid_intelligence/)?assets/chart\.umd\.min\.js|https://cdn\.jsdelivr\.net/npm/chart\.js[^\"]*)" defer></script>',
        lambda _: f"<script>{chart_js}</script>",
        text,
    )
    dashboard_path.write_text(text, encoding="utf-8")
    (ROOT / "index.html").write_text(text, encoding="utf-8")
    return dashboard_path


def main() -> None:
    prepare_outputs()
    charts = generate_charts()
    prune_stale_graphs(charts)
    dashboard = make_dashboard_standalone()
    report = build_pdf_report()
    print(f"graphs={len(charts)}")
    print(f"dashboard={dashboard}")
    print(f"report={report}")


if __name__ == "__main__":
    main()
