"""Genera el tablero HTML autónomo: filtros, KPIs, gráficos Chart.js y gobernanza."""

from __future__ import annotations

import base64
import json
from importlib.resources import files

import pandas as pd

from .common import connect_database, ensure_dirs, get_paths


def _safe_records(df: pd.DataFrame, cols: list[str] | None = None) -> list[dict]:
    if df is None or df.empty:
        return []
    out = df.copy()
    if cols is not None:
        keep = [c for c in cols if c in out.columns]
        out = out[keep]
    for c in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = out[c].dt.strftime("%Y-%m-%d")
    return out.to_dict(orient="records")


def _compact_numeric(df: pd.DataFrame, decimals: int = 4) -> pd.DataFrame:
    out = df.copy()
    num_cols = out.select_dtypes(include=["number"]).columns
    if len(num_cols) > 0:
        out[num_cols] = out[num_cols].round(decimals)
    return out


def _norm(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0.0)
    smin = float(s.min())
    smax = float(s.max())
    if smax == smin:
        return pd.Series([50.0] * len(s), index=s.index)
    return 100.0 * (s - smin) / (smax - smin)


def _fmt_zone_list(df: pd.DataFrame, col: str = "zona_id", top_n: int = 3) -> str:
    if df.empty or col not in df.columns:
        return "N/A"
    vals = df[col].head(top_n).tolist()
    return ", ".join(vals)


def _driver_label(value: object) -> str:
    labels = {
        "congestion_risk_score": "congestión estructural",
        "flexibility_gap_score": "brecha de flexibilidad",
        "asset_exposure_score": "exposición de activos",
        "electrification_pressure_score": "presión de electrificación",
        "service_impact_score": "impacto de servicio",
        "economic_priority_score": "prioridad económica",
        "sin_driver": "sin factor dominante",
    }
    return labels.get(str(value), str(value).replace("_", " "))


def _intervention_label(value: object) -> str:
    labels = {
        "intervencion_inmediata_prioritaria": "intervención inmediata prioritaria",
        "reforzar_red_local": "refuerzo local de red",
        "desplegar_almacenamiento": "despliegue de almacenamiento",
        "activar_flexibilidad": "activación de flexibilidad",
        "optimizar_operacion": "optimización operativa",
        "sustituir_activos": "sustitución de activos",
        "monitorizar": "monitorización reforzada",
    }
    return labels.get(str(value), str(value).replace("_", " "))


def build_dashboard() -> str:
    paths = ensure_dirs(get_paths())
    conn = connect_database(paths)

    zone_risk = conn.execute(
        """
        SELECT
            zona_id,
            zona_nombre,
            tipo_zona,
            region_operativa,
            horas_congestion,
            severidad_media_congestion,
            energia_afectada_congestion_mwh,
            ens_total_mwh,
            clientes_afectados_total,
            carga_punta_mw,
            carga_relativa_max_media,
            criticidad_territorial,
            tension_crecimiento_demanda,
            presion_electrificacion_media,
            brecha_flex_media,
            riesgo_operativo_score
        FROM vw_zone_operational_risk
        """
    ).df()

    scoring = conn.execute(
        """
        SELECT
            zona_id,
            investment_priority_score,
            risk_tier,
            urgency_tier,
            main_risk_driver,
            recommended_intervention,
            recommended_sequence,
            confidence_flag,
            decision_forecast,
            capex_total,
            coste_riesgo_proxy,
            congestion_risk_score,
            resilience_risk_score,
            service_impact_score,
            flexibility_gap_score,
            asset_exposure_score,
            electrification_pressure_score,
            economic_priority_score,
            ratio_nueva_demanda,
            carga_punta_avg,
            horas_congestion_avg,
            ens_avg,
            ratio_flexibilidad_estres,
            gap_tecnico_mw,
            horizonte_medio
        FROM intervention_scoring_table
        """
    ).df()

    substations = conn.execute(
        """
        SELECT
            zona_id,
            subestacion_id,
            horas_congestion,
            energia_afectada_total_mwh,
            carga_relativa_max,
            pct_horas_congestion
        FROM kpi_top_subestaciones_congestion_acumulada
        """
    ).df()

    feeders = conn.execute(
        """
        SELECT
            zona_id,
            subestacion_id,
            alimentador_id,
            MAX(tipo_activo) AS tipo_activo_dominante,
            COUNT(*) AS activos_en_alimentador,
            AVG(exposicion_activo_score) AS exposicion_media,
            AVG(probabilidad_fallo_ajustada_proxy) AS probabilidad_fallo_ajustada_media,
            SUM(ens_subestacion_mwh) AS ens_asociada_mwh
        FROM vw_assets_exposure
        WHERE alimentador_id IS NOT NULL
        GROUP BY
            zona_id,
            subestacion_id,
            alimentador_id
        """
    ).df()

    flex_gap = conn.execute(
        """
        SELECT
            zona_id,
            zona_nombre,
            tipo_zona,
            region_operativa,
            riesgo_operativo_score,
            demanda_critica_mw,
            capacidad_flexible_mw,
            storage_potencia_total_mw,
            storage_energia_total_mwh,
            coste_activacion_flex_eur_mwh,
            gap_tecnico_mw,
            ratio_flexibilidad_estres,
            horas_congestion_acumuladas
        FROM vw_flexibility_gap
        """
    ).df()

    electrification = conn.execute(
        """
        SELECT
            zona_id,
            zona_nombre,
            tipo_zona,
            region_operativa,
            demanda_ev_mwh,
            demanda_industrial_mwh,
            demanda_nueva_total_mwh,
            ratio_demanda_nueva,
            carga_relativa_max_media,
            horas_congestion
        FROM kpi_zonas_afectadas_ev_industrial
        """
    ).df()

    capex_def = conn.execute(
        """
        SELECT
            zona_id,
            zona_nombre,
            tipo_zona,
            region_operativa,
            capex_refuerzo_eur,
            capex_flexibilidad_eur,
            capex_diferible_proxy_eur,
            prioridad_media_cartera
        FROM kpi_zonas_potencial_capex_diferible
        """
    ).df()

    monthly = conn.execute(
        """
        SELECT
            strftime('%Y-%m', mes) AS mes,
            AVG(carga_relativa_max_media) AS carga_relativa,
            SUM(horas_congestion) AS horas_congestion,
            SUM(ens_mwh) AS ens,
            SUM(demanda_ev_mwh) AS demanda_ev,
            SUM(demanda_industrial_mwh) AS demanda_industrial,
            SUM(curtailment_mwh) AS curtailment
        FROM mart_zone_month_operational
        GROUP BY 1
        ORDER BY 1
        """
    ).df()

    region_hour = conn.execute(
        """
        SELECT
            z.region_operativa,
            nh.hora,
            AVG(nh.carga_relativa) AS carga_relativa_media,
            AVG(CASE WHEN nh.flag_congestion THEN 1 ELSE 0 END) AS ratio_congestion_hora
        FROM vw_node_hour_operational_state nh
        LEFT JOIN stg_zonas_red z
            ON nh.zona_id = z.zona_id
        GROUP BY
            z.region_operativa,
            nh.hora
        ORDER BY
            z.region_operativa,
            nh.hora
        """
    ).df()

    interruptions = conn.execute(
        """
        SELECT
            zona_id,
            COUNT(*) AS n_interrupciones,
            SUM(clientes_afectados) AS clientes_afectados,
            SUM(energia_no_suministrada_mwh) AS ens_mwh,
            AVG(DATEDIFF('minute', timestamp_inicio, timestamp_fin)) AS duracion_media_min,
            AVG(CASE WHEN relacion_congestion_flag THEN 1 ELSE 0 END) AS ratio_relacion_congestion
        FROM stg_interrupciones_servicio
        GROUP BY zona_id
        """
    ).df()

    option_rows = conn.execute(
        """
        SELECT
            zona_id,
            option,
            impact,
            cost_proxy,
            time_proxy,
            robustez,
            option_score
        FROM intervention_multicriteria_options
        """
    ).df()

    asset_types = conn.execute(
        """
        SELECT DISTINCT tipo_activo
        FROM vw_assets_exposure
        WHERE tipo_activo IS NOT NULL
        ORDER BY tipo_activo
        """
    ).df()

    conn.close()

    scenario_summary = (
        pd.read_csv(paths.data_processed / "scenario_summary.csv")
        if (paths.data_processed / "scenario_summary.csv").exists()
        else pd.DataFrame()
    )
    scenario_impacts = (
        pd.read_csv(paths.data_processed / "scenario_impacts.csv")
        if (paths.data_processed / "scenario_impacts.csv").exists()
        else pd.DataFrame()
    )
    forecast_pressure = (
        pd.read_csv(paths.data_processed / "forecast_predictability_pressure.csv")
        if (paths.data_processed / "forecast_predictability_pressure.csv").exists()
        else pd.DataFrame()
    )

    if not feeders.empty:
        feeders["criticidad_feeder_score"] = (
            0.45 * _norm(feeders["probabilidad_fallo_ajustada_media"])
            + 0.35 * _norm(feeders["ens_asociada_mwh"])
            + 0.20 * _norm(feeders["exposicion_media"])
        )
        feeders = feeders.sort_values("criticidad_feeder_score", ascending=False).reset_index(drop=True)

    if not option_rows.empty:
        options_summary = option_rows.groupby("option", as_index=False).agg(
            impact_medio=("impact", "mean"),
            coste_medio=("cost_proxy", "mean"),
            tiempo_medio=("time_proxy", "mean"),
            robustez_media=("robustez", "mean"),
            option_score_medio=("option_score", "mean"),
        )
    else:
        options_summary = pd.DataFrame(
            columns=["option", "impact_medio", "coste_medio", "tiempo_medio", "robustez_media", "option_score_medio"]
        )

    zone_profile = zone_risk.merge(
        scoring[
            [
                "zona_id",
                "investment_priority_score",
                "risk_tier",
                "urgency_tier",
                "main_risk_driver",
                "recommended_intervention",
                "recommended_sequence",
                "confidence_flag",
                "decision_forecast",
                "electrification_pressure_score",
                "economic_priority_score",
                "flexibility_gap_score",
                "congestion_risk_score",
                "service_impact_score",
                "resilience_risk_score",
                "asset_exposure_score",
                "capex_total",
                "coste_riesgo_proxy",
                "horizonte_medio",
            ]
        ],
        on="zona_id",
        how="left",
    )

    zone_profile = zone_profile.merge(
        interruptions,
        on="zona_id",
        how="left",
    )

    zone_profile = zone_profile.merge(
        electrification[
            ["zona_id", "demanda_ev_mwh", "demanda_industrial_mwh", "demanda_nueva_total_mwh", "ratio_demanda_nueva"]
        ],
        on="zona_id",
        how="left",
    )

    zone_profile = zone_profile.fillna(0.0)

    if not scenario_impacts.empty:
        top_by_scenario = (
            scenario_impacts.sort_values(["scenario", "investment_priority_score_scenario"], ascending=[True, False])
            .groupby("scenario", as_index=False)
            .head(5)
        )
    else:
        top_by_scenario = pd.DataFrame()

    horas_congestion = float(zone_risk["horas_congestion"].sum()) if not zone_risk.empty else 0.0
    ens_total = float(zone_risk["ens_total_mwh"].sum()) if not zone_risk.empty else 0.0
    clientes_afectados = float(zone_risk["clientes_afectados_total"].sum()) if not zone_risk.empty else 0.0
    zonas_criticas = int((zone_risk["riesgo_operativo_score"] >= 75).sum()) if not zone_risk.empty else 0
    pct_zonas_criticas = 100.0 * zonas_criticas / max(len(zone_risk), 1)

    carga_media = float(zone_risk["carga_relativa_max_media"].mean()) if not zone_risk.empty else 0.0
    utilizacion_excesiva_pct = (
        100.0 * float((zone_risk["carga_relativa_max_media"] > 1.0).mean()) if not zone_risk.empty else 0.0
    )

    coste_riesgo = float(scoring["coste_riesgo_proxy"].sum()) if "coste_riesgo_proxy" in scoring.columns else 0.0
    capex_total = float(scoring["capex_total"].sum()) if "capex_total" in scoring.columns else 0.0
    capex_diferible = (
        float(capex_def["capex_diferible_proxy_eur"].sum()) if "capex_diferible_proxy_eur" in capex_def.columns else 0.0
    )
    capex_diferible_pct = 100.0 * capex_diferible / max(capex_total, 1.0)

    sae_duracion_media = float(interruptions["duracion_media_min"].mean()) if not interruptions.empty else 0.0
    total_int = float(interruptions["n_interrupciones"].sum()) if not interruptions.empty else 0.0
    saifi_proxy = (1000.0 * total_int / max(clientes_afectados, 1.0)) if clientes_afectados > 0 else 0.0

    resiliencia_indice = (
        100.0 - float(scoring["resilience_risk_score"].mean())
        if "resilience_risk_score" in scoring.columns and len(scoring)
        else 0.0
    )

    riesgo_base_proxy = coste_riesgo
    mejor_escenario_coste = (
        float(scenario_summary["coste_riesgo_total"].min()) if not scenario_summary.empty else riesgo_base_proxy
    )
    ahorro_potencial = max(riesgo_base_proxy - mejor_escenario_coste, 0.0)

    decisiones_diferibles = (
        int(
            scoring[
                scoring["decision_forecast"].astype(str).str.contains("diferir", case=False, na=False)
                & scoring["risk_tier"].isin(["bajo", "medio"])
            ]["zona_id"].nunique()
        )
        if not scoring.empty
        else 0
    )

    coverage_start = monthly["mes"].min() if not monthly.empty else "N/A"
    coverage_end = monthly["mes"].max() if not monthly.empty else "N/A"

    top_risk = zone_profile.sort_values("investment_priority_score", ascending=False).head(3)
    top_pressure = zone_profile.sort_values("presion_electrificacion_media", ascending=False).head(3)
    top_ens = zone_profile.sort_values("ens_total_mwh", ascending=False).head(3)

    driver_mix = (
        scoring["main_risk_driver"].value_counts(normalize=True).head(3).mul(100).round(1)
        if "main_risk_driver" in scoring.columns and len(scoring)
        else pd.Series(dtype=float)
    )
    driver_mix_txt = (
        ", ".join([f"{_driver_label(k)}: {v:.1f}%" for k, v in driver_mix.items()]) if not driver_mix.empty else "N/A"
    )

    intervention_mix = (
        scoring["recommended_intervention"].value_counts(normalize=True).mul(100).round(1)
        if "recommended_intervention" in scoring.columns and len(scoring)
        else pd.Series(dtype=float)
    )
    intervention_mix_txt = (
        ", ".join([f"{_intervention_label(k)}: {v:.1f}%" for k, v in intervention_mix.head(4).items()])
        if not intervention_mix.empty
        else "N/A"
    )

    validation_summary_path = paths.outputs_reports / "validation_summary.json"
    if validation_summary_path.exists():
        validation_summary = json.loads(validation_summary_path.read_text(encoding="utf-8"))
    else:
        validation_summary = {"overall_status": "N/A", "confidence_level": "N/A"}

    payload = {
        "zoneRisk": _safe_records(_compact_numeric(zone_risk)),
        "zoneProfile": _safe_records(_compact_numeric(zone_profile)),
        "scoring": _safe_records(_compact_numeric(scoring)),
        "substations": _safe_records(_compact_numeric(substations)),
        "feeders": _safe_records(_compact_numeric(feeders)),
        "flexGap": _safe_records(_compact_numeric(flex_gap)),
        "electrification": _safe_records(_compact_numeric(electrification)),
        "capexDef": _safe_records(_compact_numeric(capex_def)),
        "monthly": _safe_records(_compact_numeric(monthly)),
        "regionHour": _safe_records(_compact_numeric(region_hour)),
        "interruptions": _safe_records(_compact_numeric(interruptions)),
        "scenarioSummary": _safe_records(_compact_numeric(scenario_summary)),
        "scenarioImpacts": _safe_records(_compact_numeric(scenario_impacts)),
        "scenarioTopZones": _safe_records(_compact_numeric(top_by_scenario)),
        "optionsSummary": _safe_records(_compact_numeric(options_summary)),
        "optionsByZone": _safe_records(_compact_numeric(option_rows)),
        "forecastPressure": _safe_records(_compact_numeric(forecast_pressure)),
        "assetTypes": asset_types["tipo_activo"].tolist() if not asset_types.empty else [],
    }

    kpi_static = {
        "horas_congestion": horas_congestion,
        "zonas_criticas": zonas_criticas,
        "pct_zonas_criticas": pct_zonas_criticas,
        "ens_total": ens_total,
        "clientes_afectados": clientes_afectados,
        "carga_media": carga_media,
        "utilizacion_excesiva_pct": utilizacion_excesiva_pct,
        "coste_riesgo": coste_riesgo,
        "capex_total": capex_total,
        "capex_diferible": capex_diferible,
        "capex_diferible_pct": capex_diferible_pct,
        "ahorro_potencial": ahorro_potencial,
        "saidi_proxy": sae_duracion_media,
        "saifi_proxy": saifi_proxy,
        "resiliencia_indice": resiliencia_indice,
        "decisiones_diferibles": decisiones_diferibles,
    }

    executive_insights = [
        f"La red no requiere una respuesta homogénea: {zonas_criticas} zonas críticas ({pct_zonas_criticas:.1f}% del total) concentran la decisión inmediata, con {_fmt_zone_list(top_risk, 'zona_id', 3)} como primer perímetro de comité.",
        f"El diagnóstico es explicable: los factores dominantes de la puntuación son {driver_mix_txt}; cualquier expediente debe demostrar que la palanca propuesta ataca el factor principal, no sólo el síntoma.",
        f"La presión futura no sustituye al riesgo actual: electrificación tensiona {_fmt_zone_list(top_pressure, 'zona_id', 3)}, mientras que la ENS obliga a proteger {_fmt_zone_list(top_ens, 'zona_id', 3)} aunque no siempre lideren congestión.",
        f"El mix de intervención obliga a gestión de cartera: {intervention_mix_txt}; refuerzo, flexibilidad, operación, almacenamiento y monitorización deben gobernarse con umbrales distintos.",
        f"CAPEX diferible estimado: €{capex_diferible:,.0f} ({capex_diferible_pct:.1f}% del CAPEX evaluado), válido sólo donde el nivel, el pronóstico y la cobertura flexible soportan diferimiento.",
        f"Ahorro potencial de coste de riesgo frente a base: €{ahorro_potencial:,.0f}; depende de ejecutar selectivamente flexibilidad/almacenamiento y no debe leerse como presupuesto aprobado.",
    ]

    chart_js = files("grid_intelligence").joinpath("assets/chart.umd.min.js").read_text(encoding="utf-8")
    chartjs_script = f"<script>{chart_js.replace('</script', '<\\/script')}</script>"

    def _font_face(family: str, filename: str, weight_range: str) -> str:
        raw = files("grid_intelligence").joinpath(f"assets/{filename}").read_bytes()
        uri = "data:font/woff2;base64," + base64.b64encode(raw).decode("ascii")
        return (
            f"@font-face{{font-family:'{family}';font-style:normal;"
            f"font-weight:{weight_range};font-display:swap;"
            f"src:url({uri}) format('woff2');}}"
        )

    fontface_css = "\n".join(
        [
            _font_face("Fraunces", "fraunces-var-latin.woff2", "340 660"),
            _font_face("Archivo", "archivo-var-latin.woff2", "360 760"),
        ]
    )

    html_template = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Centro de Decisión de Red | Inteligencia de Congestión, Flexibilidad e Inversión</title>
  __CHARTJS_SCRIPT__
  <style>
__FONTFACE__

    :root {
      /* Grid Intelligence — "Parte de situación": briefing institucional sobre papel.
         Una sola superficie continua, filetes editoriales, tipografía de tinta y un
         único acento ultramar; rojo/ámbar/verde quedan reservados a semántica de riesgo. */
      --font-display: 'Fraunces', Georgia, 'Times New Roman', serif;
      --font-sans: 'Archivo', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;

      --bg: #f3f0e8;
      --surface: #faf8f1;
      --surface-2: #ece8dc;
      --surface-3: #e4dfd0;
      --ink: #1b1c1e;
      --ink-soft: #3d3e40;
      --muted: #67665f;
      --faint: #7b7a71;
      --line: #d8d2c2;
      --line-strong: #b5ae9c;
      --rule: #1b1c1e;

      --accent: #2743a3;
      --accent-ink: #2743a3;
      --accent-weak: rgba(39, 67, 163, .09);
      --accent-line: rgba(39, 67, 163, .35);
      --amber: #8f5e0a;
      --red: #a92c21;
      --green: #226b41;

      /* Placa de tinta invertida (decisión ejecutiva). */
      --plate-bg: #191a1d;
      --plate-ink: #f0ede4;
      --plate-soft: #c9c6ba;
      --plate-muted: #98958a;
      --plate-line: rgba(240, 237, 228, .16);
      --plate-accent: #a9bdf5;

      --chart-tick: #56554e;
      --chart-grid: rgba(27, 28, 30, .08);

      --risk-critical-bg: #f1dcd6; --risk-critical-ink: #872015;
      --risk-high-bg: #f0e2cd; --risk-high-ink: #84400f;
      --risk-watch-bg: #eee5c4; --risk-watch-ink: #6b510d;
      --risk-ok-bg: #dcead7; --risk-ok-ink: #22552f;

      --radius: 8px;
      --radius-lg: 12px;
      --radius-sm: 5px;
      --focus: rgba(39, 67, 163, .5);
    }
    body[data-theme="dark"] {
      --bg: #111113;
      --surface: #18181b;
      --surface-2: #1e1e21;
      --surface-3: #242428;
      --ink: #eae7dd;
      --ink-soft: #cecbc0;
      --muted: #969389;
      --faint: #86847b;
      --line: #2b2b2f;
      --line-strong: #434347;
      --rule: #eae7dd;

      --accent: #94aef2;
      --accent-ink: #94aef2;
      --accent-weak: rgba(148, 174, 242, .13);
      --accent-line: rgba(148, 174, 242, .40);
      --amber: #d9a84e;
      --red: #ee8378;
      --green: #82c9a0;

      --plate-bg: #eae7dd;
      --plate-ink: #18181b;
      --plate-soft: #3f4042;
      --plate-muted: #5d5e57;
      --plate-line: rgba(24, 24, 27, .18);
      --plate-accent: #2743a3;

      --chart-tick: #a3a199;
      --chart-grid: rgba(234, 231, 221, .09);

      --risk-critical-bg: rgba(169, 44, 33, .26); --risk-critical-ink: #f3c1ba;
      --risk-high-bg: rgba(154, 84, 24, .26); --risk-high-ink: #efc9a2;
      --risk-watch-bg: rgba(143, 108, 20, .28); --risk-watch-ink: #ecd79a;
      --risk-ok-bg: rgba(34, 107, 65, .26); --risk-ok-ink: #b9e2c6;

      --focus: rgba(148, 174, 242, .55);
    }

    * { box-sizing: border-box; }
    /* El bloque de movimiento reducido más abajo revierte esto a `auto`. */
    html { -webkit-text-size-adjust: 100%; scroll-behavior: smooth; }
    /* Un salto desde el índice debe dejar el título del capítulo por debajo de
       la barra de perímetro pegada, no oculto tras ella. */
    .section, .exec-decision { scroll-margin-top: 68px; }
    body {
      margin: 0;
      font-family: var(--font-sans);
      font-size: 15px;
      color: var(--ink);
      background-color: var(--bg);
      line-height: 1.55;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
      text-rendering: optimizeLegibility;
      transition: background-color .3s ease, color .3s ease;
    }
    h1, h2, h3, h4 { font-family: var(--font-sans); font-weight: 700; letter-spacing: -0.01em; }
    ::selection { background: var(--accent-weak); }

    /* Cifras tabulares en cada lectura de instrumento. */
    .num, .kpi .v, .drill-metric .v, .bench-card .v,
    .hm .v, .filter-chip, .table-count, .stat-figure .v, .status-chip .v,
    .decision-card .metric, .heatmap td, table td, .whatif .rng-val, .sec-idx {
      font-variant-numeric: tabular-nums;
      font-feature-settings: "tnum" 1;
    }

    .skip-link {
      position: absolute; left: 14px; top: -52px; z-index: 1000;
      background: var(--surface); color: var(--ink);
      border: 2px solid var(--accent); border-radius: 8px;
      padding: 10px 12px; font-weight: 700; transition: top .16s ease;
    }
    .skip-link:focus { top: 12px; }
    :focus-visible { outline: 3px solid var(--focus); outline-offset: 3px; }

    /* Barras de desplazamiento finas. */
    .sidebar, .tbl-wrap, .heatmap-wrap { scrollbar-width: thin; scrollbar-color: var(--line-strong) transparent; }
    .sidebar::-webkit-scrollbar, .tbl-wrap::-webkit-scrollbar, .heatmap-wrap::-webkit-scrollbar { width: 9px; height: 9px; }
    .sidebar::-webkit-scrollbar-thumb, .tbl-wrap::-webkit-scrollbar-thumb, .heatmap-wrap::-webkit-scrollbar-thumb { background: rgba(140,136,120,.35); border-radius: 20px; border: 2px solid transparent; background-clip: padding-box; }

    .layout {
      display: grid;
      grid-template-columns: 292px minmax(0, 1fr);
      gap: 40px;
      align-items: start;
      max-width: 1640px;
      margin: 0 auto;
      padding: 0 36px;
    }

    /* ---------- Rail de documento (sidebar) ---------- */
    .sidebar {
      position: sticky;
      top: 0;
      max-height: 100vh;
      overflow-y: auto;
      padding: 34px 30px 34px 0;
      border-right: 1px solid var(--line);
    }
    .brand { display: flex; align-items: center; gap: 11px; margin-bottom: 20px; }
    .brand-mark {
      width: 34px; height: 34px; border-radius: 7px; flex: 0 0 auto;
      background: var(--ink); color: var(--bg);
      display: grid; place-items: center;
    }
    body[data-theme="dark"] .brand-mark { background: var(--rule); color: var(--bg); }
    .brand-name { font-size: .93rem; font-weight: 700; letter-spacing: .005em; color: var(--ink); line-height: 1.1; }
    .brand-sub { font-size: .6rem; text-transform: uppercase; letter-spacing: .18em; color: var(--faint); margin-top: 3px; font-weight: 600; }
    .sidebar-head { padding-bottom: 15px; border-bottom: 1px solid var(--line); margin-bottom: 13px; }
    .sidebar-eyebrow {
      font-size: .6rem; text-transform: uppercase; letter-spacing: .18em;
      color: var(--faint); margin-bottom: 6px; font-weight: 700;
    }
    .sidebar h2 { margin: 0 0 7px 0; font-size: 1.06rem; font-weight: 700; letter-spacing: -.01em; color: var(--ink); }
    .sidebar .hint { font-size: .76rem; color: var(--muted); margin: 0; line-height: 1.45; }
    .theme-switch { margin: 13px 0; }
    .theme-btn {
      width: 100%; border: 1px solid var(--line-strong);
      background: transparent; color: var(--ink);
      border-radius: 7px; padding: 9px 12px; font-family: var(--font-sans);
      font-weight: 600; font-size: .8rem; cursor: pointer;
      display: flex; align-items: center; justify-content: center; gap: 8px;
      transition: background .16s ease, border-color .16s ease;
    }
    .theme-btn:hover { background: var(--surface-2); border-color: var(--ink-soft); }
    .sidebar-section-label {
      margin-top: 18px; margin-bottom: 2px; font-size: .6rem; text-transform: uppercase;
      letter-spacing: .17em; color: var(--faint); font-weight: 700;
      display: flex; align-items: center; gap: 9px;
    }
    .sidebar-section-label::after { content: ""; flex: 1; height: 1px; background: var(--line); }
    .sidebar label { display: block; margin-top: 11px; font-size: .76rem; color: var(--ink-soft); font-weight: 500; }
    .sidebar select, .sidebar input[type="text"] {
      appearance: none; width: 100%; margin-top: 6px;
      padding: 9px 34px 9px 11px; border-radius: 7px;
      border: 1px solid var(--line);
      background-color: var(--surface); color: var(--ink);
      font-family: var(--font-sans); font-size: .83rem; outline: none;
      background-image: linear-gradient(45deg, transparent 50%, var(--muted) 50%), linear-gradient(135deg, var(--muted) 50%, transparent 50%);
      background-position: calc(100% - 17px) calc(50% + 1px), calc(100% - 12px) calc(50% + 1px);
      background-size: 5px 5px, 5px 5px; background-repeat: no-repeat;
      transition: border-color .16s ease, box-shadow .16s ease;
    }
    .sidebar select:focus, .sidebar input[type="text"]:focus {
      border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-weak);
    }
    .btn-row { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 18px; }
    .btn-row.single { grid-template-columns: 1fr; }
    .btn {
      border: 0; border-radius: 7px; padding: 10px 14px;
      font-family: var(--font-sans); font-weight: 600; font-size: .8rem;
      letter-spacing: -0.005em; cursor: pointer;
      transition: transform .16s cubic-bezier(.4,0,.2,1), filter .16s ease, background .16s ease, border-color .16s ease;
    }
    .btn:active { transform: scale(.975); }
    .btn.primary { background: var(--ink); color: var(--bg); }
    .btn.primary:hover { filter: brightness(1.16); }
    body[data-theme="dark"] .btn.primary { background: var(--rule); color: var(--bg); }
    .btn.ghost { background: transparent; color: var(--ink); border: 1px solid var(--line-strong); }
    .btn.ghost:hover { background: var(--surface-2); }
    .btn.secondary { background: transparent; color: var(--ink); border: 1px solid var(--line-strong); }
    .btn.secondary:hover { background: var(--surface-2); }

    .main { padding: 34px 0 56px; min-width: 0; }

    /* ---------- Cabecera de publicación ---------- */
    .hero {
      position: relative;
      padding: 24px 0 0;
      border-top: 3px solid var(--rule);
    }
    .hero::before { content: ""; position: absolute; top: 5px; left: 0; right: 0; height: 1px; background: var(--rule); }
    .hero-statusline {
      display: flex; align-items: center; justify-content: space-between;
      flex-wrap: wrap; gap: 10px 18px; margin-bottom: 30px;
    }
    .hero-kicker {
      font-size: .68rem; text-transform: uppercase; letter-spacing: .22em;
      color: var(--ink); font-weight: 700; margin: 0;
      display: inline-flex; align-items: center; gap: 10px;
    }
    .hero-kicker::before { content: ""; width: 8px; height: 8px; background: var(--accent); }
    .hero-status-chips { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }
    .status-chip {
      display: inline-flex; align-items: center; gap: 7px;
      padding: 4px 11px; border-radius: 999px;
      border: 1px solid var(--line-strong); background: transparent;
      font-size: .7rem; font-weight: 600; color: var(--muted); white-space: nowrap;
    }
    .status-chip .led { width: 6px; height: 6px; border-radius: 50%; background: var(--line-strong); }
    .status-chip.live .led { background: var(--green); }
    .status-chip.warn .led { background: var(--amber); }
    .status-chip .k { color: var(--faint); font-weight: 600; letter-spacing: .02em; }
    .status-chip .v { color: var(--ink); font-weight: 700; }
    .hero h1 {
      margin: 0; font-family: var(--font-display); font-size: clamp(2.5rem, 4.2vw, 3.55rem); font-weight: 480;
      font-optical-sizing: auto; font-variation-settings: "opsz" 144, "wght" 480;
      letter-spacing: -0.018em; max-width: 24ch; line-height: 1.02; color: var(--ink);
    }
    .hero .subtitle {
      margin-top: 18px; color: var(--ink-soft); max-width: 70ch;
      font-size: 1.05rem; line-height: 1.6; letter-spacing: -0.004em;
    }
    .hero-metrics {
      display: flex; flex-wrap: wrap; gap: 0; margin-top: 32px;
      border-top: 1px solid var(--line-strong);
    }
    .hm {
      padding: 15px 30px 2px 0; margin-right: 30px;
      border-right: 1px solid var(--line);
      display: flex; flex-direction: column; gap: 4px;
    }
    .hm:last-child { border-right: 0; }
    .hm .v { font-size: 1.55rem; font-weight: 700; color: var(--ink); line-height: 1; letter-spacing: -0.012em; }
    .hm .v small { font-size: .8rem; font-weight: 600; color: var(--muted); }
    .hm .k { font-size: .65rem; text-transform: uppercase; letter-spacing: .12em; color: var(--faint); font-weight: 600; }
    .hero-ridge { position: relative; height: 84px; margin: 20px 0 0; border-bottom: 1px solid var(--line-strong); }
    .hero-ridge svg { display: block; width: 100%; height: 100%; }
    .ridge-area { fill: var(--accent-weak); }
    .ridge-line { stroke: var(--accent); }
    .ridge-thr { stroke: var(--red); }

    /* ---------- Índice del parte (rail de documento) ---------- */
    .doc-index { display: flex; flex-direction: column; margin-top: 9px; }
    .doc-index a {
      position: relative; display: grid; grid-template-columns: 1.55rem 1fr;
      align-items: baseline; gap: 8px;
      padding: 5px 0 5px 11px; text-decoration: none;
      border-left: 2px solid var(--line);
      color: var(--muted); font-size: .76rem; line-height: 1.34;
      transition: color .16s ease, border-color .16s ease;
    }
    .doc-index a .di-n {
      font-variant-numeric: tabular-nums; font-size: .7rem; font-weight: 700;
      color: var(--faint); letter-spacing: .02em;
    }
    .doc-index a:hover { color: var(--ink); border-left-color: var(--line-strong); }
    .doc-index a.is-active {
      color: var(--ink); font-weight: 600; border-left-color: var(--accent);
    }
    .doc-index a.is-active .di-n { color: var(--accent-ink); }

    /* ---------- Perímetro activo (persiste durante la lectura) ---------- */
    .active-filters {
      margin-top: 16px; display: flex; align-items: center; flex-wrap: wrap;
      gap: 10px 14px;
      position: sticky; top: 0; z-index: 30;
      padding: 9px 0; background: var(--bg);
      transition: border-color .2s ease, box-shadow .2s ease;
      border-bottom: 1px solid transparent;
    }
    .active-filters.is-stuck {
      border-bottom-color: var(--line);
      box-shadow: 0 10px 20px -18px rgba(0,0,0,.55);
    }
    .active-filters-kicker {
      font-size: .62rem; text-transform: uppercase; letter-spacing: .16em;
      color: var(--faint); font-weight: 700; white-space: nowrap;
    }
    .filter-pills { display: flex; flex-wrap: wrap; gap: 7px; }
    .filter-chip {
      display: inline-flex; align-items: center; gap: 7px;
      padding: 5px 11px; border-radius: 999px; border: 1px solid var(--line-strong);
      background: transparent; color: var(--ink-soft);
      font-size: .72rem; font-weight: 700; line-height: 1.2;
    }
    .filter-chip .k { color: var(--faint); font-weight: 600; }
    .filter-chip.muted { border-color: var(--line); color: var(--muted); }

    /* ---------- Rótulos editoriales ---------- */
    .board-label { display: flex; align-items: baseline; gap: 14px; margin: 44px 0 0; }
    .board-label .t {
      font-size: .7rem; text-transform: uppercase; letter-spacing: .16em;
      font-weight: 700; color: var(--ink); white-space: nowrap;
      display: inline-flex; align-items: center; gap: 10px;
    }
    .board-label .t::before { content: ""; width: 22px; height: 2px; background: var(--accent); }
    .board-label .n { font-size: .78rem; color: var(--muted); line-height: 1.4; }

    /* ---------- Tablero de situación (registro tipográfico) ---------- */
    .decision-grid {
      margin-top: 14px; display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 26px 36px;
    }
    .decision-card {
      position: relative; background: transparent; border: 0;
      border-top: 1px solid var(--line-strong);
      padding: 13px 0 4px;
      min-width: 0; display: flex; flex-direction: column;
    }
    .decision-card:first-child { border-top: 2px solid var(--accent); }
    .decision-card .eyebrow {
      font-size: .62rem; text-transform: uppercase; letter-spacing: .13em;
      color: var(--faint); font-weight: 700; margin-bottom: 9px;
    }
    .decision-card .eyebrow .q { color: var(--accent-ink); margin-right: 3px; font-weight: 800; }
    .decision-card h3 {
      margin: 0; font-size: .8rem; font-weight: 500; line-height: 1.3;
      color: var(--muted); min-height: 2.1rem;
    }
    .decision-card .metric {
      margin-top: 7px; font-family: var(--font-display); font-size: 1.5rem; font-weight: 560;
      font-optical-sizing: auto; font-variation-settings: "opsz" 44, "wght" 560;
      line-height: 1.12; letter-spacing: -0.008em; color: var(--ink);
    }
    .decision-card p { margin: 0; margin-top: auto; padding-top: 12px; font-size: .77rem; line-height: 1.48; color: var(--muted); }
    .decision-card strong { color: var(--ink-soft); font-weight: 700; }

    /* ---------- Cifras en juego ---------- */
    .stakes-bar { margin-top: 34px; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 26px 36px; }
    .stat-figure {
      position: relative; background: transparent;
      border: 0; border-top: 1px solid var(--line-strong);
      padding: 13px 0 4px;
      display: flex; align-items: flex-start; gap: 26px;
    }
    .stakes-bar .stat-figure:nth-child(1) { border-top: 2px solid var(--red); }
    .stakes-bar .stat-figure:nth-child(2) { border-top: 2px solid var(--accent); }
    .stat-figure .lead { flex: 0 0 auto; min-width: 212px; }
    .stat-figure .eyebrow {
      font-size: .62rem; text-transform: uppercase; letter-spacing: .12em;
      color: var(--faint); font-weight: 700; margin-bottom: 5px;
      display: inline-flex; align-items: center; gap: 7px;
    }
    .stat-figure h3 { margin: 0; font-size: .82rem; font-weight: 500; color: var(--muted); }
    .stat-figure .v {
      font-family: var(--font-display); font-size: 2.05rem; font-weight: 560;
      font-optical-sizing: auto; font-variation-settings: "opsz" 60, "wght" 560;
      letter-spacing: -0.015em; line-height: 1; margin-top: 8px; color: var(--ink);
    }
    .stat-figure p { margin: 0; padding-top: 26px; font-size: .77rem; line-height: 1.48; color: var(--muted); }
    .stat-figure strong { color: var(--ink-soft); font-weight: 700; }

    /* ---------- Registro de indicadores ---------- */
    /* El registro no es un muro uniforme: una banda de cabecera en cuerpo de
       display abre la decisión y una banda densa la sostiene. */
    .kpi-grid { margin-top: 16px; display: block; }
    .kpi-band { display: grid; gap: 22px 30px; }
    .kpi-band.lead { grid-template-columns: repeat(4, minmax(0, 1fr)); margin-top: 16px; }
    .kpi-band.support { grid-template-columns: repeat(5, minmax(0, 1fr)); margin-top: 26px; }
    .kpi-group-head {
      display: flex; justify-content: space-between; align-items: baseline;
      gap: 14px; padding: 12px 0 0;
    }
    .kpi-group-head .title { font-size: .7rem; text-transform: uppercase; letter-spacing: .16em; font-weight: 700; color: var(--ink); display: inline-flex; align-items: center; gap: 10px; }
    .kpi-group-head .title::before { content: ""; width: 22px; height: 2px; background: var(--accent); }
    .kpi-group-head .note { font-size: .74rem; color: var(--muted); line-height: 1.4; text-align: right; }
    .kpi {
      position: relative; background: transparent;
      border: 0; border-top: 1px solid var(--line);
      padding: 11px 0 0;
    }
    .kpi::before { content: ""; position: absolute; top: -1px; left: 0; width: 30px; height: 2px; background: var(--line-strong); }
    .kpi.official::before { background: var(--accent); }
    .kpi.warning::before { background: var(--amber); }
    .kpi.finance::before { background: var(--green); }
    .kpi.exploratory::before { background: var(--line-strong); }
    .kpi .eyebrow { font-size: .58rem; text-transform: uppercase; letter-spacing: .14em; color: var(--faint); font-weight: 700; margin-bottom: 7px; }
    .kpi .status { display: inline-flex; align-items: center; gap: 6px; font-size: .58rem; font-weight: 700; text-transform: uppercase; letter-spacing: .08em; margin-bottom: 6px; }
    .kpi .status::before { content: ""; width: 6px; height: 6px; border-radius: 50%; background: currentColor; }
    .kpi .status.bad { color: var(--red); }
    .kpi .status.watch { color: var(--amber); }
    .kpi .status.ok { color: var(--green); }
    .kpi .t { font-size: .75rem; color: var(--ink-soft); min-height: 30px; line-height: 1.3; font-weight: 600; }
    .kpi .v { margin-top: 5px; font-size: 1.55rem; font-weight: 700; letter-spacing: -0.02em; line-height: 1.02; color: var(--ink); }
    .kpi .d { font-size: .71rem; margin-top: 6px; color: var(--muted); line-height: 1.38; }
    .kpi .interpretation {
      margin-top: 9px; padding-top: 8px; border-top: 1px solid var(--line);
      font-size: .68rem; color: var(--faint); line-height: 1.42;
      display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden;
    }

    /* Cabecera de registro: cifra en cuerpo de display, como el resto de
       lecturas que abren decisión (tablero de situación, cifras en juego). */
    .kpi.lead { border-top-color: var(--line-strong); padding-top: 13px; }
    .kpi.lead::before { height: 3px; width: 46px; }
    .kpi.lead .t { font-size: .82rem; color: var(--ink); min-height: 34px; }
    .kpi.lead .v {
      margin-top: 8px; font-family: var(--font-display); font-size: 2.15rem; font-weight: 560;
      font-optical-sizing: auto; font-variation-settings: "opsz" 60, "wght" 560;
      letter-spacing: -0.015em; line-height: 1;
    }
    .kpi.lead .d { font-size: .74rem; margin-top: 8px; }

    /* Banda de apoyo: densa y sin repetir el rótulo que ya da el intertítulo. */
    .kpi.support .t { font-size: .72rem; min-height: 28px; }
    .kpi.support .v { font-size: 1.24rem; }
    .kpi.support .d { font-size: .68rem; }
    .kpi.support .interpretation { -webkit-line-clamp: 2; font-size: .66rem; }

    /* ---------- Señales ---------- */
    .alerts { margin-top: 40px; display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 22px 36px; }
    .alert {
      position: relative; padding: 2px 0 2px 16px;
      font-size: .78rem; line-height: 1.52; color: var(--ink-soft);
    }
    .alert::before { content: ""; position: absolute; left: 0; top: 2px; bottom: 2px; width: 3px; }
    .alert.red::before { background: var(--red); }
    .alert.amber::before { background: var(--amber); }
    .alert.green::before { background: var(--green); }

    /* ---------- Capítulos editoriales ---------- */
    .panel { background: transparent; border: 0; }
    .section {
      margin-top: 58px; padding: 22px 0 0; background: transparent;
      border: 0; border-top: 2px solid var(--rule);
    }
    .section h2 {
      margin: 0 0 8px; font-family: var(--font-display); font-size: 1.72rem; font-weight: 540;
      font-optical-sizing: auto; font-variation-settings: "opsz" 44, "wght" 540;
      letter-spacing: -0.012em; line-height: 1.14; color: var(--ink);
      display: flex; align-items: baseline; gap: 16px;
    }
    .section h2 .sec-idx {
      font-family: var(--font-display); font-size: 1.12rem; font-weight: 600; color: var(--accent-ink);
      flex: 0 0 auto; letter-spacing: 0;
    }
    .section .intro { margin: 0 0 26px 0; font-size: .9rem; color: var(--muted); line-height: 1.58; max-width: 84ch; }

    .grid2 { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 30px 36px; }
    .grid3 { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 30px 36px; }
    /* Capítulo con protagonista: el gráfico que sostiene la afirmación del
       titular ocupa la proporción mayor y respira más alto; el que lo acompaña
       queda subordinado. Evita que dieciocho gráficos pesen todos igual. */
    .grid-lede { display: grid; grid-template-columns: 1.62fr 1fr; gap: 30px 36px; }
    .grid-lede > .chart-card:first-child { border-top-color: var(--line-strong); }
    .grid-lede > .chart-card:first-child .chart-title { font-size: 1.06rem; letter-spacing: -0.012em; }
    .grid-lede > .chart-card:first-child canvas { min-height: 300px; max-height: 356px; }
    .grid2 > *, .grid3 > *, .grid-lede > * { min-width: 0; }

    .chart-card {
      background: transparent; border: 0; border-top: 1px solid var(--line);
      padding: 13px 0 0; min-width: 0; position: relative;
    }
    .chart-title { font-size: .94rem; color: var(--ink); margin: 0 0 5px 0; font-weight: 700; letter-spacing: -0.01em; line-height: 1.32; }
    .chart-sub { font-size: .76rem; color: var(--muted); margin: 0 0 10px 0; line-height: 1.46; }
    canvas {
      width: 100% !important; min-height: 238px; max-height: 300px;
      background: transparent; border: 0; padding: 6px 0 0;
    }

    /* ---------- Mapa de calor ---------- */
    .heatmap-wrap {
      border: 1px solid var(--line); border-radius: var(--radius-sm); overflow: auto;
      background: var(--surface); max-width: 100%; width: 100%;
    }
    .heatmap { width: 100%; min-width: 1120px; border-collapse: collapse; table-layout: fixed; font-size: .82rem; }
    .heatmap thead th { position: sticky; top: 0; z-index: 3; background: var(--surface); color: var(--ink); font-weight: 700; }
    .heatmap th, .heatmap td {
      border-bottom: 1px solid var(--line); border-right: 1px solid var(--line);
      padding: 8px; text-align: center; white-space: nowrap; line-height: 1.2;
    }
    .heatmap th:first-child { min-width: 128px; width: 128px; }
    .heatmap th:first-child, .heatmap td:first-child {
      text-align: left; position: sticky; left: 0; background: var(--surface);
      z-index: 4; font-weight: 700; color: var(--ink);
    }
    .heatmap th:not(:first-child), .heatmap td:not(:first-child) { min-width: 40px; width: 42px; }

    /* ---------- Síntesis ---------- */
    .insight-list { margin: 0; padding-left: 18px; font-size: .81rem; line-height: 1.52; color: var(--ink-soft); }
    .insight-list li { margin-bottom: 9px; }
    .insight-list li::marker { color: var(--accent-ink); }
    .bench-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px 26px; }
    .bench-card { border: 0; border-top: 1px solid var(--line); background: transparent; padding: 9px 0 0; font-size: .76rem; }
    .bench-card .k { color: var(--ink); font-weight: 700; display: block; }
    .bench-card .v { margin-top: 5px; font-weight: 700; font-size: 1.2rem; letter-spacing: -.01em; }
    .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 7px; vertical-align: middle; }
    .status-ok { background: var(--green); }
    .status-warn { background: var(--amber); }
    .status-bad { background: var(--red); }

    /* ---------- Tablas ---------- */
    .tbl-wrap { margin-top: 12px; border: 1px solid var(--line); border-radius: var(--radius-sm); overflow: auto; background: var(--surface); }
    table { width: 100%; border-collapse: collapse; font-size: .8rem; }
    th, td { border-bottom: 1px solid var(--line); padding: 10px 12px; text-align: left; white-space: nowrap; }
    td { color: var(--ink-soft); }
    th {
      position: sticky; top: 0; background: var(--surface); color: var(--faint); z-index: 2;
      font-size: .64rem; text-transform: uppercase; letter-spacing: .09em; font-weight: 700;
      border-bottom: 1px solid var(--line-strong);
    }
    th:not([data-key]) { cursor: default; }
    th[data-key] { cursor: pointer; padding-right: 22px; position: relative; }
    th[data-key]::after { content: "↕"; position: absolute; right: 8px; top: 50%; transform: translateY(-50%); opacity: .34; font-size: .92em; }
    th[data-key]:hover { color: var(--accent-ink); }
    th[data-key][aria-sort] { color: var(--accent-ink); }
    th[data-key][aria-sort]::after { content: "↑"; opacity: 1; color: var(--accent-ink); }
    th[data-key][aria-sort="descending"]::after { content: "↓"; }
    tbody tr:nth-child(even) { background: var(--surface-2); }
    tbody tr:hover { background: var(--accent-weak); }

    .badge { display: inline-block; border-radius: 999px; padding: 3px 9px; font-size: .67rem; font-weight: 700; letter-spacing: .02em; }
    .critico { background: var(--risk-critical-bg); color: var(--risk-critical-ink); }
    .alto { background: var(--risk-high-bg); color: var(--risk-high-ink); }
    .medio { background: var(--risk-watch-bg); color: var(--risk-watch-ink); }
    .bajo { background: var(--risk-ok-bg); color: var(--risk-ok-ink); }
    .zone-link {
      border: 0; background: transparent; color: var(--accent-ink); font-weight: 700;
      cursor: pointer; padding: 0; text-decoration: none; font-size: .79rem;
      border-bottom: 1px solid var(--accent-line);
    }
    .zone-link:hover { border-bottom-color: var(--accent-ink); }

    /* ---------- Simulador táctico ---------- */
    .whatif { margin-top: 34px; padding: 14px 0 0; border: 0; border-top: 1px solid var(--line-strong); background: transparent; }
    .whatif > b { font-size: .85rem; color: var(--ink); }
    .whatif-grid { margin-top: 14px; display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px 30px; }
    .whatif label { font-size: .76rem; color: var(--ink-soft); display: block; font-weight: 500; }
    .whatif label span { color: var(--accent-ink); font-weight: 700; }
    .whatif input[type='range'] { width: 100%; height: 4px; margin: 9px 0 2px; cursor: pointer; accent-color: var(--accent); }
    .whatif-result {
      margin-top: 18px; background: transparent; color: var(--ink-soft);
      border: 0; border-left: 3px solid var(--accent);
      padding: 4px 0 4px 16px; font-size: .81rem; line-height: 1.52;
    }

    /* ---------- Detalle territorial ---------- */
    .drill-card { position: relative; background: transparent; border: 0; border-top: 1px solid var(--line); padding: 13px 0 0; font-size: .79rem; color: var(--ink-soft); line-height: 1.46; min-height: 300px; }
    .drill-card h4 { margin: 0 0 10px 0; font-size: .94rem; color: var(--ink); font-weight: 700; }
    .drill-metric { display: grid; grid-template-columns: 1fr auto; gap: 6px 12px; margin-bottom: 12px; }
    .drill-metric .k { color: var(--muted); }
    .drill-metric .v { font-weight: 700; color: var(--ink); text-align: right; }
    .inline-meta { margin-top: 8px; font-size: .74rem; color: var(--muted); }
    .small-note { margin-top: 6px; font-size: .73rem; color: var(--muted); }
    .chart-fallback { margin-top: 8px; border: 1px dashed var(--line-strong); border-radius: 8px; padding: 9px; font-size: .78rem; color: var(--muted); background: var(--surface-2); }
    .sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0,0,0,0); white-space: nowrap; border: 0; }

    .table-tools { display: grid; grid-template-columns: 1fr auto auto; gap: 10px; align-items: center; margin-top: 4px; }
    .table-tools input {
      width: 100%; padding: 10px 13px; border: 1px solid var(--line); border-radius: 7px;
      background: var(--surface); color: var(--ink); font-family: var(--font-sans); outline: none;
      transition: border-color .16s ease, box-shadow .16s ease;
    }
    .table-tools input:focus { border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-weak); }
    .table-count { font-size: .74rem; color: var(--muted); text-align: right; white-space: nowrap; }

    /* ---------- Decisión ejecutiva (placa de tinta invertida) ---------- */
    .exec-decision {
      margin-top: 58px; color: var(--plate-soft);
      border-radius: var(--radius-lg); padding: 34px 38px;
      background: var(--plate-bg);
    }
    .exec-decision li + li { margin-top: 11px; }
    .exec-decision h3 {
      margin: 0 0 18px 0; font-family: var(--font-display); font-size: 1.5rem; font-weight: 540;
      font-optical-sizing: auto; font-variation-settings: "opsz" 60, "wght" 540;
      letter-spacing: -0.008em; color: var(--plate-ink); display: flex; align-items: baseline; gap: 14px;
    }
    .exec-decision h3 .sec-idx { font-family: var(--font-display); font-size: 1.05rem; font-weight: 600; color: var(--plate-accent); }
    .exec-decision ul { margin: 0; padding-left: 20px; font-size: .87rem; line-height: 1.55; }
    .exec-decision li::marker { color: var(--plate-accent); }

    /* ---------- Colofón metodológico ---------- */
    .method {
      margin-top: 34px; padding: 16px 0 0; border: 0; border-top: 1px solid var(--line-strong);
      background: transparent; font-size: .76rem;
      line-height: 1.55; color: var(--muted);
    }
    .method b { color: var(--ink-soft); }
    .gov-line { margin-top: 14px; padding-top: 13px; border-top: 1px solid var(--line); display: flex; flex-wrap: wrap; gap: 8px 24px; align-items: center; font-size: .72rem; }
    .gov-line .gk { text-transform: uppercase; letter-spacing: .1em; font-weight: 700; color: var(--faint); margin-right: 6px; }
    .gov-line .gv { color: var(--ink-soft); font-weight: 600; }
    .gov-line .gv.pass { color: var(--green); }

    .span2 { grid-column: 1 / -1; }
    #ch_riesgo_territorio { min-height: 286px; }

    /* ---------- Revelado de carga ---------- */
    @keyframes riseIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: none; } }
    .layout > .sidebar { animation: riseIn .6s cubic-bezier(.22,.61,.36,1) both; }
    .main > * { animation: riseIn .55s cubic-bezier(.22,.61,.36,1) both; animation-delay: .26s; }
    .main > *:nth-child(1) { animation-delay: .03s; }
    .main > *:nth-child(2) { animation-delay: .07s; }
    .main > *:nth-child(3) { animation-delay: .11s; }
    .main > *:nth-child(4) { animation-delay: .15s; }
    .main > *:nth-child(5) { animation-delay: .19s; }
    .main > *:nth-child(6) { animation-delay: .23s; }

    /* ---------- Responsive ---------- */
    @media (max-width: 1450px) {
      .kpi-band.lead { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .kpi-band.support { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .alerts { grid-template-columns: 1fr; gap: 16px; }
      .bench-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 1100px) {
      .layout { grid-template-columns: 1fr; padding: 0 20px; gap: 0; }
      .main { order: -1; padding-top: 26px; }
      .sidebar { position: relative; top: auto; max-height: none; order: 1; border-right: 0; border-top: 1px solid var(--line-strong); padding: 26px 0 34px; }
      .grid2, .grid3, .grid-lede, .whatif-grid { grid-template-columns: 1fr; }
      .grid-lede > .chart-card:first-child canvas { max-height: 300px; }
      /* Con el ancho reducido las pastillas de perímetro envuelven en varias
         filas: pegada ocuparía una quinta parte de la pantalla, así que aquí
         vuelve al flujo del documento. */
      .active-filters { position: static; }
      .section, .exec-decision { scroll-margin-top: 12px; }
      .decision-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .stakes-bar { grid-template-columns: 1fr; }
      .kpi-band.lead, .kpi-band.support { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .stat-figure .lead { min-width: 0; }
    }
    @media (max-width: 720px) {
      .decision-grid { grid-template-columns: 1fr; gap: 18px; }
      .kpi-band.lead, .kpi-band.support { grid-template-columns: 1fr; }
      .table-tools { grid-template-columns: 1fr; }
      .stat-figure { flex-direction: column; align-items: flex-start; gap: 8px; }
      .stat-figure p { padding-top: 0; }
      .hero-metrics { flex-direction: column; }
      .hm { width: 100%; border-right: 0; padding: 11px 0 3px; margin: 0; }
      .hm + .hm { border-top: 1px solid var(--line); }
      .board-label { flex-direction: column; gap: 5px; }
      .kpi-group-head { flex-direction: column; }
      .kpi-group-head .note { text-align: left; }
      .exec-decision { padding: 26px 22px; }
    }
    @media (prefers-reduced-motion: reduce) {
      *, *::before, *::after {
        animation-duration: 0.01ms !important; animation-iteration-count: 1 !important;
        transition-duration: 0.01ms !important; scroll-behavior: auto !important;
      }
      .btn:active { transform: none !important; }
    }
    @media print {
      .sidebar { display: none; }
      .layout { grid-template-columns: 1fr; padding: 0; max-width: none; }
      body { background: #fff !important; background-image: none !important; }
      .main { padding: 0; }
      .exec-decision { box-shadow: none; }
    }

    /* ============================================================
       Capa de acabado — materialidad de instrumento sobre el
       registro editorial. Profundidad contenida y afordances de
       interacción que respetan los filetes: nunca cajas genéricas.
       ============================================================ */
    :root {
      --wash-a: rgba(39, 67, 163, .05);
      --wash-b: rgba(143, 94, 10, .038);
      --hover-wash: rgba(39, 67, 163, .045);
      --elev-plate: 0 1px 1px rgba(20,21,24,.5), 0 24px 60px -28px rgba(20,21,24,.55), 0 8px 22px -18px rgba(20,21,24,.4);
      --elev-panel: 0 1px 2px rgba(27,28,30,.035), 0 14px 34px -22px rgba(27,28,30,.22);
    }
    body[data-theme="dark"] {
      --wash-a: rgba(148, 174, 242, .07);
      --wash-b: rgba(217, 168, 78, .04);
      --hover-wash: rgba(148, 174, 242, .06);
      --elev-plate: 0 1px 0 rgba(255,255,255,.04) inset, 0 26px 64px -26px rgba(0,0,0,.7), 0 10px 26px -18px rgba(0,0,0,.55);
      --elev-panel: 0 1px 2px rgba(0,0,0,.34), 0 16px 38px -24px rgba(0,0,0,.6);
    }

    /* La superficie deja de ser plana: dos lavados atmosféricos fijos. */
    body {
      background-image:
        radial-gradient(1180px 640px at 82% -10%, var(--wash-a), transparent 62%),
        radial-gradient(860px 520px at -8% 2%, var(--wash-b), transparent 58%);
      background-attachment: fixed;
      background-repeat: no-repeat;
    }

    /* Masthead: filete de acento que ancla el título como cabecera de parte. */
    .hero h1 {
      position: relative; padding-left: 22px;
    }
    .hero h1::before {
      content: ""; position: absolute; left: 0; top: .16em; bottom: .16em;
      width: 3px; background: var(--accent);
    }
    .hero-kicker::before { border-radius: 1px; }

    /* Lecturas de instrumento: el filete superior se activa al enfocar,
       con un lavado tenue que se revela sólo en hover — sin cajas. */
    .kpi, .decision-card, .stat-figure, .chart-card, .drill-card {
      transition: box-shadow .24s ease, background .24s ease;
      border-radius: var(--radius-sm);
    }
    .kpi::after, .decision-card::after, .stat-figure::after,
    .chart-card::after, .drill-card::after {
      content: ""; position: absolute; inset: -1px -12px -8px -12px; z-index: -1;
      border-radius: 9px; background: var(--hover-wash);
      opacity: 0; transition: opacity .24s ease; pointer-events: none;
    }
    .kpi, .decision-card, .stat-figure, .chart-card, .drill-card { isolation: isolate; }
    .kpi:hover::after, .decision-card:hover::after, .stat-figure:hover::after,
    .chart-card:hover::after, .drill-card:hover::after { opacity: 1; }

    /* El filete superior del KPI se ensancha al enfocar la lectura. */
    .kpi::before { transition: width .24s cubic-bezier(.22,.61,.36,1); }
    .kpi:hover::before { width: 100%; }

    /* Superficies ya cerradas ganan profundidad real. */
    .exec-decision {
      box-shadow: var(--elev-plate);
      background-image: radial-gradient(120% 100% at 0% 0%, rgba(169,189,245,.06), transparent 46%);
    }
    .heatmap-wrap, .tbl-wrap { box-shadow: var(--elev-panel); }

    /* Interacción de tabla más nítida: la fila enfocada eleva su tinta. */
    tbody tr { transition: background .16s ease; }
    tbody tr:hover td { color: var(--ink); }

    /* Perilla del simulador con halo de acento al arrastrar. */
    .whatif input[type='range']::-webkit-slider-thumb { transition: box-shadow .16s ease; }
    .whatif input[type='range']:active::-webkit-slider-thumb { box-shadow: 0 0 0 6px var(--accent-weak); }

    /* Chips de estado: punto vivo con leve resplandor cuando la señal está activa. */
    .status-chip.live .led { box-shadow: 0 0 0 3px rgba(34,107,65,.16); }
    .status-chip.warn .led { box-shadow: 0 0 0 3px rgba(143,94,10,.16); }

    @media (max-width: 720px) {
      .hero h1 { padding-left: 16px; }
    }
    @media (prefers-reduced-motion: reduce) {
      .kpi:hover::before { width: 30px; }
    }
  </style>
</head>
<body>
<a class="skip-link" href="#main_content">Saltar al contenido principal</a>
<div class="layout">
  <aside class="sidebar">
    <div class="brand">
      <div class="brand-mark" aria-hidden="true">
        <svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">
          <path d="M5 5 L12 12 L19 5 M5 19 L12 12 L19 19"/>
          <circle cx="5" cy="5" r="1.9" fill="currentColor" stroke="none"/>
          <circle cx="19" cy="5" r="1.9" fill="currentColor" stroke="none"/>
          <circle cx="12" cy="12" r="2.1" fill="currentColor" stroke="none"/>
          <circle cx="5" cy="19" r="1.9" fill="currentColor" stroke="none"/>
          <circle cx="19" cy="19" r="1.9" fill="currentColor" stroke="none"/>
        </svg>
      </div>
      <div>
        <div class="brand-name">Grid Intelligence</div>
        <div class="brand-sub">Red · Electrificación</div>
      </div>
    </div>
    <div class="sidebar-head">
      <div class="sidebar-eyebrow">Panel ejecutivo</div>
      <h2>Controles de decisión</h2>
      <div class="hint">Ajusta el perímetro de análisis para leer presión territorial, compensaciones operativas y secuencia de inversión.</div>
    </div>
    <div class="theme-switch">
      <button class="theme-btn" id="btn_theme" aria-label="Cambiar tema">Modo oscuro</button>
    </div>

    <div class="sidebar-section-label">Índice del parte</div>
    <nav class="doc-index" id="doc_index" aria-label="Índice del parte"></nav>

    <div class="sidebar-section-label">Perímetro</div>
    <label>Región operativa
      <select id="f_region"><option value="">Todas</option></select>
    </label>
    <label>Zona
      <select id="f_zona"><option value="">Todas</option></select>
    </label>
    <label>Subestación
      <select id="f_sub"><option value="">Todas</option></select>
    </label>
    <label>Tipo de zona
      <select id="f_tipo"><option value="">Todas</option></select>
    </label>
    <label>Tipo de activo dominante
      <select id="f_activo"><option value="">Todos</option></select>
    </label>
    <div class="sidebar-section-label">Riesgo y respuesta</div>
    <label>Nivel de riesgo
      <select id="f_risk">
        <option value="">Todos</option>
        <option value="critico">Crítico</option>
        <option value="alto">Alto</option>
        <option value="medio">Medio</option>
        <option value="bajo">Bajo</option>
      </select>
    </label>
    <label>Intervención recomendada
      <select id="f_intervencion"><option value="">Todas</option></select>
    </label>
    <div class="sidebar-section-label">Horizonte temporal</div>
    <label>Mes desde
      <select id="f_from"><option value="">Inicio</option></select>
    </label>
    <label>Mes hasta
      <select id="f_to"><option value="">Fin</option></select>
    </label>
    <div class="sidebar-section-label">Escenario</div>
    <label>Escenario simulado
      <select id="f_scenario"><option value="">Base (sin escenario)</option></select>
    </label>

    <div class="btn-row">
      <button class="btn primary" id="btn_focus_top">Foco crítico</button>
      <button class="btn ghost" id="btn_reset">Restablecer</button>
    </div>
    <div class="btn-row single" style="margin-top:8px;">
      <button class="btn secondary" id="btn_export">Exportar CSV</button>
    </div>
  </aside>

  <main class="main" id="main_content">
    <section class="hero">
      <div class="hero-statusline">
        <span class="hero-kicker">Grid Intelligence · Parte de situación de red</span>
        <div class="hero-status-chips">
          <span class="status-chip" id="status_validation"><span class="led"></span><span class="k">Validación</span> <span class="v">--</span></span>
          <span class="status-chip" id="status_release"><span class="led"></span><span class="k">Publicación</span> <span class="v">--</span></span>
          <span class="status-chip"><span class="k">Versión</span> <span class="v" id="status_version">--</span></span>
        </div>
      </div>
      <h1>Centro de decisión de red</h1>
      <div class="subtitle">
        Dónde reforzar, flexibilizar o monitorizar la red bajo presión de electrificación, y qué aprobar ahora frente a qué madurar como expediente antes de comprometer CAPEX.
      </div>
      <div class="hero-metrics">
        <div class="hm"><span class="v">__N_ZONAS__</span><span class="k">Zonas monitorizadas</span></div>
        <div class="hm"><span class="v">__N_FEEDERS__</span><span class="k">Alimentadores</span></div>
        <div class="hm"><span class="v">__N_SUBS__</span><span class="k">Subestaciones con señal</span></div>
        <div class="hm"><span class="v" style="font-size:1.08rem;">__COVERAGE_START__ <small>→</small> __COVERAGE_END__</span><span class="k">Ventana analítica</span></div>
      </div>
      <div class="hero-ridge" id="hero_ridge" aria-hidden="true"></div>
    </section>

    <section class="active-filters" aria-label="Perímetro activo">
      <span class="active-filters-kicker">Perímetro activo</span>
      <div class="filter-pills" id="filter_pills"></div>
    </section>

    <div class="board-label">
      <span class="t">Tablero de situación</span>
      <span class="n">Seis lecturas que responden la decisión completa; se recalculan con el perímetro activo.</span>
    </div>
    <section class="decision-grid" aria-label="Tablero de situación">
      <article class="decision-card">
        <div class="eyebrow"><span class="q">1.</span> Dónde actuar</div>
        <h3 id="decision_title">Perímetro de decisión inmediata</h3>
        <div class="metric" id="exec_action_focus">--</div>
        <p id="exec_action_text"></p>
      </article>
      <article class="decision-card">
        <div class="eyebrow"><span class="q">2.</span> Por qué</div>
        <h3>Factor que explica el riesgo</h3>
        <div class="metric" id="exec_bottleneck">--</div>
        <p id="exec_bottleneck_text"></p>
      </article>
      <article class="decision-card">
        <div class="eyebrow"><span class="q">3.</span> Cuándo</div>
        <h3>Urgencia y ventana de ejecución</h3>
        <div class="metric" id="exec_when">--</div>
        <p id="exec_when_text"></p>
      </article>
      <article class="decision-card">
        <div class="eyebrow"><span class="q">4.</span> Con qué intervención</div>
        <h3>Palanca dominante recomendada</h3>
        <div class="metric" id="exec_intervention">--</div>
        <p id="exec_intervention_text"></p>
      </article>
      <article class="decision-card">
        <div class="eyebrow"><span class="q">5.</span> Bajo qué incertidumbre</div>
        <h3>Confianza analítica de la señal</h3>
        <div class="metric" id="exec_confidence">--</div>
        <p id="exec_confidence_text"></p>
      </article>
      <article class="decision-card">
        <div class="eyebrow"><span class="q">6.</span> Estado de ejecución</div>
        <h3>Preparación para decisión y publicación</h3>
        <div class="metric" id="exec_status">--</div>
        <p id="exec_status_text"></p>
      </article>
    </section>

    <section class="stakes-bar" aria-label="Coste cuantificado de la situación">
      <div class="stat-figure">
        <div class="lead">
          <div class="eyebrow">Impacto operativo</div>
          <h3>Coste de no actuar</h3>
          <div class="v" id="exec_operational_impact">--</div>
        </div>
        <p id="exec_operational_impact_text"></p>
      </div>
      <div class="stat-figure">
        <div class="lead">
          <div class="eyebrow">Capital</div>
          <h3>CAPEX potencialmente diferible</h3>
          <div class="v" id="exec_capex_release">--</div>
        </div>
        <p id="exec_capex_release_text"></p>
      </div>
    </section>

    <div class="board-label">
      <span class="t">Indicadores del sistema</span>
      <span class="n">KPIs gobernados de lectura de comité y su lectura exploratoria bajo el filtro activo.</span>
    </div>
    <section class="kpi-grid" id="kpi_grid"></section>

    <section class="alerts">
      <div class="alert red" id="alert_critico"></div>
      <div class="alert amber" id="alert_tradeoff"></div>
      <div class="alert green" id="alert_diferible"></div>
    </section>

    <section class="section panel">
      <h2>Resumen ejecutivo y señales del perímetro</h2>
      <p class="intro">Síntesis para comité: señales que cambian la decisión, umbrales fuera de tolerancia y focos que requieren escalado. La lectura separa tres planos: riesgo operativo observado, capacidad de mitigación y disciplina de capital.</p>
      <div class="grid2">
        <div class="chart-card">
          <p class="chart-title">Hallazgos prioritarios del perímetro filtrado</p>
          <ul id="auto_insights" class="insight-list"></ul>
        </div>
        <div class="chart-card">
          <p class="chart-title">Comparativa contra umbrales de operación y resiliencia</p>
          <div id="bench_grid" class="bench-grid"></div>
        </div>
      </div>
    </section>

    <section class="section panel">
      <h2><span class="sec-idx">1.</span>Estado de red y congestión</h2>
      <p class="intro">Lectura operativa: dónde, cuándo y con qué persistencia se produce tensión de capacidad. La sección diferencia estrés precursor de congestión confirmada para decidir si basta una mitigación táctica o si el territorio debe entrar en estudio estructural.</p>
      <div class="grid-lede">
        <div class="chart-card">
          <p class="chart-title">La carga relativa supera el umbral deseable en meses de punta estacional</p>
          <p class="chart-sub">Línea de umbral 1.00 para identificar riesgo de sobrecarga sistemática.</p>
          <canvas id="ch_carga"></canvas>
        </div>
        <div class="chart-card">
          <p class="chart-title">La congestión no es homogénea: concentración en un subconjunto de zonas</p>
          <p class="chart-sub">Ranking territorial para priorizar foco operativo inmediato.</p>
          <canvas id="ch_congestion_zona"></canvas>
        </div>
      </div>
      <div class="grid2" style="margin-top:10px;">
        <div class="chart-card span2">
          <p class="chart-title">Mapa de calor horario de estrés por región operativa</p>
          <p class="chart-sub">Proxy combinado de carga relativa y ratio de congestión para detectar ventanas críticas.</p>
          <div class="heatmap-wrap" id="heatmap_container"></div>
        </div>
        <div class="chart-card span2">
          <p class="chart-title">Riesgo operativo vs criticidad territorial por zona</p>
          <p class="chart-sub">Detecta territorios donde la presión técnica coincide con impacto territorial alto.</p>
          <canvas id="ch_riesgo_territorio"></canvas>
        </div>
      </div>
    </section>

    <section class="section panel">
      <h2><span class="sec-idx">2.</span>Resiliencia y calidad de servicio</h2>
      <p class="intro">Lectura de continuidad: ENS, interrupciones y clientes afectados convierten el riesgo técnico en exposición de servicio. Una zona con menos horas de congestión puede subir en prioridad si el impacto por evento es material para clientes, regulación o reputación.</p>
      <div class="grid3">
        <div class="chart-card">
          <p class="chart-title">ENS concentrada en zonas con mayor estrés estructural</p>
          <canvas id="ch_ens"></canvas>
        </div>
        <div class="chart-card">
          <p class="chart-title">Interrupciones por zona: frecuencia e intensidad</p>
          <canvas id="ch_interruptions"></canvas>
        </div>
        <div class="chart-card">
          <p class="chart-title">Subestaciones líderes por exposición de servicio</p>
          <canvas id="ch_substations"></canvas>
        </div>
      </div>
    </section>

    <section class="section panel">
      <h2><span class="sec-idx">3.</span>Flexibilidad, almacenamiento y compensaciones operativas</h2>
      <p class="intro">El objetivo no es maximizar CAPEX, sino preservar opción. La decisión compara urgencia, coste, robustez, tiempo de despliegue y capacidad real de cubrir la hora crítica; por eso una solución flexible sólo es defendible si reduce el gap técnico donde y cuando aparece el estrés.</p>
      <div class="grid-lede">
        <div class="chart-card">
          <p class="chart-title">Brecha flexible vs ratio flexibilidad/estrés</p>
          <p class="chart-sub">Cuadrante superior izquierdo: presión alta y cobertura baja, prioridad para flexibilidad/almacenamiento.</p>
          <canvas id="ch_flex_gap"></canvas>
        </div>
        <div class="chart-card">
          <p class="chart-title">Comparador multicriterio: refuerzo vs flexibilidad vs almacenamiento vs operación</p>
          <p class="chart-sub">Puntuación de alternativa = impacto + coste (inverso) + tiempo (inverso) + robustez + urgencia.</p>
          <canvas id="ch_tradeoff"></canvas>
        </div>
      </div>
      <div class="grid2" style="margin-top:10px;">
        <div class="chart-card">
          <p class="chart-title">Soporte de almacenamiento en zonas de mayor riesgo</p>
          <canvas id="ch_storage"></canvas>
        </div>
        <div class="chart-card">
          <p class="chart-title">CAPEX refuerzo vs CAPEX diferible por flexibilidad</p>
          <canvas id="ch_capex_def"></canvas>
        </div>
      </div>
    </section>

    <section class="section panel">
      <h2><span class="sec-idx">4.</span>Electrificación, nueva demanda y vertido</h2>
      <p class="intro">Lectura de presión futura: vehículos eléctricos, electrificación industrial y vertido permiten anticipar saturación, pero no sustituyen la evidencia operativa actual. La decisión robusta aparece cuando nueva demanda, congestión y baja flexibilidad coinciden en la misma zona.</p>
      <div class="grid3">
        <div class="chart-card">
          <p class="chart-title">Impacto de vehículos eléctricos en zonas de presión alta</p>
          <canvas id="ch_ev"></canvas>
        </div>
        <div class="chart-card">
          <p class="chart-title">Impacto electrificación industrial</p>
          <canvas id="ch_ind"></canvas>
        </div>
        <div class="chart-card">
          <p class="chart-title">Vertido acumulado por mes</p>
          <canvas id="ch_curt"></canvas>
        </div>
      </div>
    </section>

    <section class="section panel">
      <h2><span class="sec-idx">5.</span>Priorización de inversiones y acción operativa</h2>
      <p class="intro">La priorización debe ser defendible ante ingeniería, finanzas y operación: puntuación total, factor principal, urgencia, alternativa recomendada y trazabilidad a subestación/alimentador. El ranking no autoriza inversión; ordena expedientes y define qué evidencia falta para decidir.</p>
      <div class="grid3">
        <div class="chart-card">
          <p class="chart-title">Ranking de zonas por prioridad de intervención</p>
          <canvas id="ch_priority"></canvas>
        </div>
        <div class="chart-card">
          <p class="chart-title">Riesgo técnico vs prioridad económica</p>
          <canvas id="ch_risk_econ"></canvas>
        </div>
        <div class="chart-card">
          <p class="chart-title">Alimentadores líderes por criticidad compuesta</p>
          <canvas id="ch_feeders"></canvas>
        </div>
      </div>
    </section>

    <section class="section panel">
      <h2><span class="sec-idx">6.</span>Escenarios y simulación táctica</h2>
      <p class="intro">Comparación de escenarios para cuantificar exposición relativa, coste de no actuar y beneficio de combinar CAPEX, flexibilidad y almacenamiento. Los escenarios son pruebas de robustez, no probabilidades; sirven para diseñar secuencia y condiciones de escalada.</p>
      <div class="grid2">
        <div class="chart-card">
          <p class="chart-title">Escenario base vs alternativos: coste de riesgo e inversión requerida</p>
          <canvas id="ch_scenarios"></canvas>
        </div>
        <div class="chart-card">
          <p class="chart-title">Zonas que más empeoran bajo el escenario seleccionado</p>
          <div class="tbl-wrap" style="max-height:302px;" id="scenario_top_table"></div>
        </div>
      </div>
      <div class="whatif">
        <b>Simulador táctico rápido (zona filtrada):</b>
        <div class="whatif-grid">
          <label>Crecimiento de vehículos eléctricos (%)
            <input type="range" id="wf_ev" min="-20" max="80" value="10" step="1" />
            <span id="wf_ev_v">10%</span>
          </label>
          <label>Electrificación industrial (%)
            <input type="range" id="wf_ind" min="-20" max="80" value="10" step="1" />
            <span id="wf_ind_v">10%</span>
          </label>
          <label>Activación flexibilidad (%)
            <input type="range" id="wf_flex" min="0" max="60" value="20" step="1" />
            <span id="wf_flex_v">20%</span>
          </label>
          <label>Despliegue de almacenamiento (%)
            <input type="range" id="wf_storage" min="0" max="60" value="15" step="1" />
            <span id="wf_storage_v">15%</span>
          </label>
        </div>
        <div class="whatif-result" id="whatif_result"></div>
      </div>
    </section>

    <section class="section panel">
      <h2><span class="sec-idx">7.</span>Tabla accionable de priorización</h2>
      <p class="intro">Ruta completa dato → insight → acción. La tabla permite filtrar, ordenar y justificar intervención por territorio, manteniendo visible si la recomendación se basa en congestión, servicio, flexibilidad, activos, electrificación o economía relativa.</p>
      <div class="table-tools">
        <input id="searchBox" placeholder="Buscar por zona, factor o intervención" />
        <button class="btn secondary" id="btn_export_table" style="padding:9px 12px;">Exportar filtro</button>
        <div id="table_count" class="table-count">0 filas</div>
      </div>
      <div class="tbl-wrap" style="max-height:420px;">
        <table id="priority_table">
          <thead>
            <tr>
              <th data-key="zona_id">Zona</th>
              <th data-key="investment_priority_score">Puntuación</th>
              <th data-key="risk_tier">Nivel</th>
              <th data-key="urgency_tier">Urgencia</th>
              <th data-key="main_risk_driver">Factor</th>
              <th data-key="recommended_intervention">Intervención</th>
              <th data-key="recommended_sequence">Secuencia</th>
              <th data-key="capex_total">CAPEX asociado</th>
              <th>Justificación ejecutiva</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section class="section panel">
      <h2><span class="sec-idx">8.</span>Plan de acción por horizonte y drill-down territorial</h2>
      <p class="intro">Convierte priorización en secuencia temporal ejecutable y permite analizar una zona de referencia con sus alternativas de intervención. Cada horizonte debe tener responsable, línea base, criterio de éxito y umbral de escalada.</p>
      <div class="grid2">
        <div class="chart-card">
          <p class="chart-title">Cartera de intervención por secuencia recomendada</p>
          <p class="chart-sub">Distribución 0-3m, 0-6m, 3-12m, 6-24m y revisión trimestral.</p>
          <canvas id="ch_horizon"></canvas>
        </div>
        <div class="drill-card" id="drill_zone_panel"></div>
      </div>
    </section>

    <section class="exec-decision">
      <h3><span class="sec-idx">9.</span>Decisión ejecutiva final</h3>
      <ul id="decision_list"></ul>
    </section>

    <section class="method">
      <b>Notas metodológicas y límites</b><br>
      - Puntuaciones y costes son aproximaciones comparativas para priorización relativa; no son presupuesto regulatorio, caso financiero definitivo ni autorización de CAPEX.<br>
      - El simulador táctico orienta sensibilidad direccional; para ingeniería de detalle se requiere flujo de carga, N-1, protecciones, permisos, costes reales y restricciones de ejecución.<br>
      - Los datos son sintéticos calibrados para plausibilidad operacional. El uso correcto es soporte a decisión, diseño de gobernanza analítica y prototipado avanzado antes de conectar fuentes operativas gobernadas.
      <div class="gov-line" id="gov_footer"></div>
    </section>
  </main>
</div>

<script>
const DATA = __PAYLOAD__;
const KPI_STATIC = __KPI_STATIC__;
const EXEC_INSIGHTS = __EXEC_INSIGHTS__;
const GOVERNANCE = __GOVERNANCE__;

const CHARTS = {};
const TABLE_STATE = { sortKey: "investment_priority_score", sortDir: -1 };

function byId(id) { return document.getElementById(id); }
function num(v) { const x = Number(v); return Number.isFinite(x) ? x : 0; }
function fmt(v, d = 0) { return num(v).toLocaleString('es-ES', { maximumFractionDigits: d, minimumFractionDigits: d }); }
function uniq(arr) { return Array.from(new Set(arr)).filter(v => v !== undefined && v !== null && v !== ""); }
function esc(v) {
  return String(v ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
function humanize(v) {
  return String(v ?? "N/A").replace(/_/g, " ");
}
function riskLabel(v) {
  const map = { critico: "Crítico", alto: "Alto", medio: "Medio", bajo: "Bajo" };
  return map[String(v || "").toLowerCase()] || humanize(v);
}
function driverLabel(v) {
  const map = {
    congestion_risk_score: "congestión estructural",
    flexibility_gap_score: "brecha de flexibilidad",
    asset_exposure_score: "exposición de activos",
    electrification_pressure_score: "presión de electrificación",
    service_impact_score: "impacto de servicio",
    economic_priority_score: "prioridad económica",
    sin_driver: "sin factor dominante",
  };
  return map[String(v || "")] || humanize(v);
}
function interventionLabel(v) {
  const map = {
    intervencion_inmediata_prioritaria: "intervención inmediata prioritaria",
    reforzar_red_local: "refuerzo local de red",
    desplegar_almacenamiento: "despliegue de almacenamiento",
    activar_flexibilidad: "activación de flexibilidad",
    optimizar_operacion: "optimización operativa",
    sustituir_activos: "sustitución de activos",
    monitorizar: "monitorización reforzada",
  };
  return map[String(v || "")] || humanize(v);
}
function urgencyLabel(v) {
  const map = { inmediata: "Inmediata", alta: "Alta", planificada: "Planificada", monitorizacion: "Monitorización" };
  return map[String(v || "").toLowerCase()] || humanize(v);
}
function seqLabel(v) {
  const map = {
    "0-3m": "0-3 meses", "0-6m": "0-6 meses", "0-12m": "0-12 meses",
    "3-12m": "3-12 meses", "6-24m": "6-24 meses", revision_trimestral: "Revisión trimestral",
  };
  return map[String(v || "")] || humanize(v);
}
function scenarioLabel(v) {
  const map = {
    retraso_capex: "Retraso de CAPEX",
    evento_degradacion_activos: "Degradación de activos",
    electrificacion_industrial_intensiva: "Electrificación industrial intensiva",
    crecimiento_acelerado_ev: "Crecimiento acelerado de VE",
    mayor_penetracion_gd: "Mayor penetración de GD",
    despliegue_adicional_storage: "Almacenamiento adicional",
    despliegue_adicional_flexibilidad: "Flexibilidad adicional",
    capex_mas_flexibilidad: "CAPEX + flexibilidad",
  };
  return map[String(v || "")] || humanize(v);
}
function fmtEurCompact(v) {
  const n = num(v);
  if (Math.abs(n) >= 1e6) return `${fmt(n / 1e6, 1)} M€`;
  if (Math.abs(n) >= 1e3) return `${fmt(n / 1e3, 0)} k€`;
  return `${fmt(n, 0)} €`;
}
function share(n, d, decimals = 0) {
  return d ? `${fmt(100 * n / d, decimals)}%` : "0%";
}
function fmtAxisTick(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return v;
  const a = Math.abs(n);
  if (a >= 1e6) return `${(n / 1e6).toLocaleString('es-ES', { maximumFractionDigits: 1 })} M`;
  if (a >= 1e3) return `${(n / 1e3).toLocaleString('es-ES', { maximumFractionDigits: 0 })} k`;
  return n.toLocaleString('es-ES', { maximumFractionDigits: 2 });
}

function getTheme() {
  return document.body.getAttribute("data-theme") || "light";
}

function setTheme(theme) {
  const safeTheme = theme === "dark" ? "dark" : "light";
  document.body.setAttribute("data-theme", safeTheme);
  const btn = byId("btn_theme");
  if (btn) btn.textContent = safeTheme === "dark" ? "Modo claro" : "Modo oscuro";
  try { localStorage.setItem("dashboard_theme", safeTheme); } catch (_) {}
}

/* Modo oscuro por defecto en la primera carga: el parte de situación se lee
   como instrumento de sala de control. Sólo una elección explícita del usuario
   —persistida— anula este valor; la preferencia del sistema no lo hace. */
function initTheme() {
  let preferred = "dark";
  try {
    const saved = localStorage.getItem("dashboard_theme");
    if (saved === "dark" || saved === "light") preferred = saved;
  } catch (_) {}
  setTheme(preferred);
}

function chartTheme() {
  const styles = getComputedStyle(document.body);
  const dark = getTheme() === "dark";
  return {
    tick: styles.getPropertyValue("--chart-tick").trim() || "#334155",
    grid: styles.getPropertyValue("--chart-grid").trim() || "rgba(148, 163, 184, .20)",
    label: styles.getPropertyValue("--ink").trim() || "#0f172a",
    surface: styles.getPropertyValue("--surface").trim() || "#ffffff",
    tooltipBg: dark ? "rgba(24, 24, 27, .97)" : "rgba(250, 248, 241, .97)",
    tooltipBorder: styles.getPropertyValue("--line-strong").trim() || "#b5ae9c",
    // Paleta de datos contenida: un único acento ultramar para series, un neutro
    // de tinta para comparación, y rojo reservado a riesgo/umbral. Sensible al tema.
    accent: dark ? "rgba(148, 174, 242, .92)" : "rgba(39, 67, 163, .82)",
    accentSoft: dark ? "rgba(148, 174, 242, .18)" : "rgba(39, 67, 163, .11)",
    neutral: dark ? "rgba(163, 161, 153, .48)" : "rgba(27, 28, 30, .32)",
    risk: dark ? "rgba(238, 131, 120, .80)" : "rgba(169, 44, 33, .72)",
  };
}

function destroyChart(id) {
  if (CHARTS[id]) {
    CHARTS[id].destroy();
    delete CHARTS[id];
  }
}

function paintRiskBadge(v) {
  const key = String(v || "").toLowerCase();
  if (["critico", "alto", "medio", "bajo"].includes(key)) {
    return `<span class="badge ${key}">${esc(riskLabel(key))}</span>`;
  }
  return esc(v || "");
}

function getZoneMap() {
  const map = {};
  (DATA.zoneRisk || []).forEach(z => { map[z.zona_id] = z; });
  return map;
}

function getScoringMap() {
  const map = {};
  (DATA.scoring || []).forEach(s => { map[s.zona_id] = s; });
  return map;
}

function initFilters() {
  const zoneRisk = DATA.zoneRisk || [];
  const scoring = DATA.scoring || [];
  const subs = DATA.substations || [];
  const monthly = DATA.monthly || [];

  function addOptions(id, values, labelFn) {
    const el = byId(id);
    values.forEach(v => {
      const opt = document.createElement("option");
      opt.value = v;
      opt.textContent = labelFn ? labelFn(v) : v;
      el.appendChild(opt);
    });
  }

  addOptions("f_region", uniq(zoneRisk.map(z => z.region_operativa)).sort());
  addOptions("f_zona", uniq(zoneRisk.map(z => z.zona_id)).sort());
  addOptions("f_sub", uniq(subs.map(s => s.subestacion_id)).sort());
  addOptions("f_tipo", uniq(zoneRisk.map(z => z.tipo_zona)).sort(), humanize);
  addOptions("f_activo", uniq((DATA.assetTypes || [])).sort(), humanize);
  addOptions("f_intervencion", uniq(scoring.map(s => s.recommended_intervention)).sort(), interventionLabel);
  addOptions("f_scenario", uniq((DATA.scenarioSummary || []).map(s => s.scenario)).sort(), scenarioLabel);

  const months = uniq(monthly.map(m => m.mes)).sort();
  addOptions("f_from", months);
  addOptions("f_to", months);
  if (months.length > 0) {
    byId("f_from").value = months[0];
    byId("f_to").value = months[months.length - 1];
  }
}

function readFilters() {
  return {
    region: byId("f_region").value,
    zona: byId("f_zona").value,
    sub: byId("f_sub").value,
    tipo: byId("f_tipo").value,
    activo: byId("f_activo").value,
    risk: byId("f_risk").value,
    intervencion: byId("f_intervencion").value,
    from: byId("f_from").value,
    to: byId("f_to").value,
    scenario: byId("f_scenario").value,
  };
}

function zoneIdsBySubstation(subId) {
  if (!subId) return [];
  return uniq((DATA.substations || []).filter(s => s.subestacion_id === subId).map(s => s.zona_id));
}

function monthInRange(month, from, to) {
  if (!from && !to) return true;
  if (from && month < from) return false;
  if (to && month > to) return false;
  return true;
}

function getFilteredZoneIds(filters) {
  const zoneMap = getZoneMap();
  const scoringMap = getScoringMap();

  let ids = uniq((DATA.zoneRisk || []).map(z => z.zona_id));

  if (filters.zona) {
    ids = ids.filter(z => z === filters.zona);
  }

  if (filters.sub) {
    const allowed = zoneIdsBySubstation(filters.sub);
    ids = ids.filter(z => allowed.includes(z));
  }

  if (filters.region) {
    ids = ids.filter(z => (zoneMap[z] || {}).region_operativa === filters.region);
  }

  if (filters.tipo) {
    ids = ids.filter(z => (zoneMap[z] || {}).tipo_zona === filters.tipo);
  }

  if (filters.risk) {
    ids = ids.filter(z => String((scoringMap[z] || {}).risk_tier || "") === filters.risk);
  }

  if (filters.intervencion) {
    ids = ids.filter(z => String((scoringMap[z] || {}).recommended_intervention || "") === filters.intervencion);
  }

  if (filters.activo) {
    const feederZones = uniq((DATA.feeders || [])
      .filter(f => String(f.tipo_activo_dominante || "") === filters.activo)
      .map(f => f.zona_id));
    ids = ids.filter(z => feederZones.includes(z));
  }

  return ids;
}

function getFilteredData() {
  const filters = readFilters();
  const zoneIds = getFilteredZoneIds(filters);

  const zoneRisk = (DATA.zoneRisk || []).filter(z => zoneIds.includes(z.zona_id));
  const zoneProfile = (DATA.zoneProfile || []).filter(z => zoneIds.includes(z.zona_id));
  const scoring = (DATA.scoring || []).filter(z => zoneIds.includes(z.zona_id));
  const substations = (DATA.substations || []).filter(s => zoneIds.includes(s.zona_id));
  const feeders = (DATA.feeders || []).filter(f => zoneIds.includes(f.zona_id));
  const flexGap = (DATA.flexGap || []).filter(f => zoneIds.includes(f.zona_id));
  const interruptions = (DATA.interruptions || []).filter(i => zoneIds.includes(i.zona_id));
  const capexDef = (DATA.capexDef || []).filter(c => zoneIds.includes(c.zona_id));
  const electrification = (DATA.electrification || []).filter(e => zoneIds.includes(e.zona_id));
  const forecastPressure = (DATA.forecastPressure || []).filter(f => zoneIds.includes(f.zona_id));
  const optionsByZone = (DATA.optionsByZone || []).filter(o => zoneIds.includes(o.zona_id));

  const monthly = (DATA.monthly || []).filter(m => monthInRange(m.mes, filters.from, filters.to));

  const regionSet = uniq(zoneRisk.map(z => z.region_operativa));
  const regionHour = (DATA.regionHour || []).filter(r => regionSet.includes(r.region_operativa));

  let scenarioSummary = DATA.scenarioSummary || [];
  if (filters.scenario) {
    const selected = scenarioSummary.filter(s => s.scenario === filters.scenario);
    scenarioSummary = selected.length ? selected : scenarioSummary;
  }

  let scenarioTop = DATA.scenarioTopZones || [];
  if (filters.scenario) {
    scenarioTop = scenarioTop.filter(s => s.scenario === filters.scenario && zoneIds.includes(s.zona_id));
  } else {
    scenarioTop = scenarioTop.filter(s => zoneIds.includes(s.zona_id));
  }

  return {
    filters,
    zoneIds,
    zoneRisk,
    zoneProfile,
    scoring,
    substations,
    feeders,
    flexGap,
    interruptions,
    capexDef,
    electrification,
    forecastPressure,
    optionsByZone,
    monthly,
    regionHour,
    scenarioSummary,
    scenarioTop,
  };
}

function updateTopDecision(fd) {
  const capFirst = s => { s = String(s || ""); return s ? s.charAt(0).toUpperCase() + s.slice(1) : "--"; };
  const zonas = n => n === 1 ? "zona" : "zonas";
  const top = [...fd.scoring].sort((a,b) => num(b.investment_priority_score) - num(a.investment_priority_score));
  const topZones = top.slice(0, 3);
  const topIds = topZones.map(z => z.zona_id).join(", ") || "N/A";
  const critical = fd.scoring.filter(r => ["critico", "alto"].includes(String(r.risk_tier || "").toLowerCase()));
  const immediate = fd.scoring.filter(r => ["inmediata", "alta"].includes(String(r.urgency_tier || "").toLowerCase()));

  const driverCount = {};
  fd.scoring.forEach(s => {
    const key = String(s.main_risk_driver || "sin_driver");
    driverCount[key] = (driverCount[key] || 0) + 1;
  });
  const topDriver = Object.entries(driverCount).sort((a,b) => b[1] - a[1])[0] || ["Sin señal", 0];

  const costRisk = fd.scoring.reduce((s,r) => s + num(r.coste_riesgo_proxy), 0);
  const ens = fd.zoneRisk.reduce((s,z) => s + num(z.ens_total_mwh), 0);
  const capex = fd.scoring.reduce((s,r) => s + num(r.capex_total), 0);
  const capexDif = fd.capexDef.reduce((s,c) => s + num(c.capex_diferible_proxy_eur), 0);
  const capexDifPct = capex ? 100 * capexDif / capex : 0;

  const firstAction = top[0]?.recommended_intervention || "monitorizar";
  const topScore = top[0] ? num(top[0].investment_priority_score) : 0;
  byId("exec_action_focus").textContent = topIds;
  byId("exec_action_text").innerHTML =
    `<strong>${fmt(critical.length,0)} ${zonas(critical.length)} en nivel alto o crítico</strong> y ${fmt(immediate.length,0)} con urgencia inmediata o alta. La primera decisión debe centrarse en ${esc(topIds)}, con puntuación líder <strong>${fmt(topScore,1)}</strong> y acción dominante de <strong>${esc(interventionLabel(firstAction))}</strong>.`;

  byId("exec_bottleneck").textContent = capFirst(driverLabel(topDriver[0]));
  byId("exec_bottleneck_text").innerHTML =
    `Explica ${fmt(topDriver[1],0)} ${zonas(topDriver[1])} del perímetro (${share(topDriver[1], fd.scoring.length, 0)}). Validar si el factor exige refuerzo estructural, medida reversible o sólo monitorización con umbral de escalada.`;

  byId("exec_operational_impact").textContent = `${fmt(ens,1)} MWh`;
  byId("exec_operational_impact_text").innerHTML =
    `ENS filtrada con coste de riesgo aproximado de <strong>${fmt(costRisk,0)} EUR</strong>. Usar para ordenar respuesta semanal y exigir beneficio esperado por expediente.`;

  byId("exec_capex_release").textContent = `${fmt(capexDifPct,1)}%`;
  byId("exec_capex_release_text").innerHTML =
    `CAPEX potencialmente diferible: <strong>${fmt(capexDif,0)} EUR</strong>. Diferir sólo donde nivel, pronóstico, cobertura flexible y señal de servicio permiten preservar opción sin aumentar riesgo residual.`;

  // 03 · Cuándo — urgencia de la señal y ventana de ejecución de la zona líder.
  const leadRow = top[0];
  const leadUrgency = leadRow ? urgencyLabel(leadRow.urgency_tier) : "N/A";
  const leadSeq = leadRow ? seqLabel(leadRow.recommended_sequence) : "N/A";
  const plannedMonitor = fd.scoring.filter(r => ["planificada", "monitorizacion"].includes(String(r.urgency_tier || "").toLowerCase())).length;
  byId("exec_when").textContent = leadRow ? `${leadUrgency} · ${leadSeq}` : "--";
  byId("exec_when_text").innerHTML =
    `<strong>${fmt(immediate.length,0)} ${zonas(immediate.length)}</strong> de urgencia inmediata o alta frente a <strong>${fmt(plannedMonitor,0)}</strong> en horizonte planificado o de monitorización. La ventana marca el arranque del expediente, no la fecha de obra.`;

  // 04 · Con qué intervención — palanca dominante del perímetro filtrado.
  const intCount = {};
  fd.scoring.forEach(s => {
    const key = String(s.recommended_intervention || "monitorizar");
    intCount[key] = (intCount[key] || 0) + 1;
  });
  const intSorted = Object.entries(intCount).sort((a,b) => b[1] - a[1]);
  const topInt = intSorted[0] || ["monitorizar", 0];
  const leadInt = leadRow ? String(leadRow.recommended_intervention || topInt[0]) : topInt[0];
  byId("exec_intervention").textContent = capFirst(interventionLabel(leadInt));
  byId("exec_intervention_text").innerHTML =
    `Palanca más frecuente en el perímetro: <strong>${esc(interventionLabel(topInt[0]))}</strong>, con ${fmt(topInt[1],0)} ${zonas(topInt[1])} (${share(topInt[1], fd.scoring.length, 0)}). Cada expediente debe demostrar que la palanca ataca el factor principal, no el síntoma.`;

  // 05 · Bajo qué incertidumbre — confianza de la señal analítica.
  const confCount = {};
  fd.scoring.forEach(s => {
    const key = String(s.confidence_flag || "sin_flag");
    confCount[key] = (confCount[key] || 0) + 1;
  });
  const highConf = confCount["alta_confianza"] || 0;
  const govConf = (typeof GOVERNANCE === "object" && GOVERNANCE) ? String(GOVERNANCE.validation_confidence || "N/A") : "N/A";
  byId("exec_confidence").textContent = capFirst(govConf);
  byId("exec_confidence_text").innerHTML =
    `<strong>${fmt(highConf,0)} de ${fmt(fd.scoring.length,0)}</strong> ${zonas(fd.scoring.length)} presentan señal de alta confianza y la validación global es <strong>${esc(govConf)}</strong>. Los datos son sintéticos calibrados: la lectura sirve como soporte de decisión, no como presupuesto.`;

  // 06 · Estado de ejecución — preparación de decisión y publicación.
  const relMap = {
    "publish-ready": "Publicable",
    "publish-with-caveats": "Publicable con matices",
    "publish-blocked": "Publicación bloqueada",
  };
  const decMap = {
    "decision-support ready": "Apto como soporte de decisión",
    "decision-support only": "Solo soporte de decisión",
    "screening-grade only": "Solo cribado preliminar",
  };
  const g = (typeof GOVERNANCE === "object" && GOVERNANCE) ? GOVERNANCE : {};
  const pub = String(g.publish_state || "N/A");
  const dec = String(g.decision_state || "N/A");
  const valStatus = String(g.validation_status || "N/A");
  byId("exec_status").textContent = relMap[pub] || humanize(pub);
  byId("exec_status_text").innerHTML =
    `Validación analítica <strong>${esc(valStatus)}</strong> · ${esc(decMap[dec] || humanize(dec))}. Antes de comprometer CAPEX se requiere ingeniería de detalle, permisos y fuentes operativas gobernadas.`;
}

function buildFilterChip(label, value, muted = false) {
  return `<span class="filter-chip ${muted ? "muted" : ""}"><span class="k">${esc(label)}</span>${esc(value)}</span>`;
}

function updateFilterSummary(fd) {
  const filters = fd.filters || readFilters();
  const months = uniq((DATA.monthly || []).map(m => m.mes)).sort();
  const defaultFrom = months[0] || "";
  const defaultTo = months[months.length - 1] || "";
  const fullWindow = filters.from === defaultFrom && filters.to === defaultTo;
  const chips = [];

  if (filters.region) chips.push(buildFilterChip("Región", filters.region));
  if (filters.zona) chips.push(buildFilterChip("Zona", filters.zona));
  if (filters.sub) chips.push(buildFilterChip("Subestación", filters.sub));
  if (filters.tipo) chips.push(buildFilterChip("Tipo", humanize(filters.tipo)));
  if (filters.activo) chips.push(buildFilterChip("Activo", humanize(filters.activo)));
  if (filters.risk) chips.push(buildFilterChip("Riesgo", riskLabel(filters.risk)));
  if (filters.intervencion) chips.push(buildFilterChip("Intervención", interventionLabel(filters.intervencion)));
  if (filters.scenario) chips.push(buildFilterChip("Escenario", scenarioLabel(filters.scenario)));

  chips.push(buildFilterChip("Ventana", `${filters.from || defaultFrom || "inicio"} → ${filters.to || defaultTo || "fin"}`, fullWindow));

  if (!chips.length || (chips.length === 1 && fullWindow)) {
    chips.unshift(buildFilterChip("Cobertura", "Sistema completo", true));
  }

  chips.push(buildFilterChip("Zonas activas", fmt(fd.zoneIds.length, 0)));
  chips.push(buildFilterChip("Subestaciones", fmt(fd.substations.length, 0), fd.substations.length === 0));
  chips.push(buildFilterChip("Escala de decisión", fd.zoneIds.length <= 3 ? "Foco puntual" : fd.zoneIds.length <= 10 ? "Territorio priorizado" : "Vista ampliada"));

  byId("filter_pills").innerHTML = chips.join("");
}

function buildKpiGroupHead(title, note) {
  return `<div class="kpi-group-head"><div class="title">${title}</div><div class="note">${note}</div></div>`;
}

/* El rótulo dice de qué naturaleza es la cifra; el estado, en qué situación de
   gobierno está. Son ejes distintos, así que no comparten vocabulario: "Gobernado"
   pertenece sólo al estado (kpiStatus). Cuando el rótulo lo reutilizaba, una misma
   tarjeta podía leerse "Gobernado" y "Revisar" a la vez. */
function classifyKpi(title, detail) {
  const text = `${title} ${detail}`.toLowerCase();
  if (text.includes("aproximación económica") || text.includes("proxy económico") || title.toLowerCase().includes("capex") || title.toLowerCase().includes("coste")) {
    return { cls: "finance", eyebrow: "Impacto económico" };
  }
  if (text.includes("exploratorio") || text.includes("filtro activo") || title.toLowerCase().includes("filtrado") || title.toLowerCase().includes("filtrada")) {
    return { cls: "exploratory", eyebrow: "Filtro activo" };
  }
  if (text.includes("críticas") || text.includes("diferible") || text.includes("excesiva")) {
    return { cls: "warning", eyebrow: "Señal clave" };
  }
  return { cls: "official", eyebrow: "KPI oficial" };
}

function kpiStatus(text) {
  const t = String(text || "").toLowerCase();
  if (t.includes("crítico") || t.includes("critico") || t.includes(">1.0") || t.includes("no sustituye")) return { cls: "bad", txt: "Revisar" };
  if (t.includes("proxy") || t.includes("aproximación") || t.includes("exploratorio") || t.includes("diferible") || t.includes("pronóstico")) return { cls: "watch", txt: "Condicionado" };
  return { cls: "ok", txt: "Gobernado" };
}

/* Dos registros de lectura. `lead` son las cifras que abren la decisión y se
   componen en cuerpo de display; `support` sostienen el argumento y se componen
   densas. El intertítulo del grupo ya nombra la naturaleza del bloque, así que
   la etiqueta por tarjeta sólo se imprime cuando aporta algo distinto. */
function buildKpiCard(title, value, detail, interpretation, variant = "support") {
  const meta = classifyKpi(title, detail);
  const st = kpiStatus(`${title} ${detail} ${interpretation}`);
  const eyebrow = variant === "lead" ? `<div class="eyebrow">${meta.eyebrow}</div>` : "";
  return `
    <article class="kpi ${meta.cls} ${variant}">
      ${eyebrow}
      <div class="status ${st.cls}">${st.txt}</div>
      <div class="t">${esc(title)}</div>
      <div class="v">${esc(value)}</div>
      <div class="d">${esc(detail)}</div>
      <div class="interpretation">${esc(interpretation || "Interpretación: usar como señal contextual, no como decisión aislada.")}</div>
    </article>
  `;
}

function updateKpis(fd) {
  // Abren la decisión: cuánta tensión hay, dónde duele, qué cuesta no actuar y
  // cuánto capital admite re-secuencia.
  const officialLead = [
    buildKpiCard("Horas de congestión", fmt(KPI_STATIC.horas_congestion,0), "Acumulado del sistema en la ventana analítica", "Más horas elevan la urgencia de operación avanzada y refuerzo localizado.", "lead"),
    buildKpiCard("Zonas críticas", fmt(KPI_STATIC.zonas_criticas,0), `${fmt(KPI_STATIC.pct_zonas_criticas,1)}% del total`, "Si supera 10% del parque, la priorización debe pasar de táctica a programa territorial.", "lead"),
    buildKpiCard("Coste de riesgo (EUR)", fmt(KPI_STATIC.coste_riesgo,0), "Aproximación económica oficial", "Ordena el coste de inacción; no sustituye presupuesto regulatorio.", "lead"),
    buildKpiCard("CAPEX diferible", fmt(KPI_STATIC.capex_diferible,0), `${fmt(KPI_STATIC.capex_diferible_pct,1)}% vs CAPEX total`, "Cuantifica margen para flexibilidad/almacenamiento antes de construir activo físico.", "lead"),
  ].join("");

  const officialSupport = [
    buildKpiCard("ENS total (MWh)", fmt(KPI_STATIC.ens_total,1), "Energía no suministrada acumulada", "Mide impacto real de continuidad; prioriza zonas con daño de servicio, no sólo congestión."),
    buildKpiCard("Clientes afectados", fmt(KPI_STATIC.clientes_afectados,0), "Suma de clientes por evento de interrupción", "Convierte el riesgo técnico en exposición reputacional y regulatoria."),
    buildKpiCard("Carga relativa media", fmt(KPI_STATIC.carga_media,3), `Zonas >1.0: ${fmt(KPI_STATIC.utilizacion_excesiva_pct,1)}%`, "Valores cerca o por encima de 1.0 indican saturación estructural o ventana de punta no cubierta."),
    buildKpiCard("Resiliencia índice", fmt(KPI_STATIC.resiliencia_indice,1), "Escala 0-100; mayor es más robusto", "Lectura inversa de fragilidad: cuanto menor, más probable que la congestión derive en interrupción."),
    buildKpiCard("SAIDI aproximado (min)", fmt(KPI_STATIC.saidi_proxy,1), "Duración media por interrupción", "Duraciones altas refuerzan prioridad de resiliencia y renovación de activos."),
    buildKpiCard("Decisiones diferibles", fmt(KPI_STATIC.decisiones_diferibles,0), "Según política de pronóstico", "Sólo son diferibles si el nivel es bajo/medio y la señal predictiva permite monitorización."),
  ].join("");

  const zoneCount = fd.zoneRisk.length;
  const horasCong = fd.zoneRisk.reduce((s,z) => s + num(z.horas_congestion), 0);
  const ens = fd.zoneRisk.reduce((s,z) => s + num(z.ens_total_mwh), 0);
  const clientes = fd.zoneRisk.reduce((s,z) => s + num(z.clientes_afectados_total), 0);
  const criticas = fd.zoneRisk.filter(z => num(z.riesgo_operativo_score) >= 75).length;
  const cargaMedia = zoneCount ? fd.zoneRisk.reduce((s,z) => s + num(z.carga_relativa_max_media), 0) / zoneCount : 0;
  const overPct = zoneCount ? 100 * fd.zoneRisk.filter(z => num(z.carga_relativa_max_media) > 1).length / zoneCount : 0;

  const scoreCount = fd.scoring.length;
  const capex = fd.scoring.reduce((s,r) => s + num(r.capex_total), 0);
  const costeRiesgo = fd.scoring.reduce((s,r) => s + num(r.coste_riesgo_proxy), 0);
  const resiliencia = scoreCount ? (100 - fd.scoring.reduce((s,r) => s + num(r.resilience_risk_score), 0) / scoreCount) : 0;

  const capexDif = fd.capexDef.reduce((s,c) => s + num(c.capex_diferible_proxy_eur), 0);
  const capexDifPct = capex ? 100 * capexDif / capex : 0;

  const intN = fd.interruptions.reduce((s,i) => s + num(i.n_interrupciones), 0);
  const saidi = fd.interruptions.length ? fd.interruptions.reduce((s,i) => s + num(i.duracion_media_min), 0) / fd.interruptions.length : 0;
  const saifi = clientes ? 1000 * intN / clientes : 0;

  const evTotal = fd.electrification.reduce((s,e) => s + num(e.demanda_ev_mwh), 0);
  const indTotal = fd.electrification.reduce((s,e) => s + num(e.demanda_industrial_mwh), 0);
  const ratioNueva = fd.electrification.length ?
    fd.electrification.reduce((s,e) => s + num(e.ratio_demanda_nueva), 0) / fd.electrification.length : 0;

  const diffDecisions = fd.scoring.filter(r => String(r.decision_forecast || "").toLowerCase().includes("diferir") && ["bajo", "medio"].includes(String(r.risk_tier))).length;

  const exploratory = [
    buildKpiCard("Perímetro filtrado", fmt(zoneCount,0), "Lectura exploratoria del filtro activo", "Define si la decisión es sistémica, territorial o puntual."),
    buildKpiCard("Horas congestión filtradas", fmt(horasCong,0), "No sustituye KPI oficial", "Aísla el foco operativo que consume capacidad en el filtro activo."),
    buildKpiCard("ENS filtrada (MWh)", fmt(ens,1), "No sustituye KPI oficial", "Si concentra ENS, el filtro debe subir prioridad aunque tenga pocas zonas."),
    buildKpiCard("Carga media filtrada", fmt(cargaMedia,3), `Zonas >1.0: ${fmt(overPct,1)}%`, "Por encima del umbral exige contención o refuerzo en ventanas de punta."),
    buildKpiCard("Resiliencia filtrada", fmt(resiliencia,1), "Cálculo exploratorio", "Permite comparar fragilidad relativa entre territorios seleccionados."),
    buildKpiCard("Coste riesgo filtrado (EUR)", fmt(costeRiesgo,0), "Aproximación exploratoria", "Usar para ranking relativo dentro del filtro, no para presupuesto final."),
    buildKpiCard("CAPEX filtrado (EUR)", fmt(capex,0), `${fmt(capexDifPct,1)}% diferible en el perímetro`, "Mide cuánto capital puede re-secuenciarse si flexibilidad cubre el estrés."),
    buildKpiCard("Clientes filtrados", fmt(clientes,0), `SAIDI: ${fmt(saidi,1)} min · SAIFI: ${fmt(saifi,3)}`, "Traduce el filtro a exposición de cliente y continuidad."),
    buildKpiCard("Demanda nueva filtrada", fmt(evTotal + indTotal,0), `VE ${fmt(evTotal,0)} · industria ${fmt(indTotal,0)} · relación ${fmt(100*ratioNueva,1)}%`, "Señal de presión futura; aumenta prioridad si coincide con baja flexibilidad."),
    buildKpiCard("Decisiones diferibles filtradas", fmt(diffDecisions,0), "Exploratorio", "Identifica zonas donde monitorizar evita sobreejecutar CAPEX."),
    buildKpiCard("Brecha flexible media", fmt(fd.flexGap.length ? fd.flexGap.reduce((s,f) => s + num(f.gap_tecnico_mw), 0) / fd.flexGap.length : 0,2), "Brecha técnica por zona", "Brecha positiva sostenida indica déficit de flexibilidad gestionable."),
  ].join("");

  byId("kpi_grid").innerHTML =
    buildKpiGroupHead("KPIs oficiales", "Usar en lectura de comité, seguimiento gobernado y comparación de publicación.") +
    `<div class="kpi-band lead">${officialLead}</div>` +
    `<div class="kpi-band support">${officialSupport}</div>` +
    buildKpiGroupHead("Lectura exploratoria del filtro activo", "Sirve para focalizar decisiones territoriales sin sustituir los KPIs oficiales.") +
    `<div class="kpi-band support">${exploratory}</div>`;
}

function updateAlerts(fd) {
  const riskTop = [...fd.zoneProfile].sort((a,b) => num(b.investment_priority_score) - num(a.investment_priority_score)).slice(0,3);
  const topTxt = riskTop.map(z => `${z.zona_id} (${fmt(z.investment_priority_score,1)})`).join(", ") || "sin señal";

  const flexMean = fd.flexGap.length ? fd.flexGap.reduce((s,f) => s + num(f.ratio_flexibilidad_estres), 0) / fd.flexGap.length : 0;
  const tradeMsg = flexMean < 0.8
    ? "Trade-off crítico: la cobertura flexible no compensa el estrés en parte del perímetro."
    : "Trade-off controlado: la cobertura flexible permite absorber parte de la presión sin CAPEX inmediato.";

  const differ = fd.scoring.filter(r => String(r.decision_forecast || "").toLowerCase().includes("diferir") && ["bajo", "medio"].includes(String(r.risk_tier))).map(r => r.zona_id);
  const differTxt = differ.slice(0,4).join(", ");

  byId("alert_critico").textContent = `Alerta crítica: el riesgo se concentra en ${topTxt}. Requiere propietario, fecha de revisión y seguimiento semanal de congestión, ENS y brecha flexible.`;
  byId("alert_tradeoff").textContent = `${tradeMsg} Priorizar intervención por robustez, plazo, reversibilidad y evidencia de reducción de riesgo, no sólo por puntuación.`;
  byId("alert_diferible").textContent = differ.length
    ? `Decisiones potencialmente diferibles con monitorización reforzada: ${differTxt}${differ.length > 4 ? "..." : ""}. El diferimiento debe tener umbral explícito de escalada.`
    : "No se observan zonas claramente diferibles bajo el filtro actual.";
}

function renderAutoInsights(fd) {
  const base = [...EXEC_INSIGHTS];
  const topRisk = [...fd.scoring].sort((a,b) => num(b.investment_priority_score) - num(a.investment_priority_score)).slice(0,3);
  const topRiskTxt = topRisk.map(z => z.zona_id).join(", ");
  const topScenario = [...fd.scenarioSummary].sort((a,b) => num(a.coste_riesgo_total) - num(b.coste_riesgo_total))[0];
  const horizonShort = fd.scoring.filter(s => String(s.recommended_sequence).includes("0-")).length;
  const scoreAvg = fd.scoring.length ? fd.scoring.reduce((s,r) => s + num(r.investment_priority_score), 0) / fd.scoring.length : 0;
  const flexMean = fd.flexGap.length ? fd.flexGap.reduce((s,f) => s + num(f.ratio_flexibilidad_estres), 0) / fd.flexGap.length : 0;
  const critical = fd.scoring.filter(r => ["critico", "alto"].includes(String(r.risk_tier || "").toLowerCase())).length;

  const dyn = [];
  if (topRiskTxt) dyn.push(`Prioridad inmediata territorial: ${topRiskTxt}; convertir el ranking en expedientes, no en aprobación automática de obra.`);
  dyn.push(`Cartera de corto plazo (0-12m): ${fmt(horizonShort,0)} intervenciones; puntuación media filtrada ${fmt(scoreAvg,1)} y ${critical} zonas en nivel alto o crítico.`);
  dyn.push(`Cobertura flexible media del filtro: ${fmt(flexMean,2)}; si se mantiene por debajo del umbral, la mitigación reversible necesita prueba de disponibilidad horaria.`);
  if (topScenario) dyn.push(`Escenario más eficiente por coste de riesgo: ${scenarioLabel(topScenario.scenario)}; usarlo como referencia de secuencia, no como presupuesto aprobado.`);
  dyn.push(`El filtro activo conserva ${fmt(fd.zoneIds.length,0)} zonas y ${fmt(fd.substations.length,0)} subestaciones con señal analítica; si el perímetro es pequeño, validar dependencia regional antes de decidir.`);

  byId("auto_insights").innerHTML = [...base.slice(0,6), ...dyn].map(t => `<li>${esc(t)}</li>`).join("");
}

function _benchStatus(v, target, dir = "le") {
  if (dir === "le") {
    if (v <= target) return { cls: "status-ok", txt: "OK" };
    if (v <= target * 1.2) return { cls: "status-warn", txt: "Vigilancia" };
    return { cls: "status-bad", txt: "Crítico" };
  }
  if (v >= target) return { cls: "status-ok", txt: "OK" };
  if (v >= target * 0.8) return { cls: "status-warn", txt: "Vigilancia" };
  return { cls: "status-bad", txt: "Crítico" };
}

function renderBenchmarks(fd) {
  const zoneCount = Math.max(fd.zoneRisk.length, 1);
  const cargaMedia = fd.zoneRisk.reduce((s,z) => s + num(z.carga_relativa_max_media), 0) / zoneCount;
  const zonasCritPct = 100 * fd.zoneRisk.filter(z => num(z.riesgo_operativo_score) >= 75).length / zoneCount;
  const flexRatio = fd.flexGap.length ? fd.flexGap.reduce((s,f) => s + num(f.ratio_flexibilidad_estres), 0) / fd.flexGap.length : 0;
  const saidi = fd.interruptions.length ? fd.interruptions.reduce((s,i) => s + num(i.duracion_media_min), 0) / fd.interruptions.length : 0;
  const capex = fd.scoring.reduce((s,r) => s + num(r.capex_total), 0);
  const capexDif = fd.capexDef.reduce((s,c) => s + num(c.capex_diferible_proxy_eur), 0);
  const capexDifPct = capex ? 100 * capexDif / capex : 0;
  const ratioNueva = fd.electrification.length ? fd.electrification.reduce((s,e) => s + num(e.ratio_demanda_nueva), 0) / fd.electrification.length : 0;

  const items = [
    { k: "Carga relativa media", v: cargaMedia, t: 0.95, d: "le", f: (x) => fmt(x,3) },
    { k: "Zonas críticas (%)", v: zonasCritPct, t: 10, d: "le", f: (x) => fmt(x,1) + "%" },
    { k: "Ratio flexibilidad/estrés", v: flexRatio, t: 0.85, d: "ge", f: (x) => fmt(x,2) },
    { k: "SAIDI aproximado (min)", v: saidi, t: 120, d: "le", f: (x) => fmt(x,1) },
    { k: "CAPEX diferible (%)", v: capexDifPct, t: 12, d: "ge", f: (x) => fmt(x,1) + "%" },
    { k: "Ratio nueva demanda", v: ratioNueva, t: 0.08, d: "le", f: (x) => fmt(100*x,1) + "%" },
  ];

  byId("bench_grid").innerHTML = items.map(it => {
    const st = _benchStatus(it.v, it.t, it.d);
    return `
      <div class="bench-card">
        <span class="k"><span class="status-dot ${st.cls}"></span>${it.k}</span>
        <div class="v">${it.f(it.v)}</div>
        <div class="small-note">Objetivo: ${it.d === "le" ? "≤" : "≥"} ${it.f(it.t)} · Estado: ${st.txt}</div>
      </div>
    `;
  }).join("");
}

function makeBar(id, labels, values, label, color) {
  const pal = chartTheme();
  destroyChart(id);
  CHARTS[id] = new Chart(byId(id), {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label,
        data: values,
        backgroundColor: color,
        borderRadius: 2,
        borderSkipped: false,
        maxBarThickness: 26,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 380 },
      layout: { padding: { top: 4, right: 6, bottom: 0, left: 2 } },
      plugins: {
        legend: { display: false },
        tooltip: {
          mode: 'index',
          intersect: false,
          backgroundColor: pal.tooltipBg,
          titleColor: pal.label,
          bodyColor: pal.label,
          borderColor: pal.tooltipBorder,
          borderWidth: 1,
          padding: 10,
        }
      },
      scales: {
        x: {
          ticks: { color: pal.tick, autoSkip: true, maxRotation: 30, minRotation: 0, maxTicksLimit: 10, font: { size: 11, weight: "600" } },
          grid: { color: pal.grid },
        },
        y: {
          beginAtZero: true,
          ticks: { color: pal.tick, maxTicksLimit: 8, font: { size: 11 }, callback: (v) => fmtAxisTick(v) },
          grid: { color: pal.grid },
        }
      }
    }
  });
}

function makeLine(id, labels, datasets) {
  const pal = chartTheme();
  destroyChart(id);
  CHARTS[id] = new Chart(byId(id), {
    type: "line",
    data: {
      labels,
      datasets: datasets.map((dataset) => ({
        fill: false,
        pointRadius: dataset.pointRadius === 0 ? 0 : 2.5,
        pointHoverRadius: dataset.pointRadius === 0 ? 0 : 4,
        borderWidth: dataset.borderWidth || 2.6,
        ...dataset,
      })),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 420 },
      interaction: { mode: "index", intersect: false },
      layout: { padding: { top: 4, right: 6, bottom: 0, left: 2 } },
      plugins: {
        tooltip: {
          mode: 'index',
          intersect: false,
          backgroundColor: pal.tooltipBg,
          titleColor: pal.label,
          bodyColor: pal.label,
          borderColor: pal.tooltipBorder,
          borderWidth: 1,
          padding: 10,
        },
        legend: { labels: { color: pal.tick, usePointStyle: true, boxWidth: 10, padding: 14, font: { size: 11, weight: "700" } } },
      },
      scales: {
        x: {
          ticks: { color: pal.tick, autoSkip: true, maxTicksLimit: 12, maxRotation: 30, minRotation: 0, font: { size: 11, weight: "600" } },
          grid: { color: pal.grid },
        },
        y: {
          beginAtZero: true,
          ticks: { color: pal.tick, maxTicksLimit: 8, font: { size: 11 }, callback: (v) => fmtAxisTick(v) },
          grid: { color: pal.grid },
        }
      }
    }
  });
}

function makeScatter(id, points, titleLabel, color, xTitle, yTitle) {
  const pal = chartTheme();
  destroyChart(id);
  CHARTS[id] = new Chart(byId(id), {
    type: "scatter",
    data: {
      datasets: [{
        label: titleLabel,
        data: points,
        backgroundColor: color,
        borderColor: color,
        pointRadius: 5.5,
        pointHoverRadius: 7,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 380 },
      layout: { padding: { top: 4, right: 6, bottom: 0, left: 2 } },
      plugins: {
        tooltip: {
          backgroundColor: pal.tooltipBg,
          titleColor: pal.label,
          bodyColor: pal.label,
          borderColor: pal.tooltipBorder,
          borderWidth: 1,
          padding: 10,
          callbacks: {
            label: (ctx) => {
              const p = ctx.raw || {};
              return `${p.zona || "zona"}: x=${fmt(p.x,2)} | y=${fmt(p.y,2)} | puntuación=${fmt(p.score || 0,1)}`;
            }
          }
        }
      },
      scales: {
        x: {
          ticks: { color: pal.tick, font: { size: 11, weight: "600" } },
          grid: { color: pal.grid },
          title: { display: true, text: xTitle || "Eje X", color: pal.label, font: { size: 11, weight: "700" } },
        },
        y: {
          beginAtZero: true,
          ticks: { color: pal.tick, font: { size: 11 }, callback: (v) => fmtAxisTick(v) },
          grid: { color: pal.grid },
          title: { display: true, text: yTitle || "Eje Y", color: pal.label, font: { size: 11, weight: "700" } },
        }
      }
    }
  });
}

function makeMixedScenario(id, labels, riskVals, invVals) {
  const pal = chartTheme();
  destroyChart(id);
  CHARTS[id] = new Chart(byId(id), {
    data: {
      labels,
      datasets: [
        {
          type: "bar",
          label: "Coste de riesgo",
          data: riskVals,
          backgroundColor: pal.risk,
          borderRadius: 2,
          borderSkipped: false,
          yAxisID: "y",
        },
        {
          type: "line",
          label: "Inversión requerida",
          data: invVals,
          borderColor: pal.accent,
          backgroundColor: pal.accent,
          borderWidth: 2.8,
          pointRadius: 2.5,
          pointHoverRadius: 4,
          tension: .25,
          yAxisID: "y1",
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 420 },
      plugins: {
        tooltip: {
          mode: 'index',
          intersect: false,
          backgroundColor: pal.tooltipBg,
          titleColor: pal.label,
          bodyColor: pal.label,
          borderColor: pal.tooltipBorder,
          borderWidth: 1,
          padding: 10,
        },
        legend: { labels: { color: pal.tick, usePointStyle: true, boxWidth: 10, padding: 14, font: { size: 11, weight: "700" } } },
      },
      scales: {
        y: {
          position: "left",
          beginAtZero: true,
          ticks: { color: pal.tick, font: { size: 11 }, callback: (v) => fmtAxisTick(v) },
          grid: { color: pal.grid },
        },
        y1: {
          position: "right",
          grid: { drawOnChartArea: false, color: pal.grid },
          ticks: { color: pal.tick, font: { size: 11 }, callback: (v) => fmtAxisTick(v) },
        },
        x: {
          ticks: { color: pal.tick, maxRotation: 20, minRotation: 20, font: { size: 11, weight: "600" } },
          grid: { color: pal.grid },
        }
      }
    }
  });
}

function renderHeatmap(fd) {
  const rows = uniq(fd.regionHour.map(r => r.region_operativa)).sort();
  const hours = Array.from({length:24}, (_,i) => i);

  const keyMap = {};
  fd.regionHour.forEach(r => {
    keyMap[`${r.region_operativa}_${r.hora}`] = {
      carga: num(r.carga_relativa_media),
      cong: num(r.ratio_congestion_hora),
    };
  });

  // Rampa continua arena → tinta roja: un solo matiz creciente en saturación y
  // oscuridad, legible en ambos temas porque cada celda fija su propio texto.
  function bg(carga, cong) {
    const stress = Math.min(1, Math.max(0, 0.65 * (carga / 1.2) + 0.35 * cong));
    const hue = Math.round(46 - 34 * stress);
    const sat = Math.round(40 + 28 * stress);
    const light = Math.round(90 - 50 * stress);
    return { fill: `hsl(${hue} ${sat}% ${light}%)`, ink: light < 62 ? "#f7f3ea" : "#231e14" };
  }

  const header = `<tr><th>Región / hora</th>${hours.map(h => `<th>${h}</th>`).join("")}</tr>`;
  const body = rows.map(region => {
    const cells = hours.map(h => {
      const m = keyMap[`${region}_${h}`] || { carga: 0, cong: 0 };
      const title = `Carga: ${fmt(m.carga,2)} | Cong.: ${fmt(100*m.cong,1)}%`;
      const c = bg(m.carga, m.cong);
      return `<td title="${title}" style="background:${c.fill};color:${c.ink};">${fmt(m.carga,2)}</td>`;
    }).join("");
    return `<tr><td>${esc(region)}</td>${cells}</tr>`;
  }).join("");

  byId("heatmap_container").innerHTML = `<table class="heatmap"><thead>${header}</thead><tbody>${body}</tbody></table>`;
}

function renderScenarioTopTable(fd) {
  const rows = [...fd.scenarioTop].sort((a,b) => num(b.investment_priority_score_scenario) - num(a.investment_priority_score_scenario)).slice(0,12);
  if (!rows.length) {
    byId("scenario_top_table").innerHTML = "<div class='small-note' style='padding:10px;'>No hay datos de escenario para el filtro actual.</div>";
    return;
  }

  const html = `
    <table>
      <thead>
        <tr>
          <th>Escenario</th>
          <th>Zona</th>
          <th>Puntuación</th>
          <th>Congestión</th>
          <th>ENS</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map(r => `
          <tr>
            <td>${esc(scenarioLabel(r.scenario))}</td>
            <td>${esc(r.zona_id)}</td>
            <td>${fmt(r.investment_priority_score_scenario,1)}</td>
            <td>${fmt(r.horas_congestion_scenario,0)}</td>
            <td>${fmt(r.ens_scenario,2)}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
  byId("scenario_top_table").innerHTML = html;
}

function renderCharts(fd) {
  if (typeof Chart === "undefined") {
    document.querySelectorAll("canvas").forEach((cv) => {
      const parent = cv.parentElement;
      if (parent && !parent.querySelector(".chart-fallback")) {
        const msg = document.createElement("div");
        msg.className = "chart-fallback";
        msg.textContent = "No fue posible cargar Chart.js en este contexto. Los KPIs y la tabla siguen disponibles.";
        parent.appendChild(msg);
      }
    });
    return;
  }

  const CB = chartTheme();
  const monthly = [...fd.monthly].sort((a,b) => String(a.mes).localeCompare(String(b.mes)));
  makeLine(
    "ch_carga",
    monthly.map(m => m.mes),
    [
      {
        label: "Carga relativa media",
        data: monthly.map(m => num(m.carga_relativa)),
        borderColor: CB.accent,
        backgroundColor: CB.accentSoft,
        tension: .25,
      },
      {
        label: "Umbral capacidad",
        data: monthly.map(() => 1.0),
        borderColor: CB.risk,
        borderDash: [6, 5],
        pointRadius: 0,
        tension: 0,
      },
    ]
  );

  const topCong = [...fd.zoneRisk].sort((a,b) => num(b.horas_congestion) - num(a.horas_congestion)).slice(0,12);
  makeBar("ch_congestion_zona", topCong.map(z => z.zona_id), topCong.map(z => num(z.horas_congestion)), "Horas congestión", CB.accent);

  const riskTerr = [...fd.zoneRisk].slice().sort((a,b) => num(b.riesgo_operativo_score) - num(a.riesgo_operativo_score)).slice(0,12);
  makeBar("ch_riesgo_territorio", riskTerr.map(z => z.zona_id), riskTerr.map(z => num(z.riesgo_operativo_score)), "Riesgo operativo", CB.accent);

  const topEns = [...fd.zoneRisk].sort((a,b) => num(b.ens_total_mwh) - num(a.ens_total_mwh)).slice(0,12);
  makeBar("ch_ens", topEns.map(z => z.zona_id), topEns.map(z => num(z.ens_total_mwh)), "ENS", CB.accent);

  const topInt = [...fd.interruptions].sort((a,b) => num(b.n_interrupciones) - num(a.n_interrupciones)).slice(0,12);
  makeBar("ch_interruptions", topInt.map(i => i.zona_id), topInt.map(i => num(i.n_interrupciones)), "Interrupciones", CB.neutral);

  const topSub = [...fd.substations].sort((a,b) => num(b.horas_congestion) - num(a.horas_congestion)).slice(0,12);
  makeBar("ch_substations", topSub.map(s => s.subestacion_id), topSub.map(s => num(s.horas_congestion)), "Horas congestión", CB.accent);

  const flexPoints = fd.flexGap.map(f => ({
    x: num(f.ratio_flexibilidad_estres),
    y: num(f.gap_tecnico_mw),
    zona: f.zona_id,
    score: num(f.riesgo_operativo_score),
  }));
  makeScatter("ch_flex_gap", flexPoints, "Brecha flexible vs relación flexibilidad/estrés", CB.accent, "Cobertura flexibilidad / estrés", "Brecha técnica (MW)");

  const options = [...(DATA.optionsSummary || [])].sort((a,b) => num(b.option_score_medio) - num(a.option_score_medio));
  makeBar("ch_tradeoff", options.map(o => o.option), options.map(o => num(o.option_score_medio)), "Puntuación multicriterio media", CB.accent);

  const topStorage = [...fd.flexGap].sort((a,b) => num(b.storage_potencia_total_mw) - num(a.storage_potencia_total_mw)).slice(0,12);
  makeBar("ch_storage", topStorage.map(s => s.zona_id), topStorage.map(s => num(s.storage_potencia_total_mw)), "Potencia de almacenamiento (MW)", CB.accent);

  const topCapex = [...fd.capexDef].sort((a,b) => num(b.capex_diferible_proxy_eur) - num(a.capex_diferible_proxy_eur)).slice(0,12);
  const pal = chartTheme();
  destroyChart("ch_capex_def");
  CHARTS["ch_capex_def"] = new Chart(byId("ch_capex_def"), {
    type: "bar",
    data: {
      labels: topCapex.map(c => c.zona_id),
      datasets: [
        {
          label: "CAPEX refuerzo",
          data: topCapex.map(c => num(c.capex_refuerzo_eur)),
          backgroundColor: pal.risk,
          borderRadius: 2,
          borderSkipped: false,
          maxBarThickness: 24,
        },
        {
          label: "CAPEX diferible",
          data: topCapex.map(c => num(c.capex_diferible_proxy_eur)),
          backgroundColor: pal.accent,
          borderRadius: 2,
          borderSkipped: false,
          maxBarThickness: 24,
        },
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 400 },
      plugins: {
        legend: { labels: { color: pal.tick, usePointStyle: true, boxWidth: 10, padding: 14, font: { size: 11, weight: "700" } } },
        tooltip: {
          backgroundColor: pal.tooltipBg,
          titleColor: pal.label,
          bodyColor: pal.label,
          borderColor: pal.tooltipBorder,
          borderWidth: 1,
          padding: 10,
        },
      },
      scales: {
        x: { ticks: { color: pal.tick, autoSkip: true, maxTicksLimit: 10, font: { size: 11, weight: "600" } }, grid: { color: pal.grid } },
        y: { beginAtZero: true, ticks: { color: pal.tick, maxTicksLimit: 8, font: { size: 11 }, callback: (v) => fmtAxisTick(v) }, grid: { color: pal.grid } },
      },
    }
  });

  const topEv = [...fd.electrification].sort((a,b) => num(b.demanda_ev_mwh) - num(a.demanda_ev_mwh)).slice(0,12);
  makeBar("ch_ev", topEv.map(e => e.zona_id), topEv.map(e => num(e.demanda_ev_mwh)), "Demanda VE (MWh)", CB.accent);

  const topInd = [...fd.electrification].sort((a,b) => num(b.demanda_industrial_mwh) - num(a.demanda_industrial_mwh)).slice(0,12);
  makeBar("ch_ind", topInd.map(e => e.zona_id), topInd.map(e => num(e.demanda_industrial_mwh)), "Demanda industrial (MWh)", CB.accent);

  makeLine(
    "ch_curt",
    monthly.map(m => m.mes),
    [{
      label: "Vertido mensual",
      data: monthly.map(m => num(m.curtailment)),
      borderColor: CB.accent,
      backgroundColor: CB.accentSoft,
      tension: .25,
    }]
  );

  const topPrio = [...fd.scoring].sort((a,b) => num(b.investment_priority_score) - num(a.investment_priority_score)).slice(0,12);
  makeBar("ch_priority", topPrio.map(p => p.zona_id), topPrio.map(p => num(p.investment_priority_score)), "Puntuación de prioridad de inversión", CB.accent);

  const riskEconPoints = fd.scoring.map(s => ({
    x: num(s.congestion_risk_score),
    y: num(s.economic_priority_score),
    zona: s.zona_id,
    score: num(s.investment_priority_score),
  }));
  makeScatter("ch_risk_econ", riskEconPoints, "Riesgo técnico vs prioridad económica", CB.accent, "Riesgo técnico", "Prioridad económica");

  const topFeed = [...fd.feeders].sort((a,b) => num(b.criticidad_feeder_score) - num(a.criticidad_feeder_score)).slice(0,12);
  makeBar("ch_feeders", topFeed.map(f => f.alimentador_id), topFeed.map(f => num(f.criticidad_feeder_score)), "Criticidad feeder", CB.accent);

  const scen = [...fd.scenarioSummary].sort((a,b) => num(b.coste_riesgo_total) - num(a.coste_riesgo_total));
  makeMixedScenario("ch_scenarios", scen.map(s => scenarioLabel(s.scenario)), scen.map(s => num(s.coste_riesgo_total)), scen.map(s => num(s.inversion_requerida_total)));

  renderHeatmap(fd);
  renderScenarioTopTable(fd);
}

function justification(row) {
  const driver = String(row.main_risk_driver || "");
  const interv = String(row.recommended_intervention || "");
  const score = num(row.investment_priority_score);
  if (interv === "reforzar_red_local") {
    return `Riesgo ${fmt(score,1)} con señal estructural (${driverLabel(driver)}); abrir estudio de refuerzo y confirmar topología, contingencias, permisos y coste real antes de aprobar obra.`;
  }
  if (interv === "activar_flexibilidad") {
    return `Riesgo ${fmt(score,1)} y brecha flexible relevante; flexibilidad permite respuesta rápida si demuestra disponibilidad en la hora crítica y reduce ENS/congestión observada.`;
  }
  if (interv === "desplegar_almacenamiento") {
    return `Riesgo ${fmt(score,1)} con presión variable; el almacenamiento debe dimensionarse por duración, ubicación y efecto simultáneo sobre punta, vertido y resiliencia.`;
  }
  if (interv === "optimizar_operacion") {
    return `Riesgo ${fmt(score,1)} con ventana operativa inmediata; útil para contener ENS mientras madura la decisión estructural, con revisión si persiste el estrés.`;
  }
  if (interv === "sustituir_activos") {
    return `Riesgo ${fmt(score,1)} asociado a exposición de activos; renovación prioritaria si inspección, edad, criticidad y continuidad confirman fragilidad material.`;
  }
  return `Riesgo ${fmt(score,1)} con señal no concluyente para CAPEX inmediato; mantener monitorización reforzada con umbral de escalada y fecha de relectura.`;
}

function renderPriorityTable(fd) {
  const tbody = byId("priority_table").querySelector("tbody");
  const q = byId("searchBox").value.toLowerCase();

  const rows = [...fd.scoring]
    .filter(r => (`${r.zona_id} ${r.main_risk_driver} ${r.recommended_intervention}`.toLowerCase().includes(q)))
    .sort((a,b) => {
      const k = TABLE_STATE.sortKey;
      const av = a[k];
      const bv = b[k];
      if (av < bv) return -1 * TABLE_STATE.sortDir;
      if (av > bv) return 1 * TABLE_STATE.sortDir;
      return 0;
    });

  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><button class="zone-link" data-zone="${esc(r.zona_id)}" aria-label="Filtrar zona ${esc(r.zona_id)}">${esc(r.zona_id)}</button></td>
      <td>${fmt(r.investment_priority_score,2)}</td>
      <td>${paintRiskBadge(r.risk_tier)}</td>
      <td>${esc(urgencyLabel(r.urgency_tier))}</td>
      <td>${esc(driverLabel(r.main_risk_driver))}</td>
      <td>${esc(interventionLabel(r.recommended_intervention))}</td>
      <td>${esc(seqLabel(r.recommended_sequence))}</td>
      <td style="text-align:right; font-variant-numeric:tabular-nums">${fmtEurCompact(r.capex_total)}</td>
      <td>${esc(justification(r))}</td>
    </tr>
  `).join("");

  byId("table_count").textContent = `${fmt(rows.length,0)} filas`;
  updateSortIndicators();
  tbody.querySelectorAll(".zone-link").forEach(btn => {
    btn.addEventListener("click", () => {
      byId("f_zona").value = btn.dataset.zone || "";
      applyAll();
    });
  });
}

function updateSortIndicators() {
  document.querySelectorAll("#priority_table th[data-key]").forEach(th => {
    if (th.dataset.key === TABLE_STATE.sortKey) {
      th.setAttribute("aria-sort", TABLE_STATE.sortDir === 1 ? "ascending" : "descending");
    } else {
      th.removeAttribute("aria-sort");
    }
  });
}

function renderHorizonPlan(fd) {
  const seqCount = {};
  fd.scoring.forEach(r => {
    const key = String(r.recommended_sequence || "sin_secuencia");
    seqCount[key] = (seqCount[key] || 0) + 1;
  });
  const order = ["0-3m", "0-6m", "3-12m", "0-12m", "6-24m", "revision_trimestral", "sin_secuencia"];
  const labels = order.filter(k => seqCount[k] !== undefined);
  const values = labels.map(k => seqCount[k]);
  makeBar("ch_horizon", labels, values, "Intervenciones", chartTheme().accent);
}

function renderDrillDown(fd) {
  const selectedZone = readFilters().zona;
  let zone = null;
  if (selectedZone) {
    zone = fd.zoneProfile.find(z => z.zona_id === selectedZone) || null;
  }
  if (!zone) {
    zone = [...fd.zoneProfile].sort((a,b) => num(b.investment_priority_score) - num(a.investment_priority_score))[0] || null;
  }

  if (!zone) {
    byId("drill_zone_panel").innerHTML = "<h4>Drill-down zona</h4><p>Sin datos para el filtro actual.</p>";
    return;
  }

  const options = [...fd.optionsByZone]
    .filter(o => o.zona_id === zone.zona_id)
    .sort((a,b) => num(b.option_score) - num(a.option_score))
    .slice(0,4);

  const optionsHtml = options.length
    ? `<ul class="insight-list">${options.map(o => `<li><b>${esc(o.option)}</b>: puntuación ${fmt(o.option_score,1)}, impacto ${fmt(o.impact,1)}, coste ${fmt(o.cost_proxy,0)}</li>`).join("")}</ul>`
    : "<p class='small-note'>Sin alternativas multicriterio disponibles para esta zona.</p>";

  byId("drill_zone_panel").innerHTML = `
    <h4>Detalle de zona ${esc(zone.zona_id)} · ${esc(zone.zona_nombre || "")}</h4>
    <div class="drill-metric">
      <div class="k">Puntuación de prioridad</div><div class="v">${fmt(zone.investment_priority_score,1)}</div>
      <div class="k">Nivel de riesgo</div><div class="v">${paintRiskBadge(zone.risk_tier)}</div>
      <div class="k">Intervención recomendada</div><div class="v">${esc(interventionLabel(zone.recommended_intervention) || "N/A")}</div>
      <div class="k">Secuencia</div><div class="v">${esc(seqLabel(zone.recommended_sequence) || "N/A")}</div>
      <div class="k">ENS (MWh)</div><div class="v">${fmt(zone.ens_total_mwh,1)}</div>
      <div class="k">Horas congestión</div><div class="v">${fmt(zone.horas_congestion,0)}</div>
      <div class="k">Carga relativa max media</div><div class="v">${fmt(zone.carga_relativa_max_media,3)}</div>
      <div class="k">Brecha flex media</div><div class="v">${fmt(zone.brecha_flex_media,3)}</div>
      <div class="k">Presión electrificación</div><div class="v">${fmt(100*num(zone.presion_electrificacion_media),1)}%</div>
      <div class="k">CAPEX asociado (EUR)</div><div class="v">${fmt(zone.capex_total,0)}</div>
    </div>
    <b>Alternativas multicriterio para la zona</b>
    ${optionsHtml}
    <div class="inline-meta">Factor principal: ${esc(driverLabel(zone.main_risk_driver) || "N/A")} · Confianza de pronóstico: ${esc(humanize(zone.confidence_flag) || "N/A")}</div>
  `;
}

function _csvEscape(v) {
  const s = String(v ?? "");
  if (s.includes(",") || s.includes("\\\"") || s.includes("\\n")) {
    return `"${s.replace(/"/g, "\\\"\\\"")}"`;
  }
  return s;
}

function exportFilteredTableCSV(fd) {
  const rows = [...fd.scoring].sort((a,b) => num(b.investment_priority_score) - num(a.investment_priority_score));
  // Exportación ejecutiva: cabeceras claras, etiquetas legibles y numéricos reimportables.
  const cols = [
    ["Zona", r => r.zona_id],
    ["Puntuación prioridad", r => num(r.investment_priority_score)],
    ["Nivel riesgo", r => riskLabel(r.risk_tier)],
    ["Urgencia", r => urgencyLabel(r.urgency_tier)],
    ["Factor principal", r => driverLabel(r.main_risk_driver)],
    ["Intervención recomendada", r => interventionLabel(r.recommended_intervention)],
    ["Secuencia", r => seqLabel(r.recommended_sequence)],
    ["CAPEX asociado (EUR)", r => Math.round(num(r.capex_total))],
    ["Coste riesgo aproximado (EUR)", r => Math.round(num(r.coste_riesgo_proxy))],
  ];
  const header = cols.map(c => c[0]).join(",");
  const lines = rows.map(r => cols.map(c => _csvEscape(c[1](r))).join(","));
  const csv = [header, ...lines].join("\\n");

  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `priorizacion_filtrada_${new Date().toISOString().slice(0,10)}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(a.href);
}

function updateDecisionList(fd) {
  const list = byId("decision_list");
  const top = [...fd.scoring].sort((a,b) => num(b.investment_priority_score) - num(a.investment_priority_score));
  const topIds = top.slice(0, 5).map(x => x.zona_id).join(", ") || "N/A";

  const reinforce = top.filter(r => r.recommended_intervention === "reforzar_red_local").length;
  const flex = top.filter(r => ["activar_flexibilidad", "optimizar_operacion"].includes(String(r.recommended_intervention))).length;
  const storage = top.filter(r => r.recommended_intervention === "desplegar_almacenamiento").length;
  const monitor = top.filter(r => r.recommended_intervention === "monitorizar").length;

  const riskMean = top.length ? top.reduce((s,r) => s + num(r.investment_priority_score), 0) / top.length : 0;
  const capex = top.reduce((s,r) => s + num(r.capex_total), 0);

  const items = [
    `Intervención inmediata en zonas top del ranking: ${topIds}; abrir expediente con propietario, línea base, alternativa preferida y mitigación transitoria.`,
    `Refuerzo de red conviene donde la congestión y la presión de electrificación son persistentes (casos detectados: ${reinforce}); no debe aprobarse sin ingeniería, permisos y coste real.`,
    `Flexibilidad y operación avanzada son preferibles en zonas de urgencia alta-planificada cuando el horizonte de obra es largo (casos: ${flex}); exigir medición antes/después y umbral de renovación.`,
    `El almacenamiento se justifica en zonas con brecha flexible estructural y variabilidad de demanda (casos: ${storage}); dimensionar por MW, MWh, localización y valor de vertido evitado.`,
    `Decisiones diferibles deben limitarse a zonas monitorizables con puntuación medio-baja y pronóstico aceptable (casos monitorización: ${monitor}); diferir sin gatillo de escalada equivale a aceptar riesgo no gobernado.`,
    `En el perímetro filtrado, puntuación media ${fmt(riskMean,1)} y CAPEX agregado ${fmt(capex,0)} EUR; la secuencia debe proteger opción, reducir exposición y evitar CAPEX homogéneo sin discriminación territorial.`,
  ];

  list.innerHTML = items.map(x => `<li>${esc(x)}</li>`).join("");
}

function updateWhatIf(fd) {
  const ev = num(byId("wf_ev").value);
  const ind = num(byId("wf_ind").value);
  const flex = num(byId("wf_flex").value);
  const storage = num(byId("wf_storage").value);

  byId("wf_ev_v").textContent = `${ev}%`;
  byId("wf_ind_v").textContent = `${ind}%`;
  byId("wf_flex_v").textContent = `${flex}%`;
  byId("wf_storage_v").textContent = `${storage}%`;

  const base = [...fd.scoring].sort((a,b) => num(b.investment_priority_score) - num(a.investment_priority_score))[0];
  if (!base) {
    byId("whatif_result").textContent = "No hay datos en el filtro actual para simulación.";
    return;
  }

  const baseScore = num(base.investment_priority_score);
  const elec = num(base.electrification_pressure_score);
  const gap = num(base.flexibility_gap_score);

  const elecAdj = elec * (1 + 0.006 * ev + 0.004 * ind);
  const gapAdj = gap * (1 - 0.006 * flex - 0.0045 * storage);
  const scoreAdj = Math.max(0, Math.min(100, 0.58 * baseScore + 0.24 * elecAdj + 0.18 * gapAdj));

  let accion = "monitorizar";
  if (scoreAdj >= 85) accion = "intervencion_inmediata_prioritaria";
  else if (scoreAdj >= 74 && gapAdj >= 58) accion = "reforzar_red_local";
  else if (scoreAdj >= 62 && gapAdj >= 48) accion = "desplegar_almacenamiento";
  else if (scoreAdj >= 52) accion = "activar_flexibilidad";
  else accion = "optimizar_operacion";

  byId("whatif_result").textContent =
    `Zona referencia ${base.zona_id}: puntuación base ${fmt(baseScore,1)} → puntuación simulada ${fmt(scoreAdj,1)}. ` +
    `Bajo este supuesto, la acción sugerida evoluciona a: ${interventionLabel(accion)}.`;
}

function applyAll() {
  const fd = getFilteredData();
  updateFilterSummary(fd);
  updateTopDecision(fd);
  updateKpis(fd);
  updateAlerts(fd);
  renderAutoInsights(fd);
  renderBenchmarks(fd);
  renderCharts(fd);
  renderPriorityTable(fd);
  renderHorizonPlan(fd);
  renderDrillDown(fd);
  updateDecisionList(fd);
  updateWhatIf(fd);
}

function bindEvents() {
  byId("btn_theme").addEventListener("click", () => {
    setTheme(getTheme() === "dark" ? "light" : "dark");
    applyAll();
  });

  byId("btn_export").addEventListener("click", () => exportFilteredTableCSV(getFilteredData()));
  byId("btn_export_table").addEventListener("click", () => exportFilteredTableCSV(getFilteredData()));
  byId("btn_focus_top").addEventListener("click", () => {
    const fd = getFilteredData();
    const top = [...fd.scoring].sort((a,b) => num(b.investment_priority_score) - num(a.investment_priority_score))[0];
    if (top && top.zona_id) {
      byId("f_zona").value = top.zona_id;
      applyAll();
    }
  });

  byId("btn_reset").addEventListener("click", () => {
    ["f_region","f_zona","f_sub","f_tipo","f_activo","f_risk","f_intervencion","f_scenario"].forEach(id => byId(id).value = "");
    const months = uniq((DATA.monthly || []).map(m => m.mes)).sort();
    if (months.length) {
      byId("f_from").value = months[0];
      byId("f_to").value = months[months.length - 1];
    }
    byId("searchBox").value = "";
    applyAll();
  });

  byId("searchBox").addEventListener("input", () => renderPriorityTable(getFilteredData()));

  byId("priority_table").querySelectorAll("th[data-key]").forEach(th => {
    th.addEventListener("click", () => {
      const key = th.dataset.key;
      if (TABLE_STATE.sortKey === key) TABLE_STATE.sortDir *= -1;
      else { TABLE_STATE.sortKey = key; TABLE_STATE.sortDir = -1; }
      renderPriorityTable(getFilteredData());
    });
  });

  ["wf_ev","wf_ind","wf_flex","wf_storage"].forEach(id => {
    byId(id).addEventListener("input", () => updateWhatIf(getFilteredData()));
  });

  ["f_region","f_zona","f_sub","f_tipo","f_activo","f_risk","f_intervencion","f_from","f_to","f_scenario"].forEach(id => {
    byId(id).addEventListener("change", applyAll);
  });
}

function renderGovernance() {
  const el = byId("gov_footer");
  if (typeof GOVERNANCE !== "object" || GOVERNANCE === null) return;
  const g = GOVERNANCE;
  const cap = s => { s = String(s ?? ""); return s ? s.charAt(0).toUpperCase() + s.slice(1) : "N/A"; };
  const releaseLabel = s => ({
    "publish-ready": "Publicable",
    "publish-with-caveats": "Publicable con matices",
    "publish-blocked": "Publicación bloqueada",
    "decision-support ready": "Apto como soporte de decisión",
    "decision-support only": "Solo soporte de decisión",
    "screening-grade only": "Solo cribado preliminar",
  }[String(s ?? "")] || String(s || "N/A").replace(/[-_]/g, " "));
  const status = String(g.validation_status || "N/A");

  // Command-header status chips (governance at a glance).
  const chipVal = byId("status_validation");
  if (chipVal) {
    chipVal.querySelector(".v").textContent = status;
    chipVal.classList.toggle("live", status.toUpperCase() === "PASS");
    chipVal.classList.toggle("warn", status.toUpperCase() !== "PASS" && status !== "N/A");
  }
  const chipRel = byId("status_release");
  if (chipRel) {
    const pub = String(g.publish_state || "N/A");
    chipRel.querySelector(".v").textContent = releaseLabel(pub);
    chipRel.classList.toggle("live", pub === "publish-ready");
    chipRel.classList.toggle("warn", pub === "publish-with-caveats");
  }
  const chipVer = byId("status_version");
  if (chipVer) chipVer.textContent = "v" + String(g.version || "N/A");
  if (!el) return;
  const items = [
    ["Validación analítica", status, status.toUpperCase() === "PASS" ? "pass" : ""],
    ["Confianza", cap(g.validation_confidence), ""],
    ["Estado de publicación", releaseLabel(g.publish_state), ""],
    ["Versión del tablero", String(g.version || "N/A"), ""],
  ];
  el.innerHTML = items
    .map(([k, v, cls]) => `<span><span class="gk">${esc(k)}</span><span class="gv ${cls}">${esc(v)}</span></span>`)
    .join("");
}

// Masthead signature: 24-month load-curve ridge with the capacity threshold,
// drawn from real monthly data so the identity is substantive, not decorative.
function renderHeroRidge() {
  const host = byId("hero_ridge");
  if (!host) return;
  const rows = [...(DATA.monthly || [])].sort((a, b) => String(a.mes).localeCompare(String(b.mes)));
  const vals = rows.map(m => num(m.carga_relativa)).filter(Number.isFinite);
  if (vals.length < 2) { host.style.display = "none"; return; }
  const W = 1000, H = 66, pad = 2;
  const lo = Math.min(...vals, 0.82), hi = Math.max(...vals, 1.04);
  const span = (hi - lo) || 1;
  const x = i => pad + (i / (vals.length - 1)) * (W - 2 * pad);
  const y = v => (H - 5) - ((v - lo) / span) * (H - 15);
  const line = vals.map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const area = `${line} L${x(vals.length - 1).toFixed(1)},${H} L${x(0).toFixed(1)},${H} Z`;
  const thrShown = 1.0 >= lo && 1.0 <= hi;
  const yThr = y(1.0).toFixed(1);
  host.innerHTML = `
  <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" role="img" aria-label="Tendencia de carga relativa media mensual frente al umbral de capacidad">
    <path d="${area}" class="ridge-area"/>
    ${thrShown ? `<line x1="0" y1="${yThr}" x2="${W}" y2="${yThr}" class="ridge-thr" stroke-width="1" stroke-dasharray="5 5"/>` : ""}
    <path d="${line}" class="ridge-line" fill="none" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
  </svg>`;
}

/* Índice del parte: se deriva de los propios capítulos renderizados, de modo que
   añadir o reordenar una sección no exige mantener una lista paralela. */
function buildDocIndex() {
  const nav = byId("doc_index");
  if (!nav) return;
  const chapters = [...document.querySelectorAll(".main .section, .main .exec-decision")];
  const links = [];

  chapters.forEach((sec, i) => {
    const head = sec.querySelector("h2, h3");
    if (!head) return;
    if (!sec.id) sec.id = `cap_${i}`;
    const idxEl = head.querySelector(".sec-idx");
    const n = idxEl ? idxEl.textContent.trim().replace(".", "") : "—";
    const title = [...head.childNodes]
      .filter(node => node !== idxEl)
      .map(node => node.textContent)
      .join("")
      .trim();

    // Enlace nativo: el salto y su suavizado los resuelve el navegador
    // (scroll-behavior), que además respeta la preferencia de movimiento
    // reducido sin código adicional.
    const a = document.createElement("a");
    a.href = `#${sec.id}`;
    a.innerHTML = `<span class="di-n">${esc(n)}</span><span>${esc(title)}</span>`;
    nav.appendChild(a);
    links.push({ a, sec });
  });

  if (!links.length) return;
  const bar = document.querySelector(".active-filters");
  const hero = document.querySelector(".hero");

  // Capítulo activo: el último cuyo encabezado ha cruzado la línea de lectura.
  // Se resuelve por geometría en cada cuadro de scroll —y no con
  // IntersectionObserver— para que el rail siga al lector de forma
  // determinista en cualquier contexto de renderizado.
  function updateOnScroll() {
    const line = window.innerHeight * 0.28;
    let current = null;
    links.forEach(({ sec }) => {
      if (sec.getBoundingClientRect().top <= line) current = sec;
    });
    links.forEach(({ a, sec }) => a.classList.toggle("is-active", sec === current));
    // La barra queda pegada justo cuando la cabecera que la precede sale de
    // cuadro. Se mide contra la cabecera —y no contra offsetTop, que en un
    // elemento pegajoso devuelve ya la posición pegada— para tener una
    // referencia estable.
    if (bar && hero) bar.classList.toggle("is-stuck", hero.getBoundingClientRect().bottom <= 0);
  }

  // Lectura directa en el manejador: son diez rectángulos por evento, coste
  // despreciable frente a la complejidad de un throttle por cuadro.
  window.addEventListener("scroll", updateOnScroll, { passive: true });
  window.addEventListener("resize", updateOnScroll, { passive: true });
  updateOnScroll();
}

function bootstrap() {
  if (typeof Chart !== "undefined") {
    Chart.defaults.font.family = "'Archivo', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif";
    Chart.defaults.font.size = 11;
    Chart.defaults.font.weight = 500;
    Chart.defaults.animation.duration = 620;
    Chart.defaults.animation.easing = "easeOutQuart";
    Chart.defaults.plugins.tooltip.padding = 11;
    Chart.defaults.plugins.tooltip.cornerRadius = 8;
    Chart.defaults.plugins.tooltip.boxPadding = 6;
    Chart.defaults.plugins.tooltip.titleMarginBottom = 6;
    Chart.defaults.plugins.tooltip.usePointStyle = true;
    Chart.defaults.plugins.tooltip.borderWidth = 1;
    Chart.defaults.hover.mode = "index";
    Chart.defaults.hover.intersect = false;
  }
  initTheme();
  initFilters();
  document.querySelectorAll("canvas").forEach((canvas) => {
    const card = canvas.closest(".chart-card");
    const title = card ? card.querySelector(".chart-title")?.textContent?.trim() : "";
    const sub = card ? card.querySelector(".chart-sub")?.textContent?.trim() : "";
    canvas.setAttribute("role", "img");
    canvas.setAttribute("aria-label", title ? (sub ? `${title}. ${sub}` : title) : (canvas.id || "Gráfico"));
  });
  renderGovernance();
  renderHeroRidge();
  buildDocIndex();
  bindEvents();
  applyAll();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bootstrap);
} else {
  bootstrap();
}
</script>
</body>
</html>
"""

    dashboard_version = "3.0"
    governance_payload = {
        "validation_status": validation_summary.get("overall_status", "N/A"),
        "validation_confidence": validation_summary.get("confidence_level", "N/A"),
        "publish_state": validation_summary.get("release_readiness", {}).get("publish_state", "N/A"),
        "decision_state": validation_summary.get("release_readiness", {}).get("decision_state", "N/A"),
        "version": dashboard_version,
    }
    html = (
        html_template.replace("__CHARTJS_SCRIPT__", chartjs_script)
        .replace("__FONTFACE__", fontface_css)
        .replace("__PAYLOAD__", json.dumps(payload, ensure_ascii=False))
        .replace("__KPI_STATIC__", json.dumps(kpi_static, ensure_ascii=False))
        .replace("__EXEC_INSIGHTS__", json.dumps(executive_insights, ensure_ascii=False))
        .replace("__COVERAGE_START__", str(coverage_start))
        .replace("__COVERAGE_END__", str(coverage_end))
        .replace("__N_ZONAS__", str(len(zone_risk)))
        .replace("__N_SUBS__", str(len(substations)))
        .replace("__N_FEEDERS__", str(len(feeders)))
        .replace("__GOVERNANCE__", json.dumps(governance_payload, ensure_ascii=False))
    )

    out_official = paths.outputs_dashboard / "grid-electrification-command-center.html"
    legacy_duplicate = paths.outputs_dashboard / "dashboard_inteligencia_red_premium.html"
    if legacy_duplicate.exists():
        legacy_duplicate.unlink()

    out_official.write_text(html, encoding="utf-8")

    return str(out_official)


if __name__ == "__main__":
    path = build_dashboard()
    print(path)
