from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.io as pio

from .config import CONFIG, Config


def _kpi_card(title: str, value: str, subtitle: str) -> str:
    return f"""
    <div class='kpi-card'>
      <div class='kpi-title'>{title}</div>
      <div class='kpi-value'>{value}</div>
      <div class='kpi-subtitle'>{subtitle}</div>
    </div>
    """


def build_dashboard(
    priorities: pd.DataFrame,
    scenario_summary: pd.DataFrame,
    territory_monthly: pd.DataFrame,
    kpi_network_overview: pd.DataFrame,
    config: Config = CONFIG,
) -> Path:
    config.outputs_dashboard.mkdir(parents=True, exist_ok=True)

    overview = kpi_network_overview.iloc[0]

    top_priority = priorities.nlargest(20, "priority_score").copy()
    fig_top = px.bar(
        top_priority.sort_values("priority_score"),
        x="priority_score",
        y="feeder_id",
        color="recommended_action",
        orientation="h",
        title="Top feeders por prioridad de intervención",
        labels={"priority_score": "Priority score", "feeder_id": "Feeder"},
    )

    fig_scatter = px.scatter(
        priorities,
        x="stress_score",
        y="resilience_score",
        size="estimated_capex_k_eur",
        color="priority_tier",
        hover_data=["feeder_id", "territory_id", "recommended_action", "priority_score"],
        title="Mapa de riesgo: stress vs resiliencia",
    )

    fig_scenario = px.bar(
        scenario_summary.sort_values("avg_priority_score", ascending=False),
        x="scenario",
        y=["total_capex_m_eur", "total_ens_mwh", "total_curtailment_mwh"],
        barmode="group",
        title="Comparativa de escenarios (CAPEX y riesgo operacional)",
    )

    monthly = (
        territory_monthly.groupby("month", as_index=False)
        .agg(net_demand_mwh=("net_demand_mwh", "sum"), curtailment_mwh=("curtailment_mwh", "sum"))
        .sort_values("month")
    )
    monthly["month"] = pd.to_datetime(monthly["month"]).dt.strftime("%Y-%m")
    fig_monthly = px.line(
        monthly,
        x="month",
        y=["net_demand_mwh", "curtailment_mwh"],
        markers=True,
        title="Evolución mensual de energía neta y curtailment",
    )

    action_mix = priorities.groupby(["recommended_action", "priority_tier"], as_index=False).size()
    fig_action = px.sunburst(
        action_mix,
        path=["recommended_action", "priority_tier"],
        values="size",
        title="Distribución de acciones recomendadas por nivel de prioridad",
    )

    top_table_cols = [
        "priority_rank",
        "feeder_id",
        "territory_id",
        "priority_tier",
        "priority_score",
        "recommended_action",
        "estimated_capex_k_eur",
        "expected_annual_benefit_k_eur",
    ]
    top_table = (
        priorities[top_table_cols]
        .head(30)
        .rename(
            columns={
                "priority_rank": "Rank",
                "feeder_id": "Feeder",
                "territory_id": "Territorio",
                "priority_tier": "Nivel",
                "priority_score": "Score",
                "recommended_action": "Acción",
                "estimated_capex_k_eur": "CAPEX estimado (kEUR)",
                "expected_annual_benefit_k_eur": "Beneficio anual estimado (kEUR)",
            }
        )
        .round(2)
        .to_html(index=False, classes="table", border=0)
    )

    fig_top_html = pio.to_html(fig_top, include_plotlyjs="inline", full_html=False)
    fig_scatter_html = pio.to_html(fig_scatter, include_plotlyjs=False, full_html=False)
    fig_scenario_html = pio.to_html(fig_scenario, include_plotlyjs=False, full_html=False)
    fig_monthly_html = pio.to_html(fig_monthly, include_plotlyjs=False, full_html=False)
    fig_action_html = pio.to_html(fig_action, include_plotlyjs=False, full_html=False)

    cards = "\n".join(
        [
            _kpi_card(
                "Feeders monitorizados",
                f"{int(overview['feeders'])}",
                "Cobertura completa del sistema sintético",
            ),
            _kpi_card(
                "Horas de congestión",
                f"{int(overview['total_congestion_hours']):,}",
                "Acumulado anual en toda la red",
            ),
            _kpi_card(
                "ENS total (MWh)",
                f"{overview['total_ens_mwh']:.1f}",
                "Impacto anual por continuidad de servicio",
            ),
            _kpi_card(
                "Incremento pico 2030 (MW)",
                f"{overview['incremental_peak_2030_mw']:.1f}",
                "Presión esperada por electrificación",
            ),
        ]
    )

    html = f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Dashboard | Inteligencia de Red y Flexibilidad</title>
  <style>
    body {{
      margin: 0;
      font-family: "Source Sans 3", "Trebuchet MS", sans-serif;
      background: radial-gradient(circle at top right, #edf7ff 0%, #f8fafc 40%, #f3f4f6 100%);
      color: #0f172a;
    }}
    .container {{ max-width: 1400px; margin: 0 auto; padding: 28px 24px 44px; }}
    h1 {{ margin: 0; font-size: 2rem; letter-spacing: 0.2px; }}
    .subtitle {{ color: #334155; margin-top: 8px; margin-bottom: 22px; }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 18px; }}
    .kpi-card {{ background: #ffffff; border-radius: 12px; padding: 14px; box-shadow: 0 4px 14px rgba(15, 23, 42, 0.08); }}
    .kpi-title {{ font-size: 0.86rem; color: #334155; }}
    .kpi-value {{ font-size: 1.55rem; font-weight: 700; margin: 6px 0; }}
    .kpi-subtitle {{ font-size: 0.76rem; color: #64748b; }}
    .panel {{ background: #ffffff; border-radius: 12px; padding: 10px 14px; margin-top: 14px; box-shadow: 0 4px 14px rgba(15, 23, 42, 0.08); }}
    .table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; }}
    .table thead tr {{ background: #0f172a; color: #f8fafc; }}
    .table th, .table td {{ padding: 9px 8px; border-bottom: 1px solid #e5e7eb; text-align: left; }}
    @media (max-width: 1100px) {{
      .kpi-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 700px) {{
      .kpi-grid {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 1.55rem; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Sistema de Inteligencia de Red, Flexibilidad, Resiliencia e Inversiones</h1>
    <div class="subtitle">Dashboard ejecutivo autocontenido para planificación de red y electrificación territorial.</div>

    <div class="kpi-grid">{cards}</div>

    <div class="panel">{fig_top_html}</div>
    <div class="panel">{fig_scatter_html}</div>
    <div class="panel">{fig_scenario_html}</div>
    <div class="panel">{fig_monthly_html}</div>
    <div class="panel">{fig_action_html}</div>

    <div class="panel">
      <h3>Top 30 prioridades de inversión</h3>
      {top_table}
    </div>
  </div>
</body>
</html>
"""

    # Legacy mode no longer writes a second dashboard artifact.
    # Keep a single official HTML in outputs/dashboard/.
    out_path = config.outputs_dashboard / "dashboard_inteligencia_red.html"
    if not out_path.exists():
        out_path.write_text(
            "<!doctype html><html><body><p>Dashboard oficial não encontrado. Execute: python -m src</p></body></html>",
            encoding="utf-8",
        )
    return out_path
