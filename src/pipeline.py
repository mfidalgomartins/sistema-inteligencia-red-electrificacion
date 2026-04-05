from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

from .config import CONFIG, Config
from .dashboard import build_dashboard
from .data_generation import generate_all_raw_data
from .modeling import build_model_outputs
from .notebook_builder import build_notebooks
from .reporting import write_reports
from .scenario_engine import run_scenario_engine
from .scoring import build_investment_priorities
from .sql_runner import run_sql_layer
from .visualization import generate_charts


def _ensure_dirs(config: Config) -> None:
    for path in [
        config.data_raw,
        config.data_processed,
        config.outputs_charts,
        config.outputs_dashboard,
        config.outputs_reports,
        config.root_dir / "docs",
        config.root_dir / "notebooks",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def run_pipeline(config: Config = CONFIG) -> Dict[str, str]:
    _ensure_dirs(config)

    raw = generate_all_raw_data(config)
    sql_outputs = run_sql_layer(config)

    model_outputs = build_model_outputs(
        mart_feeder_summary=sql_outputs["mart_feeder_summary"],
        mart_feeder_daily=sql_outputs["mart_feeder_daily"],
        config=config,
    )

    for name, df in model_outputs.items():
        df.to_csv(config.data_processed / f"{name}.csv", index=False)

    priorities = build_investment_priorities(
        feeder_features=model_outputs["feeder_features"],
        territories=raw["territories"],
        flexibility_assets=raw["flexibility_assets"],
        capex_catalog=raw["capex_catalog"],
    )
    priorities.to_csv(config.data_processed / "investment_priorities.csv", index=False)

    scenario_results, scenario_summary = run_scenario_engine(priorities)
    scenario_results.to_csv(config.data_processed / "scenario_results.csv", index=False)
    scenario_summary.to_csv(config.data_processed / "scenario_summary.csv", index=False)

    territory_kpis = sql_outputs["kpi_territorial_pressure"].copy()
    territory_kpis.to_csv(config.data_processed / "territory_kpis.csv", index=False)

    generate_charts(
        priorities=priorities,
        scenario_summary=scenario_summary,
        territory_monthly=sql_outputs["mart_territory_monthly"],
        config=config,
    )

    dashboard_path = build_dashboard(
        priorities=priorities,
        scenario_summary=scenario_summary,
        territory_monthly=sql_outputs["mart_territory_monthly"],
        kpi_network_overview=sql_outputs["kpi_network_overview"],
        config=config,
    )

    reports = write_reports(
        priorities=priorities,
        scenario_summary=scenario_summary,
        validation_checks=sql_outputs["validation_checks"],
        kpi_network_overview=sql_outputs["kpi_network_overview"],
        config=config,
    )

    build_notebooks(config)

    return {
        "dashboard": str(dashboard_path),
        "memo": str(reports["memo"]),
        "validation_report": str(reports["validation"]),
        "economic_assumptions": str(reports["assumptions"]),
        "priorities": str(config.data_processed / "investment_priorities.csv"),
        "scenario_summary": str(config.data_processed / "scenario_summary.csv"),
    }


if __name__ == "__main__":
    outputs = run_pipeline()
    for key, value in outputs.items():
        print(f"{key}: {value}")
