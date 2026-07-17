from pathlib import Path

import pandas as pd

from grid_intelligence.anomaly_detection import _future_window_max
from grid_intelligence.scenario_engine import _localized_factor

ROOT = Path(__file__).resolve().parents[1]


def test_future_window_excludes_current_observation():
    series = pd.Series([0, 1, 0, 0])

    result = _future_window_max(series, window=2)

    assert result.tolist() == [1.0, 0.0, 0.0, 0.0]


def test_scenario_factor_varies_with_local_exposure():
    exposure = pd.Series([0.0, 1.0])

    adverse = _localized_factor(1.20, exposure)
    beneficial = _localized_factor(0.80, exposure)

    assert adverse.iloc[1] > adverse.iloc[0] > 1.0
    assert beneficial.iloc[1] < beneficial.iloc[0] < 1.0


def test_sql_contracts_preserve_correct_aggregation_grains():
    sql_05 = (ROOT / "sql" / "05_integrated_flexibility_assets.sql").read_text(encoding="utf-8")
    sql_06 = (ROOT / "sql" / "06_analytical_mart_node_hour.sql").read_text(encoding="utf-8")
    sql_07 = (ROOT / "sql" / "07_analytical_mart_zone_day.sql").read_text(encoding="utf-8")
    sql_09 = (ROOT / "sql" / "09_kpi_queries.sql").read_text(encoding="utf-8")

    assert "substation_capacity AS" in sql_05
    assert "nb.demanda_mw AS demanda_critica_mw" in sql_06
    assert "MAX(zh.demanda_total_mwh) AS carga_punta_mw" in sql_07
    assert "SUM(zh.flag_congestion) AS horas_congestion" in sql_07
    assert "COUNT(DISTINCT CASE WHEN nh.flag_congestion THEN nh.timestamp END)" in sql_09
