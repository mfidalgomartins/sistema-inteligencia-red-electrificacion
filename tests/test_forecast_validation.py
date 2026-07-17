import numpy as np
import pandas as pd

from grid_intelligence.common import ProjectPaths
from grid_intelligence.forecast_validation import (
    RollingBacktestConfig,
    build_uncertainty_outputs,
    classify_monitoring_status,
    rolling_origin_backtest,
    validate_forecast_artifacts,
)
from grid_intelligence.forecasting import _predict_one_step


def _history() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=260, freq="D")
    rows = []
    for zone_no, offset in ((1, 0.0), (2, 20.0)):
        signal = 100 + offset + 0.08 * np.arange(len(dates)) + 5 * np.sin(np.arange(len(dates)) * 2 * np.pi / 7)
        rows.extend(
            {"zona_id": f"Z00{zone_no}", "fecha": day, "demanda_total_mwh": value}
            for day, value in zip(dates, signal, strict=True)
        )
    return pd.DataFrame(rows)


def test_rolling_backtest_preserves_temporal_order_and_multiple_origins():
    result = rolling_origin_backtest(
        _history(),
        entity_col="zona_id",
        date_col="fecha",
        target_col="demanda_total_mwh",
        models=["naive", "seasonal_naive"],
        predictor=_predict_one_step,
        config=RollingBacktestConfig(n_folds=3, horizon=20, min_train=180),
    )

    assert result["fold"].nunique() == 3
    assert (pd.to_datetime(result["date"]) > pd.to_datetime(result["cutoff_date"])).all()
    assert result.groupby(["entity_id", "model", "fold"]).size().eq(20).all()


def test_conformal_intervals_are_bounded_and_measured_out_of_fold():
    backtest = rolling_origin_backtest(
        _history(),
        entity_col="zona_id",
        date_col="fecha",
        target_col="demanda_total_mwh",
        models=["naive", "seasonal_naive"],
        predictor=_predict_one_step,
        config=RollingBacktestConfig(n_folds=4, horizon=20, min_train=160),
    )
    outputs = build_uncertainty_outputs(backtest)
    intervals = outputs["forecast_uncertainty_intervals"]
    calibration = outputs["forecast_calibration_summary"]

    assert (intervals["lower_80"] <= intervals["pred"]).all()
    assert (intervals["pred"] <= intervals["upper_80"]).all()
    assert (intervals["lower_95"] <= intervals["lower_80"]).all()
    assert (intervals["upper_95"] >= intervals["upper_80"]).all()
    assert calibration["empirical_coverage"].between(0, 1).all()
    assert outputs["forecast_monitoring_status"].iloc[0]["monitoring_status"] in {
        "stable",
        "watch",
        "action_required",
    }


def test_monitoring_requires_material_and_exceptional_drift_for_action():
    assert classify_monitoring_status(1.60, 0.92, 1.20) == "watch"
    assert classify_monitoring_status(1.60, 0.92, 2.10) == "action_required"
    assert classify_monitoring_status(1.05, 0.84, 0.10) == "action_required"


def test_forecast_artifact_gate_detects_invalid_interval_nesting(tmp_path):
    paths = ProjectPaths(tmp_path)
    paths.data_processed.mkdir(parents=True)
    backtest = rolling_origin_backtest(
        _history(),
        entity_col="zona_id",
        date_col="fecha",
        target_col="demanda_total_mwh",
        models=["naive", "seasonal_naive"],
        predictor=_predict_one_step,
        config=RollingBacktestConfig(n_folds=4, horizon=20, min_train=160),
    )
    outputs = build_uncertainty_outputs(backtest)
    for name in (
        "forecast_uncertainty_intervals",
        "forecast_calibration_summary",
        "forecast_monitoring_status",
    ):
        outputs[name].to_csv(paths.data_processed / f"{name}.csv", index=False)

    assert validate_forecast_artifacts(paths) == []

    intervals_path = paths.data_processed / "forecast_uncertainty_intervals.csv"
    intervals = pd.read_csv(intervals_path)
    intervals.loc[0, "lower_95"] = intervals.loc[0, "lower_80"] + 1
    intervals.to_csv(intervals_path, index=False)

    assert "invalid_interval_nesting" in validate_forecast_artifacts(paths)
