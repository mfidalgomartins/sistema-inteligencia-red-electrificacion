"""Backtesting rolling-origin e intervalos conformales para demanda zonal."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .common import ProjectPaths, write_df

Predictor = Callable[[np.ndarray, str, int, int], float]
FORECAST_ARTIFACTS = {
    "calibration": "forecast_calibration_summary.csv",
    "intervals": "forecast_uncertainty_intervals.csv",
    "monitoring": "forecast_monitoring_status.csv",
}


def classify_monitoring_status(drift_ratio: float, coverage_95: float, drift_zscore: float) -> str:
    """Clasifica degradación combinando magnitud, variabilidad histórica y cobertura."""
    if coverage_95 < 0.85 or (drift_ratio > 1.50 and drift_zscore > 2.0):
        return "action_required"
    if coverage_95 < 0.90 or drift_ratio > 1.25 or drift_zscore > 1.0:
        return "watch"
    return "stable"


@dataclass(frozen=True)
class RollingBacktestConfig:
    n_folds: int = 4
    horizon: int = 30
    min_train: int = 365
    seasonal_period: int = 7
    moving_window: int = 7
    interval_levels: tuple[float, ...] = (0.80, 0.95)

    def __post_init__(self) -> None:
        if self.n_folds < 2:
            raise ValueError("n_folds debe ser al menos 2 para separar calibración y evaluación")
        if self.horizon <= 0 or self.min_train <= 0:
            raise ValueError("horizon y min_train deben ser positivos")
        if any(level <= 0 or level >= 1 for level in self.interval_levels):
            raise ValueError("Los niveles de intervalo deben estar entre 0 y 1")


def rolling_origin_backtest(
    dataframe: pd.DataFrame,
    *,
    entity_col: str,
    date_col: str,
    target_col: str,
    models: list[str],
    predictor: Predictor,
    config: RollingBacktestConfig,
) -> pd.DataFrame:
    """Evalúa pronósticos one-step-ahead en orígenes temporales consecutivos."""
    required = {entity_col, date_col, target_col}
    missing = sorted(required.difference(dataframe.columns))
    if missing:
        raise ValueError(f"Columnas ausentes para backtest: {', '.join(missing)}")
    if not models:
        raise ValueError("Debe indicarse al menos un modelo")

    rows: list[dict] = []
    for entity, group in dataframe.groupby(entity_col, sort=True):
        ordered = group.sort_values(date_col).reset_index(drop=True)
        max_folds = min(config.n_folds, (len(ordered) - config.min_train) // config.horizon)
        if max_folds < 2:
            continue
        first_test_start = len(ordered) - max_folds * config.horizon
        for fold_index in range(max_folds):
            test_start = first_test_start + fold_index * config.horizon
            test_end = test_start + config.horizon
            train = ordered.iloc[:test_start]
            test = ordered.iloc[test_start:test_end]
            cutoff = train.iloc[-1][date_col]
            for model in models:
                history = train[target_col].to_numpy(dtype=float)
                predictions: list[float] = []
                for actual in test[target_col].to_numpy(dtype=float):
                    prediction = predictor(history, model, config.seasonal_period, config.moving_window)
                    predictions.append(prediction)
                    history = np.append(history, actual)
                for row_index, (_, test_row) in enumerate(test.iterrows()):
                    actual = float(test_row[target_col])
                    prediction = float(predictions[row_index])
                    rows.append(
                        {
                            "entity_id": str(entity),
                            "date": test_row[date_col],
                            "cutoff_date": cutoff,
                            "fold": fold_index + 1,
                            "model": model,
                            "actual": actual,
                            "pred": prediction,
                            "error": prediction - actual,
                            "abs_error": abs(prediction - actual),
                        }
                    )
    if not rows:
        raise ValueError("No hay historia suficiente para ejecutar backtesting rolling-origin")
    return pd.DataFrame(rows).sort_values(["model", "entity_id", "fold", "date"]).reset_index(drop=True)


def _conformal_quantile(residuals: pd.Series, level: float) -> float:
    values = residuals.dropna().to_numpy(dtype=float)
    if len(values) == 0:
        raise ValueError("No hay residuos de calibración")
    adjusted_level = min(1.0, np.ceil((len(values) + 1) * level) / len(values))
    return float(np.quantile(values, adjusted_level, method="higher"))


def build_uncertainty_outputs(
    backtest: pd.DataFrame,
    *,
    interval_levels: tuple[float, ...] = (0.80, 0.95),
) -> dict[str, pd.DataFrame]:
    """Selecciona modelo sin fuga temporal y calibra intervalos sobre folds previos."""
    model_selection = (
        backtest.groupby("model", as_index=False)
        .agg(
            mae=("abs_error", "mean"),
            rmse=("error", lambda values: float(np.sqrt(np.mean(np.square(values))))),
            bias=("error", "mean"),
            n_obs=("error", "size"),
            n_folds=("fold", "nunique"),
        )
        .sort_values(["mae", "rmse", "model"])
        .reset_index(drop=True)
    )
    model_selection.insert(0, "model_rank", np.arange(1, len(model_selection) + 1))
    best_model = str(model_selection.iloc[0]["model"])
    selected = backtest[backtest["model"] == best_model].copy()
    latest_fold = int(selected["fold"].max())
    calibration = selected[selected["fold"] < latest_fold]
    evaluation = selected[selected["fold"] == latest_fold].copy()
    if calibration.empty or evaluation.empty:
        raise ValueError("Se requieren folds separados para calibración y evaluación")

    global_quantiles = {level: _conformal_quantile(calibration["abs_error"], level) for level in interval_levels}
    for level in interval_levels:
        suffix = str(int(level * 100))
        global_quantile = global_quantiles[level]
        entity_quantiles = calibration.groupby("entity_id")["abs_error"].apply(
            lambda values, interval_level=level, fallback=global_quantile: (
                _conformal_quantile(values, interval_level) if len(values) >= 30 else fallback
            )
        )
        evaluation[f"interval_radius_{suffix}"] = evaluation["entity_id"].map(entity_quantiles).fillna(global_quantile)
        evaluation[f"lower_{suffix}"] = (evaluation["pred"] - evaluation[f"interval_radius_{suffix}"]).clip(lower=0.0)
        evaluation[f"upper_{suffix}"] = evaluation["pred"] + evaluation[f"interval_radius_{suffix}"]
        evaluation[f"covered_{suffix}"] = evaluation["actual"].between(
            evaluation[f"lower_{suffix}"], evaluation[f"upper_{suffix}"]
        )

    calibration_rows = []
    for level in interval_levels:
        suffix = str(int(level * 100))
        calibration_rows.append(
            {
                "model": best_model,
                "interval_level": level,
                "nominal_coverage": level,
                "empirical_coverage": float(evaluation[f"covered_{suffix}"].mean()),
                "mean_interval_width": float((evaluation[f"upper_{suffix}"] - evaluation[f"lower_{suffix}"]).mean()),
                "n_calibration": len(calibration),
                "n_evaluation": len(evaluation),
                "evaluation_fold": latest_fold,
            }
        )
    calibration_summary = pd.DataFrame(calibration_rows)

    historical_mae = float(calibration["abs_error"].mean())
    latest_mae = float(evaluation["abs_error"].mean())
    drift_ratio = latest_mae / historical_mae if historical_mae > 0 else 1.0
    fold_mae = calibration.groupby("fold")["abs_error"].mean()
    historical_fold_mae_std = float(fold_mae.std(ddof=0))
    drift_zscore = (latest_mae - historical_mae) / historical_fold_mae_std if historical_fold_mae_std > 0 else 0.0
    coverage_95_rows = calibration_summary[calibration_summary["interval_level"] == 0.95]
    coverage_95 = float(coverage_95_rows.iloc[0]["empirical_coverage"]) if not coverage_95_rows.empty else np.nan
    status = classify_monitoring_status(drift_ratio, coverage_95, drift_zscore)
    monitoring = pd.DataFrame(
        [
            {
                "best_model": best_model,
                "latest_fold": latest_fold,
                "historical_mae": historical_mae,
                "latest_mae": latest_mae,
                "mae_drift_ratio": drift_ratio,
                "historical_fold_mae_std": historical_fold_mae_std,
                "mae_drift_zscore": drift_zscore,
                "coverage_95": coverage_95,
                "monitoring_status": status,
                "n_entities": int(evaluation["entity_id"].nunique()),
                "n_evaluation": len(evaluation),
                "evaluation_start": evaluation["date"].min(),
                "evaluation_end": evaluation["date"].max(),
            }
        ]
    )
    return {
        "forecast_rolling_backtest": backtest,
        "forecast_model_selection_rolling": model_selection,
        "forecast_uncertainty_intervals": evaluation.reset_index(drop=True),
        "forecast_calibration_summary": calibration_summary,
        "forecast_monitoring_status": monitoring,
    }


def validate_forecast_artifacts(paths: ProjectPaths) -> list[str]:
    """Comprueba esquema, anidación, cobertura y estado de monitorización."""
    errors: list[str] = []
    artifact_paths = {name: paths.data_processed / file_name for name, file_name in FORECAST_ARTIFACTS.items()}
    for name, path in artifact_paths.items():
        if not path.exists():
            errors.append(f"missing:{name}")
    if errors:
        return errors

    try:
        calibration = pd.read_csv(artifact_paths["calibration"])
        intervals = pd.read_csv(artifact_paths["intervals"])
        monitoring = pd.read_csv(artifact_paths["monitoring"])
    except (OSError, UnicodeError, pd.errors.ParserError):
        return ["unreadable_artifact"]

    required_calibration = {
        "interval_level",
        "nominal_coverage",
        "empirical_coverage",
        "mean_interval_width",
    }
    required_intervals = {
        "actual",
        "pred",
        "lower_80",
        "upper_80",
        "covered_80",
        "lower_95",
        "upper_95",
        "covered_95",
    }
    required_monitoring = {
        "mae_drift_ratio",
        "mae_drift_zscore",
        "coverage_95",
        "monitoring_status",
        "n_entities",
        "n_evaluation",
    }
    for name, frame, required in (
        ("calibration", calibration, required_calibration),
        ("intervals", intervals, required_intervals),
        ("monitoring", monitoring, required_monitoring),
    ):
        missing = sorted(required.difference(frame.columns))
        if missing:
            errors.append(f"missing_columns:{name}:{','.join(missing)}")
        if frame.empty:
            errors.append(f"empty:{name}")
    if errors:
        return errors

    levels = set(pd.to_numeric(calibration["interval_level"], errors="coerce").round(2).dropna())
    if levels != {0.8, 0.95}:
        errors.append(f"invalid_interval_levels:{sorted(levels)}")
    bounded_columns = ["nominal_coverage", "empirical_coverage"]
    if any(not pd.to_numeric(calibration[column], errors="coerce").between(0, 1).all() for column in bounded_columns):
        errors.append("coverage_out_of_bounds")
    if not (pd.to_numeric(calibration["mean_interval_width"], errors="coerce") >= 0).all():
        errors.append("negative_interval_width")

    numeric_interval_columns = ["actual", "pred", "lower_80", "upper_80", "lower_95", "upper_95"]
    numeric_intervals = intervals[numeric_interval_columns].apply(pd.to_numeric, errors="coerce")
    if numeric_intervals.isna().any().any():
        errors.append("non_numeric_interval_value")
    else:
        valid_order = (
            (numeric_intervals["lower_80"] <= numeric_intervals["pred"])
            & (numeric_intervals["pred"] <= numeric_intervals["upper_80"])
            & (numeric_intervals["lower_95"] <= numeric_intervals["lower_80"])
            & (numeric_intervals["upper_80"] <= numeric_intervals["upper_95"])
        )
        if not valid_order.all():
            errors.append("invalid_interval_nesting")
        for suffix in ("80", "95"):
            expected = numeric_intervals["actual"].between(
                numeric_intervals[f"lower_{suffix}"], numeric_intervals[f"upper_{suffix}"]
            )
            observed = intervals[f"covered_{suffix}"].astype(str).str.lower().map({"true": True, "false": False})
            if observed.isna().any() or not observed.eq(expected).all():
                errors.append(f"invalid_coverage_flag:{suffix}")

    if len(monitoring) != 1:
        errors.append(f"invalid_monitoring_rows:{len(monitoring)}")
        return errors
    status = str(monitoring.iloc[0]["monitoring_status"])
    if status not in {"stable", "watch", "action_required"}:
        errors.append(f"invalid_monitoring_status:{status}")
    drift = pd.to_numeric(pd.Series([monitoring.iloc[0]["mae_drift_ratio"]]), errors="coerce").iloc[0]
    coverage_95 = pd.to_numeric(pd.Series([monitoring.iloc[0]["coverage_95"]]), errors="coerce").iloc[0]
    if pd.isna(drift) or drift < 0 or pd.isna(coverage_95) or not 0 <= coverage_95 <= 1:
        errors.append("invalid_monitoring_metrics")
        return errors
    drift_zscore = pd.to_numeric(pd.Series([monitoring.iloc[0]["mae_drift_zscore"]]), errors="coerce").iloc[0]
    if pd.isna(drift_zscore):
        errors.append("invalid_monitoring_zscore")
        return errors
    expected_status = classify_monitoring_status(float(drift), float(coverage_95), float(drift_zscore))
    if status != expected_status:
        errors.append(f"monitoring_status_mismatch:{status}!={expected_status}")

    calibration_95 = calibration[pd.to_numeric(calibration["interval_level"], errors="coerce").round(2) == 0.95]
    if not calibration_95.empty:
        empirical_95 = float(calibration_95.iloc[0]["empirical_coverage"])
        if not np.isclose(empirical_95, coverage_95, rtol=0, atol=1e-9):
            errors.append("monitoring_coverage_mismatch")
    return errors


def run_continuous_forecast_validation(
    zone_day: pd.DataFrame,
    models: list[str],
    predictor: Predictor,
    paths: ProjectPaths,
    *,
    config: RollingBacktestConfig | None = None,
) -> dict[str, pd.DataFrame]:
    """Ejecuta y publica la validación temporal principal del sistema."""
    effective_config = config or RollingBacktestConfig()
    backtest = rolling_origin_backtest(
        zone_day,
        entity_col="zona_id",
        date_col="fecha",
        target_col="demanda_total_mwh",
        models=models,
        predictor=predictor,
        config=effective_config,
    )
    outputs = build_uncertainty_outputs(backtest, interval_levels=effective_config.interval_levels)
    for name, dataframe in outputs.items():
        write_df(dataframe, paths.data_processed / f"{name}.csv")
    return outputs
