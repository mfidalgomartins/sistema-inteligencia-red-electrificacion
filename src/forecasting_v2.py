from __future__ import annotations

from dataclasses import dataclass
from textwrap import dedent

import numpy as np
import pandas as pd

from .common_v2 import connect_v2, ensure_dirs, get_paths, write_df


@dataclass(frozen=True)
class ForecastTask:
    name: str
    entity_col: str
    date_col: str
    target_col: str
    seasonal_period: int
    moving_window: int
    horizon: int


def _smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    den = (np.abs(y_true) + np.abs(y_pred)) / 2.0
    den = np.where(den == 0, 1e-9, den)
    return float(np.mean(np.abs(y_true - y_pred) / den) * 100.0)


def _metrics(df: pd.DataFrame) -> dict[str, float]:
    y = df["actual"].to_numpy(dtype=float)
    p = df["pred"].to_numpy(dtype=float)
    err = p - y
    return {
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(np.sqrt(np.mean(err**2))),
        "smape": _smape(y, p),
        "bias": float(np.mean(err)),
    }


def _predict_one_step(history: np.ndarray, model: str, seasonal_period: int, moving_window: int) -> float:
    if len(history) == 0:
        return 0.0

    if model == "naive":
        return float(history[-1])

    if model == "seasonal_naive":
        if len(history) > seasonal_period:
            return float(history[-seasonal_period])
        return float(history[-1])

    if model == "moving_average":
        win = min(moving_window, len(history))
        return float(np.mean(history[-win:]))

    if model == "linear_trend":
        if len(history) < 2:
            return float(history[-1])
        y = history.astype(float)
        window = min(180, len(y))
        y_w = y[-window:]
        x_w = np.arange(window, dtype=float)
        x_mean = float(x_w.mean())
        y_mean = float(y_w.mean())
        denom = float(np.sum((x_w - x_mean) ** 2))
        if denom <= 1e-12:
            return float(y_w[-1])
        slope = float(np.sum((x_w - x_mean) * (y_w - y_mean)) / denom)
        intercept = y_mean - slope * x_mean
        return float(intercept + slope * window)

    if model == "exp_smoothing":
        alpha = 0.35
        level = float(history[0])
        for value in history[1:]:
            level = alpha * float(value) + (1 - alpha) * level
        return level

    raise ValueError(f"Modelo no soportado: {model}")


def _run_backtest(
    df: pd.DataFrame,
    task: ForecastTask,
    models: list[str],
) -> pd.DataFrame:
    rows: list[dict] = []

    for entity, group in df.groupby(task.entity_col, sort=False):
        g = group.sort_values(task.date_col).copy()
        if len(g) < task.horizon + 120:
            continue

        split_idx = len(g) - task.horizon
        train_y = g.iloc[:split_idx][task.target_col].to_numpy(dtype=float)
        test = g.iloc[split_idx:].copy()

        for model in models:
            history = train_y.copy()
            preds = []
            for y_true in test[task.target_col].to_numpy(dtype=float):
                y_hat = _predict_one_step(history, model, task.seasonal_period, task.moving_window)
                preds.append(y_hat)
                history = np.append(history, y_true)

            pred_df = test[[task.date_col]].copy()
            pred_df[task.entity_col] = entity
            pred_df["task"] = task.name
            pred_df["model"] = model
            pred_df["actual"] = test[task.target_col].to_numpy(dtype=float)
            pred_df["pred"] = np.array(preds, dtype=float)
            pred_df["abs_error"] = np.abs(pred_df["pred"] - pred_df["actual"])
            pred_df["error"] = pred_df["pred"] - pred_df["actual"]

            rows.extend(pred_df.to_dict(orient="records"))

    return pd.DataFrame(rows)


def run_forecasting_v2() -> dict[str, pd.DataFrame]:
    paths = ensure_dirs(get_paths())
    conn = connect_v2(paths)

    zone_day = conn.execute(
        dedent(
            """
            SELECT
                z.fecha,
                z.zona_id,
                z.zona_nombre,
                z.tipo_zona,
                z.region_operativa,
                z.demanda_total_mwh,
                z.demanda_ev_mwh,
                z.demanda_industrial_mwh,
                AVG(n.carga_relativa) AS carga_relativa_media
            FROM mart_zone_day_operational z
            LEFT JOIN mart_node_hour_operational_state n
                ON z.zona_id = n.zona_id
               AND z.fecha = CAST(n.timestamp AS DATE)
            GROUP BY
                z.fecha,
                z.zona_id,
                z.zona_nombre,
                z.tipo_zona,
                z.region_operativa,
                z.demanda_total_mwh,
                z.demanda_ev_mwh,
                z.demanda_industrial_mwh
            """
        )
    ).df()

    sub_day = conn.execute(
        dedent(
            """
            SELECT
                CAST(n.timestamp AS DATE) AS fecha,
                n.subestacion_id,
                s.zona_id,
                z.tipo_zona,
                SUM(n.demanda_mw) AS demanda_subestacion_mwh
            FROM mart_node_hour_operational_state n
            LEFT JOIN stg_subestaciones s
                ON n.subestacion_id = s.subestacion_id
            LEFT JOIN stg_zonas_red z
                ON s.zona_id = z.zona_id
            GROUP BY
                CAST(n.timestamp AS DATE),
                n.subestacion_id,
                s.zona_id,
                z.tipo_zona
            """
        )
    ).df()

    models = ["naive", "seasonal_naive", "moving_average", "linear_trend", "exp_smoothing"]

    tasks = [
        ForecastTask(
            name="demanda_zona",
            entity_col="zona_id",
            date_col="fecha",
            target_col="demanda_total_mwh",
            seasonal_period=7,
            moving_window=7,
            horizon=90,
        ),
        ForecastTask(
            name="demanda_subestacion",
            entity_col="subestacion_id",
            date_col="fecha",
            target_col="demanda_subestacion_mwh",
            seasonal_period=7,
            moving_window=7,
            horizon=90,
        ),
        ForecastTask(
            name="carga_relativa_zona",
            entity_col="zona_id",
            date_col="fecha",
            target_col="carga_relativa_media",
            seasonal_period=7,
            moving_window=7,
            horizon=90,
        ),
        ForecastTask(
            name="demanda_ev_zona",
            entity_col="zona_id",
            date_col="fecha",
            target_col="demanda_ev_mwh",
            seasonal_period=7,
            moving_window=7,
            horizon=90,
        ),
        ForecastTask(
            name="demanda_industrial_zona",
            entity_col="zona_id",
            date_col="fecha",
            target_col="demanda_industrial_mwh",
            seasonal_period=7,
            moving_window=7,
            horizon=90,
        ),
    ]

    all_forecasts: list[pd.DataFrame] = []

    zone_day_map = {
        "demanda_zona": zone_day,
        "carga_relativa_zona": zone_day,
        "demanda_ev_zona": zone_day,
        "demanda_industrial_zona": zone_day,
    }

    for task in tasks:
        src = zone_day_map.get(task.name, sub_day)
        fdf = _run_backtest(src, task, models)
        all_forecasts.append(fdf)

    forecast_all = pd.concat(all_forecasts, ignore_index=True)

    # Benchmark por tarea/modelo.
    benchmark_rows = []
    for (task, model), group in forecast_all.groupby(["task", "model"], sort=False):
        m = _metrics(group)
        benchmark_rows.append(
            {
                "task": task,
                "model": model,
                "mae": m["mae"],
                "rmse": m["rmse"],
                "smape": m["smape"],
                "bias": m["bias"],
                "n_obs": len(group),
            }
        )
    benchmark = pd.DataFrame(benchmark_rows).sort_values(["task", "mae"], ascending=[True, True])

    # Errores por zona y tipo de zona para demanda_zona.
    dz = forecast_all[forecast_all["task"] == "demanda_zona"].merge(
        zone_day[["fecha", "zona_id", "tipo_zona", "demanda_ev_mwh", "demanda_industrial_mwh", "demanda_total_mwh"]],
        on=["fecha", "zona_id"],
        how="left",
    )

    best_model_dz = (
        benchmark[benchmark["task"] == "demanda_zona"]
        .sort_values("mae")
        .iloc[0]["model"]
    )
    dz_best = dz[dz["model"] == best_model_dz].copy()

    error_by_zone = (
        dz_best.groupby("zona_id", as_index=False)
        .agg(
            mae=("abs_error", "mean"),
            rmse=("error", lambda x: float(np.sqrt(np.mean(np.square(x))))),
            bias=("error", "mean"),
            smape=("actual", lambda y: _smape(y.to_numpy(), dz_best.loc[y.index, "pred"].to_numpy())),
            demanda_media=("actual", "mean"),
        )
        .sort_values("mae", ascending=False)
    )
    error_by_zone["nmae"] = error_by_zone["mae"] / error_by_zone["demanda_media"].replace(0, np.nan)
    error_by_zone["nmae"] = error_by_zone["nmae"].fillna(0.0)

    error_by_tipo = (
        dz_best.groupby("tipo_zona", as_index=False)
        .agg(
            mae=("abs_error", "mean"),
            rmse=("error", lambda x: float(np.sqrt(np.mean(np.square(x))))),
            bias=("error", "mean"),
            n_obs=("error", "size"),
        )
        .sort_values("mae", ascending=False)
    )

    # Error en horas punta: forecast horario de demanda por zona.
    zone_hour = conn.execute(
        dedent(
            """
            SELECT
                n.timestamp,
                n.zona_id,
                z.tipo_zona,
                SUM(n.demanda_mw) AS demanda_zona_mw,
                MAX(CASE WHEN n.hora_punta_flag THEN 1 ELSE 0 END) AS hora_punta_flag
            FROM mart_node_hour_operational_state n
            LEFT JOIN stg_zonas_red z
                ON n.zona_id = z.zona_id
            GROUP BY
                n.timestamp,
                n.zona_id,
                z.tipo_zona
            """
        )
    ).df()

    hourly_task = ForecastTask(
        name="demanda_zona_horaria",
        entity_col="zona_id",
        date_col="timestamp",
        target_col="demanda_zona_mw",
        seasonal_period=24,
        moving_window=24,
        horizon=24 * 21,
    )
    hourly_forecast = _run_backtest(zone_hour, hourly_task, models)
    hourly_forecast = hourly_forecast.merge(
        zone_hour[["timestamp", "zona_id", "hora_punta_flag", "tipo_zona"]],
        on=["timestamp", "zona_id"],
        how="left",
    )

    peak_rows = []
    for model, group in hourly_forecast.groupby("model", sort=False):
        peak = group[group["hora_punta_flag"] == 1]
        all_h = group
        m_peak = _metrics(peak) if len(peak) > 0 else {"mae": np.nan, "rmse": np.nan, "smape": np.nan, "bias": np.nan}
        m_all = _metrics(all_h)
        peak_rows.append(
            {
                "model": model,
                "mae_peak_hours": m_peak["mae"],
                "rmse_peak_hours": m_peak["rmse"],
                "smape_peak_hours": m_peak["smape"],
                "bias_peak_hours": m_peak["bias"],
                "mae_all_hours": m_all["mae"],
                "rmse_all_hours": m_all["rmse"],
            }
        )
    error_peak = pd.DataFrame(peak_rows).sort_values("mae_peak_hours")

    # Cómo EV e industrial afectan previsibilidad.
    predictability = (
        dz_best.groupby("zona_id", as_index=False)
        .agg(
            mae=("abs_error", "mean"),
            presion_ev=("demanda_ev_mwh", "mean"),
            presion_industrial=("demanda_industrial_mwh", "mean"),
            demanda_media=("demanda_total_mwh", "mean"),
        )
    )
    predictability["ratio_ev"] = predictability["presion_ev"] / predictability["demanda_media"].replace(0, np.nan)
    predictability["ratio_industrial"] = predictability["presion_industrial"] / predictability["demanda_media"].replace(0, np.nan)
    predictability["ratio_nueva_demanda"] = (predictability["presion_ev"] + predictability["presion_industrial"]) / predictability[
        "demanda_media"
    ].replace(0, np.nan)
    predictability["nmae"] = predictability["mae"] / predictability["demanda_media"].replace(0, np.nan)
    predictability = predictability.fillna(0.0)

    # Umbral comparable entre zonas: error absoluto normalizado por demanda media.
    threshold = 0.035
    predictability["decision_forecast"] = np.where(
        predictability["nmae"] <= threshold,
        "forecast_suficiente_para_diferir_capex",
        "forecast_insuficiente_requiere_refuerzo_o_flex",
    )

    write_df(forecast_all, paths.data_processed / "forecast_actual_vs_pred.csv")
    write_df(benchmark, paths.data_processed / "forecast_model_benchmark.csv")
    write_df(error_by_zone, paths.data_processed / "forecast_error_by_zone.csv")
    write_df(error_by_tipo, paths.data_processed / "forecast_error_by_tipo_zona.csv")
    write_df(error_peak, paths.data_processed / "forecast_error_peak_hours.csv")
    write_df(predictability, paths.data_processed / "forecast_predictability_pressure.csv")

    conn.close()

    return {
        "forecast_actual_vs_pred": forecast_all,
        "forecast_model_benchmark": benchmark,
        "forecast_error_by_zone": error_by_zone,
        "forecast_error_by_tipo_zona": error_by_tipo,
        "forecast_error_peak_hours": error_peak,
        "forecast_predictability_pressure": predictability,
    }


if __name__ == "__main__":
    result = run_forecasting_v2()
    for name, df in result.items():
        print(name, len(df))
