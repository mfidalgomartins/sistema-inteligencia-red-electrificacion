from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd


@dataclass(frozen=True)
class V2Paths:
    root: Path

    @property
    def data_raw(self) -> Path:
        return self.root / "data" / "raw"

    @property
    def data_processed(self) -> Path:
        return self.root / "data" / "processed"

    @property
    def outputs_charts(self) -> Path:
        return self.root / "outputs" / "charts"

    @property
    def outputs_dashboard(self) -> Path:
        return self.root / "outputs" / "dashboard"

    @property
    def outputs_reports(self) -> Path:
        return self.root / "outputs" / "reports"

    @property
    def docs(self) -> Path:
        return self.root / "docs"

    @property
    def sql(self) -> Path:
        return self.root / "sql"

    @property
    def db_v2(self) -> Path:
        return self.data_processed / "grid_analytics_sql_layer.duckdb"


def get_paths() -> V2Paths:
    return V2Paths(root=Path(__file__).resolve().parents[1])


def ensure_dirs(paths: V2Paths | None = None) -> V2Paths:
    p = paths or get_paths()
    for d in [
        p.data_raw,
        p.data_processed,
        p.outputs_charts,
        p.outputs_dashboard,
        p.outputs_reports,
        p.docs,
    ]:
        d.mkdir(parents=True, exist_ok=True)
    return p


def connect_v2(paths: V2Paths | None = None) -> duckdb.DuckDBPyConnection:
    p = ensure_dirs(paths)
    return duckdb.connect(str(p.db_v2))


def write_df(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)


def pct(num: float, den: float) -> float:
    if den == 0:
        return 0.0
    return num / den


def minmax(series: pd.Series) -> pd.Series:
    smin = float(series.min())
    smax = float(series.max())
    if smin == smax:
        return pd.Series([50.0] * len(series), index=series.index)
    return 100.0 * (series - smin) / (smax - smin)


def chunked(iterable: Iterable, size: int):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
