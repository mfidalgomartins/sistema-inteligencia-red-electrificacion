"""Rutas del proyecto, conexión DuckDB y utilidades compartidas del pipeline."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd


@dataclass(frozen=True)
class ProjectPaths:
    root: Path

    @property
    def data_raw(self) -> Path:
        return self.root / "data" / "raw"

    @property
    def data_processed(self) -> Path:
        return self.root / "data" / "processed"

    @property
    def data_landing(self) -> Path:
        return self.root / "data" / "landing"

    @property
    def data_incremental(self) -> Path:
        return self.root / "data" / "incremental"

    @property
    def data_operational(self) -> Path:
        return self.root / "data" / "operational"

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
    def database(self) -> Path:
        return self.data_processed / "grid_analytics_sql_layer.duckdb"

    @property
    def operations_database(self) -> Path:
        return self.data_operational / "operations.duckdb"

    @property
    def incremental_database(self) -> Path:
        return self.data_operational / "incremental_analytics.duckdb"


def get_paths(root: Path | None = None) -> ProjectPaths:
    """Devuelve las rutas del proyecto o de una raíz inyectada para pruebas."""
    return ProjectPaths(root=(root or Path(__file__).resolve().parents[2]).resolve())


def project_relative(path: Path, paths: ProjectPaths | None = None) -> str:
    p = paths or get_paths()
    try:
        return path.resolve().relative_to(p.root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def ensure_dirs(paths: ProjectPaths | None = None) -> ProjectPaths:
    p = paths or get_paths()
    for d in [
        p.data_raw,
        p.data_processed,
        p.data_landing,
        p.data_incremental,
        p.data_operational,
        p.outputs_charts,
        p.outputs_dashboard,
        p.outputs_reports,
        p.docs,
    ]:
        d.mkdir(parents=True, exist_ok=True)
    return p


def connect_database(
    paths: ProjectPaths | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Abre la base DuckDB canónica."""
    p = ensure_dirs(paths)
    return duckdb.connect(str(p.database), read_only=read_only)


def write_df(df: pd.DataFrame, path: Path) -> Path:
    """Escribe un DataFrame sin índice en CSV o Parquet según la extensión."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)
    return path


def pct(num: float, den: float) -> float:
    if den == 0:
        return 0.0
    return num / den


def minmax(series: pd.Series) -> pd.Series:
    """Normaliza una serie a 0-100; una señal constante recibe valor neutro 50."""
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return pd.Series(0.0, index=series.index, dtype=float)
    smin = float(valid.min())
    smax = float(valid.max())
    if smin == smax:
        return pd.Series(50.0, index=series.index, dtype=float)
    return (100.0 * (numeric - smin) / (smax - smin)).fillna(0.0)


def chunked[T](iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """Agrupa un iterable en lotes de tamaño positivo."""
    if size <= 0:
        raise ValueError("size debe ser mayor que cero")

    batch: list[T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
