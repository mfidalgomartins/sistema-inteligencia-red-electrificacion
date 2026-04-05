from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    root_dir: Path = Path(__file__).resolve().parents[1]
    random_seed: int = 42
    start_date: str = "2025-01-01"
    end_date: str = "2025-12-31 23:00:00"
    n_territories: int = 24
    n_substations: int = 38
    n_feeders: int = 160

    @property
    def data_raw(self) -> Path:
        return self.root_dir / "data" / "raw"

    @property
    def data_processed(self) -> Path:
        return self.root_dir / "data" / "processed"

    @property
    def outputs_charts(self) -> Path:
        return self.root_dir / "outputs" / "charts"

    @property
    def outputs_dashboard(self) -> Path:
        return self.root_dir / "outputs" / "dashboard"

    @property
    def outputs_reports(self) -> Path:
        return self.root_dir / "outputs" / "reports"

    @property
    def sql_dir(self) -> Path:
        return self.root_dir / "sql"

    @property
    def db_path(self) -> Path:
        return self.data_processed / "grid_analytics.duckdb"


CONFIG = Config()
