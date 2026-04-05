from __future__ import annotations

from pathlib import Path
from typing import Dict

import duckdb
import pandas as pd

from .config import CONFIG, Config


SQL_SEQUENCE = [
    "legacy/00_load_raw.sql",
    "legacy/10_staging.sql",
    "legacy/20_marts.sql",
    "legacy/30_kpis.sql",
    "legacy/40_validations.sql",
]


def run_sql_layer(config: Config = CONFIG) -> Dict[str, pd.DataFrame]:
    config.data_processed.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(config.db_path))

    format_context = {
        "raw_path": str(config.data_raw).replace("'", "''"),
    }

    for sql_file in SQL_SEQUENCE:
        sql_path = config.sql_dir / sql_file
        sql_text = sql_path.read_text(encoding="utf-8").format(**format_context)
        conn.execute(sql_text)

    outputs = {
        "mart_feeder_daily": conn.execute("SELECT * FROM mart_feeder_daily").df(),
        "mart_feeder_summary": conn.execute("SELECT * FROM mart_feeder_summary").df(),
        "mart_territory_monthly": conn.execute("SELECT * FROM mart_territory_monthly").df(),
        "kpi_network_overview": conn.execute("SELECT * FROM kpi_network_overview").df(),
        "kpi_top_feeders_stress": conn.execute("SELECT * FROM kpi_top_feeders_stress").df(),
        "kpi_territorial_pressure": conn.execute("SELECT * FROM kpi_territorial_pressure").df(),
        "validation_checks": conn.execute("SELECT * FROM validation_checks").df(),
    }

    for name, df in outputs.items():
        output_path = config.data_processed / f"{name}.csv"
        df.to_csv(output_path, index=False)

    conn.close()
    return outputs


if __name__ == "__main__":
    run_sql_layer()
