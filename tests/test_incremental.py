from datetime import date

import duckdb
import pandas as pd

from grid_intelligence.common import ProjectPaths
from grid_intelligence.incremental import IncrementalRefreshService
from grid_intelligence.ingestion import IngestionService


def _daily_demand() -> pd.DataFrame:
    timestamps = pd.date_range("2025-02-01", periods=24, freq="h")
    rows = []
    for feeder_no, zone_id in ((1, "Z001"), (2, "Z002")):
        for timestamp in timestamps:
            rows.append(
                {
                    "timestamp": timestamp,
                    "zona_id": zone_id,
                    "subestacion_id": f"S00{feeder_no}",
                    "alimentador_id": f"A00{feeder_no}",
                    "demanda_mw": 10.0 + feeder_no,
                    "demanda_reactiva_proxy": 2.0,
                    "temperatura": 18.0,
                    "humedad": 50.0,
                    "tipo_dia": "laborable",
                    "mes": 2,
                    "hora": timestamp.hour,
                    "factor_estacional": 1.0,
                    "hora_punta_flag": timestamp.hour in {19, 20, 21},
                    "tension_sistema_proxy": 1.0,
                }
            )
    return pd.DataFrame(rows)


def test_incremental_refresh_is_partitioned_and_watermark_idempotent(tmp_path):
    paths = ProjectPaths(tmp_path)
    source = tmp_path / "daily-demand.parquet"
    _daily_demand().to_parquet(source, index=False)
    IngestionService(paths).ingest("scada_ami_demanda", source, "day-20250201")

    service = IncrementalRefreshService(paths)
    first = service.refresh_date(date(2025, 2, 1))
    replay = service.refresh_date(date(2025, 2, 1))

    assert first.skipped is False
    assert first.feeder_rows == 2
    assert first.zone_rows == 2
    assert replay.skipped is True
    assert (tmp_path / "data/incremental/mart_zone_day/fecha=2025-02-01/part-00000.parquet").is_file()

    conn = duckdb.connect(str(paths.incremental_database), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM mart_zone_day_incremental").fetchone()[0] == 2
        assert conn.execute("SELECT MIN(completitud_datos) FROM mart_zone_day_incremental").fetchone()[0] == 1.0
    finally:
        conn.close()


def test_incremental_refresh_retries_failed_watermark_without_duplicate_run(tmp_path):
    paths = ProjectPaths(tmp_path)
    source = tmp_path / "daily-demand.parquet"
    _daily_demand().to_parquet(source, index=False)
    IngestionService(paths).ingest("scada_ami_demanda", source, "day-retry")
    service = IncrementalRefreshService(paths)
    first = service.refresh_date(date(2025, 2, 1))
    service.store.execute(
        "UPDATE incremental_runs SET status = 'failed', error_message = 'simulated' WHERE input_watermark = ?",
        [first.input_watermark],
    )

    retry = service.refresh_date(date(2025, 2, 1))

    assert retry.skipped is False
    runs = service.store.fetch_all("SELECT status FROM incremental_runs")
    assert runs == [{"status": "succeeded"}]
