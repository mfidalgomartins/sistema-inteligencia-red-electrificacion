"""Actualización diaria idempotente de marts operativos particionados."""

from __future__ import annotations

import hashlib
import os
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd

from .common import ProjectPaths, ensure_dirs, get_paths
from .operations_store import OperationsStore

DEMAND_CONTRACT = "scada_ami_demanda"


@dataclass(frozen=True)
class IncrementalRefreshResult:
    process_date: str
    input_watermark: str
    feeder_rows: int
    zone_rows: int
    skipped: bool

    def to_dict(self) -> dict:
        return asdict(self)


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _input_watermark(partitions: list[dict]) -> str:
    payload = "|".join(sorted(f"{item['batch_id']}:{item['file_checksum']}" for item in partitions))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _atomic_parquet(frame: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pending_path = output_path.with_suffix(".parquet.pending")
    try:
        frame.to_parquet(pending_path, index=False, engine="pyarrow")
        os.replace(pending_path, output_path)
    finally:
        pending_path.unlink(missing_ok=True)


class IncrementalRefreshService:
    def __init__(self, paths: ProjectPaths | None = None) -> None:
        self.paths = ensure_dirs(paths or get_paths())
        self.store = OperationsStore(self.paths)

    def _partitions_for_date(self, process_date: date) -> list[dict]:
        return self.store.fetch_all(
            """
            SELECT p.relative_path, p.batch_id, p.file_checksum
            FROM ingestion_partitions p
            JOIN ingestion_batches b USING (contract_name, batch_id)
            WHERE p.contract_name = ?
              AND p.partition_value = ?
              AND b.status = 'committed'
            ORDER BY p.batch_id, p.relative_path
            """,
            [DEMAND_CONTRACT, str(process_date)],
        )

    def _existing_run(self, process_date: date, watermark: str) -> dict | None:
        return self.store.fetch_one(
            """
            SELECT * FROM incremental_runs
            WHERE process_date = ? AND input_watermark = ?
            """,
            [process_date, watermark],
        )

    def refresh_date(self, process_date: date) -> IncrementalRefreshResult:
        partitions = self._partitions_for_date(process_date)
        if not partitions:
            raise FileNotFoundError(f"No hay demanda confirmada para {process_date}")
        watermark = _input_watermark(partitions)
        existing = self._existing_run(process_date, watermark)
        if existing and existing["status"] == "succeeded":
            return IncrementalRefreshResult(
                process_date=str(process_date),
                input_watermark=watermark,
                feeder_rows=int(existing["feeder_rows"]),
                zone_rows=int(existing["zone_rows"]),
                skipped=True,
            )
        if existing and existing["status"] == "running":
            raise RuntimeError(f"La fecha {process_date} ya está en proceso para el mismo watermark")

        absolute_paths = [self.paths.root / str(item["relative_path"]) for item in partitions]
        missing = [path for path in absolute_paths if not path.exists()]
        if missing:
            raise FileNotFoundError(f"Particiones ausentes: {', '.join(str(path) for path in missing)}")
        path_list = ", ".join(_quote_literal(str(path)) for path in absolute_paths)
        source = f"read_parquet([{path_list}], union_by_name = true)"
        run_id = str(existing["run_id"]) if existing else str(uuid4())
        started_at = datetime.now(UTC).replace(tzinfo=None)
        if existing:
            self.store.execute(
                """
                UPDATE incremental_runs
                SET status = 'running', feeder_rows = 0, zone_rows = 0,
                    started_at = ?, completed_at = NULL, error_message = NULL
                WHERE run_id = ?
                """,
                [started_at, run_id],
            )
        else:
            self.store.execute(
                """
                INSERT INTO incremental_runs (
                    run_id, process_date, input_watermark, status, feeder_rows, zone_rows, started_at
                ) VALUES (?, ?, ?, 'running', 0, 0, ?)
                """,
                [run_id, process_date, watermark, started_at],
            )

        try:
            conn = duckdb.connect()
            try:
                latest = f"""
                    SELECT * EXCLUDE (_source_contract, _source_checksum)
                    FROM {source}
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY timestamp, alimentador_id
                        ORDER BY _ingested_at DESC, _batch_id DESC
                    ) = 1
                """
                feeder = conn.execute(
                    f"""
                    SELECT
                        CAST(timestamp AS DATE) AS fecha,
                        zona_id,
                        subestacion_id,
                        alimentador_id,
                        SUM(demanda_mw) AS demanda_total_mwh,
                        MAX(demanda_mw) AS demanda_punta_mw,
                        AVG(demanda_mw) AS demanda_media_mw,
                        COUNT(DISTINCT timestamp) AS horas_observadas,
                        LEAST(COUNT(DISTINCT timestamp) / 24.0, 1.0) AS completitud_datos
                    FROM ({latest})
                    GROUP BY CAST(timestamp AS DATE), zona_id, subestacion_id, alimentador_id
                    ORDER BY zona_id, subestacion_id, alimentador_id
                    """
                ).df()
                zone = conn.execute(
                    f"""
                    WITH hourly AS (
                        SELECT
                            CAST(timestamp AS DATE) AS fecha,
                            timestamp,
                            zona_id,
                            SUM(demanda_mw) AS demanda_horaria_mw
                        FROM ({latest})
                        GROUP BY CAST(timestamp AS DATE), timestamp, zona_id
                    )
                    SELECT
                        fecha,
                        zona_id,
                        SUM(demanda_horaria_mw) AS demanda_total_mwh,
                        MAX(demanda_horaria_mw) AS demanda_punta_mw,
                        AVG(demanda_horaria_mw) AS demanda_media_mw,
                        COUNT(DISTINCT timestamp) AS horas_observadas,
                        LEAST(COUNT(DISTINCT timestamp) / 24.0, 1.0) AS completitud_datos
                    FROM hourly
                    GROUP BY fecha, zona_id
                    ORDER BY zona_id
                    """
                ).df()
            finally:
                conn.close()

            feeder_path = (
                self.paths.data_incremental / "mart_feeder_day" / f"fecha={process_date}" / "part-00000.parquet"
            )
            zone_path = self.paths.data_incremental / "mart_zone_day" / f"fecha={process_date}" / "part-00000.parquet"
            _atomic_parquet(feeder, feeder_path)
            _atomic_parquet(zone, zone_path)
            completed_at = datetime.now(UTC).replace(tzinfo=None)
            self.store.execute(
                """
                UPDATE incremental_runs
                SET status = 'succeeded', feeder_rows = ?, zone_rows = ?, completed_at = ?
                WHERE run_id = ?
                """,
                [len(feeder), len(zone), completed_at, run_id],
            )
            self.rebuild_catalog()
        except Exception as exc:
            self.store.execute(
                """
                UPDATE incremental_runs
                SET status = 'failed', completed_at = ?, error_message = ?
                WHERE run_id = ?
                """,
                [datetime.now(UTC).replace(tzinfo=None), str(exc)[:1000], run_id],
            )
            raise

        return IncrementalRefreshResult(
            process_date=str(process_date),
            input_watermark=watermark,
            feeder_rows=len(feeder),
            zone_rows=len(zone),
            skipped=False,
        )

    def refresh_dates(self, process_dates: list[date]) -> list[IncrementalRefreshResult]:
        if not process_dates:
            raise ValueError("Debe indicarse al menos una fecha")
        return [self.refresh_date(process_date) for process_date in sorted(set(process_dates))]

    def rebuild_catalog(self) -> Path:
        """Publica vistas DuckDB sobre todas las particiones diarias disponibles."""
        feeder_glob = self.paths.data_incremental / "mart_feeder_day" / "fecha=*" / "*.parquet"
        zone_glob = self.paths.data_incremental / "mart_zone_day" / "fecha=*" / "*.parquet"
        if not list(self.paths.data_incremental.glob("mart_feeder_day/fecha=*/*.parquet")):
            raise FileNotFoundError("No existen particiones incrementales de alimentador")
        conn = duckdb.connect(str(self.paths.incremental_database))
        try:
            conn.execute(
                f"""
                CREATE OR REPLACE VIEW mart_feeder_day_incremental AS
                SELECT * FROM read_parquet({_quote_literal(str(feeder_glob))}, hive_partitioning = true);
                CREATE OR REPLACE VIEW mart_zone_day_incremental AS
                SELECT * FROM read_parquet({_quote_literal(str(zone_glob))}, hive_partitioning = true);
                """
            )
        finally:
            conn.close()
        return self.paths.incremental_database
