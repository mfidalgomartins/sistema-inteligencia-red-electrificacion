"""Ingestión idempotente, particionada y promovible a la capa raw canónica."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd

from ..common import ProjectPaths, ensure_dirs, get_paths
from ..operations_store import OperationsStore, safe_relative_path
from .contracts import ContractCatalog, SourceContract, load_contract_catalog, read_source_file, validate_dataframe

BATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")


class IngestionConflict(RuntimeError):
    """El identificador de lote ya existe con contenido distinto."""


@dataclass(frozen=True)
class PartitionResult:
    partition_value: str
    path: str
    row_count: int
    checksum: str


@dataclass(frozen=True)
class IngestionResult:
    contract_name: str
    batch_id: str
    row_count: int
    partitions: tuple[PartitionResult, ...]
    source_checksum: str
    idempotent_replay: bool

    def to_dict(self) -> dict:
        return asdict(self)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _quote_identifier(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class IngestionService:
    def __init__(
        self,
        paths: ProjectPaths | None = None,
        catalog: ContractCatalog | None = None,
    ) -> None:
        self.paths = ensure_dirs(paths or get_paths())
        self.catalog = catalog or load_contract_catalog()
        self.store = OperationsStore(self.paths)

    def _contract(self, contract_name: str) -> SourceContract:
        try:
            return self.catalog.contracts[contract_name]
        except KeyError as exc:
            available = ", ".join(sorted(self.catalog.contracts))
            raise KeyError(f"Contrato desconocido: {contract_name}. Disponibles: {available}") from exc

    def _existing_result(self, contract_name: str, batch_id: str, source_checksum: str) -> IngestionResult | None:
        batch = self.store.fetch_one(
            "SELECT * FROM ingestion_batches WHERE contract_name = ? AND batch_id = ?",
            [contract_name, batch_id],
        )
        if batch is None:
            return None
        if batch["source_checksum"] != source_checksum:
            raise IngestionConflict(f"batch_id reutilizado con contenido distinto: {contract_name}/{batch_id}")
        partitions = self.store.fetch_all(
            """
            SELECT partition_value, relative_path, row_count, file_checksum
            FROM ingestion_partitions
            WHERE contract_name = ? AND batch_id = ?
            ORDER BY partition_value, relative_path
            """,
            [contract_name, batch_id],
        )
        for item in partitions:
            partition_path = self.paths.root / str(item["relative_path"])
            if not partition_path.is_file():
                raise IngestionConflict(
                    f"partición registrada ausente para replay: {contract_name}/{batch_id}/{item['partition_value']}"
                )
            if _sha256(partition_path) != item["file_checksum"]:
                raise IngestionConflict(
                    f"partición registrada alterada para replay: {contract_name}/{batch_id}/{item['partition_value']}"
                )
        return IngestionResult(
            contract_name=contract_name,
            batch_id=batch_id,
            row_count=int(batch["row_count"]),
            partitions=tuple(
                PartitionResult(
                    partition_value=str(item["partition_value"]),
                    path=str(item["relative_path"]),
                    row_count=int(item["row_count"]),
                    checksum=str(item["file_checksum"]),
                )
                for item in partitions
            ),
            source_checksum=source_checksum,
            idempotent_replay=True,
        )

    def ingest(
        self,
        contract_name: str,
        source_path: Path,
        batch_id: str,
        *,
        as_of_date: date | None = None,
    ) -> IngestionResult:
        """Valida y confirma un lote sin modificar todavía la capa raw publicada."""
        if not BATCH_ID_PATTERN.fullmatch(batch_id):
            raise ValueError("batch_id debe usar 1-100 caracteres alfanuméricos, punto, guion o guion bajo")
        source_path = source_path.resolve()
        if not source_path.is_file():
            raise FileNotFoundError(source_path)
        contract = self._contract(contract_name)
        source_checksum = _sha256(source_path)
        existing = self._existing_result(contract_name, batch_id, source_checksum)
        if existing is not None:
            return existing

        frame = validate_dataframe(read_source_file(source_path), contract)
        ingested_at = datetime.now(UTC).replace(tzinfo=None)
        if contract.event_time:
            partition_values = pd.to_datetime(frame[contract.event_time]).dt.strftime("%Y-%m-%d")
            partition_key = "event_date"
        else:
            snapshot_date = as_of_date or datetime.now(UTC).date()
            partition_values = pd.Series(str(snapshot_date), index=frame.index)
            partition_key = "snapshot_date"

        audited = frame.copy()
        audited["_batch_id"] = batch_id
        audited["_source_contract"] = contract_name
        audited["_source_checksum"] = source_checksum
        audited["_ingested_at"] = ingested_at
        audited["_partition_value"] = partition_values

        created_paths: list[Path] = []
        partition_results: list[PartitionResult] = []
        try:
            for partition_value, group in audited.groupby("_partition_value", sort=True):
                partition_dir = (
                    self.paths.data_landing
                    / contract.source_system.lower()
                    / contract.target_table
                    / f"{partition_key}={partition_value}"
                    / f"batch_id={batch_id}"
                )
                partition_dir.mkdir(parents=True, exist_ok=False)
                output_path = partition_dir / "part-00000.parquet"
                group.drop(columns="_partition_value").to_parquet(output_path, index=False)
                created_paths.append(output_path)
                partition_results.append(
                    PartitionResult(
                        partition_value=str(partition_value),
                        path=safe_relative_path(output_path, self.paths.root),
                        row_count=len(group),
                        checksum=_sha256(output_path),
                    )
                )

            event_series = pd.to_datetime(frame[contract.event_time]) if contract.event_time else None
            with self.store.connection() as conn:
                conn.execute("BEGIN")
                try:
                    conn.execute(
                        """
                        INSERT INTO ingestion_batches VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'committed', ?)
                        """,
                        [
                            contract_name,
                            batch_id,
                            contract.source_system,
                            source_checksum,
                            source_path.name,
                            len(frame),
                            event_series.min().to_pydatetime() if event_series is not None else None,
                            event_series.max().to_pydatetime() if event_series is not None else None,
                            ingested_at,
                        ],
                    )
                    for item in partition_results:
                        conn.execute(
                            """
                            INSERT INTO ingestion_partitions VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            [
                                str(uuid4()),
                                contract_name,
                                batch_id,
                                item.partition_value,
                                item.path,
                                item.row_count,
                                item.checksum,
                                ingested_at,
                            ],
                        )
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
        except Exception:
            for created_path in created_paths:
                shutil.rmtree(created_path.parent, ignore_errors=True)
            raise

        return IngestionResult(
            contract_name=contract_name,
            batch_id=batch_id,
            row_count=len(frame),
            partitions=tuple(partition_results),
            source_checksum=source_checksum,
            idempotent_replay=False,
        )

    def _partition_rows(self, contract_name: str, load_strategy: str) -> list[dict]:
        rows = self.store.fetch_all(
            """
            SELECT p.relative_path, p.partition_value, p.batch_id, b.committed_at
            FROM ingestion_partitions p
            JOIN ingestion_batches b USING (contract_name, batch_id)
            WHERE p.contract_name = ? AND b.status = 'committed'
            ORDER BY b.committed_at, p.partition_value, p.relative_path
            """,
            [contract_name],
        )
        if not rows or load_strategy == "upsert":
            return rows
        latest_batch = max(rows, key=lambda item: item["committed_at"])["batch_id"]
        return [row for row in rows if row["batch_id"] == latest_batch]

    def promote(self, contract_name: str) -> tuple[Path, int]:
        """Publica la vista consolidada y deduplicada en `data/raw`."""
        contract = self._contract(contract_name)
        partitions = self._partition_rows(contract_name, contract.load_strategy)
        if not partitions:
            raise FileNotFoundError(f"No hay particiones confirmadas para {contract_name}")
        absolute_paths = [self.paths.root / str(item["relative_path"]) for item in partitions]
        missing = [path for path in absolute_paths if not path.is_file()]
        if missing:
            raise FileNotFoundError(f"Particiones ausentes: {', '.join(str(path) for path in missing)}")

        path_list = ", ".join(_quote_literal(str(path)) for path in absolute_paths)
        columns = ", ".join(_quote_identifier(column) for column in contract.columns)
        keys = ", ".join(_quote_identifier(column) for column in contract.primary_key)
        query = f"""
            SELECT {columns}
            FROM read_parquet([{path_list}], union_by_name = true)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {keys}
                ORDER BY _ingested_at DESC, _batch_id DESC
            ) = 1
            ORDER BY {keys}
        """
        output_path = self.paths.data_raw / f"{contract.target_table}.csv"
        pending_path = output_path.with_suffix(".csv.pending")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect()
        try:
            row_count = int(conn.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0])
            conn.execute(f"COPY ({query}) TO {_quote_literal(str(pending_path))} (HEADER, DELIMITER ',', QUOTE '\"')")
            os.replace(pending_path, output_path)
        finally:
            conn.close()
            pending_path.unlink(missing_ok=True)
        return output_path, row_count

    def validate_external_raw_inputs(self) -> dict[str, int]:
        """Valida la capa raw externa contra todos los contratos."""
        row_counts: dict[str, int] = {}
        reference_values: dict[str, set[str]] = {}
        for contract_name, contract in self.catalog.contracts.items():
            path = self.paths.data_raw / f"{contract.target_table}.csv"
            if not path.exists():
                raise FileNotFoundError(f"Falta input externo para {contract_name}: {path}")
            frame = validate_dataframe(read_source_file(path), contract)
            for column, reference_column in (
                ("zona_id", "zona_id"),
                ("subestacion_id", "subestacion_id"),
                ("alimentador_id", "alimentador_id"),
            ):
                if column not in frame or reference_column not in reference_values:
                    continue
                values = set(frame[column].dropna().astype(str))
                unknown = sorted(values.difference(reference_values[reference_column]))
                if unknown:
                    preview = ",".join(unknown[:5])
                    raise ValueError(f"foreign_key:{contract_name}.{column}:{len(unknown)}:{preview}")
            if contract.target_table == "zonas_red":
                reference_values["zona_id"] = set(frame["zona_id"].astype(str))
            elif contract.target_table == "subestaciones":
                reference_values["subestacion_id"] = set(frame["subestacion_id"].astype(str))
            elif contract.target_table == "alimentadores":
                reference_values["alimentador_id"] = set(frame["alimentador_id"].astype(str))
            row_counts[contract_name] = len(frame)
        return row_counts
