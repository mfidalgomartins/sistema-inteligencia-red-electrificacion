"""CLI operativo para contratos, lotes y refrescos incrementales."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from .incremental import IncrementalRefreshService
from .ingestion import IngestionService, load_contract_catalog


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use fecha ISO YYYY-MM-DD") from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Opera la ingestión externa e incremental de inteligencia de red.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("contracts", help="Lista los contratos de fuente disponibles.")

    ingest = subparsers.add_parser("ingest", help="Valida y confirma un lote CSV o Parquet.")
    ingest.add_argument("--contract", required=True)
    ingest.add_argument("--input", type=Path, required=True)
    ingest.add_argument("--batch-id", required=True)
    ingest.add_argument("--as-of-date", type=_parse_date)
    ingest.add_argument("--promote", action="store_true", help="Publica la tabla raw consolidada tras confirmar.")
    ingest.add_argument(
        "--refresh-incremental",
        action="store_true",
        help="Refresca las fechas afectadas; válido para scada_ami_demanda.",
    )

    promote = subparsers.add_parser("promote", help="Consolida un contrato confirmado en data/raw.")
    promote.add_argument("--contract", required=True)

    refresh = subparsers.add_parser("refresh", help="Reconstruye marts diarios para las fechas indicadas.")
    refresh.add_argument("--date", type=_parse_date, action="append", required=True)

    subparsers.add_parser("validate-external", help="Valida todos los inputs raw del modo externo.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    ingestion = IngestionService()

    if args.command == "contracts":
        catalog = load_contract_catalog()
        payload = {
            name: {
                "source_system": contract.source_system,
                "target_table": contract.target_table,
                "load_strategy": contract.load_strategy,
            }
            for name, contract in sorted(catalog.contracts.items())
        }
    elif args.command == "ingest":
        result = ingestion.ingest(args.contract, args.input, args.batch_id, as_of_date=args.as_of_date)
        payload = {"ingestion": result.to_dict()}
        if args.promote:
            path, rows = ingestion.promote(args.contract)
            payload["promotion"] = {"path": str(path), "row_count": rows}
        if args.refresh_incremental:
            if args.contract != "scada_ami_demanda":
                raise ValueError("--refresh-incremental solo admite scada_ami_demanda")
            dates = [date.fromisoformat(partition.partition_value) for partition in result.partitions]
            payload["incremental_refresh"] = [
                item.to_dict() for item in IncrementalRefreshService().refresh_dates(dates)
            ]
    elif args.command == "promote":
        path, rows = ingestion.promote(args.contract)
        payload = {"path": str(path), "row_count": rows}
    elif args.command == "refresh":
        payload = {"results": [item.to_dict() for item in IncrementalRefreshService().refresh_dates(args.date)]}
    elif args.command == "validate-external":
        payload = {"row_counts": ingestion.validate_external_raw_inputs()}
    else:  # pragma: no cover
        raise RuntimeError(f"Comando no implementado: {args.command}")

    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
