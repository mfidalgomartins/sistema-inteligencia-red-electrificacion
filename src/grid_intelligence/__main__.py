"""Punto de entrada canónico del pipeline (`python -m grid_intelligence`)."""

from __future__ import annotations

import argparse
import logging

from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Ejecuta el pipeline de inteligencia de red.")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Nivel de detalle del registro de ejecución.",
    )
    parser.add_argument(
        "--source-mode",
        choices=["synthetic", "external"],
        default="synthetic",
        help="Origen raw: demo sintética reproducible o inputs externos gobernados.",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    outputs = run_pipeline(source_mode=args.source_mode)
    for key, value in outputs.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
