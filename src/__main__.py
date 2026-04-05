from __future__ import annotations

import argparse

from .final_assembly_v2 import run_final_assembly_v2
from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Entrypoint oficial del proyecto. "
            "Por defecto ejecuta el pipeline v2 (canónico)."
        )
    )
    parser.add_argument(
        "--legacy",
        action="store_true",
        help="Ejecuta el pipeline legacy (solo para compatibilidad).",
    )
    args = parser.parse_args()

    outputs = run_pipeline() if args.legacy else run_final_assembly_v2()
    for key, value in outputs.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
