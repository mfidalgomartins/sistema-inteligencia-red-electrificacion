from __future__ import annotations

import argparse

from .final_assembly_v2 import run_final_assembly_v2


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Entrypoint oficial del proyecto (pipeline canónico)."
    )
    parser.parse_args()
    outputs = run_final_assembly_v2()
    for key, value in outputs.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
