from __future__ import annotations

import argparse

from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Entrypoint oficial del proyecto."
    )
    parser.parse_args()
    outputs = run_pipeline()
    for key, value in outputs.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
