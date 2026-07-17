"""Orquestación secuencial de las etapas del pipeline analítico."""

from __future__ import annotations

from typing import Literal

from .orchestration import run_release_pipeline


def run_pipeline(source_mode: Literal["synthetic", "external"] = "synthetic") -> dict[str, str]:
    """Ejecuta el flujo canónico de extremo a extremo."""
    return run_release_pipeline(source_mode=source_mode)


if __name__ == "__main__":
    result = run_pipeline()
    for key, value in result.items():
        print(f"{key}: {value}")
