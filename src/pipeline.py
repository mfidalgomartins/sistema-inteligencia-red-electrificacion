from __future__ import annotations

from .final_assembly_v2 import run_final_assembly_v2


def run_pipeline() -> dict[str, str]:
    """Ejecuta el pipeline canónico end-to-end."""
    return run_final_assembly_v2()


if __name__ == "__main__":
    result = run_pipeline()
    for key, value in result.items():
        print(f"{key}: {value}")
