from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SyntheticDataConfig:
    seed: int = 20260328
    start_timestamp: str = "2024-01-01 00:00:00"
    end_timestamp: str = "2025-12-31 23:00:00"
    n_zonas: int = 24
    output_dir: Path = Path(__file__).resolve().parents[2] / "data" / "raw"


DEFAULT_CONFIG = SyntheticDataConfig()
