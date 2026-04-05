from __future__ import annotations

import pandas as pd

from .config import CONFIG
from .scenario_engine import run_scenario_engine


def main() -> None:
    priorities_path = CONFIG.data_processed / "investment_priorities.csv"
    priorities = pd.read_csv(priorities_path)

    scenario_results, scenario_summary = run_scenario_engine(priorities)
    scenario_results.to_csv(CONFIG.data_processed / "scenario_results.csv", index=False)
    scenario_summary.to_csv(CONFIG.data_processed / "scenario_summary.csv", index=False)

    print("Escenarios ejecutados:")
    print(scenario_summary.to_string(index=False))


if __name__ == "__main__":
    main()
