from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

from .common_v2 import connect_v2, ensure_dirs, get_paths, write_df


SQL_SEQUENCE_V2 = [
    "01_staging_core_tables.sql",
    "02_integrated_network_load.sql",
    "03_integrated_grid_events.sql",
    "04_integrated_service_quality.sql",
    "05_integrated_flexibility_assets.sql",
    "06_analytical_mart_node_hour.sql",
    "07_analytical_mart_zone_day.sql",
    "08_analytical_mart_zone_month.sql",
    "09_kpi_queries.sql",
    "10_validation_queries.sql",
]


EXPORT_OBJECTS = {
    "mart_node_hour_operational_state": "mart_node_hour_operational_state.parquet",
    "mart_zone_day_operational": "mart_zone_day_operational.csv",
    "mart_zone_month_operational": "mart_zone_month_operational.csv",
    "vw_zone_operational_risk": "vw_zone_operational_risk.csv",
    "vw_assets_exposure": "vw_assets_exposure.csv",
    "vw_flexibility_gap": "vw_flexibility_gap.csv",
    "vw_investment_candidates": "vw_investment_candidates.csv",
    "validation_checks": "validation_checks_sql_v2.csv",
    "kpi_top_zonas_riesgo_operativo": "kpi_top_zonas_riesgo_operativo.csv",
    "kpi_top_subestaciones_congestion_acumulada": "kpi_top_subestaciones_congestion_acumulada.csv",
    "kpi_top_alimentadores_exposicion": "kpi_top_alimentadores_exposicion.csv",
    "kpi_zonas_mayor_ens": "kpi_zonas_mayor_ens.csv",
    "kpi_zonas_peor_ratio_flex_estres": "kpi_zonas_peor_ratio_flex_estres.csv",
    "kpi_zonas_potencial_capex_diferible": "kpi_zonas_potencial_capex_diferible.csv",
    "kpi_activos_mas_expuestos": "kpi_activos_mas_expuestos.csv",
    "kpi_zonas_afectadas_ev_industrial": "kpi_zonas_afectadas_ev_industrial.csv",
}


def run_sql_layer_v2() -> Dict[str, pd.DataFrame]:
    paths = ensure_dirs(get_paths())
    conn = connect_v2(paths)
    format_context = {"raw_path": str(paths.data_raw).replace("'", "''")}

    for file_name in SQL_SEQUENCE_V2:
        sql_path = paths.sql / file_name
        sql_text = sql_path.read_text(encoding="utf-8").format(**format_context)
        conn.execute(sql_text)

    outputs: Dict[str, pd.DataFrame] = {}
    for db_object, out_name in EXPORT_OBJECTS.items():
        df = conn.execute(f"SELECT * FROM {db_object}").df()
        outputs[db_object] = df
        write_df(df, paths.data_processed / out_name)

    # Resumen de ejecución para trazabilidad.
    summary_df = pd.DataFrame(
        [
            {"script": script, "status": "ok"}
            for script in SQL_SEQUENCE_V2
        ]
    )
    write_df(summary_df, paths.outputs_reports / "sql_v2_execution_log.csv")

    conn.close()
    return outputs


if __name__ == "__main__":
    result = run_sql_layer_v2()
    print(f"Objetos exportados: {len(result)}")
