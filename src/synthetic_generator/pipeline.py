from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

from .config import DEFAULT_CONFIG, SyntheticDataConfig
from .demand import (
    create_time_features,
    generate_demanda_electrificacion_industrial,
    generate_demanda_ev,
    generate_demanda_horaria,
)
from .entities import (
    generate_activos_red,
    generate_alimentadores,
    generate_subestaciones,
    generate_zonas_red,
)
from .generation import generate_generacion_distribuida
from .macro import build_base_macro_hourly, generate_escenario_macro
from .operations import (
    generate_almacenamiento_distribuido,
    generate_eventos_congestion,
    generate_interrupciones_servicio,
    generate_intervenciones_operativas,
    generate_inversiones_posibles,
    generate_recursos_flexibilidad,
)
from .validation import build_cardinality_summary, run_plausibility_checks, write_logic_summary


TABLE_ORDER = [
    "zonas_red",
    "subestaciones",
    "alimentadores",
    "demanda_horaria",
    "generacion_distribuida",
    "demanda_ev",
    "demanda_electrificacion_industrial",
    "eventos_congestion",
    "interrupciones_servicio",
    "activos_red",
    "recursos_flexibilidad",
    "almacenamiento_distribuido",
    "intervenciones_operativas",
    "inversiones_posibles",
    "escenario_macro",
]


def generate_synthetic_ecosystem(config: SyntheticDataConfig = DEFAULT_CONFIG) -> Dict[str, pd.DataFrame]:
    time_features = create_time_features(config)

    zonas_red = generate_zonas_red(config)
    subestaciones = generate_subestaciones(zonas_red, seed=config.seed)
    alimentadores = generate_alimentadores(subestaciones, zonas_red, seed=config.seed)

    escenario_macro = generate_escenario_macro(config)
    macro_hourly = build_base_macro_hourly(time_features, escenario_macro)

    demanda_ev = generate_demanda_ev(time_features, zonas_red, macro_hourly, seed=config.seed)
    demanda_electrificacion_industrial = generate_demanda_electrificacion_industrial(
        time_features,
        zonas_red,
        macro_hourly,
        seed=config.seed,
    )

    demanda_horaria = generate_demanda_horaria(
        time_features,
        zonas_red,
        subestaciones,
        alimentadores,
        demanda_ev,
        demanda_electrificacion_industrial,
        macro_hourly,
        seed=config.seed,
    )

    generacion_distribuida = generate_generacion_distribuida(
        time_features,
        zonas_red,
        demanda_horaria,
        macro_hourly,
        seed=config.seed,
    )

    activos_red = generate_activos_red(zonas_red, subestaciones, alimentadores, seed=config.seed)
    recursos_flexibilidad = generate_recursos_flexibilidad(zonas_red, seed=config.seed)
    almacenamiento_distribuido = generate_almacenamiento_distribuido(zonas_red, seed=config.seed)

    eventos_congestion = generate_eventos_congestion(
        demanda_horaria,
        subestaciones,
        alimentadores,
        zonas_red,
        recursos_flexibilidad,
        almacenamiento_distribuido,
        seed=config.seed,
    )

    interrupciones_servicio = generate_interrupciones_servicio(
        demanda_horaria,
        subestaciones,
        zonas_red,
        activos_red,
        eventos_congestion,
        seed=config.seed,
    )

    intervenciones_operativas = generate_intervenciones_operativas(
        zonas_red,
        recursos_flexibilidad,
        almacenamiento_distribuido,
        seed=config.seed,
    )
    inversiones_posibles = generate_inversiones_posibles(zonas_red, subestaciones, alimentadores, seed=config.seed)

    tables: Dict[str, pd.DataFrame] = {
        "zonas_red": zonas_red,
        "subestaciones": subestaciones,
        "alimentadores": alimentadores,
        "demanda_horaria": demanda_horaria,
        "generacion_distribuida": generacion_distribuida,
        "demanda_ev": demanda_ev,
        "demanda_electrificacion_industrial": demanda_electrificacion_industrial,
        "eventos_congestion": eventos_congestion,
        "interrupciones_servicio": interrupciones_servicio,
        "activos_red": activos_red,
        "recursos_flexibilidad": recursos_flexibilidad,
        "almacenamiento_distribuido": almacenamiento_distribuido,
        "intervenciones_operativas": intervenciones_operativas,
        "inversiones_posibles": inversiones_posibles,
        "escenario_macro": escenario_macro,
    }

    output = config.output_dir
    output.mkdir(parents=True, exist_ok=True)

    for table_name in TABLE_ORDER:
        tables[table_name].to_csv(output / f"{table_name}.csv", index=False)

    validaciones = run_plausibility_checks(tables, config)
    cardinalidades = build_cardinality_summary(tables)

    validaciones.to_csv(output / "validaciones_plausibilidad.csv", index=False)
    cardinalidades.to_csv(output / "resumen_cardinalidades.csv", index=False)
    write_logic_summary(output / "resumen_logica_generador.md", config, tables, cardinalidades)

    tables["validaciones_plausibilidad"] = validaciones
    tables["resumen_cardinalidades"] = cardinalidades

    return tables


def main() -> None:
    tables = generate_synthetic_ecosystem(DEFAULT_CONFIG)
    card = tables["resumen_cardinalidades"]
    checks = tables["validaciones_plausibilidad"]

    print("Generacion completada. Tablas creadas en data/raw/")
    print(card[["tabla", "filas", "columnas"]].to_string(index=False))
    print("\nChecks de plausibilidad:")
    print(checks[["check", "pasa", "valor_observado", "umbral"]].to_string(index=False))


if __name__ == "__main__":
    main()
