from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd

from .config import SyntheticDataConfig


GEOGRAFIA_BASE = [
    ("Andalucia", "Sevilla", "Sur"),
    ("Andalucia", "Malaga", "Sur"),
    ("Andalucia", "Cadiz", "Sur"),
    ("Comunidad de Madrid", "Madrid", "Centro"),
    ("Cataluna", "Barcelona", "Nordeste"),
    ("Cataluna", "Tarragona", "Nordeste"),
    ("Comunitat Valenciana", "Valencia", "Levante"),
    ("Comunitat Valenciana", "Alicante", "Levante"),
    ("Pais Vasco", "Bizkaia", "Norte"),
    ("Pais Vasco", "Gipuzkoa", "Norte"),
    ("Galicia", "A Coruna", "Noroeste"),
    ("Galicia", "Pontevedra", "Noroeste"),
    ("Castilla y Leon", "Valladolid", "Norte"),
    ("Castilla y Leon", "Leon", "Norte"),
    ("Castilla-La Mancha", "Toledo", "Centro"),
    ("Castilla-La Mancha", "Ciudad Real", "Centro"),
    ("Aragon", "Zaragoza", "Nordeste"),
    ("Region de Murcia", "Murcia", "Levante"),
    ("Extremadura", "Badajoz", "Oeste"),
    ("Asturias", "Asturias", "Norte"),
    ("Navarra", "Navarra", "Norte"),
    ("Cantabria", "Cantabria", "Norte"),
    ("La Rioja", "La Rioja", "Norte"),
    ("Illes Balears", "Illes Balears", "Insular"),
    ("Canarias", "Las Palmas", "Insular"),
    ("Canarias", "Santa Cruz de Tenerife", "Insular"),
    ("Andalucia", "Cordoba", "Sur"),
    ("Andalucia", "Granada", "Sur"),
    ("Cataluna", "Girona", "Nordeste"),
    ("Comunitat Valenciana", "Castellon", "Levante"),
]


def generate_zonas_red(config: SyntheticDataConfig) -> pd.DataFrame:
    rng = np.random.default_rng(config.seed + 11)
    geos = GEOGRAFIA_BASE[: config.n_zonas]

    tipos = rng.choice(
        ["urbana", "industrial", "rural", "mixta"],
        size=config.n_zonas,
        p=[0.34, 0.24, 0.2, 0.22],
    )

    density_base = {
        "urbana": 0.88,
        "industrial": 0.72,
        "rural": 0.33,
        "mixta": 0.57,
    }
    gd_base = {"urbana": 0.31, "industrial": 0.24, "rural": 0.53, "mixta": 0.41}
    flex_base = {"urbana": 0.52, "industrial": 0.66, "rural": 0.34, "mixta": 0.57}
    growth_base = {"urbana": 0.62, "industrial": 0.73, "rural": 0.42, "mixta": 0.58}

    region_adjust = {
        "Norte": {"clima": 0.72, "gd": 0.02},
        "Noroeste": {"clima": 0.68, "gd": 0.03},
        "Centro": {"clima": 0.52, "gd": 0.0},
        "Nordeste": {"clima": 0.48, "gd": 0.02},
        "Levante": {"clima": 0.46, "gd": 0.04},
        "Sur": {"clima": 0.58, "gd": 0.05},
        "Oeste": {"clima": 0.55, "gd": 0.02},
        "Insular": {"clima": 0.61, "gd": 0.07},
    }

    rows = []
    for idx, (ca, provincia, region) in enumerate(geos, start=1):
        tipo = tipos[idx - 1]
        dens = np.clip(density_base[tipo] + rng.normal(0, 0.08), 0.1, 0.99)
        gd = np.clip(gd_base[tipo] + region_adjust[region]["gd"] + rng.normal(0, 0.06), 0.05, 0.95)
        criticidad = np.clip(0.45 + 0.35 * dens + 0.25 * rng.random(), 0.1, 1.0)
        flex = np.clip(flex_base[tipo] + 0.25 * gd + rng.normal(0, 0.08), 0.05, 0.98)
        clima = np.clip(region_adjust[region]["clima"] + rng.normal(0, 0.08), 0.15, 0.99)
        growth = np.clip(growth_base[tipo] + 0.15 * dens + rng.normal(0, 0.06), 0.1, 0.99)

        rows.append(
            {
                "zona_id": f"Z{idx:03d}",
                "zona_nombre": f"Zona {provincia} {tipo}",
                "comunidad_autonoma": ca,
                "provincia": provincia,
                "tipo_zona": tipo,
                "region_operativa": region,
                "densidad_demanda": round(float(dens), 4),
                "penetracion_generacion_distribuida": round(float(gd), 4),
                "criticidad_territorial": round(float(criticidad), 4),
                "potencial_flexibilidad": round(float(flex), 4),
                "riesgo_climatico": round(float(clima), 4),
                "tension_crecimiento_demanda": round(float(growth), 4),
            }
        )

    return pd.DataFrame(rows)


def generate_subestaciones(zonas_red: pd.DataFrame, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 23)
    rows = []

    for zona in zonas_red.itertuples(index=False):
        n_subs = int(np.clip(np.round(1.5 + 2.5 * zona.densidad_demanda + rng.normal(0, 0.4)), 2, 4))
        total_zone_capacity = (110 + 420 * zona.densidad_demanda + 180 * zona.tension_crecimiento_demanda) * rng.uniform(0.88, 1.18)
        shares = rng.dirichlet(np.ones(n_subs) * 1.4)

        for i in range(n_subs):
            capacity = max(35.0, total_zone_capacity * shares[i])
            antig = int(np.clip(rng.normal(22 + 20 * zona.riesgo_climatico, 9), 2, 58))
            digital = np.clip(0.28 + 0.62 * zona.potencial_flexibilidad + rng.normal(0, 0.07), 0.05, 0.99)
            redun = np.clip(0.25 + 0.5 * zona.densidad_demanda + rng.normal(0, 0.09), 0.05, 0.99)
            firme = capacity * np.clip(0.72 + 0.22 * redun - 0.0016 * antig + rng.normal(0, 0.03), 0.52, 0.97)
            critic = np.clip(
                0.35 + 0.28 * zona.criticidad_territorial + 0.22 * (1 - redun) + 0.18 * (antig / 58) + rng.normal(0, 0.05),
                0.05,
                0.99,
            )

            rows.append(
                {
                    "subestacion_id": f"S{len(rows) + 1:04d}",
                    "zona_id": zona.zona_id,
                    "nombre_subestacion": f"SE_{zona.provincia[:10].upper()}_{i + 1:02d}",
                    "capacidad_mw": round(float(capacity), 3),
                    "capacidad_firme_mw": round(float(firme), 3),
                    "antiguedad_anios": antig,
                    "indice_criticidad": round(float(critic), 4),
                    "digitalizacion_nivel": round(float(digital), 4),
                    "redundancia_nivel": round(float(redun), 4),
                }
            )

    return pd.DataFrame(rows)


def generate_alimentadores(subestaciones: pd.DataFrame, zonas_red: pd.DataFrame, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 31)
    zone_lookup = zonas_red.set_index("zona_id")
    rows = []

    for sub in subestaciones.itertuples(index=False):
        zona = zone_lookup.loc[sub.zona_id]
        n_feeders = int(np.clip(np.round(2.0 + 2.4 * zona.densidad_demanda + rng.normal(0, 0.45)), 2, 4))

        type_options = {
            "urbana": ["mallada_urbana", "subterranea", "mixta"],
            "industrial": ["industrial_dedicada", "aerea_reforzada", "mixta"],
            "rural": ["aerea_rural", "mixta", "troncal_larga"],
            "mixta": ["mixta", "mallada_urbana", "aerea_reforzada"],
        }
        feeder_types = rng.choice(type_options[zona.tipo_zona], size=n_feeders, replace=True)

        shares = rng.dirichlet(np.ones(n_feeders) * 1.5)
        for i in range(n_feeders):
            tipo = feeder_types[i]
            cap = max(6.5, sub.capacidad_mw * shares[i] * rng.uniform(0.72, 1.2))

            if tipo in {"aerea_rural", "troncal_larga"}:
                longitud = rng.uniform(12, 58)
            elif tipo in {"industrial_dedicada", "aerea_reforzada"}:
                longitud = rng.uniform(7, 34)
            else:
                longitud = rng.uniform(2.2, 19)

            perdidas = np.clip(0.028 + 0.0016 * longitud + 0.04 * (1 - zona.densidad_demanda) + rng.normal(0, 0.006), 0.015, 0.18)
            expos = np.clip(0.2 + 0.65 * zona.riesgo_climatico + 0.004 * longitud + rng.normal(0, 0.05), 0.05, 0.99)
            base_load = cap * np.clip(0.46 + 0.25 * zona.densidad_demanda + 0.12 * zona.tension_crecimiento_demanda + rng.normal(0, 0.05), 0.25, 0.93)
            critic = np.clip(0.3 + 0.32 * zona.criticidad_territorial + 0.22 * expos + 0.14 * (base_load / cap) + rng.normal(0, 0.04), 0.05, 0.99)

            rows.append(
                {
                    "alimentador_id": f"A{len(rows) + 1:05d}",
                    "subestacion_id": sub.subestacion_id,
                    "tipo_red": tipo,
                    "capacidad_mw": round(float(cap), 3),
                    "longitud_km": round(float(longitud), 3),
                    "nivel_perdidas_estimado": round(float(perdidas), 5),
                    "exposicion_climatica": round(float(expos), 4),
                    "carga_base_esperada": round(float(base_load), 3),
                    "criticidad_operativa": round(float(critic), 4),
                }
            )

    return pd.DataFrame(rows)


def generate_activos_red(
    zonas_red: pd.DataFrame,
    subestaciones: pd.DataFrame,
    alimentadores: pd.DataFrame,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 43)

    sub_zone = subestaciones.set_index("subestacion_id")["zona_id"].to_dict()
    zone_info = zonas_red.set_index("zona_id")

    rows = []
    for sub in subestaciones.itertuples(index=False):
        for tipo_activo in ["transformador_potencia", "interruptor_principal", "proteccion_digital"]:
            factor_tipo = {"transformador_potencia": 1.0, "interruptor_principal": 0.7, "proteccion_digital": 0.55}[tipo_activo]
            edad = int(np.clip(sub.antiguedad_anios + rng.normal(0, 4), 1, 65))
            salud = np.clip(92 - 0.95 * edad + 12 * sub.digitalizacion_nivel + rng.normal(0, 4), 10, 98)
            fallo = np.clip(0.01 + (100 - salud) / 150 + 0.35 * sub.indice_criticidad * factor_tipo + rng.normal(0, 0.01), 0.005, 0.98)
            critic = np.clip(0.4 + 0.5 * sub.indice_criticidad + 0.2 * factor_tipo + rng.normal(0, 0.04), 0.05, 0.99)

            capex_base = {
                "transformador_potencia": 1_250_000,
                "interruptor_principal": 290_000,
                "proteccion_digital": 210_000,
            }[tipo_activo]
            opex_base = {
                "transformador_potencia": 38_000,
                "interruptor_principal": 15_500,
                "proteccion_digital": 10_500,
            }[tipo_activo]

            rows.append(
                {
                    "activo_id": f"ACT{len(rows) + 1:06d}",
                    "tipo_activo": tipo_activo,
                    "subestacion_id": sub.subestacion_id,
                    "alimentador_id": np.nan,
                    "edad_anios": edad,
                    "estado_salud": round(float(salud), 3),
                    "probabilidad_fallo_proxy": round(float(fallo), 5),
                    "criticidad": round(float(critic), 4),
                    "capex_reposicion_estimado": round(float(capex_base * rng.uniform(0.82, 1.26)), 2),
                    "opex_mantenimiento_estimado": round(float(opex_base * rng.uniform(0.8, 1.35)), 2),
                }
            )

    feeder_with_zone = alimentadores.merge(subestaciones[["subestacion_id", "zona_id", "antiguedad_anios"]], on="subestacion_id", how="left")
    for row in feeder_with_zone.itertuples(index=False):
        zona = zone_info.loc[row.zona_id]
        for tipo_activo in ["linea_mt", "reconectador", "sensor_corriente"]:
            factor_tipo = {"linea_mt": 1.0, "reconectador": 0.72, "sensor_corriente": 0.46}[tipo_activo]
            edad = int(np.clip(row.antiguedad_anios + rng.normal(2, 5), 1, 68))
            salud = np.clip(89 - 0.86 * edad - 8 * row.exposicion_climatica + rng.normal(0, 5), 8, 97)
            fallo = np.clip(0.015 + (100 - salud) / 135 + 0.22 * row.criticidad_operativa * factor_tipo + 0.08 * zona.riesgo_climatico, 0.006, 0.99)
            critic = np.clip(0.34 + 0.48 * row.criticidad_operativa + 0.14 * zona.criticidad_territorial + rng.normal(0, 0.05), 0.05, 0.99)

            capex_base = {"linea_mt": 540_000, "reconectador": 130_000, "sensor_corriente": 24_000}[tipo_activo]
            opex_base = {"linea_mt": 17_500, "reconectador": 8_200, "sensor_corriente": 2_300}[tipo_activo]

            rows.append(
                {
                    "activo_id": f"ACT{len(rows) + 1:06d}",
                    "tipo_activo": tipo_activo,
                    "subestacion_id": row.subestacion_id,
                    "alimentador_id": row.alimentador_id,
                    "edad_anios": edad,
                    "estado_salud": round(float(salud), 3),
                    "probabilidad_fallo_proxy": round(float(fallo), 5),
                    "criticidad": round(float(critic), 4),
                    "capex_reposicion_estimado": round(float(capex_base * rng.uniform(0.85, 1.32)), 2),
                    "opex_mantenimiento_estimado": round(float(opex_base * rng.uniform(0.85, 1.4)), 2),
                }
            )

    return pd.DataFrame(rows)
