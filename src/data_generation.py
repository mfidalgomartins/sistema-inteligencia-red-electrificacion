from __future__ import annotations

from dataclasses import asdict
from typing import Dict

import numpy as np
import pandas as pd

from .config import CONFIG, Config


def _normalize(series: pd.Series) -> pd.Series:
    min_v = series.min()
    max_v = series.max()
    if max_v == min_v:
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (series - min_v) / (max_v - min_v)


def generate_territories(config: Config) -> pd.DataFrame:
    rng = np.random.default_rng(config.random_seed)

    territory_ids = [f"T{idx:03d}" for idx in range(1, config.n_territories + 1)]
    base_names = [
        "Costa Norte",
        "Valle Industrial",
        "Área Metropolitana Oeste",
        "Eje Logístico Sur",
        "Corredor Minero",
        "Sierra Central",
        "Ribera Atlántica",
        "Nudo Aeroportuario",
        "Distrito Tecnológico",
        "Altiplano Interior",
        "Costa Este",
        "Cuenca Fluvial",
        "Anillo Urbano",
        "Llanura Agrícola",
        "Puerto Energético",
        "Plataforma Automoción",
        "Nodo Químico",
        "Arco Residencial",
        "Polo Turístico",
        "Sierra Eólica",
        "Vega Solar",
        "Periferia Norte",
        "Periferia Sur",
        "Corredor Intermodal",
    ]

    tipos = rng.choice(["urbano", "periurbano", "rural"], size=config.n_territories, p=[0.4, 0.35, 0.25])
    poblacion = rng.integers(80_000, 1_200_000, size=config.n_territories)
    indice_industrial = rng.uniform(45, 130, size=config.n_territories)
    riesgo_climatico = rng.uniform(0.2, 0.95, size=config.n_territories)
    restriccion_permisos = rng.uniform(0.1, 0.9, size=config.n_territories)
    madurez_digital = rng.uniform(0.2, 0.95, size=config.n_territories)
    penetracion_ev = rng.uniform(0.04, 0.24, size=config.n_territories)

    territories = pd.DataFrame(
        {
            "territory_id": territory_ids,
            "territory_name": base_names[: config.n_territories],
            "territory_type": tipos,
            "population": poblacion,
            "industrial_index": indice_industrial.round(2),
            "climate_risk_index": riesgo_climatico.round(3),
            "permitting_constraint_index": restriccion_permisos.round(3),
            "digital_maturity_index": madurez_digital.round(3),
            "ev_penetration_2025": penetracion_ev.round(3),
        }
    )

    base_demand_index = (
        0.50 * _normalize(pd.Series(poblacion))
        + 0.35 * _normalize(pd.Series(indice_industrial))
        + 0.15 * rng.uniform(0, 1, config.n_territories)
    )
    territories["base_demand_index"] = (70 + base_demand_index * 70).round(2)

    return territories


def generate_substations(config: Config, territories: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(config.random_seed + 11)
    sub_ids = [f"S{idx:03d}" for idx in range(1, config.n_substations + 1)]

    territory_weights = territories["population"].to_numpy() / territories["population"].sum()
    territory_choice = rng.choice(territories["territory_id"], p=territory_weights, size=config.n_substations)

    substations = pd.DataFrame(
        {
            "substation_id": sub_ids,
            "territory_id": territory_choice,
            "voltage_kv": rng.choice([132, 66, 45], p=[0.22, 0.53, 0.25], size=config.n_substations),
            "installed_capacity_mva": rng.uniform(70, 420, size=config.n_substations).round(2),
            "age_years": rng.integers(4, 52, size=config.n_substations),
            "automation_level": rng.uniform(0.2, 0.98, size=config.n_substations).round(3),
            "n1_margin_pct": rng.uniform(5, 38, size=config.n_substations).round(2),
            "asset_health_score": rng.uniform(45, 95, size=config.n_substations).round(2),
        }
    )
    return substations


def generate_feeders(config: Config, territories: pd.DataFrame, substations: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(config.random_seed + 23)

    feeder_ids = [f"F{idx:04d}" for idx in range(1, config.n_feeders + 1)]

    substation_weights = substations["installed_capacity_mva"].to_numpy() / substations["installed_capacity_mva"].sum()
    substation_pick = rng.choice(substations["substation_id"], p=substation_weights, size=config.n_feeders)
    feeders = pd.DataFrame({"feeder_id": feeder_ids, "substation_id": substation_pick})
    feeders = feeders.merge(substations[["substation_id", "territory_id", "age_years", "asset_health_score"]], on="substation_id")
    feeders = feeders.merge(territories[["territory_id", "base_demand_index", "industrial_index", "ev_penetration_2025"]], on="territory_id")

    feeders["feeder_type"] = rng.choice(["urbano", "industrial", "mixto", "rural"], p=[0.34, 0.24, 0.27, 0.15], size=config.n_feeders)
    feeders["length_km"] = rng.uniform(3, 42, size=config.n_feeders).round(2)
    feeders["thermal_limit_mw"] = (
        10
        + 0.19 * feeders["base_demand_index"]
        + rng.uniform(0, 22, size=config.n_feeders)
    ).round(2)
    feeders["base_load_mw"] = (
        0.62 * feeders["thermal_limit_mw"]
        + 0.05 * feeders["industrial_index"]
        + rng.uniform(-2.2, 3.7, size=config.n_feeders)
    ).clip(lower=4.8)
    feeders["technical_losses_pct"] = rng.uniform(2.5, 10.5, size=config.n_feeders).round(2)
    feeders["underground_share_pct"] = rng.uniform(10, 92, size=config.n_feeders).round(2)
    feeders["dg_capacity_mw"] = (
        rng.uniform(0.1, 0.58, size=config.n_feeders)
        * feeders["thermal_limit_mw"]
    ).round(3)

    base_flex = 0.06 * feeders["thermal_limit_mw"] + 0.08 * feeders["ev_penetration_2025"] * feeders["thermal_limit_mw"]
    feeders["flex_contract_capacity_mw"] = (base_flex + rng.uniform(0, 1.5, size=config.n_feeders)).round(3)

    feeders["asset_health_score"] = (
        feeders["asset_health_score"] - 0.28 * np.maximum(feeders["age_years"] - 30, 0)
    ).clip(lower=25, upper=97).round(2)

    return feeders[
        [
            "feeder_id",
            "substation_id",
            "territory_id",
            "feeder_type",
            "length_km",
            "thermal_limit_mw",
            "base_load_mw",
            "technical_losses_pct",
            "underground_share_pct",
            "dg_capacity_mw",
            "flex_contract_capacity_mw",
            "asset_health_score",
            "age_years",
            "industrial_index",
            "ev_penetration_2025",
        ]
    ]


def generate_hourly_load_and_generation(config: Config, feeders: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    rng = np.random.default_rng(config.random_seed + 31)

    timestamps = pd.date_range(config.start_date, config.end_date, freq="h")
    n_hours = len(timestamps)
    n_feeders = len(feeders)

    feeder_idx = np.repeat(np.arange(n_feeders), n_hours)
    ts_idx = np.tile(np.arange(n_hours), n_feeders)

    feeder_ids = feeders["feeder_id"].to_numpy()[feeder_idx]
    base_load = feeders["base_load_mw"].to_numpy()[feeder_idx]
    thermal_limit = feeders["thermal_limit_mw"].to_numpy()[feeder_idx]
    dg_capacity = feeders["dg_capacity_mw"].to_numpy()[feeder_idx]
    ev_pen = feeders["ev_penetration_2025"].to_numpy()[feeder_idx]
    industrial_idx = feeders["industrial_index"].to_numpy()[feeder_idx]

    hour = timestamps.hour.to_numpy()[ts_idx]
    dayofweek = timestamps.dayofweek.to_numpy()[ts_idx]
    month = timestamps.month.to_numpy()[ts_idx]
    dayofyear = timestamps.dayofyear.to_numpy()[ts_idx]

    weekday_factor = np.where(dayofweek < 5, 1.0, 0.9)
    daily_shape = 0.78 + 0.18 * np.sin((hour - 7) * np.pi / 12) + 0.25 * np.exp(-((hour - 20) / 3.5) ** 2)
    seasonal_shape = 0.92 + 0.09 * np.cos((dayofyear - 20) * 2 * np.pi / 365) + 0.07 * np.exp(-((month - 7) / 1.9) ** 2)

    temperature = 12 + 10 * np.sin((dayofyear - 80) * 2 * np.pi / 365) + rng.normal(0, 2.2, size=len(feeder_idx))
    heating_cooling = np.maximum(0, 18 - temperature) * 0.03 + np.maximum(0, temperature - 25) * 0.025

    ev_profile = np.exp(-((hour - 22) / 3.8) ** 2) + 0.4 * np.exp(-((hour - 8) / 2.8) ** 2)
    industrial_profile = np.where((hour >= 7) & (hour <= 20) & (dayofweek < 5), 1.0, 0.45)

    trend_growth = 1 + 0.06 * (dayofyear / 365)

    base_component = base_load * daily_shape * seasonal_shape * weekday_factor * trend_growth
    ev_component = (0.08 + 0.34 * ev_pen) * thermal_limit * ev_profile
    industrial_component = (0.015 * industrial_idx / 100) * thermal_limit * industrial_profile
    random_noise = rng.normal(0, 0.035, size=len(feeder_idx)) * thermal_limit

    demand_mw = (base_component + ev_component + industrial_component + heating_cooling + random_noise).clip(min=1.1)

    daylight = np.clip(np.sin((hour - 6) * np.pi / 12), 0, None)
    pv_season = 0.58 + 0.34 * np.sin((dayofyear - 80) * 2 * np.pi / 365)
    pv_gen = dg_capacity * 0.68 * daylight * np.clip(pv_season, 0.15, None) * rng.uniform(0.85, 1.15, size=len(feeder_idx))

    wind_profile = 0.35 + 0.18 * np.sin((hour + 3) * 2 * np.pi / 24) + rng.normal(0, 0.08, size=len(feeder_idx))
    wind_gen = dg_capacity * 0.45 * np.clip(wind_profile, 0.02, 0.95)

    dg_total = (pv_gen + wind_gen).clip(min=0)

    load = pd.DataFrame(
        {
            "timestamp": timestamps.to_numpy()[ts_idx],
            "feeder_id": feeder_ids,
            "demand_mw": demand_mw.round(4),
            "ev_load_mw": ev_component.round(4),
            "industrial_load_mw": industrial_component.round(4),
            "temperature_c": temperature.round(3),
        }
    )

    generation = pd.DataFrame(
        {
            "timestamp": timestamps.to_numpy()[ts_idx],
            "feeder_id": feeder_ids,
            "pv_generation_mw": pv_gen.round(4),
            "wind_generation_mw": wind_gen.round(4),
            "dg_total_mw": dg_total.round(4),
        }
    )

    return {"load": load, "generation": generation}


def generate_outages(config: Config, feeders: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(config.random_seed + 47)

    ts_start = pd.Timestamp(config.start_date)
    ts_end = pd.Timestamp(config.end_date)
    window_hours = int((ts_end - ts_start).total_seconds() // 3600)

    records = []
    causes = ["clima_extremo", "fallo_equipo", "maniobra", "terceros", "vegetacion"]

    for row in feeders.itertuples(index=False):
        age_factor = 1 + max(row.age_years - 25, 0) / 45
        underground_factor = 1.25 - row.underground_share_pct / 200
        lambda_events = 3.8 * age_factor * underground_factor
        n_events = rng.poisson(max(lambda_events, 0.5))

        for _ in range(n_events):
            start_offset = int(rng.integers(0, window_hours))
            start_time = ts_start + pd.Timedelta(hours=start_offset)
            duration_min = int(rng.gamma(shape=2.4, scale=24) + rng.uniform(5, 20))
            customers = int(max(120, rng.normal(4200, 1500)))
            energy_not_served = duration_min / 60 * row.base_load_mw * rng.uniform(0.4, 0.9)
            records.append(
                {
                    "event_id": f"EVT-{row.feeder_id}-{start_offset:05d}",
                    "feeder_id": row.feeder_id,
                    "start_time": start_time,
                    "duration_min": duration_min,
                    "customers_affected": customers,
                    "cause": rng.choice(causes, p=[0.25, 0.28, 0.17, 0.12, 0.18]),
                    "energy_not_served_mwh": round(energy_not_served, 3),
                }
            )

    outages = pd.DataFrame(records)
    return outages.sort_values("start_time").reset_index(drop=True)


def generate_flexibility_assets(config: Config, territories: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(config.random_seed + 59)

    dr_capacity = (
        4.2 + 0.000006 * territories["population"] + 0.03 * territories["industrial_index"]
    ) * rng.uniform(0.7, 1.25, size=len(territories))

    battery_power = (
        1.8 + 0.000004 * territories["population"] + 0.018 * territories["base_demand_index"]
    ) * rng.uniform(0.6, 1.3, size=len(territories))

    assets = territories[["territory_id"]].copy()
    assets["demand_response_mw"] = dr_capacity.round(3)
    assets["battery_power_mw"] = battery_power.round(3)
    assets["battery_energy_mwh"] = (assets["battery_power_mw"] * rng.uniform(1.8, 3.4, len(assets))).round(3)
    assets["response_time_min"] = rng.choice([5, 10, 15, 30], p=[0.2, 0.35, 0.3, 0.15], size=len(assets))
    assets["availability_pct"] = rng.uniform(82, 97, size=len(assets)).round(2)
    assets["annual_cost_k_eur"] = (
        58 * assets["demand_response_mw"] + 74 * assets["battery_power_mw"] + rng.uniform(120, 600, size=len(assets))
    ).round(2)
    return assets


def generate_electrification_forecast(config: Config, territories: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(config.random_seed + 67)
    years = np.arange(2026, 2033)
    records = []

    for t in territories.itertuples(index=False):
        demand_driver = 0.8 + 0.003 * t.industrial_index + 0.0000009 * t.population
        for year in years:
            step = year - 2025
            records.append(
                {
                    "territory_id": t.territory_id,
                    "year": year,
                    "ev_peak_additional_mw": round((1.2 + 0.8 * step) * t.ev_penetration_2025 * demand_driver * rng.uniform(0.9, 1.2), 3),
                    "industrial_peak_additional_mw": round((0.6 + 0.65 * step) * (t.industrial_index / 100) * rng.uniform(0.9, 1.3), 3),
                    "heat_pump_peak_additional_mw": round((0.45 + 0.35 * step) * rng.uniform(0.8, 1.25), 3),
                    "base_demand_growth_pct": round((1.4 + 0.38 * step + rng.uniform(-0.4, 0.7)), 3),
                }
            )

    return pd.DataFrame(records)


def generate_capex_catalog() -> pd.DataFrame:
    catalog = pd.DataFrame(
        [
            {
                "intervention_type": "refuerzo_fisico",
                "unit": "MVA",
                "capex_unit_k_eur": 710,
                "opex_unit_k_eur": 9,
                "lead_time_months": 24,
                "lifetime_years": 35,
            },
            {
                "intervention_type": "automatizacion_avanzada",
                "unit": "feeder",
                "capex_unit_k_eur": 230,
                "opex_unit_k_eur": 16,
                "lead_time_months": 8,
                "lifetime_years": 15,
            },
            {
                "intervention_type": "flexibilidad_contratada",
                "unit": "MW",
                "capex_unit_k_eur": 95,
                "opex_unit_k_eur": 52,
                "lead_time_months": 4,
                "lifetime_years": 10,
            },
            {
                "intervention_type": "almacenamiento_bateria",
                "unit": "MW",
                "capex_unit_k_eur": 530,
                "opex_unit_k_eur": 24,
                "lead_time_months": 11,
                "lifetime_years": 18,
            },
        ]
    )
    return catalog


def save_raw_datasets(datasets: Dict[str, pd.DataFrame], config: Config) -> None:
    config.data_raw.mkdir(parents=True, exist_ok=True)
    for name, df in datasets.items():
        path = config.data_raw / f"{name}.csv"
        df.to_csv(path, index=False)


def generate_all_raw_data(config: Config = CONFIG) -> Dict[str, pd.DataFrame]:
    territories = generate_territories(config)
    substations = generate_substations(config, territories)
    feeders = generate_feeders(config, territories, substations)
    hourly = generate_hourly_load_and_generation(config, feeders)
    outages = generate_outages(config, feeders)
    flexibility = generate_flexibility_assets(config, territories)
    electrification = generate_electrification_forecast(config, territories)
    capex = generate_capex_catalog()

    datasets = {
        "territories": territories,
        "substations": substations,
        "feeders": feeders,
        "feeder_demand_hourly": hourly["load"],
        "feeder_generation_hourly": hourly["generation"],
        "outage_events": outages,
        "flexibility_assets": flexibility,
        "electrification_forecast": electrification,
        "capex_catalog": capex,
        "config_snapshot": pd.DataFrame([asdict(config)]),
    }

    save_raw_datasets(datasets, config)
    return datasets


if __name__ == "__main__":
    generate_all_raw_data()
