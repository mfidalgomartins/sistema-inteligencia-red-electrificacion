from __future__ import annotations

from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

from .config import SyntheticDataConfig


def build_cardinality_summary(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    keys = {
        "zonas_red": "zona_id",
        "subestaciones": "subestacion_id",
        "alimentadores": "alimentador_id",
        "demanda_horaria": "alimentador_id",
        "generacion_distribuida": "tecnologia",
        "demanda_ev": "tipo_recarga",
        "demanda_electrificacion_industrial": "cluster_industrial",
        "eventos_congestion": "evento_id",
        "interrupciones_servicio": "interrupcion_id",
        "activos_red": "activo_id",
        "recursos_flexibilidad": "recurso_id",
        "almacenamiento_distribuido": "storage_id",
        "intervenciones_operativas": "intervencion_id",
        "inversiones_posibles": "inversion_id",
        "escenario_macro": "escenario",
    }

    records = []
    for name, df in tables.items():
        temporal_cols = [c for c in ["timestamp", "timestamp_inicio", "fecha"] if c in df.columns]
        start = pd.to_datetime(df[temporal_cols[0]]).min() if temporal_cols else pd.NaT
        end = pd.to_datetime(df[temporal_cols[0]]).max() if temporal_cols else pd.NaT

        key_col = keys.get(name)
        unique_key = df[key_col].nunique() if key_col in df.columns else np.nan

        records.append(
            {
                "tabla": name,
                "filas": int(len(df)),
                "columnas": int(df.shape[1]),
                "periodo_inicio": start,
                "periodo_fin": end,
                "n_unicos_clave_referencia": unique_key,
            }
        )

    return pd.DataFrame(records).sort_values("tabla").reset_index(drop=True)


def run_plausibility_checks(tables: Dict[str, pd.DataFrame], config: SyntheticDataConfig) -> pd.DataFrame:
    zonas = tables["zonas_red"]
    sub = tables["subestaciones"]
    al = tables["alimentadores"]
    demand = tables["demanda_horaria"]
    gd = tables["generacion_distribuida"]
    ev = tables["demanda_ev"]
    ind = tables["demanda_electrificacion_industrial"]
    cong = tables["eventos_congestion"]
    intr = tables["interrupciones_servicio"]
    assets = tables["activos_red"]

    checks = []

    n_hours = demand["timestamp"].nunique()
    checks.append(
        {
            "check": "cobertura_horaria_minima_2_anios",
            "valor_observado": int(n_hours),
            "umbral": 17520,
            "pasa": int(n_hours >= 17520),
            "detalle": "Horas unicas en demanda_horaria",
        }
    )

    key_nulls = int(demand[["zona_id", "subestacion_id", "alimentador_id", "timestamp"]].isna().any(axis=1).sum())
    checks.append(
        {
            "check": "demanda_sin_nulos_en_llaves",
            "valor_observado": key_nulls,
            "umbral": 0,
            "pasa": int(key_nulls == 0),
            "detalle": "Filas con nulos en claves criticas",
        }
    )

    negative_demand = int((demand["demanda_mw"] < 0).sum())
    checks.append(
        {
            "check": "demanda_no_negativa",
            "valor_observado": negative_demand,
            "umbral": 0,
            "pasa": int(negative_demand == 0),
            "detalle": "Filas con demanda negativa",
        }
    )

    zone_sub_ok = int(sub["zona_id"].isin(zonas["zona_id"]).all())
    feeder_sub_ok = int(al["subestacion_id"].isin(sub["subestacion_id"]).all())
    checks.append(
        {
            "check": "integridad_referencial_topologia",
            "valor_observado": int(zone_sub_ok and feeder_sub_ok),
            "umbral": 1,
            "pasa": int(zone_sub_ok and feeder_sub_ok),
            "detalle": "Subestaciones y alimentadores enlazados correctamente",
        }
    )

    curtailment_share = float((gd["curtailment_estimado_mw"] > 0).mean())
    checks.append(
        {
            "check": "curtailment_no_trivial",
            "valor_observado": round(curtailment_share, 6),
            "umbral": 0.005,
            "pasa": int(curtailment_share > 0.005),
            "detalle": "Share de filas GD con curtailment positivo",
        }
    )

    if not cong.empty:
        congest_rows = len(cong)
        impacted_share = float(cong["impacto_servicio_flag"].mean())
    else:
        congest_rows = 0
        impacted_share = 0.0
    checks.append(
        {
            "check": "eventos_congestion_presentes",
            "valor_observado": int(congest_rows),
            "umbral": 10,
            "pasa": int(congest_rows >= 10),
            "detalle": "Numero de eventos de congestion",
        }
    )

    checks.append(
        {
            "check": "congestion_con_impacto_servicio",
            "valor_observado": round(impacted_share, 6),
            "umbral": 0.05,
            "pasa": int(impacted_share >= 0.05),
            "detalle": "Fraccion de eventos de congestion con impacto en servicio",
        }
    )

    corr_age_fail = assets[["edad_anios", "probabilidad_fallo_proxy"]].corr().iloc[0, 1]
    checks.append(
        {
            "check": "correlacion_edad_fallo_positiva",
            "valor_observado": round(float(corr_age_fail), 6),
            "umbral": 0.2,
            "pasa": int(corr_age_fail > 0.2),
            "detalle": "Correlacion entre edad del activo y probabilidad de fallo",
        }
    )

    rel_cong_rate = float(intr["relacion_congestion_flag"].mean()) if len(intr) else 0.0
    checks.append(
        {
            "check": "interrupciones_relacionadas_con_congestion",
            "valor_observado": round(rel_cong_rate, 6),
            "umbral": 0.1,
            "pasa": int(rel_cong_rate >= 0.1),
            "detalle": "Fraccion de interrupciones asociadas a congestion",
        }
    )

    # EV e industrial deben tener peso visible en punta
    demand_peak = demand[demand["hora_punta_flag"] == 1].groupby("timestamp", as_index=False)["demanda_mw"].sum()["demanda_mw"].mean()
    ev_peak = ev.groupby("timestamp", as_index=False)["demanda_ev_mw"].sum()["demanda_ev_mw"].mean()
    ind_peak = ind.groupby("timestamp", as_index=False)["demanda_industrial_adicional_mw"].sum()["demanda_industrial_adicional_mw"].mean()
    ratio_extra = float((ev_peak + ind_peak) / max(demand_peak, 1e-6))
    checks.append(
        {
            "check": "peso_ev_industrial_en_demanda_total",
            "valor_observado": round(ratio_extra, 6),
            "umbral": 0.05,
            "pasa": int(ratio_extra >= 0.05),
            "detalle": "Ratio medio EV+industrial sobre demanda en punta",
        }
    )

    return pd.DataFrame(checks)


def write_logic_summary(
    output_path: Path,
    config: SyntheticDataConfig,
    tables: Dict[str, pd.DataFrame],
    cardinalidad: pd.DataFrame,
) -> None:
    demanda = tables["demanda_horaria"]
    gd = tables["generacion_distribuida"]
    congestion = tables["eventos_congestion"]
    interrupciones = tables["interrupciones_servicio"]

    text = f"""
# Resumen de Logica del Generador Sintetico

## Configuracion principal
- Seed global: {config.seed}
- Periodo: {config.start_timestamp} a {config.end_timestamp}
- Horas unicas simuladas: {demanda['timestamp'].nunique()}
- Zonas: {tables['zonas_red']['zona_id'].nunique()}
- Subestaciones: {tables['subestaciones']['subestacion_id'].nunique()}
- Alimentadores: {tables['alimentadores']['alimentador_id'].nunique()}

## Logica sintetica aplicada
1. Topologia coherente jerarquica (zona -> subestacion -> alimentador).
2. Estacionalidad diaria, semanal y anual en demanda.
3. Perfiles diferenciales por tipo de zona y tipo de red.
4. Componentes EV e industrial correlacionados con crecimiento estructural.
5. Generacion distribuida tecnologica (solar/eolica/cogeneracion) con autoconsumo, vertido y curtailment.
6. Congestion modelada sobre utilizacion, cobertura de flexibilidad y eventos de punta.
7. Interrupciones correlacionadas con envejecimiento de activos y estres por congestion.
8. Recursos de flexibilidad y almacenamiento con cobertura desigual entre zonas.
9. Catalogos de intervenciones e inversiones con trade-offs tecnico-economicos.

## Magnitudes resultantes
- Demanda_horaria filas: {len(demanda):,}
- Generacion_distribuida filas: {len(gd):,}
- Eventos de congestion: {len(congestion):,}
- Interrupciones de servicio: {len(interrupciones):,}
- Curtailment total estimado (MWh proxy): {gd['curtailment_estimado_mw'].sum():,.2f}

## Cardinalidades por tabla
{cardinalidad.to_markdown(index=False)}
"""

    output_path.write_text(text.strip() + "\n", encoding="utf-8")
