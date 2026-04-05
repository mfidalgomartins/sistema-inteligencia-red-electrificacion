from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"
DOCS_DIR = PROJECT_ROOT / "docs"


TABLE_SPECS: Dict[str, Dict[str, Any]] = {
    "zonas_red": {
        "grain": "1 fila por zona de red",
        "pk": ["zona_id"],
        "fk": {},
    },
    "subestaciones": {
        "grain": "1 fila por subestacion",
        "pk": ["subestacion_id"],
        "fk": {"zona_id": "zonas_red.zona_id"},
    },
    "alimentadores": {
        "grain": "1 fila por alimentador",
        "pk": ["alimentador_id"],
        "fk": {"subestacion_id": "subestaciones.subestacion_id"},
    },
    "demanda_horaria": {
        "grain": "1 fila por hora-zona-subestacion-alimentador",
        "pk": ["timestamp", "zona_id", "subestacion_id", "alimentador_id"],
        "fk": {
            "zona_id": "zonas_red.zona_id",
            "subestacion_id": "subestaciones.subestacion_id",
            "alimentador_id": "alimentadores.alimentador_id",
        },
    },
    "generacion_distribuida": {
        "grain": "1 fila por hora-zona-tecnologia",
        "pk": ["timestamp", "zona_id", "tecnologia"],
        "fk": {"zona_id": "zonas_red.zona_id"},
    },
    "demanda_ev": {
        "grain": "1 fila por hora-zona-tipo_recarga",
        "pk": ["timestamp", "zona_id", "tipo_recarga"],
        "fk": {"zona_id": "zonas_red.zona_id"},
    },
    "demanda_electrificacion_industrial": {
        "grain": "1 fila por hora-zona-cluster_industrial",
        "pk": ["timestamp", "zona_id", "cluster_industrial"],
        "fk": {"zona_id": "zonas_red.zona_id"},
    },
    "eventos_congestion": {
        "grain": "1 fila por evento de congestion",
        "pk": ["evento_id"],
        "fk": {
            "zona_id": "zonas_red.zona_id",
            "subestacion_id": "subestaciones.subestacion_id",
            "alimentador_id": "alimentadores.alimentador_id",
        },
    },
    "interrupciones_servicio": {
        "grain": "1 fila por interrupcion de servicio",
        "pk": ["interrupcion_id"],
        "fk": {
            "zona_id": "zonas_red.zona_id",
            "subestacion_id": "subestaciones.subestacion_id",
        },
    },
    "activos_red": {
        "grain": "1 fila por activo",
        "pk": ["activo_id"],
        "fk": {
            "subestacion_id": "subestaciones.subestacion_id",
            "alimentador_id": "alimentadores.alimentador_id (nullable en activos de subestacion)",
        },
    },
    "recursos_flexibilidad": {
        "grain": "1 fila por recurso de flexibilidad",
        "pk": ["recurso_id"],
        "fk": {"zona_id": "zonas_red.zona_id"},
    },
    "almacenamiento_distribuido": {
        "grain": "1 fila por sistema de almacenamiento",
        "pk": ["storage_id"],
        "fk": {"zona_id": "zonas_red.zona_id"},
    },
    "intervenciones_operativas": {
        "grain": "1 fila por intervencion operativa catalogada",
        "pk": ["intervencion_id"],
        "fk": {"zona_id": "zonas_red.zona_id"},
    },
    "inversiones_posibles": {
        "grain": "1 fila por opcion de inversion",
        "pk": ["inversion_id"],
        "fk": {"zona_id": "zonas_red.zona_id"},
    },
    "escenario_macro": {
        "grain": "1 fila por fecha-escenario",
        "pk": ["fecha", "escenario"],
        "fk": {},
    },
}


def _quote(col: str) -> str:
    return f'"{col}"'


def _detect_column_type(col: str, dtype: str) -> str:
    lc = col.lower()
    dt = dtype.upper()

    if lc.endswith("_id") or lc in {"evento_id", "interrupcion_id", "storage_id", "fecha"} and lc.endswith("_id"):
        return "identificadores"
    if "timestamp" in lc or lc in {"fecha", "mes", "hora"}:
        return "temporales"
    if lc.endswith("_flag"):
        return "booleanas"
    if any(k in lc for k in ["tipo_", "tecnologia", "cluster", "perfil", "causa", "region", "comunidad", "provincia", "nombre", "horario", "severidad"]):
        return "estructurales"
    if any(k in dt for k in ["INT", "DOUBLE", "FLOAT", "DECIMAL", "BIGINT", "SMALLINT", "TINYINT"]):
        return "metricas"
    return "dimensiones"


def _temporal_columns(columns: List[str]) -> List[str]:
    return [c for c in columns if "timestamp" in c or c in {"fecha"}]


def _get_table_columns(conn: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    return conn.execute(f"DESCRIBE SELECT * FROM {table}").df().rename(columns={"column_name": "column", "column_type": "dtype"})


def _null_stats(conn: duckdb.DuckDBPyConnection, table: str, columns: List[str]) -> pd.DataFrame:
    exprs = [f"SUM(CASE WHEN {_quote(c)} IS NULL THEN 1 ELSE 0 END) AS null_{i}" for i, c in enumerate(columns)]
    q = f"SELECT COUNT(*) AS n_rows, {', '.join(exprs)} FROM {table}"
    row = conn.execute(q).fetchone()
    n_rows = int(row[0])

    records = []
    for i, col in enumerate(columns):
        n_null = int(row[i + 1])
        records.append(
            {
                "column": col,
                "null_count": n_null,
                "null_rate": (n_null / n_rows) if n_rows > 0 else np.nan,
            }
        )
    return pd.DataFrame(records).sort_values("null_rate", ascending=False).reset_index(drop=True)


def _duplicate_count(conn: duckdb.DuckDBPyConnection, table: str, key_cols: List[str]) -> int:
    if not key_cols:
        return 0
    key_expr = ", ".join([_quote(c) for c in key_cols])
    q = f"""
    SELECT COALESCE(SUM(cnt - 1), 0) AS dup_rows
    FROM (
      SELECT {key_expr}, COUNT(*) AS cnt
      FROM {table}
      GROUP BY {key_expr}
      HAVING COUNT(*) > 1
    ) t
    """
    return int(conn.execute(q).fetchone()[0])


def _numeric_columns(col_df: pd.DataFrame) -> List[str]:
    out = []
    for r in col_df.itertuples(index=False):
        dt = str(r.dtype).upper()
        if any(k in dt for k in ["INT", "DOUBLE", "FLOAT", "DECIMAL", "BIGINT", "SMALLINT", "TINYINT"]):
            out.append(r.column)
    return out


def _categorical_columns(col_df: pd.DataFrame) -> List[str]:
    out = []
    for r in col_df.itertuples(index=False):
        dt = str(r.dtype).upper()
        if any(k in dt for k in ["VARCHAR", "CHAR", "TEXT"]):
            out.append(r.column)
    return out


def _distribution_snapshot(conn: duckdb.DuckDBPyConnection, table: str, numeric_cols: List[str], limit_cols: int = 10) -> pd.DataFrame:
    stats = []
    for c in numeric_cols[:limit_cols]:
        q = f"""
        SELECT
          MIN({_quote(c)}) AS min_v,
          QUANTILE_CONT({_quote(c)}, 0.5) AS p50,
          QUANTILE_CONT({_quote(c)}, 0.95) AS p95,
          MAX({_quote(c)}) AS max_v,
          AVG({_quote(c)}) AS mean_v
        FROM {table}
        """
        min_v, p50, p95, max_v, mean_v = conn.execute(q).fetchone()
        stats.append(
            {
                "column": c,
                "min": min_v,
                "p50": p50,
                "p95": p95,
                "max": max_v,
                "mean": mean_v,
            }
        )
    return pd.DataFrame(stats)


def _categorical_cardinality(conn: duckdb.DuckDBPyConnection, table: str, cat_cols: List[str], limit_cols: int = 8) -> pd.DataFrame:
    rows = []
    for c in cat_cols[:limit_cols]:
        unique_count = conn.execute(f"SELECT COUNT(DISTINCT {_quote(c)}) FROM {table}").fetchone()[0]
        top = conn.execute(
            f"SELECT {_quote(c)} AS valor, COUNT(*) AS n FROM {table} GROUP BY 1 ORDER BY n DESC LIMIT 3"
        ).df()
        top_str = "; ".join([f"{r.valor}:{int(r.n)}" for r in top.itertuples(index=False)])
        rows.append({"column": c, "n_unique": int(unique_count), "top_values": top_str})
    return pd.DataFrame(rows)


def _temporal_coverage(conn: duckdb.DuckDBPyConnection, table: str, cols: List[str]) -> tuple[Any, Any]:
    if not cols:
        return (pd.NaT, pd.NaT)
    c = cols[0]
    min_v, max_v = conn.execute(f"SELECT MIN({_quote(c)}), MAX({_quote(c)}) FROM {table}").fetchone()
    return min_v, max_v


def _key_nulls(conn: duckdb.DuckDBPyConnection, table: str, key_cols: List[str]) -> int:
    if not key_cols:
        return 0
    cond = " OR ".join([f"{_quote(c)} IS NULL" for c in key_cols])
    return int(conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {cond}").fetchone()[0])


def table_specific_checks(conn: duckdb.DuckDBPyConnection, table: str) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []

    if table == "subestaciones":
        v = conn.execute("SELECT COUNT(*) FROM subestaciones WHERE capacidad_firme_mw > capacidad_mw").fetchone()[0]
        checks.append({"check": "capacidad_firme_mayor_capacidad", "failed_rows": int(v)})

    if table == "alimentadores":
        v = conn.execute("SELECT COUNT(*) FROM alimentadores WHERE carga_base_esperada > capacidad_mw * 1.2").fetchone()[0]
        checks.append({"check": "carga_base_excesiva_vs_capacidad", "failed_rows": int(v)})

    if table == "demanda_horaria":
        q1 = "SELECT COUNT(*) FROM demanda_horaria WHERE EXTRACT('hour' FROM CAST(timestamp AS TIMESTAMP)) <> hora"
        q2 = "SELECT COUNT(*) FROM demanda_horaria WHERE EXTRACT('month' FROM CAST(timestamp AS TIMESTAMP)) <> mes"
        q3 = "SELECT COUNT(*) FROM demanda_horaria WHERE demanda_mw < 0 OR demanda_reactiva_proxy < 0"
        checks.extend(
            [
                {"check": "hora_no_coincide_con_timestamp", "failed_rows": int(conn.execute(q1).fetchone()[0])},
                {"check": "mes_no_coincide_con_timestamp", "failed_rows": int(conn.execute(q2).fetchone()[0])},
                {"check": "metricas_negativas_imposibles", "failed_rows": int(conn.execute(q3).fetchone()[0])},
            ]
        )

    if table == "generacion_distribuida":
        checks.extend(
            [
                {
                    "check": "autoconsumo_mayor_generacion",
                    "failed_rows": int(
                        conn.execute("SELECT COUNT(*) FROM generacion_distribuida WHERE autoconsumo_estimado_mw > generacion_mw + 1e-6").fetchone()[0]
                    ),
                },
                {
                    "check": "vertido_negativo_o_curtailment_negativo",
                    "failed_rows": int(
                        conn.execute("SELECT COUNT(*) FROM generacion_distribuida WHERE vertido_estimado_mw < 0 OR curtailment_estimado_mw < 0").fetchone()[0]
                    ),
                },
                {
                    "check": "curtailment_mayor_que_vertido",
                    "failed_rows": int(
                        conn.execute("SELECT COUNT(*) FROM generacion_distribuida WHERE curtailment_estimado_mw > vertido_estimado_mw + 1e-6").fetchone()[0]
                    ),
                },
            ]
        )

    if table == "eventos_congestion":
        checks.extend(
            [
                {
                    "check": "fin_antes_de_inicio",
                    "failed_rows": int(
                        conn.execute("SELECT COUNT(*) FROM eventos_congestion WHERE CAST(timestamp_fin AS TIMESTAMP) < CAST(timestamp_inicio AS TIMESTAMP)").fetchone()[0]
                    ),
                },
                {
                    "check": "energia_o_carga_relativa_invalidas",
                    "failed_rows": int(
                        conn.execute("SELECT COUNT(*) FROM eventos_congestion WHERE energia_afectada_mwh <= 0 OR carga_relativa_max < 1").fetchone()[0]
                    ),
                },
            ]
        )

    if table == "interrupciones_servicio":
        checks.extend(
            [
                {
                    "check": "fin_antes_de_inicio",
                    "failed_rows": int(
                        conn.execute("SELECT COUNT(*) FROM interrupciones_servicio WHERE CAST(timestamp_fin AS TIMESTAMP) < CAST(timestamp_inicio AS TIMESTAMP)").fetchone()[0]
                    ),
                },
                {
                    "check": "clientes_o_ens_invalidos",
                    "failed_rows": int(
                        conn.execute("SELECT COUNT(*) FROM interrupciones_servicio WHERE clientes_afectados <= 0 OR energia_no_suministrada_mwh < 0").fetchone()[0]
                    ),
                },
            ]
        )

    if table in {"recursos_flexibilidad", "almacenamiento_distribuido"}:
        if table == "recursos_flexibilidad":
            q = "SELECT COUNT(*) FROM recursos_flexibilidad WHERE disponibilidad_media NOT BETWEEN 0 AND 1 OR fiabilidad_activacion NOT BETWEEN 0 AND 1 OR madurez_operativa NOT BETWEEN 0 AND 1"
            checks.append({"check": "indices_fuera_de_rango_0_1", "failed_rows": int(conn.execute(q).fetchone()[0])})
        else:
            q = "SELECT COUNT(*) FROM almacenamiento_distribuido WHERE disponibilidad_media NOT BETWEEN 0 AND 1 OR eficiencia_roundtrip NOT BETWEEN 0 AND 1"
            checks.append({"check": "indices_fuera_de_rango_0_1", "failed_rows": int(conn.execute(q).fetchone()[0])})

    if table == "inversiones_posibles":
        q = "SELECT COUNT(*) FROM inversiones_posibles WHERE facilidad_implementacion NOT BETWEEN 0 AND 1 OR impacto_resiliencia NOT BETWEEN 0 AND 1 OR reduccion_riesgo_esperada NOT BETWEEN 0 AND 1"
        checks.append({"check": "indices_fuera_de_rango_0_1", "failed_rows": int(conn.execute(q).fetchone()[0])})

    return checks


def cross_table_checks(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    results: Dict[str, Any] = {}

    # Inconsistencias capacidad-demanda
    q_cap = """
    SELECT
      COUNT(*) AS rows_total,
      SUM(CASE WHEN d.demanda_mw > a.capacidad_mw THEN 1 ELSE 0 END) AS rows_over_capacity,
      AVG(CASE WHEN d.demanda_mw > a.capacidad_mw THEN 1.0 ELSE 0.0 END) AS over_capacity_rate,
      MAX(d.demanda_mw / NULLIF(a.capacidad_mw, 0)) AS max_load_ratio
    FROM demanda_horaria d
    JOIN alimentadores a ON d.alimentador_id = a.alimentador_id
    """
    rows_total, rows_over, over_rate, max_ratio = conn.execute(q_cap).fetchone()
    results["capacidad_demanda"] = {
        "rows_total": int(rows_total),
        "rows_over_capacity": int(rows_over),
        "over_capacity_rate": float(over_rate),
        "max_load_ratio": float(max_ratio),
    }

    # Congestion vs interrupciones
    q_intr_vs_cong = """
    WITH intr_rel AS (
      SELECT * FROM interrupciones_servicio WHERE relacion_congestion_flag = 1
    ),
    matched AS (
      SELECT DISTINCT i.interrupcion_id
      FROM intr_rel i
      JOIN eventos_congestion c
        ON i.subestacion_id = c.subestacion_id
       AND CAST(c.timestamp_inicio AS TIMESTAMP) <= CAST(i.timestamp_fin AS TIMESTAMP)
       AND CAST(c.timestamp_fin AS TIMESTAMP) >= CAST(i.timestamp_inicio AS TIMESTAMP)
    )
    SELECT
      (SELECT COUNT(*) FROM intr_rel) AS intr_rel_total,
      (SELECT COUNT(*) FROM matched) AS intr_rel_with_congestion
    """
    intr_rel_total, intr_rel_with = conn.execute(q_intr_vs_cong).fetchone()
    mismatch = intr_rel_total - intr_rel_with
    mismatch_rate = mismatch / intr_rel_total if intr_rel_total else 0.0
    results["congestion_interrupciones"] = {
        "intr_rel_total": int(intr_rel_total),
        "intr_rel_with_congestion": int(intr_rel_with),
        "intr_rel_without_congestion": int(mismatch),
        "intr_rel_without_congestion_rate": float(mismatch_rate),
    }

    # Coherencia de stress operativo -> ens
    q_stress_ens = """
    WITH cong_sub AS (
      SELECT subestacion_id, COUNT(*) AS n_cong
      FROM eventos_congestion
      GROUP BY 1
    ),
    ens_sub AS (
      SELECT subestacion_id, SUM(energia_no_suministrada_mwh) AS ens_total
      FROM interrupciones_servicio
      GROUP BY 1
    )
    SELECT CORR(c.n_cong::DOUBLE, e.ens_total::DOUBLE)
    FROM cong_sub c
    JOIN ens_sub e USING(subestacion_id)
    """
    corr = conn.execute(q_stress_ens).fetchone()[0]
    results["stress_vs_continuidad"] = {"corr_congestion_ens_subestacion": float(corr) if corr is not None else np.nan}

    return results


def build_official_joins() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "join_name": "topologia_zona_subestacion_alimentador",
                "left_table": "subestaciones",
                "right_table": "zonas_red",
                "join_keys": "subestaciones.zona_id = zonas_red.zona_id",
                "join_type": "INNER",
                "purpose": "Contexto territorial y operativo de nodos",
            },
            {
                "join_name": "demanda_con_capacidad_alimentador",
                "left_table": "demanda_horaria",
                "right_table": "alimentadores",
                "join_keys": "demanda_horaria.alimentador_id = alimentadores.alimentador_id",
                "join_type": "INNER",
                "purpose": "Calcular utilizacion, sobrecargas y perdidas",
            },
            {
                "join_name": "demanda_gd_por_zona_hora",
                "left_table": "demanda_horaria_agg_zona_hora",
                "right_table": "generacion_distribuida_agg_zona_hora",
                "join_keys": "zona_id + timestamp",
                "join_type": "INNER",
                "purpose": "Balance neto, vertido y curtailment",
            },
            {
                "join_name": "demanda_componentes_ev_industrial",
                "left_table": "demanda_horaria_agg_zona_hora",
                "right_table": "demanda_ev_agg + demanda_electrificacion_industrial_agg",
                "join_keys": "zona_id + timestamp",
                "join_type": "LEFT",
                "purpose": "Descomponer drivers de pico",
            },
            {
                "join_name": "congestion_con_interrupciones",
                "left_table": "eventos_congestion",
                "right_table": "interrupciones_servicio",
                "join_keys": "subestacion_id + solape temporal",
                "join_type": "LEFT",
                "purpose": "Analisis impacto continuidad asociado a estres",
            },
            {
                "join_name": "riesgo_activos_con_eventos",
                "left_table": "activos_red",
                "right_table": "eventos_congestion/interrupciones_servicio",
                "join_keys": "subestacion_id y opcionalmente alimentador_id",
                "join_type": "LEFT",
                "purpose": "Modelos de riesgo y priorizacion de mantenimiento",
            },
            {
                "join_name": "capacidad_alivio_operativo",
                "left_table": "zonas_red",
                "right_table": "recursos_flexibilidad + almacenamiento_distribuido + intervenciones_operativas",
                "join_keys": "zona_id",
                "join_type": "LEFT",
                "purpose": "Analizar mitigacion no-fisica de congestion",
            },
            {
                "join_name": "roadmap_inversiones",
                "left_table": "inversiones_posibles",
                "right_table": "zonas_red + escenario_macro",
                "join_keys": "zona_id y fecha/escenario para stress testing",
                "join_type": "LEFT",
                "purpose": "Priorizacion tecnico-economica de cartera",
            },
        ]
    )


def build_mart_candidates() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "mart_name": "mart_operacion_horaria_feeder",
                "grain": "timestamp + alimentador_id",
                "sources": "demanda_horaria + alimentadores + subestaciones + zonas_red",
                "core_metrics": "demanda_mw, demanda_reactiva_proxy, utilization, tension_sistema_proxy",
                "use_cases": "monitoreo operativo, anomalias, forecasting intradia",
            },
            {
                "mart_name": "mart_balance_zona_horario",
                "grain": "timestamp + zona_id",
                "sources": "demanda_horaria agg + generacion_distribuida agg + demanda_ev agg + demanda_industrial agg",
                "core_metrics": "demanda_total, gd_total, net_load, curtailment, share_ev, share_industrial",
                "use_cases": "planeamiento de capacidad y hosting",
            },
            {
                "mart_name": "mart_congestion_eventos",
                "grain": "evento_id",
                "sources": "eventos_congestion + topologia + capacidad flex/storage por zona",
                "core_metrics": "severidad, energia_afectada_mwh, carga_relativa_max, impacto_servicio_flag",
                "use_cases": "diagnostico de estres y mitigacion",
            },
            {
                "mart_name": "mart_continuidad_servicio",
                "grain": "interrupcion_id",
                "sources": "interrupciones_servicio + eventos_congestion + activos_red",
                "core_metrics": "duracion_h, ens_mwh, clientes_afectados, relacion_congestion_flag",
                "use_cases": "SAIDI/SAIFI proxy y resiliencia",
            },
            {
                "mart_name": "mart_riesgo_activos",
                "grain": "activo_id",
                "sources": "activos_red + topologia + eventos/interrupciones agregados",
                "core_metrics": "edad, estado_salud, probabilidad_fallo_proxy, exposicion_eventos",
                "use_cases": "priorizacion de reemplazo y mantenimiento",
            },
            {
                "mart_name": "mart_portafolio_inversion",
                "grain": "zona_id + tipo_inversion",
                "sources": "inversiones_posibles + intervenciones_operativas + escenario_macro + riesgos agregados",
                "core_metrics": "capex, opex, reduccion_riesgo, facilidad, impacto_resiliencia",
                "use_cases": "scoring multicriterio y comite de inversiones",
            },
        ]
    )


def make_issue(severity: str, issue_type: str, table_scope: str, description: str, impact: str, recommendation: str, observed_value: Any) -> Dict[str, Any]:
    return {
        "severity": severity,
        "issue_type": issue_type,
        "table_scope": table_scope,
        "description": description,
        "impact": impact,
        "recommendation": recommendation,
        "observed_value": observed_value,
    }


def generate_issues(
    dataset_summary: pd.DataFrame,
    table_checks: pd.DataFrame,
    cross_checks: Dict[str, Any],
) -> pd.DataFrame:
    issues: List[Dict[str, Any]] = []

    # Critical nulls and duplicates
    for r in dataset_summary.itertuples(index=False):
        if r.key_null_rows > 0:
            issues.append(
                make_issue(
                    "P1",
                    "null_critico",
                    r.table,
                    f"Nulos en columnas clave candidata ({r.pk_candidate}).",
                    "Riesgo alto de joins perdidos y sesgo en métricas.",
                    "Aplicar reglas NOT NULL en staging y cuarentena de registros inválidos.",
                    int(r.key_null_rows),
                )
            )

        if r.duplicate_rows_on_key > 0:
            issues.append(
                make_issue(
                    "P1",
                    "duplicado_clave",
                    r.table,
                    f"Duplicados sobre clave candidata ({r.pk_candidate}).",
                    "Sobreconteo en agregados y distorsión de KPIs.",
                    "Deduplicar por versión de ingestión y registrar regla de supervivencia.",
                    int(r.duplicate_rows_on_key),
                )
            )

    # Table-specific failed checks
    for r in table_checks.itertuples(index=False):
        if r.failed_rows > 0:
            sev = "P1" if any(k in r.check for k in ["fin_antes_de_inicio", "imposibles"]) else "P2"
            issues.append(
                make_issue(
                    sev,
                    "inconsistencia_logica",
                    r.table,
                    f"Check fallido: {r.check}",
                    "Puede romper series temporales o reglas de negocio.",
                    "Corregir en generador/staging y añadir test de regresión.",
                    int(r.failed_rows),
                )
            )

    # Cross-checks
    cap = cross_checks["capacidad_demanda"]
    if cap["over_capacity_rate"] > 0.35:
        issues.append(
            make_issue(
                "P2",
                "capacidad_vs_demanda",
                "demanda_horaria + alimentadores",
                "Exceso de horas sobre capacidad por encima del rango esperado operativo.",
                "Puede sobredimensionar estimación de congestión y sesgar scoring.",
                "Recalibrar perfiles de demanda o capacidad para mantener estrés plausible.",
                round(cap["over_capacity_rate"], 6),
            )
        )

    cong_intr = cross_checks["congestion_interrupciones"]
    if cong_intr["intr_rel_without_congestion_rate"] > 0.2:
        issues.append(
            make_issue(
                "P2",
                "congestion_vs_interrupciones",
                "eventos_congestion + interrupciones_servicio",
                "Interrupciones marcadas como relacionadas con congestión sin solape temporal suficiente.",
                "Debilita trazabilidad causal en análisis diagnóstico.",
                "Ajustar regla de relacion_congestion_flag y ventana temporal oficial.",
                round(cong_intr["intr_rel_without_congestion_rate"], 6),
            )
        )

    # Redundancies and modeling risks (always relevant)
    issues.append(
        make_issue(
            "P3",
            "redundancia_derivada",
            "demanda_horaria",
            "Columnas `mes`, `hora`, `tipo_dia` y `factor_estacional` son derivables de timestamp/calendario.",
            "Riesgo de inconsistencias si se recalculan distinto entre capas.",
            "Tratar como campos derivados en marts; usar timestamp como única verdad temporal.",
            "aplica",
        )
    )
    issues.append(
        make_issue(
            "P3",
            "riesgo_modelado",
            "escenario_macro",
            "Serie macro diaria requiere expansión horaria controlada para forecasting horario.",
            "Si se mezcla granularidad diaria/horaria sin cuidado, hay leakage temporal.",
            "Crear feature store con reglas explícitas de forward-fill y lag temporal.",
            "aplica",
        )
    )

    if not issues:
        issues.append(
            make_issue(
                "P3",
                "sin_hallazgos_criticos",
                "global",
                "No se detectaron incidencias críticas automáticas.",
                "Mantener monitorización continua.",
                "Formalizar tests DQ en pipeline CI.",
                "0",
            )
        )

    order = {"P1": 1, "P2": 2, "P3": 3}
    issue_df = pd.DataFrame(issues)
    issue_df["_order"] = issue_df["severity"].map(order)
    issue_df = issue_df.sort_values(["_order", "issue_type", "table_scope"]).drop(columns=["_order"]).reset_index(drop=True)
    return issue_df


def build_recommendations() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "priority": "Alta",
                "topic": "Normalizacion temporal",
                "recommendation": "Construir dim_calendario y recalcular mes/hora/tipo_dia en staging para evitar inconsistencias derivadas.",
                "impact": "Mejora calidad para forecasting y dashboards temporales.",
            },
            {
                "priority": "Alta",
                "topic": "Contratos de datos",
                "recommendation": "Definir contratos de llaves (NOT NULL/UNIQUE) por tabla en SQL staging.",
                "impact": "Reduce riesgo de joins incompletos en scoring.",
            },
            {
                "priority": "Alta",
                "topic": "Reglas de integridad evento",
                "recommendation": "Validar siempre timestamp_fin >= timestamp_inicio y relaciones de causalidad congestion-interrupcion.",
                "impact": "Fortalece análisis de resiliencia y continuidad.",
            },
            {
                "priority": "Media",
                "topic": "Gestión de outliers",
                "recommendation": "Aplicar winsorization/flags para extremos en demanda_mw, energia_afectada_mwh y ENS antes de modelado.",
                "impact": "Mayor robustez en anomaly detection.",
            },
            {
                "priority": "Media",
                "topic": "Feature governance",
                "recommendation": "Separar features de estado actual vs derivadas de escenario para evitar leakage en modelos predictivos.",
                "impact": "Mejora validez de forecast y escenario analysis.",
            },
            {
                "priority": "Media",
                "topic": "Semántica de métricas",
                "recommendation": "Documentar definición oficial de curtailment, vertido y energía afectada para coherencia de KPI.",
                "impact": "Evita interpretaciones divergentes en comité ejecutivo.",
            },
        ]
    )


def profile_all_tables(conn: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    summary_rows = []
    classification_rows = []
    table_check_rows = []
    details: Dict[str, Any] = {}

    for table, spec in TABLE_SPECS.items():
        col_df = _get_table_columns(conn, table)
        columns = col_df["column"].tolist()
        row_count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        temporal_cols = _temporal_columns(columns)
        period_start, period_end = _temporal_coverage(conn, table, temporal_cols)

        null_df = _null_stats(conn, table, columns)
        dup_count = _duplicate_count(conn, table, spec["pk"])
        key_nulls = _key_nulls(conn, table, spec["pk"])

        numeric_cols = _numeric_columns(col_df)
        cat_cols = _categorical_columns(col_df)
        dist_df = _distribution_snapshot(conn, table, numeric_cols, limit_cols=10)
        card_df = _categorical_cardinality(conn, table, cat_cols, limit_cols=8)

        for r in col_df.itertuples(index=False):
            classification_rows.append(
                {
                    "table": table,
                    "column": r.column,
                    "dtype": r.dtype,
                    "classification": _detect_column_type(r.column, r.dtype),
                }
            )

        checks = table_specific_checks(conn, table)
        for c in checks:
            c["table"] = table
            table_check_rows.append(c)

        summary_rows.append(
            {
                "table": table,
                "grain": spec["grain"],
                "pk_candidate": ", ".join(spec["pk"]),
                "expected_fks": "; ".join([f"{k}->{v}" for k, v in spec["fk"].items()]) if spec["fk"] else "-",
                "rows": row_count,
                "columns": len(columns),
                "period_start": period_start,
                "period_end": period_end,
                "key_null_rows": key_nulls,
                "duplicate_rows_on_key": dup_count,
                "null_rate_max_column": float(null_df["null_rate"].max()) if len(null_df) else np.nan,
                "n_numeric_cols": len(numeric_cols),
                "n_categorical_cols": len(cat_cols),
            }
        )

        details[table] = {
            "nulls": null_df,
            "distributions": dist_df,
            "cardinality": card_df,
            "checks": pd.DataFrame(checks),
            "columns": col_df,
        }

    summary_df = pd.DataFrame(summary_rows).sort_values("table").reset_index(drop=True)
    class_df = pd.DataFrame(classification_rows).sort_values(["table", "column"]).reset_index(drop=True)
    checks_df = pd.DataFrame(table_check_rows).sort_values(["table", "check"]).reset_index(drop=True)
    cross = cross_table_checks(conn)

    return summary_df, class_df, checks_df, {"details": details, "cross": cross}


def write_markdown_report(
    summary_df: pd.DataFrame,
    class_df: pd.DataFrame,
    checks_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    recommendations_df: pd.DataFrame,
    joins_df: pd.DataFrame,
    marts_df: pd.DataFrame,
    context: Dict[str, Any],
) -> str:
    details = context["details"]
    cross = context["cross"]

    lines: List[str] = []
    lines.append("# Explore-Data Audit | Sistema de Inteligencia de Red")
    lines.append("")
    lines.append("## Resumen Ejecutivo")
    lines.append(f"- Datasets auditados: **{len(summary_df)}**")
    lines.append(f"- Filas totales auditadas: **{int(summary_df['rows'].sum()):,}**")
    lines.append(f"- Issues priorizados: **{len(issues_df)}**")
    lines.append("")

    lines.append("## Tabla Resumen por Dataset")
    lines.append(summary_df.to_markdown(index=False))
    lines.append("")

    lines.append("## Hallazgos Cross-Dataset")
    lines.append(f"- Over-capacity rate demanda vs capacidad: **{cross['capacidad_demanda']['over_capacity_rate']:.4%}**")
    lines.append(f"- Max load ratio demanda/capacidad: **{cross['capacidad_demanda']['max_load_ratio']:.3f}x**")
    lines.append(
        "- Interrupciones marcadas con congestión sin solape: "
        f"**{cross['congestion_interrupciones']['intr_rel_without_congestion']}** "
        f"({cross['congestion_interrupciones']['intr_rel_without_congestion_rate']:.2%})"
    )
    lines.append(
        "- Correlación congestión vs ENS por subestación: "
        f"**{cross['stress_vs_continuidad']['corr_congestion_ens_subestacion']:.4f}**"
    )
    lines.append("")

    lines.append("## Issues Priorizados")
    lines.append(issues_df.to_markdown(index=False))
    lines.append("")

    lines.append("## Recomendaciones para Transformación Analítica")
    lines.append(recommendations_df.to_markdown(index=False))
    lines.append("")

    lines.append("## Propuesta de Joins Oficiales")
    lines.append(joins_df.to_markdown(index=False))
    lines.append("")

    lines.append("## Tablas Candidatas para Marts Analíticos")
    lines.append(marts_df.to_markdown(index=False))
    lines.append("")

    lines.append("## Clasificación de Columnas")
    cls_summary = class_df.groupby(["table", "classification"], as_index=False).size().rename(columns={"size": "n_columnas"})
    lines.append(cls_summary.to_markdown(index=False))
    lines.append("")

    lines.append("## Perfil Detallado por Tabla")
    for table in TABLE_SPECS:
        dt = details[table]
        lines.append(f"### {table}")
        spec = TABLE_SPECS[table]
        lines.append(f"- Grain: `{spec['grain']}`")
        lines.append(f"- Candidate key: `{', '.join(spec['pk'])}`")
        lines.append(f"- Foreign keys esperadas: `{'; '.join([f'{k}->{v}' for k, v in spec['fk'].items()]) if spec['fk'] else '-'}`")

        row = summary_df[summary_df["table"] == table].iloc[0]
        lines.append(f"- Filas/Columnas: `{int(row['rows']):,}` / `{int(row['columns'])}`")
        lines.append(f"- Cobertura temporal: `{row['period_start']}` -> `{row['period_end']}`")
        lines.append(f"- Key null rows: `{int(row['key_null_rows'])}`")
        lines.append(f"- Duplicados sobre key: `{int(row['duplicate_rows_on_key'])}`")

        null_top = dt["nulls"].head(8)
        lines.append("- Null rates (top 8):")
        lines.append(null_top.to_markdown(index=False))

        if len(dt["cardinality"]):
            lines.append("- Cardinalidad categórica (muestra):")
            lines.append(dt["cardinality"].to_markdown(index=False))

        if len(dt["distributions"]):
            lines.append("- Distribuciones numéricas (muestra):")
            lines.append(dt["distributions"].round(5).to_markdown(index=False))

        table_checks = checks_df[checks_df["table"] == table]
        if len(table_checks):
            lines.append("- Coherencia lógica (checks):")
            lines.append(table_checks[["check", "failed_rows"]].to_markdown(index=False))

        utilidad = "Alta"
        if table in {"escenario_macro", "inversiones_posibles", "intervenciones_operativas"}:
            utilidad = "Media-Alta"
        lines.append(f"- Utilidad analítica estimada: **{utilidad}** para forecasting/scoring/dashboard.")
        lines.append("")

    return "\n".join(lines)


def write_html_report(
    summary_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    recommendations_df: pd.DataFrame,
    joins_df: pd.DataFrame,
    marts_df: pd.DataFrame,
    checks_df: pd.DataFrame,
    class_df: pd.DataFrame,
    context: Dict[str, Any],
) -> str:
    cross = context["cross"]
    cls_summary = class_df.groupby(["table", "classification"], as_index=False).size().rename(columns={"size": "n_columnas"})

    html = f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <title>Explore-Data Audit Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #111827; }}
    h1, h2, h3 {{ color: #0f172a; }}
    .kpi {{ display: inline-block; margin-right: 18px; padding: 10px 12px; background: #f3f4f6; border-radius: 8px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 20px; font-size: 13px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 6px 8px; text-align: left; }}
    th {{ background: #e5e7eb; }}
    .sev-P1 {{ color: #991b1b; font-weight: 700; }}
    .sev-P2 {{ color: #92400e; font-weight: 700; }}
    .sev-P3 {{ color: #1e3a8a; font-weight: 700; }}
  </style>
</head>
<body>
  <h1>Explore-Data Audit | Sistema de Inteligencia de Red</h1>
  <div class="kpi">Datasets: <b>{len(summary_df)}</b></div>
  <div class="kpi">Filas auditadas: <b>{int(summary_df['rows'].sum()):,}</b></div>
  <div class="kpi">Issues: <b>{len(issues_df)}</b></div>

  <h2>Resumen por Dataset</h2>
  {summary_df.to_html(index=False)}

  <h2>Cross-checks</h2>
  <ul>
    <li>Over-capacity rate: <b>{cross['capacidad_demanda']['over_capacity_rate']:.4%}</b></li>
    <li>Max load ratio: <b>{cross['capacidad_demanda']['max_load_ratio']:.3f}x</b></li>
    <li>Interrupciones con flag congestión sin solape: <b>{cross['congestion_interrupciones']['intr_rel_without_congestion']}</b> ({cross['congestion_interrupciones']['intr_rel_without_congestion_rate']:.2%})</li>
    <li>Correlación congestión-ENS (subestación): <b>{cross['stress_vs_continuidad']['corr_congestion_ens_subestacion']:.4f}</b></li>
  </ul>

  <h2>Issues Priorizados</h2>
  {issues_df.to_html(index=False)}

  <h2>Recomendaciones</h2>
  {recommendations_df.to_html(index=False)}

  <h2>Joins Oficiales</h2>
  {joins_df.to_html(index=False)}

  <h2>Marts Candidatas</h2>
  {marts_df.to_html(index=False)}

  <h2>Clasificación de Columnas</h2>
  {cls_summary.to_html(index=False)}

  <h2>Checks por Tabla</h2>
  {checks_df.to_html(index=False)}
</body>
</html>
"""
    return html


def save_docs_markdowns(joins_df: pd.DataFrame, marts_df: pd.DataFrame, report_md: str) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    joins_md = "# Propuesta de Joins Oficiales\n\n" + joins_df.to_markdown(index=False) + "\n"
    marts_md = "# Propuesta de Marts Analíticos\n\n" + marts_df.to_markdown(index=False) + "\n"

    (DOCS_DIR / "joins_oficiales.md").write_text(joins_md, encoding="utf-8")
    (DOCS_DIR / "marts_candidatas.md").write_text(marts_md, encoding="utf-8")
    (DOCS_DIR / "explore_data_readiness.md").write_text(report_md, encoding="utf-8")


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()
    for table in TABLE_SPECS:
        csv_path = RAW_DIR / f"{table}.csv"
        conn.execute(
            f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_csv_auto('{str(csv_path).replace("'", "''")}', HEADER=TRUE)"
        )

    summary_df, class_df, checks_df, context = profile_all_tables(conn)
    joins_df = build_official_joins()
    marts_df = build_mart_candidates()
    issues_df = generate_issues(summary_df, checks_df, context["cross"])
    recommendations_df = build_recommendations()

    report_md = write_markdown_report(
        summary_df,
        class_df,
        checks_df,
        issues_df,
        recommendations_df,
        joins_df,
        marts_df,
        context,
    )
    report_html = write_html_report(
        summary_df,
        issues_df,
        recommendations_df,
        joins_df,
        marts_df,
        checks_df,
        class_df,
        context,
    )

    summary_df.to_csv(REPORT_DIR / "explore_data_dataset_summary.csv", index=False)
    class_df.to_csv(REPORT_DIR / "explore_data_column_classification.csv", index=False)
    checks_df.to_csv(REPORT_DIR / "explore_data_table_checks.csv", index=False)
    issues_df.to_csv(REPORT_DIR / "explore_data_issues_priorizados.csv", index=False)
    recommendations_df.to_csv(REPORT_DIR / "explore_data_recommendations.csv", index=False)
    joins_df.to_csv(REPORT_DIR / "explore_data_joins_oficiales.csv", index=False)
    marts_df.to_csv(REPORT_DIR / "explore_data_marts_candidatas.csv", index=False)

    (REPORT_DIR / "explore_data_audit_report.md").write_text(report_md, encoding="utf-8")
    (REPORT_DIR / "explore_data_audit_report.html").write_text(report_html, encoding="utf-8")

    save_docs_markdowns(joins_df, marts_df, report_md)

    print("Explore-data audit finalizado.")
    print(f"Reporte MD: {REPORT_DIR / 'explore_data_audit_report.md'}")
    print(f"Reporte HTML: {REPORT_DIR / 'explore_data_audit_report.html'}")
    print(f"Issues: {REPORT_DIR / 'explore_data_issues_priorizados.csv'}")


if __name__ == "__main__":
    main()
