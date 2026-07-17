"""Consultas read-only y paginadas sobre marts y artefactos canónicos."""

from __future__ import annotations

import json
from datetime import date

import duckdb
import pandas as pd

from ..common import ProjectPaths, ensure_dirs, get_paths


def _records(frame: pd.DataFrame) -> list[dict]:
    if frame.empty:
        return []
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _page(frame: pd.DataFrame, *, limit: int, offset: int) -> tuple[list[dict], int]:
    total = len(frame)
    return _records(frame.iloc[offset : offset + limit]), total


class AnalyticsReadService:
    def __init__(self, paths: ProjectPaths | None = None) -> None:
        self.paths = ensure_dirs(paths or get_paths())

    def _read_csv(self, name: str) -> pd.DataFrame:
        path = self.paths.data_processed / name
        if not path.exists():
            raise FileNotFoundError(path)
        return pd.read_csv(path)

    def zones(
        self,
        *,
        risk_tier: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict], int]:
        columns = [
            "zona_id",
            "priority_rank",
            "investment_priority_score",
            "risk_tier",
            "urgency_tier",
            "recommended_intervention",
            "recommended_sequence",
            "main_risk_driver",
            "confidence_flag",
            "congestion_risk_score",
            "service_impact_score",
            "flexibility_gap_score",
            "asset_exposure_score",
            "electrification_pressure_score",
            "economic_priority_score",
            "capex_total",
            "coste_riesgo_proxy",
        ]
        frame = self._read_csv("intervention_scoring_table.csv")[columns]
        profile_path = self.paths.data_processed / "mart_zone_month_operational.csv"
        if profile_path.exists():
            profile_columns = ["zona_id", "zona_nombre", "tipo_zona", "region_operativa"]
            profile = pd.read_csv(profile_path, usecols=profile_columns).drop_duplicates("zona_id")
            frame = frame.merge(profile, on="zona_id", how="left", validate="one_to_one")
        if risk_tier:
            frame = frame[frame["risk_tier"] == risk_tier]
        return _page(frame.sort_values(["priority_rank", "zona_id"]), limit=limit, offset=offset)

    def feeders(
        self,
        *,
        zona_id: str | None,
        nivel_prioridad: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict], int]:
        frame = self._read_csv("prioridades_inversion_alimentadores.csv")
        if zona_id:
            frame = frame[frame["zona_id"] == zona_id]
        if nivel_prioridad:
            frame = frame[frame["nivel_prioridad"] == nivel_prioridad]
        return _page(frame.sort_values(["ranking_prioridad", "alimentador_id"]), limit=limit, offset=offset)

    def scenarios(self, *, limit: int, offset: int) -> tuple[list[dict], int]:
        frame = self._read_csv("scenario_summary.csv").sort_values(
            ["coste_riesgo_total", "scenario"], ascending=[False, True]
        )
        return _page(frame, limit=limit, offset=offset)

    def forecast_monitoring(self) -> dict:
        frame = self._read_csv("forecast_monitoring_status.csv")
        return _records(frame)[0] if not frame.empty else {}

    def _mart_page(
        self,
        *,
        table: str,
        time_column: str,
        start_date: date | None,
        end_date: date | None,
        zona_id: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict], int]:
        allowed = {
            "mart_zone_day_operational": "fecha",
            "mart_zone_month_operational": "mes",
        }
        if allowed.get(table) != time_column:
            raise ValueError("Mart no permitido")
        clauses: list[str] = []
        parameters: list = []
        if start_date:
            clauses.append(f"{time_column} >= ?")
            parameters.append(start_date)
        if end_date:
            clauses.append(f"{time_column} <= ?")
            parameters.append(end_date)
        if zona_id:
            clauses.append("zona_id = ?")
            parameters.append(zona_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        conn = duckdb.connect(str(self.paths.database), read_only=True)
        try:
            total = int(conn.execute(f"SELECT COUNT(*) FROM {table} {where}", parameters).fetchone()[0])
            frame = conn.execute(
                f"""
                SELECT * FROM {table} {where}
                ORDER BY {time_column}, zona_id
                LIMIT ? OFFSET ?
                """,
                [*parameters, limit, offset],
            ).df()
        finally:
            conn.close()
        return _records(frame), total

    def zone_day(
        self,
        *,
        start_date: date | None,
        end_date: date | None,
        zona_id: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict], int]:
        return self._mart_page(
            table="mart_zone_day_operational",
            time_column="fecha",
            start_date=start_date,
            end_date=end_date,
            zona_id=zona_id,
            limit=limit,
            offset=offset,
        )

    def zone_month(
        self,
        *,
        start_date: date | None,
        end_date: date | None,
        zona_id: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict], int]:
        return self._mart_page(
            table="mart_zone_month_operational",
            time_column="mes",
            start_date=start_date,
            end_date=end_date,
            zona_id=zona_id,
            limit=limit,
            offset=offset,
        )

    def health(self) -> dict:
        analytics_database = self.paths.database.exists()
        operations_database = self.paths.operations_database.exists()
        artifacts = [
            self.paths.data_processed / "intervention_scoring_table.csv",
            self.paths.data_processed / "prioridades_inversion_alimentadores.csv",
            self.paths.data_processed / "scenario_summary.csv",
        ]
        if analytics_database:
            conn = duckdb.connect(str(self.paths.database), read_only=True)
            try:
                conn.execute("SELECT 1").fetchone()
            except duckdb.Error:
                analytics_database = False
            finally:
                conn.close()
        return {
            "status": "ok"
            if analytics_database and operations_database and all(path.exists() for path in artifacts)
            else "degraded",
            "analytics_database": analytics_database,
            "operations_database": operations_database,
            "artifacts_ready": all(path.exists() for path in artifacts),
            "api_version": "1.0.0",
        }
