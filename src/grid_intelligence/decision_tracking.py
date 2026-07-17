"""Ciclo de vida auditable de decisiones de red y realización de beneficios."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Literal
from uuid import uuid4

import pandas as pd

from .common import ProjectPaths, ensure_dirs, get_paths
from .operations_store import OperationsStore

DecisionStatus = Literal[
    "proposed",
    "under_review",
    "approved",
    "rejected",
    "in_execution",
    "implemented",
    "verified",
    "cancelled",
]

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "proposed": {"under_review", "cancelled"},
    "under_review": {"approved", "rejected", "cancelled"},
    "approved": {"in_execution", "cancelled"},
    "rejected": set(),
    "in_execution": {"implemented", "cancelled"},
    "implemented": {"verified"},
    "verified": set(),
    "cancelled": set(),
}


class DecisionNotFound(LookupError):
    pass


class DecisionConflict(RuntimeError):
    pass


class InvalidTransition(ValueError):
    pass


class DecisionReferenceError(ValueError):
    pass


def _validate_non_negative(name: str, value: float | None) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{name} no puede ser negativo")


def _validate_percentage(name: str, value: float | None) -> None:
    if value is not None and not 0 <= value <= 100:
        raise ValueError(f"{name} debe estar entre 0 y 100")


@dataclass(frozen=True)
class DecisionCreate:
    idempotency_key: str
    zona_id: str
    recomendacion: str
    owner: str
    alimentador_id: str | None = None
    expected_capex_eur: float | None = None
    expected_annual_benefit_eur: float | None = None
    expected_risk_reduction_pct: float | None = None
    verification_due_date: date | None = None
    currency: str = "EUR"

    def __post_init__(self) -> None:
        for field_name in ("idempotency_key", "zona_id", "recomendacion", "owner"):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"{field_name} es obligatorio")
        if len(self.idempotency_key) > 120:
            raise ValueError("idempotency_key excede 120 caracteres")
        if self.currency != "EUR":
            raise ValueError("La versión actual gobierna importes exclusivamente en EUR")
        _validate_non_negative("expected_capex_eur", self.expected_capex_eur)
        _validate_non_negative("expected_annual_benefit_eur", self.expected_annual_benefit_eur)
        _validate_percentage("expected_risk_reduction_pct", self.expected_risk_reduction_pct)


@dataclass(frozen=True)
class DecisionTransition:
    to_status: DecisionStatus
    expected_version: int
    actor: str
    notes: str | None = None
    actual_capex_eur: float | None = None
    actual_annual_benefit_eur: float | None = None
    actual_risk_reduction_pct: float | None = None

    def __post_init__(self) -> None:
        if self.expected_version < 1:
            raise ValueError("expected_version debe ser positivo")
        if not self.actor.strip():
            raise ValueError("actor es obligatorio")
        _validate_non_negative("actual_capex_eur", self.actual_capex_eur)
        _validate_non_negative("actual_annual_benefit_eur", self.actual_annual_benefit_eur)
        _validate_percentage("actual_risk_reduction_pct", self.actual_risk_reduction_pct)


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _canonical_hash(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class DecisionService:
    def __init__(self, paths: ProjectPaths | None = None) -> None:
        self.paths = ensure_dirs(paths or get_paths())
        self.store = OperationsStore(self.paths)

    def _validate_references(self, request: DecisionCreate) -> None:
        zones_path = self.paths.data_processed / "intervention_scoring_table.csv"
        if not zones_path.exists():
            raise DecisionReferenceError("No existe el ranking zonal canónico")
        zones = pd.read_csv(zones_path, usecols=["zona_id"])
        if request.zona_id not in set(zones["zona_id"].astype(str)):
            raise DecisionReferenceError(f"zona_id desconocida: {request.zona_id}")
        if request.alimentador_id is None:
            return
        feeders_path = self.paths.data_processed / "prioridades_inversion_alimentadores.csv"
        if not feeders_path.exists():
            raise DecisionReferenceError("No existe el ranking de alimentadores canónico")
        feeders = pd.read_csv(feeders_path, usecols=["alimentador_id", "zona_id"])
        matches = feeders[feeders["alimentador_id"].astype(str) == request.alimentador_id]
        if matches.empty:
            raise DecisionReferenceError(f"alimentador_id desconocido: {request.alimentador_id}")
        if str(matches.iloc[0]["zona_id"]) != request.zona_id:
            raise DecisionReferenceError("El alimentador no pertenece a la zona indicada")

    def get(self, decision_id: str) -> dict:
        decision = self.store.fetch_one("SELECT * FROM decisions WHERE decision_id = ?", [decision_id])
        if decision is None:
            raise DecisionNotFound(decision_id)
        return decision

    def events(self, decision_id: str) -> list[dict]:
        self.get(decision_id)
        return self.store.fetch_all(
            "SELECT * FROM decision_events WHERE decision_id = ? ORDER BY occurred_at, event_id",
            [decision_id],
        )

    def create(self, request: DecisionCreate) -> tuple[dict, bool]:
        self._validate_references(request)
        request_payload = asdict(request)
        request_hash = _canonical_hash(request_payload)
        existing = self.store.fetch_one(
            "SELECT decision_id, request_hash FROM decisions WHERE idempotency_key = ?",
            [request.idempotency_key],
        )
        if existing:
            if existing["request_hash"] != request_hash:
                raise DecisionConflict("idempotency_key reutilizada con otra solicitud")
            return self.get(str(existing["decision_id"])), True

        now = _utc_now()
        decision_id = str(uuid4())
        event_id = str(uuid4())
        with self.store.connection() as conn:
            conn.execute("BEGIN")
            try:
                conn.execute(
                    """
                    INSERT INTO decisions (
                        decision_id, idempotency_key, request_hash, zona_id, alimentador_id,
                        recomendacion, owner, status, currency, expected_capex_eur,
                        expected_annual_benefit_eur, expected_risk_reduction_pct,
                        verification_due_date, created_at, updated_at, version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'proposed', ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    [
                        decision_id,
                        request.idempotency_key,
                        request_hash,
                        request.zona_id,
                        request.alimentador_id,
                        request.recomendacion,
                        request.owner,
                        request.currency,
                        request.expected_capex_eur,
                        request.expected_annual_benefit_eur,
                        request.expected_risk_reduction_pct,
                        request.verification_due_date,
                        now,
                        now,
                    ],
                )
                conn.execute(
                    """
                    INSERT INTO decision_events VALUES (?, ?, 'created', NULL, 'proposed', ?, NULL, ?, ?)
                    """,
                    [event_id, decision_id, request.owner, json.dumps(request_payload, default=str), now],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self.get(decision_id), False

    def transition(self, decision_id: str, transition: DecisionTransition) -> dict:
        current = self.get(decision_id)
        current_status = str(current["status"])
        if int(current["version"]) != transition.expected_version:
            raise DecisionConflict(
                f"Versión obsoleta: esperada {transition.expected_version}, actual {current['version']}"
            )
        if transition.to_status not in ALLOWED_TRANSITIONS[current_status]:
            raise InvalidTransition(f"Transición no permitida: {current_status} -> {transition.to_status}")

        actual_capex = (
            transition.actual_capex_eur if transition.actual_capex_eur is not None else current["actual_capex_eur"]
        )
        actual_benefit = (
            transition.actual_annual_benefit_eur
            if transition.actual_annual_benefit_eur is not None
            else current["actual_annual_benefit_eur"]
        )
        actual_risk = (
            transition.actual_risk_reduction_pct
            if transition.actual_risk_reduction_pct is not None
            else current["actual_risk_reduction_pct"]
        )
        if transition.to_status == "verified" and (actual_benefit is None or actual_risk is None):
            raise InvalidTransition("La verificación exige beneficio y reducción de riesgo observados")

        now = _utc_now()
        approved_at = now if transition.to_status == "approved" else current["approved_at"]
        execution_started_at = now if transition.to_status == "in_execution" else current["execution_started_at"]
        implemented_at = now if transition.to_status == "implemented" else current["implemented_at"]
        verified_at = now if transition.to_status == "verified" else current["verified_at"]
        payload = {
            "actual_capex_eur": transition.actual_capex_eur,
            "actual_annual_benefit_eur": transition.actual_annual_benefit_eur,
            "actual_risk_reduction_pct": transition.actual_risk_reduction_pct,
        }

        with self.store.connection() as conn:
            conn.execute("BEGIN")
            try:
                updated = conn.execute(
                    """
                    UPDATE decisions
                    SET status = ?, actual_capex_eur = ?, actual_annual_benefit_eur = ?,
                        actual_risk_reduction_pct = ?, approved_at = ?, execution_started_at = ?,
                        implemented_at = ?, verified_at = ?, updated_at = ?, version = version + 1
                    WHERE decision_id = ? AND version = ?
                    RETURNING decision_id
                    """,
                    [
                        transition.to_status,
                        actual_capex,
                        actual_benefit,
                        actual_risk,
                        approved_at,
                        execution_started_at,
                        implemented_at,
                        verified_at,
                        now,
                        decision_id,
                        transition.expected_version,
                    ],
                ).fetchone()
                if updated is None:
                    raise DecisionConflict("La decisión fue modificada concurrentemente")
                conn.execute(
                    """
                    INSERT INTO decision_events VALUES (?, ?, 'status_changed', ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(uuid4()),
                        decision_id,
                        current_status,
                        transition.to_status,
                        transition.actor,
                        transition.notes,
                        json.dumps(payload, ensure_ascii=False),
                        now,
                    ],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self.get(decision_id)

    def list(
        self,
        *,
        status: str | None = None,
        zona_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        clauses: list[str] = []
        parameters: list = []
        if status:
            if status not in ALLOWED_TRANSITIONS:
                raise ValueError(f"status desconocido: {status}")
            clauses.append("status = ?")
            parameters.append(status)
        if zona_id:
            clauses.append("zona_id = ?")
            parameters.append(zona_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        total = self.store.fetch_one(f"SELECT COUNT(*) AS n FROM decisions {where}", parameters)
        rows = self.store.fetch_all(
            f"""
            SELECT * FROM decisions {where}
            ORDER BY updated_at DESC, decision_id
            LIMIT ? OFFSET ?
            """,
            [*parameters, limit, offset],
        )
        return rows, int(total["n"] if total else 0)

    def portfolio_metrics(self) -> dict:
        summary = (
            self.store.fetch_one(
                """
            SELECT
                COUNT(*) AS total_decisions,
                COUNT(*) FILTER (WHERE status = 'verified') AS verified_decisions,
                COALESCE(SUM(expected_capex_eur) FILTER (WHERE status IN ('approved', 'in_execution', 'implemented', 'verified')), 0) AS approved_capex_eur,
                COALESCE(SUM(actual_capex_eur) FILTER (WHERE status IN ('implemented', 'verified')), 0) AS actual_capex_eur,
                COALESCE(SUM(expected_annual_benefit_eur) FILTER (WHERE status IN ('implemented', 'verified')), 0) AS expected_annual_benefit_eur,
                COALESCE(SUM(actual_annual_benefit_eur) FILTER (WHERE status IN ('implemented', 'verified')), 0) AS actual_annual_benefit_eur,
                AVG(DATE_DIFF('day', created_at, implemented_at)) FILTER (WHERE implemented_at IS NOT NULL) AS avg_implementation_days
            FROM decisions
            """
            )
            or {}
        )
        by_status = self.store.fetch_all(
            "SELECT status, COUNT(*) AS decisions FROM decisions GROUP BY status ORDER BY status"
        )
        expected = float(summary.get("expected_annual_benefit_eur") or 0)
        actual = float(summary.get("actual_annual_benefit_eur") or 0)
        summary["benefit_realization_ratio"] = actual / expected if expected else None
        summary["by_status"] = by_status
        return summary
