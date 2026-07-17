"""Contratos HTTP versionados de la API."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from ..decision_tracking import DecisionCreate, DecisionTransition


class PageResponse(BaseModel):
    items: list[dict[str, Any]]
    total: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)


class DecisionCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    idempotency_key: str = Field(min_length=1, max_length=120)
    zona_id: str = Field(min_length=1, max_length=50)
    alimentador_id: str | None = Field(default=None, max_length=50)
    recomendacion: str = Field(min_length=1, max_length=200)
    owner: str = Field(min_length=1, max_length=200)
    expected_capex_eur: float | None = Field(default=None, ge=0)
    expected_annual_benefit_eur: float | None = Field(default=None, ge=0)
    expected_risk_reduction_pct: float | None = Field(default=None, ge=0, le=100)
    verification_due_date: date | None = None
    currency: Literal["EUR"] = "EUR"

    def to_domain(self) -> DecisionCreate:
        return DecisionCreate(**self.model_dump())


DecisionStatus = Literal[
    "under_review",
    "approved",
    "rejected",
    "in_execution",
    "implemented",
    "verified",
    "cancelled",
]


class DecisionTransitionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    to_status: DecisionStatus
    expected_version: int = Field(ge=1)
    actor: str = Field(min_length=1, max_length=200)
    notes: str | None = Field(default=None, max_length=2000)
    actual_capex_eur: float | None = Field(default=None, ge=0)
    actual_annual_benefit_eur: float | None = Field(default=None, ge=0)
    actual_risk_reduction_pct: float | None = Field(default=None, ge=0, le=100)

    def to_domain(self) -> DecisionTransition:
        return DecisionTransition(**self.model_dump())


class DecisionResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    decision_id: str
    idempotency_key: str
    zona_id: str
    alimentador_id: str | None
    recomendacion: str
    owner: str
    status: str
    currency: str
    expected_capex_eur: float | None
    expected_annual_benefit_eur: float | None
    expected_risk_reduction_pct: float | None
    actual_capex_eur: float | None
    actual_annual_benefit_eur: float | None
    actual_risk_reduction_pct: float | None
    verification_due_date: date | None
    created_at: datetime
    updated_at: datetime
    approved_at: datetime | None
    execution_started_at: datetime | None
    implemented_at: datetime | None
    verified_at: datetime | None
    version: int


class DecisionEventResponse(BaseModel):
    event_id: str
    decision_id: str
    event_type: str
    from_status: str | None
    to_status: str
    actor: str
    notes: str | None
    payload_json: str
    occurred_at: datetime


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    analytics_database: bool
    operations_database: bool
    artifacts_ready: bool
    api_version: str
