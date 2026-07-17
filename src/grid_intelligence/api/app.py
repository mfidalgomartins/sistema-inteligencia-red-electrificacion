"""Aplicación FastAPI: lectura analítica y mutaciones autenticadas de decisiones."""

from __future__ import annotations

import logging
import os
import re
import secrets
from dataclasses import dataclass
from datetime import date
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse

from ..common import ProjectPaths, get_paths
from ..decision_tracking import (
    DecisionConflict,
    DecisionNotFound,
    DecisionReferenceError,
    DecisionService,
    DecisionStatus,
    InvalidTransition,
)
from .data_service import AnalyticsReadService
from .models import (
    DecisionCreateRequest,
    DecisionEventResponse,
    DecisionResponse,
    DecisionTransitionRequest,
    HealthResponse,
    PageResponse,
)

LOGGER = logging.getLogger(__name__)
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,100}$")


@dataclass(frozen=True)
class ApiSettings:
    mutation_api_key: str | None = None

    @classmethod
    def from_environment(cls) -> ApiSettings:
        return cls(mutation_api_key=os.getenv("GRID_API_KEY"))


def _require_mutation_key(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    expected = request.app.state.settings.mutation_api_key
    if not expected:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="mutation_auth_not_configured")
    if x_api_key is None or not secrets.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid_api_key")


def create_app(
    *,
    paths: ProjectPaths | None = None,
    settings: ApiSettings | None = None,
) -> FastAPI:
    effective_paths = paths or get_paths()
    app = FastAPI(
        title="Grid Intelligence API",
        version="1.0.0",
        description="Marts analíticos y ciclo de decisiones de electrificación de red.",
    )
    app.state.settings = settings or ApiSettings.from_environment()
    app.state.analytics = AnalyticsReadService(effective_paths)
    app.state.decisions = DecisionService(effective_paths)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        supplied = request.headers.get("X-Request-ID", "")
        request_id = supplied if REQUEST_ID_PATTERN.fullmatch(supplied) else str(uuid4())
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        LOGGER.info(
            "api_request request_id=%s method=%s path=%s status=%s",
            request_id,
            request.method,
            request.url.path,
            response.status_code,
        )
        return response

    @app.exception_handler(DecisionNotFound)
    async def decision_not_found(_: Request, exc: DecisionNotFound):
        return JSONResponse(status_code=404, content={"detail": "decision_not_found", "decision_id": str(exc)})

    @app.exception_handler(DecisionConflict)
    async def decision_conflict(_: Request, exc: DecisionConflict):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(InvalidTransition)
    @app.exception_handler(DecisionReferenceError)
    async def invalid_decision(_: Request, exc: ValueError):
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(FileNotFoundError)
    async def missing_artifact(_: Request, exc: FileNotFoundError):
        LOGGER.warning("api_artifact_missing path=%s", exc)
        return JSONResponse(status_code=503, content={"detail": "analytical_artifact_unavailable"})

    def page(items: list[dict], total: int, limit: int, offset: int) -> PageResponse:
        return PageResponse(items=items, total=total, limit=limit, offset=offset)

    @app.get("/health", response_model=HealthResponse, tags=["operations"])
    def health() -> dict:
        return app.state.analytics.health()

    @app.get("/v1/zones", response_model=PageResponse, tags=["analytics"])
    def zones(
        risk_tier: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> PageResponse:
        items, total = app.state.analytics.zones(risk_tier=risk_tier, limit=limit, offset=offset)
        return page(items, total, limit, offset)

    @app.get("/v1/feeders", response_model=PageResponse, tags=["analytics"])
    def feeders(
        zona_id: str | None = None,
        nivel_prioridad: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> PageResponse:
        items, total = app.state.analytics.feeders(
            zona_id=zona_id,
            nivel_prioridad=nivel_prioridad,
            limit=limit,
            offset=offset,
        )
        return page(items, total, limit, offset)

    @app.get("/v1/scenarios", response_model=PageResponse, tags=["analytics"])
    def scenarios(
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> PageResponse:
        items, total = app.state.analytics.scenarios(limit=limit, offset=offset)
        return page(items, total, limit, offset)

    @app.get("/v1/forecast-monitoring", tags=["analytics"])
    def forecast_monitoring() -> dict:
        return app.state.analytics.forecast_monitoring()

    @app.get("/v1/marts/zone-day", response_model=PageResponse, tags=["marts"])
    def zone_day(
        start_date: date | None = None,
        end_date: date | None = None,
        zona_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> PageResponse:
        items, total = app.state.analytics.zone_day(
            start_date=start_date,
            end_date=end_date,
            zona_id=zona_id,
            limit=limit,
            offset=offset,
        )
        return page(items, total, limit, offset)

    @app.get("/v1/marts/zone-month", response_model=PageResponse, tags=["marts"])
    def zone_month(
        start_date: date | None = None,
        end_date: date | None = None,
        zona_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> PageResponse:
        items, total = app.state.analytics.zone_month(
            start_date=start_date,
            end_date=end_date,
            zona_id=zona_id,
            limit=limit,
            offset=offset,
        )
        return page(items, total, limit, offset)

    @app.get("/v1/decisions", response_model=PageResponse, tags=["decisions"])
    def decisions(
        decision_status: Annotated[DecisionStatus | None, Query(alias="status")] = None,
        zona_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> PageResponse:
        items, total = app.state.decisions.list(
            status=decision_status,
            zona_id=zona_id,
            limit=limit,
            offset=offset,
        )
        return page(items, total, limit, offset)

    @app.post(
        "/v1/decisions",
        response_model=DecisionResponse,
        status_code=201,
        tags=["decisions"],
        dependencies=[Depends(_require_mutation_key)],
    )
    def create_decision(payload: DecisionCreateRequest, response: Response) -> dict:
        decision, replay = app.state.decisions.create(payload.to_domain())
        if replay:
            response.status_code = 200
        return decision

    @app.get("/v1/decisions/metrics", tags=["decisions"])
    def decision_metrics() -> dict:
        return app.state.decisions.portfolio_metrics()

    @app.get("/v1/decisions/{decision_id}", response_model=DecisionResponse, tags=["decisions"])
    def get_decision(decision_id: str) -> dict:
        return app.state.decisions.get(decision_id)

    @app.get(
        "/v1/decisions/{decision_id}/events",
        response_model=list[DecisionEventResponse],
        tags=["decisions"],
    )
    def decision_events(decision_id: str) -> list[dict]:
        return app.state.decisions.events(decision_id)

    @app.post(
        "/v1/decisions/{decision_id}/transitions",
        response_model=DecisionResponse,
        tags=["decisions"],
        dependencies=[Depends(_require_mutation_key)],
    )
    def transition_decision(decision_id: str, payload: DecisionTransitionRequest) -> dict:
        return app.state.decisions.transition(decision_id, payload.to_domain())

    return app


app = create_app()
