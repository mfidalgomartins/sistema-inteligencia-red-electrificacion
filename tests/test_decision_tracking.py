import pandas as pd
import pytest

from grid_intelligence.common import ProjectPaths, ensure_dirs
from grid_intelligence.decision_tracking import (
    DecisionConflict,
    DecisionCreate,
    DecisionService,
    DecisionTransition,
    InvalidTransition,
)


def _service(tmp_path) -> DecisionService:
    paths = ensure_dirs(ProjectPaths(tmp_path))
    pd.DataFrame({"zona_id": ["Z001"], "investment_priority_score": [88.0]}).to_csv(
        paths.data_processed / "intervention_scoring_table.csv", index=False
    )
    pd.DataFrame({"alimentador_id": ["A001"], "zona_id": ["Z001"]}).to_csv(
        paths.data_processed / "prioridades_inversion_alimentadores.csv", index=False
    )
    return DecisionService(paths)


def _request() -> DecisionCreate:
    return DecisionCreate(
        idempotency_key="portfolio-case-001",
        zona_id="Z001",
        alimentador_id="A001",
        recomendacion="refuerzo_selectivo",
        owner="Planificación de red",
        expected_capex_eur=1_000_000,
        expected_annual_benefit_eur=180_000,
        expected_risk_reduction_pct=25,
    )


def test_decision_creation_is_idempotent_and_audited(tmp_path):
    service = _service(tmp_path)
    created, replayed = service.create(_request())
    replay, replayed_again = service.create(_request())

    assert replayed is False
    assert replayed_again is True
    assert replay["decision_id"] == created["decision_id"]
    assert service.events(created["decision_id"])[0]["event_type"] == "created"

    different = DecisionCreate(**{**_request().__dict__, "expected_capex_eur": 2_000_000})
    with pytest.raises(DecisionConflict):
        service.create(different)


def test_decision_lifecycle_enforces_transition_and_optimistic_version(tmp_path):
    service = _service(tmp_path)
    decision, _ = service.create(_request())

    with pytest.raises(InvalidTransition):
        service.transition(
            decision["decision_id"],
            DecisionTransition(to_status="approved", expected_version=1, actor="Comité"),
        )

    for version, status in enumerate(("under_review", "approved", "in_execution"), start=1):
        decision = service.transition(
            decision["decision_id"],
            DecisionTransition(to_status=status, expected_version=version, actor="Comité"),
        )

    with pytest.raises(DecisionConflict):
        service.transition(
            decision["decision_id"],
            DecisionTransition(to_status="implemented", expected_version=2, actor="PMO"),
        )

    decision = service.transition(
        decision["decision_id"],
        DecisionTransition(
            to_status="implemented",
            expected_version=4,
            actor="PMO",
            actual_capex_eur=980_000,
        ),
    )
    with pytest.raises(InvalidTransition, match="beneficio"):
        service.transition(
            decision["decision_id"],
            DecisionTransition(to_status="verified", expected_version=5, actor="Control de gestión"),
        )

    verified = service.transition(
        decision["decision_id"],
        DecisionTransition(
            to_status="verified",
            expected_version=5,
            actor="Control de gestión",
            actual_annual_benefit_eur=190_000,
            actual_risk_reduction_pct=27,
        ),
    )
    metrics = service.portfolio_metrics()

    assert verified["status"] == "verified"
    assert metrics["verified_decisions"] == 1
    assert metrics["benefit_realization_ratio"] == pytest.approx(190_000 / 180_000)
