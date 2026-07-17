from datetime import date

import pandas as pd
import pytest

from grid_intelligence.common import ProjectPaths
from grid_intelligence.ingestion import (
    ContractViolation,
    IngestionConflict,
    IngestionService,
    load_contract_catalog,
    validate_dataframe,
)


def _demand_frame(periods: int = 3) -> pd.DataFrame:
    timestamps = pd.date_range("2025-01-01", periods=periods, freq="h")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "zona_id": ["Z001"] * periods,
            "subestacion_id": ["S001"] * periods,
            "alimentador_id": ["A001"] * periods,
            "demanda_mw": [10.0 + index for index in range(periods)],
            "demanda_reactiva_proxy": [2.0] * periods,
            "temperatura": [20.0] * periods,
            "humedad": [55.0] * periods,
            "tipo_dia": ["laborable"] * periods,
            "mes": [1] * periods,
            "hora": timestamps.hour,
            "factor_estacional": [1.0] * periods,
            "hora_punta_flag": [False] * periods,
            "tension_sistema_proxy": [1.0] * periods,
        }
    )


def test_catalog_covers_authoritative_source_systems():
    catalog = load_contract_catalog()
    source_systems = {contract.source_system for contract in catalog.contracts.values()}

    assert {"SCADA_AMI", "GIS", "OMS", "EAM", "CAPEX"}.issubset(source_systems)
    assert len(catalog.contracts) == 15


def test_contract_rejects_duplicate_key_and_invalid_bounds():
    contract = load_contract_catalog().contracts["scada_ami_demanda"]
    frame = _demand_frame(2)
    frame.loc[1, "timestamp"] = frame.loc[0, "timestamp"]
    frame.loc[0, "humedad"] = 140

    with pytest.raises(ContractViolation) as exc_info:
        validate_dataframe(frame, contract)

    assert "duplicate_primary_key" in str(exc_info.value)
    assert "above_maximum:humedad" in str(exc_info.value)


def test_ingestion_is_idempotent_and_promotes_canonical_raw(tmp_path):
    paths = ProjectPaths(tmp_path)
    source = tmp_path / "demand.csv"
    _demand_frame().to_csv(source, index=False)
    service = IngestionService(paths)

    first = service.ingest("scada_ami_demanda", source, "batch-001")
    replay = service.ingest("scada_ami_demanda", source, "batch-001")
    promoted_path, row_count = service.promote("scada_ami_demanda")

    assert first.idempotent_replay is False
    assert replay.idempotent_replay is True
    assert len(first.partitions) == 1
    assert row_count == 3
    assert promoted_path == tmp_path / "data/raw/demanda_horaria.csv"
    assert pd.read_csv(promoted_path)["alimentador_id"].tolist() == ["A001"] * 3

    _demand_frame(2).to_csv(source, index=False)
    with pytest.raises(IngestionConflict):
        service.ingest("scada_ami_demanda", source, "batch-001")


def test_snapshot_partition_uses_governed_as_of_date(tmp_path):
    paths = ProjectPaths(tmp_path)
    source = tmp_path / "zones.csv"
    frame = pd.DataFrame(
        [
            {
                "zona_id": "Z001",
                "zona_nombre": "Zona uno",
                "comunidad_autonoma": "CA",
                "provincia": "Provincia",
                "tipo_zona": "urbana",
                "region_operativa": "Centro",
                "densidad_demanda": 60.0,
                "penetracion_generacion_distribuida": 0.2,
                "criticidad_territorial": 70.0,
                "potencial_flexibilidad": 45.0,
                "riesgo_climatico": 30.0,
                "tension_crecimiento_demanda": 55.0,
            }
        ]
    )
    frame.to_csv(source, index=False)

    result = IngestionService(paths).ingest("gis_zonas", source, "snapshot-001", as_of_date=date(2025, 1, 31))

    assert result.partitions[0].partition_value == "2025-01-31"
    assert "snapshot_date=2025-01-31" in result.partitions[0].path


def test_idempotent_replay_rejects_tampered_partition(tmp_path):
    paths = ProjectPaths(tmp_path)
    source = tmp_path / "demand.csv"
    _demand_frame().to_csv(source, index=False)
    service = IngestionService(paths)
    result = service.ingest("scada_ami_demanda", source, "batch-integrity")
    partition = tmp_path / result.partitions[0].path
    with partition.open("ab") as handle:
        handle.write(b"tampered")

    with pytest.raises(IngestionConflict, match="alterada"):
        service.ingest("scada_ami_demanda", source, "batch-integrity")
