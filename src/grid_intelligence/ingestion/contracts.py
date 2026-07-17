"""Contratos tipados y validación de datasets externos."""

from __future__ import annotations

import json
from importlib.resources import files
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

DataType = Literal["string", "integer", "float", "boolean", "datetime", "date"]
LoadStrategy = Literal["snapshot", "upsert"]


class SourceContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_system: str = Field(min_length=1)
    target_table: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    load_strategy: LoadStrategy
    primary_key: list[str] = Field(min_length=1)
    event_time: str | None
    columns: dict[str, DataType] = Field(min_length=1)
    nullable_columns: set[str] = Field(default_factory=set)
    bounds: dict[str, tuple[float | None, float | None]] = Field(default_factory=dict)
    allow_extra_columns: bool = False
    min_rows: int = Field(default=1, ge=1)
    max_future_hours: int = Field(default=24, ge=0)

    @model_validator(mode="after")
    def validate_references(self) -> SourceContract:
        column_names = set(self.columns)
        unknown = set(self.primary_key) | self.nullable_columns | set(self.bounds)
        if self.event_time:
            unknown.add(self.event_time)
        missing = sorted(unknown.difference(column_names))
        if missing:
            raise ValueError(f"Referencias a columnas no declaradas: {', '.join(missing)}")
        if self.nullable_columns.intersection(self.primary_key):
            raise ValueError("Las claves primarias no pueden ser anulables")
        return self


class ContractCatalog(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_version: str = Field(pattern=r"^\d+\.\d+\.\d+$")
    contracts: dict[str, SourceContract] = Field(min_length=1)


class ContractViolation(ValueError):
    def __init__(self, issues: list[str]) -> None:
        self.issues = issues
        super().__init__("; ".join(issues))


def load_contract_catalog(path: Path | None = None) -> ContractCatalog:
    """Carga el catálogo incluido en el paquete o una ruta gobernada alternativa."""
    if path is None:
        raw = files("grid_intelligence").joinpath("resources/source_contracts.json").read_text(encoding="utf-8")
    else:
        raw = path.read_text(encoding="utf-8")
    payload = json.loads(raw)
    return ContractCatalog.model_validate(payload)


def read_source_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Formato no soportado: {suffix}; use CSV o Parquet")


def _coerce_boolean(series: pd.Series) -> pd.Series:
    mapping = {
        "true": True,
        "1": True,
        "yes": True,
        "si": True,
        "sí": True,
        "false": False,
        "0": False,
        "no": False,
    }
    normalized = series.astype("string").str.strip().str.lower()
    return normalized.map(mapping).astype("boolean")


def _coerce_column(series: pd.Series, dtype: DataType) -> pd.Series:
    if dtype == "string":
        return series.astype("string").str.strip().replace("", pd.NA)
    if dtype == "float":
        return pd.to_numeric(series, errors="coerce").astype(float)
    if dtype == "integer":
        numeric = pd.to_numeric(series, errors="coerce")
        non_integer = numeric.notna() & (numeric % 1 != 0)
        numeric = numeric.mask(non_integer)
        return numeric.astype("Int64")
    if dtype == "boolean":
        return _coerce_boolean(series)
    if dtype == "datetime":
        parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
        return parsed.dt.tz_convert(None)
    if dtype == "date":
        parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
        return parsed.dt.date
    raise ValueError(f"Tipo no soportado: {dtype}")


def validate_dataframe(dataframe: pd.DataFrame, contract: SourceContract) -> pd.DataFrame:
    """Valida esquema, tipos, nulos, claves, límites y temporalidad."""
    issues: list[str] = []
    expected = list(contract.columns)
    actual = list(dataframe.columns)
    missing = sorted(set(expected).difference(actual))
    extras = sorted(set(actual).difference(expected))
    if missing:
        issues.append(f"missing_columns:{','.join(missing)}")
    if extras and not contract.allow_extra_columns:
        issues.append(f"unexpected_columns:{','.join(extras)}")
    if len(dataframe) < contract.min_rows:
        issues.append(f"insufficient_rows:{len(dataframe)}<{contract.min_rows}")
    if issues:
        raise ContractViolation(issues)

    result = dataframe.copy()
    original_not_null = {column: result[column].notna() for column in expected}
    for column, dtype in contract.columns.items():
        result[column] = _coerce_column(result[column], dtype)
        invalid_count = int((original_not_null[column] & result[column].isna()).sum())
        if invalid_count:
            issues.append(f"invalid_type:{column}:{invalid_count}")

    required_columns = set(expected).difference(contract.nullable_columns)
    for column in sorted(required_columns):
        null_count = int(result[column].isna().sum())
        if null_count:
            issues.append(f"null_not_allowed:{column}:{null_count}")

    if not result.empty:
        duplicate_count = int(result.duplicated(subset=contract.primary_key, keep=False).sum())
        if duplicate_count:
            issues.append(f"duplicate_primary_key:{duplicate_count}")

    for column, (minimum, maximum) in contract.bounds.items():
        values = pd.to_numeric(result[column], errors="coerce")
        if minimum is not None:
            below = int((values < minimum).sum())
            if below:
                issues.append(f"below_minimum:{column}:{below}")
        if maximum is not None:
            above = int((values > maximum).sum())
            if above:
                issues.append(f"above_maximum:{column}:{above}")

    if contract.event_time:
        event_time = pd.to_datetime(result[contract.event_time], errors="coerce", utc=True)
        future_limit = pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=contract.max_future_hours)
        future_count = int((event_time > future_limit).sum())
        if future_count:
            issues.append(f"future_event_time:{future_count}")

    if issues:
        raise ContractViolation(issues)
    return result[expected + extras] if contract.allow_extra_columns else result[expected]
