"""Ingestión gobernada de fuentes operativas externas."""

from .contracts import ContractViolation, SourceContract, load_contract_catalog, validate_dataframe
from .service import IngestionConflict, IngestionResult, IngestionService

__all__ = [
    "ContractViolation",
    "IngestionConflict",
    "IngestionResult",
    "IngestionService",
    "SourceContract",
    "load_contract_catalog",
    "validate_dataframe",
]
