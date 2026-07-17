"""Persistencia transaccional para ingestión, ejecución incremental y decisiones."""

from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager
from importlib.resources import files
from pathlib import Path
from typing import Any

import duckdb

from .common import ProjectPaths, ensure_dirs, get_paths


class OperationsStore:
    """Abre conexiones cortas y aplica migraciones idempotentes de DuckDB."""

    def __init__(self, paths: ProjectPaths | None = None) -> None:
        self.paths = ensure_dirs(paths or get_paths())
        self.database_path = self.paths.operations_database
        self.migrate()

    @contextmanager
    def connection(self, *, read_only: bool = False):
        conn = duckdb.connect(str(self.database_path), read_only=read_only)
        try:
            yield conn
        finally:
            conn.close()

    def migrate(self) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version VARCHAR PRIMARY KEY,
                    applied_at TIMESTAMP NOT NULL
                )
                """
            )
            applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations").fetchall()}
            migration_root = files("grid_intelligence").joinpath("resources/migrations")
            for migration in sorted(migration_root.iterdir(), key=lambda item: item.name):
                if not migration.name.endswith(".sql") or migration.name in applied:
                    continue
                conn.execute("BEGIN")
                try:
                    conn.execute(migration.read_text(encoding="utf-8"))
                    conn.execute(
                        "INSERT INTO schema_migrations VALUES (?, CURRENT_TIMESTAMP)",
                        [migration.name],
                    )
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

    def fetch_one(self, query: str, parameters: Iterable[Any] = ()) -> dict[str, Any] | None:
        with self.connection(read_only=True) as conn:
            cursor = conn.execute(query, list(parameters))
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [item[0] for item in cursor.description]
            return dict(zip(columns, row, strict=True))

    def fetch_all(self, query: str, parameters: Iterable[Any] = ()) -> list[dict[str, Any]]:
        with self.connection(read_only=True) as conn:
            cursor = conn.execute(query, list(parameters))
            columns = [item[0] for item in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def execute(self, query: str, parameters: Iterable[Any] = ()) -> None:
        with self.connection() as conn:
            conn.execute(query, list(parameters))


def safe_relative_path(path: Path, root: Path) -> str:
    """Guarda rutas operativas relativas a la raíz para que el proyecto sea movible."""
    return path.resolve().relative_to(root.resolve()).as_posix()
