"""Servidor ASGI configurable por variables de entorno."""

from __future__ import annotations

import os

import uvicorn


def main() -> None:
    host = os.getenv("GRID_API_HOST", "127.0.0.1")
    port = int(os.getenv("GRID_API_PORT", "8000"))
    log_level = os.getenv("GRID_API_LOG_LEVEL", "info").lower()
    uvicorn.run("grid_intelligence.api.app:app", host=host, port=port, log_level=log_level)


if __name__ == "__main__":
    main()
