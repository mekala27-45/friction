"""The ASGI entry point: ``uvicorn friction_api.main:app``."""

from __future__ import annotations

from friction_api.app import from_environment

app = from_environment()
