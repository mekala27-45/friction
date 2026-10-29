"""Structured logs, JSON, ids only.

A log line never carries a card number, a user id in clear, an IP, a user agent or a token. The
processor below refuses those keys outright rather than trusting every call site.
"""

from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any

import structlog

FORBIDDEN_KEYS = frozenset(
    {
        "ip",
        "ip_address",
        "user_id",
        "card",
        "card_number",
        "cvv",
        "user_agent",
        "token",
        "password",
        "address",
    }
)


def _refuse_restricted(_: Any, __: str, event: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    bad = FORBIDDEN_KEYS.intersection(event)
    if bad:
        raise ValueError(f"refusing to log restricted keys: {sorted(bad)}")
    return event


def configure() -> None:
    structlog.configure(
        processors=[
            _refuse_restricted,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(sort_keys=True),
        ]
    )


def get(name: str) -> Any:
    return structlog.get_logger(name)
