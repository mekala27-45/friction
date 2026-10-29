"""Digests used for the lock files, the drops, the checkpoints, the evaluation plan and the gold tables.

Every digest is SHA-256 over a canonical form, so the same input always gives the same key on any
machine and under any PYTHONHASHSEED.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def canonical_json(value: Any) -> str:
    """JSON with sorted keys, no spaces and no NaN, the form every digest is taken over."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False, default=str
    )


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_text(canonical_json(value))


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def short(digest: str, n: int = 12) -> str:
    return digest[:n]
