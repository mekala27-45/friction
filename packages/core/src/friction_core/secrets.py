"""Secrets read from the environment, checked before any client puts them in a header."""

from __future__ import annotations

import os


def single_line(name: str) -> str:
    """The variable's value without surrounding whitespace. A value that still holds whitespace was
    pasted wrong (two keys, or a key broken over two lines); the error names the variable, never
    the value, because an HTTP client that refuses such a header prints it."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is not set")
    if any(c.isspace() for c in value):
        raise SystemExit(f"{name} holds more than one line or a space; set it to the single key")
    return value
