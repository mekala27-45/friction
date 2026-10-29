"""Number formats, ported from doorstep and readback with the rates this build needs.

A value is stored raw in the manifest with a named format, so a number in a document and the same
number on the site cannot be printed two ways. A unit is part of a value: every money format prints
its currency, and every rate format says per what.
"""

from __future__ import annotations

import math
from collections.abc import Callable

FormatFn = Callable[[float | int | str | None], str]


def _num(value: float | int | str | None) -> float:
    if value is None or isinstance(value, str):
        raise TypeError(f"expected a number, got {value!r}")
    return float(value)


def _is_zero(text: str) -> bool:
    digits = text.replace(",", "").lstrip("+-")
    return float(digits) == 0


def _plain(value: float, decimals: int) -> str:
    text = f"{value:,.{decimals}f}"
    # A value that rounds to zero prints without a sign, whatever its sign before rounding.
    return text[1:] if text.startswith("-") and _is_zero(text) else text


def _signed(value: float, decimals: int) -> str:
    text = _plain(value, decimals)
    return f"+{text}" if value > 0 and not _is_zero(text) else text


def fmt_int(value: float | int | str | None) -> str:
    return f"{round(_num(value)):,}"


def _fixed(decimals: int) -> FormatFn:
    def inner(value: float | int | str | None) -> str:
        return _plain(_num(value), decimals)

    return inner


def _signed_fixed(decimals: int) -> FormatFn:
    def inner(value: float | int | str | None) -> str:
        return _signed(_num(value), decimals)

    return inner


def _pct(decimals: int) -> FormatFn:
    def inner(value: float | int | str | None) -> str:
        return f"{_plain(_num(value) * 100.0, decimals)}%"

    return inner


def _signed_pct(decimals: int) -> FormatFn:
    def inner(value: float | int | str | None) -> str:
        return f"{_signed(_num(value) * 100.0, decimals)}%"

    return inner


def _points(decimals: int) -> FormatFn:
    """A difference of two shares, stored as a fraction, printed in percentage points."""

    def inner(value: float | int | str | None) -> str:
        return f"{_signed(_num(value) * 100.0, decimals)} pts"

    return inner


def _per_thousand(decimals: int) -> FormatFn:
    """A rate stored as a fraction of good events, printed per 1,000 good events."""

    def inner(value: float | int | str | None) -> str:
        return f"{_plain(_num(value) * 1000.0, decimals)} per 1,000"

    return inner


def _usd(decimals: int) -> FormatFn:
    def inner(value: float | int | str | None) -> str:
        v = _num(value)
        text = f"${abs(v):,.{decimals}f}"
        return f"-{text}" if v < 0 and not _is_zero(text[1:]) else text

    return inner


def fmt_usd_m1(value: float | int | str | None) -> str:
    v = _num(value) / 1_000_000.0
    text = f"${abs(v):,.1f} million"
    return f"-{text}" if v < 0 and not _is_zero(f"{abs(v):.1f}") else text


def fmt_millions1(value: float | int | str | None) -> str:
    return f"{_plain(_num(value) / 1_000_000.0, 1)} million"


def fmt_ms0(value: float | int | str | None) -> str:
    return f"{round(_num(value)):,} ms"


def fmt_s1(value: float | int | str | None) -> str:
    return f"{_plain(_num(value), 1)} s"


def fmt_days1(value: float | int | str | None) -> str:
    return f"{_plain(_num(value), 1)} days"


def fmt_days0(value: float | int | str | None) -> str:
    v = round(_num(value))
    return "1 day" if v == 1 else f"{v:,} days"


def fmt_text(value: float | int | str | None) -> str:
    if value is None:
        return "not available"
    return str(value)


def fmt_year(value: float | int | str | None) -> str:
    if value is None:
        return "not available"
    return str(round(_num(value)))


def fmt_ratio2(value: float | int | str | None) -> str:
    return f"{_plain(_num(value), 2)}x"


def fmt_usd_per_million(value: float | int | str | None) -> str:
    """Compute cost per million events, in dollars of list price."""
    return f"${_plain(_num(value), 2)} per million events"


def fmt_s_per_million(value: float | int | str | None) -> str:
    return f"{_plain(_num(value), 1)} s per million events"


FORMATS: dict[str, FormatFn] = {
    "int": fmt_int,
    "float1": _fixed(1),
    "float2": _fixed(2),
    "float3": _fixed(3),
    "float4": _fixed(4),
    "sfloat2": _signed_fixed(2),
    "sfloat3": _signed_fixed(3),
    "pct0": _pct(0),
    "pct1": _pct(1),
    "pct2": _pct(2),
    "pct3": _pct(3),
    "spct1": _signed_pct(1),
    "pts1": _points(1),
    "pts2": _points(2),
    "per1k1": _per_thousand(1),
    "per1k2": _per_thousand(2),
    "usd0": _usd(0),
    "usd2": _usd(2),
    "usdm1": fmt_usd_m1,
    "millions1": fmt_millions1,
    "ms": fmt_ms0,
    "s1": fmt_s1,
    "days0": fmt_days0,
    "days1": fmt_days1,
    "ratio2": fmt_ratio2,
    "usd_per_m": fmt_usd_per_million,
    "s_per_m": fmt_s_per_million,
    "year": fmt_year,
    "text": fmt_text,
}


def format_value(value: float | int | str | None, fmt: str) -> str:
    if fmt not in FORMATS:
        raise KeyError(f"unknown format {fmt!r}")
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return "not available"
    if value is None and fmt != "text":
        return "not applicable"
    return FORMATS[fmt](value)
