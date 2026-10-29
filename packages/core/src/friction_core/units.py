"""A unit is part of a value (the Day 15 rule).

Every amount carries its currency and every rate says per what. The pipeline keeps amounts in
integer cents with a currency column; these types are what crosses a boundary in Python.
"""

from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Literal

from pydantic import field_validator

from friction_core.model import StrictModel

Currency = Literal["USD"]


class Money(StrictModel):
    """An amount in minor units (cents) with its currency."""

    cents: int
    currency: Currency = "USD"

    @property
    def dollars(self) -> float:
        return self.cents / 100.0

    def __add__(self, other: Money) -> Money:
        if other.currency != self.currency:
            raise ValueError(f"cannot add {other.currency} to {self.currency}")
        return Money(cents=self.cents + other.cents, currency=self.currency)


class Rate(StrictModel):
    """A count over a stated base, for example step ups per 1,000 good payment events."""

    numerator: float
    denominator: float
    per: str

    @field_validator("per")
    @classmethod
    def _says_per_what(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("a rate must say per what")
        return value

    @property
    def value(self) -> float:
        return self.numerator / self.denominator if self.denominator else float("nan")


def parse_amount_cents(text: str | None) -> int | None:
    """TabFormer writes amounts as $134.09 or $-77.00. Returns cents, or None when it does not parse."""
    if text is None:
        return None
    raw = text.strip().replace(",", "")
    if raw.startswith("$"):
        raw = raw[1:]
    if not raw:
        return None
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None
    if not value.is_finite():
        return None
    return int((value * 100).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN))
