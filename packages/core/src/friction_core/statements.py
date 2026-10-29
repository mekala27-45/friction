"""The statement that appears on every surface.

Written once here. Every page of the site, every API response, every rendered document and every
model card reads it from this module, and a gate checks each surface for it verbatim.
"""

from __future__ import annotations

PROJECT = "friction"

STATEMENT = (
    "friction is a portfolio demonstration built on two published synthetic data sets (the IBM "
    "TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with "
    "known truth. No real person, card or account appears in it, its numbers describe synthetic data, "
    "and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, "
    "which is for non commercial use."
)


def statement_markdown() -> str:
    return f"> {STATEMENT}"


def statement_payload() -> dict[str, str]:
    return {"statement": STATEMENT}


def contains_statement(text: str) -> bool:
    flat = " ".join(text.split())
    return " ".join(STATEMENT.split()) in flat
