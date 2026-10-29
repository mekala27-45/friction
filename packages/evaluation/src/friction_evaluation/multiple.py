"""Benjamini-Hochberg across a family of comparisons (the Day 3 rule)."""

from __future__ import annotations

import numpy as np


def bh(p_values: list[float], q: float = 0.05) -> list[bool]:
    """Which hypotheses are rejected at false discovery rate q, in the input order."""
    m = len(p_values)
    if m == 0:
        return []
    order = np.argsort(p_values, kind="stable")
    ranked = np.asarray(p_values)[order]
    below = ranked <= q * (np.arange(1, m + 1) / m)
    k = int(np.max(np.nonzero(below)[0]) + 1) if below.any() else 0
    reject = np.zeros(m, dtype=bool)
    reject[order[:k]] = True
    return reject.tolist()


def adjusted(p_values: list[float]) -> list[float]:
    """Benjamini-Hochberg adjusted p values, in the input order."""
    m = len(p_values)
    order = np.argsort(p_values, kind="stable")
    ranked = np.asarray(p_values, dtype=float)[order] * m / np.arange(1, m + 1)
    ranked = np.minimum.accumulate(ranked[::-1])[::-1]
    out = np.empty(m)
    out[order] = np.minimum(ranked, 1.0)
    return out.tolist()
