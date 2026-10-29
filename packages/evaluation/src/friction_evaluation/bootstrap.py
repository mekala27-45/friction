"""Cluster bootstrap over users, from per user sufficient statistics.

A user's events move together (one compromised card, one account), so the resampling unit is the
user. Each resample draws user multiplicities from a multinomial, and every metric is a ratio of
weighted sums of per user totals, so a resample costs one matrix product, not a pass over events.

The login desk has about a million users in its test months, and a thousand by a million weight
matrix does not fit in memory. ``resampled_sums`` draws the same multinomial in two stages: first
how many of each resample's draws fall in each chunk of users, then a multinomial inside each
chunk. That is the same distribution as one multinomial over every user, chunk by chunk. Seeded
numpy generator; the same seed gives the same intervals on any machine and under any
PYTHONHASHSEED.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt
import pandas as pd

RESAMPLES = 1000
SEED = 20261029
CHUNK = 20_000

Matrix = npt.NDArray[np.float64]


@dataclass(frozen=True)
class Interval:
    estimate: float
    low: float
    high: float

    def contains(self, value: float) -> bool:
        return self.low <= value <= self.high

    def clear_of_zero(self) -> bool:
        return self.low > 0 or self.high < 0


def per_user(frame: pd.DataFrame, user: str, columns: list[str]) -> Matrix:
    """Sum the given columns per user; rows are users, in a stable order."""
    grouped = frame.groupby(user, sort=True)[columns].sum()
    return grouped.to_numpy(dtype=np.float64)


def by_user(codes: npt.NDArray[np.int64], n_users: int, columns: list[npt.NDArray[np.float64]]) -> Matrix:
    """Per user sums of event level columns, with users already coded 0..n_users-1."""
    out = np.empty((n_users, len(columns)), dtype=np.float64)
    for j, col in enumerate(columns):
        out[:, j] = np.bincount(codes, weights=col, minlength=n_users)
    return out


def resampled_sums(stats: Matrix, resamples: int = RESAMPLES, seed: int = SEED, chunk: int = CHUNK) -> Matrix:
    """resamples x columns: the column sums of each cluster bootstrap resample of the rows."""
    n = stats.shape[0]
    rng = np.random.default_rng(seed)
    bounds = [*range(0, n, chunk), n]
    sizes = np.diff(bounds)
    counts = rng.multinomial(n, sizes / n, size=resamples)
    out = np.zeros((resamples, stats.shape[1]), dtype=np.float64)
    for c, (lo, hi) in enumerate(itertools.pairwise(bounds)):
        size = hi - lo
        w = rng.multinomial(counts[:, c], np.full(size, 1.0 / size))
        out += w.astype(np.float64) @ stats[lo:hi]
    return out


def _quantiles(values: npt.NDArray[np.float64], level: float) -> tuple[float, float]:
    v = values[~np.isnan(values)]
    if len(v) == 0:
        return float("nan"), float("nan")
    lo, hi = np.quantile(v, [(1 - level) / 2, 1 - (1 - level) / 2])
    return float(lo), float(hi)


def ratio(stats: Matrix, sums: Matrix, num: int, den: int, level: float = 0.95) -> Interval:
    total = stats[:, den].sum()
    est = stats[:, num].sum() / total if total else float("nan")
    with np.errstate(invalid="ignore", divide="ignore"):
        boots = sums[:, num] / sums[:, den]
    lo, hi = _quantiles(boots, level)
    return Interval(float(est), lo, hi)


def difference(
    stats: Matrix, sums: Matrix, a: tuple[int, int], b: tuple[int, int], level: float = 0.95
) -> tuple[Interval, float]:
    """The ratio a[0]/a[1] minus the ratio b[0]/b[1], both over the same users, and the two sided
    bootstrap p value for no difference, (count + 1) / (resamples + 1) so it is never zero."""

    def r(m: Matrix, i: tuple[int, int]) -> npt.NDArray[np.float64] | float:
        with np.errstate(invalid="ignore", divide="ignore"):
            if m.ndim == 1:
                return float(m[i[0]] / m[i[1]]) if m[i[1]] else float("nan")
            return m[:, i[0]] / m[:, i[1]]

    totals = stats.sum(axis=0)
    est = float(r(totals, a)) - float(r(totals, b))
    d = np.asarray(r(sums, a)) - np.asarray(r(sums, b))
    d = d[~np.isnan(d)]
    lo, hi = _quantiles(d, level)
    n = len(d)
    p = 2 * min(((d <= 0).sum() + 1) / (n + 1), ((d >= 0).sum() + 1) / (n + 1))
    return Interval(est, lo, hi), float(min(1.0, p))


def one_sided_above(stats: Matrix, sums: Matrix, a: tuple[int, int], b: tuple[int, int]) -> tuple[Interval, float]:
    """As difference, with the one sided p value for a above b."""
    interval, _ = difference(stats, sums, a, b)
    with np.errstate(invalid="ignore", divide="ignore"):
        d = sums[:, a[0]] / sums[:, a[1]] - sums[:, b[0]] / sums[:, b[1]]
    d = d[~np.isnan(d)]
    return interval, float(((d <= 0).sum() + 1) / (len(d) + 1))
