"""Cluster bootstrap over users, from per user sufficient statistics.

A user's events move together (one compromised card, one account), so the resampling unit is the
user. Each resample draws user multiplicities from a multinomial and every metric is a ratio of
weighted sums of per user totals, so a resample costs one matrix product, not a pass over events.
Seeded numpy generator; the same seed gives the same intervals on any machine and under any
PYTHONHASHSEED.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import numpy.typing as npt
import pandas as pd

RESAMPLES = 1000
SEED = 20261029


@dataclass(frozen=True)
class Interval:
    estimate: float
    low: float
    high: float


def per_user(frame: pd.DataFrame, user: str, columns: list[str]) -> npt.NDArray[np.float64]:
    """Sum the given columns per user; rows are users, in a stable order."""
    grouped = frame.groupby(user, sort=True)[columns].sum()
    return grouped.to_numpy(dtype=np.float64)


def weights(n_users: int, resamples: int = RESAMPLES, seed: int = SEED) -> npt.NDArray[np.float64]:
    rng = np.random.default_rng(seed)
    p = np.full(n_users, 1.0 / n_users)
    return rng.multinomial(n_users, p, size=resamples).astype(np.float64)


def ratio(
    stats: npt.NDArray[np.float64], num: int, den: int, w: npt.NDArray[np.float64], level: float = 0.95
) -> Interval:
    est = stats[:, num].sum() / stats[:, den].sum() if stats[:, den].sum() else float("nan")
    sums = w @ stats[:, [num, den]]
    with np.errstate(invalid="ignore", divide="ignore"):
        boots = sums[:, 0] / sums[:, 1]
    lo, hi = np.nanquantile(boots, [(1 - level) / 2, 1 - (1 - level) / 2])
    return Interval(float(est), float(lo), float(hi))


def difference(
    a: npt.NDArray[np.float64], b: npt.NDArray[np.float64], num: int, den: int, w: npt.NDArray[np.float64]
) -> tuple[Interval, float]:
    """a and b are per user stats of two models on the same users. Returns the interval of a minus b
    and the two sided bootstrap p value for no difference."""
    est = a[:, num].sum() / a[:, den].sum() - b[:, num].sum() / b[:, den].sum()
    sa, sb = w @ a[:, [num, den]], w @ b[:, [num, den]]
    with np.errstate(invalid="ignore", divide="ignore"):
        d = sa[:, 0] / sa[:, 1] - sb[:, 0] / sb[:, 1]
    d = d[~np.isnan(d)]
    lo, hi = np.quantile(d, [0.025, 0.975])
    p = 2 * min((d <= 0).mean(), (d >= 0).mean())
    return Interval(float(est), float(lo), float(hi)), float(min(1.0, p))
