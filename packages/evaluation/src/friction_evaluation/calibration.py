"""Calibration: expected calibration error and the reliability curve, ten equal count bins."""

from __future__ import annotations

import numpy as np
import numpy.typing as npt


def reliability(p: npt.NDArray[np.float64], y: npt.NDArray[np.bool_], bins: int = 10) -> list[tuple[float, float, int]]:
    order = np.argsort(p, kind="stable")
    out = []
    for chunk in np.array_split(order, bins):
        if len(chunk):
            out.append((float(p[chunk].mean()), float(y[chunk].mean()), len(chunk)))
    return out


def ece(p: npt.NDArray[np.float64], y: npt.NDArray[np.bool_], bins: int = 10) -> float:
    total = len(p)
    return float(sum(n / total * abs(mp - my) for mp, my, n in reliability(p, y, bins)))
