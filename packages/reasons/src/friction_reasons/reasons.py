"""Reason codes from a fixed table, ranked by each feature's contribution toward the decision.

Every step up and decline carries up to three codes: the features that pushed the score toward the
decision the most, mapped through evaluation/reason_codes.yaml. Contributions come from TreeSHAP
for LightGBM (LightGBM's own implementation) and from the coefficient times the standardized value
for logistic regression. The floor carries the codes of the rules that fired.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import yaml


def load_table(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data


def feature_to_code(table: dict[str, Any], desk: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for code, entry in table[desk].items():
        for feature in entry["features"]:
            out[feature] = code
    return out


def top_codes(
    contributions: npt.NDArray[np.float64], names: Sequence[str], mapping: dict[str, str], k: int = 3, eps: float = 1e-9
) -> list[str]:
    """Codes of the features with the largest positive contribution, distinct, at most k."""
    order = np.argsort(-contributions, kind="stable")
    codes: list[str] = []
    for i in order:
        if contributions[i] <= eps:
            break
        code = mapping.get(names[i])
        if code and code not in codes:
            codes.append(code)
        if len(codes) == k:
            break
    return codes
