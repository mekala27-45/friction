"""The floors: the rules an analyst writes on the first day, scored and graded like a model.

Written from the feature definitions alone, before any label rate by feature was examined, and
committed in that form (DECISIONS.md, 29 Oct 2026). Each rule adds points; the score is the sum,
so the floor has a frontier like any model, and the rule that fired names its reason code.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence

import numpy as np
import numpy.typing as npt
from friction_features import logins, payments

Row = Sequence[float] | npt.NDArray[np.float64]
PAY = {name: i for i, name in enumerate(payments.FEATURES)}
LOG = {name: i for i, name in enumerate(logins.FEATURES)}

# (code, points, description). The code is a key of evaluation/reason_codes.yaml.
PAYMENT_RULES: tuple[tuple[str, float, str], ...] = (
    ("P03", 2.0, "three or more purchases on the card within an hour"),
    ("P01", 2.0, "amount at least five times the card's median"),
    ("P07", 2.0, "first purchase at this merchant, online, between midnight and six"),
    ("P10", 1.0, "card reissued less than a day ago"),
    ("P08", 1.0, "first in person purchase in this state or country"),
)
LOGIN_RULES: tuple[tuple[str, float, str], ...] = (
    ("L02", 2.0, "first sign in from this country"),
    ("L04", 1.0, "first sign in from this device type, browser or operating system"),
    ("L03", 1.0, "first sign in from this network (ASN)"),
)


def payment_fired(row: Row) -> list[str]:
    fired = []
    if row[PAY["cnt_1h"]] >= 3:
        fired.append("P03")
    if row[PAY["log_amount_to_median"]] >= math.log(5.0) and row[PAY["log_n_prior"]] > 0:
        fired.append("P01")
    hour = row[PAY["hour"]]
    if row[PAY["merchant_new"]] == 1 and row[PAY["channel"]] == 2 and 0 <= hour < 6:
        fired.append("P07")
    if 0 <= row[PAY["days_since_issue"]] < 1:
        fired.append("P10")
    if row[PAY["location_new"]] == 1 and row[PAY["log_n_prior"]] > 0:
        fired.append("P08")
    return fired


def login_fired(row: Row) -> list[str]:
    if row[LOG["n_success_prior"]] == 0:
        return []
    fired = []
    if row[LOG["country_count"]] == 0:
        fired.append("L02")
    if row[LOG["device_count"]] == 0 or row[LOG["browser_count"]] == 0 or row[LOG["os_count"]] == 0:
        fired.append("L04")
    if row[LOG["asn_count"]] == 0:
        fired.append("L03")
    return fired


def _score(
    rows: npt.NDArray[np.float64], fired: Callable[[Row], list[str]], rules: tuple[tuple[str, float, str], ...]
) -> npt.NDArray[np.float64]:
    points = {code: pts for code, pts, _ in rules}
    out = np.empty(len(rows), dtype=np.float64)
    for i, row in enumerate(rows):
        out[i] = float(sum(points[c] for c in fired(row)))
    return out


def payment_scores(rows: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    # A tiny tie breaker from the amount spreads the floor's few score levels into a frontier; it
    # can never move a transaction across a whole point.
    s = _score(rows, payment_fired, PAYMENT_RULES)
    return s + 1e-6 * np.clip(rows[:, PAY["amount_usd"]], 0, 10_000) / 10_000


def login_scores(rows: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    return _score(rows, login_fired, LOGIN_RULES)
