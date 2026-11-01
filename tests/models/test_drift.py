"""Drift: the Spark index equals the numpy one on the same bins, and the rule asks to retrain only on a shift."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pytest
from friction_models import drift

pytestmark = pytest.mark.spark

CONFIG = {"window_days": 28, "score_bound": 0.2, "feature_bound": 0.25, "features_over": 2,
          "features": ["amount_usd", "hour"]}  # fmt: skip


def _frame(spark, shift: float, seed: int = 4):  # type: ignore[no-untyped-def]
    rng = np.random.default_rng(seed)
    rows = []
    start = date(2019, 7, 1)
    for day in range(120):
        late = day >= 92
        for i in range(40):
            s = float(rng.beta(1, 30) + (shift if late else 0.0))
            amount = float(rng.lognormal(3 + (shift * 10 if late else 0), 1))
            rows.append((day * 100 + i, start + timedelta(days=day), min(s, 1.0), amount, float(rng.integers(0, 24))))
    return spark.createDataFrame(rows, "txn_ref long, event_date date, score double, amount_usd double, hour double")


def test_the_index_from_spark_counts_equals_numpy_on_the_same_bins(spark) -> None:  # type: ignore[no-untyped-def]
    rng = np.random.default_rng(1)
    a, b = rng.normal(size=3000), rng.normal(0.4, 1.2, size=2000)
    ref = spark.createDataFrame([(float(v),) for v in a], "x double")
    cur = spark.createDataFrame([(float(v),) for v in b], "x double")
    inner = drift.edges(ref, "x")
    ea = np.bincount(np.searchsorted(inner, a, side="right"), minlength=len(inner) + 1)
    eb = np.bincount(np.searchsorted(inner, b, side="right"), minlength=len(inner) + 1)
    assert drift.bin_counts(ref, "x", inner) == ea.tolist() and drift.bin_counts(cur, "x", inner) == eb.tolist()
    pe, pb = np.clip(ea / ea.sum(), 1e-6, None), np.clip(eb / eb.sum(), 1e-6, None)
    assert drift.psi(ref, cur, "x") == pytest.approx(float(np.sum((pb - pe) * np.log(pb / pe))), rel=1e-12)
    assert drift.psi(ref, ref, "x") == 0.0
    flat = spark.createDataFrame([(1.0,)] * 50, "x double")
    assert drift.psi(flat, cur, "x") == 0.0


def test_a_shift_asks_to_retrain_and_a_steady_desk_does_not(spark) -> None:  # type: ignore[no-untyped-def]
    steady = drift.check(_frame(spark, 0.0), CONFIG, "2019-07-01")
    assert not steady["retrain"] and steady["rows"] == {"reference": 28 * 40, "current": 28 * 40}
    assert steady["reference"] == ["2019-07-01", "2019-07-29"] and steady["current"][1] == "2019-10-29"
    moved = drift.check(_frame(spark, 0.08), CONFIG, "2019-07-01")
    assert moved["retrain"] and moved["score_psi"] >= 0.2 and "amount_usd" in moved["features_over"]
    assert drift.psi_from_counts([0, 0], [1, 2]) == 0.0
