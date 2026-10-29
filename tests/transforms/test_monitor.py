"""Each monitor check fires on a planted breach and stays quiet on a clean report."""

from __future__ import annotations

import yaml
from friction_transforms import monitor

from tests.conftest import ROOT

CFG = yaml.safe_load((ROOT / "config" / "monitor.yaml").read_text(encoding="utf-8"))


def _clean() -> dict:  # type: ignore[type-arg]
    return {
        "desks": {
            "payments": {
                "feature_drift": [{"feature": "amount_z", "month": "2019-08", "psi": 0.02}],
                "expectations": [{"drop_id": "0386_2020-01-06", "rule": "P_LATE", "rows": 1, "share": 0.0001}],
                "completeness": [{"drop_id": "0386_2020-01-06", "delivered": 8000, "kept": 8000, "ratio": 1.0}],
                "clock": [],
            }
        }
    }


def test_clean_report_raises_nothing() -> None:
    assert monitor.alerts(_clean(), CFG, {"2019-08"}) == []


def test_each_check_fires_on_its_planted_breach() -> None:
    r = _clean()
    d = r["desks"]["payments"]
    d["feature_drift"].append({"feature": "hour", "month": "2019-09", "psi": 0.4})
    d["expectations"].append({"drop_id": "0387_2020-01-07", "rule": "P_RESCUED", "rows": 4000, "share": 0.5})
    d["completeness"].append({"drop_id": "0387_2020-01-07", "delivered": 8000, "kept": 4000, "ratio": 0.5})
    d["clock"].append({"drop_id": "0387_2020-01-07", "file": "part2.csv.gz", "rows": 900, "share": 0.25})
    got = {a["check"] for a in monitor.alerts(r, CFG, {"2019-08", "2019-09"})}
    assert got == {"feature_psi", "expectation_rate", "completeness", "clock"}


def test_drift_outside_the_scored_months_does_not_page() -> None:
    r = _clean()
    r["desks"]["payments"]["feature_drift"].append({"feature": "hour", "month": "2005-01", "psi": 0.9})
    assert monitor.alerts(r, CFG, {"2019-08"}) == []


def test_psi_from_counts() -> None:
    assert monitor.psi_from_counts([10, 10, 10], [10, 10, 10]) == 0.0
    assert monitor.psi_from_counts([10, 10, 10], [1, 1, 28]) > 0.25


def test_expectations_completeness_and_clock_per_drop(spark) -> None:  # type: ignore[no-untyped-def]
    bronze = spark.createDataFrame(
        [("d1", "f1", "1"), ("d1", "f1", "2"), ("d1", "f2", "3"), ("d2", "f3", "3"), ("d2", "f3", "4")],
        "drop_id string, source_file string, txn_id string",
    )
    silver = spark.createDataFrame([(1,), (3,)], "txn_ref bigint")
    quarantine = spark.createDataFrame(
        [("payments", "d1", "f2", "P_LATE", 3), ("payments", "d2", "f3", "P_LATE", 4), ("logins", "d1", "f1", "L", 1)],
        "desk string, drop_id string, source_file string, rule string, event_ref bigint",
    )
    rates = monitor.expectation_rates(quarantine, bronze, "payments")
    assert [(r["drop_id"], r["rows"], round(r["share"], 3)) for r in rates] == [("d1", 1, 0.333), ("d2", 1, 0.5)]
    kept = monitor.completeness(silver, bronze, "txn_ref", "txn_id")
    assert [(r["drop_id"], r["delivered"], r["kept"]) for r in kept] == [("d1", 3, 2), ("d2", 2, 1)]
    late = monitor.clock(quarantine, bronze, "payments", "P_LATE")
    assert [(r["file"], r["share"]) for r in late] == [("f2", 1.0), ("f3", 0.5)]


def test_label_maturity_counts_frauds_known_by_each_checkpoint(spark) -> None:  # type: ignore[no-untyped-def]
    from datetime import datetime

    history = spark.createDataFrame(
        [
            (1, True, datetime(2019, 3, 1), (datetime(2019, 3, 5),)),
            (2, True, datetime(2019, 3, 2), (datetime(2019, 5, 1),)),
            (3, False, datetime(2019, 3, 3), (datetime(2019, 3, 4),)),
        ],
        "event_ref bigint, is_fraud boolean, event_ts timestamp, __START_AT struct<reported_at: timestamp>",
    )
    rows = monitor.label_maturity(history, [7, 90])
    assert rows == [{"month": "2019-03", "frauds": 2, "d7": 0.5, "d90": 1.0}]


def test_feature_drift_is_zero_on_the_reference_and_positive_on_a_shift(spark) -> None:  # type: ignore[no-untyped-def]
    from datetime import datetime

    ref = [(i, datetime(2019, 1, 1 + i % 28), float(i % 10)) for i in range(200)]
    shifted = [(1000 + i, datetime(2019, 2, 1 + i % 28), float(9 + i % 3)) for i in range(200)]
    rows = ref + shifted
    events = spark.createDataFrame([(r[0], r[1]) for r in rows], "txn_ref bigint, event_ts timestamp")
    features = spark.createDataFrame([(r[0], r[2]) for r in rows], "txn_ref bigint, amount_z double")
    drift = monitor.feature_drift(features, events, "txn_ref", ["amount_z"], ("2019-01-01", "2019-02-01"), bins=5)
    by_month = {r["month"]: r["psi"] for r in drift}
    assert by_month["2019-01"] < 1e-9 and by_month["2019-02"] > 0.25
