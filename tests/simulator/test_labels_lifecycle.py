from __future__ import annotations

import numpy as np
import pandas as pd
from friction_simulator import config, labels, lifecycle


def _sim() -> config.Simulator:
    return config.simulator()


def _frauds() -> pd.DataFrame:
    ts = pd.to_datetime(
        ["2019-01-01 10:00", "2019-01-02 11:00", "2019-03-01 09:00", "2019-01-05 12:00", "2019-01-06 12:00"]
    )
    return pd.DataFrame(
        {"txn_ref": [10, 11, 12, 20, 21], "user": [1, 1, 1, 2, 2], "card": [0, 0, 0, 1, 1], "event_ts": ts}
    )


def test_incidents_split_on_gaps() -> None:
    f = labels.incidents(_frauds(), gap_days=7)
    by_ref = dict(zip(f["txn_ref"], f["incident_id"], strict=True))
    assert by_ref[10] == by_ref[11] != by_ref[12]
    assert by_ref[20] == by_ref[21] != by_ref[10]


def test_reports_are_after_the_event_and_shared_within_an_incident() -> None:
    lab = labels.chargeback_labels(_frauds(), _sim(), "default")
    assert (lab["reported_at"] > lab["event_ts"]).all()
    same = lab[lab["event_ref"].isin([10, 11])]
    if len(same) == 2:
        assert same["reported_at"].nunique() <= 2


def test_slow_setting_reports_later_on_average() -> None:
    n = 3000
    f = pd.DataFrame({"txn_ref": np.arange(n), "user": np.arange(n), "card": 0, "event_ts": pd.Timestamp("2019-01-01")})
    sim = _sim()
    fast = labels.chargeback_labels(f, sim, "fast")
    slow = labels.chargeback_labels(f, sim, "slow")
    lag = lambda d: (d["reported_at"] - d["event_ts"]).dt.days.median()  # noqa: E731
    assert lag(fast) < lag(slow)
    assert len(slow) < len(fast)  # more never reported in the slow setting


def test_friendly_fraud_has_a_claim_and_a_reversal() -> None:
    sim = _sim().model_copy(update={"chargebacks": _sim().chargebacks.model_copy(update={"false_claim_rate": 0.5})})
    refs = np.arange(200, dtype=np.int64)
    ts = pd.Series(pd.date_range("2019-01-01", periods=200, freq="h"))
    out = labels.friendly_fraud(refs, ts, sim)
    claims = out[out["source"] == "claim"]
    reversals = out[out["source"] == "reversal"]
    assert len(claims) == len(reversals) > 50
    m = claims.merge(reversals, on="event_ref", suffixes=("_c", "_r"))
    assert (m["reported_at_r"] > m["reported_at_c"]).all() and m["is_fraud_c"].all() and (~m["is_fraud_r"]).all()


def test_sequence_orders_system_time() -> None:
    lab = labels.with_sequence(labels.chargeback_labels(_frauds(), _sim(), "default"))
    assert lab["seq"].is_monotonic_increasing
    assert lab["reported_at"].is_monotonic_increasing


def test_lifecycle_feed() -> None:
    acc = pd.DataFrame(
        {
            "user": [1, 2],
            "card": [0, 1],
            "first_ts": pd.to_datetime(["2010-01-01", "2015-01-01"]),
            "last_ts": pd.to_datetime(["2019-12-01", "2016-01-01"]),
        }
    )
    reports = pd.DataFrame({"user": [1], "card": [0], "reported_at": pd.to_datetime(["2018-06-01 10:00"])})
    feed = lifecycle.build(acc, reports, pd.Timestamp("2020-03-01"), _sim())
    a1 = feed[feed["account_ref"] == 100].drop_duplicates(["seq"]).sort_values("seq")
    assert a1.iloc[0]["op"] == "INSERT" and a1.iloc[0]["status"] == "active"
    statuses = list(a1["status"])
    assert "blocked" in statuses
    blocked_at = statuses.index("blocked")
    assert a1.iloc[blocked_at + 1]["card_generation"] == 1 and a1.iloc[blocked_at + 1]["status"] == "active"
    a2 = feed[feed["account_ref"] == 201].drop_duplicates(["seq"]).sort_values("seq")
    assert a2.iloc[-1]["op"] == "DELETE"  # quiet for more than half a year before the data ends
    assert (feed["emitted_at"] >= feed["changed_at"]).all()
