"""The partition functions: one state per entity, whatever else shares the group.

A group holds many entities (a hash bucket), so the bucket count must not change a feature: two
entities processed together give the rows each gives alone, and rows arrive in any order.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from friction_features import batch, logins, payments


def _payments() -> pd.DataFrame:
    rows = []
    for acc, base in (("a1", 1_000_000), ("a2", 2_000_000)):
        for i in range(6):
            rows.append(
                {
                    "account_key": acc, "txn_ref": base + i, "event_ts_s": base + 3600 * i,
                    "amount_cents": 1000 + 250 * i, "merchant_key": f"m{i % 3}", "mcc": 5411 + i % 2,
                    "channel": i % 2, "location": "NJ" if i % 3 else "", "foreign": i == 5, "has_error": i == 4,
                    "days_since_issue": 30.0 + i, "mcc_fraud_rate": 0.001,
                }
            )  # fmt: skip
    return pd.DataFrame(rows)


def _logins() -> pd.DataFrame:
    rows = []
    for user, base in (("u1", 1_600_000_000_000), ("u2", 1_600_000_500_000)):
        for i in range(7):
            row = {"user_key": user, "ip_key": f"i{i % 2}", "login_ref": base // 1000 + i,
                   "event_ts_ms": base + 600_000 * i, "rtt_ms": None if i == 0 else 100.0 + 10 * i,
                   "success": i != 3, "synthetic": False}  # fmt: skip
            for lvl in logins.LEVELS:
                row[f"v_{lvl}"] = f"{lvl}{i % 3}"
            rows.append(row)
    return pd.DataFrame(rows)


def _alone(fn, frame: pd.DataFrame, key: str, ref: str) -> pd.DataFrame:  # type: ignore[no-untyped-def]
    parts = [fn(g) for _, g in frame.groupby(key, sort=True)]
    return pd.concat(parts).sort_values(ref).reset_index(drop=True)


def test_payments_partition_columns_and_entity_isolation() -> None:
    frame = _payments()
    together = batch.payments_partition(frame.sample(frac=1.0, random_state=7)).sort_values("txn_ref")
    assert list(together.columns) == ["txn_ref", *payments.FEATURES]
    assert together["txn_ref"].dtype == np.int64 and len(together) == 12
    pd.testing.assert_frame_equal(together.reset_index(drop=True), _alone(batch.payments_partition, frame,
                                                                          "account_key", "txn_ref"))  # fmt: skip


def test_logins_partition_columns_and_entity_isolation() -> None:
    frame = _logins()
    together = batch.logins_partition(frame.sample(frac=1.0, random_state=3)).sort_values("login_ref")
    assert list(together.columns) == ["login_ref", *logins.FEATURES]
    pd.testing.assert_frame_equal(together.reset_index(drop=True), _alone(batch.logins_partition, frame,
                                                                          "user_key", "login_ref"))  # fmt: skip
    first = together.iloc[0]
    assert first[f"{logins.LEVELS[0]}_distinct"] == 0.0


def test_ip_partition_counts_by_address() -> None:
    frame = _logins()[["ip_key", "login_ref", "event_ts_ms", "success", "synthetic"]]
    out = batch.ip_partition(frame).sort_values("login_ref").reset_index(drop=True)
    assert list(out.columns) == ["login_ref", *logins.IP_FEATURES]
    pd.testing.assert_frame_equal(out, _alone(batch.ip_partition, frame, "ip_key", "login_ref"))


def test_schemas_name_every_feature() -> None:
    assert batch.payments_schema().split(", ")[0] == "txn_ref long"
    assert len(batch.logins_schema().split(", ")) == 1 + len(logins.FEATURES)
    assert len(batch.ip_schema().split(", ")) == 1 + len(logins.IP_FEATURES)
