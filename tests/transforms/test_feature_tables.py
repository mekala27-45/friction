"""Feature tables against brute force, and the point in time lookup reading only days that had ended."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from friction_transforms import feature_tables as ft

from tests.transforms.test_graph import _payments

pytestmark = pytest.mark.spark


def _labels(spark, labels):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    return spark.createDataFrame(labels).select(
        "event_ref", "is_fraud", F.struct(F.col("reported_at"), F.lit(0).alias("seq")).alias("__START_AT")
    )


def _known(rows: list[dict], labels: list[dict]) -> dict[str, datetime]:  # type: ignore[type-arg]
    card_of = {r["txn_ref"]: r["account_key"] for r in rows}
    out: dict[str, datetime] = {}
    for lab in labels:
        if lab["is_fraud"]:
            c = card_of[lab["event_ref"]]
            out[c] = min(out.get(c, lab["reported_at"]), lab["reported_at"])
    return out


def _end_of(day: datetime) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(days=1)


@pytest.mark.parametrize("seed", [1, 2])
def test_daily_tables_hold_the_state_at_the_end_of_each_changed_day(spark, seed: int) -> None:  # type: ignore[no-untyped-def]
    rows, labels = _payments(seed)
    silver, lab = spark.createDataFrame(rows), _labels(spark, labels)
    known = _known(rows, labels)
    merchants = {(r["merchant_key"], r["as_of"].replace(tzinfo=UTC)): r.asDict()
                 for r in ft.merchant_daily(silver, lab).collect()}  # fmt: skip
    for (m, as_of), got in merchants.items():
        upto = [r for r in rows if r["merchant_key"] == m and _end_of(r["event_ts"]) <= as_of]
        first: dict[str, datetime] = {}
        for r in upto:
            first[r["account_key"]] = min(first.get(r["account_key"], r["event_ts"]), r["event_ts"])
        fraud = {c for c, f in first.items() if c in known and _end_of(max(f, known[c])) <= as_of}
        assert got["merchant_cards_seen"] == len(first) and got["merchant_txn_total"] == len(upto), (m, as_of)
        assert got["merchant_fraud_cards_known"] == len(fraud)
        assert got["merchant_fraud_share"] == pytest.approx(len(fraud) / len(first) if first else 0.0)
    cards = {
        (r["account_key"], r["as_of"].replace(tzinfo=UTC)): r.asDict() for r in ft.card_daily(silver, lab).collect()
    }
    for (c, as_of), got in cards.items():
        n = sum(1 for r in rows if r["account_key"] == c and _end_of(r["event_ts"]) <= as_of)
        assert got["card_txn_total"] == n
        assert got["card_known_fraud"] == int(c in known and _end_of(known[c]) <= as_of)


def test_the_lookup_takes_the_latest_row_at_or_before_the_event_and_nothing_later(spark) -> None:  # type: ignore[no-untyped-def]
    t = lambda d, h=0: datetime(2019, 7, d, h, tzinfo=UTC)  # noqa: E731
    table = spark.createDataFrame(
        [("m1", t(2), 1, 0.0), ("m1", t(4), 3, 0.5), ("m2", t(3), 7, 0.1)],
        "merchant_key string, as_of timestamp, cards_seen long, fraud_share double",
    )
    events = spark.createDataFrame(
        [
            (1, "m1", t(1, 12)),
            (2, "m1", t(2)),
            (3, "m1", t(3, 23)),
            (4, "m1", t(9)),
            (5, "m2", t(2, 5)),
            (6, "m3", t(5)),
        ],
        "txn_ref long, merchant_key string, event_ts timestamp",
    )
    got = {r["txn_ref"]: (r["cards_seen"], r["fraud_share"])
           for r in ft.pit_lookup(events, table, "merchant_key", "event_ts", ["cards_seen", "fraud_share"], "txn_ref")
           .collect()}  # fmt: skip
    assert got == {1: (None, None), 2: (1, 0.0), 3: (1, 0.0), 4: (3, 0.5), 5: (None, None), 6: (None, None)}
