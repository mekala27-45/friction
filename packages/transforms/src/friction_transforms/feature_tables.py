"""Feature tables keyed by entity and time, and the point in time lookup that reads them.

Two daily tables in the form Unity Catalog serves as feature tables (a primary key of the entity
and a TIMESERIES column): the state of each merchant and of each card at the end of every day
something about it changed, valid from the first instant of the next day (as_of). A lookup for an
event at time t takes the latest row with as_of at or before t, so it only ever reads days that had
ended: the snapshot of the event's own day is never visible to it. The same rule is the one the
Databricks feature engineering client applies to a timestamp lookup key, and jobs/feature_tables.py
checks the two agree row for row.

  fs_merchant_daily  merchant_key, as_of: merchant_cards_seen, merchant_fraud_cards_known, merchant_txn_total,
                     merchant_fraud_share
  fs_card_daily      account_key, as_of: card_txn_total, card_known_fraud

The names carry their entity, so a training set can look both up without a clash.

A card counts as known fraud at a merchant from the later of its first purchase there and the
first time the bank knew of a fraud on it, as in friction_transforms.graph.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from friction_transforms.graph import card_fraud_known

MERCHANT = ("merchant_cards_seen", "merchant_fraud_cards_known", "merchant_txn_total", "merchant_fraud_share")
CARD = ("card_txn_total", "card_known_fraud")


def _next_day(seconds: Column) -> Column:
    """The first instant of the day after the one these epoch seconds fall in, as a UTC timestamp."""
    return F.to_timestamp(F.date_add(F.to_date(F.timestamp_seconds(seconds)), 1))


def merchant_daily(silver: DataFrame, labels: DataFrame) -> DataFrame:
    ev = silver.select("txn_ref", "account_key", "merchant_key", F.unix_timestamp("event_ts").alias("t"))
    known = card_fraud_known(silver, labels).select("account_key", "card_known")
    pairs = (
        ev.groupBy("merchant_key", "account_key").agg(F.min("t").alias("first_t")).join(known, "account_key", "left")
    )
    zero = F.lit(0).cast("long")
    one = F.lit(1).cast("long")
    txns = ev.select("merchant_key", "t", zero.alias("visit"), zero.alias("fraud"), one.alias("txn"))
    visits = pairs.select("merchant_key", F.col("first_t").alias("t"), one.alias("visit"), zero.alias("fraud"),
                          zero.alias("txn"))  # fmt: skip
    frauds = pairs.where(F.col("card_known").isNotNull()).select(
        "merchant_key", F.greatest("first_t", "card_known").alias("t"), zero.alias("visit"), one.alias("fraud"),
        zero.alias("txn"),
    )  # fmt: skip
    marks = txns.unionByName(visits).unionByName(frauds)
    daily = marks.groupBy("merchant_key", _next_day(F.col("t")).alias("as_of")).agg(
        F.sum("visit").alias("v"), F.sum("fraud").alias("f"), F.sum("txn").alias("n")
    )
    upto = Window.partitionBy("merchant_key").orderBy("as_of").rowsBetween(Window.unboundedPreceding, 0)
    out = daily.select(
        "merchant_key",
        "as_of",
        F.sum("v").over(upto).alias("merchant_cards_seen"),
        F.sum("f").over(upto).alias("merchant_fraud_cards_known"),
        F.sum("n").over(upto).alias("merchant_txn_total"),
    )
    seen, fraud = F.col("merchant_cards_seen"), F.col("merchant_fraud_cards_known")
    share = F.when(seen > 0, fraud / seen).otherwise(0.0)
    return out.withColumn("merchant_fraud_share", share.cast("double"))


def card_daily(silver: DataFrame, labels: DataFrame) -> DataFrame:
    ev = silver.select("account_key", F.unix_timestamp("event_ts").alias("t"))
    known = card_fraud_known(silver, labels).groupBy("account_key").agg(F.min("card_known").alias("card_known"))
    zero = F.lit(0).cast("long")
    one = F.lit(1).cast("long")
    marks = ev.select("account_key", "t", one.alias("txn"), zero.alias("fraud")).unionByName(
        known.select("account_key", F.col("card_known").alias("t"), zero.alias("txn"), one.alias("fraud"))
    )
    daily = marks.groupBy("account_key", _next_day(F.col("t")).alias("as_of")).agg(
        F.sum("txn").alias("n"), F.max("fraud").alias("f")
    )
    upto = Window.partitionBy("account_key").orderBy("as_of").rowsBetween(Window.unboundedPreceding, 0)
    return daily.select(
        "account_key",
        "as_of",
        F.sum("n").over(upto).alias("card_txn_total"),
        F.max("f").over(upto).cast("long").alias("card_known_fraud"),
    )


def pit_lookup(
    events: DataFrame, table: DataFrame, key: str, ts: str, features: Sequence[str], row_key: str
) -> DataFrame:
    """events with each feature from the latest table row of the same key with as_of at or before ts.

    One sorted pass: the table rows and the events in one partition per key, ordered by time with a
    table row before an event at the same instant (at or before), and the last non null value carried
    forward. An event with no earlier row gets nulls. row_key is unique per event.
    """
    none = F.lit(None).cast(events.schema[row_key].dataType)
    rows = table.select(key, F.col("as_of").alias("_t"), F.lit(0).alias("_kind"), none.alias("_row"), *features)
    blank = [F.lit(None).cast(table.schema[f].dataType).alias(f) for f in features]
    probe = events.select(key, F.col(ts).alias("_t"), F.lit(1).alias("_kind"), F.col(row_key).alias("_row"), *blank)
    w = Window.partitionBy(key).orderBy("_t", "_kind").rowsBetween(Window.unboundedPreceding, 0)
    carried = rows.unionByName(probe).select(
        "_row", "_kind", *[F.last(f, ignorenulls=True).over(w).alias(f) for f in features]
    )
    found = carried.where(F.col("_kind") == 1).select(F.col("_row").alias(row_key), *features)
    return events.join(found, row_key)
