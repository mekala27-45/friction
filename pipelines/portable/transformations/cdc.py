"""Card and label history on open source Spark, by the reference SCD2 (open source AUTO CDC is type 1)."""

from friction_transforms import scd, silver
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CARD_TRACKED = ["card_generation", "status", "credit_limit_cents", "currency"]
LABEL_TRACKED = ["is_fraud", "event_ts", "source"]


def _cards() -> tuple[DataFrame, DataFrame]:
    typed = silver.cdc_typed(spark.read.table("cdc_cards_bronze"))  # noqa: F821
    return scd.scd2(typed, ["account_ref"], ["changed_at", "seq"], CARD_TRACKED, arrival=["emitted_at", "drop_id"])


def _labels(desk: str, events: str, key: str) -> tuple[DataFrame, DataFrame]:
    typed = silver.labels_typed(spark.read.table(f"labels_{desk}_bronze"))  # noqa: F821
    checked = silver.labels_checked(typed, spark.read.table(events), key, has_refunds=desk == "payments")  # noqa: F821
    ok = checked.where(F.size("violations") == 0)
    history, _ = scd.scd2(
        ok, ["event_ref"], ["reported_at", "seq"], LABEL_TRACKED, op_col="source", delete_value="__none__"
    )
    return history, checked.where(F.size("violations") > 0)


@dp.materialized_view(name="cards_history", comment="Card lifecycle as SCD type 2, from the CDC feed")
def cards_history() -> DataFrame:
    return _cards()[0]


@dp.materialized_view(name="labels_payments_history", comment="Chargeback labels on two clocks, SCD type 2")
def labels_payments_history() -> DataFrame:
    return _labels("payments", "payments_silver", "txn_ref")[0]


@dp.materialized_view(name="labels_logins_history", comment="Takeover labels on two clocks, SCD type 2")
def labels_logins_history() -> DataFrame:
    return _labels("logins", "logins_silver", "login_ref")[0]


@dp.materialized_view(name="quarantine_feeds", comment="Label and CDC records a rule refused")
def quarantine_feeds() -> DataFrame:
    cards = _cards()[1].select(
        F.lit("cdc").alias("desk"),
        F.col("account_ref").alias("event_ref"),
        F.lit("C_SEQ_CONFLICT").alias("rule"),
        "drop_id",
        "source_file",
        F.col("_payload").alias("payload"),
        F.lit(None).cast("string").alias("rescued"),
    )
    parts = [cards]
    for desk, events, key in (("payments", "payments_silver", "txn_ref"), ("logins", "logins_silver", "login_ref")):
        bad = _labels(desk, events, key)[1]
        parts.append(
            bad.select(
                F.lit(f"labels_{desk}").alias("desk"),
                F.col("event_ref"),
                F.explode("violations").alias("rule"),
                "drop_id",
                "source_file",
                F.lit(None).cast("string").alias("payload"),
                F.lit(None).cast("string").alias("rescued"),
            )
        )
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out
