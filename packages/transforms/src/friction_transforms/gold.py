"""Gold: point in time features, and the two as of inputs they need.

Every feature describes an event using only what was known before it. The card's own history comes
from the state machines in friction_features, run per card account (or user, or IP) in event
order. Two inputs cross entities and are joined as of the event: the issue time of the card
generation in effect (from the card SCD2 history) and the merchant category's fraud rate from the
labels the bank knew at the start of the event's month, on the system time clock.
"""

from __future__ import annotations

from friction_features import batch
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

BUCKETS = 64
# A merchant category's rate is shrunk toward the overall rate as if it had this many more events.
MCC_PRIOR_EVENTS = 2000
MCC_WINDOW_MONTHS = 12


def card_issue_times(cards_history: DataFrame) -> DataFrame:
    """account_ref, card_generation, issued_at: when each generation of a card took effect."""
    return cards_history.groupBy("account_ref", "card_generation").agg(
        F.min(F.col("__START_AT.changed_at")).alias("issued_at")
    )


def with_issue_age(silver: DataFrame, issues: DataFrame) -> DataFrame:
    """Days from the issue of the card generation in effect at the event to the event, or -1."""
    ev = silver.select(
        "txn_ref",
        "account_ref",
        F.col("event_ts").alias("_t"),
        F.lit(1).alias("_k"),
        F.lit(None).cast("timestamp").alias("_iss"),
    )
    iss = issues.select(
        F.lit(None).cast("bigint").alias("txn_ref"),
        "account_ref",
        F.col("issued_at").alias("_t"),
        F.lit(0).alias("_k"),
        F.col("issued_at").alias("_iss"),
    )
    w = Window.partitionBy("account_ref").orderBy("_t", "_k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    asof = (
        ev.unionByName(iss)
        .withColumn("_last", F.last("_iss", ignorenulls=True).over(w))
        .where(F.col("_k") == 1)
        .select(
            "txn_ref",
            F.coalesce((F.unix_timestamp("_t") - F.unix_timestamp("_last")) / 86400.0, F.lit(-1.0)).alias(
                "days_since_issue"
            ),
        )
    )
    return silver.join(asof, "txn_ref", "left")


def mcc_rates(silver: DataFrame, label_history: DataFrame) -> DataFrame:
    """mcc, month_start, n, frauds_known, rate: as of the first instant of each month."""
    debits = silver.where(~F.col("is_refund")).withColumn("month", F.trunc("event_date", "month"))
    counts = debits.groupBy("mcc", "month").agg(F.count("*").alias("n"))
    months = debits.select("month").distinct()
    mccs = debits.select("mcc").distinct()
    grid = months.crossJoin(mccs)
    by_mcc = (
        Window.partitionBy("mcc")
        .orderBy(F.col("month").cast("timestamp").cast("long"))
        .rangeBetween(-MCC_WINDOW_MONTHS * 31 * 86400, -1)
    )
    rolled = (
        grid.join(counts, ["mcc", "month"], "left")
        .fillna(0, subset=["n"])
        .withColumn("n_window", F.sum("n").over(by_mcc))
    )
    # Labels in effect at the month start (system time), for events in the trailing window.
    fraud_versions = (
        label_history.where(F.col("is_fraud"))
        .select(
            F.col("event_ref").alias("txn_ref"),
            F.col("__START_AT.reported_at").alias("known_from"),
            F.col("__END_AT.reported_at").alias("known_to"),
        )
        .join(debits.select("txn_ref", "mcc", F.col("month").alias("event_month")), "txn_ref")
    )
    known = (
        fraud_versions.crossJoin(months.withColumnRenamed("month", "asof"))
        .where(
            (F.col("known_from") < F.col("asof").cast("timestamp"))
            & (F.col("known_to").isNull() | (F.col("known_to") >= F.col("asof").cast("timestamp")))
            & (F.col("event_month") < F.col("asof"))
            & (F.col("event_month") >= F.add_months("asof", -MCC_WINDOW_MONTHS))
        )
        .groupBy("mcc", F.col("asof").alias("month"))
        .agg(F.countDistinct("txn_ref").alias("frauds_known"))
    )
    joined = rolled.join(known, ["mcc", "month"], "left").fillna(0, subset=["frauds_known"])
    overall = joined.groupBy("month").agg(
        (F.sum("frauds_known") / F.greatest(F.sum("n_window"), F.lit(1))).alias("overall")
    )
    return (
        joined.join(overall, "month")
        .withColumn(
            "rate",
            (F.col("frauds_known") + MCC_PRIOR_EVENTS * F.col("overall")) / (F.col("n_window") + MCC_PRIOR_EVENTS),
        )
        .select("mcc", F.col("month").alias("month_start"), "n_window", "frauds_known", "rate")
    )


def with_mcc_rate(silver: DataFrame, rates: DataFrame) -> DataFrame:
    r = rates.select("mcc", F.col("month_start").alias("_m"), F.col("rate").alias("mcc_fraud_rate"))
    out = silver.withColumn("_m", F.trunc("event_date", "month")).join(r, ["mcc", "_m"], "left").drop("_m")
    return out.fillna(0.0, subset=["mcc_fraud_rate"])


def payments_features(enriched: DataFrame) -> DataFrame:
    """enriched: silver payments with days_since_issue and mcc_fraud_rate. One row per txn_ref."""
    inputs = enriched.select(
        "account_key",
        "txn_ref",
        F.unix_timestamp("event_ts").alias("event_ts_s"),
        "amount_cents",
        "merchant_key",
        "mcc",
        "channel",
        "location",
        "foreign",
        "has_error",
        "days_since_issue",
        "mcc_fraud_rate",
        F.pmod(F.hash("account_key"), F.lit(BUCKETS)).alias("_bucket"),
    )
    return inputs.groupBy("_bucket").applyInPandas(batch.payments_partition, batch.payments_schema())


LEVEL_COLUMNS = {
    "ip": "ip_key",
    "asn": "asn",
    "country": "country",
    "region": "region",
    "city": "city",
    "ua": "ua_key",
    "browser": "browser",
    "os": "os",
    "device": "device_type",
}


def login_inputs(logins: DataFrame) -> DataFrame:
    """logins: silver logins, optionally with synthetic attacks (synthetic = true) unioned in."""
    synthetic = F.col("synthetic") if "synthetic" in logins.columns else F.lit(False)
    return logins.select(
        "user_key",
        "ip_key",
        F.col("login_ref"),
        (F.unix_timestamp("event_ts") * 1000 + F.date_format("event_ts", "SSS").cast("long")).alias("event_ts_ms"),
        *[F.coalesce(F.col(c).cast("string"), F.lit("")).alias(f"v_{lvl}") for lvl, c in LEVEL_COLUMNS.items()],
        F.col("rtt_ms"),
        F.col("success"),
        synthetic.alias("synthetic"),
    )


def logins_features(inputs: DataFrame) -> DataFrame:
    user = (
        inputs.withColumn("_bucket", F.pmod(F.hash("user_key"), F.lit(BUCKETS)))
        .groupBy("_bucket")
        .applyInPandas(batch.logins_partition, batch.logins_schema())
    )
    ip = (
        inputs.select("ip_key", "login_ref", "event_ts_ms", "success", "synthetic")
        .withColumn("_bucket", F.pmod(F.hash("ip_key"), F.lit(BUCKETS)))
        .groupBy("_bucket")
        .applyInPandas(batch.ip_partition, batch.ip_schema())
    )
    return user.join(ip, "login_ref")
