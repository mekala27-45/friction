"""Gold: point in time features, and the two as of inputs they need.

Every feature describes an event using only what was known before it. The card's own history comes
from the state machines in friction_features, run per card account (or user, or IP) in event
order. Two inputs cross entities and are joined as of the event: the issue time of the card
generation in effect (from the card SCD2 history) and the merchant category's fraud rate from the
labels the bank knew at the start of the event's month, on the system time clock.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from friction_features import batch, logins, payments
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Groups for the state machines. Serverless caps Python UDF memory at 1 GB, and a group arrives as
# one pandas frame, so 31 million logins go in 1,024 groups of about 30,000 rows, not 64 of 490,000.
# The count changes no feature: the state resets at each entity inside a group.
BUCKETS = 1024


def _assume(df: DataFrame, fields: Sequence[tuple[str, T.DataType]]) -> DataFrame:
    """Tell a Spark Connect DataFrame its own schema so applyInPandas need not ask the server.

    applyInPandas reads the input's column names, and in a pipeline query function any request to
    analyse a plan is refused (ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION), because the inputs do
    not exist yet. The schema here is the select just above it, so it cannot drift.
    """
    if hasattr(df, "_cached_schema"):
        setattr(df, "_cached_schema", T.StructType([T.StructField(n, t) for n, t in fields]))  # noqa: B010
    return df


def _out(key: str, names: tuple[str, ...]) -> T.StructType:
    return T.StructType([T.StructField(key, T.LongType()), *[T.StructField(n, T.DoubleType()) for n in names]])


S, L, INT, D, B = T.StringType(), T.LongType(), T.IntegerType(), T.DoubleType(), T.BooleanType()
PAYMENT_INPUT = [
    ("account_key", S), ("txn_ref", L), ("event_ts_s", L), ("amount_cents", L), ("merchant_key", S), ("mcc", INT),
    ("channel", INT), ("location", S), ("foreign", B), ("has_error", B), ("days_since_issue", D),
    ("mcc_fraud_rate", D), ("_bucket", INT),
]  # fmt: skip
# A merchant category's rate is shrunk toward the overall rate as if it had this many more events.
MCC_PRIOR_EVENTS = 2000
MCC_WINDOW_MONTHS = 12


def card_issue_times(cards_history: DataFrame) -> DataFrame:
    """account_ref, card_generation, issued_at: when each generation of a card took effect."""
    return cards_history.groupBy("account_ref", "card_generation").agg(
        F.min(F.col("__START_AT.changed_at")).alias("issued_at")
    )


def issue_age(silver: DataFrame, issues: DataFrame) -> DataFrame:
    """txn_ref, days_since_issue: days from the issue of the generation in effect, or -1.

    Events and issue times are unioned on a fixed set of columns and ordered per account; each event
    takes the last issue at or before it. A pipeline graph analyses a flow before its inputs exist,
    so a flow here reads each input once and names every column it unions; the result joins back to
    silver in the next dataset.
    """
    ev = silver.select(
        "txn_ref", "account_ref", F.col("event_ts").alias("_t"), F.lit(1).alias("_k"),
        F.lit(None).cast("timestamp").alias("_iss"),
    )  # fmt: skip
    iss = issues.select(
        F.lit(None).cast("bigint").alias("txn_ref"),
        "account_ref",
        F.col("issued_at").alias("_t"),
        F.lit(0).alias("_k"),
        F.col("issued_at").alias("_iss"),
    )
    w = Window.partitionBy("account_ref").orderBy("_t", "_k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
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


def enrich(silver: DataFrame, ages: DataFrame, rates: DataFrame) -> DataFrame:
    """Silver payments with the two as of inputs joined on (no drop or fillna: see issue_age)."""
    r = rates.select(F.col("mcc").alias("_r_mcc"), F.col("month_start").alias("_r_month"), F.col("rate").alias("_rate"))
    on = (F.col("mcc") == F.col("_r_mcc")) & (F.trunc("event_date", "month") == F.col("_r_month"))
    out = silver.join(ages, "txn_ref", "left").join(r, on, "left")
    return out.withColumn("mcc_fraud_rate", F.coalesce(F.col("_rate"), F.lit(0.0))).withColumn(
        "days_since_issue", F.coalesce(F.col("days_since_issue"), F.lit(-1.0))
    )


def debit_counts(silver: DataFrame) -> DataFrame:
    """mcc, month, n and the rolling count of the twelve months before the month."""
    counts = (
        silver.where(~F.col("is_refund"))
        .groupBy("mcc", F.trunc("event_date", "month").alias("month"))
        .agg(F.count(F.lit(1)).alias("n"))
    )
    months = F.months_between(F.col("month"), F.lit("1970-01-01")).cast("long")
    by_mcc = Window.partitionBy("mcc").orderBy(months).rangeBetween(-MCC_WINDOW_MONTHS, -1)
    return counts.withColumn("n_window", F.coalesce(F.sum("n").over(by_mcc), F.lit(0)))


def known_frauds(label_history: DataFrame, silver: DataFrame) -> DataFrame:
    """mcc, month_start, frauds_known: frauds in the twelve months before, known at month start."""
    versions = label_history.where(F.col("is_fraud")).select(
        F.col("event_ref").alias("txn_ref"),
        F.col("__START_AT.reported_at").alias("known_from"),
        F.col("__END_AT.reported_at").alias("known_to"),
    )
    events = silver.where(~F.col("is_refund")).select(
        "txn_ref", "mcc", F.trunc("event_date", "month").alias("event_month")
    )
    asof = F.explode(
        F.sequence(
            F.add_months("event_month", 1), F.add_months("event_month", MCC_WINDOW_MONTHS), F.expr("interval 1 month")
        )
    )
    return (
        versions.join(events, "txn_ref")
        .select("txn_ref", "mcc", "known_from", "known_to", asof.alias("month_start"))
        .where(
            (F.col("known_from") < F.col("month_start").cast("timestamp"))
            & (F.col("known_to").isNull() | (F.col("known_to") >= F.col("month_start").cast("timestamp")))
        )
        .groupBy("mcc", "month_start")
        .agg(F.countDistinct("txn_ref").alias("frauds_known"))
    )


def mcc_rates(counts: DataFrame, known: DataFrame) -> DataFrame:
    """mcc, month_start, n_window, frauds_known, rate, shrunk toward the month's overall rate."""
    joined = counts.select("mcc", F.col("month").alias("month_start"), "n_window").join(
        known, ["mcc", "month_start"], "left"
    )
    joined = joined.withColumn("frauds_known", F.coalesce(F.col("frauds_known"), F.lit(0)))
    month = Window.partitionBy("month_start")
    overall = F.sum("frauds_known").over(month) / F.greatest(F.sum("n_window").over(month), F.lit(1))
    return joined.withColumn(
        "rate", (F.col("frauds_known") + MCC_PRIOR_EVENTS * overall) / (F.col("n_window") + MCC_PRIOR_EVENTS)
    ).select("mcc", "month_start", "n_window", "frauds_known", "rate")


def payment_inputs(enriched: DataFrame) -> DataFrame:
    """The columns the payments state machine reads, with the bucket it is grouped by."""
    return enriched.select(
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


def payments_features(inputs: DataFrame) -> DataFrame:
    """inputs: payment_inputs(), materialized. Runs as a job task, outside the pipeline graph.

    A pipeline query function cannot use applyInPandas: the planner analyses a grouped map's input
    when the flow is defined, before the input table exists. So the pipeline materializes the
    inputs, and the features task runs the state machine over them with an ordinary session.
    """
    return (
        _assume(inputs, PAYMENT_INPUT)
        .groupBy("_bucket")
        .applyInPandas(batch.payments_partition, _out("txn_ref", payments.FEATURES))
    )


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


def login_inputs(logins: DataFrame, *, has_synthetic: bool = False) -> DataFrame:
    """logins: silver logins, or silver with synthetic attacks (synthetic = true) unioned in.

    Materialized by the pipeline; the user and IP passes run as a job task (see payments_features).
    """
    synthetic = F.col("synthetic") if has_synthetic else F.lit(False)
    # Every attempt in the unknown account bucket is a stranger to the user state machine; the IP
    # state machine still counts it, which is where those attempts tell you something.
    unknown = F.lit(False) if has_synthetic else F.col("unknown_account")
    user_key = F.when(unknown, F.concat(F.lit("z"), F.col("login_ref").cast("string"))).otherwise(F.col("user_key"))
    return logins.select(
        user_key.alias("user_key"),
        "ip_key",
        F.col("login_ref"),
        (F.unix_timestamp("event_ts") * 1000 + F.date_format("event_ts", "SSS").cast("long")).alias("event_ts_ms"),
        *[F.coalesce(F.col(c).cast("string"), F.lit("")).alias(f"v_{lvl}") for lvl, c in LEVEL_COLUMNS.items()],
        F.col("rtt_ms"),
        F.col("success"),
        synthetic.alias("synthetic"),
    )


LOGIN_INPUT = [
    ("user_key", S), ("ip_key", S), ("login_ref", L), ("event_ts_ms", L),
    *[(f"v_{lvl}", S) for lvl in LEVEL_COLUMNS], ("rtt_ms", D), ("success", B), ("synthetic", B),
]  # fmt: skip


def logins_user_features(inputs: DataFrame) -> DataFrame:
    bucketed = inputs.withColumn("_bucket", F.pmod(F.hash("user_key"), F.lit(BUCKETS)))
    return (
        _assume(bucketed, [*LOGIN_INPUT, ("_bucket", INT)])
        .groupBy("_bucket")
        .applyInPandas(batch.logins_partition, _out("login_ref", logins.FEATURES))
    )


def logins_ip_features(inputs: DataFrame) -> DataFrame:
    bucketed = inputs.select("ip_key", "login_ref", "event_ts_ms", "success", "synthetic").withColumn(
        "_bucket", F.pmod(F.hash("ip_key"), F.lit(BUCKETS))
    )
    fields = [("ip_key", S), ("login_ref", L), ("event_ts_ms", L), ("success", B), ("synthetic", B), ("_bucket", INT)]
    return (
        _assume(bucketed, fields)
        .groupBy("_bucket")
        .applyInPandas(batch.ip_partition, _out("login_ref", logins.IP_FEATURES))
    )


def logins_features(user: DataFrame, ip: DataFrame) -> DataFrame:
    """The two passes joined on the login. Each pass is its own dataset in a pipeline."""
    return user.join(ip, "login_ref")


def features_step(
    spark: Any,
    read: Callable[[str], DataFrame],
    write: Callable[[str, DataFrame], None],
    attacks: DataFrame | None = None,
) -> dict[str, int]:
    """Run the state machines over the materialized inputs and write the feature tables.

    The same function runs on the build machine (reading and writing Parquet) and in the workspace
    features task (reading and writing Delta), so both engines compute features with one code path.
    Synthetic attacks, when given, are scored against each victim's state and never change it.
    Writes replace the tables whole, which is what makes a rerun leave them identical.
    """
    pay = payments_features(read("payment_feature_inputs"))
    write("payments_features", pay)
    inputs = read("login_feature_inputs")
    if attacks is not None:
        inputs = inputs.unionByName(login_inputs(attacks, has_synthetic=True))
    write("logins_user_features", logins_user_features(inputs))
    write("logins_ip_features", logins_ip_features(inputs))
    write("logins_features", logins_features(read("logins_user_features"), read("logins_ip_features")))
    return {name: read(name).count() for name in ("payments_features", "logins_features")}
