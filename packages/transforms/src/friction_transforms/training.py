"""Training sets as of a cut, and the inputs of the attacker generator.

A label has two dates. ``label_as_of`` keeps, for each event, the version of its label in effect at
a system time instant: the version that began at or before the cut and had not been replaced by
then. A training set cut on 1 November 2018 therefore reads only the chargebacks the bank knew of
on 1 November 2018 (tests/transforms/test_training.py plants a label known one day later and finds
it absent). Without a cut the latest version is used: the label as eventually reported.
"""

from __future__ import annotations

from friction_features import logins as login_features
from friction_features import payments as payment_features
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def label_as_of(history: DataFrame, cut: str | None) -> DataFrame:
    """event_ref, is_fraud: the label version in effect at system time ``cut`` (latest if None)."""
    known_from = F.col("__START_AT.reported_at")
    known_to = F.col("__END_AT.reported_at")
    if cut is None:
        current = history.where(F.col("__END_AT").isNull())
    else:
        t = F.to_timestamp(F.lit(cut))
        current = history.where((known_from <= t) & (F.col("__END_AT").isNull() | (known_to > t)))
    return current.select("event_ref", "is_fraud")


def payments_set(
    features: DataFrame, silver: DataFrame, history: DataFrame, start: str, end: str, cut: str | None
) -> DataFrame:
    """Features and context of the transactions in [start, end), labelled as known at ``cut``."""
    # channel and is_refund are features too; they come from the feature table, once.
    events = silver.where((F.col("event_ts") >= F.lit(start)) & (F.col("event_ts") < F.lit(end))).select(
        "txn_ref", "account_key", "event_ts", "amount_cents"
    )
    lab = label_as_of(history, cut).select(F.col("event_ref").alias("txn_ref"), F.col("is_fraud").alias("_y"))
    return (
        events.join(features, "txn_ref")
        .join(lab, "txn_ref", "left")
        .withColumn("label", F.coalesce(F.col("_y"), F.lit(False)))
        .withColumn("labelled", F.col("_y").isNotNull())
        .select(
            "txn_ref",
            "account_key",
            "event_ts",
            "amount_cents",
            "label",
            "labelled",
            *payment_features.FEATURES,
        )
    )


LOGIN_CONTEXT = ("login_ref", "user_key", "event_ts", "success", "is_attack_ip", "country", "device_type")
LOGIN_VALUES = ("ip_key", "asn", "country", "region", "city", "ua_key", "browser", "os", "device_type")


def _values() -> list:  # type: ignore[type-arg]
    """The values the Freeman model reads, as v_ip, v_asn and so on, never null."""
    pairs = zip(login_features.LEVELS, LOGIN_VALUES, strict=True)
    return [F.coalesce(F.col(c).cast("string"), F.lit("")).alias(f"v_{lvl}") for lvl, c in pairs]


def logins_set(
    features: DataFrame,
    logins: DataFrame,
    attacks: DataFrame | None,
    history: DataFrame,
    start: str,
    end: str,
    cut: str | None,
    purpose: str | None,
) -> DataFrame:
    """Legitimate attempts in [start, end) plus the synthetic attacks generated for ``purpose``.

    label is true for a takeover known at ``cut`` and for every synthetic attack. The values the
    Freeman model reads travel with the row, as v_ip, v_asn and so on.
    """
    window = (F.col("event_ts") >= F.lit(start)) & (F.col("event_ts") < F.lit(end))
    # The unknown account bucket is nobody's account: never graded, never a victim (DECISIONS.md).
    real = logins.where(window & ~F.col("unknown_account")).select(
        *LOGIN_CONTEXT,
        *[
            F.coalesce(F.col(c).cast("string"), F.lit("")).alias(f"v_{lvl}")
            for lvl, c in zip(login_features.LEVELS, LOGIN_VALUES, strict=True)
        ],
        F.lit(False).alias("synthetic"),
        F.lit(None).cast("string").alias("attacker_model"),
    )
    lab = label_as_of(history, cut).select(F.col("event_ref").alias("login_ref"), F.col("is_fraud").alias("_y"))
    real = real.join(lab, "login_ref", "left").withColumn("label", F.coalesce(F.col("_y"), F.lit(False)))
    real = real.withColumn("labelled", F.lit(True))
    real = real.select(*[c for c in real.columns if c != "_y"])
    parts = [real]
    if attacks is not None and purpose is not None:
        att = attacks.where(F.col("purpose") == purpose).select(
            "login_ref", "user_key", "event_ts", F.lit(True).alias("success"), F.lit(False).alias("is_attack_ip"),
            "country", "device_type",
            *_values(),
            F.lit(True).alias("synthetic"), "attacker_model", F.lit(True).alias("label"), F.lit(True).alias("labelled"),
        )  # fmt: skip
        parts.append(att)
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.join(features, "login_ref").select(*out.columns, *login_features.FEATURES, *login_features.IP_FEATURES)


def mode_by_user(df: DataFrame, column: str) -> DataFrame:
    counts = df.groupBy("user_key", column).agg(F.count(F.lit(1)).alias("_n"))
    best = Window.partitionBy("user_key").orderBy(F.col("_n").desc(), F.col(column).asc())
    return (
        counts.withColumn("_r", F.row_number().over(best))
        .where(F.col("_r") == 1)
        .select("user_key", F.col(column).alias(f"main_{column}"))
    )


def victim_profiles(logins: DataFrame, before: str) -> DataFrame:
    """user_key and the user's usual country, region, browser, OS, device and browser string."""
    ok = logins.where(F.col("success") & ~F.col("unknown_account") & (F.col("event_ts") < F.lit(before)))
    out = mode_by_user(ok, "country")
    for c in ("region", "browser", "os", "device_type", "ua_key"):
        out = out.join(mode_by_user(ok, c), "user_key")
    return (
        out.withColumnRenamed("main_device_type", "main_device")
        .withColumnRenamed("main_ua_key", "main_ua_key")
        .join(ok.groupBy("user_key").agg(F.count(F.lit(1)).alias("n_success")), "user_key")
    )


def active_users(logins: DataFrame, start: str, end: str) -> DataFrame:
    window = (F.col("event_ts") >= F.lit(start)) & (F.col("event_ts") < F.lit(end))
    return logins.where(window & F.col("success") & ~F.col("unknown_account")).select("user_key").distinct()


def ip_pool(logins: DataFrame, start: str, end: str, per_country: int = 200) -> DataFrame:
    """Failed logins from known attack addresses in the window, one row per address, the busiest first."""
    window = (F.col("event_ts") >= F.lit(start)) & (F.col("event_ts") < F.lit(end))
    failed = logins.where(window & F.col("is_attack_ip") & ~F.col("success"))
    per_ip = failed.groupBy("ip_key").agg(
        F.first("ip_prefix16", ignorenulls=True).alias("ip_prefix16"),
        F.first("asn", ignorenulls=True).alias("asn"),
        F.first("country", ignorenulls=True).alias("country"),
        F.first("region", ignorenulls=True).alias("region"),
        F.first("city", ignorenulls=True).alias("city"),
        F.percentile_approx("rtt_ms", 0.5).alias("rtt_ms"),
        F.count(F.lit(1)).alias("n"),
    )
    rank = Window.partitionBy("country").orderBy(F.col("n").desc(), F.col("ip_key"))
    return (
        per_ip.withColumn("_r", F.row_number().over(rank))
        .where(F.col("_r") <= per_country)
        .select("ip_key", "ip_prefix16", "asn", "country", "region", "city", "rtt_ms", "n")
    )


def ua_counts(logins: DataFrame, start: str, end: str, *, attack: bool, top: int = 500) -> DataFrame:
    window = (F.col("event_ts") >= F.lit(start)) & (F.col("event_ts") < F.lit(end))
    cond = (F.col("is_attack_ip") & ~F.col("success")) if attack else (F.col("success") & ~F.col("is_attack_ip"))
    return (
        logins.where(window & cond)
        .groupBy("ua_key", "browser", "os", "device_type")
        .agg(F.count(F.lit(1)).alias("n"))
        .orderBy(F.col("n").desc(), F.col("ua_key"))
        .limit(top)
    )
