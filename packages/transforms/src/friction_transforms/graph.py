"""Graph features: what the entities an event touches have been connected to before it.

A fraud ring shows in the graph of cards, merchants, users, addresses and browsers long before it
shows in any one card's history: a merchant that suddenly sees many new cards, a merchant other
compromised cards have already used, a user whose other card was charged back last week, an
address many accounts started signing in from today. Each feature here counts edges of that graph
with one rule: only edges that existed strictly before the event's own timestamp, and only labels
the bank had received by then (the label's system time, not the event's).

Every count is an exact window sum over one sorted partition, never a set held in memory: an edge
becomes a marker row at the second it appears (a card's first purchase at a merchant, a card's
first known fraud), the events to describe are rows with no weight, and a range frame ending one
unit before the row counts the markers strictly earlier. The same functions run on both engines.

Payments, per transaction:
  m_cards_before        distinct cards seen at the merchant before
  m_new_cards_7d        cards whose first purchase at the merchant fell in the 7 days before
  m_txn_7d              purchases at the merchant in the 7 days before
  m_fraud_cards_known   cards seen at the merchant whose fraud was known before the event
  m_fraud_share         m_fraud_cards_known over m_cards_before (zero when no card was seen)
  u_other_cards_known   the user's other cards with fraud known before the event
  card_known_fraud      1 if this card already had a known fraud before the event

A chargeback that is later reversed keeps the card flagged from its first known fraud: an analyst
treats a card that was once charged back as compromised, and the flag never reads a label before it
existed.

Logins, per attempt (synthetic attacks are described against the real graph and never join it):
  ip_users_before       distinct known accounts seen from the address before
  ip_new_users_1d       known accounts first seen from the address in the 24 hours before
  ip_unknown_1h         attempts on unknown accounts from the address in the hour before
  asn_new_users_1h      known accounts first seen on the network in the hour before
  ua_users_before       distinct known accounts seen with the same browser string before
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window, WindowSpec
from pyspark.sql import functions as F

DAY_S = 86_400
HOUR_MS = 3_600_000
DAY_MS = 86_400_000

PAYMENT_GRAPH = ("m_cards_before", "m_new_cards_7d", "m_txn_7d", "m_fraud_cards_known", "m_fraud_share",
                 "u_other_cards_known", "card_known_fraud")  # fmt: skip
LOGIN_GRAPH = ("ip_users_before", "ip_new_users_1d", "ip_unknown_1h", "asn_new_users_1h", "ua_users_before")


def _before(key: str, start: int | None = None) -> WindowSpec:
    """Rows of the same key strictly earlier than this one, back to start units if given."""
    lower = Window.unboundedPreceding if start is None else -start
    return Window.partitionBy(key).orderBy("t").rangeBetween(lower, -1)


def _sum(col: str, window: WindowSpec) -> Column:
    return F.coalesce(F.sum(col).over(window), F.lit(0)).cast("long")


def card_fraud_known(silver: DataFrame, labels: DataFrame) -> DataFrame:
    """account_key, user_key, card_known: the second the bank first knew of a fraud on the card."""
    fraud = labels.where(F.col("is_fraud")).select(
        F.col("event_ref").alias("txn_ref"), F.unix_timestamp(F.col("__START_AT.reported_at")).alias("known")
    )
    return (
        fraud.join(silver.select("txn_ref", "account_key", "user_key"), "txn_ref")
        .groupBy("account_key", "user_key")
        .agg(F.min("known").alias("card_known"))
    )


def payments_graph(silver: DataFrame, labels: DataFrame) -> DataFrame:
    ev = silver.select("txn_ref", "account_key", "user_key", "merchant_key", F.unix_timestamp("event_ts").alias("t"))
    known = card_fraud_known(silver, labels)
    pairs = (
        ev.groupBy("merchant_key", "account_key")
        .agg(F.min("t").alias("first_t"))
        .join(known.select("account_key", "card_known"), "account_key", "left")
    )
    zero = F.lit(0).cast("long")
    events = ev.select("merchant_key", "t", "txn_ref", zero.alias("visit"), zero.alias("fraud"), F.lit(1).alias("txn"))
    visits = pairs.select("merchant_key", F.col("first_t").alias("t"), F.lit(None).cast("long").alias("txn_ref"),
                          F.lit(1).alias("visit"), zero.alias("fraud"), zero.alias("txn"))  # fmt: skip
    frauds = pairs.where(F.col("card_known").isNotNull()).select(
        "merchant_key", F.greatest("first_t", "card_known").alias("t"), F.lit(None).cast("long").alias("txn_ref"),
        zero.alias("visit"), F.lit(1).alias("fraud"), zero.alias("txn"),
    )  # fmt: skip
    rows = events.unionByName(visits).unionByName(frauds)
    ever, week = _before("merchant_key"), _before("merchant_key", 7 * DAY_S)
    merchant = (
        rows.select(
            "txn_ref",
            _sum("visit", ever).alias("m_cards_before"),
            _sum("visit", week).alias("m_new_cards_7d"),
            _sum("txn", week).alias("m_txn_7d"),
            _sum("fraud", ever).alias("m_fraud_cards_known"),
        )
        .where(F.col("txn_ref").isNotNull())
        .withColumn(
            "m_fraud_share",
            F.when(F.col("m_cards_before") > 0, F.col("m_fraud_cards_known") / F.col("m_cards_before")).otherwise(0.0),
        )
    )

    # The user's cards with a known fraud, counted at the second each became known.
    own = ev.join(known.select("account_key", "card_known"), "account_key", "left").withColumn(
        "card_known_fraud", F.when(F.col("card_known") < F.col("t"), 1).otherwise(0)
    )
    user_rows = own.select("user_key", "t", "txn_ref", zero.alias("fraud")).unionByName(
        known.select(
            "user_key",
            F.col("card_known").alias("t"),
            F.lit(None).cast("long").alias("txn_ref"),
            F.lit(1).alias("fraud"),
        )
    )
    user = (
        user_rows.select("txn_ref", _sum("fraud", _before("user_key")).alias("user_cards_known"))
        .where(F.col("txn_ref").isNotNull())
        .join(own.select("txn_ref", "card_known_fraud"), "txn_ref")
        .select(
            "txn_ref",
            (F.col("user_cards_known") - F.col("card_known_fraud")).alias("u_other_cards_known"),
            "card_known_fraud",
        )
    )
    return merchant.join(user, "txn_ref").select("txn_ref", *PAYMENT_GRAPH)


def logins_graph(inputs: DataFrame) -> DataFrame:
    """inputs: login_feature_inputs, with synthetic attacks unioned in when there are any. An unknown
    account's attempts carry a user key that starts with z (gold.login_inputs)."""
    li = inputs.select(
        "login_ref", "user_key", F.coalesce("ip_key", F.lit("")).alias("ip"), F.col("v_asn").alias("asn"),
        F.col("v_ua").alias("ua"), F.col("event_ts_ms").alias("t"), F.col("synthetic"),
    )  # fmt: skip
    real = li.where(~F.col("synthetic"))
    known_user = ~F.col("user_key").startswith("z")
    ref = F.lit(None).cast("long").alias("login_ref")
    zero = F.lit(0).cast("long")

    def first_seen(key: str) -> DataFrame:
        return (
            real.where(known_user)
            .groupBy(key, "user_key")
            .agg(F.min("t").alias("t"))
            .select(key, "t", ref, F.lit(1).alias("seen"), zero.alias("unknown"))
        )

    def attempts(key: str) -> DataFrame:
        unknown = F.when(~F.col("synthetic") & ~known_user, 1).otherwise(0)
        return li.select(key, "t", "login_ref", zero.alias("seen"), unknown.alias("unknown"))

    ip_rows = attempts("ip").unionByName(first_seen("ip"))
    ip = ip_rows.select(
        "login_ref",
        _sum("seen", _before("ip")).alias("ip_users_before"),
        _sum("seen", _before("ip", DAY_MS)).alias("ip_new_users_1d"),
        _sum("unknown", _before("ip", HOUR_MS)).alias("ip_unknown_1h"),
    ).where(F.col("login_ref").isNotNull())
    asn = (
        attempts("asn")
        .unionByName(first_seen("asn"))
        .select("login_ref", _sum("seen", _before("asn", HOUR_MS)).alias("asn_new_users_1h"))
        .where(F.col("login_ref").isNotNull())
    )
    ua = (
        attempts("ua")
        .unionByName(first_seen("ua"))
        .select("login_ref", _sum("seen", _before("ua")).alias("ua_users_before"))
        .where(F.col("login_ref").isNotNull())
    )
    return ip.join(asn, "login_ref").join(ua, "login_ref").select("login_ref", *LOGIN_GRAPH)
