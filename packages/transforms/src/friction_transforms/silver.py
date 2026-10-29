"""Silver: typed, conformed, deduplicated, and checked by named rules.

Each desk has a ``checked`` function that types every column and attaches ``violations``, an array
of the rule codes a row breaks. Silver is the rows with none; the quarantine is the rows with any,
kept with their rule codes and never deleted. The rules are in contracts/quarantine_rules.yaml with
their origin (the first look at the real files, or the specification) and each has a test.

Exact repeats of a natural key with the same payload (a drop delivered twice) are deduplicated,
keeping the first arrival; they are not quarantined. A natural key that arrives again with a
different payload is quarantined, the later arrival only.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from friction_transforms.keys import merchant_key_sql, token_col

# Every output names its columns: drop() would make Spark Connect ask the server for the schema,
# which a pipeline refuses while it defines a flow (its inputs do not exist yet).
PAYMENT_TYPED = (
    "txn_ref", "account_key", "user_key", "card_slot", "account_ref", "event_ts", "event_date", "amount_cents",
    "currency", "channel", "merchant_key", "merchant_city", "merchant_state", "merchant_country", "zip5", "mcc",
    "errors", "_rescued_data", "drop_id", "drop_end", "source_file", "ingested_at", "location", "foreign",
    "has_error", "is_refund",
)  # fmt: skip
PAYMENT_SILVER = (*tuple(c for c in PAYMENT_TYPED if c != "_rescued_data"), "payload")
LOGIN_TYPED = (
    "login_ref", "user_key", "event_ts", "event_date", "rtt_ms", "ip_key", "ip_prefix16", "country", "region",
    "city", "asn", "ua_key", "browser", "browser_family", "os", "device_type", "success", "is_attack_ip",
    "flag_asn_unrealistic", "flag_ip_private", "_rescued_data", "drop_id", "drop_end", "source_file", "ingested_at",
)  # fmt: skip
LOGIN_SILVER = (*tuple(c for c in LOGIN_TYPED if c != "_rescued_data"), "payload", "unknown_account")
# The first look found one account id holding 14,025,899 attempts (45 percent of the file) with four
# successes: the bucket the service files sign ins against unknown accounts under. An account with
# more attempts than this and almost no successes is that bucket; its rows stay in silver, flagged,
# and the user state machine treats each of them as a stranger.
UNKNOWN_ACCOUNT_ATTEMPTS = 1_000_000
UNKNOWN_ACCOUNT_SUCCESS_SHARE = 0.001

CHANNEL = F.create_map(
    F.lit("Swipe Transaction"),
    F.lit(0),
    F.lit("Chip Transaction"),
    F.lit(1),
    F.lit("Online Transaction"),
    F.lit(2),
)
# A daily drop that carries an event more than this many days old breaks the watermark rule.
WATERMARK_DAYS = 8
RBA_FIRST = "2020-02-01"
RBA_END = "2021-03-01"


def _drop_end(drop_id: Column) -> Column:
    """When a drop closes: the next day for a daily drop, the next month for a monthly one."""
    label = F.regexp_extract(drop_id, r"^\d{4}_(.*)$", 1)
    is_day = F.length(label) == 10
    as_day = F.to_timestamp(F.date_add(F.to_date(label, "yyyy-MM-dd"), 1))
    as_month = F.to_timestamp(F.add_months(F.to_date(F.concat(label, F.lit("-01")), "yyyy-MM-dd"), 1))
    return F.when(is_day, as_day).otherwise(as_month)


def _rules(*pairs: tuple[str, Column]) -> Column:
    return F.filter(F.array(*[F.when(cond, F.lit(code)) for code, cond in pairs]), lambda x: x.isNotNull())


def apply_contract(bronze: DataFrame, renames: dict[str, str], rejected_files: tuple[str, ...]) -> DataFrame:
    """Approved renames read a column back out of rescued data; rejected files are marked.

    Both are contract changes, made by a reviewed commit and a dated DECISIONS.md entry, never by
    hand in the workspace. ``_rescued_left`` is what rescued data holds once the approved renames
    and Auto Loader's own ``_file_path`` key are taken out; anything left breaks the rescued rule.
    """
    rescued = F.from_json(F.col("_rescued_data"), "map<string,string>")
    out = bronze
    for source, target in renames.items():
        out = out.withColumn(target, F.coalesce(F.col(target), rescued[source]))
    ignore = ["_file_path", *renames]
    left = F.map_filter(rescued, lambda k, v: ~k.isin(*ignore))
    name = F.regexp_extract(F.col("source_file"), r"([^/]+)$", 1)
    rejected = name.isin(*rejected_files) if rejected_files else F.lit(False)
    return out.withColumn("_rescued_left", F.coalesce(F.size(left), F.lit(0))).withColumn("_rejected", rejected)


def payments_checked(
    bronze: DataFrame,
    salt: Column,
    renames: dict[str, str] | None = None,
    rejected_files: tuple[str, ...] = (),
) -> DataFrame:
    bronze = apply_contract(bronze, renames or {}, rejected_files)
    month = F.lpad(F.col("month"), 2, "0")
    day = F.lpad(F.col("day"), 2, "0")
    ts = F.try_to_timestamp(F.concat_ws(" ", F.concat_ws("-", "year", month, day), "time"), F.lit("yyyy-MM-dd HH:mm"))
    amount = F.regexp_replace("amount", r"[$,]", "").try_cast("decimal(12,2)")
    state = F.trim(F.col("merchant_state"))
    typed = bronze.select(
        F.col("txn_ref").try_cast("bigint").alias("txn_ref"),
        token_col("account", F.concat_ws(":", "user", "card"), salt).alias("account_key"),
        token_col("user", F.col("user"), salt).alias("user_key"),
        F.col("card").try_cast("int").alias("card_slot"),
        (F.col("user").try_cast("bigint") * 100 + F.col("card").try_cast("bigint")).alias("account_ref"),
        ts.alias("event_ts"),
        F.to_date(ts).alias("event_date"),
        (amount * 100).cast("bigint").alias("amount_cents"),
        F.lit("USD").alias("currency"),
        CHANNEL[F.col("use_chip")].alias("channel"),
        merchant_key_sql(F.col("merchant_name")).alias("merchant_key"),
        F.trim("merchant_city").alias("merchant_city"),
        F.when(F.length(state) == 2, F.upper(state)).alias("merchant_state"),
        F.when(state == "", F.lit(None))
        .when(F.length(state) == 2, F.lit("US"))
        .otherwise(state)
        .alias("merchant_country"),
        F.when(F.col("zip") == "", F.lit(None))
        .otherwise(F.lpad(F.col("zip").try_cast("double").try_cast("int").cast("string"), 5, "0"))
        .alias("zip5"),
        F.col("mcc").try_cast("int").alias("mcc"),
        F.filter(F.split(F.coalesce("errors", F.lit("")), ","), lambda x: x != "").alias("errors"),
        F.col("_rescued_data"),
        F.col("_rescued_left"),
        F.col("_rejected"),
        F.col("drop_id"),
        _drop_end(F.col("drop_id")).alias("drop_end"),
        F.col("source_file"),
        F.col("ingested_at"),
    )
    typed = (
        typed.withColumn("location", F.coalesce("merchant_state", "merchant_country", F.lit("")))
        .withColumn("foreign", F.coalesce(F.col("merchant_country") != "US", F.lit(False)))
        .withColumn("has_error", F.size("errors") > 0)
        .withColumn("is_refund", F.col("amount_cents") < 0)
    )
    is_daily = F.length(F.regexp_extract("drop_id", r"^\d{4}_(.*)$", 1)) == 10
    return typed.withColumn(
        "violations",
        _rules(
            ("P_KEY", F.col("txn_ref").isNull()),
            ("P_TIME", F.col("event_ts").isNull()),
            ("P_AMOUNT", F.col("amount_cents").isNull()),
            ("P_MCC", F.col("mcc").isNull() | (F.col("mcc") < 1) | (F.col("mcc") > 9999)),
            ("P_CHANNEL", F.col("channel").isNull()),
            ("P_RESCUED", F.col("_rescued_left") > 0),
            ("P_REJECTED_FILE", F.col("_rejected")),
            ("P_FUTURE", F.col("event_ts") >= F.col("drop_end")),
            (
                "P_LATE",
                is_daily & (F.datediff(F.to_date("drop_end"), F.col("event_date")) > WATERMARK_DAYS + 1),
            ),
        ),
    ).select(*PAYMENT_TYPED, "violations")


PAYMENT_PAYLOAD = (
    "account_key", "event_ts", "amount_cents", "channel", "merchant_key", "merchant_city", "merchant_state",
    "merchant_country", "zip5", "mcc", "errors",
)  # fmt: skip
PAYMENT_BUSINESS_KEY = ("account_key", "event_ts", "amount_cents", "merchant_key")


def _payload_hash(cols: tuple[str, ...]) -> Column:
    return F.sha2(F.to_json(F.struct(*[F.col(c) for c in cols])), 256)


def payments_split(checked: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Deduplicate exact repeats, quarantine conflicting repeats and rule breaks. Returns (silver, quarantine)."""
    arrival = Window.partitionBy("txn_ref").orderBy("drop_id", "source_file")
    base = checked.withColumn("payload", _payload_hash(PAYMENT_PAYLOAD))
    first_payload = F.first("payload").over(arrival)
    rank = F.row_number().over(arrival.orderBy("drop_id", "source_file", "payload"))
    ranked = base.withColumn("_first", first_payload).withColumn("_rank", rank)
    # A later arrival with the payload of the first is a duplicate delivery: dropped, counted.
    deduped = ranked.where((F.col("_rank") == 1) | (F.col("payload") != F.col("_first")))
    conflict = F.when(F.col("_rank") > 1, F.array(F.lit("P_KEY_CONFLICT"))).otherwise(F.array())
    deduped = deduped.withColumn("violations", F.array_union("violations", conflict))
    # The source itself has transactions that share a business key with different payloads.
    bkey = Window.partitionBy(*PAYMENT_BUSINESS_KEY)
    business = F.size(F.collect_set("payload").over(bkey)) > 1
    deduped = deduped.withColumn(
        "violations",
        F.when(
            F.col("account_key").isNotNull() & F.col("event_ts").isNotNull() & business,
            F.array_union("violations", F.array(F.lit("P_BUSINESS_KEY"))),
        ).otherwise(F.col("violations")),
    )
    silver = deduped.where(F.size("violations") == 0).select(*PAYMENT_SILVER)
    quarantine = deduped.where(F.size("violations") > 0)
    return silver, quarantine


def logins_checked(
    bronze: DataFrame,
    salt: Column,
    renames: dict[str, str] | None = None,
    rejected_files: tuple[str, ...] = (),
) -> DataFrame:
    bronze = apply_contract(bronze, renames or {}, rejected_files)
    ts = F.try_to_timestamp(F.col("login_timestamp"), F.lit("yyyy-MM-dd HH:mm:ss.SSS"))
    ip_ok = F.col("ip_address").rlike(r"^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$")
    octets = F.split("ip_address", r"\.")
    prefix = F.when(ip_ok, F.concat(octets[0], F.lit("."), octets[1], F.lit(".0.0/16")))
    browser = F.col("browser")
    typed = bronze.select(
        F.col("login_index").try_cast("bigint").alias("login_ref"),
        token_col("user", F.col("user_id"), salt).alias("user_key"),
        ts.alias("event_ts"),
        F.to_date(ts).alias("event_date"),
        F.col("rtt_ms").try_cast("double").alias("rtt_ms"),
        token_col("ip", F.col("ip_address"), salt).alias("ip_key"),
        prefix.alias("ip_prefix16"),
        F.col("country"),
        F.when(F.col("region") == "-", F.lit(None)).otherwise(F.col("region")).alias("region"),
        F.when(F.col("city") == "-", F.lit(None)).otherwise(F.col("city")).alias("city"),
        F.col("asn").try_cast("bigint").alias("asn"),
        token_col("ua", F.col("user_agent"), salt).alias("ua_key"),
        browser.alias("browser"),
        F.regexp_extract(browser, r"^([A-Za-z][A-Za-z .]*?)(?:\s+\d.*)?$", 1).alias("browser_family"),
        F.col("os"),
        F.col("device_type"),
        (F.col("login_successful") == "True").alias("success"),
        (F.col("is_attack_ip") == "True").alias("is_attack_ip"),
        (F.col("asn").try_cast("bigint") >= 500000).alias("flag_asn_unrealistic"),
        F.col("ip_address").startswith("10.").alias("flag_ip_private"),
        F.col("_rescued_data"),
        F.col("_rescued_left"),
        F.col("_rejected"),
        F.col("drop_id"),
        _drop_end(F.col("drop_id")).alias("drop_end"),
        F.col("source_file"),
        F.col("ingested_at"),
        ip_ok.alias("_ip_ok"),
    )
    is_daily = F.length(F.regexp_extract("drop_id", r"^\d{4}_(.*)$", 1)) == 10
    return typed.withColumn(
        "violations",
        _rules(
            ("L_KEY", F.col("login_ref").isNull()),
            ("L_TIME", F.col("event_ts").isNull()),
            ("L_SPAN", (F.col("event_ts") < F.lit(RBA_FIRST)) | (F.col("event_ts") >= F.lit(RBA_END))),
            ("L_IP", ~F.col("_ip_ok")),
            ("L_UA", F.col("device_type").isNull() | (F.col("device_type") == "")),
            ("L_RESCUED", F.col("_rescued_left") > 0),
            ("L_REJECTED_FILE", F.col("_rejected")),
            ("L_FUTURE", F.col("event_ts") >= F.col("drop_end")),
            (
                "L_LATE",
                is_daily & (F.datediff(F.to_date("drop_end"), F.col("event_date")) > WATERMARK_DAYS + 1),
            ),
        ),
    ).select(*LOGIN_TYPED, "violations")


LOGIN_PAYLOAD = (
    "user_key", "event_ts", "rtt_ms", "ip_key", "country", "region", "city", "asn", "ua_key", "browser", "os",
    "device_type", "success", "is_attack_ip",
)  # fmt: skip


def login_account_stats(
    bronze: DataFrame,
    salt: Column,
    attempts: int = UNKNOWN_ACCOUNT_ATTEMPTS,
    share: float = UNKNOWN_ACCOUNT_SUCCESS_SHARE,
) -> DataFrame:
    """user_key, attempts, successes over every attempt delivered, and the unknown account flag."""
    return (
        bronze.groupBy(token_col("user", F.col("user_id"), salt).alias("user_key"))
        .agg(
            F.count(F.lit(1)).alias("attempts"),
            F.count_if(F.col("login_successful") == "True").alias("successes"),
        )
        .withColumn(
            "unknown_account",
            (F.col("attempts") > F.lit(attempts)) & (F.col("successes") < F.col("attempts") * F.lit(share)),
        )
    )


def logins_split(checked: DataFrame, accounts: DataFrame) -> tuple[DataFrame, DataFrame]:
    """accounts: login_account_stats(). Silver carries its unknown account flag."""
    flags = accounts.select("user_key", "unknown_account")
    checked = checked.join(flags, "user_key", "left").withColumn(
        "unknown_account", F.coalesce(F.col("unknown_account"), F.lit(False))
    )
    arrival = Window.partitionBy("login_ref").orderBy("drop_id", "source_file")
    base = checked.withColumn("payload", _payload_hash(LOGIN_PAYLOAD))
    ranked = base.withColumn("_first", F.first("payload").over(arrival)).withColumn(
        "_rank", F.row_number().over(arrival.orderBy("drop_id", "source_file", "payload"))
    )
    deduped = ranked.where((F.col("_rank") == 1) | (F.col("payload") != F.col("_first")))
    conflict = F.when(F.col("_rank") > 1, F.array(F.lit("L_KEY_CONFLICT"))).otherwise(F.array())
    deduped = deduped.withColumn("violations", F.array_union("violations", conflict))
    silver = deduped.where(F.size("violations") == 0).select(*LOGIN_SILVER)
    quarantine = deduped.where(F.size("violations") > 0)
    return silver, quarantine


def quarantine_rows(quarantine: DataFrame, desk: str, key: str) -> DataFrame:
    """One row per rule broken, the shape of the shared quarantine table."""
    return quarantine.select(
        F.lit(desk).alias("desk"),
        F.col(key).cast("bigint").alias("event_ref"),
        F.explode("violations").alias("rule"),
        F.col("drop_id"),
        F.col("source_file"),
        F.col("payload"),
        F.col("_rescued_data").alias("rescued"),
    )


LABEL_COLUMNS = (
    "label_ref", "event_ref", "desk", "is_fraud", "event_ts", "reported_at", "seq", "source", "drop_id", "source_file",
)  # fmt: skip


def labels_typed(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.col("label_ref").try_cast("bigint").alias("label_ref"),
        F.col("event_ref").try_cast("bigint").alias("event_ref"),
        F.col("desk"),
        (F.lower(F.col("is_fraud")) == "true").alias("is_fraud"),
        F.try_to_timestamp(F.col("event_ts")).alias("event_ts"),
        F.try_to_timestamp(F.col("reported_at")).alias("reported_at"),
        F.col("seq").try_cast("bigint").alias("seq"),
        F.col("source"),
        F.col("drop_id"),
        F.col("source_file"),
    )


def labels_checked(labels: DataFrame, events: DataFrame, key: str, *, has_refunds: bool) -> DataFrame:
    """Attach the label rules. events: the desk's silver with its key and, for payments, is_refund."""
    refund = F.col("is_refund") if has_refunds else F.lit(False)
    known = events.select(F.col(key).alias("event_ref"), refund.alias("_refund"), F.lit(True).alias("_exists"))
    joined = labels.join(known, "event_ref", "left")
    return joined.withColumn(
        "violations",
        _rules(
            ("B_NO_EVENT", F.col("_exists").isNull()),
            (
                "B_REFUND",
                F.coalesce(F.col("_refund"), F.lit(False)) & F.col("is_fraud") & (F.col("source") == "chargeback"),
            ),
        ),
    ).select(*LABEL_COLUMNS, "violations")


def cdc_typed(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.col("account_ref").try_cast("bigint").alias("account_ref"),
        F.col("op"),
        F.col("seq").try_cast("bigint").alias("seq"),
        F.try_to_timestamp(F.col("changed_at")).alias("changed_at"),
        F.try_to_timestamp(F.col("emitted_at")).alias("emitted_at"),
        F.col("card_generation").try_cast("bigint").alias("card_generation"),
        F.col("status"),
        F.col("credit_limit_cents").try_cast("bigint").alias("credit_limit_cents"),
        F.col("currency"),
        F.col("drop_id"),
        F.col("source_file"),
    )
