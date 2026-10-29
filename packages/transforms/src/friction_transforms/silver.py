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
    amount = F.try_cast(F.regexp_replace("amount", r"[$,]", ""), "decimal(12,2)")
    state = F.trim(F.col("merchant_state"))
    typed = bronze.select(
        F.try_cast("txn_ref", "bigint").alias("txn_ref"),
        token_col("account", F.concat_ws(":", "user", "card"), salt).alias("account_key"),
        token_col("user", F.col("user"), salt).alias("user_key"),
        F.try_cast("card", "int").alias("card_slot"),
        (F.try_cast("user", "bigint") * 100 + F.try_cast("card", "bigint")).alias("account_ref"),
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
        .otherwise(F.lpad(F.try_cast(F.try_cast("zip", "double"), "int").cast("string"), 5, "0"))
        .alias("zip5"),
        F.try_cast("mcc", "int").alias("mcc"),
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
    ).drop("_rescued_left", "_rejected")


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
    ).drop("_first", "_rank")
    silver = deduped.where(F.size("violations") == 0).drop("violations", "_rescued_data")
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
        F.try_cast("login_index", "bigint").alias("login_ref"),
        token_col("user", F.col("user_id"), salt).alias("user_key"),
        ts.alias("event_ts"),
        F.to_date(ts).alias("event_date"),
        F.try_cast("rtt_ms", "double").alias("rtt_ms"),
        token_col("ip", F.col("ip_address"), salt).alias("ip_key"),
        prefix.alias("ip_prefix16"),
        F.col("country"),
        F.when(F.col("region") == "-", F.lit(None)).otherwise(F.col("region")).alias("region"),
        F.when(F.col("city") == "-", F.lit(None)).otherwise(F.col("city")).alias("city"),
        F.try_cast("asn", "bigint").alias("asn"),
        token_col("ua", F.col("user_agent"), salt).alias("ua_key"),
        browser.alias("browser"),
        F.regexp_extract(browser, r"^([A-Za-z][A-Za-z .]*?)(?:\s+\d.*)?$", 1).alias("browser_family"),
        F.col("os"),
        F.col("device_type"),
        (F.col("login_successful") == "True").alias("success"),
        (F.col("is_attack_ip") == "True").alias("is_attack_ip"),
        (F.try_cast("asn", "bigint") >= 500000).alias("flag_asn_unrealistic"),
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
    ).drop("_ip_ok", "_rescued_left", "_rejected")


LOGIN_PAYLOAD = (
    "user_key", "event_ts", "rtt_ms", "ip_key", "country", "region", "city", "asn", "ua_key", "browser", "os",
    "device_type", "success", "is_attack_ip",
)  # fmt: skip


def logins_split(checked: DataFrame) -> tuple[DataFrame, DataFrame]:
    arrival = Window.partitionBy("login_ref").orderBy("drop_id", "source_file")
    base = checked.withColumn("payload", _payload_hash(LOGIN_PAYLOAD))
    ranked = base.withColumn("_first", F.first("payload").over(arrival)).withColumn(
        "_rank", F.row_number().over(arrival.orderBy("drop_id", "source_file", "payload"))
    )
    deduped = ranked.where((F.col("_rank") == 1) | (F.col("payload") != F.col("_first")))
    conflict = F.when(F.col("_rank") > 1, F.array(F.lit("L_KEY_CONFLICT"))).otherwise(F.array())
    deduped = deduped.withColumn("violations", F.array_union("violations", conflict)).drop("_first", "_rank")
    silver = deduped.where(F.size("violations") == 0).drop("violations", "_rescued_data")
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
