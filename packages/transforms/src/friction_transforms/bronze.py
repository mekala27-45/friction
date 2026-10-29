"""Bronze: the drops as they landed, every value still text, with where and when each row came from.

The reader (Auto Loader in the workspace, the file source on open source Spark) hands these
functions the drop's columns as strings plus the rescued data column and the file's path and
modification time. Bronze adds the drop id read from the path and the ingest time, and nothing
else: no typing, no cleaning. Restricted columns (the user id in clear, the IP address, the user
agent string) are present here only, behind column masks in the workspace; silver carries tokens.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

PAYMENT_TEXT = (
    "txn_ref", "user", "card", "year", "month", "day", "time", "amount", "use_chip",
    "merchant_name", "merchant_city", "merchant_state", "zip", "mcc", "errors",
)  # fmt: skip
LOGIN_TEXT = (
    "login_index", "login_timestamp", "user_id", "rtt_ms", "ip_address", "country", "region", "city",
    "asn", "user_agent", "browser", "os", "device_type", "login_successful", "is_attack_ip",
)  # fmt: skip
LABEL_TEXT = ("label_ref", "event_ref", "desk", "is_fraud", "event_ts", "reported_at", "seq", "source")
CDC_TEXT = (
    "account_ref", "user", "card", "op", "seq", "changed_at", "emitted_at", "card_generation", "status",
    "credit_limit_cents", "currency",
)  # fmt: skip

TEXT_COLUMNS: dict[str, tuple[str, ...]] = {
    "payments": PAYMENT_TEXT,
    "logins": LOGIN_TEXT,
    "labels_payments": LABEL_TEXT,
    "labels_logins": LABEL_TEXT,
    "cdc_cards": CDC_TEXT,
}
METADATA = ("_rescued_data", "source_file", "file_modified_at", "drop_id", "ingested_at")


def text_schema(feed: str) -> str:
    """The reader's schema: every expected column as a string. Anything else goes to rescued data."""
    return ", ".join(f"`{c}` STRING" for c in TEXT_COLUMNS[feed])


def with_metadata(raw: DataFrame) -> DataFrame:
    """raw has the text columns, _rescued_data, source_file and file_modified_at."""
    return raw.withColumn("drop_id", F.regexp_extract(F.col("source_file"), r"/drops/([^/]+)/", 1)).withColumn(
        "ingested_at", F.current_timestamp()
    )


def select(feed: str, raw: DataFrame) -> DataFrame:
    cols: list[Column] = [F.col(f"`{c}`").alias(c) for c in TEXT_COLUMNS[feed]]
    meta = [F.col(c) for c in METADATA]
    return with_metadata(raw).select(*cols, *meta)
