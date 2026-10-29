"""Order independent digests of tables, and the reconcile statistics, the same on both engines.

A table digest is the row count, the XOR and the sum (modulo a prime) of xxhash64 over each row's
content columns. XOR alone would hide a row present twice; the sum catches it. Metadata columns
(ingest time, file paths, modification times) are left out, so re-running a day or running on the
other engine gives the same digest when the content is the same. xxhash64 is the same function in
open source Spark and on Databricks, which is what makes the two engine reconcile possible.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

METADATA = frozenset(
    {"ingested_at", "source_file", "file_modified_at", "drop_id", "drop_end", "_rescued_data", "payload", "rescued"}
)
PRIME = 2305843009213693951  # 2**61 - 1


def content_columns(df: DataFrame) -> list[str]:
    return sorted(c for c in df.columns if c not in METADATA and not c.startswith("_"))


def _row_hash(df: DataFrame, cols: list[str]) -> Column:
    parts = [F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("\u0000")) for c in cols]
    return F.xxhash64(*parts)


def table_digest(df: DataFrame, cols: list[str] | None = None) -> dict[str, Any]:
    use = cols or content_columns(df)
    h = _row_hash(df, use)
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.bit_xor(h).alias("xor"),
        # Each term is below 2**61, so a 64 bit sum overflows after a few rows; 38 digits hold
        # the sum of every row either source has, and the remainder is taken in Python.
        F.sum(F.pmod(h, F.lit(PRIME)).cast("decimal(38,0)")).alias("sum"),
    ).collect()[0]
    total = int(row["sum"] or 0) % PRIME
    return {
        "rows": int(row["rows"]),
        "digest": f"{int(row['rows'])}:{int(row['xor'] or 0) & (2**64 - 1):016x}:{total:016x}",
    }


def grain_stats(df: DataFrame, grain: dict[str, Column], measures: dict[str, Column], key: str) -> list[dict[str, Any]]:
    """Counts, distinct keys and sums of measures at one grain, sorted, as plain values."""
    grouped = df.groupBy(*[v.alias(k) for k, v in grain.items()]).agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(key).alias("keys"),
        *[F.sum(v).alias(k) for k, v in measures.items()],
    )
    out = [r.asDict() for r in grouped.orderBy(*grain.keys()).collect()]
    for r in out:
        for k, v in list(r.items()):
            if hasattr(v, "isoformat"):
                r[k] = v.isoformat()
            elif v is not None and not isinstance(v, (int, float, str, bool)):
                r[k] = float(v)
    return out


def reconcile_stats(tables: dict[str, DataFrame]) -> dict[str, Any]:
    """The four grains the build reconciles: desk, month, day and user bucket."""
    out: dict[str, Any] = {}
    pay = tables["payments_silver"]
    pay_measures = {"amount_cents": F.col("amount_cents"), "refunds": F.col("is_refund").cast("int")}
    log = tables["logins_silver"]
    log_measures = {"successes": F.col("success").cast("int"), "attack_ip": F.col("is_attack_ip").cast("int")}
    for name, df, key, measures, user in (
        ("payments", pay, "txn_ref", pay_measures, "account_key"),
        ("logins", log, "login_ref", log_measures, "user_key"),
    ):
        out[name] = {
            "desk": grain_stats(df, {"desk": F.lit(name)}, measures, key),
            "month": grain_stats(df, {"month": F.date_format("event_date", "yyyy-MM")}, measures, key),
            "day": grain_stats(df, {"day": F.col("event_date")}, measures, key),
            "user_bucket": grain_stats(df, {"bucket": F.pmod(F.xxhash64(user), F.lit(32))}, measures, key),
        }
    feats = {}
    for name in ("payments_features", "logins_features"):
        if name in tables:
            df = tables[name]
            cols = [c for c in df.columns if c not in {"txn_ref", "login_ref"}]
            row = df.agg(F.count(F.lit(1)).alias("rows"), *[F.sum(F.col(c)).alias(c) for c in cols]).collect()[0]
            feats[name] = {k: (float(v) if v is not None else None) for k, v in row.asDict().items()}
    out["feature_sums"] = feats
    labels = {}
    for name in ("labels_payments_history", "labels_logins_history", "cards_history"):
        if name in tables:
            labels[name] = table_digest(tables[name])
    out["histories"] = labels
    out["quarantine"] = [
        r.asDict() for r in tables["quarantine"].groupBy("desk", "rule").count().orderBy("desk", "rule").collect()
    ]
    return out
