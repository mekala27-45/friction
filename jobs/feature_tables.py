"""The feature tables task, and the check of the point in time lookup against the feature store client.

--step publish writes gold.fs_merchant_daily and gold.fs_card_daily (friction_transforms.feature_tables)
as tables with a primary key on the entity and a TIMESERIES column, which Unity Catalog serves as
feature tables, with change data feed on. --step check builds a training set for one test month of
payments with the Databricks feature engineering client, a timestamp lookup on both tables, and
compares every value with friction's own lookup; the record goes to the exports volume.
"""

from __future__ import annotations

import json
import os

import _boot

args = _boot.parse([("--step", {"default": "publish"}), ("--tag", {"default": "feature-tables"}),
                    ("--month", {"default": "2019-07"})])  # fmt: skip
spark = _boot.session()

from friction_transforms import feature_tables as ft  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

TABLES = {
    "fs_merchant_daily": ("merchant_key", ft.MERCHANT, ft.merchant_daily),
    "fs_card_daily": ("account_key", ft.CARD, ft.card_daily),
}
folder = f"{_boot.volume(args, 'exports')}/{args.tag}"
os.makedirs(folder, exist_ok=True)
ledger = _boot.Ledger(spark, args, f"feature_tables_{args.step}")


def publish() -> dict[str, int]:
    silver = spark.read.table(_boot.fq(args, "silver", "payments_silver"))
    labels = spark.read.table(_boot.fq(args, "cdc", "labels_payments_history"))
    counts = {}
    for name, (key, cols, build) in TABLES.items():
        table = _boot.fq(args, "gold", name)
        build(silver, labels).createOrReplaceTempView("_fs")
        types = ", ".join(f"{c} {'DOUBLE' if c.endswith('share') else 'BIGINT'}" for c in cols)
        spark.sql(f"CREATE OR REPLACE TABLE {table} ({key} STRING NOT NULL, as_of TIMESTAMP NOT NULL, {types}, "
                  f"CONSTRAINT {name}_pk PRIMARY KEY ({key}, as_of TIMESERIES)) "
                  "TBLPROPERTIES (delta.enableChangeDataFeed = true)")  # fmt: skip
        spark.sql(f"INSERT INTO {table} SELECT {key}, as_of, {', '.join(cols)} FROM _fs")
        counts[name] = int(spark.read.table(table).count())
    return counts


def check() -> dict[str, object]:
    from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

    start = F.to_timestamp(F.lit(f"{args.month}-01"))
    events = (
        spark.read.table(_boot.fq(args, "silver", "payments_silver"))
        .where((F.col("event_ts") >= start) & (F.col("event_ts") < F.add_months(start, 1)))
        .select("txn_ref", "merchant_key", "account_key", "event_ts")
    )
    lookups = [FeatureLookup(table_name=_boot.fq(args, "gold", name), lookup_key=key, timestamp_lookup_key="event_ts",
                             feature_names=list(cols)) for name, (key, cols, _) in TABLES.items()]  # fmt: skip
    theirs = FeatureEngineeringClient().create_training_set(df=events, feature_lookups=lookups, label=None).load_df()
    ours = events
    for name, (key, cols, _) in TABLES.items():
        ours = ft.pit_lookup(ours, spark.read.table(_boot.fq(args, "gold", name)), key, "event_ts", cols, "txn_ref")
    features = [c for _, cols, _ in TABLES.values() for c in cols]
    a = theirs.select("txn_ref", *[F.col(c).alias(f"a_{c}") for c in features])
    b = ours.select("txn_ref", *[F.col(c).alias(f"b_{c}") for c in features])
    joined = a.join(b, "txn_ref", "full_outer")
    same = F.lit(True)
    for c in features:
        same = same & F.col(f"a_{c}").eqNullSafe(F.col(f"b_{c}"))
    empty = F.col(f"a_{features[0]}").isNull()
    totals = [F.count(F.lit(1)).alias("n"), F.sum(F.when(same, 1).otherwise(0)).alias("equal"),
              F.sum(F.when(empty, 1).otherwise(0)).alias("no_history")]  # fmt: skip
    row = joined.agg(*totals).collect()[0]
    return {"month": args.month, "events": int(events.count()), "compared": int(row["n"]), "equal": int(row["equal"]),
            "without_history": int(row["no_history"]), "features": features}  # fmt: skip


try:
    record = publish() if args.step == "publish" else check()
    with open(f"{folder}/{args.step}.json", "w", encoding="utf-8") as fh:
        json.dump(record, fh, indent=1, default=str)
    ledger.write(inputs={"step": args.step}, outputs=record, rows_in=0, rows_out=0, status="succeeded")
    print(json.dumps(record, indent=1, default=str))
    if args.step == "check" and (record["equal"] != record["compared"] or record["compared"] != record["events"]):
        raise RuntimeError(f"the feature store lookup and friction's differ: {record}")
except Exception as exc:
    ledger.write(inputs={"step": args.step}, outputs=[], rows_in=0, rows_out=0, status="failed", detail=str(exc))
    raise
