"""Performance experiments on the build machine, each a before and an after with the same inputs.

    python scripts/perf_experiments.py --lake data/lake/full

1. Skew. The unknown account holds 45 percent of the login file. Grouped as one user, the user
   state machine holds it in one group; with the per row stranger keys of gold.login_inputs it is
   spread across the buckets. Measured on the bucket's first 720,476 rows, the size of the group
   that killed the first month's features task on the 1 GB serverless cap: the bytes of the
   largest group a worker must hold, the peak Python allocation, and the time.
2. Bucket count. The state machines group by a hash bucket of the entity. Payments features over a
   fixed slice of card accounts with 8, 64, 256 and 1,024 buckets: time and the largest group.
3. File sizing. The login bronze table as written (many files per drop) against the same rows
   compacted to files of about 128 MB: a full scan and an aggregate.

Writes results/performance.json. Each number is a wall clock on this machine, so the page prints
the machine beside it.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import sys
import time
import tracemalloc
import zlib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from friction_features import batch
from friction_transforms import session
from friction_transforms.gold import BUCKETS
from pyspark.sql import functions as F


def measured(fn: Any) -> tuple[float, float]:
    """Seconds and the peak Python allocation in MB (numpy reports its buffers to tracemalloc)."""
    tracemalloc.start()
    t = time.monotonic()
    fn()
    secs = round(time.monotonic() - t, 2)
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    return secs, round(peak / 1e6, 1)


def timed(fn: Any) -> tuple[float, Any]:
    t = time.monotonic()
    out = fn()
    return round(time.monotonic() - t, 2), out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "performance.json")
    parser.add_argument("--sample", type=int, default=720_476)
    args = parser.parse_args()
    wh = (args.lake / "spark-warehouse").resolve()
    spark = session.get(shuffle_partitions=16)
    read = lambda name: spark.read.parquet(str(wh / name))  # noqa: E731
    result: dict[str, Any] = {
        "machine": {"cores": os.cpu_count(), "python": platform.python_version(), "system": platform.system()},
        "experiments": {},
    }

    # 1. Skew: the unknown account's rows through the user state machine, as one user and as strangers.
    unknown = read("login_account_stats").where(F.col("unknown_account")).select("user_key")
    rows = (
        read("logins_silver")
        .join(unknown, "user_key", "left_semi")
        .orderBy("event_ts", "login_ref")
        .limit(args.sample)
        .select(
            "user_key", "ip_key", "login_ref",
            (F.unix_timestamp("event_ts") * 1000).alias("event_ts_ms"),
            *[F.coalesce(F.col(c).cast("string"), F.lit("")).alias(f"v_{lvl}") for lvl, c in (
                ("ip", "ip_key"), ("asn", "asn"), ("country", "country"), ("region", "region"), ("city", "city"),
                ("ua", "ua_key"), ("browser", "browser"), ("os", "os"), ("device", "device_type"))],
            "rtt_ms", "success", F.lit(False).alias("synthetic"),
        )
        .toPandas()
    )  # fmt: skip
    if rows.empty:
        raise SystemExit("this lake has no unknown account; the skew experiment needs the full login file")
    strangers = rows.assign(user_key="z" + rows["login_ref"].astype(str))
    spread = strangers["user_key"].map(lambda k: zlib.crc32(k.encode()) % BUCKETS)
    as_one, peak_one = measured(lambda: batch.logins_partition(rows))

    def bucketed() -> None:
        for _, group in strangers.groupby(spread, sort=False):
            batch.logins_partition(group)

    as_strangers, peak_spread = measured(bucketed)
    largest = int(spread.value_counts().max())
    result["experiments"]["skew"] = {
        "rows": len(rows),
        "before_seconds": as_one,
        "after_seconds": as_strangers,
        "before_largest_group": len(rows),
        "after_largest_group": largest,
        "before_group_mb": round(float(rows.memory_usage(deep=True).sum()) / 1e6, 1),
        "after_group_mb": round(
            float(strangers[spread == spread.value_counts().idxmax()].memory_usage(deep=True).sum()) / 1e6, 2
        ),
        "before_peak_python_mb": peak_one,
        "after_peak_python_mb": peak_spread,
        "serverless_udf_cap_mb": 1024,
        "note": "the unknown account's rows through the user state machine, one group against strangers in "
        f"{BUCKETS} buckets",
    }

    # 2. Bucket count for the payments state machine on a fixed slice of accounts.
    inputs = read("payment_feature_inputs")
    accounts = inputs.select("account_key").distinct().orderBy("account_key").limit(400)
    slice_ = inputs.join(accounts, "account_key", "left_semi").drop("_bucket")
    n = slice_.count()
    buckets, largest_by = {}, {}
    for b in (8, 64, 256, BUCKETS):
        df = slice_.withColumn("_b", F.pmod(F.hash("account_key"), F.lit(b)))
        grouped = df.groupBy("_b").applyInPandas(
            lambda pdf: batch.payments_partition(pdf.drop(columns=["_b"])), batch.payments_schema()
        )
        secs, _ = timed(grouped.count)
        buckets[str(b)] = secs
        largest_by[str(b)] = int(df.groupBy("_b").count().agg(F.max("count")).collect()[0][0])
    result["experiments"]["buckets"] = {
        "rows": n,
        "seconds_by_buckets": buckets,
        "largest_group_by_buckets": largest_by,
    }

    # 3. File sizing: bronze as written against the same rows compacted.
    bronze = wh / "logins_bronze"
    files_before = len(list(bronze.glob("*.parquet")))
    compact = args.lake / "perf_logins_bronze_compact"
    if compact.exists():
        shutil.rmtree(compact)
    total_mb = sum(p.stat().st_size for p in bronze.glob("*.parquet")) / 1e6
    target = max(1, round(total_mb / 128))
    spark.read.parquet(str(bronze)).repartition(target).write.parquet(str(compact))
    files_after = len(list(compact.glob("*.parquet")))

    def scan(path: Path) -> int:
        return int(spark.read.parquet(str(path)).groupBy("drop_id").count().count())

    before, _ = timed(lambda: scan(bronze))
    after, _ = timed(lambda: scan(compact))
    shutil.rmtree(compact)
    result["experiments"]["file_sizing"] = {
        "files_before": files_before, "files_after": files_after, "mb": round(total_mb),
        "before_seconds": before, "after_seconds": after,
    }  # fmt: skip
    args.out.write_text(json.dumps(result, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result["experiments"], indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
