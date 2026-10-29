"""Row counts, digests, quarantine counts and the reconcile statistics of one lake.

    python scripts/summarize_lake.py --lake data/lake/full --engine local --out results/reconcile.json

The workspace's export job writes the same document from Delta (jobs/export.py), and
scripts/reconcile.py compares the two at the desk, month, day and user bucket grains. Each engine's
summary goes under its own key, so the file holds both sides of the comparison.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from friction_transforms import digests, session
from pyspark.sql import functions as F

TABLES = (
    "payments_bronze", "logins_bronze", "labels_payments_bronze", "labels_logins_bronze", "cdc_cards_bronze",
    "payments_silver", "logins_silver", "quarantine", "cards_history", "labels_payments_history",
    "labels_logins_history", "mcc_fraud_rates", "login_account_stats", "payments_features", "logins_features",
)  # fmt: skip


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", type=Path, required=True)
    parser.add_argument("--engine", choices=["local", "workspace", "fixture"], required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "reconcile.json")
    args = parser.parse_args()
    started = time.monotonic()
    wh = (args.lake / "spark-warehouse").resolve()
    spark = session.get(shuffle_partitions=32)
    tables = {}
    for name in TABLES:
        if (wh / name).exists():
            tables[name] = spark.read.parquet(str(wh / name))
    summary: dict[str, object] = {"engine": args.engine, "tables": {}}
    for name, df in tables.items():
        summary["tables"][name] = digests.table_digest(df)  # type: ignore[index]
    summary["reconcile"] = digests.reconcile_stats(tables)
    acc = tables["login_account_stats"]
    unknown = (
        acc.where(F.col("unknown_account"))
        .agg(
            F.count(F.lit(1)).alias("accounts"),
            F.sum("attempts").alias("attempts"),
            F.sum("successes").alias("successes"),
        )
        .collect()[0]
    )
    summary["unknown_account"] = {k: int(unknown[k] or 0) for k in ("accounts", "attempts", "successes")}
    summary["users"] = {
        "logins_all": int(acc.count()),
        "logins_known": int(acc.where(~F.col("unknown_account")).count()),
        "payments_accounts": int(tables["payments_silver"].select("account_key").distinct().count()),
    }
    summary["seconds"] = round(time.monotonic() - started, 1)
    existing = json.loads(args.out.read_text(encoding="utf-8")) if args.out.exists() else {}
    existing[args.engine] = summary
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(existing, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(f"summarized {len(tables)} tables in {summary['seconds']} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
