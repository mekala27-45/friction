"""The day 40 incident on the build machine: the drops around it, before and after the fix.

    python scripts/replay_incident.py

Runs the portable pipeline twice over the payments drops around replay day 40 (the other feeds come
from the fixture slice, which is enough for the pipeline to run): once with the contract as it was,
once with the approved change (the renamed column read back out of rescued data, the clock shifted
part rejected) and the corrected redelivery landed. Reports, for the incident drop, the quarantine
by rule, completeness, the clock check, and the silver totals of the rows that arrived in the drop
against the corrected source, and the digest of every row from every other drop, which must not
move. Writes results/incident.json.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import yaml
from friction_transforms import digests, monitor, session
from pyspark.sql import functions as F

DROP = "0387_2020-01-07"
DAY = "2020-01-07"
AROUND = range(380, 395)


def landing(base: Path, src: Path, fixture: Path, fixed: bool) -> Path:
    out = base / "landing"
    if out.exists():
        shutil.rmtree(out)
    for feed in ("logins", "labels_payments", "labels_logins", "cdc_cards"):
        shutil.copytree(fixture / feed, out / feed, symlinks=True)
    drops = out / "payments" / "drops"
    drops.mkdir(parents=True)
    for d in sorted((src / "payments" / "drops").iterdir()):
        if int(d.name[:4]) not in AROUND:
            continue
        target = drops / d.name
        target.mkdir()
        for f in sorted(d.iterdir()):
            (target / f.name).symlink_to(f.resolve())
        if fixed and d.name == DROP:
            for f in sorted((src / "payments" / "redelivery" / DROP).iterdir()):
                (target / f.name).symlink_to(f.resolve())
    return out


def run(landing_dir: Path, out: Path, contract: Path) -> tuple[float, str]:
    started = time.monotonic()
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "run_portable.py"), "--landing", str(landing_dir), "--out", str(out),
         "--contract", str(contract), "--memory", "2g", "--partitions", "8"],
        capture_output=True, text=True, env={**os.environ, "FRICTION_ALLOW_TEST_SALT": "1"},
    )  # fmt: skip
    if proc.returncode != 0:
        raise RuntimeError(proc.stdout[-2000:] + proc.stderr[-2000:])
    return round(time.monotonic() - started, 1), datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def truth(src: Path) -> dict[str, float]:
    f = next((src / "payments" / "redelivery" / DROP).iterdir())
    df = pd.read_csv(f, dtype=str)
    cents = df["amount"].str.replace(r"[$,]", "", regex=True).astype(float).mul(100).round().astype("int64")
    return {"rows": len(df), "amount_cents": int(cents.sum())}


def summarize(spark: Any, out: Path) -> dict[str, Any]:
    wh = out / "spark-warehouse"
    read = lambda n: spark.read.parquet(str(wh / n))  # noqa: E731
    silver, bronze, quarantine = read("payments_silver"), read("payments_bronze"), read("quarantine")
    # Rows that arrived in the incident drop (its redelivery included) against everything else. A
    # day's events also arrive late in later drops, so the comparison is by drop, not by date.
    from_drop = F.col("drop_id").startswith(DROP[:5])
    drop = silver.where(from_drop)
    totals = drop.agg(F.count(F.lit(1)).alias("rows"), F.sum("amount_cents").alias("amount_cents")).collect()[0]
    other = silver.where(~from_drop)
    day = silver.where(F.col("event_date") == F.lit(DAY))
    dated = day.agg(F.count(F.lit(1)).alias("rows"), F.sum("amount_cents").alias("amount_cents")).collect()[0]
    q = quarantine.where((F.col("desk") == "payments") & (F.col("drop_id") == DROP))
    return {
        "drop_totals": {"rows": int(totals["rows"]), "amount_cents": int(totals["amount_cents"] or 0)},
        "day_totals": {"rows": int(dated["rows"]), "amount_cents": int(dated["amount_cents"] or 0)},
        "quarantine": {r["rule"]: int(r["count"]) for r in q.groupBy("rule").count().collect()},
        "completeness": [r for r in monitor.completeness(silver, bronze, "txn_ref", "txn_ref") if r["drop_id"] == DROP],
        "clock": [r for r in monitor.clock(quarantine, bronze, "payments", "P_FUTURE") if r["drop_id"] == DROP],
        "digest_other_drops": digests.table_digest(other)["digest"],
        "digest_drop": digests.table_digest(drop)["digest"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work", type=Path, default=ROOT / "data" / "work" / "incident")
    args = parser.parse_args()
    src, fixture = ROOT / "data" / "landing", ROOT / "data" / "fixtures" / "landing"
    approved = yaml.safe_load((ROOT / "contracts" / "approved_changes.yaml").read_text(encoding="utf-8"))
    before_contract = args.work / "contract_before.yaml"
    args.work.mkdir(parents=True, exist_ok=True)
    before = {"active": {"payments": {"renames": {}, "rejected_files": []},
                         "logins": {"renames": {}, "rejected_files": []}}}  # fmt: skip
    before_contract.write_text(yaml.safe_dump(before), encoding="utf-8")
    after_contract = ROOT / "contracts" / "approved_changes.yaml"
    timeline = []
    l_before = landing(args.work / "before", src, fixture, fixed=False)
    secs, at = run(l_before, args.work / "before" / "lake", before_contract)
    timeline.append(
        {"at": at, "what": "pipeline run over the drops around day 40, contract as it was", "seconds": secs}
    )
    l_after = landing(args.work / "after", src, fixture, fixed=True)
    secs, at = run(l_after, args.work / "after" / "lake", after_contract)
    timeline.append({"at": at, "what": "replay of the same drops with the approved change and the redelivery",
                     "seconds": secs})  # fmt: skip
    spark = session.get(shuffle_partitions=8)
    result = {
        "drop": DROP,
        "day": DAY,
        "truth": truth(src),
        "before": summarize(spark, args.work / "before" / "lake"),
        "after": summarize(spark, args.work / "after" / "lake"),
        "approved_change": approved["active"]["payments"],
        "timeline": timeline,
    }
    result["other_drops_unchanged"] = result["before"]["digest_other_drops"] == result["after"]["digest_other_drops"]
    result["drop_matches_truth"] = result["after"]["drop_totals"] == result["truth"]
    (ROOT / "results" / "incident.json").write_text(
        json.dumps(result, indent=1, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({k: result[k] for k in ("truth", "other_drops_unchanged", "drop_matches_truth")}, indent=1))
    print(json.dumps(result["before"]["drop_totals"]), json.dumps(result["after"]["drop_totals"]))
    return 0 if result["drop_matches_truth"] and result["other_drops_unchanged"] else 1


if __name__ == "__main__":
    sys.exit(main())
