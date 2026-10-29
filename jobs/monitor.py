"""The monitor task in the workspace: the checks of friction_transforms.monitor over Delta tables.

Writes the report and the alert rows to the exports volume (the workspace workflow opens an issue
for each breach from alerts.json) and fails the task when any alert fires.
"""

from __future__ import annotations

import json
from pathlib import Path

import _boot

args = _boot.parse([("--export", {"default": "monitor"}), ("--no-fail", {"action": "store_true"})])
spark = _boot.session()
ledger = _boot.Ledger(spark, args, "monitor")
import yaml  # noqa: E402
from friction_features import logins as login_features  # noqa: E402
from friction_features import payments as payment_features  # noqa: E402
from friction_transforms import monitor  # noqa: E402

root = args.root
cfg = yaml.safe_load(Path(f"{root}/config/monitor.yaml").read_text(encoding="utf-8"))
plan = yaml.safe_load(Path(f"{root}/evaluation/plan.yaml").read_text(encoding="utf-8"))
report: dict[str, object] = {"desks": {}}
scored: set[str] = set()
for desk, key, src_key, names, feats in (
    ("payments", "txn_ref", "txn_ref", list(payment_features.FEATURES), "payments_features"),
    ("logins", "login_ref", "login_index", list(login_features.FEATURES), "logins_features"),
):
    p = plan["desks"][desk]["splits"]
    for split in ("validation", "test"):
        rows = spark.sql(
            f"SELECT date_format(explode(sequence(to_date('{p[split]['start']}'), to_date('{p[split]['end']}') - 1, "
            "interval 1 month)), 'yyyy-MM') AS m"
        ).collect()
        scored |= {r["m"] for r in rows}
    silver = spark.read.table(_boot.fq(args, "silver", f"{desk}_silver"))
    bronze = spark.read.table(_boot.fq(args, "bronze", f"{desk}_bronze"))
    quarantine = spark.read.table(_boot.fq(args, "silver", "quarantine"))
    report["desks"][desk] = {  # type: ignore[index]
        "feature_drift": monitor.feature_drift(
            spark.read.table(_boot.fq(args, "gold", feats)),
            silver,
            key,
            names,
            (p["train"]["start"], p["train"]["end"]),
        ),
        "expectations": monitor.expectation_rates(quarantine, bronze, desk),
        "completeness": monitor.completeness(silver, bronze, key, src_key),
        "clock": monitor.clock(quarantine, bronze, desk, "P_FUTURE" if desk == "payments" else "L_FUTURE"),
    }
report["label_maturity"] = monitor.label_maturity(
    spark.read.table(_boot.fq(args, "cdc", "labels_payments_history")), cfg["label_maturity"]["checkpoints_days"]
)
alerts = monitor.alerts(report, cfg, scored)
base = f"{_boot.volume(args, 'exports')}/{args.export}"
for name, doc in (("report.json", report), ("alerts.json", alerts)):
    spark.createDataFrame([(json.dumps(doc, default=str),)], "doc string").coalesce(1).write.mode("overwrite").text(
        f"{base}/{name}"
    )
status = "failed" if alerts else "succeeded"
ledger.write(
    inputs={"thresholds": cfg}, outputs={"alerts": len(alerts)}, rows_in=0, rows_out=len(alerts), status=status
)
if alerts and not args.no_fail:
    raise SystemExit(f"{len(alerts)} monitor alerts; see {base}/alerts.json and docs/runbooks/ingestion.md")
