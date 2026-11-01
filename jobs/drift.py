"""The drift task: the latest decisions against the first weeks after go live, and a retrain flag.

Reads gold.payments_decisions joined to the feature vectors, computes the indices of
friction_models.drift under config/drift.yaml, appends one row to ops.drift_checks, writes the
record to the exports volume, and sets the task value retrain. The job's condition task runs the
train job only when it is true. --score-bound overrides the registered bound for a drill, and the
record says so.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import _boot

args = _boot.parse(
    [("--tag", {"default": "drift"}), ("--live-from", {"default": "2019-07-01"}), ("--score-bound", {"default": ""})]
)
spark = _boot.session()

import yaml  # noqa: E402
from friction_models import drift  # noqa: E402

root = Path(args.root)
ledger = _boot.Ledger(spark, args, "drift")
try:
    config = yaml.safe_load((root / "config" / "drift.yaml").read_text(encoding="utf-8"))
    drill = bool(args.score_bound)
    if drill:
        config["score_bound"] = float(args.score_bound)
    decisions = spark.read.table(_boot.fq(args, "gold", "payments_decisions"))
    features = spark.read.table(_boot.fq(args, "gold", "payments_features")).select("txn_ref", *config["features"])
    frame = decisions.select("txn_ref", "event_date", "score").join(features, "txn_ref")
    record = {**drift.check(frame, config, args.live_from), "drill": drill,
              "checked_at": datetime.now(UTC).isoformat(timespec="seconds")}  # fmt: skip
    row = [(str(args.run_id), record["checked_at"], float(record["score_psi"]), bool(record["retrain"]), drill,
            json.dumps(record, default=str))]  # fmt: skip
    schema = "run_id string, checked_at string, score_psi double, retrain boolean, drill boolean, record string"
    spark.createDataFrame(row, schema).write.mode("append").saveAsTable(_boot.fq(args, "ops", "drift_checks"))
    folder = f"{_boot.volume(args, 'exports')}/{args.tag}"
    os.makedirs(folder, exist_ok=True)
    with open(f"{folder}/drift.json", "w", encoding="utf-8") as fh:
        json.dump(record, fh, indent=1, default=str)
    from databricks.sdk.runtime import dbutils

    dbutils.jobs.taskValues.set(key="retrain", value="true" if record["retrain"] else "false")
    ledger.write(inputs=record["rule"], outputs=record, rows_in=sum(record["rows"].values()), rows_out=1,
                 status="succeeded")  # fmt: skip
    print(json.dumps(record, indent=1, default=str))
except Exception as exc:
    ledger.write(inputs=[], outputs=[], rows_in=0, rows_out=0, status="failed", detail=str(exc))
    raise
