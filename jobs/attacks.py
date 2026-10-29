"""The attacks task: synthetic attacker logins for held out users (test) and others (training)."""

from __future__ import annotations

from pathlib import Path

import _boot

args = _boot.parse()
spark = _boot.session()

from friction_models import steps  # noqa: E402

ledger = _boot.Ledger(spark, args, "attacks")
plan = steps.load_plan(Path(args.root))
try:
    frame = steps.attacks_step(spark, lambda n: spark.read.table(_boot.fq(args, "silver", n)), plan)
    df = spark.createDataFrame(frame)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(_boot.fq(args, "gold", "login_attacks"))
    counts = frame.groupby(["purpose", "attacker_model"]).size().to_dict()
    ledger.write(inputs=["logins_silver"], outputs={str(k): int(v) for k, v in counts.items()}, rows_in=0,
                 rows_out=len(frame), status="succeeded")  # fmt: skip
    print(counts)
except Exception as exc:
    ledger.write(inputs=[], outputs=[], rows_in=0, rows_out=0, status="failed", detail=str(exc))
    raise
