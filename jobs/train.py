"""The train task: training sets as of the cut, the floors, logistic, LightGBM and Freeman.

Model files go to the exports volume under models/<tag>/ and, for the default tag, every model of
both desks, the floors included, is logged to MLflow with a signature and registered in Unity
Catalog under the ml schema (friction_models.registry): the learned models with the alias
challenger, the floors with the alias floor. Promotion to champion is a separate, reviewed request
(promote-a-model skill).
"""

from __future__ import annotations

import json
from pathlib import Path

import _boot

args = _boot.parse(
    [("--tag", {"default": "default"}), ("--train-months", {"type": int, "default": 24}),
     ("--label-table", {"default": "labels_payments_history"})]
)  # fmt: skip
spark = _boot.session()

from friction_models import steps  # noqa: E402

LAYER = {"payments_silver": "silver", "logins_silver": "silver", "payments_features": "gold", "logins_features": "gold",
         "login_attacks": "gold", "labels_payments_history": "cdc", "labels_logins_history": "cdc"}  # fmt: skip


def read(name: str):  # type: ignore[no-untyped-def]
    layer = LAYER.get(name, "cdc" if name.startswith("labels_") else "gold")
    return spark.read.table(_boot.fq(args, layer, name))


ledger = _boot.Ledger(spark, args, f"train_{args.tag}")
out = Path(_boot.volume(args, "exports")) / "models"
plan = steps.load_plan(Path(args.root))
try:
    summary = steps.train_step(
        spark, read, plan, out, label_table=args.label_table, train_months=args.train_months, tag=args.tag
    )
    registered: dict[str, object] = {}
    if args.tag == "default":
        from friction_models import registry

        registry.write_floors(out / args.tag)
        experiment = str(Path(args.root).parent / "friction_models").removeprefix("/Workspace")
        tags = {"plan_sha256": steps.plan_hash(Path(args.root)), "job_run_id": str(args.run_id)}
        try:
            registered = dict(registry.register(out / args.tag, args.catalog, f"{args.prefix}_ml", experiment,
                                                Path(args.root), tags))  # fmt: skip
        except Exception as exc:  # recorded, and the promotion gate refuses a model with no registration
            registered["error"] = f"{type(exc).__name__}: {str(exc)[:600]}"
    summary["registered"] = registered
    (out / args.tag / "summary.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    ledger.write(inputs=summary, outputs=registered, rows_in=summary.get("payments", {}).get("rows", 0),
                 rows_out=len(registered), status="succeeded")  # fmt: skip
    print(json.dumps(summary, indent=1, default=str))
except Exception as exc:
    ledger.write(inputs=[], outputs=[], rows_in=0, rows_out=0, status="failed", detail=str(exc))
    raise
