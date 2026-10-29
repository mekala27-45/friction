"""The train task: training sets as of the cut, the floors, logistic, LightGBM and Freeman.

Model files go to the exports volume under models/<tag>/ and, for the default tag, each learned
model is logged to MLflow and registered in Unity Catalog under the ml schema with the alias
challenger. Promotion to champion is a separate, reviewed request (promote-a-model skill).
"""

from __future__ import annotations

import contextlib
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
    registered = {}
    if args.tag == "default":
        try:
            import mlflow

            mlflow.set_registry_uri("databricks-uc")
            client = mlflow.MlflowClient()
            for name in (
                "payments_lightgbm.txt",
                "payments_logistic.json",
                "logins_freeman.json",
                "logins_lightgbm.txt",
            ):
                model_name = f"{args.catalog}.{args.prefix}_ml.{name.split('.')[0]}"
                with mlflow.start_run(run_name=f"friction-{name}") as run:
                    mlflow.log_artifact(str(out / args.tag / name), artifact_path="model")
                    mlflow.log_dict(summary, "summary.json")
                    mlflow.log_dict(plan, "plan.json")
                with contextlib.suppress(Exception):  # already registered
                    client.create_registered_model(model_name)
                mv = client.create_model_version(model_name, f"runs:/{run.info.run_id}/model", run.info.run_id)
                client.set_registered_model_alias(model_name, "challenger", mv.version)
                registered[model_name] = {"version": mv.version, "alias": "challenger"}
        except Exception as exc:  # recorded, and the promotion gate refuses a model with no registration
            registered["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    summary["registered"] = registered
    (out / args.tag / "summary.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    ledger.write(inputs=summary, outputs=registered, rows_in=summary.get("payments", {}).get("rows", 0),
                 rows_out=len(registered), status="succeeded")  # fmt: skip
    print(json.dumps(summary, indent=1, default=str))
except Exception as exc:
    ledger.write(inputs=[], outputs=[], rows_in=0, rows_out=0, status="failed", detail=str(exc))
    raise
