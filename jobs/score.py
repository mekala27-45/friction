"""The evaluate task: score validation and test of both desks with every model, export the sets.

Refuses to run unless evaluation/plan.yaml matches evaluation/plan.lock, so no test month is
scored under a plan that was not registered first.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import _boot

args = _boot.parse([("--tag", {"default": "scored"})])
spark = _boot.session()

import friction_features  # noqa: E402
import friction_models  # noqa: E402
import friction_reasons  # noqa: E402
import yaml  # noqa: E402
from friction_models import steps  # noqa: E402
from pyspark import cloudpickle  # noqa: E402

for module in (friction_features, friction_models, friction_reasons):
    cloudpickle.register_pickle_by_value(module)

root = Path(args.root)
plan_text = (root / "evaluation" / "plan.yaml").read_text(encoding="utf-8")
digest = hashlib.sha256(
    json.dumps(yaml.safe_load(plan_text), sort_keys=True, separators=(",", ":"), default=str).encode()
).hexdigest()
lock = json.loads((root / "evaluation" / "plan.lock").read_text(encoding="utf-8"))
if lock["sha256"] != digest:
    raise SystemExit(f"evaluation/plan.yaml does not match its lock ({digest[:12]} against {lock['sha256'][:12]})")

LAYER = {"payments_silver": "silver", "logins_silver": "silver"}


def read(name: str):  # type: ignore[no-untyped-def]
    layer = LAYER.get(name, "cdc" if name.startswith("labels_") else "gold")
    return spark.read.table(_boot.fq(args, layer, name))


ledger = _boot.Ledger(spark, args, "evaluate")
volume = Path(_boot.volume(args, "exports"))
try:
    summary = steps.score_step(spark, read, steps.load_plan(root), volume / "models", volume / args.tag, root)
    ledger.write(inputs={"plan": digest}, outputs=summary, rows_in=0, rows_out=len(summary), status="succeeded")
    print(json.dumps(summary, indent=1))
except Exception as exc:
    ledger.write(inputs={"plan": digest}, outputs=[], rows_in=0, rows_out=0, status="failed", detail=str(exc))
    raise
