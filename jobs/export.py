"""Export what the repository grades and reconciles: digests, reconcile statistics, quarantine.

Writes JSON into the exports volume under the run's folder; the workspace workflow copies the
folder to the results branch. Nothing here grades anything.
"""

from __future__ import annotations

import json
import os

import _boot

args = _boot.parse([("--tag", {"default": "export"}), ("--tables", {"default": "all"})])
spark = _boot.session()

from friction_transforms import digests, integration  # noqa: E402

ledger = _boot.Ledger(spark, args, "export")
names = integration.EXPORTED
tables = {k: spark.read.table(_boot.fq(args, layer, t)) for k, (layer, t) in names.items()}
out = {
    "tag": args.tag,
    "digests": {k: digests.table_digest(df) for k, df in tables.items()},
    "reconcile": digests.reconcile_stats(tables),
}
folder = f"{_boot.volume(args, 'exports')}/{args.tag}"
os.makedirs(folder, exist_ok=True)
with open(f"{folder}/export.json", "w", encoding="utf-8") as fh:
    json.dump(out, fh, indent=1, default=str)
ledger.write(inputs=list(names), outputs=out["digests"], rows_in=0, rows_out=len(names), status="succeeded")
print(json.dumps(out["digests"], indent=1))
