"""Export each desk's served model and the parity replay set, after grading.

    python scripts/export_models.py --lake data/lake/full --work data/work/models/full

Reads results/evaluation.json for the champion and its thresholds, the model files and scored sets
under --work, and the lake's tables for the parity window and its state snapshot. Writes
results/models/<desk>/ and results/parity/<desk>.json.
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

from friction_models import export, steps
from friction_reasons.reasons import feature_to_code, load_table
from friction_transforms import session


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--evaluation", type=Path, default=ROOT / "results" / "evaluation.json")
    parser.add_argument("--out", type=Path, default=ROOT / "results")
    args = parser.parse_args()
    started = time.monotonic()
    wh = (args.lake / "spark-warehouse").resolve()
    spark = session.get(shuffle_partitions=16)
    read = lambda name: spark.read.parquet(str(wh / name))  # noqa: E731
    table = load_table(ROOT / "evaluation" / "reason_codes.yaml")
    maps = {d: feature_to_code(table, d) for d in ("payments", "logins")}
    evaluation = json.loads(args.evaluation.read_text(encoding="utf-8"))
    summary = export.export_step(
        read, steps.load_plan(ROOT), evaluation, args.work / "models" / "default", args.work / "scored", args.out, maps
    )
    print(json.dumps(summary, indent=1), f"in {round(time.monotonic() - started, 1)} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
