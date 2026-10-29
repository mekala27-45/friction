"""The model steps on the build machine, over a local lake: attacks, features, train, score.

    python scripts/run_models.py --lake data/lake/fixture --out data/work/models/fixture

The workspace runs the same steps as jobs (jobs/attacks.py, jobs/features.py, jobs/train.py,
jobs/score.py); this exists so the code is exercised before a request spends workspace quota.
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

from friction_models import steps
from friction_transforms import gold, session


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--skip", nargs="*", default=[])
    args = parser.parse_args()
    wh = (args.lake / "spark-warehouse").resolve()
    spark = session.get(shuffle_partitions=16)
    plan = steps.load_plan(ROOT)
    read = lambda name: spark.read.parquet(str(wh / name))  # noqa: E731

    def write(name, df) -> None:  # type: ignore[no-untyped-def]
        df.write.mode("overwrite").parquet(str(wh / name))

    timings = {}
    if "attacks" not in args.skip:
        t = time.monotonic()
        attacks = steps.attacks_step(spark, read, plan)
        write("login_attacks", spark.createDataFrame(attacks))
        timings["attacks"] = round(time.monotonic() - t, 1)
    if "features" not in args.skip:
        t = time.monotonic()
        gold.features_step(spark, read, write, attacks=read("login_attacks"))
        timings["features"] = round(time.monotonic() - t, 1)
    if "train" not in args.skip:
        t = time.monotonic()
        print(json.dumps(steps.train_step(spark, read, plan, args.out / "models"), indent=1))
        timings["train"] = round(time.monotonic() - t, 1)
    if "score" not in args.skip:
        t = time.monotonic()
        print(json.dumps(steps.score_step(spark, read, plan, args.out / "models", args.out / "scored", ROOT), indent=1))
        timings["score"] = round(time.monotonic() - t, 1)
    print(timings)
    return 0


if __name__ == "__main__":
    sys.exit(main())
