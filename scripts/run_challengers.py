"""The challengers of evaluation/plan_v2.yaml over a local lake: train, then score.

    python scripts/run_challengers.py --lake data/lake/fixture --out data/work/models/fixture

Run after scripts/run_models.py over the same lake and folder: it reads the graph feature tables the
features step wrote and writes models/v2 and scored_v2 beside the v1 files. The portable request
runs it on the runner over every drop when its params ask for challengers.
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

from friction_models import challengers, steps
from friction_transforms import session

from scripts.check_registration import addendum_hash


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--skip", nargs="*", default=[])
    args = parser.parse_args()
    registered = addendum_hash(ROOT, Path("evaluation/plan_v2.yaml"))
    wh = (args.lake / "spark-warehouse").resolve()
    spark = session.get(shuffle_partitions=16)
    plan = steps.load_plan(ROOT)
    read = lambda name: spark.read.parquet(str(wh / name))  # noqa: E731
    timings: dict[str, float] = {}
    if "train" not in args.skip:
        t = time.monotonic()
        summary = challengers.train_step(spark, read, plan, args.out / "models")
        print(json.dumps({d: {k: v for k, v in e.items() if k != "models"} for d, e in summary.items() if d != "tag"}))
        timings["train_v2"] = round(time.monotonic() - t, 1)
    if "score" not in args.skip:
        t = time.monotonic()
        print(json.dumps(challengers.score_step(spark, read, plan, args.out / "models", args.out / "scored_v2", ROOT),
                         indent=1))  # fmt: skip
        timings["score_v2"] = round(time.monotonic() - t, 1)
    print(f"plan_v2 {registered[:12]}", timings)
    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / "timings.json"
    prior = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    path.write_text(json.dumps({**prior, **timings}, indent=1) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
