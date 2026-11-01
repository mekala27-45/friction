"""The sequence challenger of evaluation/plan_v3.yaml over a local lake: lags, train, score.

    python scripts/run_sequence.py --lake data/lake/fixture --out data/work/models/fixture

Run after scripts/run_models.py over the same lake and folder. Writes the lag table into the lake
once, models/v3 (the network as plain arrays and its training means) and scored_v3 beside the v1
files. Needs the sequence extra (JAX and optax) to fit; scoring is NumPy.
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

from friction_models import sequence, steps
from friction_transforms import session

from scripts.check_registration import addendum_hash


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    registered = addendum_hash(ROOT, Path("evaluation/plan_v3.yaml"))
    wh = (args.lake / "spark-warehouse").resolve()
    spark = session.get(shuffle_partitions=16)
    plan = steps.load_plan(ROOT)
    read = lambda name: spark.read.parquet(str(wh / name))  # noqa: E731
    timings: dict[str, float] = {}
    t = time.monotonic()
    sequence.lagged(read("payments_features"), read("payments_silver")).write.mode("overwrite").parquet(
        str(wh / sequence.LAG_TABLE)
    )
    timings["lags_v3"] = round(time.monotonic() - t, 1)
    t = time.monotonic()
    print(json.dumps(sequence.train_step(read, plan, args.out / "models"), indent=1))
    timings["train_v3"] = round(time.monotonic() - t, 1)
    t = time.monotonic()
    summary = sequence.score_step(spark, read, plan, args.out / "models", args.out / "scored_v3", ROOT)
    print(json.dumps(summary, indent=1))
    timings["score_v3"] = round(time.monotonic() - t, 1)
    print(f"plan_v3 {registered[:12]}", timings)
    path = args.out / "timings.json"
    prior = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    path.write_text(json.dumps({**prior, **timings}, indent=1) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
