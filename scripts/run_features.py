"""The features step on the build machine: the state machines over the portable pipeline's inputs.

python scripts/run_features.py --lake data/lake/fixture
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from friction_transforms import gold, session


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", type=Path, required=True)
    parser.add_argument("--partitions", type=int, default=16)
    args = parser.parse_args()
    wh = (args.lake / "spark-warehouse").resolve()
    spark = session.get(shuffle_partitions=args.partitions)
    started = time.monotonic()

    def read(name: str):  # type: ignore[no-untyped-def]
        return spark.read.parquet(str(wh / name))

    def write(name: str, df) -> None:  # type: ignore[no-untyped-def]
        df.write.mode("overwrite").parquet(str(wh / name))

    counts = gold.features_step(spark, read, write)
    print(f"features: {counts} in {round(time.monotonic() - started, 1)} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
