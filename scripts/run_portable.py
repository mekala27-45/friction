"""Run the portable pipeline with open source Spark Declarative Pipelines.

    python scripts/run_portable.py --landing data/fixtures/landing --out data/lake/fixture
    python scripts/run_portable.py --landing data/landing --out data/lake/full

Writes a concrete spark-pipeline.yml beside the output (the CLI reads no environment variables),
runs ``spark-pipelines run --full-refresh-all`` through a local Spark Connect server, and leaves
every table as Parquet under <out>/spark-warehouse/<table>/ (the run's working directory is <out>).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import yaml
from friction_core.tokens import TEST_SALT

CONTRACT = ROOT / "contracts" / "approved_changes.yaml"


def contract_json(path: Path = CONTRACT) -> str:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
    return json.dumps(data.get("active", {}) or {})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--landing", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--memory", default="4g")
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--cores", type=int, default=os.cpu_count() or 2)
    parser.add_argument("--contract", type=Path, default=CONTRACT, help="approved changes to apply")
    args = parser.parse_args()
    out = args.out.resolve()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    spec = {
        "name": "friction_portable",
        "storage": (out / "storage").as_uri(),
        # The CLI takes only globs relative to the spec, so the definitions are linked in beside it.
        "libraries": [{"glob": {"include": "transformations/**"}}],
        "configuration": {
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.shuffle.partitions": str(args.partitions),
            "spark.sql.ansi.enabled": "true",
        },
    }
    (out / "transformations").symlink_to(ROOT / "pipelines" / "portable" / "transformations", target_is_directory=True)
    spec_path = out / "spark-pipeline.yml"
    spec_path.write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")
    venv_bin = Path(sys.executable).parent
    env = {
        **os.environ,
        "PATH": f"{venv_bin}:{os.environ.get('PATH', '')}",
        "PYSPARK_PYTHON": sys.executable,
        "PYSPARK_DRIVER_PYTHON": sys.executable,
        "SPARK_DRIVER_MEMORY": args.memory,
        "PYTHONPATH": str(ROOT),
        "TZ": "UTC",
        "FRICTION_LANDING": str(args.landing.resolve()),
        "FRICTION_SALT": os.environ.get("FRICTION_SALT", TEST_SALT),
        "FRICTION_CONTRACT": contract_json(args.contract),
    }
    started = time.monotonic()
    proc = subprocess.run(
        # The CLI's own default is --remote local, one thread; --cores names more. --cores 0 keeps the default.
        [
            str(venv_bin / "spark-pipelines"),
            *(["--remote", f"local[{args.cores}]"] if args.cores > 0 else []),
            "run",
            "--spec",
            str(spec_path),
            "--full-refresh-all",
        ],
        cwd=out,
        env=env,
        capture_output=True,
        text=True,
    )
    (out / "run.log").write_text(proc.stdout + proc.stderr, encoding="utf-8")
    seconds = round(time.monotonic() - started, 1)
    print(f"portable pipeline exited {proc.returncode} after {seconds} s; log in {out / 'run.log'}")
    return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
