"""Shared start of every workspace job: import paths, the session, names, and the run ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import UTC, datetime
from typing import Any


def parse(extra: list[tuple[str, dict[str, Any]]] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True, help="the bundle's synced files, ${workspace.file_path}")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--prefix", required=True)
    p.add_argument("--job", default="")
    p.add_argument("--run-id", default="")
    for name, kw in extra or []:
        p.add_argument(name, **kw)
    args, _ = p.parse_known_args()
    for sub in (
        "packages/core/src",
        "packages/transforms/src",
        "packages/features/src",
        "packages/models/src",
        "packages/evaluation/src",
        "packages/policy/src",
        "packages/reasons/src",
        "packages/simulator/src",
    ):
        path = f"{args.root}/{sub}"
        if path not in sys.path:
            sys.path.insert(0, path)
    return args


def session() -> Any:
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def fq(args: argparse.Namespace, layer: str, name: str) -> str:
    return f"{args.catalog}.{args.prefix}_{layer}.{name}"


def volume(args: argparse.Namespace, name: str) -> str:
    return f"/Volumes/{args.catalog}/{args.prefix}_raw/{name}"


LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  job STRING, task STRING, run_id STRING, inputs_digest STRING, outputs_digest STRING,
  rows_in BIGINT, rows_out BIGINT, compute_seconds DOUBLE, status STRING, detail STRING, recorded_at TIMESTAMP
) CLUSTER BY (job, task)
"""


class Ledger:
    """One row per task per run in ops.run_ledger, written when the task ends, success or not."""

    def __init__(self, spark: Any, args: argparse.Namespace, task: str) -> None:
        self.spark, self.args, self.task = spark, args, task
        self.table = fq(args, "ops", "run_ledger")
        self.started = time.monotonic()
        spark.sql(LEDGER_DDL.format(table=self.table))

    def write(self, *, inputs: Any, outputs: Any, rows_in: int, rows_out: int, status: str, detail: Any = None) -> None:
        digest = lambda v: hashlib.sha256(json.dumps(v, sort_keys=True, default=str).encode()).hexdigest()[:16]  # noqa: E731
        row = [(self.args.job, self.task, str(self.args.run_id), digest(inputs), digest(outputs), int(rows_in),
                int(rows_out), round(time.monotonic() - self.started, 2), status,
                json.dumps(detail, default=str)[:4000] if detail is not None else None, datetime.now(UTC))]  # fmt: skip
        schema = (
            "job string, task string, run_id string, inputs_digest string, outputs_digest string, "
            "rows_in bigint, rows_out bigint, compute_seconds double, status string, detail string, "
            "recorded_at timestamp"
        )
        self.spark.createDataFrame(row, schema).write.mode("append").saveAsTable(self.table)
