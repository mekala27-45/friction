"""The decisions task: payments bronze as a stream, each event decided by the champion.

A streaming read of the bronze table (append only, every drop as it lands) joined to the live silver
events and their feature vectors as a stream to static join, scored inside mapInPandas, and written
to an append only log with an available now trigger and a checkpoint: one run reads what landed
since the last run and stops, and the Delta sink writes each micro batch once. The run's log rows
are then merged into gold.payments_decisions on txn_ref, so a delivery seen twice is decided once,
and a catch up pass decides any live event that has a feature vector and no decision. The model
file comes from the Unity Catalog version the champion alias names and must hash to the served
bundle's file. A summary, the parity of the decisions against the parity events, and a second catch
up count (zero when every live event is decided) go to the exports volume under the tag.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import _boot

args = _boot.parse(
    [("--tag", {"default": "decisions"}), ("--live-from", {"default": "2019-07-01"}),
     ("--reset", {"default": "false"})]
)  # fmt: skip
spark = _boot.session()

import friction_core  # noqa: E402
import friction_features  # noqa: E402
import friction_models  # noqa: E402
import friction_policy  # noqa: E402
import friction_reasons  # noqa: E402
from friction_core.shipping import ship  # noqa: E402
from friction_models import stream  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

ship(friction_core, friction_features, friction_models, friction_policy, friction_reasons)

root = Path(args.root)
target = _boot.fq(args, "gold", "payments_decisions")
log = _boot.fq(args, "gold", "payments_decisions_log")
model_name = _boot.fq(args, "ml", "payments_lightgbm")
checkpoint = f"{_boot.volume(args, 'exports')}/checkpoints/payments_decisions"
folder = f"{_boot.volume(args, 'exports')}/{args.tag}"
ledger = _boot.Ledger(spark, args, "decisions")


def champion() -> tuple[str, str, str]:
    """The champion's version, its model file and the file's sha256, checked against the bundle."""
    import mlflow

    mlflow.set_registry_uri("databricks-uc")
    mv = mlflow.MlflowClient().get_model_version_by_alias(model_name, "champion")
    local = Path(mlflow.artifacts.download_artifacts(f"models:/{model_name}@champion"))
    files = [p for p in (local / "artifacts").iterdir() if p.is_file()]
    if len(files) != 1:
        raise RuntimeError(f"the champion holds {len(files)} artifact files")
    text = files[0].read_text(encoding="utf-8")
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    served = stream.load_bundle(root)["files"]["model.txt"]
    if sha != served or sha != mv.tags.get("sha256"):
        raise RuntimeError(f"the champion file {sha[:12]} is not the served file {served[:12]}")
    return str(mv.version), text, sha


def merge(df) -> int:  # type: ignore[no-untyped-def]
    """Insert decisions not yet in the table; a new champion version replaces older decisions."""
    if not spark.catalog.tableExists(target):
        df.limit(0).write.saveAsTable(target)
        spark.sql(f"ALTER TABLE {target} CLUSTER BY (event_date)")
        spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    df.dropDuplicates(["txn_ref"]).createOrReplaceTempView("incoming")
    before = spark.read.table(target).count()
    spark.sql(f"""
        MERGE INTO {target} t USING incoming s ON t.txn_ref = s.txn_ref
        WHEN MATCHED AND t.model_sha256 <> s.model_sha256 THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return int(spark.read.table(target).count() - before)


try:
    bundle = stream.load_bundle(root)
    version, text, sha = champion()
    if args.reset == "true":
        spark.sql(f"DROP TABLE IF EXISTS {target}")
        spark.sql(f"DROP TABLE IF EXISTS {log}")
        import shutil

        shutil.rmtree(checkpoint, ignore_errors=True)
    codes = spark.createDataFrame(stream.reason_rows(root), "desk string, code string, says string, "
                                  "features string, source string")  # fmt: skip
    codes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(_boot.fq(args, "gold", "reason_codes"))
    silver = spark.read.table(_boot.fq(args, "silver", "payments_silver"))
    features = spark.read.table(_boot.fq(args, "gold", "payments_features"))
    # Bronze is append only; a commit that changed rows would be skipped, never read as new events.
    reader = spark.readStream.option("maxFilesPerTrigger", 200).option("skipChangeCommits", "true")
    bronze = reader.table(_boot.fq(args, "bronze", "payments_bronze"))
    scored = stream.score(stream.batch_rows(bronze, silver, features, args.live_from), text, bundle, version, sha)
    query = (
        scored.withColumn("run_id", F.lit(str(args.run_id))).writeStream.format("delta")
        .option("checkpointLocation", checkpoint).trigger(availableNow=True).toTable(log)
    )  # fmt: skip
    query.awaitTermination()
    batches = len(query.recentProgress)
    logged = spark.read.table(log).where(F.col("run_id") == str(args.run_id)).drop("run_id")
    counts = {"batches": batches, "logged": logged.count(), "streamed": merge(logged)}
    decisions = spark.read.table(target) if spark.catalog.tableExists(target) else None
    caught = merge(stream.score(stream.pending(silver, features, decisions, args.live_from), text, bundle, version,
                                sha))  # fmt: skip
    # A second catch up must find nothing: every live event is decided exactly once.
    again = stream.pending(silver, features, spark.read.table(target), args.live_from).count()
    table = spark.read.table(target)
    parity_doc = json.loads((root / "results" / "parity" / "payments.json").read_text(encoding="utf-8"))
    expected = {int(e["event_ref"]): e for e in parity_doc["expected"]}
    got = {int(r["txn_ref"]): r for r in table.where(table.txn_ref.isin(list(expected))).collect()}
    differ = [k for k, e in expected.items() if k in got and (got[k]["verdict"] != e["verdict"]
              or abs(got[k]["score"] - e["score"]) > 1e-9 * max(abs(e["score"]), 1e-12))]  # fmt: skip
    codes = [k for k, e in expected.items() if k in got and got[k]["codes"] != ",".join(e.get("codes", []))]
    record = {
        "model_version": version, "model_sha256": sha, "live_from": args.live_from, **counts, "caught_up": caught,
        "pending_after": int(again), **stream.summary(table),
        "parity": {"events": len(expected), "decided": len(got), "differ": len(differ), "codes_differ": len(codes)},
    }  # fmt: skip
    os.makedirs(folder, exist_ok=True)
    with open(f"{folder}/decisions.json", "w", encoding="utf-8") as fh:
        json.dump(record, fh, indent=1, default=str)
    ledger.write(inputs={"version": version}, outputs=record, rows_in=counts["streamed"] + caught,
                 rows_out=record["decisions"], status="succeeded")  # fmt: skip
    print(json.dumps(record, indent=1, default=str))
    if again or differ:
        raise RuntimeError(f"{again} live events undecided, {len(differ)} parity decisions differ")
except Exception as exc:
    ledger.write(inputs=[], outputs=[], rows_in=0, rows_out=0, status="failed", detail=str(exc))
    raise
