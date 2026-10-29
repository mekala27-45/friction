"""The features task: the state machines over the pipeline's materialized inputs, into gold.

The same friction_transforms.gold.features_step the build machine runs over Parquet. Each table
is replaced whole, so rerunning the task over the same inputs leaves it identical.
"""

from __future__ import annotations

import _boot

args = _boot.parse()
spark = _boot.session()

import friction_features  # noqa: E402
from friction_transforms import gold  # noqa: E402
from pyspark import cloudpickle  # noqa: E402

cloudpickle.register_pickle_by_value(friction_features)
ledger = _boot.Ledger(spark, args, "features")


def read(name: str):  # type: ignore[no-untyped-def]
    return spark.read.table(_boot.fq(args, "gold", name))


def write(name: str, df) -> None:  # type: ignore[no-untyped-def]
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(_boot.fq(args, "gold", name))


attacks_table = _boot.fq(args, "gold", "login_attacks")
attacks = spark.read.table(attacks_table) if spark.catalog.tableExists(attacks_table) else None
try:
    counts = gold.features_step(spark, read, write, attacks=attacks)
    rows_in = read("payment_feature_inputs").count() + read("login_feature_inputs").count()
    ledger.write(inputs=["payment_feature_inputs", "login_feature_inputs"], outputs=counts, rows_in=rows_in,
                 rows_out=sum(counts.values()), status="succeeded")  # fmt: skip
    print(counts)
except Exception as exc:
    ledger.write(inputs=[], outputs=[], rows_in=0, rows_out=0, status="failed", detail=str(exc))
    raise
