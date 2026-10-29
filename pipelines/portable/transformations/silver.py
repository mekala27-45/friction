"""Silver and the quarantine on open source Spark, from the same functions the workspace uses."""

import json
import os

from friction_transforms import silver
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SALT = F.lit(os.environ["FRICTION_SALT"])
CONTRACT = json.loads(os.environ.get("FRICTION_CONTRACT", "{}"))


def _payments() -> tuple[DataFrame, DataFrame]:
    checked = silver.payments_checked(
        spark.read.table("payments_bronze"),  # noqa: F821
        SALT,
        CONTRACT.get("payments", {}).get("renames", {}),
        tuple(CONTRACT.get("payments", {}).get("rejected_files", [])),
    )
    return silver.payments_split(checked)


def _logins() -> tuple[DataFrame, DataFrame]:
    checked = silver.logins_checked(
        spark.read.table("logins_bronze"),  # noqa: F821
        SALT,
        CONTRACT.get("logins", {}).get("renames", {}),
        tuple(CONTRACT.get("logins", {}).get("rejected_files", [])),
    )
    return silver.logins_split(checked)


@dp.materialized_view(name="payments_silver", comment="Typed, conformed, deduplicated card transactions")
def payments_silver() -> DataFrame:
    return _payments()[0]


@dp.materialized_view(name="logins_silver", comment="Typed, conformed, deduplicated login attempts")
def logins_silver() -> DataFrame:
    return _logins()[0]


@dp.materialized_view(name="quarantine", comment="Every row a rule refused, one row per rule, nothing deleted")
def quarantine() -> DataFrame:
    p = silver.quarantine_rows(_payments()[1], "payments", "txn_ref")
    lg = silver.quarantine_rows(_logins()[1], "logins", "login_ref")
    return p.unionByName(lg)
