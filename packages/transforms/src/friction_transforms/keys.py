"""Tokens in Spark, matching friction_core.tokens exactly (a test compares them row by row)."""

from __future__ import annotations

from friction_core.tokens import MERCHANT_SALT
from pyspark.sql import Column
from pyspark.sql import functions as F


def token_col(kind: str, value: Column, salt: Column) -> Column:
    material = F.concat(salt, F.lit(f":{kind}:"), value.cast("string"))
    return F.when(value.isNull(), F.lit(None)).otherwise(
        F.concat(F.lit(kind[0]), F.substring(F.sha2(material, 256), 1, 19))
    )


def merchant_key_sql(value: Column) -> Column:
    return token_col("merchant", value, F.lit(MERCHANT_SALT))
