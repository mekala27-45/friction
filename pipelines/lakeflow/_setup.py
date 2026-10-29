"""Shared setup for the Lakeflow pipeline files: import paths, names and configuration.

The bundle syncs the repository to the workspace and passes its path as spark.friction.source_root, so
the pipeline imports the same packages the tests import. The state machine module is pickled by
value into the UDF, so the workers need nothing installed.
"""

import sys

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
ROOT = spark.conf.get("spark.friction.source_root")
for _p in ("packages/core/src", "packages/transforms/src", "packages/features/src"):
    if f"{ROOT}/{_p}" not in sys.path:
        sys.path.insert(0, f"{ROOT}/{_p}")

import friction_features
import yaml
from friction_core.shipping import ship

ship(friction_features)

CATALOG = spark.conf.get("spark.friction.catalog")
PREFIX = spark.conf.get("spark.friction.prefix")
LANDING = spark.conf.get("spark.friction.landing")
# Approved contract changes are a reviewed commit, synced by the bundle, read from the file.
with open(f"{ROOT}/contracts/approved_changes.yaml", encoding="utf-8") as _fh:
    CONTRACT = (yaml.safe_load(_fh) or {}).get("active", {}) or {}


def fq(layer: str, name: str) -> str:
    return f"{CATALOG}.{PREFIX}_{layer}.{name}"
