"""One way to get a Spark session, local or in the workspace.

Locally the session is a Spark Connect client to a server that pyspark starts on this machine
(``remote("local[n]")``), so the code meets the same rules it meets on serverless: names resolve
at execution, RDD APIs are refused, and ``CACHE TABLE`` and ``checkpoint()`` are not used. In the
workspace the active session is used as it is.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any


def get(cores: int | None = None, memory: str = "4g", shuffle_partitions: int = 8) -> Any:
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return SparkSession.builder.getOrCreate()
    # Every time in the build is UTC. Spark Connect hands timestamps back in the client's zone, so
    # the client process runs in UTC too, whatever the machine is set to.
    os.environ["TZ"] = "UTC"
    time.tzset()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_DRIVER_MEMORY", memory)
    n = cores or int(os.environ.get("FRICTION_SPARK_CORES", os.cpu_count() or 2))
    spark = (
        SparkSession.builder.remote(f"local[{n}]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark
