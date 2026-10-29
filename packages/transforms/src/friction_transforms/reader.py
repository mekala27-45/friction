"""Reading drops by header name on any engine, with unexpected columns rescued.

Auto Loader in the workspace matches CSV headers to the schema by name and puts anything else in
``_rescued_data``. Open source Spark has no rescued data column, and a CSV read with a header maps
columns by position, so a renamed column would land silently in the wrong place. This reader gives
open source Spark the same behaviour: it reads every line without a header, finds each file's
header row, and looks each expected column up by name in that file's own header. Columns the
contract does not know go into ``_rescued_data`` as JSON, the way Auto Loader writes them.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from friction_transforms.bronze import TEXT_COLUMNS

WIDTH = 24  # wide enough for every drop, with room for columns nobody expected


def read_drops(spark: SparkSession, path: str, feed: str) -> DataFrame:
    expected = TEXT_COLUMNS[feed]
    schema = ", ".join(f"_c{i} STRING" for i in range(WIDTH))
    raw = (
        spark.read.format("csv")
        .option("header", "false")
        .option("multiLine", "false")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.csv.gz")
        .schema(schema)
        .load(path)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("file_modified_at", F.col("_metadata.file_modification_time"))
    )
    values = F.array(*[F.col(f"_c{i}") for i in range(WIDTH)])
    rows = raw.withColumn("_values", values)
    first = expected[0]
    headers = rows.where(F.col("_c0") == first).select(
        "source_file", F.filter("_values", lambda x: x.isNotNull()).alias("_header")
    )
    data = rows.where(F.col("_c0") != first).join(headers, "source_file", "left")
    # Pairs of (name, value) for every header column the contract does not expect.
    zipped = F.arrays_zip(F.col("_header").alias("k"), F.slice("_values", 1, F.size("_header")).alias("v"))
    extra = F.filter(zipped, lambda p: ~p["k"].isin(*expected))
    rescued = F.when(
        F.size(extra) > 0,
        F.to_json(F.map_from_entries(F.transform(extra, lambda p: F.struct(p["k"], p["v"])))),
    )
    # array_position returns 0 when the file has no such column; try_element_at gives null for it.
    named = [F.try_element_at("_values", F.array_position("_header", c).cast("int")).alias(c) for c in expected]
    return data.select(*named, rescued.alias("_rescued_data"), "source_file", "file_modified_at")
