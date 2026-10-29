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


def _local_headers(path: str) -> dict[str, list[str]] | None:
    """drop/file -> header columns for every file under a local path, or None for a remote one."""
    import csv
    import gzip
    from pathlib import Path

    root = Path(path.removeprefix("file:"))
    if not root.is_dir():
        return None
    out: dict[str, list[str]] = {}
    for f in sorted(root.rglob("*.csv.gz")):
        with gzip.open(f, "rt", encoding="utf-8", newline="") as fh:
            out[f"{f.parent.name}/{f.name}"] = next(csv.reader(fh), [])
    return out


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
    local = _local_headers(path)
    if local is not None:
        # The header of each file is its first line, read on the driver without decompressing the
        # rest and carried as a literal map, so the rows are read once and never shuffled or joined.
        # A literal, not a DataFrame: a pipeline query function may not trigger analysis.
        rel = F.regexp_extract(F.col("source_file"), r"([^/]+/[^/]+)$", 1)
        pairs = [x for k, v in local.items() for x in (F.lit(k), F.array(*[F.lit(c) for c in v]))]
        lookup = F.create_map(*pairs) if pairs else F.create_map()
        data = rows.where(F.col("_c0") != first).withColumn("_header", F.try_element_at(lookup, rel))
    else:
        headers = rows.where(F.col("_c0") == first).select(
            "source_file", F.filter("_values", lambda x: x.isNotNull()).alias("_header")
        )
        data = rows.where(F.col("_c0") != first).join(F.broadcast(headers), "source_file", "left")
    # Pairs of (name, value) for every header column the contract does not expect.
    zipped = F.arrays_zip(F.col("_header").alias("k"), F.slice("_values", 1, F.size("_header")).alias("v"))
    extra = F.filter(zipped, lambda p: ~p["k"].isin(*expected))
    rescued = F.when(
        F.size(extra) > 0,
        F.to_json(F.map_from_entries(F.transform(extra, lambda p: F.struct(p["k"], p["v"])))),
    )
    # array_position gives 0 when the file has no such column, and element_at(.., 0) is an error even
    # in its try_ form, so the lookup is guarded.
    named = []
    for c in expected:
        pos = F.array_position("_header", c).cast("int")
        named.append(F.when(pos > 0, F.element_at("_values", pos)).alias(c))
    return data.select(*named, rescued.alias("_rescued_data"), "source_file", "file_modified_at")
