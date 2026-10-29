"""Reading drops by header name on any engine, with unexpected columns rescued.

Auto Loader in the workspace matches CSV headers to the schema by name and puts anything else in
``_rescued_data``. Open source Spark has no rescued data column, and a CSV read with a header maps
columns by position, so a renamed column would land silently in the wrong place. This reader gives
open source Spark the same behaviour: it reads every line without a header, finds each file's
header row, and looks each expected column up by name in that file's own header. Columns the
contract does not know go into ``_rescued_data`` as JSON, the way Auto Loader writes them.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from friction_transforms.bronze import TEXT_COLUMNS

WIDTH = 24  # wide enough for every drop, with room for columns nobody expected


def _local_headers(path: str) -> dict[str, list[str]] | None:
    """drop/file -> header columns for every file under a local path, or None for a remote one."""
    import csv
    import gzip

    root = Path(path.removeprefix("file:"))
    if not root.is_dir():
        return None
    out: dict[str, list[str]] = {}
    for f in sorted(root.rglob("*.csv.gz")):
        with gzip.open(f, "rt", encoding="utf-8", newline="") as fh:
            out[f"{f.parent.name}/{f.name}"] = next(csv.reader(fh), [])
    return out


def _read(spark: SparkSession, paths: str | list[str]) -> DataFrame:
    schema = ", ".join(f"_c{i} STRING" for i in range(WIDTH))
    reader = (
        spark.read.format("csv")
        .option("header", "false")
        .option("multiLine", "false")
        .option("pathGlobFilter", "*.csv.gz")
        .schema(schema)
    )
    if isinstance(paths, str):
        reader = reader.option("recursiveFileLookup", "true")
    raw = reader.load(paths)
    return raw.withColumn("source_file", F.col("_metadata.file_path")).withColumn(
        "file_modified_at", F.col("_metadata.file_modification_time")
    )


def _named(raw: DataFrame, header: list[str], expected: tuple[str, ...]) -> DataFrame:
    """One group of files that share a header: columns by name, the rest into rescued data."""
    first = expected[0]
    data = raw.where(F.col("_c0") != first)
    pos = {name: i for i, name in enumerate(header) if i < WIDTH}
    named = [(F.col(f"_c{pos[c]}") if c in pos else F.lit(None).cast("string")).alias(c) for c in expected]
    extra = [(name, i) for name, i in pos.items() if name not in expected]
    if extra:
        rescued = F.to_json(F.create_map(*[x for name, i in extra for x in (F.lit(name), F.col(f"_c{i}"))]))
    else:
        rescued = F.lit(None).cast("string")
    return data.select(*named, rescued.alias("_rescued_data"), "source_file", "file_modified_at")


def read_drops(spark: SparkSession, path: str, feed: str) -> DataFrame:
    expected = TEXT_COLUMNS[feed]
    local = _local_headers(path)
    if local is not None:
        # Each file's header is its first line, read on the driver without decompressing the rest.
        # Files that share a header are read together and mapped by name with plain column
        # references, so every row is read once, with no join and no lookup per row.
        root = Path(path.removeprefix("file:"))
        groups: dict[tuple[str, ...], list[str]] = {}
        for rel, header in sorted(local.items()):
            groups.setdefault(tuple(header), []).append(str(root / rel))
        parts = [_named(_read(spark, files), list(h), expected) for h, files in sorted(groups.items())]
        out = parts[0]
        for part in parts[1:]:
            out = out.unionByName(part)
        return out
    raw = _read(spark, path)
    values = F.array(*[F.col(f"_c{i}") for i in range(WIDTH)])
    rows = raw.withColumn("_values", values)
    first = expected[0]
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
