"""Bronze in the workspace: Auto Loader streaming tables over the landing volume.

Each feed's drops are read incrementally, only new files, with a checkpoint per table. The schema
is the contract's text schema and Auto Loader's rescued data column catches anything else, which
is how the day 40 renamed column shows up. Restricted columns are masked by the governance job.
"""

from _setup import LANDING, fq, spark
from friction_transforms import bronze
from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _bronze(feed: str) -> None:
    @dp.table(name=fq("bronze", f"{feed}_bronze"), comment=f"Every {feed} drop as it landed, text, with its file")
    def _table():  # type: ignore[no-untyped-def]
        raw = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("rescuedDataColumn", "_rescued_data")
            .option("cloudFiles.includeExistingFiles", "true")
            .schema(bronze.text_schema(feed))
            .load(f"{LANDING}/{feed}/drops/")
            .withColumn("source_file", F.col("_metadata.file_path"))
            .withColumn("file_modified_at", F.col("_metadata.file_modification_time"))
        )
        return bronze.select(feed, raw)


for _feed in bronze.TEXT_COLUMNS:
    _bronze(_feed)
