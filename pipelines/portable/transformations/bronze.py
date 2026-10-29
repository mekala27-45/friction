"""Bronze on open source Spark: every drop of every feed, read by header name with rescued data."""

import os

from friction_transforms import bronze, reader
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

# The open source CLI applies the spec's configuration when the graph runs, not while the files
# are imported, so scripts/run_portable.py passes these through the environment.
LANDING = os.environ["FRICTION_LANDING"]


def _bronze(feed: str) -> None:
    @dp.materialized_view(name=f"{feed}_bronze", comment=f"Every {feed} drop as it landed, text, with its file")
    def _view() -> DataFrame:
        raw = reader.read_drops(spark, f"{LANDING}/{feed}/drops", feed)  # noqa: F821
        return bronze.select(feed, raw)


for _feed in bronze.TEXT_COLUMNS:
    _bronze(_feed)
