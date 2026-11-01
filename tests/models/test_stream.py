"""Streaming decisions: each live event decided once, a rerun adds nothing, verdicts as the API decides."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from friction_models import stream
from friction_models.learners import Gbm
from friction_policy import policy

ROOT = Path(__file__).resolve().parents[2]
LAKE = ROOT / "data" / "lake" / "fixture" / "spark-warehouse"


def _model(n: int = 3000) -> tuple[str, pd.DataFrame]:
    rng = np.random.default_rng(3)
    frame = pd.DataFrame(rng.normal(size=(n, len(stream.FEATURES))), columns=stream.FEATURES)
    frame["channel"] = rng.integers(0, 3, n).astype(float)
    y = (frame["amount_z"] > 1.5).astype(int).to_numpy()
    return Gbm.fit(frame[stream.FEATURES].to_numpy(np.float64), y, stream.FEATURES).to_text(), frame


def test_a_decision_is_the_policy_at_the_served_thresholds_with_the_served_codes() -> None:
    text, frame = _model()
    bundle = stream.load_bundle(ROOT)
    frame["txn_ref"] = np.arange(len(frame))
    frame["account_key"] = "a"
    frame["event_ts"] = pd.Timestamp("2019-07-02")
    frame["amount_cents"] = 100
    out = stream.decide_frame(frame, text, bundle, "1", "sha")
    assert list(out.columns) == list(stream.OUT)
    gbm = Gbm.from_text(text)
    want = policy.payments(gbm.predict(frame[stream.FEATURES].to_numpy(np.float64)), frame["channel"].astype(int)
                           .to_numpy(), policy.Thresholds(**bundle["thresholds"]))  # fmt: skip
    assert list(out["verdict"]) == [policy.VERDICTS[v] for v in want]
    flagged = out["verdict"] != "approve"
    assert flagged.any() and out.loc[flagged, "codes"].str.len().gt(0).all()
    assert out.loc[~flagged, "codes"].eq("").all()
    assert set(",".join(out.loc[flagged, "codes"]).split(",")) <= set(bundle["reason_map"].values())
    assert stream.decide_frame(frame.iloc[:0], text, bundle, "1", "sha").empty
    rows = stream.reason_rows(ROOT)
    assert len({(d, c) for d, c, *_ in rows}) == len(rows) == 12 + 12 + 3 + 2


@pytest.mark.spark
def test_the_stream_decides_each_live_event_once_and_a_rerun_adds_nothing(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    if not (LAKE / "payments_features").exists():
        pytest.skip("needs the fixture lake: scripts/run_portable.py then scripts/run_features.py")
    from pyspark.sql import functions as F

    text, _ = _model()
    bundle = stream.load_bundle(ROOT)
    silver = spark.read.parquet(str(LAKE / "payments_silver"))
    features = spark.read.parquet(str(LAKE / "payments_features"))
    days = sorted(r["d"] for r in silver.select(F.to_date("event_ts").alias("d")).distinct().collect())
    live_from = str(days[len(days) // 2])
    # Bronze as two deliveries, the second repeating part of the first: a stream over a folder.
    bronze = spark.read.parquet(str(LAKE / "payments_bronze")).select("txn_ref")
    source, log, check = tmp_path / "bronze", tmp_path / "log", tmp_path / "checkpoint"
    rows = bronze.orderBy("txn_ref").collect()
    half = len(rows) // 2

    def deliver(part: list, name: str) -> None:  # type: ignore[type-arg]
        spark.createDataFrame(part, bronze.schema).write.mode("overwrite").parquet(str(source / name))

    def run() -> None:
        reader = spark.readStream.schema(bronze.schema).option("recursiveFileLookup", "true").parquet(str(source))
        scored = stream.score(stream.batch_rows(reader, silver, features, live_from), text, bundle, "1", "sha")
        q = scored.writeStream.format("parquet").option("checkpointLocation", str(check)).trigger(availableNow=True)
        q.start(str(log)).awaitTermination()

    deliver(rows[:half], "d1")
    run()
    deliver(rows[half - 50 :], "d2")
    run()
    first = spark.read.parquet(str(log)).count()
    run()  # nothing new landed
    decided = spark.read.parquet(str(log)).dropDuplicates(["txn_ref"])
    assert spark.read.parquet(str(log)).count() == first
    expected = stream.live(silver, features, live_from)
    assert decided.count() == expected.count() > 0
    assert stream.pending(silver, features, decided, live_from).count() == 0
    assert stream.pending(silver, features, None, live_from).count() == expected.count()
    summary = stream.summary(decided)
    assert summary["decisions"] == expected.count() and summary["first_day"] >= live_from
