"""Decisions as events arrive: the payments champion over each new micro batch, merged by key.

jobs/decisions.py reads payments bronze as a stream (append only, every drop as it lands) with an
available now trigger and a checkpoint, so each run picks up exactly the rows that landed since the
last one. For each micro batch it takes the transaction references, keeps those that passed into
silver on or after the day the thresholds went live, joins their feature vectors, and scores them
with the model file of the version the champion alias names, checked against the served bundle by
sha256. The result is merged into the decisions table on txn_ref, so a replayed batch changes
nothing. A catch up pass then scores any live event with a feature vector and no decision yet (a row
whose features were computed after its bronze row was read), so every event is decided once.

The functions here hold everything but the stream and the merge, and run on both engines.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
from friction_features import payments as payment_features
from friction_policy import policy
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from friction_models.learners import Gbm

FEATURES = list(payment_features.FEATURES)
KEY = "txn_ref"
CONTEXT = ("txn_ref", "account_key", "event_ts", "amount_cents")
OUT = ("txn_ref", "account_key", "event_ts", "event_date", "amount_cents", "channel", "score", "verdict", "codes",
       "model_version", "model_sha256")  # fmt: skip


def schema() -> Any:
    from pyspark.sql import types as T

    s, d, ln = T.StringType(), T.DoubleType(), T.LongType()
    types = {"txn_ref": ln, "account_key": s, "event_ts": T.TimestampType(), "event_date": T.DateType(),
             "amount_cents": ln, "channel": T.IntegerType(), "score": d, "verdict": s, "codes": s,
             "model_version": s, "model_sha256": s}  # fmt: skip
    return T.StructType([T.StructField(n, types[n]) for n in OUT])


def load_bundle(root: Path) -> dict[str, Any]:
    bundle: dict[str, Any] = json.loads((root / "results" / "models" / "payments" / "bundle.json").read_text())
    return bundle


def decide_frame(pdf: pd.DataFrame, model_text: str, bundle: dict[str, Any], version: str, sha: str) -> pd.DataFrame:
    """Score, verdict at the served thresholds, and the served reason codes for every step up or decline."""
    from friction_reasons.reasons import top_codes

    gbm = Gbm.from_text(model_text)
    x = pdf[FEATURES].astype(float).to_numpy(np.float64)
    scores = gbm.predict(x) if len(pdf) else np.zeros(0)
    t = policy.Thresholds(**bundle["thresholds"])
    channel: npt.NDArray[np.int_] = np.asarray(pdf["channel"].to_numpy(np.float64), dtype=np.int_)
    verdicts = policy.payments(scores, channel, t)
    codes = [""] * len(pdf)
    flagged = np.nonzero(verdicts != policy.APPROVE)[0]
    if len(flagged):
        contrib = gbm.contributions(x[flagged])
        for i, row in zip(flagged, contrib, strict=True):
            codes[i] = ",".join(top_codes(row, FEATURES, bundle["reason_map"]))
    out = pdf[list(CONTEXT)].copy()
    out["event_date"] = pd.to_datetime(out["event_ts"]).dt.date
    out["channel"] = channel.astype("int32")
    out["score"] = scores
    out["verdict"] = [policy.VERDICTS[v] for v in verdicts]
    out["codes"] = codes
    out["model_version"] = version
    out["model_sha256"] = sha
    return out[list(OUT)]


def live(silver: DataFrame, features: DataFrame, live_from: str) -> DataFrame:
    """Every silver event on or after live_from with its feature vector: what is decided."""
    events = silver.where(F.col("event_ts") >= F.lit(live_from)).select(*CONTEXT)
    return events.join(features.select(KEY, *FEATURES), KEY)


def batch_rows(batch: DataFrame, silver: DataFrame, features: DataFrame, live_from: str) -> DataFrame:
    """The live events among bronze rows, a stream or a batch (a quarantined row never reaches silver).

    No distinct: a stream would need state for it. A delivery seen twice gives two rows, and the merge
    keeps one."""
    refs = batch.select(F.col("txn_ref").try_cast("bigint").alias(KEY)).where(F.col(KEY).isNotNull())
    return refs.join(live(silver, features, live_from), KEY)


def pending(silver: DataFrame, features: DataFrame, decisions: DataFrame | None, live_from: str) -> DataFrame:
    """Live events with a feature vector and no decision."""
    rows = live(silver, features, live_from)
    return rows if decisions is None else rows.join(decisions.select(KEY), KEY, "left_anti")


def score(rows: DataFrame, model_text: str, bundle: dict[str, Any], version: str, sha: str) -> DataFrame:
    def run(batches):  # type: ignore[no-untyped-def]
        for pdf in batches:
            yield decide_frame(pdf, model_text, bundle, version, sha)

    return rows.mapInPandas(run, schema())


def reason_rows(root: Path) -> list[tuple[str, str, str, str, str]]:
    """desk, code, says, features, table: both code tables as rows, for the dashboard and Genie."""
    import yaml

    out = []
    for table in ("reason_codes.yaml", "reason_codes_v2.yaml"):
        data = yaml.safe_load((root / "evaluation" / table).read_text(encoding="utf-8"))
        for desk in ("payments", "logins"):
            for code, entry in data[desk].items():
                out.append((desk, code, entry["says"], ",".join(entry["features"]), table))
    return sorted(out)


def summary(decisions: DataFrame) -> dict[str, Any]:
    """Counts by verdict and the date range: what the job and the dashboard print."""
    by = {r["verdict"]: int(r["n"]) for r in decisions.groupBy("verdict").agg(F.count(F.lit(1)).alias("n")).collect()}
    span = decisions.agg(F.min("event_date").alias("a"), F.max("event_date").alias("b")).collect()[0]
    return {"decisions": sum(by.values()), "by_verdict": dict(sorted(by.items())),
            "first_day": str(span["a"]), "last_day": str(span["b"])}  # fmt: skip
