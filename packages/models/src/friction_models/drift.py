"""Drift between a reference window of decisions and the latest one, and the rule that asks to retrain.

The population stability index of the score and of the features that move the payments model most,
on decile bins of the reference window, computed with Spark counts so no window is collected. The
rule is registered in config/drift.yaml: the score's index at or above the plan's stability bound
(0.2), or enough features at or above their own bound. A retrain registers new versions under the
challenger alias and changes no champion; promotion stays a reviewed request.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FLOOR = 1e-6


def psi_from_counts(expected: Sequence[int], actual: Sequence[int]) -> float:
    """The index from bin counts, each share floored as friction_evaluation.grade.psi floors it."""
    te, ta = sum(expected), sum(actual)
    if te == 0 or ta == 0:
        return 0.0
    total = 0.0
    for e, a in zip(expected, actual, strict=True):
        pe, pa = max(e / te, FLOOR), max(a / ta, FLOOR)
        total += (pa - pe) * math.log(pa / pe)
    return total


def edges(reference: DataFrame, column: str, bins: int = 10) -> list[float]:
    """Inner decile edges of the reference window, distinct and sorted (exact quantiles)."""
    qs = [i / bins for i in range(1, bins)]
    got = reference.approxQuantile(column, qs, 0.0)
    return sorted(set(float(v) for v in got))


def bin_counts(frame: DataFrame, column: str, inner: list[float]) -> list[int]:
    """Counts per bin, a value equal to an edge going to the bin above it (searchsorted right)."""
    b = F.lit(0)
    for edge in inner:
        b = b + F.when(F.col(column) >= F.lit(edge), 1).otherwise(0)
    rows = {int(r["b"]): int(r["n"]) for r in frame.select(b.alias("b")).groupBy("b").count()
            .withColumnRenamed("count", "n").collect()}  # fmt: skip
    return [rows.get(i, 0) for i in range(len(inner) + 1)]


def psi(reference: DataFrame, current: DataFrame, column: str, bins: int = 10) -> float:
    inner = edges(reference, column, bins)
    if len(inner) < 2:  # a column with almost one value carries no distribution to compare
        return 0.0
    return psi_from_counts(bin_counts(reference, column, inner), bin_counts(current, column, inner))


def windows(decisions: DataFrame, live_from: str, days: int) -> tuple[str, str, str, str]:
    """The first days after live_from and the last days of decisions: (ref start, ref end, cur start, cur end)."""
    last = decisions.agg(F.max("event_date").alias("d")).collect()[0]["d"]
    ref_start = F.to_date(F.lit(live_from))
    row = decisions.select(
        ref_start.alias("a"), F.date_add(ref_start, days).alias("b"),
        F.date_sub(F.lit(last), days - 1).alias("c"), F.date_add(F.lit(last), 1).alias("d"),
    ).limit(1).collect()[0]  # fmt: skip
    return str(row["a"]), str(row["b"]), str(row["c"]), str(row["d"])


def check(frame: DataFrame, config: dict[str, Any], live_from: str) -> dict[str, Any]:
    """frame: decisions joined to their feature vectors. Returns the indices and the verdict."""
    days = int(config["window_days"])
    a, b, c, d = windows(frame, live_from, days)
    ref = frame.where((F.col("event_date") >= F.lit(a)) & (F.col("event_date") < F.lit(b)))
    cur = frame.where((F.col("event_date") >= F.lit(c)) & (F.col("event_date") < F.lit(d)))
    score = psi(ref, cur, "score")
    features = {f: psi(ref, cur, f) for f in config["features"]}
    over = sorted(f for f, v in features.items() if v >= float(config["feature_bound"]))
    retrain = score >= float(config["score_bound"]) or len(over) >= int(config["features_over"])
    return {
        "reference": [a, b], "current": [c, d], "rows": {"reference": ref.count(), "current": cur.count()},
        "score_psi": score, "feature_psi": features, "features_over": over, "retrain": bool(retrain),
        "rule": {k: config[k] for k in ("score_bound", "feature_bound", "features_over", "window_days")},
    }  # fmt: skip
