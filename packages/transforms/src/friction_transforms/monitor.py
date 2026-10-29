"""The monitor: drift, expectation rates, freshness, completeness, clock and label maturity.

Runs after every scoring run (jobs/monitor.py in the workspace, scripts/monitor.py on the build
machine) over the same tables. Each check has its threshold in config/monitor.yaml and returns
alert rows in the shape of ops.alerts; the task fails when any row is an alert, and the workspace
workflow opens a GitHub issue with the rows, the run id and the runbook link.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RUNBOOK = "docs/runbooks/ingestion.md"


def _alert(check: str, scope: str, value: float, threshold: float, severity: str, note: str) -> dict[str, Any]:
    return {
        "check": check,
        "scope": scope,
        "value": float(value),
        "threshold": float(threshold),
        "severity": severity,
        "note": note,
        "runbook": RUNBOOK,
    }


def months(start: str, end: str) -> list[str]:
    """yyyy-MM of every month from start up to the day before end, the months a split scores."""
    y, m = int(start[:4]), int(start[5:7])
    last = date.fromisoformat(end[:10]) - timedelta(days=1)
    out = []
    while (y, m) <= (last.year, last.month):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def psi_from_counts(expected: list[float], actual: list[float]) -> float:
    te, ta = sum(expected), sum(actual)
    if te == 0 or ta == 0:
        return 0.0
    total = 0.0
    for e, a in zip(expected, actual, strict=True):
        pe, pa = max(e / te, 1e-6), max(a / ta, 1e-6)
        total += (pa - pe) * math.log(pa / pe)
    return total


def feature_drift(
    features: DataFrame, events: DataFrame, key: str, names: list[str], reference: tuple[str, str], bins: int = 10
) -> list[dict[str, Any]]:
    """PSI of each feature, every month against the reference window, decile bins of the reference."""
    joined = features.join(events.select(key, F.date_format("event_ts", "yyyy-MM").alias("month"), "event_ts"), key)
    ref = joined.where((F.col("event_ts") >= F.lit(reference[0])) & (F.col("event_ts") < F.lit(reference[1])))
    probs = [i / bins for i in range(1, bins)]
    edges = ref.approxQuantile(names, probs, 0.001)
    out = []
    for name, cuts in zip(names, edges, strict=True):
        uniq = sorted(set(cuts))
        bucket = F.lit(0)
        for i, c in enumerate(uniq):
            bucket = F.when(F.col(name) > F.lit(c), F.lit(i + 1)).otherwise(bucket)
        counts = joined.groupBy("month", bucket.alias("b")).count().collect()
        ref_counts = ref.groupBy(bucket.alias("b")).count().collect()
        n = len(uniq) + 1
        base = [0.0] * n
        for r in ref_counts:
            base[int(r["b"])] = float(r["count"])
        by_month: dict[str, list[float]] = {}
        for r in counts:
            by_month.setdefault(r["month"], [0.0] * n)[int(r["b"])] = float(r["count"])
        for month, vals in sorted(by_month.items()):
            out.append({"feature": name, "month": month, "psi": psi_from_counts(base, vals)})
    return out


def expectation_rates(quarantine: DataFrame, bronze: DataFrame, desk: str) -> list[dict[str, Any]]:
    """Share of each drop's rows quarantined by each rule."""
    delivered = {r["drop_id"]: int(r["count"]) for r in bronze.groupBy("drop_id").count().collect()}
    rows = (
        quarantine.where(F.col("desk") == desk)
        .groupBy("drop_id", "rule")
        .agg(F.countDistinct("event_ref").alias("rows"))
        .collect()
    )
    return [
        {
            "desk": desk,
            "drop_id": r["drop_id"],
            "rule": r["rule"],
            "rows": int(r["rows"]),
            "share": int(r["rows"]) / max(delivered.get(r["drop_id"], 0), 1),
        }
        for r in sorted(rows, key=lambda r: (r["drop_id"], r["rule"]))
    ]


def completeness(silver: DataFrame, bronze: DataFrame, key: str, source_key: str) -> list[dict[str, Any]]:
    """Per drop: distinct delivered keys that reached silver (from this drop or an earlier one) over
    distinct delivered keys. A resent file's rows already in silver count as kept: deduplication is
    not loss."""
    delivered = bronze.select("drop_id", F.col(source_key).try_cast("bigint").alias(key)).distinct()
    kept = delivered.join(silver.select(key).distinct(), key, "left_semi").groupBy("drop_id").count()
    counts = delivered.groupBy("drop_id").count().withColumnRenamed("count", "delivered")
    rows = counts.join(kept.withColumnRenamed("count", "kept"), "drop_id", "left").collect()
    return [
        {
            "drop_id": r["drop_id"],
            "delivered": int(r["delivered"]),
            "kept": int(r["kept"] or 0),
            "ratio": int(r["kept"] or 0) / max(int(r["delivered"]), 1),
        }
        for r in sorted(rows, key=lambda r: r["drop_id"])
    ]


def clock(quarantine: DataFrame, bronze: DataFrame, desk: str, rule: str) -> list[dict[str, Any]]:
    """Per file: share of events stamped at or after the drop that delivered them closed."""
    files = {r["source_file"]: int(r["count"]) for r in bronze.groupBy("source_file").count().collect()}
    late = (
        quarantine.where((F.col("desk") == desk) & (F.col("rule") == rule))
        .groupBy("drop_id", "source_file")
        .agg(F.countDistinct("event_ref").alias("rows"))
        .collect()
    )
    return [
        {
            "drop_id": r["drop_id"],
            "file": str(r["source_file"]).rsplit("/", 1)[-1],
            "rows": int(r["rows"]),
            "share": int(r["rows"]) / max(files.get(r["source_file"], 0), 1),
        }
        for r in sorted(late, key=lambda r: (r["drop_id"], r["source_file"]))
    ]


def label_maturity(history: DataFrame, checkpoints: list[int]) -> list[dict[str, Any]]:
    """Per event month: the share of its eventually known frauds known within each checkpoint."""
    first = (
        history.where(F.col("is_fraud"))
        .groupBy("event_ref")
        .agg(F.min("__START_AT.reported_at").alias("known"), F.min("event_ts").alias("event_ts"))
    )
    lag = (F.unix_timestamp("known") - F.unix_timestamp("event_ts")) / 86400.0
    aggs = [F.count(F.lit(1)).alias("frauds")]
    aggs += [F.avg(F.when(lag <= d, 1.0).otherwise(0.0)).alias(f"d{d}") for d in checkpoints]
    rows = first.groupBy(F.date_format("event_ts", "yyyy-MM").alias("month")).agg(*aggs).collect()
    return [
        {"month": r["month"], "frauds": int(r["frauds"]), **{f"d{d}": float(r[f"d{d}"]) for d in checkpoints}}
        for r in sorted(rows, key=lambda r: r["month"])
    ]


def alerts(report: dict[str, Any], cfg: dict[str, Any], scored_months: set[str]) -> list[dict[str, Any]]:
    """Alert rows from a report and the thresholds of config/monitor.yaml."""
    out: list[dict[str, Any]] = []
    for desk, d in report["desks"].items():
        for r in d.get("feature_drift", []):
            if r["month"] in scored_months and r["psi"] >= cfg["psi"]["feature_alert"]:
                out.append(
                    _alert(
                        "feature_psi",
                        f"{desk}/{r['feature']}/{r['month']}",
                        r["psi"],
                        cfg["psi"]["feature_alert"],
                        "serious",
                        "drift against the training months",
                    )
                )
        for r in d.get("expectations", []):
            if r["share"] > cfg["expectations"]["rule_alert"]:
                out.append(
                    _alert(
                        "expectation_rate",
                        f"{desk}/{r['drop_id']}/{r['rule']}",
                        r["share"],
                        cfg["expectations"]["rule_alert"],
                        "serious",
                        f"{r['rows']} rows quarantined",
                    )
                )
        for r in d.get("completeness", []):
            if r["ratio"] < cfg["completeness"]["min_ratio"]:
                out.append(
                    _alert(
                        "completeness",
                        f"{desk}/{r['drop_id']}",
                        r["ratio"],
                        cfg["completeness"]["min_ratio"],
                        "critical",
                        f"{r['delivered'] - r['kept']} delivered rows did not reach silver",
                    )
                )
        for r in d.get("clock", []):
            if r["share"] > cfg["clock"]["max_future_share"]:
                out.append(
                    _alert(
                        "clock",
                        f"{desk}/{r['drop_id']}/{r['file']}",
                        r["share"],
                        cfg["clock"]["max_future_share"],
                        "critical",
                        "events stamped after their drop closed",
                    )
                )
    return out
