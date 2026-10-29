"""The export: each desk's champion in a portable form, and the parity replay set.

The bundle is what the API loads (packages/api/src/friction_api/bundle.py): the model file, the
operating point thresholds chosen on validation, the reason code map, and the reference tables the
features read. The parity set is the first 1,000 events of each desk's test months in event order,
every one of them, with the online state of every card, user and address they touch as it stood
just before the first, and the lakehouse's own score, verdict and codes for each. Taking every
event in a window rather than a sample of users is what lets the address features match: an
address's counts depend on everyone who signed in from it.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from friction_core.hashing import sha256_bytes
from friction_features import logins as login_features
from friction_features import payments as payment_features
from friction_policy.policy import Thresholds, one
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

Read = Callable[[str], DataFrame]
PARITY_EVENTS = 1000


def _rows(frame: pd.DataFrame) -> list[Any]:
    """Rows as attribute bags; plain Python values, so the state machines see what batch.py sees."""
    from types import SimpleNamespace

    return [SimpleNamespace(**{str(k): v for k, v in rec.items()}) for rec in frame.to_dict(orient="records")]


def _first(frame: pd.DataFrame, column: str) -> int:
    return int(frame[column].to_numpy()[0])
MODEL_FILES = {
    ("payments", "logistic"): ("payments_logistic.json", "model.json"),
    ("payments", "lightgbm"): ("payments_lightgbm.txt", "model.txt"),
    ("logins", "freeman"): ("logins_freeman.json", "model.json"),
    ("logins", "lightgbm"): ("logins_lightgbm.txt", "model.txt"),
    ("logins", "lightgbm_rep"): ("logins_lightgbm_rep.txt", "model.txt"),
}


def served_model(desk_result: dict[str, Any]) -> tuple[str, str]:
    """The champion if one cleared the gates, otherwise the floor: a desk without a champion runs on
    the day one rules, as a bank would, and says so in /health."""
    champ = desk_result["champion"]["model"]
    if champ:
        return champ, "champion"
    return "floor", "floor: no model cleared every gate"


def write_bundle(
    folder: Path, desk: str, model: str, status: str, thresholds: dict[str, float], models: Path,
    reason_map: dict[str, str], extra: dict[str, bytes],
) -> dict[str, Any]:  # fmt: skip
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)
    files: dict[str, bytes] = dict(extra)
    if model != "floor":
        source, target = MODEL_FILES[(desk, model)]
        files[target] = (models / source).read_bytes()
    for name, data in files.items():
        (folder / name).write_bytes(data)
    meta = {
        "desk": desk,
        "model": model,
        "status": status,
        "thresholds": thresholds,
        "files": {name: sha256_bytes(data) for name, data in sorted(files.items())},
        "reason_map": reason_map,
    }
    (folder / "bundle.json").write_text(json.dumps(meta, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return meta


def mcc_rates_json(rates: DataFrame) -> bytes:
    rows = rates.select(F.date_format("month_start", "yyyy-MM-dd").alias("m"), "mcc", "rate").collect()
    out: dict[str, dict[str, float]] = {}
    for r in rows:
        out.setdefault(r["m"], {})[str(int(r["mcc"]))] = float(r["rate"])
    return json.dumps({m: dict(sorted(v.items())) for m, v in sorted(out.items())}, sort_keys=True).encode()


def _card_snapshots(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(["account_key", "event_ts_s", "txn_ref"], kind="mergesort")
    out = []
    for acc, g in pdf.groupby("account_key", sort=True):
        state = payment_features.CardState()
        for r in _rows(g):
            ev = payment_features.PaymentEvent(
                int(r.event_ts_s), int(r.txn_ref), int(r.amount_cents), str(r.merchant_key), int(r.mcc),
                int(r.channel), str(r.location or ""), bool(r.foreign), bool(r.has_error),
                float(r.days_since_issue), float(r.mcc_fraud_rate),
            )  # fmt: skip
            # features() expires old window entries, as the offline pass does before each update.
            state.features(ev)
            state.update(ev)
        out.append((acc, json.dumps(state.snapshot())))
    return pd.DataFrame(out, columns=["key", "state"])


def _user_snapshots(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(["user_key", "event_ts_ms", "login_ref"], kind="mergesort")
    out = []
    for user, g in pdf.groupby("user_key", sort=True):
        state = login_features.UserState()
        for r in _rows(g):
            values = tuple(str(getattr(r, f"v_{lvl}") or "") for lvl in login_features.LEVELS)
            rtt = None if r.rtt_ms is None or (isinstance(r.rtt_ms, float) and np.isnan(r.rtt_ms)) else float(r.rtt_ms)
            ev = login_features.LoginEvent(int(r.event_ts_ms), int(r.login_ref), values, rtt, bool(r.success))
            state.features(ev)
            state.update(ev)
        out.append((user, json.dumps(state.snapshot())))
    return pd.DataFrame(out, columns=["key", "state"])


def _ip_snapshots(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(["ip_key", "event_ts_ms", "login_ref"], kind="mergesort")
    out = []
    for ip, g in pdf.groupby("ip_key", sort=True):
        state = login_features.IpState()
        for r in _rows(g):
            state.features(int(r.event_ts_ms))
            state.update(int(r.event_ts_ms), bool(r.success))
        out.append((ip, json.dumps(state.snapshot())))
    return pd.DataFrame(out, columns=["key", "state"])


def _snapshots(history: DataFrame, key: str, fn: Callable[[pd.DataFrame], pd.DataFrame]) -> dict[str, Any]:
    from pyspark.sql import types as T

    schema = T.StructType([T.StructField("key", T.StringType()), T.StructField("state", T.StringType())])
    bucketed = history.withColumn("_b", F.pmod(F.hash(key), F.lit(16)))
    rows = bucketed.groupBy("_b").applyInPandas(lambda pdf: fn(pdf.drop(columns=["_b"])), schema).collect()
    return {r["key"]: json.loads(r["state"]) for r in sorted(rows, key=lambda r: r["key"])}


def payments_parity(read: Read, scored: Path, start: str, model: str, t: Thresholds) -> dict[str, Any]:
    inputs = read("payment_feature_inputs")
    first = inputs.where(F.col("event_ts_s") >= F.unix_timestamp(F.lit(start)))
    window = first.orderBy("event_ts_s", "txn_ref").limit(PARITY_EVENTS).toPandas()
    window = window.sort_values(["event_ts_s", "txn_ref"], kind="mergesort").reset_index(drop=True)
    t0, r0 = _first(window, "event_ts_s"), _first(window, "txn_ref")
    accounts = sorted(window["account_key"].unique().tolist())
    spark_accounts = inputs.sparkSession.createDataFrame([(a,) for a in accounts], "account_key string")
    before = (F.col("event_ts_s") < t0) | ((F.col("event_ts_s") == t0) & (F.col("txn_ref") < r0))
    history = inputs.join(spark_accounts, "account_key").where(before).drop("_bucket")
    cards = _snapshots(history, "account_key", _card_snapshots)
    silver = read("payments_silver").select("account_key", "account_ref").distinct().join(spark_accounts, "account_key")
    issues = (
        read("cards_history")
        .groupBy("account_ref", "card_generation")
        .agg(F.unix_timestamp(F.min(F.col("__START_AT.changed_at"))).alias("issued_at"))
        .join(silver, "account_ref")
        .groupBy("account_key")
        .agg(F.sort_array(F.collect_list("issued_at")).alias("issued"))
        .collect()
    )
    import pyarrow.parquet as pq

    sc = pq.read_table(scored / "payments_test").to_pandas()
    sc = sc[sc["txn_ref"].isin(window["txn_ref"])].set_index("txn_ref")
    events, expected = [], []
    for r in _rows(window):
        ts = pd.Timestamp(int(r.event_ts_s), unit="s").strftime("%Y-%m-%dT%H:%M:%S")
        events.append({
            "event_ref": int(r.txn_ref), "ts": ts, "account_key": r.account_key, "amount_cents": int(r.amount_cents),
            "merchant_key": r.merchant_key, "mcc": int(r.mcc), "channel": int(r.channel),
            "location": str(r.location or ""), "foreign": bool(r.foreign), "has_error": bool(r.has_error),
        })  # fmt: skip
        if int(r.txn_ref) in sc.index:
            row = sc.loc[int(r.txn_ref)]
            score = float(row[f"score_{model}"])
            verdict = one(score, int(r.channel), t)
            codes = (
                [c for c in str(row.get(f"codes_{model}", "") or "").split(",") if c] if verdict != "approve" else []
            )
            expected.append({"event_ref": int(r.txn_ref), "score": score, "verdict": verdict, "codes": codes})
    snapshot = {"card": cards, "card_issues": {r["account_key"]: [int(x) for x in r["issued"]] for r in issues}}
    return {"desk": "payments", "model": model, "events": events, "expected": expected, "snapshot": snapshot}


def logins_parity(read: Read, scored: Path, start: str, model: str, t: Thresholds) -> dict[str, Any]:
    silver = read("logins_silver")
    inputs = read("login_feature_inputs")
    ms = F.unix_timestamp("event_ts") * 1000 + F.date_format("event_ts", "SSS").cast("long")
    first = silver.where(F.col("event_ts") >= F.to_timestamp(F.lit(start))).withColumn("event_ts_ms", ms)
    cols = ["login_ref", "user_key", "event_ts_ms", "ip_key", "asn", "country", "region", "city", "ua_key", "browser",
            "os", "device_type", "rtt_ms", "success", "is_attack_ip", "unknown_account"]  # fmt: skip
    window = first.orderBy("event_ts_ms", "login_ref").limit(PARITY_EVENTS).select(*cols).toPandas()
    window = window.sort_values(["event_ts_ms", "login_ref"], kind="mergesort").reset_index(drop=True)
    t0, r0 = _first(window, "event_ts_ms"), _first(window, "login_ref")
    before = (F.col("event_ts_ms") < t0) | ((F.col("event_ts_ms") == t0) & (F.col("login_ref") < r0))
    spark = silver.sparkSession
    users = sorted(window.loc[~window["unknown_account"].astype(bool), "user_key"].unique().tolist())
    ips = sorted(window["ip_key"].unique().tolist())
    u = spark.createDataFrame([(x,) for x in users], "user_key string")
    i = spark.createDataFrame([(x,) for x in ips], "ip_key string")
    real = inputs.where(~F.col("synthetic") & before)
    user_states = _snapshots(real.join(u, "user_key"), "user_key", _user_snapshots)
    ip_states = _snapshots(real.join(i, "ip_key"), "ip_key", _ip_snapshots)
    import pyarrow.parquet as pq

    sc = pq.read_table(scored / "logins_test").to_pandas()
    sc = sc[~sc["synthetic"].astype(bool) & sc["login_ref"].isin(window["login_ref"])].set_index("login_ref")
    events, expected = [], []
    for r in _rows(window):
        ts = pd.Timestamp(int(r.event_ts_ms), unit="ms").strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        rtt = None if r.rtt_ms is None or (isinstance(r.rtt_ms, float) and np.isnan(r.rtt_ms)) else float(r.rtt_ms)
        events.append({
            "event_ref": int(r.login_ref), "ts": ts, "user_key": r.user_key, "ip_key": r.ip_key, "ua_key": r.ua_key,
            "asn": None if pd.isna(r.asn) else int(r.asn), "country": r.country, "region": r.region, "city": r.city,
            "browser": r.browser, "os": r.os, "device_type": r.device_type, "rtt_ms": rtt, "success": bool(r.success),
            "is_attack_ip": bool(r.is_attack_ip),
        })  # fmt: skip
        if int(r.login_ref) in sc.index:
            row = sc.loc[int(r.login_ref)]
            score = float(row[f"score_{model}"])
            verdict = one(score, None, t)
            codes = (
                [c for c in str(row.get(f"codes_{model}", "") or "").split(",") if c] if verdict != "approve" else []
            )
            expected.append({"event_ref": int(r.login_ref), "score": score, "verdict": verdict, "codes": codes})
    snapshot = {"user": user_states, "ip": ip_states}
    unknown = sorted(window.loc[window["unknown_account"].astype(bool), "user_key"].unique().tolist())
    return {"desk": "logins", "model": model, "events": events, "expected": expected, "snapshot": snapshot,
            "unknown_accounts": unknown}  # fmt: skip


def export_step(read: Read, plan: dict[str, Any], evaluation: dict[str, Any], models: Path, scored: Path,
                out: Path, reason_maps: dict[str, dict[str, str]]) -> dict[str, Any]:  # fmt: skip
    """Write results/models/<desk>/ and results/parity/<desk>.json. Returns a summary."""
    summary: dict[str, Any] = {}
    unknown = sorted(
        r["user_key"] for r in read("login_account_stats").where(F.col("unknown_account")).select("user_key").collect()
    )
    for desk in ("payments", "logins"):
        result = evaluation["desks"][desk]
        model, status = served_model(result)
        th = result["models"][model]["thresholds"]
        extra: dict[str, bytes] = {}
        if desk == "payments":
            extra["mcc_rates.json"] = mcc_rates_json(read("mcc_fraud_rates"))
        else:
            extra["unknown_accounts.json"] = json.dumps(unknown).encode()
        meta = write_bundle(out / "models" / desk, desk, model, status, th, models, reason_maps[desk], extra)
        start = plan["desks"][desk]["splits"]["test"]["start"]
        t = Thresholds(**th)
        fn = payments_parity if desk == "payments" else logins_parity
        parity = fn(read, scored, start, model, t)
        parity["bundle_files"] = meta["files"]
        target = out / "parity" / f"{desk}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(parity, sort_keys=True) + "\n", encoding="utf-8")
        summary[desk] = {"model": model, "status": status, "events": len(parity["events"]),
                         "expected": len(parity["expected"])}  # fmt: skip
    return summary
