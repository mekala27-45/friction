"""The model steps, written once and run in two places.

Each step takes a Spark session, a ``read(name)`` that returns a table, and a folder. The workspace
jobs (jobs/attacks.py, jobs/train.py, jobs/score.py) pass Delta tables and a volume; the build
machine passes Parquet and a local folder. Nothing here grades: the scored sets are exported and
the repository grades them (packages/evaluation, scripts/grade.py).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml
from friction_features import logins as login_features
from friction_features import payments as payment_features
from friction_simulator import attackers
from friction_transforms import training
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from friction_models import floors
from friction_models.freeman import CODE_FEATURES, MIN_COUNT, PRUNED, Freeman
from friction_models.learners import Gbm, Logistic

Read = Callable[[str], DataFrame]
PAY_FEATURES = list(payment_features.FEATURES)
LOGIN_FEATURES = [*login_features.FEATURES, *login_features.IP_FEATURES]
LOGIN_FEATURES_REP = [*LOGIN_FEATURES, "is_attack_ip"]
NEGATIVES_FOR_TRAINING = 1_500_000
SEED = 20261029


def load_plan(root: Path) -> dict[str, Any]:
    plan: dict[str, Any] = yaml.safe_load((root / "evaluation" / "plan.yaml").read_text(encoding="utf-8"))
    return plan


def attacks_step(spark: Any, read: Read, plan: dict[str, Any]) -> pd.DataFrame:
    """Synthetic attacks: held out users in the test months, other users in the training months."""
    p = plan["desks"]["logins"]
    silver = read("logins_silver")
    out = []
    test_users = None
    for purpose, split, n_key in (("test", "test", "held_out_users"), ("train", "train", "training_users")):
        start, end = p["splits"][split]["start"], p["splits"][split]["end"]
        active = training.active_users(silver, start, end)
        profiles = training.victim_profiles(silver, start).join(active, "user_key")
        if purpose == "train" and test_users is not None:
            profiles = profiles.join(test_users, "user_key", "left_anti")
        n = int(p["attackers"][n_key])
        chosen = (
            profiles.withColumn("_h", F.xxhash64(F.lit(f"{SEED}:{purpose}"), "user_key"))
            .orderBy("_h", "user_key")
            .limit(n)
        )
        victims = chosen.drop("_h").toPandas()
        if purpose == "test":
            test_users = chosen.select("user_key")
        pool_start = p["splits"]["train"]["start"]
        pool = training.ip_pool(silver, pool_start, end).toPandas()
        att_ua = training.ua_counts(silver, pool_start, end, attack=True).toPandas()
        leg_ua = training.ua_counts(silver, pool_start, end, attack=False).toPandas()
        gen = attackers.generate(victims, pool, att_ua, leg_ua, pd.Timestamp(start), pd.Timestamp(end), SEED, purpose)
        out.append(gen)
    return pd.concat(out, ignore_index=True)


def _collect_training(frame: DataFrame, key: str) -> pd.DataFrame:
    """All positives and a keyed sample of negatives, with weights that restore the negative count."""
    counts = frame.groupBy("label").count().collect()
    negatives = next((int(r["count"]) for r in counts if not r["label"]), 0)
    rate = min(1.0, NEGATIVES_FOR_TRAINING / max(negatives, 1))
    u = (F.abs(F.xxhash64(F.lit(SEED), F.col(key))) % 1_000_000) / 1_000_000.0
    kept = frame.where(F.col("label") | (u < F.lit(rate)))
    pdf = kept.toPandas()
    pdf["weight"] = np.where(pdf["label"], 1.0, 1.0 / rate if rate > 0 else 1.0)
    return pdf.sort_values(key, kind="mergesort").reset_index(drop=True)


def train_step(
    spark: Any, read: Read, plan: dict[str, Any], out: Path, *, label_table: str = "labels_payments_history",
    train_months: int = 24, tag: str = "default",
) -> dict[str, Any]:  # fmt: skip
    """Fit the payments and login models; write them under out/<tag>/. Returns a summary."""
    folder = out / tag
    folder.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {"tag": tag}
    pay = plan["desks"]["payments"]
    train = pay["splits"]["train"]
    start = str((pd.Timestamp(train["end"]) - pd.DateOffset(months=train_months)).date())
    tset = training.payments_set(
        read("payments_features"), read("payments_silver"), read(label_table), start, train["end"],
        pay["label_cuts"]["train"],
    )  # fmt: skip
    pdf = _collect_training(tset, "txn_ref")
    x = pdf[PAY_FEATURES].to_numpy(np.float64)
    y = pdf["label"].to_numpy(bool).astype(int)
    w = pdf["weight"].to_numpy(np.float64)
    logistic = Logistic.fit(x, y, weight=w)
    gbm = Gbm.fit(x, y, PAY_FEATURES, weight=w)
    (folder / "payments_logistic.json").write_text(json.dumps(logistic.to_dict()), encoding="utf-8")
    (folder / "payments_lightgbm.txt").write_text(gbm.to_text(), encoding="utf-8")
    summary["payments"] = {"rows": len(pdf), "positives": int(y.sum()), "start": start, "end": train["end"]}
    if tag != "default":
        return summary
    lg = plan["desks"]["logins"]
    ltrain = lg["splits"]["train"]
    silver = read("logins_silver")
    ok = silver.where(
        (F.col("event_ts") >= F.lit(ltrain["start"]))
        & (F.col("event_ts") < F.lit(ltrain["end"]))
        & F.col("success")
        & ~F.col("unknown_account")
    )
    legit = int(ok.count())
    users = int(silver.where(~F.col("unknown_account")).select("user_key").distinct().count())
    counts: dict[str, dict[str, int]] = {}
    distinct: dict[str, int] = {}
    for lvl, col in zip(login_features.LEVELS, training.LOGIN_VALUES, strict=True):
        if lvl in ("region", "city"):
            continue
        grouped = ok.groupBy(F.coalesce(F.col(col).cast("string"), F.lit("")).alias("v")).count()
        distinct[lvl] = int(grouped.count())
        if lvl in PRUNED:
            grouped = grouped.where(F.col("count") >= MIN_COUNT)
        counts[lvl] = {r["v"]: int(r["count"]) for r in grouped.collect()}
    freeman = Freeman.from_counts(counts, distinct, legit, users)
    (folder / "logins_freeman.json").write_text(freeman.to_json(), encoding="utf-8")
    lset = training.logins_set(
        read("logins_features"), silver, read("login_attacks"), read("labels_logins_history"), ltrain["start"],
        ltrain["end"], lg["label_cuts"]["train"], "train",
    )  # fmt: skip
    lpdf = _collect_training(lset, "login_ref")
    for with_rep, name in ((False, "logins_lightgbm.txt"), (True, "logins_lightgbm_rep.txt")):
        cols = LOGIN_FEATURES_REP if with_rep else LOGIN_FEATURES
        lx = lpdf[cols].astype(float).to_numpy(np.float64)
        ly = lpdf["label"].to_numpy(bool).astype(int)
        lw = lpdf["weight"].to_numpy(np.float64)
        model = Gbm.fit(lx, ly, cols, weight=lw)
        (folder / name).write_text(model.to_text(), encoding="utf-8")
    summary["logins"] = {
        "rows": len(lpdf),
        "positives": int(lpdf["label"].sum()),
        "legit_logins": legit,
        "users": users,
    }
    (folder / "summary.json").write_text(json.dumps(summary, indent=1), encoding="utf-8")
    return summary


def _codes(contrib: np.ndarray, names: list[str], mapping: dict[str, str]) -> list[str]:
    from friction_reasons.reasons import top_codes

    return [",".join(top_codes(row, names, mapping)) for row in contrib]


def score_payments(pdf: pd.DataFrame, folder: Path) -> pd.DataFrame:
    """Scores of every model, and the floor's codes (the rules that fired)."""
    x = pdf[PAY_FEATURES].to_numpy(np.float64)
    logistic = Logistic.from_dict(json.loads((folder / "payments_logistic.json").read_text(encoding="utf-8")))
    gbm = Gbm.from_text((folder / "payments_lightgbm.txt").read_text(encoding="utf-8"))
    out = pdf[["txn_ref", "account_key", "event_ts", "amount_cents", "channel", "label", "labelled"]].copy()
    for name in HYPOTHESIS_COLUMNS:
        out[name] = pdf[name].astype(float)
    out["score_floor"] = floors.payment_scores(x)
    out["codes_floor"] = [",".join(floors.payment_fired(r)) for r in x]
    out["score_logistic"] = logistic.predict(x)
    out["score_lightgbm"] = gbm.predict(x)
    return out


def code_payments(pdf: pd.DataFrame, folder: Path, mapping: dict[str, str], cut: dict[str, float]) -> pd.DataFrame:
    """Reason codes for the rows at or above each model's cutoff; empty below it."""
    x = pdf[PAY_FEATURES].to_numpy(np.float64)
    out = pdf[["txn_ref"]].copy()
    models: dict[str, Logistic | Gbm] = {
        "logistic": Logistic.from_dict(json.loads((folder / "payments_logistic.json").read_text(encoding="utf-8"))),
        "lightgbm": Gbm.from_text((folder / "payments_lightgbm.txt").read_text(encoding="utf-8")),
    }
    for name, model in models.items():
        codes = [""] * len(pdf)
        rows = np.nonzero(pdf[f"score_{name}"].to_numpy(np.float64) >= cut[name])[0]
        if len(rows):
            for i, c in zip(rows, _codes(model.contributions(x[rows]), PAY_FEATURES, mapping), strict=True):
                codes[i] = c
        out[f"codes_{name}"] = codes
    return out


def score_logins(pdf: pd.DataFrame, folder: Path) -> pd.DataFrame:
    """Scores of every model, the floor's codes, and the columns the hypotheses and the RBA comparison read."""
    freeman = Freeman.from_json((folder / "logins_freeman.json").read_text(encoding="utf-8"))
    values = list(zip(*[pdf[f"v_{lvl}"].astype(str) for lvl in login_features.LEVELS], strict=True))
    feats = pdf[list(login_features.FEATURES)].to_numpy(np.float64)
    out = pdf[
        ["login_ref", "user_key", "event_ts", "success", "is_attack_ip", "synthetic", "attacker_model", "label"]
    ].copy()
    out["n_success_prior"] = pdf["n_success_prior"].astype("int64")
    for lvl in ("ip", "asn", "country", "city", "device"):
        out[f"new_{lvl}"] = pdf[f"{lvl}_count"].to_numpy(np.float64) == 0
    out["score_floor"] = floors.login_scores(feats)
    out["codes_floor"] = [",".join(floors.login_fired(r)) for r in feats]
    out["score_freeman"] = freeman.scores(values, feats)
    for name, cols in (("lightgbm", LOGIN_FEATURES), ("lightgbm_rep", LOGIN_FEATURES_REP)):
        model = Gbm.from_text((folder / f"logins_{name}.txt").read_text(encoding="utf-8"))
        out[f"score_{name}"] = model.predict(pdf[cols].astype(float).to_numpy(np.float64))
    return out


def code_logins(pdf: pd.DataFrame, folder: Path, mapping: dict[str, str], cut: dict[str, float]) -> pd.DataFrame:
    out = pdf[["login_ref"]].copy()
    freeman = Freeman.from_json((folder / "logins_freeman.json").read_text(encoding="utf-8"))
    values = list(zip(*[pdf[f"v_{lvl}"].astype(str) for lvl in login_features.LEVELS], strict=True))
    feats = pdf[list(login_features.FEATURES)].to_numpy(np.float64)
    codes = [""] * len(pdf)
    rows = np.nonzero(pdf["score_freeman"].to_numpy(np.float64) >= cut["freeman"])[0]
    if len(rows):
        contrib = np.array([freeman.contributions(values[i], feats[i]) for i in rows], dtype=np.float64)
        for i, c in zip(
            rows, _codes(contrib.reshape(len(rows), len(CODE_FEATURES)), CODE_FEATURES, mapping), strict=True
        ):
            codes[i] = c
    out["codes_freeman"] = codes
    for name, cols in (("lightgbm", LOGIN_FEATURES), ("lightgbm_rep", LOGIN_FEATURES_REP)):
        model = Gbm.from_text((folder / f"logins_{name}.txt").read_text(encoding="utf-8"))
        x = pdf[cols].astype(float).to_numpy(np.float64)
        codes = [""] * len(pdf)
        rows = np.nonzero(pdf[f"score_{name}"].to_numpy(np.float64) >= cut[name])[0]
        if len(rows):
            for i, c in zip(rows, _codes(model.contributions(x[rows]), cols, mapping), strict=True):
                codes[i] = c
        out[f"codes_{name}"] = codes
    return out


# The payment features evaluation/hypotheses.yaml reads, carried into the scored sets.
HYPOTHESIS_COLUMNS = ("hour", "merchant_new", "foreign")


# Exact TreeSHAP costs about a millisecond a row; reason codes are computed for the highest scores
# of each model on each split, this share of them, which covers every step up and decline at the
# registered budgets with room to spare (DECISIONS.md). The coverage gate checks it on test.
CODE_SHARE = {"payments": 0.05, "logins": 0.15}
CODED = {"payments": ("logistic", "lightgbm"), "logins": ("freeman", "lightgbm", "lightgbm_rep")}


def _codes_schema(desk: str) -> Any:
    from pyspark.sql import types as T

    key = "txn_ref" if desk == "payments" else "login_ref"
    fields = [T.StructField(key, T.LongType())]
    fields += [T.StructField(f"codes_{m}", T.StringType()) for m in CODED[desk]]
    return T.StructType(fields)


def _scored_schema(desk: str, with_codes: bool = True) -> Any:
    from pyspark.sql import types as T

    S, D, L, B, TS = T.StringType(), T.DoubleType(), T.LongType(), T.BooleanType(), T.TimestampType()
    if desk == "payments":
        fields = [("txn_ref", L), ("account_key", S), ("event_ts", TS), ("amount_cents", L)]
        fields += [("channel", T.IntegerType()), ("label", B), ("labelled", B)]
        fields += [(name, D) for name in HYPOTHESIS_COLUMNS]
        models: tuple[str, ...] = ("floor", "logistic", "lightgbm")
    else:
        fields = [("login_ref", L), ("user_key", S), ("event_ts", TS), ("success", B), ("is_attack_ip", B),
                  ("synthetic", B), ("attacker_model", S), ("label", B), ("n_success_prior", L)]  # fmt: skip
        fields += [(f"new_{lvl}", B) for lvl in ("ip", "asn", "country", "city", "device")]
        models = ("floor", "freeman", "lightgbm", "lightgbm_rep")
    for m in models:
        fields += [(f"score_{m}", D)]
        if with_codes or m == "floor":
            fields += [(f"codes_{m}", S)]
    return T.StructType([T.StructField(n, t) for n, t in fields])


def score_step(spark: Any, read: Read, plan: dict[str, Any], models: Path, out: Path, root: Path) -> dict[str, Any]:
    """Score validation and test of both desks with every model, in Spark, and write Parquet sets.

    Scoring runs inside mapInPandas so no split is ever collected to one machine; the model files
    travel to the workers as text in the function's closure.
    """
    from friction_reasons.reasons import feature_to_code, load_table

    table = load_table(root / "evaluation" / "reason_codes.yaml")
    pay_map, log_map = feature_to_code(table, "payments"), feature_to_code(table, "logins")
    folder = models / "default"
    texts = {p.name: p.read_text(encoding="utf-8") for p in folder.iterdir() if p.suffix in {".json", ".txt"}}
    out.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {}

    def with_models(fn: Callable[[pd.DataFrame, Path], pd.DataFrame]) -> Callable[[Any], Any]:
        def run(batches):  # type: ignore[no-untyped-def]
            import tempfile

            with tempfile.TemporaryDirectory() as tmp:
                for name, text in texts.items():
                    (Path(tmp) / name).write_text(text, encoding="utf-8")
                for pdf in batches:
                    yield fn(pdf, Path(tmp))

        return run

    def two_passes(desk: str, frame: Any, key: str, target: Path) -> Any:
        """Scores for every row, then reason codes for the rows above each model's cutoff."""
        scorer = score_payments if desk == "payments" else score_logins
        coder = code_payments if desk == "payments" else code_logins
        mapping = pay_map if desk == "payments" else log_map
        tmp = target.parent / f"_{target.name}_scores"
        frame.mapInPandas(with_models(scorer), _scored_schema(desk, with_codes=False)).write.mode("overwrite").parquet(
            str(tmp)
        )
        scores = spark.read.parquet(str(tmp))
        q = 1.0 - CODE_SHARE[desk]
        cut = {m: float(scores.approxQuantile(f"score_{m}", [q], 0.0005)[0]) for m in CODED[desk]}
        above = scores.where(" OR ".join(f"score_{m} >= {cut[m]!r}" for m in CODED[desk]))
        need = frame.join(above.select(key, *[f"score_{m}" for m in CODED[desk]]), key)
        codes = need.mapInPandas(with_models(lambda pdf, f: coder(pdf, f, mapping, cut)), _codes_schema(desk))
        out_df = scores.join(codes, key, "left")
        for m in CODED[desk]:
            out_df = out_df.withColumn(f"codes_{m}", F.coalesce(F.col(f"codes_{m}"), F.lit("")))
        summary[f"{desk}_{target.name}_code_cutoffs"] = cut
        return out_df

    pay = plan["desks"]["payments"]
    for split in ("validation", "test"):
        s = pay["splits"][split]
        cut = pay["label_cuts"]["validation"] if split == "validation" else None
        frame = training.payments_set(
            read("payments_features"),
            read("payments_silver"),
            read("labels_payments_history"),
            s["start"],
            s["end"],
            cut,
        )
        target = out / f"payments_{split}"
        scored = two_passes("payments", frame, "txn_ref", target)
        if split == "test":
            asof = training.label_as_of(read("labels_payments_history"), pay["label_cuts"]["test_as_of"]).select(
                F.col("event_ref").alias("txn_ref"), F.col("is_fraud").alias("label_as_of_end")
            )
            scored = scored.join(asof, "txn_ref", "left").withColumn(
                "label_as_of_end", F.coalesce(F.col("label_as_of_end"), F.lit(False))
            )
        scored.orderBy("txn_ref").coalesce(1).write.mode("overwrite").parquet(str(target))
        summary[f"payments_{split}"] = str(target)
    lg = plan["desks"]["logins"]
    for split in ("validation", "test"):
        s = lg["splits"][split]
        purpose = "test" if split == "test" else None
        cut = lg["label_cuts"]["validation"] if split == "validation" else None
        lset = training.logins_set(
            read("logins_features"), read("logins_silver"), read("login_attacks"), read("labels_logins_history"),
            s["start"], s["end"], cut, purpose,
        )  # fmt: skip
        target = out / f"logins_{split}"
        scored = two_passes("logins", lset, "login_ref", target)
        scored.orderBy("login_ref").coalesce(4).write.mode("overwrite").parquet(str(target))
        summary[f"logins_{split}"] = str(target)
    (out / "summary.json").write_text(json.dumps(summary, indent=1), encoding="utf-8")
    return summary
