"""The challengers of evaluation/plan_v2.yaml: the champion's learner with the graph features.

Each challenger is fitted on exactly the rows, sample and weights of the model it challenges (the
same training set function, the same keyed negative sample in steps._collect_training), joined to
the graph feature table on the event key. Only the columns differ, so a difference in capture is the
columns' doing. The v1 steps, files and scored sets are untouched: the challengers write their own
folder (models/v2) and their own scored sets, which hold the key, the score and the reason codes
and nothing else; scripts/grade_v2.py joins them to the v1 sets on the key.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from friction_features.graph import LOGIN_GRAPH, PAYMENT_GRAPH, PAYMENT_STRUCTURE
from friction_transforms import training
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from friction_models import steps
from friction_models.learners import Gbm

TAG = "v2"
MODELS: dict[str, dict[str, list[str]]] = {
    "payments": {
        "lightgbm_graph": [*steps.PAY_FEATURES, *PAYMENT_GRAPH],
        "lightgbm_structure": [*steps.PAY_FEATURES, *PAYMENT_STRUCTURE],
    },
    "logins": {"lightgbm_graph": [*steps.LOGIN_FEATURES, *LOGIN_GRAPH]},
}
KEY = {"payments": "txn_ref", "logins": "login_ref"}
GRAPH_TABLE = {"payments": "payments_graph_features", "logins": "logins_graph_features"}


def file_name(desk: str, model: str) -> str:
    return f"{desk}_{model}.txt"


def mappings(root: Path) -> dict[str, dict[str, str]]:
    """Feature to code for both desks, the v1 table and the graph addendum together."""
    from friction_reasons.reasons import feature_to_code, load_table

    out = {}
    for desk in ("payments", "logins"):
        merged = {**load_table(root / "evaluation" / "reason_codes.yaml")[desk]}
        extra = load_table(root / "evaluation" / "reason_codes_v2.yaml")[desk]
        if set(merged) & set(extra):
            raise ValueError(f"the graph codes of {desk} reuse a v1 code")
        merged.update(extra)
        out[desk] = feature_to_code({desk: merged}, desk)
    return out


def _payments_frame(read: steps.Read, start: str, end: str, cut: str | None) -> DataFrame:
    frame = training.payments_set(
        read("payments_features"), read("payments_silver"), read("labels_payments_history"), start, end, cut
    )
    return frame.join(read(GRAPH_TABLE["payments"]), "txn_ref")


def _logins_frame(read: steps.Read, start: str, end: str, cut: str | None, purpose: str | None) -> DataFrame:
    frame = training.logins_set(
        read("logins_features"), read("logins_silver"), read("login_attacks"), read("labels_logins_history"),
        start, end, cut, purpose,
    )  # fmt: skip
    return frame.join(read(GRAPH_TABLE["logins"]), "login_ref")


def _importance(model: Gbm, names: list[str]) -> dict[str, float]:
    gain = np.asarray(model.booster.feature_importance(importance_type="gain"), dtype=np.float64)
    total = float(gain.sum()) or 1.0
    order = np.argsort(-gain, kind="stable")
    return {names[i]: round(float(gain[i] / total), 6) for i in order}


def train_step(spark: Any, read: steps.Read, plan: dict[str, Any], out: Path) -> dict[str, Any]:
    """Fit every challenger on its champion's training set; write them under out/v2/."""
    folder = out / TAG
    folder.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {"tag": TAG}
    pay = plan["desks"]["payments"]
    train = pay["splits"]["train"]
    start = str((pd.Timestamp(train["end"]) - pd.DateOffset(months=24)).date())
    pdf = steps._collect_training(_payments_frame(read, start, train["end"], pay["label_cuts"]["train"]),
                                  "txn_ref")  # fmt: skip
    lg = plan["desks"]["logins"]
    lt = lg["splits"]["train"]
    lpdf = steps._collect_training(_logins_frame(read, lt["start"], lt["end"], lg["label_cuts"]["train"], "train"),
                                   "login_ref")  # fmt: skip
    for desk, frame in (("payments", pdf), ("logins", lpdf)):
        y = frame["label"].to_numpy(bool).astype(int)
        w = frame["weight"].to_numpy(np.float64)
        entry: dict[str, Any] = {"rows": len(frame), "positives": int(y.sum()), "models": {}}
        for name, cols in MODELS[desk].items():
            model = Gbm.fit(frame[cols].astype(float).to_numpy(np.float64), y, cols, weight=w)
            (folder / file_name(desk, name)).write_text(model.to_text(), encoding="utf-8")
            entry["models"][name] = {"features": len(cols), "gain_share": _importance(model, cols)}
        summary[desk] = entry
    (folder / "summary.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    return summary


def score_frame(pdf: pd.DataFrame, folder: Path, desk: str) -> pd.DataFrame:
    """The key and every challenger's score."""
    out = pdf[[KEY[desk]]].copy()
    for name, cols in MODELS[desk].items():
        model = Gbm.from_text((folder / file_name(desk, name)).read_text(encoding="utf-8"))
        out[f"score_{name}"] = model.predict(pdf[cols].astype(float).to_numpy(np.float64))
    return out


def code_frame(
    pdf: pd.DataFrame, folder: Path, desk: str, mapping: dict[str, str], cut: dict[str, float]
) -> pd.DataFrame:
    """Reason codes for the rows at or above each challenger's cutoff; empty below it."""
    out = pdf[[KEY[desk]]].copy()
    for name, cols in MODELS[desk].items():
        model = Gbm.from_text((folder / file_name(desk, name)).read_text(encoding="utf-8"))
        x = pdf[cols].astype(float).to_numpy(np.float64)
        codes = [""] * len(pdf)
        rows = np.nonzero(pdf[f"score_{name}"].to_numpy(np.float64) >= cut[name])[0]
        if len(rows):
            for i, c in zip(rows, steps._codes(model.contributions(x[rows]), cols, mapping), strict=True):
                codes[i] = c
        out[f"codes_{name}"] = codes
    return out


def _schema(desk: str, with_codes: bool) -> Any:
    from pyspark.sql import types as T

    fields = [T.StructField(KEY[desk], T.LongType())]
    for name in MODELS[desk]:
        if not with_codes:
            fields.append(T.StructField(f"score_{name}", T.DoubleType()))
        else:
            fields.append(T.StructField(f"codes_{name}", T.StringType()))
    return T.StructType(fields)


def score_step(
    spark: Any, read: steps.Read, plan: dict[str, Any], models: Path, out: Path, root: Path
) -> dict[str, Any]:
    """Score validation and test of both desks with every challenger, as steps.score_step does."""
    maps = mappings(root)
    folder = models / TAG
    texts = {p.name: p.read_text(encoding="utf-8") for p in folder.iterdir() if p.suffix == ".txt"}
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

    def two_passes(desk: str, frame: DataFrame, target: Path) -> DataFrame:
        key = KEY[desk]
        tmp = target.parent / f"_{target.name}_scores"
        scorer = with_models(lambda pdf, f: score_frame(pdf, f, desk))
        frame.mapInPandas(scorer, _schema(desk, with_codes=False)).write.mode("overwrite").parquet(str(tmp))
        scores: DataFrame = spark.read.parquet(str(tmp))
        q = 1.0 - steps.CODE_SHARE[desk]
        cut = {m: float(scores.approxQuantile(f"score_{m}", [q], 0.0005)[0]) for m in MODELS[desk]}
        above = scores.where(" OR ".join(f"score_{m} >= {cut[m]!r}" for m in MODELS[desk]))
        need = frame.join(above, key)
        coder = with_models(lambda pdf, f: code_frame(pdf, f, desk, maps[desk], cut))
        codes = need.mapInPandas(coder, _schema(desk, with_codes=True))
        joined = scores.join(codes, key, "left")
        for m in MODELS[desk]:
            joined = joined.withColumn(f"codes_{m}", F.coalesce(F.col(f"codes_{m}"), F.lit("")))
        summary[f"{target.name}_code_cutoffs"] = cut
        return joined

    pay = plan["desks"]["payments"]
    lg = plan["desks"]["logins"]
    for split in ("validation", "test"):
        s = pay["splits"][split]
        cut = pay["label_cuts"]["validation"] if split == "validation" else None
        target = out / f"payments_{split}"
        scored = two_passes("payments", _payments_frame(read, s["start"], s["end"], cut), target)
        scored.orderBy("txn_ref").coalesce(1).write.mode("overwrite").parquet(str(target))
        summary[f"payments_{split}"] = str(target)
        s = lg["splits"][split]
        cut = lg["label_cuts"]["validation"] if split == "validation" else None
        purpose = "test" if split == "test" else None
        target = out / f"logins_{split}"
        scored = two_passes("logins", _logins_frame(read, s["start"], s["end"], cut, purpose), target)
        scored.orderBy("login_ref").coalesce(4).write.mode("overwrite").parquet(str(target))
        summary[f"logins_{split}"] = str(target)
    (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    return summary
