"""The sequence challenger of evaluation/plan_v3.yaml: a GRU over a card's last 20 events and this one.

Each step is fourteen numbers derived from ten point in time features of one event (steps()); the
lakehouse carries the ten features of the 20 events before each one as lag columns, which only ever
read earlier rows of the card (lagged()). Training runs in JAX on the build machine or the runner
and is written as plain arrays; scoring is a NumPy forward pass of the same network, so the scoring
path needs neither JAX nor a GPU, and tests check the two forward passes agree. Reason codes come
from occlusion: each attribute set to its training mean over every step, the drop in the logit is its
contribution, mapped to the v1 code of the feature it comes from.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd

F32 = npt.NDArray[np.float32]
LAGS = 20
SINCE = "2015-11-01"
SEED = 20261029
HIDDEN = 32
EPOCHS = 4
BATCH = 2048
LEARNING_RATE = 0.003
# The ten features each step reads, and the v1 reason code each attribute maps to.
STEP = ("amount_usd", "is_refund", "hour", "channel", "log_secs_since_prev", "merchant_new", "mcc_fraud_rate",
        "foreign", "errors_24h", "amount_z")  # fmt: skip
ATTRIBUTES = {"amount": ("amount_usd", 1), "refund": ("is_refund", 1), "hour": ("hour", 2), "channel": ("channel", 3),
              "gap": ("log_secs_since_prev", 1), "merchant_new": ("merchant_new", 1),
              "mcc_rate": ("mcc_fraud_rate", 1), "foreign": ("foreign", 1), "errors": ("errors_24h", 1),
              "amount_z": ("amount_z", 1)}  # fmt: skip
WIDTH = sum(n for _, n in ATTRIBUTES.values()) + 1  # the last number says the step is present


def lag_columns() -> list[str]:
    return [f"{c}__{k}" for k in range(LAGS, 0, -1) for c in STEP]


def lagged(features: Any, silver: Any) -> Any:
    """txn_ref and the ten step features of each of the card's 20 earlier events, as lag columns."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    hist = (
        silver.where(F.col("event_ts") >= F.lit(SINCE))
        .select("txn_ref", "account_key", "event_ts")
        .join(features.select("txn_ref", *STEP), "txn_ref")
    )
    w = Window.partitionBy("account_key").orderBy("event_ts", "txn_ref")
    cols = [F.lag(F.col(c).cast("float"), k).over(w).alias(f"{c}__{k}") for k in range(LAGS, 0, -1) for c in STEP]
    return hist.select("txn_ref", *cols)


def _one(values: dict[str, npt.NDArray[np.float64]], present: npt.NDArray[np.bool_]) -> F32:
    """One step's fourteen numbers and its presence flag, for every row; zero where absent."""
    hour = values["hour"]
    channel = values["channel"].astype(int)
    parts = [
        np.log1p(np.clip(values["amount_usd"], 0, None)) / 10.0,
        values["is_refund"],
        np.sin(2 * np.pi * hour / 24.0), np.cos(2 * np.pi * hour / 24.0),
        channel == 0, channel == 1, channel == 2,
        np.clip(values["log_secs_since_prev"], -1, 20) / 20.0,
        values["merchant_new"],
        np.clip(values["mcc_fraud_rate"], 0, 1) * 100.0,
        values["foreign"],
        np.clip(values["errors_24h"], 0, 5) / 5.0,
        np.clip(values["amount_z"], -5, 5) / 5.0,
    ]  # fmt: skip
    out = np.stack([np.asarray(p, dtype=np.float32) for p in parts] + [present.astype(np.float32)], axis=-1)
    out[~present] = 0.0
    return out


def steps(frame: pd.DataFrame) -> F32:
    """rows x 21 x WIDTH: the 20 lags oldest first, then the event itself."""
    seq = []
    for k in range(LAGS, 0, -1):
        cols = {c: frame[f"{c}__{k}"].to_numpy(np.float64) for c in STEP}
        present = ~np.isnan(cols["amount_usd"])
        seq.append(_one({c: np.nan_to_num(v) for c, v in cols.items()}, present))
    now = {c: frame[c].to_numpy(np.float64) for c in STEP}
    seq.append(_one(now, np.ones(len(frame), dtype=bool)))
    return np.stack(seq, axis=1)


def attribute_slices() -> dict[str, slice]:
    out, i = {}, 0
    for name, (_, n) in ATTRIBUTES.items():
        out[name] = slice(i, i + n)
        i += n
    return out


# ------------------------------------------------------------------------------------------------
# The network: plain arrays, a NumPy forward pass, and JAX only to fit them


def init(seed: int = SEED) -> dict[str, F32]:
    rng = np.random.default_rng(seed)
    scale = 1.0 / np.sqrt(HIDDEN)
    shape_x, shape_h = (WIDTH, 3 * HIDDEN), (HIDDEN, 3 * HIDDEN)
    return {
        "wx": rng.uniform(-scale, scale, shape_x).astype(np.float32),
        "wh": rng.uniform(-scale, scale, shape_h).astype(np.float32),
        "b": np.zeros(3 * HIDDEN, np.float32),
        "wo": rng.uniform(-scale, scale, (HIDDEN,)).astype(np.float32),
        "bo": np.zeros((), np.float32),
    }


def _sigmoid(z: Any, xp: Any) -> Any:
    return 1.0 / (1.0 + xp.exp(-z))


def _cell(p: dict[str, Any], h: Any, x: Any, xp: Any) -> Any:
    """A GRU step; a step marked absent leaves the state as it was."""
    gx = x @ p["wx"] + p["b"]
    gh = h @ p["wh"]
    n = HIDDEN
    r = _sigmoid(gx[:, :n] + gh[:, :n], xp)
    z = _sigmoid(gx[:, n : 2 * n] + gh[:, n : 2 * n], xp)
    c = xp.tanh(gx[:, 2 * n :] + r * gh[:, 2 * n :])
    new = (1 - z) * c + z * h
    present = x[:, -1:]
    return present * new + (1 - present) * h


def logits(p: dict[str, F32], seq: F32) -> npt.NDArray[np.float64]:
    h = np.zeros((seq.shape[0], HIDDEN), np.float32)
    for t in range(seq.shape[1]):
        h = _cell(p, h, seq[:, t, :], np)
    return np.asarray(h @ p["wo"] + p["bo"], dtype=np.float64)


def predict(p: dict[str, F32], seq: F32) -> npt.NDArray[np.float64]:
    return np.asarray(1.0 / (1.0 + np.exp(-logits(p, seq))), dtype=np.float64)


def fit(seq: F32, y: npt.NDArray[np.int_], weight: npt.NDArray[np.float64], epochs: int = EPOCHS,
        log: Callable[[str], None] = print, batch: int = BATCH) -> dict[str, F32]:  # fmt: skip
    """Weighted cross entropy, Adam, fixed passes and a fixed shuffle order; nothing read from any other split."""
    import jax
    import jax.numpy as jnp
    import optax

    params = {k: jnp.asarray(v) for k, v in init().items()}
    opt = optax.adam(LEARNING_RATE)
    state = opt.init(params)
    w = weight / weight.mean()

    def loss(p: Any, x: Any, t: Any, wt: Any) -> Any:
        h = jnp.zeros((x.shape[0], HIDDEN), jnp.float32)
        h, _ = jax.lax.scan(lambda h, xt: (_cell(p, h, xt, jnp), None), h, jnp.swapaxes(x, 0, 1))
        z = h @ p["wo"] + p["bo"]
        return jnp.sum(wt * (jnp.logaddexp(0.0, z) - t * z)) / jnp.sum(wt)

    @jax.jit
    def step(p: Any, s: Any, x: Any, t: Any, wt: Any) -> Any:
        value, grads = jax.value_and_grad(loss)(p, x, t, wt)
        updates, s = opt.update(grads, s, p)
        return optax.apply_updates(p, updates), s, value

    rng = np.random.default_rng(SEED)
    n = len(y)
    for epoch in range(epochs):
        order = rng.permutation(n)
        total = 0.0
        for i in range(0, n - batch + 1, batch):  # full batches only, one compiled shape
            b = order[i : i + batch]
            params, state, value = step(params, state, seq[b], y[b].astype(np.float32), w[b].astype(np.float32))
            total += float(value)
        log(f"gru epoch {epoch + 1}: mean loss {total / max(1, n // batch):.6f}")
    return {k: np.asarray(v, dtype=np.float32) for k, v in params.items()}


def to_json(p: dict[str, F32], means: F32) -> str:
    return json.dumps({"params": {k: v.tolist() for k, v in p.items()}, "means": means.tolist(),
                       "hidden": HIDDEN, "lags": LAGS, "step": list(STEP)})  # fmt: skip


def from_json(text: str) -> tuple[dict[str, F32], F32]:
    d = json.loads(text)
    return {k: np.asarray(v, dtype=np.float32) for k, v in d["params"].items()}, np.asarray(d["means"], np.float32)


def training_means(seq: F32) -> F32:
    """Each number's mean over present steps of the training rows: the occlusion baseline."""
    present = seq[:, :, -1] > 0
    return np.asarray(seq[present].mean(axis=0), dtype=np.float32)


def contributions(p: dict[str, F32], means: F32, seq: F32) -> npt.NDArray[np.float64]:
    """rows x attributes: the drop in the logit when the attribute is set to its mean on present steps."""
    base = logits(p, seq)
    present = seq[:, :, -1:] > 0
    out = []
    for sl in attribute_slices().values():
        occluded = seq.copy()
        occluded[:, :, sl] = np.where(present, means[sl], occluded[:, :, sl])
        out.append(base - logits(p, occluded))
    return np.stack(out, axis=1)


def code_map(reason_map: dict[str, str]) -> dict[str, str]:
    """Each attribute to the v1 code of the feature it comes from."""
    return {name: reason_map[feature] for name, (feature, _) in ATTRIBUTES.items()}


def save(folder: Path, p: dict[str, F32], means: F32) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "payments_gru.json"
    path.write_text(to_json(p, means), encoding="utf-8")
    return path


# ------------------------------------------------------------------------------------------------
# The steps: train on the champion's rows, score validation and test, as friction_models.challengers


TAG = "v3"
LAG_TABLE = "payments_lags"


def frame(read: Callable[[str], Any], start: str, end: str, cut: str | None) -> Any:
    from friction_transforms import training

    base = training.payments_set(read("payments_features"), read("payments_silver"), read("labels_payments_history"),
                                 start, end, cut)  # fmt: skip
    return base.join(read(LAG_TABLE), "txn_ref")


def train_step(read: Callable[[str], Any], plan: dict[str, Any], out: Path) -> dict[str, Any]:
    from friction_models import steps as v1

    pay = plan["desks"]["payments"]
    train = pay["splits"]["train"]
    start = str((pd.Timestamp(train["end"]) - pd.DateOffset(months=24)).date())
    pdf = v1._collect_training(frame(read, start, train["end"], pay["label_cuts"]["train"]), "txn_ref")
    seq = steps(pdf)
    y = pdf["label"].to_numpy(bool).astype(int)
    w = pdf["weight"].to_numpy(np.float64)
    params = fit(seq, y, w)
    means = training_means(seq)
    path = save(out / TAG, params, means)
    z = logits(params, seq[:50_000])
    loss = float(np.average(np.logaddexp(0, z) - y[:50_000] * z, weights=w[:50_000]))
    summary = {"tag": TAG, "rows": len(pdf), "positives": int(y.sum()), "file": path.name, "train_logloss": loss}
    (out / TAG / "summary.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    return summary


def score_frame(pdf: pd.DataFrame, text: str) -> pd.DataFrame:
    params, _ = from_json(text)
    out = pdf[["txn_ref"]].copy()
    out["score_gru"] = predict(params, steps(pdf)) if len(pdf) else np.zeros(0)
    return out


def code_frame(pdf: pd.DataFrame, text: str, mapping: dict[str, str], cut: float) -> pd.DataFrame:
    from friction_reasons.reasons import top_codes

    params, means = from_json(text)
    out = pdf[["txn_ref"]].copy()
    codes = [""] * len(pdf)
    rows = np.nonzero(pdf["score_gru"].to_numpy(np.float64) >= cut)[0]
    if len(rows):
        contrib = contributions(params, means, steps(pdf.iloc[rows]))
        names = list(ATTRIBUTES)
        for i, row in zip(rows, contrib, strict=True):
            codes[i] = ",".join(top_codes(row, names, mapping))
    out["codes_gru"] = codes
    return out


def score_step(spark: Any, read: Callable[[str], Any], plan: dict[str, Any], models: Path, out: Path,
               root: Path) -> dict[str, Any]:  # fmt: skip
    from friction_reasons.reasons import feature_to_code, load_table
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from friction_models import steps as v1

    mapping = code_map(feature_to_code(load_table(root / "evaluation" / "reason_codes.yaml"), "payments"))
    text = (models / TAG / "payments_gru.json").read_text(encoding="utf-8")
    scores_schema = T.StructType([T.StructField("txn_ref", T.LongType()), T.StructField("score_gru", T.DoubleType())])
    codes_schema = T.StructType([T.StructField("txn_ref", T.LongType()), T.StructField("codes_gru", T.StringType())])
    out.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {}
    pay = plan["desks"]["payments"]
    for split in ("validation", "test"):
        s = pay["splits"][split]
        cut_label = pay["label_cuts"]["validation"] if split == "validation" else None
        rows = frame(read, s["start"], s["end"], cut_label)
        tmp = out / f"_payments_{split}_scores"

        def scorer(batches):  # type: ignore[no-untyped-def]
            for pdf in batches:
                yield score_frame(pdf, text)

        rows.mapInPandas(scorer, scores_schema).write.mode("overwrite").parquet(str(tmp))
        scores = spark.read.parquet(str(tmp))
        cut = float(scores.approxQuantile("score_gru", [1.0 - v1.CODE_SHARE["payments"]], 0.0005)[0])

        def coder(batches, cut: float = cut):  # type: ignore[no-untyped-def]
            for pdf in batches:
                yield code_frame(pdf, text, mapping, cut)

        need = rows.join(scores.where(F.col("score_gru") >= cut), "txn_ref")
        codes = need.mapInPandas(coder, codes_schema)
        scored = scores.join(codes, "txn_ref", "left").withColumn("codes_gru", F.coalesce("codes_gru", F.lit("")))
        target = out / f"payments_{split}"
        scored.orderBy("txn_ref").coalesce(1).write.mode("overwrite").parquet(str(target))
        summary[f"payments_{split}"] = str(target)
        summary[f"payments_{split}_code_cutoff"] = cut
    (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    return summary
