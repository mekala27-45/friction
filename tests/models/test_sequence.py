"""The sequence challenger: lags read only earlier events, the two forward passes agree, it learns a
pattern only a sequence holds, and occlusion names the attribute that moved the score."""

from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from friction_models import sequence as sq

ROOT = Path(__file__).resolve().parents[2]
HAS_JAX = importlib.util.find_spec("jax") is not None


def _rows(n: int, rng: np.random.Generator) -> pd.DataFrame:
    data = {c: rng.normal(size=n) for c in sq.STEP}
    data["hour"] = rng.integers(0, 24, n).astype(float)
    data["channel"] = rng.integers(0, 3, n).astype(float)
    data["amount_usd"] = rng.lognormal(3, 1, n)
    for c in ("is_refund", "merchant_new", "foreign"):
        data[c] = (rng.random(n) < 0.2).astype(float)
    return pd.DataFrame(data)


def _sequences(n: int, seed: int = 0) -> tuple[pd.DataFrame, np.ndarray]:
    """A fraud is a burst: several of the earlier events abroad. The event itself looks ordinary."""
    rng = np.random.default_rng(seed)
    frame = _rows(n, rng)
    y = rng.random(n) < 0.3
    cols = {}
    for k in range(sq.LAGS, 0, -1):
        lag = _rows(n, rng)
        lag["foreign"] = np.where(y & (k <= 5), 1.0, 0.0)
        missing = rng.random(n) < 0.1 * k / sq.LAGS
        for c in sq.STEP:
            cols[f"{c}__{k}"] = np.where(missing, np.nan, lag[c])
    return pd.concat([frame, pd.DataFrame(cols)], axis=1), y.astype(int)


def test_steps_put_the_event_last_and_mask_missing_lags() -> None:
    frame, _ = _sequences(50)
    seq = sq.steps(frame)
    assert seq.shape == (50, sq.LAGS + 1, sq.WIDTH)
    assert (seq[:, -1, -1] == 1).all()
    missing = np.isnan(frame[f"amount_usd__{sq.LAGS}"].to_numpy())
    assert (seq[missing, 0, :] == 0).all() and (seq[~missing, 0, -1] == 1).all()
    assert sum(s.stop - s.start for s in sq.attribute_slices().values()) == sq.WIDTH - 1


def test_a_missing_step_leaves_the_state_as_it_was() -> None:
    p = sq.init()
    frame, _ = _sequences(20)
    seq = sq.steps(frame)
    blank = seq.copy()
    blank[:, :5, :] = 0.0  # the five oldest steps absent
    trimmed = seq.copy()
    trimmed[:, :5, :] = 0.0
    trimmed[:, 2, :3] = 7.0  # values on an absent step change nothing
    trimmed[:, 2, -1] = 0.0
    assert np.allclose(sq.logits(p, blank), sq.logits(p, trimmed))


@pytest.mark.skipif(not HAS_JAX, reason="needs the sequence extra (jax, optax)")
def test_it_learns_a_burst_only_the_sequence_shows_and_occlusion_names_it() -> None:
    import jax.numpy as jnp

    frame, y = _sequences(8192, seed=1)
    seq = sq.steps(frame)
    params = sq.fit(seq, y, np.ones(len(y)), epochs=8, log=lambda _: None, batch=256)
    test, ty = _sequences(2000, seed=2)
    tseq = sq.steps(test)
    s = sq.predict(params, tseq)
    assert s[ty == 1].mean() > s[ty == 0].mean() + 0.3
    # The NumPy forward pass the scorer runs equals the one JAX trained.
    jp = {k: jnp.asarray(v) for k, v in params.items()}
    h = jnp.zeros((len(tseq), sq.HIDDEN), jnp.float32)
    for t in range(tseq.shape[1]):
        h = sq._cell(jp, h, jnp.asarray(tseq[:, t, :]), jnp)
    assert np.allclose(np.asarray(h @ jp["wo"] + jp["bo"]), sq.logits(params, tseq), atol=1e-4)
    means = sq.training_means(seq)
    top = sq.contributions(params, means, tseq[ty == 1][:200]).mean(axis=0)
    assert list(sq.ATTRIBUTES)[int(np.argmax(top))] == "foreign"
    p2, m2 = sq.from_json(sq.to_json(params, means))
    assert np.array_equal(sq.logits(p2, tseq), sq.logits(params, tseq)) and np.array_equal(m2, means)


def test_scoring_and_codes_frames_carry_the_key_and_codes_above_the_cut() -> None:
    from friction_reasons.reasons import feature_to_code, load_table

    frame, _ = _sequences(300, seed=3)
    frame["txn_ref"] = np.arange(300)
    text = sq.to_json(sq.init(), np.zeros(sq.WIDTH, np.float32))
    scored = sq.score_frame(frame, text)
    both = frame.merge(scored, on="txn_ref")
    cut = float(np.quantile(both["score_gru"], 0.9))
    mapping = sq.code_map(feature_to_code(load_table(ROOT / "evaluation" / "reason_codes.yaml"), "payments"))
    coded = sq.code_frame(both, text, mapping, cut)
    above = both["score_gru"].to_numpy() >= cut
    assert not any(coded["codes_gru"].to_numpy()[~above])
    assert set(mapping.values()) <= {f"P{i:02d}" for i in range(1, 13)}


@pytest.mark.spark
def test_lags_read_only_earlier_events_of_the_same_card(spark) -> None:  # type: ignore[no-untyped-def]
    t0 = datetime(2019, 1, 1)
    silver = [{"txn_ref": i, "account_key": "a" if i % 2 else "b", "event_ts": t0 + timedelta(hours=i // 2)}
              for i in range(60)]  # fmt: skip
    features = [{"txn_ref": i, **{c: float(i) for c in sq.STEP}} for i in range(60)]
    got = {r["txn_ref"]: r.asDict() for r in sq.lagged(spark.createDataFrame(features), spark.createDataFrame(silver))
           .collect()}  # fmt: skip
    for i in range(60):
        same = [j for j in range(60) if j % 2 == i % 2 and j < i]
        for k in range(1, sq.LAGS + 1):
            want = float(same[-k]) if len(same) >= k else None
            assert got[i][f"amount_usd__{k}"] == want, (i, k)


@pytest.mark.spark
def test_the_sample_through_parquet_is_the_champions_sample(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    from friction_models import steps as v1

    rng = np.random.default_rng(3)
    frame, y = _sequences(3000, seed=3)
    frame["txn_ref"] = rng.permutation(len(frame)).astype(np.int64)
    frame["label"] = y.astype(bool)
    frame["unused"] = 1.0
    rows = spark.createDataFrame(frame)
    old, n = v1.NEGATIVES_FOR_TRAINING, int((~frame["label"]).sum())
    v1.NEGATIVES_FOR_TRAINING = n // 2  # a real sample, not every row
    try:
        got = sq.collect(rows, tmp_path / "spill")
        want = v1._collect_training(rows.drop("unused"), "txn_ref")
    finally:
        v1.NEGATIVES_FOR_TRAINING = old
    assert not (tmp_path / "spill").exists()
    assert 0 < len(got) < len(frame) and "unused" not in got
    pd.testing.assert_frame_equal(got, want[got.columns], check_dtype=False)
    assert np.array_equal(sq.steps(got), sq.steps(want))


def test_one_row_built_from_a_history_is_the_row_the_lake_scores() -> None:
    from friction_features import payments as payment_features

    rng = np.random.default_rng(5)
    names = list(payment_features.FEATURES)
    n = 60
    frame = _rows(n, rng)
    held = rng.integers(0, sq.LAGS + 1, n)  # how many earlier events each card has, the most recent ones
    for k in range(sq.LAGS, 0, -1):
        lag = _rows(n, rng)
        for c in sq.STEP:  # the lag columns are Spark floats
            frame[f"{c}__{k}"] = np.where(k <= held, lag[c].to_numpy(np.float32).astype(np.float64), np.nan)
    whole = sq.steps(frame)
    p = sq.init()
    batch = sq.predict(p, whole)
    for r in range(n):
        x = [0.0] * len(names)
        for c in sq.STEP:
            x[names.index(c)] = float(frame[c].iloc[r])
        history = [[float(frame[f"{c}__{k}"].iloc[r]) for c in sq.STEP] for k in range(sq.LAGS, 0, -1) if k <= held[r]]
        one = sq.row_steps(x, names, history)
        assert np.array_equal(one[0], whole[r]), r
        assert abs(float(sq.predict(p, one)[0]) - float(batch[r])) < 1e-6
        assert sq.step_values(x, names) == [float(np.float32(frame[c].iloc[r])) for c in sq.STEP]
    longer = [[1.0] * len(sq.STEP)] * (sq.LAGS + 5)
    assert np.array_equal(sq.row_steps(x, names, longer), sq.row_steps(x, names, longer[-sq.LAGS :]))


@pytest.mark.spark
def test_the_seeded_history_and_the_online_appends_rebuild_the_lake_lags(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """The parity set's history, then each event's values appended as the API appends them, gives
    every event of the window the lag columns the lake gave it: the scores agree, one row against a
    batch, and so do the verdicts."""
    import json

    from friction_core.hashing import sha256_bytes
    from friction_features import payments as payment_features
    from pyspark.sql import functions as F

    from scripts import export_sequence_parity as esp

    lake = ROOT / "data" / "lake" / "fixture" / "spark-warehouse"
    if not (lake / "payments_features").exists():
        pytest.skip("needs the fixture lake: scripts/run_portable.py then scripts/run_features.py")

    def read(name: str):  # type: ignore[no-untyped-def]
        return spark.read.parquet(str(lake / name))

    from friction_models import steps as v1

    start = v1.load_plan(ROOT)["desks"]["payments"]["splits"]["test"]["start"]
    since = F.unix_timestamp(F.to_timestamp(F.lit(start)))
    first = read("payment_feature_inputs").where(F.col("event_ts_s") >= since).orderBy("event_ts_s", "txn_ref")
    window = first.limit(300).select("txn_ref", "account_key", "channel").collect()
    events = [{"event_ref": int(r["txn_ref"]), "account_key": r["account_key"], "channel": int(r["channel"])}
              for r in window]  # fmt: skip
    p = sq.init()
    p["bo"] = p["bo"] - 1.0
    shadow = tmp_path / "shadow"
    shadow.mkdir()
    gru = sq.to_json(p, np.zeros(sq.WIDTH, np.float32)).encode()
    (shadow / "payments_gru.json").write_bytes(gru)
    from friction_reasons.reasons import feature_to_code, load_table

    reasons = feature_to_code(load_table(ROOT / "evaluation" / "reason_codes.yaml"), "payments")
    meta = {"desk": "payments", "model": "gru", "status": "shadow", "thresholds": {"step_up": 0.3, "decline": 0.6},
            "files": {"payments_gru.json": sha256_bytes(gru)}, "reason_map": reasons}  # fmt: skip
    (shadow / "bundle.json").write_text(json.dumps(meta), encoding="utf-8")
    out = esp.build(read, spark, {"events": events}, shadow)
    assert out["cards"] == len({e["account_key"] for e in events}) and len(out["expected"]) == len(events)
    assert out["cards"] < len(events)  # some card comes back within the window, so the appends are tested
    names = list(payment_features.FEATURES)
    refs = spark.createDataFrame([(e["event_ref"],) for e in events], "txn_ref long")
    vectors = {int(r["txn_ref"]): [float(r[c]) for c in names] for r in read("payments_features").join(refs, "txn_ref")
               .collect()}  # fmt: skip
    history = {k: list(v["steps"]) for k, v in out["snapshot"]["card_steps"].items()}
    worst = 0.0
    for e, want in zip(events, out["expected"], strict=True):
        x = vectors[e["event_ref"]]
        got = float(sq.predict(p, sq.row_steps(x, names, history[e["account_key"]]))[0])
        worst = max(worst, abs(got - want["score"]))
        history[e["account_key"]] = [*history[e["account_key"]], sq.step_values(x, names)][-sq.LAGS :]
    assert worst < 1e-6
    assert any(len(v["steps"]) == sq.LAGS for v in out["snapshot"]["card_steps"].values())
