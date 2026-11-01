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
