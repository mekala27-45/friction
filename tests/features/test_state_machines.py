"""The state machines read only the past, survive a snapshot, and agree with a naive recomputation."""

from __future__ import annotations

import math
import random

import pytest
from friction_features import logins, payments


def _payment_events(n: int, seed: int) -> list[payments.PaymentEvent]:
    rng = random.Random(seed)
    ts = 1_500_000_000
    out = []
    for i in range(n):
        ts += rng.choice([30, 600, 3600, 7200, 86_400, 200_000])
        out.append(
            payments.PaymentEvent(
                ts=ts, ref=i, amount_cents=rng.choice([-500, 1299, 2500, 8000, 125_000]),
                merchant_key=f"m{rng.randrange(6)}", mcc=rng.choice([5411, 5812, 4900]), channel=rng.randrange(3),
                location=rng.choice(["NV", "CA", "Mexico"]), foreign=False, has_error=rng.random() < 0.1,
                days_since_issue=float(i), mcc_fraud_rate=0.001,
            )
        )  # fmt: skip
    return out


def test_appending_future_events_changes_no_past_feature() -> None:
    events = _payment_events(300, 1)
    head, _ = payments.run_history(events[:200])
    full, _ = payments.run_history(events)
    assert full[:200] == head


def test_snapshot_restore_gives_the_same_next_features() -> None:
    events = _payment_events(250, 2)
    _, state = payments.run_history(events[:240])
    restored = payments.CardState.restore(state.snapshot())
    a, _ = payments.run_history(events[240:], state)
    b, _ = payments.run_history(events[240:], restored)
    assert a == b


def test_velocity_against_a_naive_count() -> None:
    events = _payment_events(400, 3)
    feats, _ = payments.run_history(events)
    idx = {name: i for i, name in enumerate(payments.FEATURES)}
    for k, ev in enumerate(events):
        prior = events[:k]
        for name, span in (("1h", 3600), ("24h", 86_400), ("7d", 604_800)):
            inside = [p for p in prior if ev.ts - span < p.ts]
            assert feats[k][idx[f"cnt_{name}"]] == len(inside)
            assert feats[k][idx[f"sum_{name}_usd"]] == pytest.approx(sum(p.amount_cents / 100 for p in inside))
        seen = sum(1 for p in prior if p.merchant_key == ev.merchant_key)
        assert feats[k][idx["merchant_seen"]] == pytest.approx(math.log1p(seen))
        assert feats[k][idx["merchant_new"]] == (1.0 if seen == 0 else 0.0)


def test_online_location_rule() -> None:
    online = payments.PaymentEvent(1, 1, 1000, "m", 5411, 2, "", False, False, 1.0, 0.0)
    state = payments.CardState()
    assert state.features(online)[payments.FEATURES.index("location_new")] == 0.0


def _logins(n: int, seed: int, start: int = 1_600_000_000_000) -> list[logins.LoginEvent]:
    rng = random.Random(seed)
    ts = start
    out = []
    for i in range(n):
        ts += rng.choice([1000, 60_000, 3_600_000, 86_400_000])
        values = (f"ip{rng.randrange(4)}", f"as{rng.randrange(2)}", "NO", "Viken", "Oslo", f"ua{rng.randrange(3)}",
                  "Chrome 85", "Windows 10", "desktop")  # fmt: skip
        out.append(logins.LoginEvent(ts, i, values, rng.choice([None, 40.0, 80.0]), rng.random() < 0.8))
    return out


def test_logins_only_successes_build_history_and_synthetic_never_updates() -> None:
    events = _logins(200, 4)
    state = logins.UserState()
    for ev in events:
        state.update(ev)
    assert state.n_success == sum(e.success for e in events)
    attack = logins.LoginEvent(events[-1].ts_ms + 1, 999, events[0].values, 50.0, True, synthetic=True)
    state.features(attack)  # prunes expired failures, which no feature can see
    before = state.snapshot()
    state.update(attack)
    assert state.snapshot() == before


def test_login_counts_against_a_naive_count() -> None:
    events = _logins(150, 5)
    state = logins.UserState()
    idx = {name: i for i, name in enumerate(logins.FEATURES)}
    for k, ev in enumerate(events):
        f = state.features(ev)
        prior_ok = [p for p in events[:k] if p.success]
        assert f[idx["n_success_prior"]] == len(prior_ok)
        assert f[idx["ip_count"]] == sum(1 for p in prior_ok if p.values[0] == ev.values[0])
        assert f[idx["ua_distinct"]] == len({p.values[5] for p in prior_ok})
        recent_fail = [p for p in events[:k] if not p.success and p.ts_ms > ev.ts_ms - 3_600_000]
        assert f[idx["failed_1h_user"]] == len(recent_fail)
        state.update(ev)
    restored = logins.UserState.restore(state.snapshot())
    nxt = _logins(5, 6, start=events[-1].ts_ms)
    assert [state.features(e) for e in nxt] == [restored.features(e) for e in nxt]
