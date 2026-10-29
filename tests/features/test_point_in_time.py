"""The 500 vector recomputation and the planted future event.

Each feature vector the streaming pass produced is recomputed from scratch: a fresh state fed
only the events strictly before the one described, in (time, reference) order. They must be equal
to the last bit. A planted event from the future, inserted anywhere after an event, must change
none of that event's features.
"""

from __future__ import annotations

import random

from friction_features import logins, payments

from tests.features.test_state_machines import _payment_events


def _login_events(n: int, seed: int) -> list[logins.LoginEvent]:
    rng = random.Random(seed)
    ts = 1_600_000_000_000
    out = []
    for i in range(n):
        ts += rng.choice([500, 60_000, 3_600_000, 86_400_000])
        values = (
            f"i{rng.randrange(5)}",
            str(rng.choice([1, 2, 3])),
            rng.choice(["NO", "US"]),
            "",
            "",
            f"u{rng.randrange(3)}",
            "Chrome",
            rng.choice(["Android", "iOS"]),
            rng.choice(["mobile", "desktop"]),
        )
        out.append(logins.LoginEvent(ts, i, values, rng.choice([None, 300.0, 900.0]), rng.random() < 0.8))
    return out


def test_five_hundred_payment_vectors_recomputed_from_scratch() -> None:
    events = _payment_events(900, 11)
    streamed, _ = payments.run_history(events)
    rng = random.Random(5)
    for k in sorted(rng.sample(range(len(events)), 500)):
        state = payments.CardState()
        for p in events[:k]:
            state.features(p)
            state.update(p)
        assert state.features(events[k]) == streamed[k]


def test_five_hundred_login_vectors_recomputed_from_scratch() -> None:
    events = _login_events(800, 12)
    state = logins.UserState()
    streamed = []
    for ev in events:
        streamed.append(state.features(ev))
        state.update(ev)
    rng = random.Random(6)
    for k in sorted(rng.sample(range(len(events)), 500)):
        fresh = logins.UserState()
        for p in events[:k]:
            fresh.features(p)
            fresh.update(p)
        assert fresh.features(events[k]) == streamed[k]


def test_a_planted_future_event_changes_no_feature() -> None:
    events = _payment_events(300, 13)
    before, _ = payments.run_history(events)
    k = 150
    future = payments.PaymentEvent(
        ts=events[-1].ts + 10**7, ref=10**6, amount_cents=999_999, merchant_key="m_future", mcc=7995, channel=2,
        location="", foreign=True, has_error=True, days_since_issue=0.0, mcc_fraud_rate=0.5,
    )  # fmt: skip
    # The state machine is fed in (time, reference) order, so a future event sorts after every
    # real one wherever it was delivered; the features of the first k events cannot see it.
    delivered = sorted([*events[:k], future, *events[k:]], key=lambda e: (e.ts, e.ref))
    after, _ = payments.run_history(delivered)
    assert after[: len(events)] == before
