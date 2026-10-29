"""Payments features as one state machine per card account.

The same class computes features in the lakehouse (run over a card's history in event order inside
``applyInPandas``) and in the scoring API (one event at a time against the online store). Every
feature reads only the state built from events strictly before the one it describes, in the order
(event time, transaction reference); ``features()`` never looks at the event's own outcome and
``update()`` is called only after it. tests/features/test_point_in_time.py recomputes sampled
vectors from raw events with an independent implementation and asserts equality.

Two inputs come from outside the card's own history and are carried on the event: the days since
the card was issued or reissued (from the card SCD2 history, as of the event) and the merchant
category's fraud rate from labels known as of the event's day.
"""

from __future__ import annotations

import math
from collections import Counter, deque
from dataclasses import dataclass
from typing import Any

CHANNELS = {"Swipe Transaction": 0, "Chip Transaction": 1, "Online Transaction": 2}
WINDOWS = (("1h", 3_600), ("24h", 86_400), ("7d", 604_800))
MEDIAN_SPAN = 50

FEATURES: tuple[str, ...] = (
    "amount_usd",
    "is_refund",
    "log_amount_to_median",
    "amount_z",
    "log_n_prior",
    "cnt_1h",
    "sum_1h_usd",
    "cnt_24h",
    "sum_24h_usd",
    "cnt_7d",
    "sum_7d_usd",
    "log_secs_since_prev",
    "merchant_seen",
    "merchant_new",
    "mcc_new",
    "channel",
    "channel_new",
    "location_new",
    "foreign",
    "errors_24h",
    "hour",
    "hour_share",
    "days_since_issue",
    "mcc_fraud_rate",
)


@dataclass(frozen=True, slots=True)
class PaymentEvent:
    ts: int  # seconds since the epoch, UTC by convention (the source has no zone)
    ref: int  # the transaction reference, the tie breaker within a minute
    amount_cents: int
    merchant_key: str
    mcc: int
    channel: int
    location: str  # merchant state, or country when outside the US, or "" online
    foreign: bool
    has_error: bool
    days_since_issue: float
    mcc_fraud_rate: float


class CardState:
    """What the issuer knows about one card account from the events before now."""

    __slots__ = (
        "channels",
        "errors",
        "hours",
        "last50",
        "last_ts",
        "locations",
        "mccs",
        "mean",
        "merchants",
        "n",
        "n_debit",
        "recent",
        "sq",
    )

    def __init__(self) -> None:
        self.n = 0
        self.n_debit = 0
        self.mean = 0.0
        self.sq = 0.0
        self.last_ts: int | None = None
        self.recent: deque[tuple[int, float]] = deque()
        self.errors: deque[int] = deque()
        self.last50: deque[float] = deque(maxlen=MEDIAN_SPAN)
        self.merchants: Counter[str] = Counter()
        self.mccs: set[int] = set()
        self.channels: set[int] = set()
        self.locations: set[str] = set()
        self.hours = [0] * 24

    def _expire(self, ts: int) -> None:
        horizon = ts - WINDOWS[-1][1]
        while self.recent and self.recent[0][0] <= horizon:
            self.recent.popleft()
        day = ts - 86_400
        while self.errors and self.errors[0] <= day:
            self.errors.popleft()

    def features(self, ev: PaymentEvent) -> list[float]:
        self._expire(ev.ts)
        usd = ev.amount_cents / 100.0
        refund = ev.amount_cents < 0
        if self.last50:
            ordered = sorted(self.last50)
            mid = len(ordered) // 2
            median = ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2.0
            ratio = math.log((abs(usd) + 1.0) / (median + 1.0))
        else:
            ratio = 0.0
        if self.n_debit >= 2:
            std = math.sqrt(self.sq / (self.n_debit - 1))
            z = (abs(usd) - self.mean) / std if std > 0 else 0.0
        else:
            z = 0.0
        windows: list[float] = []
        for _, span in WINDOWS:
            cut = ev.ts - span
            cnt = 0
            total = 0.0
            for t, a in reversed(self.recent):
                if t <= cut:
                    break
                cnt += 1
                total += a
            windows.extend((float(cnt), total))
        since = math.log1p(ev.ts - self.last_ts) if self.last_ts is not None else -1.0
        seen = self.merchants.get(ev.merchant_key, 0)
        hour = (ev.ts // 3600) % 24
        near = self.hours[hour] + self.hours[(hour - 1) % 24] + self.hours[(hour + 1) % 24]
        in_person = ev.channel != 2
        return [
            usd,
            1.0 if refund else 0.0,
            ratio,
            z,
            math.log1p(self.n),
            *windows,
            since,
            math.log1p(seen),
            1.0 if seen == 0 else 0.0,
            1.0 if ev.mcc not in self.mccs else 0.0,
            float(ev.channel),
            1.0 if ev.channel not in self.channels else 0.0,
            1.0 if in_person and ev.location not in self.locations else 0.0,
            1.0 if ev.foreign else 0.0,
            float(len(self.errors)),
            float(hour),
            near / self.n if self.n else 0.0,
            ev.days_since_issue,
            ev.mcc_fraud_rate,
        ]

    def update(self, ev: PaymentEvent) -> None:
        usd = ev.amount_cents / 100.0
        self.n += 1
        if ev.amount_cents >= 0:
            self.n_debit += 1
            delta = usd - self.mean
            self.mean += delta / self.n_debit
            self.sq += delta * (usd - self.mean)
            self.last50.append(usd)
        self.recent.append((ev.ts, usd))
        if ev.has_error:
            self.errors.append(ev.ts)
        self.last_ts = ev.ts
        self.merchants[ev.merchant_key] += 1
        self.mccs.add(ev.mcc)
        self.channels.add(ev.channel)
        if ev.channel != 2:
            self.locations.add(ev.location)
        self.hours[(ev.ts // 3600) % 24] += 1

    def snapshot(self) -> dict[str, Any]:
        """The state as plain JSON, which is what the online store keeps per card."""
        return {
            "n": self.n,
            "n_debit": self.n_debit,
            "mean": self.mean,
            "sq": self.sq,
            "last_ts": self.last_ts,
            "recent": [list(x) for x in self.recent],
            "errors": list(self.errors),
            "last50": list(self.last50),
            "merchants": dict(self.merchants),
            "mccs": sorted(self.mccs),
            "channels": sorted(self.channels),
            "locations": sorted(self.locations),
            "hours": list(self.hours),
        }

    @classmethod
    def restore(cls, data: dict[str, Any]) -> CardState:
        s = cls()
        s.n = int(data["n"])
        s.n_debit = int(data["n_debit"])
        s.mean = float(data["mean"])
        s.sq = float(data["sq"])
        s.last_ts = data["last_ts"]
        s.recent = deque((int(t), float(a)) for t, a in data["recent"])
        s.errors = deque(int(t) for t in data["errors"])
        s.last50 = deque((float(a) for a in data["last50"]), maxlen=MEDIAN_SPAN)
        s.merchants = Counter({str(k): int(v) for k, v in data["merchants"].items()})
        s.mccs = {int(m) for m in data["mccs"]}
        s.channels = {int(c) for c in data["channels"]}
        s.locations = {str(x) for x in data["locations"]}
        s.hours = [int(h) for h in data["hours"]]
        return s


def run_history(events: list[PaymentEvent], state: CardState | None = None) -> tuple[list[list[float]], CardState]:
    """Features for each event of one card in order, then the state after the last."""
    st = state or CardState()
    out: list[list[float]] = []
    for ev in events:
        out.append(st.features(ev))
        st.update(ev)
    return out, st
