"""Login features as a state machine per user, and a second, smaller one per IP address.

The user state holds what the service has seen in the user's earlier successful logins: how often
each IP, ASN, country, region, city, user agent, browser, OS and device type appeared, how many
distinct values of each, the time of the last success, recent failures, and the running mean and
spread of the log round trip time. Those counts are exactly what the Freeman et al. (2016) model
reads (packages/models/src/friction_models/freeman.py), and the gradient boosted model reads them
too, with a few more.

The IP state is the same idea keyed by the IP token: logins and failures from that address in the
last hour, across every user.

A synthetic attacker login (evaluation only) is scored against the state and never updates it.
"""

from __future__ import annotations

import math
from collections import Counter, deque
from dataclasses import dataclass
from typing import Any

LEVELS: tuple[str, ...] = ("ip", "asn", "country", "region", "city", "ua", "browser", "os", "device")
DEVICES = {"mobile": 0, "desktop": 1, "tablet": 2, "bot": 3, "unknown": 4, "": 5}

FEATURES: tuple[str, ...] = (
    "n_success_prior",
    *(f"{lvl}_count" for lvl in LEVELS),
    *(f"{lvl}_distinct" for lvl in LEVELS),
    "log_secs_since_success",
    "failed_1h_user",
    "rtt_z_user",
    "rtt_missing",
    "device_type",
)
IP_FEATURES: tuple[str, ...] = ("logins_1h_ip", "failed_1h_ip")


@dataclass(frozen=True, slots=True)
class LoginEvent:
    ts_ms: int
    ref: int
    values: tuple[str, ...]  # one per LEVELS entry, already tokenized where restricted
    rtt_ms: float | None
    success: bool
    synthetic: bool = False


class UserState:
    __slots__ = ("counts", "failed", "last_success_ms", "n_rtt", "n_success", "rtt_mean", "rtt_sq")

    def __init__(self) -> None:
        self.counts: list[Counter[str]] = [Counter() for _ in LEVELS]
        self.n_success = 0
        self.last_success_ms: int | None = None
        self.failed: deque[int] = deque()
        self.n_rtt = 0
        self.rtt_mean = 0.0
        self.rtt_sq = 0.0

    def features(self, ev: LoginEvent) -> list[float]:
        cut = ev.ts_ms - 3_600_000
        while self.failed and self.failed[0] <= cut:
            self.failed.popleft()
        seen = [float(c.get(v, 0)) for c, v in zip(self.counts, ev.values, strict=True)]
        distinct = [float(len(c)) for c in self.counts]
        since = math.log1p((ev.ts_ms - self.last_success_ms) / 1000.0) if self.last_success_ms is not None else -1.0
        if ev.rtt_ms is not None and ev.rtt_ms > 0 and self.n_rtt >= 3:
            std = math.sqrt(self.rtt_sq / (self.n_rtt - 1))
            rtt_z = (math.log(ev.rtt_ms) - self.rtt_mean) / std if std > 0 else 0.0
        else:
            rtt_z = 0.0
        return [
            float(self.n_success),
            *seen,
            *distinct,
            since,
            float(len(self.failed)),
            rtt_z,
            1.0 if ev.rtt_ms is None else 0.0,
            float(DEVICES.get(ev.values[LEVELS.index("device")], 4)),
        ]

    def update(self, ev: LoginEvent) -> None:
        if ev.synthetic:
            return
        if not ev.success:
            self.failed.append(ev.ts_ms)
            return
        self.n_success += 1
        self.last_success_ms = ev.ts_ms
        for c, v in zip(self.counts, ev.values, strict=True):
            c[v] += 1
        if ev.rtt_ms is not None and ev.rtt_ms > 0:
            x = math.log(ev.rtt_ms)
            self.n_rtt += 1
            delta = x - self.rtt_mean
            self.rtt_mean += delta / self.n_rtt
            self.rtt_sq += delta * (x - self.rtt_mean)

    def snapshot(self) -> dict[str, Any]:
        return {
            "counts": [dict(c) for c in self.counts],
            "n_success": self.n_success,
            "last_success_ms": self.last_success_ms,
            "failed": list(self.failed),
            "n_rtt": self.n_rtt,
            "rtt_mean": self.rtt_mean,
            "rtt_sq": self.rtt_sq,
        }

    @classmethod
    def restore(cls, data: dict[str, Any]) -> UserState:
        s = cls()
        s.counts = [Counter({str(k): int(v) for k, v in c.items()}) for c in data["counts"]]
        s.n_success = int(data["n_success"])
        s.last_success_ms = data["last_success_ms"]
        s.failed = deque(int(t) for t in data["failed"])
        s.n_rtt = int(data["n_rtt"])
        s.rtt_mean = float(data["rtt_mean"])
        s.rtt_sq = float(data["rtt_sq"])
        return s


class IpState:
    __slots__ = ("failed", "seen")

    def __init__(self) -> None:
        self.seen: deque[int] = deque()
        self.failed: deque[int] = deque()

    def features(self, ts_ms: int) -> list[float]:
        cut = ts_ms - 3_600_000
        while self.seen and self.seen[0] <= cut:
            self.seen.popleft()
        while self.failed and self.failed[0] <= cut:
            self.failed.popleft()
        return [float(len(self.seen)), float(len(self.failed))]

    def update(self, ts_ms: int, success: bool, synthetic: bool = False) -> None:
        if synthetic:
            return
        self.seen.append(ts_ms)
        if not success:
            self.failed.append(ts_ms)

    def snapshot(self) -> dict[str, Any]:
        return {"seen": list(self.seen), "failed": list(self.failed)}

    @classmethod
    def restore(cls, data: dict[str, Any]) -> IpState:
        s = cls()
        s.seen = deque(int(t) for t in data["seen"])
        s.failed = deque(int(t) for t in data["failed"])
        return s
