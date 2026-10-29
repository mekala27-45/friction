"""Labels on two clocks: when the fraud happened, and when the bank learned of it. Simulated.

TabFormer marks fraud at the transaction. A bank learns of it later, when the cardholder disputes
the charge. The simulator groups each card's fraud into incidents (a run with no gap longer than
``incident_gap_days``), gives each incident one report time from a lognormal lag, and lets a
stated share never be reported. It also adds friendly fraud: a few good transactions get a claim
that is reversed after investigation, which is what gives a label a second version on the system
time clock. Account takeovers on the login desk are reported by incident response after a lag.

Every label record carries ``event_ts`` (valid time), ``reported_at`` (system time) and ``seq``.
The feed is the input of the CDC flow that keeps labels as SCD type 2 on the system time clock.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from friction_simulator import draws
from friction_simulator.config import Simulator

LABEL_COLUMNS = ["label_ref", "event_ref", "desk", "is_fraud", "event_ts", "reported_at", "seq", "source"]


def incidents(frauds: pd.DataFrame, gap_days: int) -> pd.DataFrame:
    """frauds: txn_ref, user, card, event_ts. Adds incident_id and incident_start, sorted by key."""
    f = frauds.sort_values(["user", "card", "event_ts", "txn_ref"], kind="mergesort").reset_index(drop=True)
    gap = f.groupby(["user", "card"], sort=False)["event_ts"].diff()
    new = gap.isna() | (gap > pd.Timedelta(days=gap_days))
    f["incident_id"] = new.cumsum().astype(np.int64)
    first = f.groupby("incident_id")["event_ts"].transform("min")
    first_ref = f.groupby("incident_id")["txn_ref"].transform("min")
    f["incident_start"] = first
    f["incident_key"] = first_ref.astype(np.int64)
    return f


def chargeback_labels(frauds: pd.DataFrame, cfg: Simulator, setting: str) -> pd.DataFrame:
    """One label row per reported fraud transaction for a lag setting."""
    lag = cfg.chargebacks.settings[setting]
    f = incidents(frauds, cfg.chargebacks.incident_gap_days)
    keys = f["incident_key"].to_numpy()
    days = draws.lognormal(keys, lag.median_days, lag.sigma, cfg.seed, "chargeback_lag", setting)
    days = np.clip(days, 1.0, float(lag.max_days))
    never = draws.uniform(keys, cfg.seed, "never_reported", setting) < lag.never_reported
    report = f["incident_start"] + pd.to_timedelta(days, unit="D")
    # A card is blocked when the incident is reported; fraud the generator still writes after that
    # is reported within a day of happening.
    report = report.where(report >= f["event_ts"] + pd.Timedelta(hours=1), f["event_ts"] + pd.Timedelta(days=1))
    out = pd.DataFrame(
        {
            "label_ref": f["txn_ref"].astype(np.int64) * 10,
            "event_ref": f["txn_ref"].astype(np.int64),
            "desk": "payments",
            "is_fraud": True,
            "event_ts": f["event_ts"],
            "reported_at": report.dt.floor("s"),
            "source": "chargeback",
            "incident_key": f["incident_key"],
        }
    )[~never]
    return out.reset_index(drop=True)


def friendly_fraud(good_refs: np.ndarray, good_ts: pd.Series, cfg: Simulator) -> pd.DataFrame:
    """Claims on good transactions, then their reversals. good_refs and good_ts align."""
    claim_mask = draws.uniform(good_refs, cfg.seed, "false_claim") < cfg.chargebacks.false_claim_rate
    refs = good_refs[claim_mask]
    ts = good_ts.to_numpy()[claim_mask]
    claim_days = draws.integers(refs, 5, 30, cfg.seed, "claim_days")
    rev = cfg.chargebacks.reversal_days
    rev_days = draws.integers(refs, rev.min, rev.max, cfg.seed, "reversal_days")
    claimed = pd.to_datetime(ts) + pd.to_timedelta(claim_days, unit="D")
    reversed_at = claimed + pd.to_timedelta(rev_days, unit="D")
    claims = pd.DataFrame(
        {
            "label_ref": refs * 10 + 1,
            "event_ref": refs,
            "desk": "payments",
            "is_fraud": True,
            "event_ts": pd.to_datetime(ts),
            "reported_at": claimed,
            "source": "claim",
        }
    )
    reversals = claims.assign(label_ref=refs * 10 + 2, is_fraud=False, reported_at=reversed_at, source="reversal")
    return pd.concat([claims, reversals], ignore_index=True)


def takeover_labels(takeovers: pd.DataFrame, cfg: Simulator) -> pd.DataFrame:
    """takeovers: login_index, event_ts for every login the data set marks as a takeover."""
    keys = takeovers["login_index"].to_numpy(dtype=np.int64)
    lag = cfg.takeovers.report_lag_days
    days = draws.integers(keys, lag.min, lag.max, cfg.seed, "takeover_lag")
    secs = draws.integers(keys, 0, 86_399, cfg.seed, "takeover_second")
    report = takeovers["event_ts"] + pd.to_timedelta(days, unit="D") + pd.to_timedelta(secs, unit="s")
    return pd.DataFrame(
        {
            "label_ref": keys * 10,
            "event_ref": keys,
            "desk": "logins",
            "is_fraud": True,
            "event_ts": takeovers["event_ts"],
            "reported_at": report.dt.floor("s"),
            "source": "incident_response",
        }
    )


def with_sequence(labels: pd.DataFrame) -> pd.DataFrame:
    """seq orders the versions of one event's label on the system time clock, ties broken by label_ref."""
    out = labels.sort_values(["reported_at", "label_ref"], kind="mergesort").reset_index(drop=True)
    out["seq"] = np.arange(1, len(out) + 1, dtype=np.int64)
    return out[[*LABEL_COLUMNS, *[c for c in out.columns if c not in LABEL_COLUMNS]]]
