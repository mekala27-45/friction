"""The card lifecycle CDC feed. Simulated.

TabFormer has accounts (a user and a card slot) but no card history. The simulator writes the
changes an issuer's card system would emit: an issue before first use, a block on the day an
incident is reported, a reissue a few days later under a new card generation, credit limit
changes, and a closure after the last use. Each record has a per account sequence number. A stated
share is delivered late (in a later drop than its sequence implies) and a stated share twice,
because a CDC export does that and the flow that reads it must not care.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from friction_simulator import draws
from friction_simulator.config import Simulator

CDC_COLUMNS = [
    "account_ref",
    "user",
    "card",
    "op",
    "seq",
    "changed_at",
    "emitted_at",
    "card_generation",
    "status",
    "credit_limit_cents",
    "currency",
]


def account_ref(user: pd.Series, card: pd.Series) -> pd.Series:
    return user.astype(np.int64) * 100 + card.astype(np.int64)


def build(accounts: pd.DataFrame, reports: pd.DataFrame, data_end: pd.Timestamp, cfg: Simulator) -> pd.DataFrame:
    """accounts: user, card, first_ts, last_ts. reports: user, card, reported_at (one per incident)."""
    lc = cfg.lifecycle
    acc = accounts.sort_values(["user", "card"], kind="mergesort").reset_index(drop=True)
    acc["account_ref"] = account_ref(acc["user"], acc["card"])
    keys = acc["account_ref"].to_numpy(np.int64)
    issue_days = draws.integers(
        keys, lc.issue_before_first_use_days.min, lc.issue_before_first_use_days.max, cfg.seed, "issue"
    )
    limit_usd = draws.integers(keys, lc.credit_limit_usd.min, lc.credit_limit_usd.max, cfg.seed, "limit")
    events: list[pd.DataFrame] = []
    issued = acc["first_ts"].dt.floor("D") - pd.to_timedelta(issue_days, unit="D")
    events.append(
        pd.DataFrame(
            {
                "account_ref": acc["account_ref"],
                "user": acc["user"],
                "card": acc["card"],
                "op": "INSERT",
                "changed_at": issued,
                "kind": "issue",
                "limit_usd": limit_usd,
            }
        )
    )
    rep = reports.copy()
    rep["account_ref"] = account_ref(rep["user"], rep["card"])
    rep = rep.sort_values(["account_ref", "reported_at"], kind="mergesort").reset_index(drop=True)
    rkeys = rep["account_ref"].to_numpy(np.int64) * 1_000_003 + rep.groupby("account_ref").cumcount().to_numpy()
    if len(rep):
        events.append(
            rep.assign(op="UPDATE", changed_at=rep["reported_at"], kind="block")[
                ["account_ref", "user", "card", "op", "changed_at", "kind"]
            ]
        )
        after = draws.integers(
            rkeys, lc.reissue_after_block_days.min, lc.reissue_after_block_days.max, cfg.seed, "reissue"
        )
        events.append(
            rep.assign(op="UPDATE", changed_at=rep["reported_at"] + pd.to_timedelta(after, unit="D"), kind="reissue")[
                ["account_ref", "user", "card", "op", "changed_at", "kind"]
            ]
        )
    # Credit limit changes: a whole number of changes per account from its active years.
    years = ((acc["last_ts"] - issued).dt.days / 365.25).to_numpy()
    n_changes = np.floor(years * lc.limit_changes_per_year + draws.uniform(keys, cfg.seed, "n_changes")).astype(int)
    rows = []
    for ref, user, card, start, end, n in zip(
        acc["account_ref"], acc["user"], acc["card"], issued, acc["last_ts"], n_changes, strict=True
    ):
        if n <= 0:
            continue
        ck = np.int64(ref) * 1000 + np.arange(n, dtype=np.int64)
        offsets = draws.uniform(ck, cfg.seed, "change_at")
        span = (end - start).total_seconds()
        when = [start + pd.Timedelta(seconds=float(o) * span) for o in np.sort(offsets)]
        new_limits = draws.integers(ck, lc.credit_limit_usd.min, lc.credit_limit_usd.max, cfg.seed, "new_limit")
        for w, lim in zip(when, new_limits, strict=True):
            rows.append((ref, user, card, "UPDATE", w.floor("s"), "limit", int(lim)))
    if rows:
        events.append(
            pd.DataFrame(rows, columns=["account_ref", "user", "card", "op", "changed_at", "kind", "limit_usd"])
        )
    # Closure for accounts quiet for half a year before the data ends.
    quiet = acc["last_ts"] < data_end - pd.Timedelta(days=180)
    close_days = draws.integers(
        keys, lc.close_after_last_use_days.min, lc.close_after_last_use_days.max, cfg.seed, "close"
    )
    closes = acc[quiet].assign(
        op="DELETE",
        changed_at=acc["last_ts"][quiet].dt.floor("D") + pd.to_timedelta(close_days[quiet.to_numpy()], unit="D"),
        kind="close",
    )
    events.append(closes[["account_ref", "user", "card", "op", "changed_at", "kind"]])
    ev = pd.concat(events, ignore_index=True)
    ev = ev.sort_values(["account_ref", "changed_at", "kind"], kind="mergesort").reset_index(drop=True)
    ev["seq"] = ev.groupby("account_ref").cumcount().astype(np.int64) + 1
    # Carry state forward per account: generation, status and limit after each change.
    gens, statuses, limits = [], [], []
    for _, grp in ev.groupby("account_ref", sort=False):
        gen, status, limit = 0, "active", None
        for kind, lim in zip(grp["kind"], grp.get("limit_usd", pd.Series([np.nan] * len(grp))), strict=True):
            if kind == "issue":
                gen, status, limit = 0, "active", int(lim)
            elif kind == "block":
                status = "blocked"
            elif kind == "reissue":
                gen, status = gen + 1, "active"
            elif kind == "limit":
                limit = int(lim)
            elif kind == "close":
                status = "closed"
            gens.append(gen)
            statuses.append(status)
            limits.append(limit if limit is not None else 0)
    ev["card_generation"] = np.asarray(gens, dtype=np.int64)
    ev["status"] = statuses
    ev["credit_limit_cents"] = np.asarray(limits, dtype=np.int64) * 100
    ev["currency"] = "USD"
    # Delivery: most records are emitted within the hour; some late, some twice.
    rk = ev["account_ref"].to_numpy(np.int64) * 10_000 + ev["seq"].to_numpy(np.int64)
    emit_min = draws.integers(rk, 1, 59, cfg.seed, "emit_minutes")
    late = draws.uniform(rk, cfg.seed, "cdc_late") < lc.share_out_of_order
    late_days = draws.integers(rk, 1, 5, cfg.seed, "cdc_late_days")
    emitted = (
        ev["changed_at"] + pd.to_timedelta(emit_min, unit="m") + pd.to_timedelta(np.where(late, late_days, 0), unit="D")
    )
    ev["emitted_at"] = emitted.dt.floor("s")
    dup = draws.uniform(rk, cfg.seed, "cdc_dup") < lc.share_duplicated
    twice = ev[dup].assign(emitted_at=ev["emitted_at"][dup] + pd.Timedelta(days=1))
    out = pd.concat([ev, twice], ignore_index=True)
    out = out.sort_values(["emitted_at", "account_ref", "seq"], kind="mergesort").reset_index(drop=True)
    return out[CDC_COLUMNS]
