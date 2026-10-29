"""The state machines run over a whole partition, for ``applyInPandas`` in the lakehouse.

Spark groups events by a hash bucket of the entity (card account or user), not by the entity
itself, because three million single user groups cost more in scheduling than in work. Inside a
bucket the rows are sorted by (entity, event time, reference) and the state resets at each new
entity. The output carries the event reference and the feature vector, nothing else, so the join
back to the event is by key.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from friction_features import logins, payments

PAYMENT_INPUT = (
    "account_key", "txn_ref", "event_ts_s", "amount_cents", "merchant_key", "mcc", "channel",
    "location", "foreign", "has_error", "days_since_issue", "mcc_fraud_rate",
)  # fmt: skip
LOGIN_INPUT = (
    "user_key", "login_ref", "event_ts_ms", *(f"v_{lvl}" for lvl in logins.LEVELS), "rtt_ms", "success", "synthetic",
)  # fmt: skip


def payments_schema() -> str:
    return "txn_ref long, " + ", ".join(f"{f} double" for f in payments.FEATURES)


def logins_schema() -> str:
    return "login_ref long, " + ", ".join(f"{f} double" for f in logins.FEATURES)


def ip_schema() -> str:
    return "login_ref long, " + ", ".join(f"{f} double" for f in logins.IP_FEATURES)


def payments_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(["account_key", "event_ts_s", "txn_ref"], kind="mergesort")
    rows: list[list[float]] = []
    refs = pdf["txn_ref"].to_numpy(np.int64)
    state = payments.CardState()
    current: str | None = None
    for acc, ts, ref, amt, merch, mcc, ch, loc, foreign, err, dsi, rate in zip(
        pdf["account_key"].tolist(),
        pdf["event_ts_s"].tolist(),
        refs.tolist(),
        pdf["amount_cents"].tolist(),
        pdf["merchant_key"].tolist(),
        pdf["mcc"].tolist(),
        pdf["channel"].tolist(),
        pdf["location"].tolist(),
        pdf["foreign"].tolist(),
        pdf["has_error"].tolist(),
        pdf["days_since_issue"].tolist(),
        pdf["mcc_fraud_rate"].tolist(),
        strict=True,
    ):
        if acc != current:
            state = payments.CardState()
            current = acc
        ev = payments.PaymentEvent(
            int(ts), int(ref), int(amt), str(merch), int(mcc), int(ch), str(loc or ""), bool(foreign),
            bool(err), float(dsi), float(rate),
        )  # fmt: skip
        rows.append(state.features(ev))
        state.update(ev)
    out = pd.DataFrame(rows, columns=list(payments.FEATURES), dtype=np.float64)
    out.insert(0, "txn_ref", refs)
    return out


def logins_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(["user_key", "event_ts_ms", "login_ref"], kind="mergesort")
    refs = pdf["login_ref"].to_numpy(np.int64)
    value_cols = [pdf[f"v_{lvl}"].fillna("").astype(str).tolist() for lvl in logins.LEVELS]
    rtts = pdf["rtt_ms"].tolist()
    rows: list[list[float]] = []
    state = logins.UserState()
    current: str | None = None
    for i, (user, ts, ref, ok, synth) in enumerate(
        zip(
            pdf["user_key"].tolist(),
            pdf["event_ts_ms"].tolist(),
            refs.tolist(),
            pdf["success"].tolist(),
            pdf["synthetic"].tolist(),
            strict=True,
        )
    ):
        if user != current:
            state = logins.UserState()
            current = user
        rtt = rtts[i]
        ev = logins.LoginEvent(
            int(ts), int(ref), tuple(col[i] for col in value_cols),
            None if rtt is None or (isinstance(rtt, float) and np.isnan(rtt)) else float(rtt),
            bool(ok), bool(synth),
        )  # fmt: skip
        rows.append(state.features(ev))
        state.update(ev)
    out = pd.DataFrame(rows, columns=list(logins.FEATURES), dtype=np.float64)
    out.insert(0, "login_ref", refs)
    return out


def ip_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(["ip_key", "event_ts_ms", "login_ref"], kind="mergesort")
    refs = pdf["login_ref"].to_numpy(np.int64)
    rows: list[list[float]] = []
    state = logins.IpState()
    current: str | None = None
    for ip, ts, ok, synth in zip(
        pdf["ip_key"].tolist(),
        pdf["event_ts_ms"].tolist(),
        pdf["success"].tolist(),
        pdf["synthetic"].tolist(),
        strict=True,
    ):
        if ip != current:
            state = logins.IpState()
            current = ip
        rows.append(state.features(int(ts)))
        state.update(int(ts), bool(ok), bool(synth))
    out = pd.DataFrame(rows, columns=list(logins.IP_FEATURES), dtype=np.float64)
    out.insert(0, "login_ref", refs)
    return out
