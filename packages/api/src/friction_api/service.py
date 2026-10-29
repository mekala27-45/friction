"""Scoring one event against the online state: the same feature code the lakehouse imports.

The request carries an event as the upstream system sends it. Restricted values (an IP address, a
user agent, a card holder's ids) are accepted either already tokenized by the edge, which is how
the parity replay and the site send them, or raw, in which case they are tokenized here with the
build's salt and never stored, logged or returned.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from friction_core import tokens
from friction_core.units import parse_amount_cents
from friction_features import logins as login_features
from friction_features import payments as payment_features
from pydantic import BaseModel, ConfigDict, Field, model_validator

from friction_api.bundle import Bundle
from friction_api.store import Tx

CHANNELS = {"swipe": 0, "chip": 1, "online": 2}


class PaymentIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_ref: int
    ts: datetime
    account_key: str | None = None
    user: int | None = None
    card: int | None = None
    amount_cents: int | None = None
    amount: str | None = None
    merchant_key: str | None = None
    merchant_name: str | None = None
    mcc: int
    channel: Literal["swipe", "chip", "online"] | int
    location: str = ""
    foreign: bool = False
    has_error: bool = False

    @model_validator(mode="after")
    def _complete(self) -> PaymentIn:
        if self.account_key is None and (self.user is None or self.card is None):
            raise ValueError("account_key, or user and card")
        if self.amount_cents is None and self.amount is None:
            raise ValueError("amount_cents or amount")
        if self.amount_cents is None and parse_amount_cents(self.amount) is None:
            raise ValueError("amount does not parse")
        if self.merchant_key is None and self.merchant_name is None:
            raise ValueError("merchant_key or merchant_name")
        return self


class LoginIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_ref: int
    ts: datetime
    user_key: str | None = None
    user_id: str | None = None
    ip_key: str | None = None
    ip_address: str | None = Field(default=None, repr=False)
    ua_key: str | None = None
    user_agent: str | None = Field(default=None, repr=False)
    asn: int | None = None
    country: str | None = None
    region: str | None = None
    city: str | None = None
    browser: str | None = None
    os: str | None = None
    device_type: str | None = None
    rtt_ms: float | None = None
    success: bool = True
    is_attack_ip: bool = False

    @model_validator(mode="after")
    def _complete(self) -> LoginIn:
        for tok, raw in (("user_key", "user_id"), ("ip_key", "ip_address"), ("ua_key", "user_agent")):
            if getattr(self, tok) is None and getattr(self, raw) is None:
                raise ValueError(f"{tok} or {raw}")
        return self


class DecisionOut(BaseModel):
    decision_id: str
    desk: str
    event_ref: int
    verdict: str
    score: float
    codes: list[str]
    model: str


def epoch_seconds(ts: datetime) -> int:
    t = ts if ts.tzinfo else ts.replace(tzinfo=UTC)
    return int(t.timestamp())


def epoch_ms(ts: datetime) -> int:
    t = ts if ts.tzinfo else ts.replace(tzinfo=UTC)
    return int(t.timestamp()) * 1000 + t.microsecond // 1000


def days_since_issue(issued: list[int], ts: int) -> float:
    before = [i for i in issued if i <= ts]
    return (ts - max(before)) / 86400.0 if before else -1.0


def payment_event(e: PaymentIn, bundle: Bundle, issued: list[int]) -> tuple[str, payment_features.PaymentEvent]:
    account = e.account_key or tokens.token("account", f"{e.user}:{e.card}")
    merchant = e.merchant_key or tokens.token("merchant", str(e.merchant_name), tokens.MERCHANT_SALT)
    cents = e.amount_cents if e.amount_cents is not None else parse_amount_cents(e.amount)
    assert cents is not None
    ts = epoch_seconds(e.ts)
    month = datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-01")
    channel = CHANNELS[e.channel] if isinstance(e.channel, str) else int(e.channel)
    ev = payment_features.PaymentEvent(
        ts=ts, ref=e.event_ref, amount_cents=int(cents), merchant_key=merchant, mcc=e.mcc, channel=channel,
        location=e.location or "", foreign=e.foreign, has_error=e.has_error,
        days_since_issue=days_since_issue(issued, ts), mcc_fraud_rate=bundle.mcc_rate(month, e.mcc),
    )  # fmt: skip
    return account, ev


def score_payment(tx: Tx, bundle: Bundle, e: PaymentIn) -> DecisionOut:
    account = e.account_key or tokens.token("account", f"{e.user}:{e.card}")
    saved = tx.lock_state("card", account)
    state = payment_features.CardState.restore(saved) if saved else payment_features.CardState()
    _, ev = payment_event(e, bundle, tx.issues(account))
    x = state.features(ev)
    score, codes = bundle.score(x)
    verdict = bundle.decide(score, ev.channel)
    state.update(ev)
    tx.put_state("card", account, state.snapshot())
    carried = codes if verdict != "approve" else []
    did = tx.log("payments", e.event_ref, account, score, verdict, carried, bundle.model, bundle.digest)
    return DecisionOut(decision_id=did, desk="payments", event_ref=e.event_ref, verdict=verdict, score=score,
                       codes=carried, model=bundle.model)  # fmt: skip


def login_values(e: LoginIn) -> tuple[str, str, tuple[str, ...]]:
    user = e.user_key or tokens.token("user", str(e.user_id))
    ip = e.ip_key or tokens.token("ip", str(e.ip_address))
    ua = e.ua_key or tokens.token("ua", str(e.user_agent))
    values = (
        ip, "" if e.asn is None else str(e.asn), e.country or "", e.region or "", e.city or "", ua,
        e.browser or "", e.os or "", e.device_type or "",
    )  # fmt: skip
    return user, ip, values


def score_login(tx: Tx, bundle: Bundle, e: LoginIn) -> DecisionOut:
    user, ip, values = login_values(e)
    stranger = user in bundle.unknown_accounts
    # Lock in a fixed order so two requests for the same user and address cannot deadlock.
    order = sorted([("ip", ip), ("user", user)] if not stranger else [("ip", ip)])
    saved: dict[str, Any] = {kind: tx.lock_state(kind, key) for kind, key in order}
    ustate = login_features.UserState.restore(saved["user"]) if saved.get("user") else login_features.UserState()
    istate = login_features.IpState.restore(saved["ip"]) if saved.get("ip") else login_features.IpState()
    ts = epoch_ms(e.ts)
    ev = login_features.LoginEvent(ts, e.event_ref, values, e.rtt_ms, e.success, False)
    x = [*ustate.features(ev), *istate.features(ts)]
    if bundle.model == "lightgbm_rep":
        x.append(1.0 if e.is_attack_ip else 0.0)
    score, codes = bundle.score(x, values)
    verdict = bundle.decide(score, None)
    ustate.update(ev)
    istate.update(ts, e.success, False)
    if not stranger:
        tx.put_state("user", user, ustate.snapshot())
    tx.put_state("ip", ip, istate.snapshot())
    carried = codes if verdict != "approve" else []
    did = tx.log("logins", e.event_ref, user, score, verdict, carried, bundle.model, bundle.digest)
    return DecisionOut(decision_id=did, desk="logins", event_ref=e.event_ref, verdict=verdict, score=score,
                       codes=carried, model=bundle.model)  # fmt: skip
