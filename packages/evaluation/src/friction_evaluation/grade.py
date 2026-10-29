"""Grading in the repository, from the exported scored sets.

One function per question the plan registered. Every threshold is chosen on validation by the
operating point rule and taken to test unchanged; every number on test carries a cluster bootstrap
interval over the desk's unit of resampling; the family of model comparisons is corrected with
Benjamini-Hochberg; the hypotheses are tested once on validation. Nothing here reads a test label
before a threshold is fixed.

The payments desk grades every event; the login desk grades successful sign ins only, because risk
based authentication runs after the password was right, and every attacker model holds the
password. Failed attempts still count in the failure hypothesis.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
from friction_policy import policy
from friction_policy.policy import APPROVE, DECLINE, STEP_UP, Thresholds

from friction_evaluation import bootstrap, calibration, frontier, multiple
from friction_evaluation.frontier import Budget

F64 = npt.NDArray[np.float64]
FRONTIER_POINTS = 30


@dataclass(frozen=True)
class Desk:
    name: str
    key: str
    user: str
    models: tuple[str, ...]
    family: tuple[str, ...]
    probabilities: frozenset[str]
    budget: Budget
    extra: tuple[str, ...] = field(default=())

    @property
    def online_only(self) -> bool:
        return self.name == "payments"


def desks(plan: Mapping[str, Any]) -> dict[str, Desk]:
    pay, log = plan["desks"]["payments"], plan["desks"]["logins"]
    return {
        "payments": Desk(
            name="payments",
            key="txn_ref",
            user="account_key",
            models=("floor", "logistic", "lightgbm"),
            family=("floor", "logistic", "lightgbm"),
            probabilities=frozenset({"logistic", "lightgbm"}),
            budget=Budget(**pay["budget"]),
        ),
        "logins": Desk(
            name="logins",
            key="login_ref",
            user="user_key",
            models=("floor", "freeman", "lightgbm", "lightgbm_rep"),
            family=("floor", "freeman", "lightgbm"),
            probabilities=frozenset({"lightgbm", "lightgbm_rep"}),
            budget=Budget(**log["budget"]),
            extra=("lightgbm_rep",),
        ),
    }


# ---------------------------------------------------------------------------------------------
# Preparing a scored set


@dataclass(frozen=True)
class Graded:
    """The arrays one split of one desk is graded on."""

    frame: pd.DataFrame
    fraud: npt.NDArray[np.bool_]
    usd: F64
    channels: npt.NDArray[np.int64] | None
    users: npt.NDArray[np.int64]
    n_users: int

    def scores(self, model: str) -> F64:
        return self.frame[f"score_{model}"].to_numpy(np.float64)


def prepare(desk: Desk, frame: pd.DataFrame, label: str = "label") -> Graded:
    if desk.name == "logins":
        frame = frame[frame["success"].astype(bool)]
    frame = frame.sort_values(desk.key, kind="mergesort").reset_index(drop=True)
    fraud = frame[label].astype(bool).to_numpy()
    if desk.name == "payments":
        usd = np.clip(frame["amount_cents"].to_numpy(np.float64) / 100.0, 0, None)
        channels: npt.NDArray[np.int64] | None = frame["channel"].to_numpy(np.int64)
    else:
        usd = np.ones(len(frame), dtype=np.float64)
        channels = None
    codes, uniques = pd.factorize(frame[desk.user], sort=True)
    return Graded(frame, fraud, usd, channels, codes.astype(np.int64), len(uniques))


def decide(desk: Desk, g: Graded, model: str, t: Thresholds) -> npt.NDArray[np.int8]:
    s = g.scores(model)
    if desk.name == "payments":
        assert g.channels is not None
        return policy.payments(s, g.channels, t)
    return policy.logins(s, t)


def choose(desk: Desk, g: Graded, model: str, budget: Budget | None = None) -> Thresholds:
    return frontier.choose(g.scores(model), g.fraud, g.channels if desk.online_only else None, budget or desk.budget)


# ---------------------------------------------------------------------------------------------
# Event columns and per user statistics

COLUMNS = ("captured", "captured_usd", "good_flagged", "good_stepped", "good_declined", "flagged", "flagged_fraud")
BASE = ("fraud", "fraud_usd", "good", "events")


def event_columns(g: Graded, decisions: npt.NDArray[np.int8]) -> dict[str, F64]:
    flagged = decisions != APPROVE
    good = ~g.fraud
    return {
        "captured": (flagged & g.fraud).astype(np.float64),
        "captured_usd": np.where(flagged & g.fraud, g.usd, 0.0),
        "good_flagged": (flagged & good).astype(np.float64),
        "good_stepped": ((decisions == STEP_UP) & good).astype(np.float64),
        "good_declined": ((decisions == DECLINE) & good).astype(np.float64),
        "flagged": flagged.astype(np.float64),
        "flagged_fraud": (flagged & g.fraud).astype(np.float64),
    }


def base_columns(g: Graded) -> dict[str, F64]:
    return {
        "fraud": g.fraud.astype(np.float64),
        "fraud_usd": np.where(g.fraud, g.usd, 0.0),
        "good": (~g.fraud).astype(np.float64),
        "events": np.ones(len(g.fraud), dtype=np.float64),
    }


class Stats:
    """Named per user columns and their bootstrap sums, built once per split."""

    def __init__(self, g: Graded) -> None:
        self.g = g
        self.names: list[str] = []
        self.cols: list[F64] = []
        for k, v in base_columns(g).items():
            self.add(k, v)

    def add(self, name: str, column: F64) -> None:
        # Summed per user at once: a million users by a few hundred columns fits, a few million
        # events by a few hundred columns does not.
        self.names.append(name)
        self.cols.append(np.bincount(self.g.users, weights=column, minlength=self.g.n_users))

    def build(self, resamples: int, seed: int) -> None:
        self.matrix = np.column_stack(self.cols)
        self.cols = []
        self.sums = bootstrap.resampled_sums(self.matrix, resamples, seed)
        self.index = {n: i for i, n in enumerate(self.names)}

    def ratio(self, num: str, den: str, scale: float = 1.0) -> dict[str, float]:
        iv = bootstrap.ratio(self.matrix, self.sums, self.index[num], self.index[den])
        return {"estimate": iv.estimate * scale, "low": iv.low * scale, "high": iv.high * scale}

    def difference(self, a: tuple[str, str], b: tuple[str, str]) -> tuple[dict[str, float], float]:
        i = self.index
        iv, p = bootstrap.difference(self.matrix, self.sums, (i[a[0]], i[a[1]]), (i[b[0]], i[b[1]]))
        return {"estimate": iv.estimate, "low": iv.low, "high": iv.high}, p


# ---------------------------------------------------------------------------------------------
# Threshold free reading, calibration and stability


def roc_auc(scores: F64, y: npt.NDArray[np.bool_]) -> float:
    pos, neg = int(y.sum()), int((~y).sum())
    if pos == 0 or neg == 0:
        return float("nan")
    ranks = pd.Series(scores).rank(method="average").to_numpy()
    return float((ranks[y].sum() - pos * (pos + 1) / 2) / (pos * neg))


def average_precision(scores: F64, y: npt.NDArray[np.bool_]) -> float:
    pos = int(y.sum())
    if pos == 0:
        return float("nan")
    order = np.argsort(-scores, kind="mergesort")
    ys = y[order]
    s = scores[order]
    # Ties are scored as one block: precision at the end of each block of equal scores.
    ends = np.r_[np.nonzero(np.diff(s))[0], len(s) - 1]
    tp = np.cumsum(ys)[ends]
    seen = ends + 1
    gained = np.diff(np.r_[0, tp])
    return float(np.sum(gained * (tp / seen)) / pos)


def psi(expected: F64, actual: F64, bins: int = 10) -> float:
    """Population stability index of actual against expected, decile bins of expected."""
    edges = np.unique(np.quantile(expected, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return 0.0
    inner = edges[1:-1]
    e = np.bincount(np.searchsorted(inner, expected, side="right"), minlength=len(inner) + 1) / len(expected)
    a = np.bincount(np.searchsorted(inner, actual, side="right"), minlength=len(inner) + 1) / len(actual)
    e, a = np.clip(e, 1e-6, None), np.clip(a, 1e-6, None)
    return float(np.sum((a - e) * np.log(a / e)))


def reason_coverage(g: Graded, model: str, decisions: npt.NDArray[np.int8]) -> dict[str, Any]:
    codes = g.frame[f"codes_{model}"].fillna("").astype(str).to_numpy()
    flagged = decisions != APPROVE
    n = int(flagged.sum())
    covered = int((flagged & (np.char.str_len(codes.astype("U")) > 0)).sum())
    counts: dict[str, int] = {}
    for c in codes[flagged]:
        for code in filter(None, c.split(",")):
            counts[code] = counts.get(code, 0) + 1
    return {
        "flagged": n,
        "covered": covered,
        "coverage": covered / n if n else 1.0,
        "by_code": dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))),
    }


# ---------------------------------------------------------------------------------------------
# One desk


def outcome_dict(st: Stats, model: str, budget_scale: float = 1000.0) -> dict[str, Any]:
    m = model
    return {
        "capture_usd": st.ratio(f"{m}:captured_usd", "fraud_usd"),
        "capture": st.ratio(f"{m}:captured", "fraud"),
        "friction_per_1000": st.ratio(f"{m}:good_flagged", "good", budget_scale),
        "step_ups_per_1000": st.ratio(f"{m}:good_stepped", "good", budget_scale),
        "declines_per_1000": st.ratio(f"{m}:good_declined", "good", budget_scale),
        "false_positive_rate": st.ratio(f"{m}:good_flagged", "good"),
        "precision_all": st.ratio(f"{m}:flagged_fraud", f"{m}:flagged"),
    }


def grade_desk(
    desk: Desk,
    validation: pd.DataFrame,
    test: pd.DataFrame,
    *,
    resamples: int = bootstrap.RESAMPLES,
    seed: int = bootstrap.SEED,
    as_of_label: str | None = None,
) -> dict[str, Any]:
    val = prepare(desk, validation)
    tst = prepare(desk, test)
    out: dict[str, Any] = {
        "desk": desk.name,
        "budget": {"step_up_per_1000": desk.budget.step_up_per_1000, "decline_per_1000": desk.budget.decline_per_1000},
        "validation": {"events": len(val.fraud), "frauds": int(val.fraud.sum()), "users": val.n_users},
        "test": {
            "events": len(tst.fraud),
            "frauds": int(tst.fraud.sum()),
            "fraud_usd": float(tst.usd[tst.fraud].sum()),
            "goods": int((~tst.fraud).sum()),
            "users": tst.n_users,
        },
        "models": {},
    }
    st = Stats(tst)
    thresholds: dict[str, Thresholds] = {}
    decisions: dict[str, npt.NDArray[np.int8]] = {}
    for m in desk.models:
        t = choose(desk, val, m)
        thresholds[m] = t
        d = decide(desk, tst, m, t)
        decisions[m] = d
        for k, v in event_columns(tst, d).items():
            st.add(f"{m}:{k}", v)
    traces: dict[str, list[tuple[float, float]]] = {}
    for m in desk.models:
        s = tst.scores(m)
        qs = np.unique(np.quantile(s, np.linspace(0.5, 1.0, FRONTIER_POINTS)))[::-1]
        pts = []
        for i, q in enumerate(qs):
            step = float(min(q, thresholds[m].decline))
            d = decide(desk, tst, m, Thresholds(step_up=step, decline=thresholds[m].decline))
            cols = event_columns(tst, d)
            st.add(f"{m}:p{i}:captured_usd", cols["captured_usd"])
            goods = float((~tst.fraud).sum())
            pts.append((step, float(cols["good_flagged"].sum() / goods * 1000.0) if goods else float("nan")))
        traces[m] = pts
    st.build(resamples, seed)
    for m in desk.models:
        d = decisions[m]
        s_val, s_tst = val.scores(m), tst.scores(m)
        entry: dict[str, Any] = {
            "thresholds": {"step_up": thresholds[m].step_up, "decline": thresholds[m].decline},
            "validation": validation_outcome(desk, val, m, thresholds[m]),
            "test": outcome_dict(st, m),
            "verdicts": frontier.counts_by_verdict(d),
            "roc_auc": roc_auc(s_tst, tst.fraud),
            "average_precision": average_precision(s_tst, tst.fraud),
            "psi_validation_to_test": psi(s_val, s_tst),
            "reasons": reason_coverage(tst, m, d),
        }
        if m in desk.probabilities:
            entry["calibration"] = {
                "ece": calibration.ece(s_tst, tst.fraud),
                "reliability": [
                    {"mean_score": a, "fraud_rate": b, "n": n} for a, b, n in calibration.reliability(s_tst, tst.fraud)
                ],
            }
        else:
            entry["calibration"] = {"ece": None, "note": "the score is not a probability"}
        frontier_rows = []
        for i, (step, friction) in enumerate(traces[m]):
            frontier_rows.append(
                {
                    "step_up": step,
                    "friction_per_1000": friction,
                    "capture_usd": st.ratio(f"{m}:p{i}:captured_usd", "fraud_usd"),
                }
            )
        entry["frontier"] = frontier_rows
        out["models"][m] = entry
    comparisons: list[dict[str, Any]] = []
    fam = list(desk.family)
    for i, a in enumerate(fam):
        for b in fam[:i]:
            iv, p = st.difference((f"{a}:captured_usd", "fraud_usd"), (f"{b}:captured_usd", "fraud_usd"))
            comparisons.append({"a": a, "b": b, "difference": iv, "p": p})
    adj = multiple.adjusted([c["p"] for c in comparisons]) if comparisons else []
    rej = multiple.bh([c["p"] for c in comparisons]) if comparisons else []
    for c, q, r in zip(comparisons, adj, rej, strict=True):
        c["p_adjusted"] = q
        c["rejected"] = bool(r)
        c["clear_of_zero"] = c["difference"]["low"] > 0 or c["difference"]["high"] < 0
    out["comparisons"] = comparisons
    if as_of_label is not None and as_of_label in test.columns:
        out["as_of"] = as_of_view(desk, test, thresholds, as_of_label, resamples, seed)
    out["_thresholds"] = thresholds
    out["_decisions"] = decisions
    out["_graded"] = tst
    out["_validation"] = val
    return out


def validation_outcome(desk: Desk, g: Graded, model: str, t: Thresholds) -> dict[str, float]:
    cols = event_columns(g, decide(desk, g, model, t))
    fusd = float(np.where(g.fraud, g.usd, 0.0).sum())
    goods = float((~g.fraud).sum())
    return {
        "capture_usd": float(cols["captured_usd"].sum() / fusd) if fusd else float("nan"),
        "step_ups_per_1000": float(cols["good_stepped"].sum() / goods * 1000.0) if goods else float("nan"),
        "declines_per_1000": float(cols["good_declined"].sum() / goods * 1000.0) if goods else float("nan"),
    }


def champion(desk: Desk, graded: Mapping[str, Any], gate_results: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """Among the models that clear every repository gate, the best validation dollar capture.

    Chosen on validation, so the test months never pick the winner. Parity and latency are decided
    by serving and recorded beside it; a model that has not cleared them yet is a candidate.
    """
    passing = [m for m, g in gate_results.items() if all(v["passed"] for v in g.values())]
    failed = {m: [k for k, v in g.items() if not v["passed"]] for m, g in gate_results.items()}
    if not passing:
        return {"model": None, "candidates": [], "failed": failed}
    best = max(sorted(passing), key=lambda m: graded["models"][m]["validation"]["capture_usd"])
    return {"model": best, "candidates": passing, "failed": failed}


def as_of_view(
    desk: Desk, test: pd.DataFrame, thresholds: Mapping[str, Thresholds], label: str, resamples: int, seed: int
) -> dict[str, Any]:
    """The same thresholds graded on the labels known at the as of cut."""
    g = prepare(desk, test, label=label)
    st = Stats(g)
    for m in desk.models:
        for k, v in event_columns(g, decide(desk, g, m, thresholds[m])).items():
            st.add(f"{m}:{k}", v)
    st.build(resamples, seed)
    return {
        "frauds_known": int(g.fraud.sum()),
        "models": {m: {"capture_usd": st.ratio(f"{m}:captured_usd", "fraud_usd")} for m in desk.models},
    }


# ---------------------------------------------------------------------------------------------
# The login desk's own questions


def per_attacker(graded: Mapping[str, Any], desk: Desk) -> dict[str, Any]:
    """Capture per attacker model and on real takeovers, at each model's operating point."""
    g: Graded = graded["_graded"]
    frame = g.frame
    synthetic = frame["synthetic"].astype(bool).to_numpy()
    kind = frame["attacker_model"].fillna("").astype(str).to_numpy()
    real = g.fraud & ~synthetic
    groups = {name: synthetic & (kind == name) for name in ("naive", "vpn", "targeted")}
    groups["takeover"] = real
    out: dict[str, Any] = {"counts": {k: int(v.sum()) for k, v in groups.items()}, "models": {}}
    for m in desk.models:
        d = graded["_decisions"][m]
        flagged = d != APPROVE
        out["models"][m] = {k: (float((flagged & v).sum() / v.sum()) if v.sum() else None) for k, v in groups.items()}
    return out


def rba_comparison(
    graded: Mapping[str, Any], published: Mapping[str, Any], history: tuple[int, int] = (10, 14)
) -> dict[str, Any]:
    """The Freeman reimplementation against the figures read from the data set repository.

    Risk score relation: median attacker score over median legitimate score, computed on the log
    score as the exponential of the difference of medians (the median commutes with a monotone
    map). Median re-authentication rate: for each attacker model and true positive rate, the
    threshold that challenges that share of the model's attacks; then, over users with between ten
    and fourteen earlier successful sign ins at a test sign in, the median share of those sign ins
    that would be challenged.
    """
    g: Graded = graded["_graded"]
    frame = g.frame
    s = g.scores("freeman")
    synthetic = frame["synthetic"].astype(bool).to_numpy()
    kind = frame["attacker_model"].fillna("").astype(str).to_numpy()
    prior = frame["n_success_prior"].to_numpy(np.float64)
    legit = ~g.fraud
    with_history = legit & (prior >= 1)
    band = legit & (prior >= history[0]) & (prior <= history[1])
    users = frame["user_key"].to_numpy()
    rsr: dict[str, float] = {}
    rates: dict[str, dict[str, float | None]] = {}
    base = float(np.median(s[with_history])) if with_history.any() else float("nan")
    for name in ("naive", "vpn", "targeted"):
        att = s[synthetic & (kind == name)]
        if len(att) == 0:
            continue
        rsr[name] = float(np.exp(float(np.median(att)) - base))
        rates[name] = {}
        for tpr_text in published["median_reauthentication_rate"]["by_attacker"].get(name, {}):
            tpr = float(tpr_text)
            t = float(np.quantile(att, 1.0 - tpr))
            flagged = s >= t
            sel = band
            if not sel.any():
                rates[name][tpr_text] = None
                continue
            per = pd.DataFrame({"u": users[sel], "f": flagged[sel]}).groupby("u")["f"].mean()
            rates[name][tpr_text] = float(per.median())
    return {
        "history_band": list(history),
        "users_in_band": int(pd.Series(users[band]).nunique()),
        "risk_score_relation": {"ours": rsr, "published": dict(published["risk_score_relation"]["attackers"])},
        "median_reauthentication_rate": {
            "ours": rates,
            "published": {k: dict(v) for k, v in published["median_reauthentication_rate"]["by_attacker"].items()},
        },
    }


# ---------------------------------------------------------------------------------------------
# Hypotheses, gates, cost and the interior


def hypotheses(
    registered: Mapping[str, Any],
    sets: Mapping[str, pd.DataFrame],
    desks_: Mapping[str, Desk],
    resamples: int = bootstrap.RESAMPLES,
    seed: int = bootstrap.SEED,
) -> list[dict[str, Any]]:
    """Each registered hypothesis tested once on validation, one sided, the family corrected."""
    rows: list[dict[str, Any]] = []
    for desk_name, items in registered.items():
        desk = desks_[desk_name]
        frame = sets[desk_name].sort_values(desk.key, kind="mergesort").reset_index(drop=True)
        codes, uniques = pd.factorize(frame[desk.user], sort=True)
        for hid, h in items.items():
            a = np.asarray(frame.eval(h["a"]), dtype=bool)
            b = np.asarray(frame.eval(h["b"]), dtype=bool)
            if h["measure"] == "failure":
                num = (~frame["success"].astype(bool)).to_numpy(np.float64)
                den = np.ones(len(frame))
            elif h["measure"] == "dollars":
                usd = np.clip(frame["amount_cents"].to_numpy(np.float64) / 100.0, 0, None)
                num = np.where(frame["label"].astype(bool), usd, 0.0)
                den = usd
            else:
                num = frame["label"].astype(bool).to_numpy(np.float64)
                den = np.ones(len(frame))
            cols = [num * a, den * a, num * b, den * b]
            matrix = bootstrap.by_user(codes.astype(np.int64), len(uniques), cols)
            sums = bootstrap.resampled_sums(matrix, resamples, seed)
            iv, p = bootstrap.one_sided_above(matrix, sums, (0, 1), (2, 3))
            totals = matrix.sum(axis=0)
            rows.append(
                {
                    "id": hid,
                    "desk": desk_name,
                    "question": h["question"],
                    "measure": h["measure"],
                    "a_rate": float(totals[0] / totals[1]) if totals[1] else None,
                    "b_rate": float(totals[2] / totals[3]) if totals[3] else None,
                    "a_events": int(a.sum()),
                    "b_events": int(b.sum()),
                    "a_positives": float((num * a).sum()) if h["measure"] != "dollars" else None,
                    "difference": {"estimate": iv.estimate, "low": iv.low, "high": iv.high},
                    "p": p,
                }
            )
    ps = [r["p"] for r in rows]
    for r, q, rej in zip(rows, multiple.adjusted(ps), multiple.bh(ps), strict=True):
        r["p_adjusted"] = q
        r["verdict"] = "supported" if rej else "not supported"
    return rows


def gates(desk: Desk, graded: Mapping[str, Any], bounds: Mapping[str, float]) -> dict[str, dict[str, Any]]:
    """The promotion gates the repository can decide; parity and latency come from serving."""
    out: dict[str, dict[str, Any]] = {}
    by_pair = {(c["a"], c["b"]): c for c in graded["comparisons"]}
    for m in desk.models:
        if m == "floor":
            continue
        e = graded["models"][m]
        t = e["test"]
        g: dict[str, Any] = {}
        pair = by_pair.get((m, "floor"))
        g["capture_above_floor"] = {
            "passed": bool(pair and pair["difference"]["low"] > 0),
            "value": pair["difference"] if pair else None,
        }
        steps_ok = t["step_ups_per_1000"]["estimate"] <= desk.budget.step_up_per_1000 * bounds["friction_factor"]
        decl_ok = t["declines_per_1000"]["estimate"] <= desk.budget.decline_per_1000 * bounds["friction_factor"]
        g["friction_within_budget"] = {
            "passed": bool(steps_ok and decl_ok),
            "value": {"step_ups": t["step_ups_per_1000"]["estimate"], "declines": t["declines_per_1000"]["estimate"]},
        }
        ece = e["calibration"]["ece"]
        g["calibration_error"] = {"passed": ece is not None and ece < bounds["ece"], "value": ece}
        g["score_stability"] = {
            "passed": e["psi_validation_to_test"] < bounds["psi"],
            "value": e["psi_validation_to_test"],
        }
        g["reason_code_coverage"] = {"passed": e["reasons"]["coverage"] >= 1.0, "value": e["reasons"]["coverage"]}
        out[m] = g
    return out


def expected_cost(desk: Desk, graded: Mapping[str, Any], costs: Mapping[str, float]) -> dict[str, Any]:
    """Cost per 1,000 events at each model's operating point, every parameter an assumption."""
    g: Graded = graded["_graded"]
    n = len(g.fraud)

    def total(model: str, c: Mapping[str, float]) -> float:
        d = graded["_decisions"][model]
        flagged = d != APPROVE
        missed = g.fraud & ~flagged
        good = ~g.fraud
        if desk.name == "payments":
            loss = g.usd[missed].sum() * c["fraud_loss_multiplier"] + missed.sum() * c["chargeback_handling_usd"]
            stepped = good & (d == STEP_UP)
            declined = good & (d == DECLINE)
            friction = stepped.sum() * c["step_up_cost_usd"]
            friction += g.usd[stepped].sum() * c["step_up_abandon_rate"] * c["decline_margin_rate"]
            friction += g.usd[declined].sum() * c["decline_margin_rate"] + declined.sum() * c["decline_attrition_usd"]
        else:
            loss = missed.sum() * c["takeover_loss_usd"]
            friction = (good & flagged).sum() * c["reauth_cost_usd"]
        return float((loss + friction) / n * 1000.0)

    keys = (
        ["fraud_loss_multiplier", "chargeback_handling_usd", "step_up_cost_usd", "step_up_abandon_rate",
         "decline_margin_rate", "decline_attrition_usd"]
        if desk.name == "payments"
        else ["reauth_cost_usd", "takeover_loss_usd"]
    )  # fmt: skip
    base = {m: total(m, costs) for m in desk.models}
    sensitivity = []
    for k in keys:
        for factor in (0.5, 2.0):
            c = dict(costs)
            c[k] = costs[k] * factor
            values = {m: total(m, c) for m in desk.models}
            best = min(sorted(values), key=lambda m: values[m])
            sensitivity.append({"parameter": k, "factor": factor, "per_1000": values, "cheapest": best})
    cheapest = min(sorted(base), key=lambda m: base[m])
    return {"per_1000_events": base, "cheapest": cheapest, "assumptions": {k: costs[k] for k in keys},
            "sensitivity": sensitivity}  # fmt: skip


def interior_budget(desk: Desk, graded: Mapping[str, Any], models: Sequence[str]) -> dict[str, Any]:
    """Test dollar capture of each model with the budget halved and doubled, thresholds re-chosen."""
    val: Graded = graded["_validation"]
    tst: Graded = graded["_graded"]
    out: dict[str, dict[str, float]] = {}
    for label, factor in (("half", 0.5), ("registered", 1.0), ("double", 2.0)):
        b = Budget(desk.budget.step_up_per_1000 * factor, desk.budget.decline_per_1000 * factor)
        row = {}
        for m in models:
            t = choose(desk, val, m, b)
            d = decide(desk, tst, m, t)
            cols = event_columns(tst, d)
            fusd = float(np.where(tst.fraud, tst.usd, 0).sum())
            row[m] = float(cols["captured_usd"].sum() / fusd) if fusd else float("nan")
        out[label] = row
    return out
