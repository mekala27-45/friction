"""Graph features against a brute force count over the events strictly before each one."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest
from friction_transforms import graph

pytestmark = pytest.mark.spark

T0 = datetime(2019, 1, 1, tzinfo=UTC)


def _payments(seed: int) -> tuple[list[dict], list[dict]]:
    rng = random.Random(seed)
    users = {f"u{u}": [f"u{u}c{c}" for c in range(rng.randint(1, 3))] for u in range(6)}
    rows, labels = [], []
    for i in range(160):
        user = rng.choice(sorted(users))
        card = rng.choice(users[user])
        # Seconds on a coarse grid, so many events share a second with another.
        ts = T0 + timedelta(seconds=rng.randrange(0, 20) * 3600 * 6)
        rows.append({"txn_ref": i, "account_key": card, "user_key": user, "merchant_key": f"m{rng.randrange(4)}",
                     "event_ts": ts})  # fmt: skip
        if rng.random() < 0.15:
            known = ts + timedelta(seconds=rng.randrange(0, 5) * 3600 * 6)
            labels.append({"event_ref": i, "is_fraud": True, "reported_at": known})
            if rng.random() < 0.3:
                labels.append({"event_ref": i, "is_fraud": False, "reported_at": known + timedelta(days=2)})
    return rows, labels


def _expected_payments(rows: list[dict], labels: list[dict]) -> dict[int, dict[str, float]]:
    card_of = {r["txn_ref"]: r for r in rows}
    known: dict[str, datetime] = {}
    for lab in labels:
        if lab["is_fraud"]:
            card = card_of[lab["event_ref"]]["account_key"]
            known[card] = min(known.get(card, lab["reported_at"]), lab["reported_at"])
    out = {}
    for r in rows:
        t, m = r["event_ts"], r["merchant_key"]
        before = [x for x in rows if x["merchant_key"] == m and x["event_ts"] < t]
        week = [x for x in before if x["event_ts"] >= t - timedelta(days=7)]
        first = {}
        for x in before:
            first[x["account_key"]] = min(first.get(x["account_key"], x["event_ts"]), x["event_ts"])
        cards = set(first)
        fraud_cards = {c for c in cards if c in known and known[c] < t}
        own_known = int(r["account_key"] in known and known[r["account_key"]] < t)
        user_cards = {x["account_key"] for x in rows if x["user_key"] == r["user_key"]}
        other = sum(1 for c in user_cards if c != r["account_key"] and c in known and known[c] < t)
        out[r["txn_ref"]] = {
            "m_cards_before": len(cards),
            "m_new_cards_7d": sum(1 for c, f in first.items() if f >= t - timedelta(days=7)),
            "m_txn_7d": len(week),
            "m_fraud_cards_known": len(fraud_cards),
            "m_fraud_share": len(fraud_cards) / len(cards) if cards else 0.0,
            "u_other_cards_known": other,
            "card_known_fraud": own_known,
        }
    return out


@pytest.mark.parametrize("seed", [1, 2])
def test_payment_graph_counts_only_what_came_before(spark, seed: int) -> None:  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    rows, labels = _payments(seed)
    silver = spark.createDataFrame(rows)
    lab = spark.createDataFrame(labels).select(
        "event_ref", "is_fraud", F.struct(F.col("reported_at"), F.lit(0).alias("seq")).alias("__START_AT")
    )
    got = {r["txn_ref"]: r.asDict() for r in graph.payments_graph(silver, lab).collect()}
    want = _expected_payments(rows, labels)
    assert set(got) == set(want)
    for ref, exp in want.items():
        for k, v in exp.items():
            assert got[ref][k] == pytest.approx(v), (ref, k, got[ref][k], v)


def _logins(seed: int) -> list[dict]:
    rng = random.Random(seed)
    out = []
    for i in range(150):
        unknown = rng.random() < 0.1
        out.append({
            "login_ref": i, "user_key": f"z{i}" if unknown else f"u{rng.randrange(8)}",
            "ip_key": f"i{rng.randrange(4)}", "v_asn": f"a{rng.randrange(2)}", "v_ua": f"ua{rng.randrange(3)}",
            "event_ts_ms": rng.randrange(0, 40) * 1_800_000, "synthetic": rng.random() < 0.1,
        })  # fmt: skip
    return out


def test_login_graph_counts_real_attempts_before_and_ignores_synthetic(spark) -> None:  # type: ignore[no-untyped-def]
    rows = _logins(3)
    got = {r["login_ref"]: r.asDict() for r in graph.logins_graph(spark.createDataFrame(rows)).collect()}
    real = [r for r in rows if not r["synthetic"]]
    for r in rows:
        t = r["event_ts_ms"]
        known = [x for x in real if not x["user_key"].startswith("z") and x["event_ts_ms"] < t]

        def first(key: str, xs: list[dict] = known, row: dict = r) -> dict[str, int]:
            seen: dict[str, int] = {}
            for x in xs:
                if x[key] == row[key]:
                    seen[x["user_key"]] = min(seen.get(x["user_key"], x["event_ts_ms"]), x["event_ts_ms"])
            return seen

        ip, asn, ua = first("ip_key"), first("v_asn"), first("v_ua")
        exp = {
            "ip_users_before": len(ip),
            "ip_new_users_1d": sum(1 for f in ip.values() if f >= t - graph.DAY_MS),
            "ip_unknown_1h": sum(
                1
                for x in real
                if x["ip_key"] == r["ip_key"]
                and x["user_key"].startswith("z")
                and t - graph.HOUR_MS <= x["event_ts_ms"] < t
            ),
            "asn_new_users_1h": sum(1 for f in asn.values() if f >= t - graph.HOUR_MS),
            "ua_users_before": len(ua),
        }
        assert {k: got[r["login_ref"]][k] for k in exp} == exp, r["login_ref"]
