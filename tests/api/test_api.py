"""The API against a real Postgres: decisions observed independently, the budget, parity."""

from __future__ import annotations

import json
from typing import Any

import numpy as np
import pytest
from friction_features import payments as payment_features

from tests.api.conftest import REPLAY_KEY, key_headers, login, new_namespace, payment

pytestmark = pytest.mark.postgres


def test_health_reports_models_digests_and_the_statement(client) -> None:  # type: ignore[no-untyped-def]
    body = client.get("/health").json()
    assert body["models"]["payments"]["model"] == "logistic"
    assert len(body["models"]["payments"]["files"]["model.json"]) == 64
    assert "synthetic" in body["statement"]


def test_decision_is_committed(client, store) -> None:  # type: ignore[no-untyped-def]
    r = client.post("/v1/payments/score", json=payment(1))
    assert r.status_code == 200, r.text
    did = r.json()["decision_id"]
    # A separate connection, opened after the response, sees the row.
    with store.connect() as conn:
        row = conn.execute("SELECT verdict, event_ref FROM decision_log WHERE id = %s", (did,)).fetchone()
    assert row is not None and row[1] == 1
    assert client.get(f"/v1/decisions/{did}").json()["event_ref"] == 1


def test_audit_precedes_response(client, store, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from friction_api import store as store_module

    def broken(self: Any, *a: Any, **k: Any) -> str:
        raise RuntimeError("the decision log is down")

    monkeypatch.setattr(store_module.Tx, "log", broken)
    with pytest.raises(RuntimeError):
        client.post("/v1/payments/score", json=payment(2))
    with store.connect() as conn:
        assert conn.execute("SELECT count(*) FROM decision_log").fetchone()[0] == 0


def test_state_and_decision_share_a_transaction(client, store, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from friction_api import store as store_module

    assert client.post("/v1/payments/score", json=payment(3)).status_code == 200
    before = store.state("public", "card", "a_card_one")
    original = store_module.Tx.log

    def fails_after_state(self: Any, *a: Any, **k: Any) -> str:
        original(self, *a, **k)
        raise RuntimeError("after the write")

    monkeypatch.setattr(store_module.Tx, "log", fails_after_state)
    with pytest.raises(RuntimeError):
        client.post("/v1/payments/score", json=payment(4, minute=5))
    # Neither the new state nor the new decision survived: they were one transaction.
    assert store.state("public", "card", "a_card_one") == before
    with store.connect() as conn:
        assert conn.execute("SELECT count(*) FROM decision_log WHERE event_ref = 4").fetchone()[0] == 0


def test_client_budget_refuses_past_the_cap(client) -> None:  # type: ignore[no-untyped-def]
    codes = [client.post("/v1/payments/score", json=payment(10 + i, minute=i)).status_code for i in range(7)]
    assert codes[:5] == [200] * 5 and codes[5:] == [429, 429]
    # The replay key is not budgeted.
    assert client.post("/v1/payments/score", json=payment(20, minute=9), headers=key_headers()).status_code == 200


def test_a_namespace_needs_the_key(client) -> None:  # type: ignore[no-untyped-def]
    r = client.post("/v1/payments/score", json=payment(30), headers={"x-friction-namespace": "parity"})
    assert r.status_code == 403
    assert client.post("/v1/admin/seed", json={"namespace": "x", "snapshot": {}}).status_code == 403
    bad = {"authorization": "Bearer wrong" + REPLAY_KEY}
    assert client.post("/v1/admin/seed", json={"namespace": "x", "snapshot": {}}, headers=bad).status_code == 403


def test_raw_values_are_tokenized_and_never_stored(client, store) -> None:  # type: ignore[no-untyped-def]
    body = login(40)
    for k in ("user_key", "ip_key", "ua_key"):
        body.pop(k)
    body.update(user_id="12345", ip_address="10.20.30.40", user_agent="Mozilla/5.0 (test)")
    assert client.post("/v1/logins/score", json=body).status_code == 200
    with store.connect() as conn:
        dump = json.dumps([list(r) for r in conn.execute("SELECT key, state::text FROM online_state").fetchall()])
        dump += json.dumps([list(map(str, r)) for r in conn.execute("SELECT * FROM decision_log").fetchall()])
    assert "10.20.30.40" not in dump and "Mozilla" not in dump and "12345" not in dump


def test_online_state_after_a_replay_equals_the_offline_state(client, store, bundles) -> None:  # type: ignore[no-untyped-def]
    """Parity in miniature: the same events through the API and through the state machine."""
    from friction_api.bundle import Bundle
    from friction_api.service import PaymentIn, payment_event

    ns = new_namespace()
    issued = [1561939200 - 40 * 86400]
    seed = {"card_issues": {"a_card_one": issued}}
    assert (
        client.post("/v1/admin/seed", json={"namespace": ns, "snapshot": seed}, headers=key_headers()).status_code
        == 200
    )
    events = [payment(100 + i, minute=i * 3, amount_cents=500 + 700 * i, mcc=5411 if i % 2 else 5812) for i in range(8)]
    online = [client.post("/v1/payments/score", json=e, headers=key_headers(ns)).json() for e in events]
    b = Bundle.load(bundles["payments"])
    state = payment_features.CardState()
    for e, got in zip(events, online, strict=True):
        _, ev = payment_event(PaymentIn(**e), b, issued)
        score, _ = b.score(state.features(ev))
        state.update(ev)
        assert got["score"] == score
    assert store.state(ns, "card", "a_card_one") == json.loads(json.dumps(state.snapshot()))


def test_unknown_account_attempts_are_strangers(client, store) -> None:  # type: ignore[no-untyped-def]
    for i in range(3):
        assert client.post("/v1/logins/score", json=login(200 + i, i, user_key="u_unknown")).status_code == 200
    assert store.state("public", "user", "u_unknown") is None
    assert store.state("public", "ip", "i_home")["seen"]


def test_login_floor_steps_up_a_new_country_after_history(client) -> None:  # type: ignore[no-untyped-def]
    for i in range(3):
        assert client.post("/v1/logins/score", json=login(300 + i, i)).json()["verdict"] == "approve"
    r = client.post("/v1/logins/score", json=login(310, 20, country="BR", asn=7738)).json()
    assert r["verdict"] == "step_up" and "L02" in r["codes"]


def test_the_container_layout_starts(bundles, store, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """In the image the module is three folders deep: /app/src/friction_api/app.py. The repository
    fallback for the models folder must not be worked out when FRICTION_MODELS is set (the first
    Vercel deployment failed on exactly that)."""
    import os

    from friction_api import app as app_module

    monkeypatch.setattr(app_module, "__file__", "/app/src/friction_api/app.py")
    monkeypatch.setenv("FRICTION_MODELS", str(next(iter(bundles.values())).parent))
    monkeypatch.setenv("DATABASE_URL", os.environ["FRICTION_TEST_DATABASE_URL"])
    from fastapi.testclient import TestClient

    health = TestClient(app_module.from_environment()).get("/health").json()
    assert sorted(health["models"]) == ["logins", "payments"]


def test_the_image_reports_the_package_version() -> None:
    from importlib.metadata import version

    import friction_api

    assert friction_api.__version__ == version("friction-api")


def test_a_snapshot_seeded_in_parts_equals_one_seeded_whole(client, store) -> None:  # type: ignore[no-untyped-def]
    from scripts.check_parity import parts

    snapshot = {"card": {f"c{i}": {"n": i, "pad": "x" * 50} for i in range(30)},
                "card_issues": {f"c{i}": [1_600_000_000 + i] for i in range(30)}}  # fmt: skip
    pieces = parts(snapshot, limit=400)
    assert len(pieces) > 3
    assert sum(len(p.get("card", {})) for p in pieces) == 30
    ns = new_namespace()
    for i, part in enumerate(pieces):
        r = client.post("/v1/admin/seed", json={"namespace": ns, "snapshot": part, "replace": i == 0},
                        headers=key_headers())  # fmt: skip
        assert r.status_code == 200
    assert store.state(ns, "card", "c29")["n"] == 29
    whole = new_namespace()
    client.post("/v1/admin/seed", json={"namespace": whole, "snapshot": snapshot}, headers=key_headers())
    for i in (0, 15, 29):
        assert store.state(ns, "card", f"c{i}") == store.state(whole, "card", f"c{i}")
    client.post("/v1/admin/seed", json={"namespace": ns, "snapshot": pieces[0]}, headers=key_headers())
    assert store.state(ns, "card", "c29") is None  # a first part replaces what was there


def _shadow_client(store, bundles, tmp_path, thresholds):  # type: ignore[no-untyped-def]
    from fastapi.testclient import TestClient
    from friction_api.app import create_app
    from friction_api.bundle import Bundle

    from tests.api.conftest import write_bundle

    model = (bundles["payments"] / "model.json").read_bytes()
    shadow = write_bundle(tmp_path / "shadow", "payments", "logistic", {"model.json": model}, thresholds)
    loaded = {k: Bundle.load(v) for k, v in bundles.items()}
    return TestClient(create_app(store, loaded, REPLAY_KEY, budget=50, shadows={"payments": Bundle.load(shadow)}))


def test_the_shadow_scores_the_champions_vector_and_never_decides(store, bundles, tmp_path) -> None:  # type: ignore[no-untyped-def]
    # The same model with a step up threshold of zero: the same score on every event, another verdict.
    client = _shadow_client(store, bundles, tmp_path, {"step_up": 0.0, "decline": 2.0})
    out = [client.post("/v1/payments/score", json=payment(500 + i, i, amount_cents=999 + 50_000 * i)).json()
           for i in range(4)]  # fmt: skip
    assert all("shadow" not in o for o in out)
    for o in out:
        row = client.get(f"/v1/decisions/{o['decision_id']}").json()
        assert row["verdict"] == o["verdict"] and row["shadow"]["score"] == o["score"]
        assert row["shadow"]["verdict"] == "step_up" and row["shadow"]["model"] == "logistic"
    summary = client.get("/v1/shadow/payments").json()
    assert summary["decisions"] == summary["shadowed"] == 4 and summary["shadow_flagged"] == 4
    assert summary["mean_abs_score_gap"] == 0.0
    assert client.get("/health").json()["shadows"]["payments"]["model"] == "logistic"


def test_the_shadow_summary_counts_only_the_decisions_the_shadow_scored(store) -> None:  # type: ignore[no-untyped-def]
    ns = new_namespace()
    with store.transaction(ns) as tx:
        for i in range(3):  # logged before a shadow was loaded
            tx.log("payments", i, "c", 0.9, "decline", ["P01"], "lightgbm", "d")
        tx.log("payments", 10, "c", 0.9, "decline", ["P01"], "lightgbm", "d",
               shadow={"model": "logistic", "score": 0.1, "verdict": "approve"})  # fmt: skip
        tx.log("payments", 11, "c", 0.1, "approve", [], "lightgbm", "d",
               shadow={"model": "logistic", "score": 0.1, "verdict": "approve"})  # fmt: skip
    s = store.shadow_summary(ns, "payments")
    assert (s["decisions"], s["shadowed"], s["champion_flagged"], s["shadow_flagged"]) == (5, 2, 1, 0)
    assert s["same_verdict"] == 1 and s["both_flagged"] == 0


def test_a_shadow_that_reads_another_vector_refuses_to_load(store, bundles) -> None:  # type: ignore[no-untyped-def]
    from friction_api.app import create_app
    from friction_api.bundle import Bundle

    loaded = {k: Bundle.load(v) for k, v in bundles.items()}
    with pytest.raises(ValueError, match="does not read the vector"):
        create_app(store, loaded, REPLAY_KEY, shadows={"payments": loaded["logins"]})


def _sequence_client(store, bundles, tmp_path):  # type: ignore[no-untyped-def]
    from fastapi.testclient import TestClient
    from friction_api.app import create_app
    from friction_api.bundle import Bundle
    from friction_models import sequence

    from tests.api.conftest import write_bundle

    p = sequence.init()
    p["bo"] = p["bo"] + 3.0  # every score high, so every online event is stepped up and carries codes
    gru = sequence.to_json(p, np.zeros(sequence.WIDTH, np.float32)).encode()
    shadow = write_bundle(tmp_path / "gru", "payments", "gru", {"payments_gru.json": gru},
                          {"step_up": 0.5, "decline": 0.999})  # fmt: skip
    loaded = {k: Bundle.load(v) for k, v in bundles.items()}
    return TestClient(create_app(store, loaded, REPLAY_KEY, budget=100, shadows={"payments": Bundle.load(shadow)}))


def test_the_sequence_shadow_keeps_the_last_events_of_each_card(store, bundles, tmp_path) -> None:  # type: ignore[no-untyped-def]
    from friction_models import sequence

    client = _sequence_client(store, bundles, tmp_path)
    ns = new_namespace()
    amounts = [1000 + 37 * i for i in range(sequence.LAGS + 4)]
    scores = []
    for i, cents in enumerate(amounts):
        o = client.post("/v1/payments/score", json=payment(900 + i, i, amount_cents=cents), headers=key_headers(ns))
        assert o.status_code == 200, o.text
        row = client.get(f"/v1/decisions/{o.json()['decision_id']}").json()
        assert row["verdict"] == o.json()["verdict"] and row["model"] != "gru"  # the champion decides
        assert row["shadow"]["model"] == "gru" and row["shadow"]["verdict"] == "step_up" and row["shadow"]["codes"]
        scores.append(row["shadow"]["score"])
    steps = store.state(ns, "card_steps", "a_card_one")["steps"]
    assert len(steps) == sequence.LAGS  # the oldest four fell off
    amount, hour, channel = (sequence.STEP.index(c) for c in ("amount_usd", "hour", "channel"))
    assert [s[amount] for s in steps] == [float(np.float32(c / 100)) for c in amounts[-sequence.LAGS :]]
    assert {s[hour] for s in steps} == {10.0} and {s[channel] for s in steps} == {2.0}
    assert len(set(scores)) > 1  # the same network, another history, another score
    said = client.get(f"/v1/admin/shadow/{ns}/payments", headers=key_headers()).json()["decisions"]
    assert [d["event_ref"] for d in said] == [900 + i for i in range(len(amounts))]
    assert [d["shadow"]["score"] for d in said] == scores
    assert client.get(f"/v1/admin/shadow/{ns}/payments").status_code == 403
    # A card's history is its own: another card starts from nothing.
    other = client.post("/v1/payments/score", json=payment(990, 0, account_key="a_card_two"), headers=key_headers(ns))
    assert other.status_code == 200
    assert len(store.state(ns, "card_steps", "a_card_two")["steps"]) == 1


def test_the_sequence_history_rolls_back_with_the_decision(store, bundles, tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from friction_api import store as store_module

    client = _sequence_client(store, bundles, tmp_path)
    assert client.post("/v1/payments/score", json=payment(950)).status_code == 200
    before = store.state("public", "card_steps", "a_card_one")

    def broken(self: Any, *a: Any, **k: Any) -> str:
        raise RuntimeError("the decision log is down")

    monkeypatch.setattr(store_module.Tx, "log", broken)
    with pytest.raises(RuntimeError):
        client.post("/v1/payments/score", json=payment(951, minute=3))
    assert store.state("public", "card_steps", "a_card_one") == before
