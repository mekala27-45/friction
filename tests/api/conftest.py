"""A real Postgres, a tiny bundle per desk, and the app built over them."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pytest
from friction_core.hashing import sha256_bytes
from friction_models.learners import Logistic

REPLAY_KEY = "replay-key-for-tests-only"


def write_bundle(folder: Path, desk: str, model: str, files: dict[str, bytes], thresholds: dict[str, float]) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    for name, data in files.items():
        (folder / name).write_bytes(data)
    from friction_reasons.reasons import feature_to_code, load_table

    from tests.conftest import ROOT

    table = load_table(ROOT / "evaluation" / "reason_codes.yaml")
    meta = {
        "desk": desk,
        "model": model,
        "status": "test",
        "thresholds": thresholds,
        "files": {name: sha256_bytes(data) for name, data in files.items()},
        "reason_map": feature_to_code(table, desk),
    }
    (folder / "bundle.json").write_text(json.dumps(meta, indent=1), encoding="utf-8")
    return folder


@pytest.fixture
def bundles(tmp_path: Path) -> dict[str, Path]:
    from friction_api.bundle import PAY_FEATURES

    rng = np.random.default_rng(0)
    x = rng.normal(size=(400, len(PAY_FEATURES)))
    y = (x[:, 0] + rng.normal(size=400) > 1.5).astype(int)
    lr = Logistic.fit(x, y)
    rates = {"2019-07-01": {"5411": 0.002}}
    pay = write_bundle(
        tmp_path / "models" / "payments", "payments", "logistic",
        {"model.json": json.dumps(lr.to_dict()).encode(), "mcc_rates.json": json.dumps(rates).encode()},
        {"step_up": 0.5, "decline": 0.9},
    )  # fmt: skip
    log = write_bundle(
        tmp_path / "models" / "logins", "logins", "floor",
        {"unknown_accounts.json": json.dumps(["u_unknown"]).encode()},
        {"step_up": 1.0, "decline": 4.0},
    )  # fmt: skip
    return {"payments": pay, "logins": log}


@pytest.fixture
def store() -> Iterator[object]:
    from friction_api.store import Store

    url = os.environ["FRICTION_TEST_DATABASE_URL"]
    s = Store(url)
    s.migrate()
    with s.connect() as conn:
        conn.execute("TRUNCATE online_state, card_issues, decision_log, client_budget")
        conn.commit()
    yield s


@pytest.fixture
def client(store: object, bundles: dict[str, Path]):  # type: ignore[no-untyped-def]
    from fastapi.testclient import TestClient
    from friction_api.app import create_app
    from friction_api.bundle import Bundle

    app = create_app(store, {k: Bundle.load(v) for k, v in bundles.items()}, REPLAY_KEY, budget=5)  # type: ignore[arg-type]
    return TestClient(app)


def payment(ref: int, minute: int = 0, **over: object) -> dict[str, object]:
    body: dict[str, object] = {
        "event_ref": ref, "ts": f"2019-07-01T10:{minute:02d}:00", "account_key": "a_card_one", "amount_cents": 2599,
        "merchant_key": "m_shop", "mcc": 5411, "channel": "online", "location": "", "foreign": False,
        "has_error": False,
    }  # fmt: skip
    body.update(over)
    return body


def login(ref: int, second: int = 0, **over: object) -> dict[str, object]:
    body: dict[str, object] = {
        "event_ref": ref, "ts": f"2020-12-20T08:00:{second:02d}.250", "user_key": "u_one", "ip_key": "i_home",
        "ua_key": "u_phone", "asn": 29695, "country": "NO", "browser": "Chrome Mobile", "os": "Android",
        "device_type": "mobile", "rtt_ms": None, "success": True,
    }  # fmt: skip
    body.update(over)
    return body


def key_headers(namespace: str | None = None) -> dict[str, str]:
    h = {"authorization": f"Bearer {REPLAY_KEY}"}
    if namespace:
        h["x-friction-namespace"] = namespace
    return h


def new_namespace() -> str:
    return f"t{uuid.uuid4().hex[:8]}"
