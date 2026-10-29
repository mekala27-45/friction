"""The out of process check: start the server, score a payment, read it back from another process."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from tests.api.conftest import payment

pytestmark = pytest.mark.postgres

READER = """
import json, sys, psycopg
url, decision_id = sys.argv[1], sys.argv[2]
with psycopg.connect(url) as conn:
    d = conn.execute("SELECT event_ref, verdict FROM decision_log WHERE id = %s", (decision_id,)).fetchone()
    s = conn.execute("SELECT state FROM online_state WHERE namespace = 'public' AND kind = 'card' AND key = %s",
                     ("a_card_one",)).fetchone()
print(json.dumps({"decision": list(d) if d else None, "n": s[0]["n"] if s else None}))
"""


def _port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_out_of_process(bundles: dict[str, Path], store: object) -> None:
    url = os.environ["FRICTION_TEST_DATABASE_URL"]
    port = _port()
    env = {**os.environ, "DATABASE_URL": url, "FRICTION_MODELS": str(bundles["payments"].parent)}
    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "friction_api.main:app", "--port", str(port), "--log-level", "warning"],
        env=env,
    )
    try:
        base = f"http://127.0.0.1:{port}"
        for _ in range(100):
            try:
                if httpx.get(f"{base}/health", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.1)
        r = httpx.post(f"{base}/v1/payments/score", json=payment(7), timeout=10)
        assert r.status_code == 200, r.text
        did = r.json()["decision_id"]
        out = subprocess.run([sys.executable, "-c", READER, url, did], capture_output=True, text=True, check=True)
        seen = json.loads(out.stdout)
        assert seen["decision"] == [7, r.json()["verdict"]] and seen["n"] == 1
    finally:
        server.terminate()
        server.wait(timeout=10)
