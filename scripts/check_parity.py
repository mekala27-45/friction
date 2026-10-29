"""Parity: replay the exported events through the API and compare with the lakehouse.

    python scripts/check_parity.py --local                       # in process, against FRICTION_TEST_DATABASE_URL
    python scripts/check_parity.py --url https://<api> --name live  # needs FRICTION_REPLAY_KEY in the environment

For each desk: seed a namespace of its own with the snapshot of the state just before the first
event, send every event in order, and compare each decision the lakehouse made with the API's:
the verdict exactly, the score within 1e-9. Writes the result under its name in results/parity.json
and exits non zero on any difference.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

TOLERANCE = 1e-9
ROUTES = {"payments": "/v1/payments/score", "logins": "/v1/logins/score"}


def replay(post: Any, desk: str, parity: dict[str, Any], key: str) -> dict[str, Any]:
    ns = f"parity-{desk}"
    auth = {"authorization": f"Bearer {key}"}
    r = post("/v1/admin/seed", {"namespace": ns, "snapshot": parity["snapshot"]}, auth)
    if r.status_code != 200:
        raise RuntimeError(f"seed failed: {r.status_code} {r.text[:200]}")
    expected = {e["event_ref"]: e for e in parity["expected"]}
    headers = {**auth, "x-friction-namespace": ns}
    mismatches: list[dict[str, Any]] = []
    compared = equal = codes_equal = 0
    worst = 0.0
    started = time.monotonic()
    for event in parity["events"]:
        resp = post(ROUTES[desk], event, headers)
        if resp.status_code != 200:
            raise RuntimeError(f"{desk} event {event['event_ref']}: {resp.status_code} {resp.text[:200]}")
        got = resp.json()
        exp = expected.get(event["event_ref"])
        if exp is None:
            continue
        compared += 1
        diff = abs(float(got["score"]) - float(exp["score"]))
        worst = max(worst, diff)
        same = got["verdict"] == exp["verdict"] and diff <= TOLERANCE
        equal += int(same)
        codes_equal += int(got["codes"] == exp["codes"])
        if not same and len(mismatches) < 5:
            mismatches.append({"event_ref": event["event_ref"], "expected": exp, "got": got})
    return {
        "events_sent": len(parity["events"]),
        "compared": compared,
        "decisions_equal": equal,
        "codes_equal": codes_equal,
        "max_abs_score_difference": worst,
        "mismatches": mismatches,
        "seconds": round(time.monotonic() - started, 1),
        "model": parity["model"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--name", default=None)
    parser.add_argument("--parity", type=Path, default=ROOT / "results" / "parity")
    parser.add_argument("--models", type=Path, default=ROOT / "results" / "models")
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "parity.json")
    args = parser.parse_args()
    if args.local:
        from fastapi.testclient import TestClient
        from friction_api.app import create_app
        from friction_api.bundle import Bundle
        from friction_api.store import Store

        key = "local-parity-key"
        store = Store(os.environ["FRICTION_TEST_DATABASE_URL"])
        store.migrate()
        bundles = {p.parent.name: Bundle.load(p.parent) for p in sorted(args.models.glob("*/bundle.json"))}
        # Entered once: a TestClient used without its context starts an event loop per request.
        client = TestClient(create_app(store, bundles, key)).__enter__()

        def post(path: str, body: Any, headers: dict[str, str]) -> Any:
            return client.post(path, json=body, headers=headers)

        name = args.name or "local"
    else:
        import httpx

        key = os.environ["FRICTION_REPLAY_KEY"]
        http = httpx.Client(base_url=args.url, timeout=60)

        def post(path: str, body: Any, headers: dict[str, str]) -> Any:
            return http.post(path, json=body, headers=headers)

        name = args.name or "live"
    result: dict[str, Any] = {"target": name, "url": args.url, "at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")}
    ok = True
    for desk in ("payments", "logins"):
        parity = json.loads((args.parity / f"{desk}.json").read_text(encoding="utf-8"))
        r = replay(post, desk, parity, key)
        result[desk] = r
        ok &= r["decisions_equal"] == r["compared"] and r["compared"] > 0
        print(f"{desk}: {r['decisions_equal']} of {r['compared']} decisions equal, worst score difference "
              f"{r['max_abs_score_difference']:.3g}, {r['events_sent']} events sent")  # fmt: skip
    existing = json.loads(args.out.read_text(encoding="utf-8")) if args.out.exists() else {}
    existing[name] = result
    args.out.write_text(json.dumps(existing, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
