"""Latency of the live API: the cold start measured apart, then p50 and p99 per endpoint.

    FRICTION_REPLAY_KEY=... python scripts/load_test.py --url https://<api> [--requests 200]

The replay key keeps the run out of the client budget and in a namespace of its own, so it never
touches what visitors scored. Writes results/latency.json.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

import httpx
from friction_core.secrets import single_line


def pct(values: list[float], q: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, round(q * (len(s) - 1)))]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "latency.json")
    args = parser.parse_args()
    key = single_line("FRICTION_REPLAY_KEY")
    headers = {"authorization": f"Bearer {key}", "x-friction-namespace": "loadtest"}
    client = httpx.Client(base_url=args.url, timeout=120)
    t = time.monotonic()
    first = client.get("/health")
    cold_ms = (time.monotonic() - t) * 1000
    result: dict[str, object] = {"url": args.url, "at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                 "cold_start_ms": round(cold_ms), "cold_status": first.status_code}  # fmt: skip
    for desk in ("payments", "logins"):
        parity = json.loads((ROOT / "results" / "parity" / f"{desk}.json").read_text(encoding="utf-8"))
        events = parity["events"][: args.requests]
        times = []
        for e in events:
            t = time.monotonic()
            r = client.post(f"/v1/{desk}/score", json=e, headers=headers)
            r.raise_for_status()
            times.append((time.monotonic() - t) * 1000)
        result[desk] = {"n": len(times), "p50_ms": round(pct(times, 0.5)), "p99_ms": round(pct(times, 0.99)),
                        "mean_ms": round(statistics.fmean(times))}  # fmt: skip
    args.out.write_text(json.dumps(result, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
