"""The live check: the published site in a real browser, and its decisions read back by another client.

    python scripts/live_check.py --site https://mekala27-45.github.io/friction --api https://<api> --name awake

A headless Chromium opens every route and checks the statement, then sends the replay page's
events through the live API the way a visitor does. A separate client, with no browser and no
key, reads each decision the browser printed back from GET /v1/decisions/{id} and compares the
verdict and the score. Run once after the API has been idle long enough to scale down (asleep)
and once straight after (awake). Writes the result under its name in results/live_check.json and
exits non zero on any failure. Without --api only the routes are checked. With --video the session
is recorded, which is the demo recording.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
ROUTES = [
    "/",
    "/payments/",
    "/logins/",
    "/platform/",
    "/governance/",
    "/operations/",
    "/replay/",
    "/models/",
    "/report/",
]
EVENTS = 20
TOLERANCE = 1e-9


def compare(printed: list[dict[str, Any]], read_back: dict[str, dict[str, Any] | None]) -> list[dict[str, Any]]:
    """Slips whose decision the other client could not read, or read with another verdict or score."""
    out = []
    for slip in printed:
        got = read_back.get(slip["decision"])
        if got is None:
            out.append({"decision": slip["decision"], "problem": "not found"})
        elif got.get("verdict") != slip["verdict"] or abs(float(got.get("score", "nan")) - slip["score"]) > TOLERANCE:
            stored = [got.get("verdict"), got.get("score")]
            out.append({"decision": slip["decision"], "problem": "differs", "printed": [slip["verdict"], slip["score"]],
                        "stored": stored})  # fmt: skip
    return out


def failures(result: dict[str, Any], api: bool) -> list[str]:
    bad = [f"route {r['route']}" for r in result["routes"] if r["status"] != 200 or not r["statement"]]
    if api:
        replay = result.get("replay", {})
        if replay.get("mode") != "live":
            bad.append(f"replay mode {replay.get('mode')}")
        if replay.get("live_slips", 0) < EVENTS:
            bad.append(f"{replay.get('live_slips', 0)} of {EVENTS} slips live")
        bad += [f"decision {d['decision']} {d['problem']}" for d in replay.get("mismatches", [])]
    return bad


def check(site: str, api: str | None, video: Path | None = None) -> dict[str, Any]:
    import httpx
    from playwright.sync_api import sync_playwright

    result: dict[str, Any] = {"routes": []}
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        size = {"width": 390, "height": 844}
        context = browser.new_context(viewport=size, **({"record_video_dir": str(video), "record_video_size": size}
                                                        if video else {}))  # fmt: skip
        page = context.new_page()
        for route in ROUTES:
            started = time.monotonic()
            response = page.goto(site.rstrip("/") + route, wait_until="networkidle")
            if response is not None and response.status == 503:  # a sleeping host's first answer
                response = page.goto(site.rstrip("/") + route, wait_until="networkidle")
            statement = page.get_by_test_id("statement")
            text = statement.first.inner_text() if statement.count() else ""
            result["routes"].append({"route": route, "status": response.status if response else None,
                                     "statement": "synthetic" in text,
                                     "seconds": round(time.monotonic() - started, 2)})  # fmt: skip
        if api:
            page.goto(site.rstrip("/") + "/replay/", wait_until="networkidle")
            started = time.monotonic()
            page.get_by_role("button", name="Send").click()
            mode = page.get_by_test_id("replay-mode")
            page.wait_for_function(
                "document.querySelector('[data-testid=replay-mode]').textContent.length > 0", timeout=180_000
            )
            to_mode = round(time.monotonic() - started, 2)
            slips = page.get_by_test_id("slip")
            deadline = time.monotonic() + 180
            while slips.count() < EVENTS and time.monotonic() < deadline:
                time.sleep(0.5)
            to_all = round(time.monotonic() - started, 2)
            printed = []
            for i in range(slips.count()):
                slip = slips.nth(i)
                decision = slip.get_attribute("data-decision")
                if decision:
                    printed.append({"decision": decision, "verdict": slip.get_attribute("data-verdict"),
                                    "score": float(slip.get_attribute("data-score") or "nan")})  # fmt: skip
            text = mode.inner_text()
            result["replay"] = {
                "mode": "live" if "live" in text else "recorded" if "recorded" in text else text,
                "seconds_to_mode": to_mode, "seconds_to_all_slips": to_all, "slips": slips.count(),
                "live_slips": len(printed),
            }  # fmt: skip
        context.close()  # writes the recording, when one was asked for
        browser.close()
    if api:
        # The other client: no browser, no key, no cookie; only the decision ids the page printed.
        with httpx.Client(base_url=api, timeout=60) as client:
            read_back: dict[str, dict[str, Any] | None] = {}
            for slip in printed:
                r = client.get(f"/v1/decisions/{slip['decision']}")
                read_back[slip["decision"]] = r.json() if r.status_code == 200 else None
            health = client.get("/health").json()
        result["replay"]["mismatches"] = compare(printed, read_back)
        result["replay"]["decisions_read_back"] = sum(v is not None for v in read_back.values())
        result["health"] = {"version": health.get("version"),
                            "models": {k: v.get("model") for k, v in health.get("models", {}).items()}}  # fmt: skip
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", required=True)
    parser.add_argument("--api")
    parser.add_argument("--name", required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "live_check.json")
    parser.add_argument("--video", type=Path, help="a folder for a recording of the session (the demo)")
    args = parser.parse_args()
    started = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = {"at": started, "site": args.site, **check(args.site, args.api, args.video)}
    result["failures"] = failures(result, bool(args.api))
    existing = json.loads(args.out.read_text(encoding="utf-8")) if args.out.exists() else {}
    existing[args.name] = result
    args.out.write_text(json.dumps(existing, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "routes"}, indent=1))
    return 1 if result["failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
