"""Probe the hosts the build depends on, from wherever it runs. Status codes only.

python scripts/probe_hosts.py [--api https://<api>]
"""

from __future__ import annotations

import argparse
import json
import sys

import httpx

HOSTS = ["https://github.com", "https://api.github.com", "https://mekala27-45.github.io/friction/",
         "https://console.neon.tech", "https://vercel.com"]  # fmt: skip


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api")
    args = parser.parse_args()
    out = {}
    for url in [*HOSTS, *([f"{args.api}/health"] if args.api else [])]:
        try:
            out[url] = httpx.get(url, timeout=20, follow_redirects=True).status_code
        except httpx.HTTPError as exc:
            out[url] = f"unreachable: {type(exc).__name__}"
    print(json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
