"""CI asserts that the dependencies tests skip without (a JVM, Postgres) were present in CI.

A skipped test passes; this makes sure CI never passes by skipping. Locally the same skips are
allowed and named.

    python scripts/check_test_dependencies.py
"""

from __future__ import annotations

import os
import shutil
import sys


def main() -> int:
    missing = []
    if shutil.which("java") is None:
        missing.append("java (the Spark tests)")
    try:
        import pyspark  # noqa: F401
    except ImportError:
        missing.append("pyspark (the Spark tests)")
    url = os.environ.get("FRICTION_TEST_DATABASE_URL")
    try:
        import psycopg

        if not url:
            raise RuntimeError("FRICTION_TEST_DATABASE_URL is not set")
        with psycopg.connect(url, connect_timeout=5) as conn:
            conn.execute("SELECT 1")
    except Exception as exc:
        missing.append(f"postgres (the API tests): {exc}")
    for m in missing:
        print(f"missing in CI: {m}")
    print("test dependencies: all present" if not missing else "test dependencies: some missing")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
