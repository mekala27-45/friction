"""Write packages/api/requirements-serving.txt: the serving image's packages, every one pinned to the
version uv.lock resolved and to the hashes of its files, so the image installs with --require-hashes.

    python scripts/lock_serving.py

The top level packages are in packages/api/requirements-serving.in; their dependencies are resolved
for the image's platform (Linux x86_64, CPython 3.12) under the lock's versions as constraints.
tests/api/test_serving_image.py checks every pin equals the lock.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HEADER = """# The serving image installs only what the scoring path imports (no pandas, Spark or DuckDB), every
# package pinned to the version in uv.lock and to the hashes of its files: pip runs --require-hashes.
# Written by scripts/lock_serving.py from requirements-serving.in; tests/api/test_serving_image.py
# checks the pins agree with the lock.
"""


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        exported = Path(tmp) / "lock.txt"
        subprocess.run(["uv", "export", "--frozen", "--no-hashes", "--all-extras", "--no-emit-project",
                        "--no-emit-workspace", "--format", "requirements-txt", "-o", str(exported)],
                       cwd=ROOT, check=True, capture_output=True)  # fmt: skip
        pins = [line.split(";")[0].strip() for line in exported.read_text().splitlines() if "==" in line]
        constraints = Path(tmp) / "constraints.txt"
        constraints.write_text("\n".join(pins) + "\n")
        out = subprocess.run(["uv", "pip", "compile", "packages/api/requirements-serving.in", "--generate-hashes",
                              "--python-version", "3.12", "--python-platform", "x86_64-manylinux_2_28",
                              "--constraints", str(constraints), "--no-annotate", "--no-header", "--quiet"],
                             cwd=ROOT, check=True, capture_output=True, text=True).stdout  # fmt: skip
    (ROOT / "packages" / "api" / "requirements-serving.txt").write_text(HEADER + out, encoding="utf-8")
    print(f"pinned {out.count('==')} packages with hashes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
