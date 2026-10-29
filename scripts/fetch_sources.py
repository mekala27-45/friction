"""Fetch both sources, verify them against data/sources.lock.json, and record what was asked for.

Runs in GitHub Actions for the workspace (then uploads to the raw volume with the Databricks CLI,
see .github/workflows/workspace.yml) and on the build machine for the first look and the local
run. TabFormer is tried through Git LFS first and the IBM Box link second; the status file records
which one served it. A digest that differs from the lock fails the fetch and nothing is unpacked.

    python scripts/fetch_sources.py            # fetch what is missing, verify, unpack
    python scripts/fetch_sources.py --verify   # verify only
"""

from __future__ import annotations

import argparse
import json
import sys
import tarfile
import time
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from friction_core.hashing import sha256_file
from friction_core.paths import raw_dir

LOCK = ROOT / "data" / "sources.lock.json"
STATUS = ROOT / "results" / "fetch_status.json"


class DigestMismatch(ValueError):
    pass


def load_lock() -> dict[str, dict[str, object]]:
    data = json.loads(LOCK.read_text(encoding="utf-8"))
    return {k: v for k, v in data.items() if not k.startswith("_")}


def verify(path: Path, expected: str, size: int | None = None) -> str:
    if size is not None and path.stat().st_size != size:
        raise DigestMismatch(f"{path.name}: size {path.stat().st_size} is not the locked {size}")
    digest = sha256_file(path)
    if digest != expected:
        raise DigestMismatch(f"{path.name}: sha256 {digest[:12]} is not the locked {expected[:12]}")
    return digest


def download(url: str, target: Path) -> None:
    tmp = target.with_suffix(target.suffix + ".part")
    with urllib.request.urlopen(url, timeout=120) as response, tmp.open("wb") as out:
        while block := response.read(1 << 20):
            out.write(block)
    tmp.replace(target)


def unpack(archive: Path, member: str, folder: Path) -> Path:
    out = folder / member
    if out.exists():
        return out
    if archive.suffix == ".zip":
        with zipfile.ZipFile(archive) as z:
            z.extract(member, folder)
    else:
        with tarfile.open(archive) as t:
            t.extract(member, folder, filter="data")
    return out


def fetch(name: str, entry: dict[str, object], *, verify_only: bool) -> dict[str, object]:
    folder = raw_dir() / name
    folder.mkdir(parents=True, exist_ok=True)
    archive = folder / str(entry["archive"])
    record: dict[str, object] = {"source": name, "asked_at": datetime.now(UTC).isoformat(timespec="seconds")}
    served_by = "already present"
    if not archive.exists():
        if verify_only:
            raise FileNotFoundError(f"{archive} is missing and --verify was given")
        errors: list[str] = []
        for url in entry["urls"]:  # type: ignore[attr-defined]
            try:
                started = time.monotonic()
                download(str(url), archive)
                served_by = str(url)
                record["download_seconds"] = round(time.monotonic() - started, 1)
                break
            except OSError as exc:
                errors.append(f"{url}: {exc}")
        else:
            raise RuntimeError(f"{name}: every URL failed: {errors}")
    record["served_by"] = served_by
    record["archive_sha256"] = verify(archive, str(entry["sha256"]), int(entry["size"]))  # type: ignore[call-overload]
    member = unpack(archive, str(entry["member"]), folder)
    record["member_sha256"] = verify(member, str(entry["member_sha256"]), int(entry["member_size"]))  # type: ignore[call-overload]
    record["result"] = "verified"
    return record


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--only", choices=["tabformer", "rba"])
    args = parser.parse_args()
    records = []
    for name, entry in load_lock().items():
        if args.only and name != args.only:
            continue
        records.append(fetch(name, entry, verify_only=args.verify))
        print(f"{name}: {records[-1]['result']} ({records[-1]['served_by']})")
    STATUS.parent.mkdir(parents=True, exist_ok=True)
    STATUS.write_text(json.dumps({"fetches": records}, indent=1) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
