"""The integration job: idempotency, order and late label tests, in the workspace, on real drops.

Runs only under the friction_it prefix, in schemas and a pipeline of its own, because it resets its
tables between tests. Drops are copied from the dev outbox volume. Each test lands drops, runs a
pipeline update and digests every bronze, silver, CDC and gold table:

1. Re-landing a day: land replay day 10 of both desks, run, digest; land it again and run again,
   then copy its files over themselves and run again; the digests must be identical each time.
2. Order: days 11 to 13 landed in order, one update each, digest; reset; the same days landed
   13, 11, 12 (the late events and the later label versions first), digest; identical.
3. A backfill over a processed range: land days 11 to 13 again and run; nothing changes.
4. Labels out of order: the payment months the replay window's chargebacks point back to (August
   to November 2019), then the label drops 0345 to 0359 at once, digest; reset; the same months,
   then the label drops in three groups delivered latest first; the label history must be identical
   and not empty.
5. The month rerun (tests=month, part of the rederive, never part of all): reset, land the first
   drop of every feed, run, compute the features, and digest the nine tables the export digests,
   for comparison with the open source engine's run of the same month.

Writes integration.json to the exports volume and fails the task if any test failed.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from typing import Any

import _boot

args = _boot.parse(
    [
        ("--pipeline-id", {"required": True}),
        ("--source-outbox", {"required": True}),
        ("--day", {"type": int, "default": 10}),
        ("--tests", {"default": "all", "help": "all, or a comma list of: relanding, order, labels, month"}),
    ]
)
spark = _boot.session()
from friction_transforms import digests  # noqa: E402
from friction_transforms import integration as it  # noqa: E402

it.guard(args.prefix)
ledger = _boot.Ledger(spark, args, "integration")
landing = _boot.volume(args, "landing")
log: list[dict[str, Any]] = []


def note(what: str, **detail: Any) -> None:
    log.append({"at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "what": what, **detail})
    print(what, detail)


def update(full_refresh: bool = False) -> float:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    started = time.monotonic()
    u = w.pipelines.start_update(pipeline_id=args.pipeline_id, full_refresh=full_refresh)
    while True:
        info = w.pipelines.get_update(pipeline_id=args.pipeline_id, update_id=u.update_id).update
        state = info.state.value if info and info.state else "UNKNOWN"
        if state == "COMPLETED":
            break
        if state in ("FAILED", "CANCELED"):
            raise RuntimeError(f"pipeline update {u.update_id} ended {state}")
        time.sleep(15)
    secs = round(time.monotonic() - started, 1)
    note("pipeline update", full_refresh=full_refresh, update_id=u.update_id, seconds=secs)
    return secs


def reset() -> None:
    for feeds in it.FEEDS.values():
        for feed in feeds:
            shutil.rmtree(f"{landing}/{feed}", ignore_errors=True)
            os.makedirs(f"{landing}/{feed}/drops", exist_ok=True)
    note("landing cleared")
    update(full_refresh=True)


def land(day: int, overwrite: bool = False) -> list[str]:
    """Copy replay day N of both desks, every feed of the desk, skipping files already there unless
    overwrite is set (a resent file under its old name)."""
    copied: list[str] = []
    for desk, feeds in it.FEEDS.items():
        drop = it.day_drop(os.listdir(f"{args.source_outbox}/{desk}/drops"), day)
        for feed in feeds:
            base = f"{args.source_outbox}/{feed}/drops"
            for name in it.same_date(os.listdir(base) if os.path.isdir(base) else [], drop):
                for f in sorted(os.listdir(f"{base}/{name}")):
                    dst = f"{landing}/{feed}/drops/{name}/{f}"
                    same = os.path.exists(dst) and os.path.getsize(dst) == os.path.getsize(f"{base}/{name}/{f}")
                    if same and not overwrite:
                        continue
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    shutil.copyfile(f"{base}/{name}/{f}", dst)
                    copied.append(f"{feed}/{name}/{f}")
    note("landed", day=day, copied=len(copied), overwrite=overwrite)
    return copied


def land_feed(feed: str, low: int, high: int) -> list[str]:
    """Every drop of one feed with a sequence number in [low, high]."""
    base = f"{args.source_outbox}/{feed}/drops"
    names = sorted(n for n in os.listdir(base) if n[:4].isdigit() and low <= int(n[:4]) <= high)
    for name in names:
        shutil.copytree(f"{base}/{name}", f"{landing}/{feed}/drops/{name}", dirs_exist_ok=True)
    note("landed feed", feed=feed, drops=len(names), low=low, high=high)
    return names


def label_digests() -> dict[str, dict[str, Any]]:
    return {t: digests.table_digest(spark.read.table(_boot.fq(args, layer, t)))
            for layer, t in (("cdc", "labels_payments_history"), ("silver", "payments_silver"))}  # fmt: skip


def digest_all() -> dict[str, dict[str, Any]]:
    out = {}
    for layer, tables in it.TABLES.items():
        for t in tables:
            out[f"{layer}.{t}"] = digests.table_digest(spark.read.table(_boot.fq(args, layer, t)))
    return out


def month_rerun() -> dict[str, Any]:
    import friction_features
    from friction_core.shipping import ship
    from friction_transforms import gold

    ship(friction_features)
    reset()
    for feeds in it.FEEDS.values():
        for feed in feeds:
            land_feed(feed, 1, 1)
    update()

    def read(name: str):  # type: ignore[no-untyped-def]
        return spark.read.table(_boot.fq(args, "gold", name))

    def write(name: str, df) -> None:  # type: ignore[no-untyped-def]
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(_boot.fq(args, "gold", name))

    counts = gold.features_step(spark, read, write, attacks=None)
    note("features", **counts)
    found = {k: digests.table_digest(spark.read.table(_boot.fq(args, layer, t)))
             for k, (layer, t) in it.EXPORTED.items()}  # fmt: skip
    return {"digests": found, "differences": []}


tests: dict[str, dict[str, Any]] = {}
wanted = {"relanding", "order", "labels"} if args.tests == "all" else set(args.tests.split(","))
try:
    d = args.day
    if "month" in wanted:
        tests["month_rerun"] = month_rerun()
    if "labels" in wanted:
        reset()
        land_feed("payments", 344, 347)
        labels = land_feed("labels_payments", 345, 359)
        update()
        at_once = label_digests()
        reset()
        land_feed("payments", 344, 347)
        third = max(1, len(labels) // 3)
        groups = [labels[2 * third :], labels[:third], labels[third : 2 * third]]
        for group in groups:
            for name in group:
                seq = int(name[:4])
                land_feed("labels_payments", seq, seq)
            update()
        late_first = label_digests()
        rows = at_once["labels_payments_history"]["rows"]
        tests["labels_out_of_order"] = {
            "label_drops": labels, "delivered": groups, "history_rows": rows,
            "differences": it.differences(at_once, late_first) + ([] if rows > 0 else ["empty label history"]),
        }  # fmt: skip
    if "relanding" not in wanted and "order" not in wanted:
        raise StopIteration
    reset()
    land(d)
    update()
    first = digest_all()
    again = land(d)
    update()
    second = digest_all()
    rewritten = land(d, overwrite=True)
    update()
    third = digest_all()
    changed = sorted(set(it.differences(first, second)) | set(it.differences(first, third)))
    tests["relanding_a_day"] = {"day": d, "files_copied_again": len(again), "files_overwritten": len(rewritten),
                                "differences": changed, "rows": {k: v["rows"] for k, v in first.items()}}  # fmt: skip

    reset()
    for day in (d + 1, d + 2, d + 3):
        land(day)
        update()
    in_order = digest_all()
    reset()
    for day in (d + 3, d + 1, d + 2):
        land(day)
        update()
    shuffled = digest_all()
    tests["order_and_late_labels"] = {"in_order": [d + 1, d + 2, d + 3], "shuffled": [d + 3, d + 1, d + 2],
                                      "differences": it.differences(in_order, shuffled),
                                      "rows": {k: v["rows"] for k, v in in_order.items()}}  # fmt: skip

    for day in (d + 1, d + 2, d + 3):
        land(day)
    update()
    tests["backfill_over_a_processed_range"] = {"days": [d + 1, d + 2, d + 3],
                                                "differences": it.differences(shuffled, digest_all())}  # fmt: skip
    status = "succeeded" if all(not t["differences"] for t in tests.values()) else "failed"
except StopIteration:
    status = "succeeded" if all(not t["differences"] for t in tests.values()) else "failed"
except Exception as exc:
    status = "failed"
    tests["error"] = {"differences": ["error"], "detail": f"{type(exc).__name__}: {str(exc)[:400]}"}

folder = f"{_boot.volume(args, 'exports')}/integration"
os.makedirs(folder, exist_ok=True)
with open(f"{folder}/integration.json", "w", encoding="utf-8") as fh:
    json.dump({"status": status, "tests": tests, "log": log}, fh, indent=1, default=str)
ledger.write(inputs={"day": args.day}, outputs={k: v.get("differences") for k, v in tests.items()}, rows_in=0,
             rows_out=len(tests), status=status)  # fmt: skip
if status != "succeeded":
    raise SystemExit(f"integration tests failed: {json.dumps({k: v.get('differences') for k, v in tests.items()})}")
