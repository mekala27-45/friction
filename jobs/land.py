"""Land drops: copy them from the outbox volume into the landing zone, through a drop number.

Idempotent by construction: a file already in the landing zone with the same size is skipped, so
landing the same range twice changes nothing and Auto Loader sees no new file. Feeds follow their
desk's calendar (labels and the card CDC feed with payments; takeover labels with logins).

    land.py --desk payments --through 387 [--redelivery]
"""

from __future__ import annotations

import os
import shutil

import _boot

FEEDS = {"payments": ["payments", "labels_payments", "cdc_cards"], "logins": ["logins", "labels_logins"]}

args = _boot.parse(
    [
        ("--desk", {"choices": ["payments", "logins"], "required": True}),
        ("--through", {"type": int, "required": True}),
        ("--from", {"type": int, "default": 1, "dest": "start"}),
        ("--redelivery", {"action": "store_true"}),
    ]
)
spark = _boot.session()
ledger = _boot.Ledger(spark, args, f"land_{args.desk}")
outbox, landing = _boot.volume(args, "outbox"), _boot.volume(args, "landing")
copied, skipped, files = 0, 0, []
try:
    for feed in FEEDS[args.desk]:
        # Every feed's folder exists from the first landing, so Auto Loader starts on an empty
        # folder rather than a missing path; the first payments label drop is in 1995.
        os.makedirs(f"{landing}/{feed}/drops", exist_ok=True)
        base = f"{outbox}/{feed}/drops"
        if not os.path.isdir(base):
            continue
        for drop in sorted(os.listdir(base)):
            seq = int(drop[:4])
            if seq < args.start or seq > args.through:
                continue
            for name in sorted(os.listdir(f"{base}/{drop}")):
                src = f"{base}/{drop}/{name}"
                dst = f"{landing}/{feed}/drops/{drop}/{name}"
                files.append(f"{feed}/{drop}/{name}")
                if os.path.exists(dst) and os.path.getsize(dst) == os.path.getsize(src):
                    skipped += 1
                    continue
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copyfile(src, dst)
                copied += 1
    if args.redelivery:
        base = f"{outbox}/{args.desk}/redelivery"
        for drop in sorted(os.listdir(base)) if os.path.isdir(base) else []:
            for name in sorted(os.listdir(f"{base}/{drop}")):
                dst = f"{landing}/{args.desk}/drops/{drop}_redelivery/{name}"
                files.append(f"redelivery/{drop}/{name}")
                if os.path.exists(dst):
                    skipped += 1
                    continue
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copyfile(f"{base}/{drop}/{name}", dst)
                copied += 1
    ledger.write(inputs=files, outputs=files, rows_in=len(files), rows_out=copied, status="succeeded",
                 detail={"copied": copied, "skipped": skipped, "through": args.through})  # fmt: skip
    print(f"landed {copied} files, {skipped} already there, through drop {args.through}")
except Exception as exc:
    ledger.write(inputs=files, outputs=[], rows_in=len(files), rows_out=copied, status="failed", detail=str(exc))
    raise
