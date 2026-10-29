---
name: backfill-a-range
description: Write and verify a workspace backfill request for a range of drops of one desk. Use when a range of days must be reprocessed, for example after a contract fix or the day 40 incident.
---

# Backfill a range

1. Find the drops: `data/replay.lock.json` lists every drop id per desk (`NNNN_YYYY-MM-DD`).
2. Write `requests/<date>-<nn>-backfill-<desk>-<first>-<last>.json` with `kind: bundle_run`, the job
   `backfill`, and parameters `desk`, `from_drop`, `to_drop`. One request per desk; ranges never overlap
   a running request.
3. Commit and push. The workspace workflow runs pending requests in file name order and writes
   `runs/<request id>/` on the results branch.
4. Verify from the run, not the job status:
   - the run ledger (`ops.run_ledger`) has one row per task per drop, with the range stated;
   - the land task copied each drop once (its checkpoint advanced once per drop);
   - the export's table digests for the range equal the digests from before the backfill wherever the
     contract did not change, and differ only in the rows the fix was meant to recover.
5. Promote the result with `scripts/promote_results.py` and record the run id in DECISIONS.md if the
   backfill followed an incident.

Never backfill by appending. Every task takes a range and overwrites it.
