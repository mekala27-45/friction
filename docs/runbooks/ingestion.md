# Runbook: an ingestion alert

Use when the monitor raises an expectation rate, completeness or clock alert for a drop, or when a
GitHub issue titled "monitor:" opens. The day 40 postmortem (docs/incidents) followed these steps.

## 1. Identify

- Read the alert rows in the issue: the check, the scope (desk, drop, file or rule) and the value.
- In the workspace, from a SQL request (requests/*.json, kind sql), read:
  - the pipeline event log for the update that processed the drop: expectation counts per rule;
  - `quarantine` grouped by `rule` and `source_file` for the drop;
  - `ops.run_ledger` for the land and pipeline tasks of the drop: start, end, rows in, rows out.
- On the build machine, the same counts come from `scripts/monitor.py` over the local lake.
- Decide which of three it is: a resent file (rows deduplicated, nothing lost), a contract change
  upstream (rows in rescued data, quarantined), or a clock fault (events after their drop closed).

## 2. Contain

- Pause the daily job (`bundle_run` request with the job paused) so later drops do not stack on top.
- Do not delete anything from bronze. Bronze is the record of what arrived.

## 3. Fix

- A contract change: add the mapping to `contracts/approved_changes.yaml` under `active`, with a
  `history` entry naming the DECISIONS.md entry that approves it. The pipelines read renames back out
  of rescued data; nothing is edited by hand.
- A clock fault: if the source confirms the shift, add the file to `rejected_files` and ask for a
  corrected redelivery; never shift timestamps in the pipeline.
- A resend: nothing to fix if completeness held; record it.

## 4. Replay

- Commit a backfill request for the drop range (.claude/skills/backfill-a-range). The backfill job
  overwrites the range; it never appends.

## 5. Verify

- The export's table digests for the range: unchanged where the contract did not change, changed
  exactly where rows were recovered.
- The gold totals for the day: equal to the sum of the corrected source, with no double count from
  the resend.
- The monitor reruns clean for the drop. Close the issue with the run ids.
