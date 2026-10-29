-- statement
-- The card age feature: -1 on nearly every payment in the workspace, about 1,615 days on average on
-- the open source run of the same drops. Aggregates, types and synthetic reference numbers only.
-- save: history_types
SELECT typeof(account_ref) AS account_ref_type, typeof(card_generation) AS generation_type,
       typeof(__START_AT) AS start_type, typeof(__END_AT) AS end_type, count(*) AS rows
FROM ${cdc}.cards_history GROUP BY 1, 2, 3, 4
-- statement
-- save: history_sample
SELECT account_ref, card_generation, status, __START_AT, __END_AT
FROM ${cdc}.cards_history ORDER BY account_ref, card_generation LIMIT 8
-- statement
-- save: issue_times
SELECT count(*) AS generations, count(issued_at) AS with_time, min(issued_at) AS first, max(issued_at) AS last
FROM (SELECT account_ref, card_generation, min(__START_AT.changed_at) AS issued_at
      FROM ${cdc}.cards_history GROUP BY account_ref, card_generation)
-- statement
-- save: silver_types
SELECT typeof(account_ref) AS account_ref_type, count(*) AS rows, count(DISTINCT account_ref) AS accounts,
       min(event_ts) AS first, max(event_ts) AS last
FROM ${silver}.payments_silver GROUP BY 1
-- statement
-- save: ages
SELECT count(*) AS rows, sum(CASE WHEN days_since_issue = -1 THEN 1 ELSE 0 END) AS unknown,
       min(days_since_issue) AS low, max(days_since_issue) AS high, avg(days_since_issue) AS mean
FROM ${gold}.payments_issue_age
-- statement
-- save: account_match
SELECT count(DISTINCT s.account_ref) AS silver_accounts,
       count(DISTINCT CASE WHEN h.account_ref IS NOT NULL THEN s.account_ref END) AS with_history
FROM (SELECT DISTINCT account_ref FROM ${silver}.payments_silver) s
LEFT JOIN (SELECT DISTINCT account_ref FROM ${cdc}.cards_history) h ON s.account_ref = h.account_ref
-- statement
-- save: run_ledger
SELECT job, task, status, count(*) AS runs, sum(rows_out) AS rows_out, round(sum(compute_seconds), 1) AS seconds,
       min(recorded_at) AS first, max(recorded_at) AS last
FROM ${ops}.run_ledger GROUP BY job, task, status ORDER BY first
