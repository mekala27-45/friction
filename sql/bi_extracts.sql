-- The extracts a BI team opens: the decisions at day, channel and verdict grain with the fraud and
-- dollars a measure needs, the reason codes on flagged decisions, the code table and the refused
-- rows. scripts/build_bi.py turns them into CSV, a Power BI project and a Tableau workbook, and
-- checks the capture they give on the test months against results/evaluation.json.
-- save: bi_decisions_daily
SELECT d.event_date AS day, d.channel, d.verdict, COUNT(*) AS decisions,
       SUM(GREATEST(d.amount_cents, 0)) AS amount_cents,
       SUM(CASE WHEN l.is_fraud THEN 1 ELSE 0 END) AS frauds,
       SUM(CASE WHEN l.is_fraud THEN GREATEST(d.amount_cents, 0) ELSE 0 END) AS fraud_cents
FROM workspace.friction_dev_gold.payments_decisions d
LEFT JOIN (SELECT event_ref, is_fraud FROM workspace.friction_dev_cdc.labels_payments_history WHERE __END_AT IS NULL) l
  ON l.event_ref = d.txn_ref
GROUP BY ALL ORDER BY day, channel, verdict
-- statement
-- save: bi_codes_daily
SELECT d.event_date AS day, d.verdict, c.code, COUNT(*) AS decisions
FROM workspace.friction_dev_gold.payments_decisions d LATERAL VIEW EXPLODE(SPLIT(d.codes, ',')) c AS code
WHERE d.verdict <> 'approve' AND d.codes <> ''
GROUP BY ALL ORDER BY day, verdict, code
-- statement
-- save: bi_reason_codes
SELECT desk, code, says, features, source FROM workspace.friction_dev_gold.reason_codes ORDER BY desk, code
-- statement
-- save: bi_quarantine
SELECT desk, rule, COUNT(*) AS rows_refused FROM workspace.friction_dev_silver.quarantine GROUP BY ALL ORDER BY desk, rule
