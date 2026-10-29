-- statement
-- The rows that arrived in drop 387, its redelivery included, after the replay with the approved change.
-- save: incident_drop
SELECT count(*) AS rows, sum(amount_cents) AS amount_cents
FROM ${silver}.payments_silver WHERE drop_id LIKE '0387%'
-- statement
-- save: incident_quarantine
SELECT rule, count(*) AS rows FROM ${silver}.quarantine
WHERE desk = 'payments' AND drop_id LIKE '0387%' GROUP BY rule ORDER BY rule
