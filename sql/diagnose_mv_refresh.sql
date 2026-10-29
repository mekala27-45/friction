-- statement
-- How each pipeline update handled the card age view: its flow status and the refresh technique the
-- planner chose. Names and states only.
-- save: issue_age_events
SELECT timestamp, origin.update_id AS update_id, event_type,
       details:flow_progress.status AS status,
       details:planning_information.technique_information[0].maintenance_type AS technique,
       details:planning_information.technique_information[0].incrementalization_issues AS issues
FROM event_log(TABLE(${gold}.payments_issue_age))
WHERE event_type IN ('flow_progress', 'planning_information') AND origin.flow_name LIKE '%payments_issue_age%'
ORDER BY timestamp
-- statement
-- save: issue_age_now
SELECT count(*) AS rows, sum(CASE WHEN days_since_issue = -1 THEN 1 ELSE 0 END) AS unknown
FROM ${gold}.payments_issue_age
