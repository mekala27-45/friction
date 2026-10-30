-- statement
-- What the pipeline recorded for the update that replayed drop 387: each gold and silver flow's start
-- and end, and the inputs and refresh technique of the card age view. Names, states and times only.
-- save: flow_times
SELECT origin.flow_name AS flow, details:flow_progress.status AS status, min(timestamp) AS at
FROM event_log(TABLE(${gold}.payments_issue_age))
WHERE origin.update_id = '${update}' AND event_type = 'flow_progress'
  AND details:flow_progress.status IN ('RUNNING', 'COMPLETED')
GROUP BY 1, 2 ORDER BY at
-- statement
-- save: issue_age_definition
SELECT event_type, details:flow_definition.input_datasets AS inputs, details:planning_information AS planning
FROM event_log(TABLE(${gold}.payments_issue_age))
WHERE origin.update_id = '${update}' AND origin.flow_name LIKE '%payments_issue_age%'
  AND event_type IN ('flow_definition', 'planning_information')
