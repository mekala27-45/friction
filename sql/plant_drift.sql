-- statement
-- The planted drift: a grant terraform manages, revoked by hand in the workspace. The next
-- terraform plan must show it as a change and the apply must restore it.
REVOKE SELECT ON SCHEMA ${catalog}.friction_dev_gold FROM `account users`
-- statement
-- save: grants_after_revoke
SHOW GRANTS ON SCHEMA ${catalog}.friction_dev_gold
