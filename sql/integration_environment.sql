-- statement
-- The integration job's own schemas and volumes under the friction_it prefix. Terraform keeps one
-- state for dev; this environment is disposable, so it is created by a reviewed SQL request.
CREATE SCHEMA IF NOT EXISTS ${catalog}.friction_it_raw
-- statement
CREATE SCHEMA IF NOT EXISTS ${catalog}.friction_it_bronze
-- statement
CREATE SCHEMA IF NOT EXISTS ${catalog}.friction_it_silver
-- statement
CREATE SCHEMA IF NOT EXISTS ${catalog}.friction_it_cdc
-- statement
CREATE SCHEMA IF NOT EXISTS ${catalog}.friction_it_gold
-- statement
CREATE SCHEMA IF NOT EXISTS ${catalog}.friction_it_ml
-- statement
CREATE SCHEMA IF NOT EXISTS ${catalog}.friction_it_ops
-- statement
CREATE VOLUME IF NOT EXISTS ${catalog}.friction_it_raw.landing
-- statement
CREATE VOLUME IF NOT EXISTS ${catalog}.friction_it_raw.exports
-- statement
CREATE VOLUME IF NOT EXISTS ${catalog}.friction_it_raw.outbox
-- statement
-- save: it_schemas
SELECT schema_name FROM ${catalog}.information_schema.schemata WHERE schema_name LIKE 'friction_it_%' ORDER BY 1
