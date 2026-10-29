# Decisions

Dated, with the reason. A decision that reverses an earlier one says so. One that chooses a
workspace path cites the documentation page and its date.

## 2026-10-29 Schemas with environment prefixes, not a catalog per environment

The probe (results/workspace_probe.json, request 2026-10-29-probe) asked the API to create a
catalog and was refused: the metastore has no storage root, and Default Storage catalogs can only
be made in the UI. Terraform goes through the same API. So the environments are schemas with
prefixes under the `workspace` catalog: `friction_dev_bronze`, `friction_prod_bronze` and so on.
One workspace with prefixed schemas stands in for two workspaces; it separates names and grants,
not compute, quotas or metastores, and every page that shows the platform says so. Source:
Databricks, Free Edition limitations, updated 18 Oct 2026, and the probe.

## 2026-10-29 The RBA file has 31,269,264 rows

Its README says more than 33 million login attempts. The released file (v1.0.0, digest in
data/sources.lock.json) has 31,269,264 data rows, and its index column runs from 0 to 31,269,263
with no gaps. The build uses the file as released and prints the measured count everywhere.

## 2026-10-29 Keyed draws instead of a seeded stream

Every random number in the simulator and the replay splitter is SplitMix64 of a seed, a stream
name and the row's natural key (packages/simulator/src/friction_simulator/draws.py). A row's draw
does not depend on partitioning or arrival order, which is a stronger guarantee than sorting before
a seeded sample (the Day 9 and Day 13 rule). The determinism tests still run under two
PYTHONHASHSEED values.

## 2026-10-29 Open source SDP 4.2.1 has AUTO CDC, but only SCD type 1

The prompt says AUTO CDC is Databricks only. In pyspark 4.2.1, `pyspark.pipelines` exposes
`create_auto_cdc_flow`, and its signature accepts `stored_as_scd_type` of 1 only. History (SCD
type 2) and the bitemporal tracking are Lakeflow features. So the portable pipeline builds card and
label history with the plain reference implementation in packages/transforms/scd.py, and the
reconcile compares that history with the Lakeflow AUTO CDC history key by key.

## 2026-10-29 The statement names the licence of the RBA data only

The specification wrote "(the IBM TabFormer card transactions and the RBA login data set, CC BY
4.0)", which reads as if both carried CC BY 4.0. TabFormer's repository is Apache 2.0 and the data
states no separate terms. The statement on every surface now reads "the IBM TabFormer card
transactions, and the RBA login data set under CC BY 4.0".
