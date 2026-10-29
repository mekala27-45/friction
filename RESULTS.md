# Results

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

Rendered from results/manifest.json by the claim gate; a number here that the manifest does not hold
fails CI. Source of each block in its first line.

## The definition of done

7 of eighteen lines done.

| Step | Status | Line | Evidence |
|---:|---|---|---|
| 1 | done | Both sources fetched with digests in Actions, profiled at full size, quarantine rules, contracts, provenance | results/workspace/fetch_status_on_runner.json (fetched and verified in Actions), results/profile, contracts/, data/PROVENANCE.md |
| 2 | done | The replay splitter, the lateness rule, the simulator and the incident config, deterministic with digests | data/replay.lock.json, equal to the lock regenerated in Actions; tests/simulator |
| 3 | done | The portable pipeline on open source Spark 4.2 with local tests and the full local run | results/workspace/full_open_source.json: every drop on open source Spark, on the Actions runner (the build machine has 2 cores and 19 GB of disk); tests/transforms |
| 4 | done | Terraform with state in Neon, the bundle, the workspace workflow, the results branch, reviewed promotion | requests/ and the results branch; terraform state in Neon (request 03c); results/provenance.json |
| 5 | not done | The Lakeflow pipeline deployed to dev and run; the reconcile against the local run | the first month is equal on all nine tables (results/reconcile_first_month.json); over every row 8 of 9 are equal and the card age feature differs (results/reconcile_full.json) |
| 6 | not done | Jobs with retries and the run ledger; idempotency, order and late label tests in the workspace | the jobs are defined in databricks.yml; their workspace tests have not run |
| 7 | done | Every row of both sources backfilled in the workspace | results/workspace/full_export.json: 24,383,185 payments and 31,267,738 logins in silver |
| 8 | not done | Governance: tags, masks, the row filter, grants, lineage, the scan; the planted drift caught by the plan | the probe found masks and functions allowed (results/workspace_probe.json); nothing applied |
| 9 | done | Point in time features with the leakage test; training sets as of a cut with the two clocks test | tests/features/test_point_in_time.py; tests/transforms/test_training.py |
| 10 | not done | Floors and models on both desks registered in Unity Catalog; the plan hashed before any test month was scored | floors and models fit on the local run, plan hashed before scoring (DECISIONS.md); Unity Catalog registration waits on the workspace |
| 11 | not done | The frontier, operating points, intervals, BH, interior tests, calibration, hypotheses, the RBA comparison | no scored sets graded yet |
| 12 | not done | Reason codes, model cards, validation reports, promotion gates and the champion aliases | codes, gates and cards are in the repository; the champion alias waits on Unity Catalog |
| 13 | not done | The API live on Vercel with the online store and the decision log; parity exact against the live URL | the API runs and passes parity locally; Vercel and Neon are not connected yet |
| 14 | done | Persistence: independent connection, audit before response, shared transaction, out of process check | tests/api/test_api.py and tests/api/test_out_of_process.py against Postgres |
| 15 | not done | Monitoring with delayed label cohorts; the day 40 incident detected, handled by the runbook and written up | config/monitor.yaml and the incident plant exist; the incident has not been replayed |
| 16 | not done | Performance experiments and the compute ledger published | no performance experiment has run |
| 17 | not done | The site on Pages; the live check; latency with cold start; RESULTS, README, DECISIONS, notebooks, AGENTS | the site is not built yet |
| 18 | not done | Coverage, mypy, ruff, gates, skills valid, forty to sixty commits, the rederive, v0.1.0 after the commits | the tag waits on every line above |

Stretch items (the sequence challenger, a workspace endpoint, the dashboard and Genie space, the
readback warehouse, BI exports, hardening): not started, and not counted.

## The sources

TabFormer: 24,386,900 transactions from 2,000 users and
6,139 cards. RBA: 31,269,264 login attempts from
4,304,857 user ids, 12,541,442 of them successful; round trip time
missing on 95.9% of rows; 141 attempts marked as
account takeovers.
One account id holds 14,025,899 attempts with
4 successes: the bucket the service files sign ins against
unknown accounts under. It is flagged in silver by a rule on counts, never by its id (DECISIONS.md).

The replay delivers 24,386,900 payment rows in 439 drops
and 31,269,264 login rows in 102 drops; the simulated label
feed carries 29,512 chargeback rows.

## The full open source run

Every table of the open source run over every drop, on the Actions runner with the workspace salt,
with its rows and its digest (the reconcile compares these with the workspace's export).

| Table | Rows | Digest |
|---|---:|---|
| cards_history | 31,504 | 0da1ee0aeeca |
| cdc_cards_bronze | 33,254 | 39b4ffdcffb2 |
| labels_logins_bronze | 141 | c59efa59dcc7 |
| labels_logins_history | 141 | 3885c57fd4df |
| labels_payments_bronze | 29,512 | 45011ba048ca |
| labels_payments_history | 28,448 | 9d3641d66659 |
| login_account_stats | 4,304,857 | 08c75cf6f396 |
| logins_bronze | 31,269,264 | 63ef89a5b16a |
| logins_features | 31,267,738 | 9e3a9413c50a |
| logins_silver | 31,267,738 | 9c9ad48eb61d |
| mcc_fraud_rates | 32,836 | acad09ef94d2 |
| payments_bronze | 24,392,482 | 8cef845aad25 |
| payments_features | 24,383,185 | 8c543febdd29 |
| payments_silver | 24,383,185 | 009abd3ed589 |
| quarantine | 8,161 | 032ed41f8571 |

Quarantine by rule:

| Desk | Rule | Rows |
|---|---|---:|
| logins | L_UA | 1,526 |
| payments | P_BUSINESS_KEY | 308 |
| payments | P_FUTURE | 487 |
| payments | P_MCC | 2,920 |
| payments | P_RESCUED | 2,920 |


## What Free Edition allowed

| Check | Result |
|---|---|
| apps_list | allowed |
| catalog_create | refused |
| catalogs_visible | allowed |
| create_mask_function | allowed |
| create_table | allowed |
| delta_sharing_shares | refused |
| experiments_list | allowed |
| groups_list | allowed |
| identity | allowed |
| jobs_list | allowed |
| liquid_clustering | allowed |
| pipelines_list | allowed |
| read:system.access.audit | allowed |
| read:system.access.column_lineage | allowed |
| read:system.access.table_lineage | allowed |
| read:system.billing.list_prices | allowed |
| read:system.billing.usage | allowed |
| read:system.information_schema.tables | allowed |
| read:system.lakeflow.jobs | allowed |
| read:system.lakeflow.pipelines | allowed |
| registered_models_list | allowed |
| row_filter_function | allowed |
| schema_create | allowed |
| service_principals_list | allowed |
| serving_endpoints_list | allowed |
| set_column_mask | allowed |
| set_row_filter | allowed |
| set_tag | allowed |
| sql_version | allowed |
| volume_create | allowed |
| warehouses | allowed |

## Limitations

- The build machine never holds the production salt. Runs that must match the workspace token for token
  (the reconcile, the graded models and the served bundles) run on the Actions runner, which holds it as
  a secret; the build machine's own runs use the public test salt and are labeled as such.
- The payment labels are simulated chargebacks on TabFormer's fraud flag, with a lag drawn from a
  lognormal. TabFormer marks no fraud in two stretches of its calendar (DECISIONS.md names them); the
  modeling window ends before the second.
- No real takeover falls in the login test months, so the login desk is graded on synthetic attackers
  built from the paper's definitions, which are not attackers.
- The attack IP flag is close to a label; the LightGBM model with it is printed beside the one without.
- The cost view multiplies probabilities by dollar assumptions that no bank supplied.
