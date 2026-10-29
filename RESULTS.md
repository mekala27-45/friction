# Results

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

Rendered from results/manifest.json by the claim gate; a number here that the manifest does not hold
fails CI. Source of each block in its first line.

## The definition of done

3 of eighteen lines done.

| Step | Status | Line | Evidence |
|---:|---|---|---|
| 1 | not done | Both sources fetched with digests in Actions, profiled at full size, quarantine rules, contracts, provenance | fetched and profiled at full size on the build machine (results/profile); the Actions fetch waits on the workspace secrets |
| 2 | done | The replay splitter, the lateness rule, the simulator and the incident config, deterministic with digests | data/replay.lock.json; tests/simulator |
| 3 | not done | The portable pipeline on open source Spark 4.2 with local tests and the full local run | the full local run has not finished |
| 4 | not done | Terraform with state in Neon, the bundle, the workspace workflow, the results branch, reviewed promotion | written and validated in CI; apply waits on the Actions secrets FRICTION_SALT, TF_STATE_CONN and DATABASE_URL |
| 5 | not done | The Lakeflow pipeline deployed to dev and run; the reconcile against the local run | requests 03 to 07 are committed and wait on the same secrets |
| 6 | not done | Jobs with retries and the run ledger; idempotency, order and late label tests in the workspace | the jobs are defined in databricks.yml; their workspace tests have not run |
| 7 | not done | Every row of both sources backfilled in the workspace | nothing is backfilled in the workspace yet; the full local run holds every row |
| 8 | not done | Governance: tags, masks, the row filter, grants, lineage, the scan; the planted drift caught by the plan | the probe found masks and functions allowed (results/workspace_probe.json); nothing applied |
| 9 | done | Point in time features with the leakage test; training sets as of a cut with the two clocks test | tests/features/test_point_in_time.py; tests/transforms/test_training.py |
| 10 | not done | Floors and models on both desks registered in Unity Catalog; the plan hashed before any test month was scored | floors and models fit on the local run, plan hashed before scoring (DECISIONS.md); Unity Catalog registration waits on the workspace |
| 11 | not done | The frontier, operating points, intervals, BH, interior tests, calibration, hypotheses, the RBA comparison | no scored sets graded yet |
| 12 | not done | Reason codes, model cards, validation reports, promotion gates and the champion aliases | codes, gates and cards are in the repository; the champion alias waits on Unity Catalog |
| 13 | not done | The API live on Vercel with the online store and the decision log; parity exact against the live URL | the API runs and passes parity locally; Vercel and Neon are not connected yet |
| 14 | done | Persistence: independent connection, audit before response, shared transaction, out of process check | tests/api/test_api.py and tests/api/test_out_of_process.py against Postgres |
| 15 | not done | Monitoring with delayed label cohorts; the day 40 incident detected, handled by the runbook and written up | config/monitor.yaml and the incident plant exist; the monitor has not run |
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

The replay delivers 24,386,900 payment rows in 439 drops
and 31,269,264 login rows in 102 drops; the simulated label
feed carries 29,512 chargeback rows.



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

- The graded numbers come from the local open source Spark run until the workspace runs; the two engines
  run the same transforms, but the tokens differ because the build machine never holds the production
  salt, so the attacker victims and the user bucket grain differ between them.
- The payment labels are simulated chargebacks on TabFormer's fraud flag, with a lag drawn from a
  lognormal. TabFormer marks no fraud in two stretches of its calendar (DECISIONS.md names them); the
  modeling window ends before the second.
- No real takeover falls in the login test months, so the login desk is graded on synthetic attackers
  built from the paper's definitions, which are not attackers.
- The attack IP flag is close to a label; the LightGBM model with it is printed beside the one without.
- The cost view multiplies probabilities by dollar assumptions that no bank supplied.
