# Results

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

Rendered from results/manifest.json by the claim gate; a number here that the manifest does not hold
fails CI. Source of each block in its first line.

## The definition of done

14 of eighteen lines done.

| Step | Status | Line | Evidence |
|---:|---|---|---|
| 1 | done | Both sources fetched with digests in Actions, profiled at full size, quarantine rules, contracts, provenance | results/workspace/fetch_status_on_runner.json (fetched and verified in Actions), results/profile, contracts/, data/PROVENANCE.md |
| 2 | done | The replay splitter, the lateness rule, the simulator and the incident config, deterministic with digests | data/replay.lock.json, equal to the lock regenerated in Actions; tests/simulator |
| 3 | done | The portable pipeline on open source Spark 4.2 with local tests and the full local run | results/workspace/full_open_source.json: every drop on open source Spark, on the Actions runner (the build machine has 2 cores and 19 GB of disk); tests/transforms |
| 4 | done | Terraform with state in Neon, the bundle, the workspace workflow, the results branch, reviewed promotion | requests/ and the results branch; terraform state in Neon (request 03c); results/provenance.json |
| 5 | done | The Lakeflow pipeline deployed to dev and run; the reconcile against the local run | results/reconcile_first_month.json and results/reconcile_full.json: every exported table equal by digest across the engines |
| 6 | done | Jobs with retries and the run ledger; idempotency, order and late label tests in the workspace | results/workspace/integration.json and integration_labels.json (every table digest identical after re-landing, shuffled delivery, a repeat backfill and out of order labels); retries in databricks.yml; results/workspace/run_ledger.json |
| 7 | done | Every row of both sources backfilled in the workspace | results/workspace/full_export.json: 24,386,592 payments and 31,267,738 logins in silver |
| 8 | done | Governance: tags, masks, the row filter, grants, lineage, the scan; the planted drift caught by the plan | results/workspace/governance.json (tags, masks, the row filter, grants, lineage, the scan); results/workspace/drift_terraform_plan.txt (the revoked grant caught as one change) |
| 9 | done | Point in time features with the leakage test; training sets as of a cut with the two clocks test | tests/features/test_point_in_time.py; tests/transforms/test_training.py |
| 10 | done | Floors and models on both desks registered in Unity Catalog; the plan hashed before any test month was scored | results/workspace/train_summary.json: 6 models of both desks, the floors included, registered in Unity Catalog; the registered payments LightGBM is byte identical to the served file; plan hashed before scoring (evaluation/plan.lock) |
| 11 | done | The frontier, operating points, intervals, BH, interior tests, calibration, hypotheses, the RBA comparison | results/evaluation.json from the local run's scored sets |
| 12 | not done | Reason codes, model cards, validation reports, promotion gates and the champion aliases | codes, gates, cards and validation reports are in the repository; the champion alias is set by a promotion request after the live parity and latency gates |
| 13 | not done | The API live on Vercel with the online store and the decision log; parity exact against the live URL | the API passes parity against a local container and Postgres; the Vercel deployment has not been checked yet |
| 14 | done | Persistence: independent connection, audit before response, shared transaction, out of process check | tests/api/test_api.py and tests/api/test_out_of_process.py against Postgres |
| 15 | done | Monitoring with delayed label cohorts; the day 40 incident detected, handled by the runbook and written up | results/workspace/monitor_alerts.json (GitHub issue 1), results/incident.json, results/workspace/incident_drop.json, docs/incidents/2026-10-29-day-40.md |
| 16 | done | Performance experiments and the compute ledger published | results/performance.json (Actions runner, full data), results/compute_ledger.json (wall seconds per request; with workspace usage from system.billing.usage) |
| 17 | not done | The site on Pages; the live check; latency with cold start; RESULTS, README, DECISIONS, notebooks, AGENTS | the site deploys to Pages from CI and every route passes its test; missing: the live check awake, the live check asleep, the latency with the cold start |
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
| cards_history | 31,504 | e98c431c119b |
| cdc_cards_bronze | 33,254 | 39b4ffdcffb2 |
| labels_logins_bronze | 141 | c59efa59dcc7 |
| labels_logins_history | 141 | 14a63a5366b9 |
| labels_payments_bronze | 29,512 | 45011ba048ca |
| labels_payments_history | 28,448 | 99fd8ed2c18e |
| login_account_stats | 4,304,857 | 08c75cf6f396 |
| logins_bronze | 31,269,264 | 63ef89a5b16a |
| logins_features | 31,267,738 | 9e3a9413c50a |
| logins_silver | 31,267,738 | 9c9ad48eb61d |
| mcc_fraud_rates | 32,836 | db4795f062ae |
| payments_bronze | 24,398,331 | dcb495b2d210 |
| payments_features | 24,386,592 | 1c8b7c9a8a25 |
| payments_silver | 24,386,592 | 654eac164853 |
| quarantine | 8,179 | 66b24b8bd9ad |

Quarantine by rule:

| Desk | Rule | Rows |
|---|---|---:|
| logins | L_UA | 1,526 |
| payments | P_BUSINESS_KEY | 308 |
| payments | P_FUTURE | 487 |
| payments | P_KEY_CONFLICT | 2,929 |
| payments | P_REJECTED_FILE | 2,929 |

## The payments desk

Source: open source run. Test months 2019-07-01 to
2019-11-01: 580,552 transactions,
786 frauds worth $69,206, resampled over
3,897 card accounts.

| Model | Capture | Low | High | Step ups | Declines | Precision | ECE | ROC AUC | AP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| the floor | 1.8% | 0.0% | 4.4% | 9.75 per 1,000 | 0.98 per 1,000 | 0.0% | not applicable | 0.597 | 0.003 |
| LightGBM | 90.2% | 85.0% | 94.6% | 8.65 per 1,000 | 1.18 per 1,000 | 11.0% | 0.000 | 1.000 | 0.801 |
| logistic regression | 81.2% | 76.5% | 85.0% | 9.45 per 1,000 | 1.21 per 1,000 | 8.4% | 0.000 | 0.999 | 0.557 |

Model against floor and model against model, dollar capture at the operating point:

| Model | Against | Difference | Low | High | p | Adjusted p | Rejected |
|---|---|---:|---:|---:|---:|---:|---|
| logistic regression | the floor | +79.4 pts | +73.7 pts | +84.1 pts | 0.002 | 0.002 | yes |
| LightGBM | the floor | +88.5 pts | +81.6 pts | +93.6 pts | 0.002 | 0.002 | yes |
| LightGBM | logistic regression | +9.0 pts | +3.5 pts | +14.5 pts | 0.002 | 0.002 | yes |

Promotion gates:

| Model | Gate | Result |
|---|---|---|
| LightGBM | expected calibration error under the bound | passed |
| LightGBM | dollar capture above the floor, interval clear of zero | passed |
| LightGBM | friction within the budget on test | passed |
| LightGBM | reason code coverage of every step up and decline | passed |
| LightGBM | score stability, PSI under the bound | passed |
| logistic regression | expected calibration error under the bound | passed |
| logistic regression | dollar capture above the floor, interval clear of zero | passed |
| logistic regression | friction within the budget on test | passed |
| logistic regression | reason code coverage of every step up and decline | passed |
| logistic regression | score stability, PSI under the bound | passed |

The interior test on the budget: stable.

| Budget | LightGBM | logistic regression |
|---|---:|---:|
| double | 98.1% | 91.3% |
| half | 80.0% | 62.8% |
| registered | 90.2% | 81.2% |

Expected cost per 1,000 events at the assumptions in config/costs.yaml (each one an assumption): the
cheapest is LightGBM; assumptions that change that when halved or doubled:
none.

| Model | Cost per 1,000 events |
|---|---:|
| the floor | $175.10 |
| LightGBM | $39.35 |
| logistic regression | $58.20 |

## The login desk

Test months 2020-12-15 to 2021-03-01:
2,304,675 successful sign ins, 60,000 of them attacks,
resampled over 1,046,519 users. Real takeovers in the test months:
0, so the graded positives are the synthetic attackers.

| Model | Capture | Low | High | Step ups | Declines | Precision | ECE | ROC AUC | AP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| the floor | 58.5% | 58.3% | 58.7% | 57.67 per 1,000 | 0.00 per 1,000 | 21.3% | not applicable | 0.863 | 0.320 |
| Freeman RBA | 0.0% | 0.0% | 0.0% | 0.00 per 1,000 | 0.00 per 1,000 | not applicable | not applicable | 0.384 | 0.020 |
| LightGBM | 25.9% | 25.6% | 26.1% | 41.30 per 1,000 | 0.82 per 1,000 | 14.1% | 0.026 | 0.636 | 0.096 |
| LightGBM with the attack IP flag | 27.5% | 27.3% | 27.8% | 60.37 per 1,000 | 0.74 per 1,000 | 10.8% | 0.026 | 0.613 | 0.095 |

| Model | Against | Difference | Low | High | p | Adjusted p | Rejected |
|---|---|---:|---:|---:|---:|---:|---|
| Freeman RBA | the floor | -58.5 pts | -58.7 pts | -58.3 pts | 0.002 | 0.002 | yes |
| LightGBM | the floor | -32.6 pts | -33.0 pts | -32.3 pts | 0.002 | 0.002 | yes |
| LightGBM | Freeman RBA | +25.9 pts | +25.6 pts | +26.1 pts | 0.002 | 0.002 | yes |

Capture by attacker model at each model's operating point:

| Model | Naive | VPN | Targeted | Takeovers |
|---|---:|---:|---:|---:|
| the floor | 100.0% | 75.5% | 0.0% | not applicable |
| Freeman RBA | 0.0% | 0.0% | 0.0% | not applicable |
| LightGBM | 72.9% | 1.9% | 2.8% | not applicable |
| LightGBM with the attack IP flag | 79.3% | 0.7% | 2.6% | not applicable |

Promotion gates:

| Model | Gate | Result |
|---|---|---|
| Freeman RBA | expected calibration error under the bound | failed |
| Freeman RBA | dollar capture above the floor, interval clear of zero | failed |
| Freeman RBA | friction within the budget on test | passed |
| Freeman RBA | reason code coverage of every step up and decline | passed |
| Freeman RBA | score stability, PSI under the bound | passed |
| LightGBM | expected calibration error under the bound | passed |
| LightGBM | dollar capture above the floor, interval clear of zero | failed |
| LightGBM | friction within the budget on test | passed |
| LightGBM | reason code coverage of every step up and decline | passed |
| LightGBM | score stability, PSI under the bound | passed |
| LightGBM with the attack IP flag | expected calibration error under the bound | passed |
| LightGBM with the attack IP flag | dollar capture above the floor, interval clear of zero | failed |
| LightGBM with the attack IP flag | friction within the budget on test | passed |
| LightGBM with the attack IP flag | reason code coverage of every step up and decline | passed |
| LightGBM with the attack IP flag | score stability, PSI under the bound | passed |

The interior test on the budget: stable.

| Budget | Freeman RBA | LightGBM |
|---|---:|---:|
| double | 2.5% | 35.7% |
| half | 0.0% | 25.9% |
| registered | 0.0% | 25.9% |

### Against the published RBA results

Risk score relation of the Freeman reimplementation against the table in the data set repository:

| Attacker | Ours | Published |
|---|---:|---:|
| naive | 0.00 | 8.48 |
| vpn | 5.47 | 5.05 |
| targeted | 2.30 | 8.94 |

Median re-authentication rate per user, over 94,003 users with ten
to fourteen earlier successful sign ins, against values read from figure rq1-general at a history of
twelve:

| Attacker | TPR | Ours | Published |
|---|---|---:|---:|
| naive | 0.980 | 100% | 9% |
| naive | 0.990 | 100% | 17% |
| naive | 0.995 | 100% | 24% |
| naive | 0.999 | 100% | 56% |
| targeted | 0.980 | 100% | 17% |
| targeted | 0.990 | 100% | 31% |
| targeted | 0.995 | 100% | 38% |
| targeted | 0.999 | 100% | 69% |
| vpn | 0.970 | 100% | 16% |
| vpn | 0.980 | 100% | 19% |
| vpn | 0.990 | 100% | 26% |
| vpn | 0.995 | 100% | 31% |
| vpn | 0.999 | 100% | 62% |

## The hypotheses

Registered in evaluation/hypotheses.yaml (hash 2fa80e2d5af8), each tested once on
validation, the five corrected together.

| Id | Question | A | B | Adjusted p | Verdict |
|---|---|---:|---:|---:|---|
| H1 | online purchases between midnight and six carry more fraud per dollar than chip purchases | 0.0000 | 0.0027 | 1.000 | not supported |
| H2 | purchases on a card's first visit to a merchant are more often fraud than repeat visits | 0.0266 | 0.0006 | 0.001 | supported |
| H3 | purchases abroad are more often fraud than purchases at home | 0.1778 | 0.0000 | 0.001 | supported |
| H4 | a successful sign in from a new network (ASN) is more often an attack than one from a new city on a known network | 0.0001 | 0.0000 | 0.001 | supported |
| H5 | sign ins from addresses on the attacker list fail more often than others | 0.4527 | 0.2552 | 0.001 | supported |

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
- TabFormer's fraud is far easier to separate than real card fraud: the payments LightGBM reaches a
  ROC AUC near one on the test months. The point of the payments desk here is the friction frontier
  and the gates, not the size of that number.
- The payment labels are simulated chargebacks on TabFormer's fraud flag, with a lag drawn from a
  lognormal. TabFormer marks no fraud in two stretches of its calendar (DECISIONS.md names them); the
  modeling window ends before the second.
- No real takeover falls in the login test months, so the login desk is graded on synthetic attackers
  built from the paper's definitions, which are not attackers.
- The attack IP flag is close to a label; the LightGBM model with it is printed beside the one without.
- The cost view multiplies probabilities by dollar assumptions that no bank supplied.
