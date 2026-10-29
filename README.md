# friction

**Not complete.** 11 of the eighteen mandatory lines of the definition of done are
done; lines 5, 10, 12, 13, 15, 17, 18 are not, and RESULTS.md says for each what exists and what it
waits on. Every number below says which engine produced it: the Lakeflow pipeline in the
Databricks workspace, or open source Spark running the same code.

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

## What it is

A fraud and identity risk lakehouse with two desks, graded on what it costs good customers. The
payments desk decides card transactions (approve, step up with a one time code where the channel
allows it, decline). The login desk decides sign ins (allow, re-authenticate, block). Each desk has a
floor (the rules an analyst writes on the first day), models, and a registered friction budget, and
every result is a point on the frontier of capture against friction at that budget.

## The frontier at the registered budget

Source: open source run, test months, thresholds chosen on validation and taken to test
unchanged, cluster bootstrap intervals over 1,000 resamples.

**Payments**, budget 10.0 per 1,000 step ups and 1.0 per 1,000
declines of good transactions. Champion: LightGBM.

| Model | Capture | Low | High | Step ups | Declines | Precision | ECE | ROC AUC | AP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| the floor | 1.8% | 0.0% | 4.4% | 9.75 per 1,000 | 0.98 per 1,000 | 0.0% | not applicable | 0.597 | 0.003 |
| LightGBM | 90.2% | 85.0% | 94.6% | 8.65 per 1,000 | 1.18 per 1,000 | 11.0% | 0.000 | 1.000 | 0.801 |
| logistic regression | 81.2% | 76.5% | 85.0% | 9.45 per 1,000 | 1.21 per 1,000 | 8.4% | 0.000 | 0.999 | 0.557 |

**Logins**, budget 100.0 per 1,000 re-authentications and 1.0 per 1,000
blocks of legitimate sign ins. Champion: none.

| Model | Capture | Low | High | Step ups | Declines | Precision | ECE | ROC AUC | AP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| the floor | 58.5% | 58.3% | 58.7% | 57.67 per 1,000 | 0.00 per 1,000 | 21.3% | not applicable | 0.863 | 0.320 |
| Freeman RBA | 0.0% | 0.0% | 0.0% | 0.00 per 1,000 | 0.00 per 1,000 | not applicable | not applicable | 0.384 | 0.020 |
| LightGBM | 25.9% | 25.6% | 26.1% | 41.30 per 1,000 | 0.82 per 1,000 | 14.1% | 0.026 | 0.636 | 0.096 |
| LightGBM with the attack IP flag | 27.5% | 27.3% | 27.8% | 60.37 per 1,000 | 0.74 per 1,000 | 10.8% | 0.026 | 0.613 | 0.095 |

Capture is of fraud dollars on the payments desk and of attacks on the login desk. ROC AUC and average
precision are threshold free and sit in the last columns because they are not the decision.

## How it is built

- **Sources.** The IBM TabFormer card transactions and the RBA login data set, fetched against digests
  (data/sources.lock.json), profiled at full size, and replayed as daily and monthly drops with a
  lateness rule (data/replay.lock.json). A simulator adds what the sources lack, labeled as simulated:
  chargebacks with a lag, card lifecycle changes as a CDC feed, and the attacker models of the RBA papers.
- **Two engines, one set of transforms.** packages/transforms runs in a Lakeflow pipeline on Databricks
  Free Edition (Auto Loader, expectations, AUTO CDC) and in open source Spark Declarative Pipelines on
  a GitHub Actions runner, both with the workspace salt. The two are compared by table digest and at the
  desk, month, day and user bucket grains (scripts/reconcile.py).
- **Features as of the event.** State machines in packages/features compute every feature from strictly
  earlier events; the API imports the same code, so online and offline agree by construction and parity
  checks it.
- **Grading in the repository.** The engines export scored sets; scripts/grade.py
  applies the registered plan (evaluation/plan.yaml, hashed in evaluation/plan.lock before any test
  month was scored).
- **Serving.** A FastAPI container with an online store and a decision log in Postgres
  (packages/api), a client budget, and a parity replay against the lakehouse.

## Skills, and where each one lives

| Skill | Where it lives |
|---|---|
| Lakehouse design (medallion, contracts) | pipelines/, contracts/, docs/lakehouse.md |
| Delta and Unity Catalog | pipelines/lakeflow, infra/terraform, docs/governance.md |
| Incremental and streaming ingestion | Auto Loader tables, the replay, the idempotency tests |
| CDC and slowly changing dimensions | AUTO CDC flows, packages/transforms/scd.py, the reconcile |
| Late arriving data and backfills | the lateness rule, the backfill job, the order tests |
| Orchestration and reliability | databricks.yml jobs, ops.run_ledger, docs/runbooks |
| Incident response and RCA | docs/incidents/, the monitor, the issue |
| Performance tuning | results/performance.json, notebook 03 |
| Cost management | the compute ledger, the quota plan |
| Infrastructure as code | infra/terraform (pg state), databricks.yml |
| CI/CD and promotion | .github/workflows, requests/, the gates |
| Data governance and privacy | tags, masks, the row filter, the scan |
| Data quality | expectations, quarantine, contracts |
| Testing for data | tests/, the reconcile, two engines |
| Fraud feature engineering | packages/features, the as of test |
| Model evaluation for fraud | packages/evaluation, the frontier |
| Decision policy and cost | packages/policy, config/costs.yaml |
| Explainability | packages/reasons, evaluation/reason_codes.yaml |
| Model risk documentation | model cards, validation reports |
| Identity and account takeover risk | the login desk, the Freeman model |
| Production serving and online features | packages/api, the online store, parity |
| Monitoring with delayed labels | the monitor task, maturity curves |
| Statistics for evaluation | cluster bootstrap, Benjamini-Hochberg, calibration, interior tests |
| Spark and PySpark | packages/transforms, notebooks |
| Agent assisted engineering | AGENTS.md, .claude/skills, docs/SPEC.md |
| Communication | report/briefing.md, the pushback paragraphs |

## Running it

```
uv sync --extra spark                    # Python 3.12, Java 21 for Spark
uv run pytest                            # Spark tests run through a local Spark Connect session
uv run python scripts/run_portable.py --landing data/fixtures/landing --out data/lake/fixture
uv run python scripts/run_features.py --lake data/lake/fixture
uv run python scripts/build_manifest.py && uv run python scripts/check_published_numbers.py
```

The workspace is reached only from GitHub Actions: commit a request under requests/ and the workspace
workflow runs it and writes the result to the results branch (docs/runbooks/requests.md).

## What a platform engineer would push back on

The data is synthetic and the payment labels carry a simulated delay. A file landing zone read with
Auto Loader is not a message bus. Free Edition hides the cloud account, so no bucket, role or key owned
by the build is shown. One workspace with prefixed schemas stands in for two workspaces and separates
names and grants, not compute. Takeover labels are rare and their intervals are wide. The RBA comparison
is against figures read from a repository.

## Licences

Code: Apache 2.0. The TabFormer data comes from IBM's repository (Apache 2.0, no separate terms for the
data). The RBA data set is CC BY 4.0; cite Wiefling et al., ACM TOPS 2022, doi 10.1145/3546069.
