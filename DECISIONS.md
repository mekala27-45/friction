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

## 2026-10-29 The payments modeling window ends on 31 October 2019

The first look (results/profile/tabformer.json) found that TabFormer marks no fraud from November
2019 to the end of the file in February 2020, and none from January to October 2017. A test month
with no fraud cannot grade a fraud model. The 36 month window is therefore 1 November 2016 to
31 October 2019: 24 months to train, a two month gap, four months of validation, a two month gap,
and July to October 2019 as test. The four months after it are the production period: replayed
day by day (the final 92 days of the desk), scored and watched for drift, never graded. The test
months' chargebacks arrive during that period, which is where label maturity is measured.
The 2017 gap sits inside the training months and the training window interior test (12, 18 and
24 months) shows what it does.

## 2026-10-29 TabFormer has 2,000 users, not 20,000

The paper describes 24 million transactions from 20,000 users. The released file has 2,000 users
(0 to 1,999) and 6,139 card accounts. The cluster bootstrap resamples the card accounts; every page
prints the measured counts.

## 2026-10-29 The floors were written from the feature definitions, before label rates by feature

The specification asks for the analyst's day one rules to be written before the data is opened.
The first look had already opened the data (row counts, ranges and fraud by month), but no rate of
fraud by any feature had been computed when packages/models/src/friction_models/floors.py was
written and committed. That is the honest version of the rule here.

## 2026-10-29 The state machines run beside the pipeline, not inside it

Open source Spark Declarative Pipelines refuse any operation that analyses a plan inside a flow
(ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION), and the Spark Connect planner analyses the input of a
grouped map (applyInPandas) when the flow is defined. So both pipelines end at materialized feature
inputs, and the features task of the jobs runs friction_transforms.gold.features_step over them,
the same function on both engines. The specification's job order already had features as its own
task after the pipeline update.

## 2026-10-29 Freeman global tables keep IP addresses and browser strings seen at least three times

The global proportion of an IP address or a full browser string needs a table with millions of
rows, which the scoring API would have to carry. Values seen fewer than three times in the training
months are treated as unseen, 1/(N+M), with N and M still the full totals; the smoothing of Freeman
et al. (2016, Eq. 8, p. 4) gives a value seen once or twice a proportion within a factor of about
two of that. The lakehouse and the API score with the same pruned model.

## 2026-10-29 Attacker models, read from Wiefling et al. (2022)

Page numbers are the running heads of the arXiv version (2206.15139); every attacker holds the
victim's password (Section 5, p. 6:9).
- Naive: half from a random address somewhere in the world with a random browser string, half from
  the busiest attack address with the busiest attack browser string (p. 6:9). Addresses come from
  the failed logins from known attack addresses, as in the paper.
- VPN: knows the victim's country; an address in it from the failed attempts, and one of the most
  popular browser strings of legitimate logins (p. 6:9).
- Targeted: knows the victim's location, device and browser; an address in the victim's country
  (its region when the attack data has one) and a browser string whose browser, operating system
  and device type match the victim's usual ones (p. 6:9).
The paper attacks each user at their last login (p. 6:19); this build attacks held out users at a
keyed random time in the test months, because the test months are what is graded.

## 2026-10-29 Freeman model as implemented

Score as Eq. 7 of Freeman et al. (2016, p. 3) without the reputation term, smoothing as Eq. 8
(p. 4) with M the number of distinct values seen (p. 9), hierarchies combined by linear
interpolation (Eq. 11, p. 5) with the weights of Wiefling et al. (2022, Section 4): IP 0.6, ASN 0.3,
country 0.1; browser string 0.53, browser 0.27, OS 0.19, device 0.01. A user with no successful
login yet is scored with the user terms equal to the global ones.

## 2026-10-29 Reason codes from LightGBM's own TreeSHAP

LightGBM's ``pred_contrib`` computes the same exact TreeSHAP values as the shap package's
TreeExplainer, without numba and the shap package in the scoring container. Logistic regression
uses the coefficient times the standardized value.

## 2026-10-29 The evaluation plan is registered

evaluation/plan.yaml, sha256 2ba61fcd4da0fe0e6fa6a73e155e016b21c55eed3b5f8e850ed93c677deeb4e2,
locked in evaluation/plan.lock before any test month of either desk was scored. Budgets: payments
10 step ups and 1 decline per 1,000 good events; logins 100 re-authentications and 1 block per
1,000 legitimate sign ins, chosen after reading the median re-authentication rates the RBA data set
repository reproduces (about 0.2 to 0.6 per user at a true positive rate of 0.995 to 0.999 for a
history of twelve logins).

## 2026-10-29 What the first look at the RBA file found

results/profile/rba.json, over all 31,269,264 rows:
- 4,304,857 distinct user ids. The README says more than 3.3 million users; 3,304,501 of them have
  at least one successful sign in, which is probably the number it meant.
- RTT is missing on 29,993,329 rows (96 percent). The feature `rtt_missing` carries that, and the
  user RTT z score is computed over the rows that have one.
- 7,291,335 addresses fall in 10.0.0.0/8 and 1,457,824 rows carry an ASN of 500,000 or more; the
  README says both were assigned during synthesis. Kept, flagged, counted (quarantine_rules.yaml).
- 141 attempts are marked as takeovers, all between February and November 2020. None falls in the
  registered test months (15 December 2020 to 28 February 2021), so the login test grades the
  synthetic attackers only, and the real takeovers are reported on validation with their count
  printed beside every rate. The plan was registered before this was looked at and is not changed.

## 2026-10-29 The unknown account bucket

One user id holds 14,025,899 attempts, 45 percent of the file, with four successes, 2,047,733
distinct addresses and 1,650,627 attempts from attack addresses. The next largest account has
70,028 attempts. It is the bucket the service files sign ins against unknown accounts under, not a
person. Read as an account it would put fourteen million rows through one user state machine and
make "new to this user" meaningless for it.

The rule is data driven, not an id: an account with more than 100,000 attempts delivered and
fewer than one success per thousand is flagged `unknown_account` in silver
(silver.login_account_stats). Its attempts stay in silver and feed the IP state machine, which is
where they say something; to the user state machine each one is a stranger with no history. They
are never graded, never chosen as a victim of a synthetic attack, and not counted as users. The
Lakeflow pipeline counts them with an expectation, `known_account`, so the event log shows the
number on every update.

## 2026-10-29 The hypotheses are registered beside the plan

evaluation/hypotheses.yaml, sha256 2fa80e2d5af8ecb9c6256355daf59cadc6faf63533b9159a213c5af86772aab8,
added to evaluation/plan.lock before any rate of fraud by feature was computed and before any
validation month was scored. The plan hash is unchanged. The groups are written as expressions over
the scored validation set, so the test that runs is the one that was registered.

## 2026-10-29 Sampling weights, not class balancing (reverses the class weights of commit 07e85c5)

The first training step weighted positives up to balance the classes, as the specification's "class
weights on training only" suggests. That made every predicted probability several times too high,
and the expected cost view and the calibration gate both read the probability as a probability.
Reversed: negatives are sampled down to 1,500,000 by a keyed draw and carry the inverse of their
sampling rate as a weight; positives carry one. The weights are used on training only, which is the
part of the instruction that matters, and the models stay calibrated.

## 2026-10-29 The unknown account is a flag, not a user (reverses treating every id as a person)

Until the full size profile, the pipeline treated every user id as a person, which is what the
fixture slice could show. The profile found one id with 14,025,899 attempts. Reversed: see "The
unknown account bucket" above.

## 2026-10-29 The API writes SQL through psycopg, not SQLModel and Alembic

The specification names SQLModel and Alembic. The API has four tables and one transaction shape (lock
the state rows, write state and decision, commit), and the persistence tests are about that
transaction. Plain psycopg with explicit SQL shows it line by line; the schema is idempotent DDL run at
start (packages/api/src/friction_api/store.py). Alembic would add a migration history for a schema
that has had one version.

## 2026-10-29 Postgres for tests is a CI service container, not testcontainers

The CI runner has no Docker in Docker by default and the local build machine has Postgres 16. The
tests read FRICTION_TEST_DATABASE_URL and skip by name without it; scripts/check_test_dependencies.py
fails CI if Postgres, Java or pyspark is missing there, so CI cannot pass by skipping.

## 2026-10-29 The API accepts restricted values tokenized at the edge

A login event carries an IP address. The repository, the site and the parity set may not hold one, so
the API accepts the salted tokens (ip_key, ua_key, user_key) that an upstream tokenizer would produce,
and also the raw values, which it tokenizes in memory and never stores. The parity replay and the
replay page send tokens.

## 2026-10-29 The serving image installs the scoring path only

The Vercel container installs numpy, scipy, LightGBM, FastAPI, uvicorn, psycopg, pydantic, PyYAML and
structlog at the versions in uv.lock, and copies the six packages the API imports from their sources.
No pandas, Spark or DuckDB goes in. tests/api/test_serving_image.py checks the pins against the lock and
that no module on the path imports a heavy package at import time. Render is the named fallback host
for the same image (render.yaml, Dockerfile.render). Source: Vercel, Container Runtime, updated 7 July
2026: a root Dockerfile.vercel selects the container preset, port 80 by default, beta on all plans.

## 2026-10-29 The pipelines CLI runs on one thread unless told otherwise

`spark-pipelines run` starts its own Spark Connect server with `--remote local`, which is one thread.
The first full local run took that default; scripts/run_portable.py now passes
`--remote local[<cores>]`.

## 2026-10-29 Reason codes for the highest scores only

LightGBM's exact TreeSHAP measured about a millisecond per row on the build machine, and the scored
sets hold millions of rows. Scoring now runs in two passes: every row gets every model's score, then
the rows at or above each model's top five percent (payments) or fifteen percent (logins) get reason
codes. At the registered budgets a model flags about one percent of good payments and ten percent of
good sign ins, so every step up and decline falls inside; the coverage gate checks it on test, and
the API computes codes for every event it flags.

## 2026-10-29 The replay lock is taken under the pinned pandas

The first replay lock was written under pandas 3.1.0. pandas was later pinned below 3 for the
Spark side, and 2.3.3 writes the reported_at column of the logins label files with nine fraction
digits where 3.1.0 wrote six. The rows and the instants are the same and Spark parses both, but
the bytes differ, so the workspace refused to upload drops that did not match the lock. The lock
was regenerated under the pinned version; the fourteen logins label files are the only change.
The runner now keeps the regenerated lock beside a refused upload, so the next mismatch shows
which files moved.

## 2026-10-29 The unknown account bar is 100,000 attempts, so it holds on one month

The first bar was 1,000,000 attempts, set against the whole file. The workspace lands month by
month, and in the first month the bucket has 720,476 attempts and one success, under that bar. It
went through the user state machine as one account, one pandas frame of 720,476 rows, and the
features task died on the 1 GB serverless cap for Python UDF code. At 100,000 the full file flags
the same single account (the next largest has 70,028 attempts), and the first month flags it too.
The partition functions were also rewritten to fill one block of doubles, and the groups went
from 64 to 1,024; neither changes a feature.

## 2026-10-29 The open source runs that must match the workspace run on the Actions runner

The plan was for the build machine to run the full open source pipeline and reconcile it with the
workspace. Two things moved it. The build machine never holds the production salt, so every salted
table it writes differs from the workspace's by construction and only the unsalted grains could be
compared. And this build machine has 2 cores, 8 GB and 19 GB of free disk: the login silver
shuffle filled the disk twice. The Actions runner holds the salt as a secret and has 4 cores and
16 GB, so a request of kind portable copies drops from the outbox volume and runs the same
pipeline, features, models, grade and model export there. Every salted table can then be compared
by digest: the first month matched on all nine exported tables, and over every row eight of nine
matched (the card age feature differs and is being traced). The build machine keeps the fixture
runs, the tests and the gates, with the public test salt.

## 2026-10-29 What the Freeman grade shows, and why the model stays as the paper wrote it

On the full data the Freeman reimplementation fails the login gates: ROC AUC 0.384 on the test
months, and no threshold meets the budget, because every first sign in of a user gets the same
score (the user terms fall back to the global ones and p(u | legit) is at its smallest), and those
sign ins are a large share of the good traffic. Against the published figures the risk score
relation for the vpn attacker reproduces (5.47 against 5.05) and the targeted one is lower (2.30
against 8.94). The naive attacker is the outlier (0.0002 against 8.48): the simulator picks its
country uniformly from the countries in the attack pool, so it signs in from places almost no
legitimate user signs in from, and Freeman uses the global share of a value as its attack
likelihood, which makes those sign ins look unlikely for an attacker too.

The histories hold successful sign ins only, as the evaluation paper does (Wiefling et al. 2022,
Sections 11.1 and 12.1). The model is not refit after reading the test months; the result is
published as it came out, the login desk serves its floor, and the naive attacker's country draw
is the first thing to change in a version two, registered before any test month is read again.

## 2026-10-29 The day 40 contract change

The workspace monitor (request 2026-10-29-12g-monitor, Actions run 40958240161) raised four
alerts on drop 0387_2020-01-07 and the workflow opened GitHub issue 1. The event counts and the
quarantine identified three faults in the day's delivery: the previous day's file resent under a
new name (deduplicated by natural key, nothing lost); part one of the day with the mcc column
renamed merchant_category (2,920 rows in rescued data, quarantined by P_RESCUED and P_MCC); and
part two with every timestamp six hours ahead (487 rows caught by P_FUTURE, completeness 0.702).
Approved: read merchant_category back as mcc, reject payments_2020-01-07_part2.csv.gz whole, and
land the corrected redelivery the source sent for part two. Nothing in bronze is edited; the
change is contracts/approved_changes.yaml and the replay is a backfill of drop 387 with the
redelivery. The same monitor run flagged drift in mcc_fraud_rate across the 2019 months against
the training months (PSI near 2.8); that is the label calendar of TabFormer, not the incident,
and it is recorded in the postmortem, not fixed here.

## 2026-10-29 Reversal: a rejected file's rows no longer win a key

The first replay of drop 387 in the workspace (request 2026-10-29-13a) kept 2,920 rows of the 5,849
the corrected source holds. Deduplication ranked the copies of a transaction by drop and file, so the
rejected part two, which arrived first, held every part two key, and the corrected redelivery, which
the land job puts in a drop folder of its own after the drop it corrects, was quarantined as a
conflicting repeat. The local replay had passed because it placed the redelivery inside the same
drop folder, where its file name sorted first. Two changes: rows of a rejected file rank after every
accepted copy of their key, and a redelivery's drop id reads its date without the suffix, so it
closes with the drop it corrects. The local replay now lands the redelivery as the land job does,
and tests/transforms covers both. The first replay's count is kept in
results/workspace/incident_drop_first_replay.json.
