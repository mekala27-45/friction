# friction: the specification

Sections 1 to 21 of the build prompt, as written on 28 October 2026. Deviations are recorded, dated, in DECISIONS.md; where this file and DECISIONS.md disagree, DECISIONS.md is what was built.

## 1. What we are building

friction is a fraud and identity risk lakehouse on Databricks Free Edition with two desks. The payments desk scores card transactions from the IBM TabFormer data set; the login desk scores sign ins from the RBA login data set. Both arrive as files in a landing volume, replayed day by day after a monthly historical backfill, with planted lateness and defects. A Lakeflow pipeline takes them through bronze, silver and gold with expectations and a quarantine; AUTO CDC keeps the card lifecycle and the labels with history, the labels on two clocks. Gold holds point in time features. Lakeflow Jobs land, process, train, score, monitor and export, every task idempotent and re-runnable. Unity Catalog classifies, masks and grants; Terraform and a bundle make all of it code; GitHub Actions is the only machine that touches the workspace. Each desk has a rules floor and models graded on the friction frontier under budgets registered before the test months were opened. A scoring API on Vercel with an online store in Neon reproduces the lakehouse decision event for event. A monitor watches drift and delayed label performance, and one planted incident is detected, handled and written up. The site shows the frontier, both desks, the platform, the governance, the operations and a live replay.

The reader is a hiring manager for a Databricks data engineer, a fraud data scientist or a machine learning engineer at a bank, deciding in ten minutes whether this person can build the system, not just the model.

## 2. Hard constraints

- **The eighteen mandatory steps in section 17 are the product.** The build is not done while any of them is missing, and the README first paragraph says which are missing until none are.
- **The workspace is deployed at step six**, before any model or web page: the bundle deployed to the dev target from Actions, the pipeline run on one landed month, and the probe, the run and the reconcile results committed; the workspace URL of the dev target is in the README from that step on.
- **Every row of both data sets.** The TabFormer file in full and the RBA file in full, landed, cleaned, featured and scored. The modeling windows are stated slices of the full history; every earlier row still feeds the features. Nothing is sampled anywhere, and a test asserts the silver row counts equal the source counts minus the quarantine.
- **Actions holds the token, nothing else does.** The workspace host and token live only in GitHub Actions secrets. The build machine never sees them and never calls the workspace. Every workspace run is requested by a committed file under requests/, run by a workflow, and its outputs come back on a results branch for a reviewed promotion with a provenance file. A scan in CI fails on anything that looks like a Databricks token or host credential in any tracked file.
- **Idempotent and re-runnable, proven.** Re-running any landed day leaves every gold table identical to its digest; a backfill over a range already processed changes nothing; out of order delivery of late events and late labels gives the same final tables as in order delivery. Each is a test that runs in the workspace integration job.
- **Labels on two clocks.** Valid time and system time on every label row; a training set cut at a date reads only labels whose system time is on or before it; a test plants a label known one day after the cut and asserts it is absent.
- **Point in time features.** Every feature is computed from events strictly before the event it describes; the leakage test recomputes 500 sampled feature vectors from raw events with an independent implementation and asserts equality.
- **No resampling of validation or test**, ever. Class weights or resampling on training only, recorded.
- **The floor is graded like a model**, on both desks, on every scorecard, as the first row.
- **The workspace trains and scores; the repository grades.** Every metric is recomputed in CI from the exported scored test sets by code in packages/evaluation, and the claim gate renders only from that.
- **The pre-registered plan**: splits, budgets, metrics, the operating point rule, the gates and the attacker models, hashed and committed before the first test month is scored.
- **No card number, CVV, address or raw IP past bronze.** Source columns that carry them are dropped or tokenized at bronze by the contract; the planted identifier scan runs over silver, gold, every export, the decision log and the site bundle.
- **Free tier only, no card.** Databricks Free Edition, Vercel Hobby, Neon, GitHub Pages and Actions, Render free as the fallback. The probe runs before the first deploy and every deploy step reads it.
- **The statement on every surface**, gated. **No em dashes anywhere.** The vocabulary gate.
- **Every number in a document is rendered from the manifest** by the Day 3 renderer, whole file diff in CI.
- **Forty to sixty conventional commits**, made as the build proceeds, one module at a time, with the tag pushed after the commits.

## 3. Stack

```
Core            Python 3.12, uv workspace, StrictModel ported from trajectory,
                Pydantic v2 for every contract, request and response
Spark           PySpark 4.2 with the pipelines extra for local work and CI, run
                through a local Spark Connect session so behavior matches
                serverless; open source SDP (spark-pipelines) for the portable
                pipeline; chispa for DataFrame equality in tests
Workspace       Databricks Free Edition; Unity Catalog; managed volumes; Auto
                Loader (cloudFiles) into streaming tables; Lakeflow Spark
                Declarative Pipelines (pyspark.pipelines as dp, expectations,
                create_auto_cdc_flow with SCD type 2 and bitemporal tracking where
                the workspace offers it); Lakeflow Jobs; MLflow 3 with models in
                Unity Catalog and aliases; serverless SQL warehouse for exports
Deployment      Databricks Asset Bundle (databricks.yml) with dev and prod targets;
                the Databricks CLI and the Python SDK in GitHub Actions only;
                Terraform with the databricks provider at workspace level, state
                in Neon through the pg backend
Contracts       YAML data contracts per table (schema, units, keys, freshness,
                classification), checked against the pipeline schemas in CI
Models          scikit-learn (logistic regression), LightGBM, the rules floors,
                the Freeman et al. RBA model written from the paper; SHAP
                TreeExplainer for reason codes
Evaluation      packages/evaluation: frontier, budgets, cluster bootstrap over
                users, Benjamini-Hochberg, interior tests, calibration, the RBA
                comparison; runs in CI on exported scored test sets
API             FastAPI, async, deployed as a Vercel container (Dockerfile.vercel,
                port 80); SQLModel over Neon for the decision log, the online
                store and the client budget; Alembic with the versioned table
                pattern from Day 13
Rendering       the Day 3 claim gate renderer, Jinja templates, the manifest
Web             Next.js 16, TypeScript strict, Tailwind v4, static export on GitHub
                Pages, DuckDB-WASM over the committed results, the series SVG chart
                library, link prefetch off
Quality         ruff, mypy strict, pytest, hypothesis, testcontainers for Postgres,
                nbmake, skills-ref, Playwright against a test server that refuses
                HEAD and answers a first 503, 80 percent coverage floor
Logging         structlog, JSON, ids only, never a card, a user id in clear, an IP
                or a token
```

## 4. Architecture

```
  SOURCES      TabFormer transactions (IBM, git LFS, 24 million rows)
               RBA login data set (das-group, release asset, 33 million rows)
        |
        v  fetch, digest, lock      data/sources.lock.json, PROVENANCE.md
        v  replay splitter          monthly backfill drops, then one drop per
                                    day for the final 92 days of each desk;
                                    planted lateness, the planted incident
        v  simulator                chargeback lag, card lifecycle CDC feed,
                                    RBA attacker models (known truth)
   +--------------------------------------------------------------------------+
   |  DATABRICKS FREE EDITION (deployed by the bundle from GitHub Actions)    |
   |    volumes   /landing/payments  /landing/logins  /landing/cdc  /labels   |
   |    bronze    streaming tables by Auto Loader, rescued data, file metadata |
   |    silver    typed, deduplicated in event time, conformed; quarantine    |
   |    cdc       cards (SCD2), labels (bitemporal SCD2) by AUTO CDC          |
   |    gold      point in time features, training sets as of a cut, scores,  |
   |              decisions, metrics; liquid clustering                       |
   |    ops       run ledger, SLOs, drift, alerts, compute ledger, event log  |
   |    ml        MLflow experiments, models in Unity Catalog (champion,      |
   |              challenger aliases)                                         |
   |    jobs      land > pipeline > features > train > score > monitor >      |
   |              export; backfill; governance; integration                   |
   +--------------------------------------------------------------------------+
        |  results branch (requested by commit, promoted by review)
        v
   REPOSITORY   packages/evaluation grades the exported scored test sets
                manifest.json > renderer > README, RESULTS, briefing, cards
        |
        v
   VERCEL       /v1/payments/score  /v1/logins/score  /v1/decisions/{id}
                /health; Neon: decision log, online store, client budget
   PAGES        frontier, payments, logins, platform, governance, operations,
                replay, models, report
   CI LOCAL     the portable pipeline on open source Spark 4.2 over a fixture
                slice and the full files; reconcile against the workspace
```

State in ARCHITECTURE.md, in plain sentences: why files in a volume and not a message bus (the build has no broker on a free tier, and most bank lakehouses ingest batch extracts and CDC exports as files; the README says so); why Actions is the only machine with the token; why the repository grades and the workspace does not; why two engines; why the labels are bitemporal; why the online store is Postgres and what a key value store would change; why one workspace with two catalogs stands in for two workspaces, and what that does not prove.

## 5. The two data sets, the replay, the simulator and the quarantine

**TabFormer.** The synthetic card transactions from the TabFormer repository of IBM: 24 million transactions from 20,000 synthetic users per the paper ([Padhi et al., ICASSP 2021](https://arxiv.org/pdf/2011.01843); generator described in [Altman, 2019](https://arxiv.org/pdf/1910.03033)). The file is `data/credit_card/transactions.tgz` under git LFS; the pointer committed in the repository names `oid sha256:e9f589a0958f40d60f81b1a2e8428db86e00c05755caf44fb055827976c0efa2` and `size 278576638`, and the fetch verifies both. The README of the repository gives a direct link at IBM Box for when LFS bandwidth runs out; the fetch tries LFS first and the Box link second, and records which one served the file. The repository is Apache 2.0; record in PROVENANCE.md what the data itself says about its terms, quoting it, and treat it as research use. Read every column name, type and range from the file itself before writing a transformation, and record the span of years; the modeling window is the final 36 months of the file and every earlier month feeds the features.

**RBA.** The login data set for risk based authentication from das-group: more than 33 million login attempts by more than 3.3 million users, synthesized from a single sign on service in Norway whose original data was collected from February 2020 to February 2021; fifteen features including IP address, country, region, city, ASN, user agent, OS, browser, device type, user id, login timestamp, round trip time, login successful, is attack IP and is account takeover; CC BY 4.0, with the TOPS article to be cited ([repository](https://www.github.com/das-group/rba-dataset), [Wiefling et al., ACM TOPS 2022](https://arxiv.org/pdf/2206.15139.pdf)). The repository warns that the feature values are artificial and must not be used in productive systems; that IP addresses and user agents were generated from publicly available data, so an address can look like, or be, a real one; and that some unrealistic values had to be assigned (ASNs of 500,000 and above, addresses in 10.0.0.0/8) which do not change the Freeman risk scores. All three go into PROVENANCE.md, the statement covers the first, the second is why the IP is a restricted column, and the third values are kept and flagged, with their count published, not quarantined. The release asset is downloaded in Actions; its SHA-256 is recorded on first download in data/sources.lock.json and every later fetch is held to it.

**The fetch.** scripts/fetch_sources.py, run in Actions, writes both files to the runner, verifies them against the lock, and uploads them to the raw volume with the Databricks CLI. A planted wrong digest fails the fetch, with a test. A fetch status file records what was asked for, from where, when, and the result.

**The first look, before any transformation.** Profile both files at full size on the build machine with Spark: row counts, nulls per column, ranges, distinct counts, label rates by month, the time span, duplicated natural keys, and the ten strangest rows of each desk. The first ten real defects become the first quarantine rules, each with a test. The days 14 and 15 lesson stands: the real file finds what no fixture does.

**The replay splitter.** scripts/replay.py cuts each source by event time into drop files: one file per month for the history, then one file per day for the final 92 days of the desk. A seeded lateness rule moves a stated share of events into a later drop (most by one day, a tail by up to seven), so the pipeline meets late data the way a bank does. The splitter is deterministic, its outputs are digested, and the digest goes into the lock file.

**The planted incident.** On replay day 40 of the payments desk the splitter adds three faults, stated in config/incident.yaml: the file of the previous day delivered again under a new name, a file in which the upstream renamed one column, and a file whose timestamps moved six hours. Section 13 says what must happen.

**The simulator, labeled as simulated everywhere it appears.**

```
chargeback lag       TabFormer marks fraud at the transaction. The simulator gives
                     each fraud a report date from a stated lag distribution in
                     config/simulator.yaml (default: median three weeks, a tail to
                     four months, a stated share never reported), seeded and
                     sorted before sampling. The evaluation is repeated under two
                     other lag settings and the frontier is printed for each.
card lifecycle CDC   From the reported frauds and a stated rate of routine events:
                     a card blocked on the report date, reissued a few days later
                     under a new card key, credit limit changes, closures. Emitted
                     as a CDC feed (operation, key, sequence, payload) into
                     /landing/cdc, including out of order and duplicate records.
RBA attacker models  The naive, VPN and targeted attackers as the RBA papers define
                     them, generated for held out legitimate users in the test
                     months: the login of the attacker carries features drawn as the
                     paper states for each model. Every definition is a
                     DECISIONS.md entry citing the page it came from.
```

**The quarantine**, named, counted, nothing deleted, each rule with its test: a natural key seen twice with different payloads; an event time in the future of its drop; an amount that does not parse or is negative where the source says it cannot be; a merchant category code outside the published range; a login timestamp outside the stated span of the data set; an IP that does not parse; a user agent that does not parse; a row whose schema does not match the contract (rescued data present); a CDC record whose sequence goes backwards; a label for a key that does not exist. Publish the quarantine by desk, rule and drop.

## 6. The lakehouse

**Catalogs and schemas.** One catalog per environment, `friction_dev` and `friction_prod` if the probe finds catalog creation allowed, otherwise schemas with environment prefixes under the workspace catalog, recorded in DECISIONS.md. Schemas: raw (volumes), bronze, silver, cdc, gold, ml, ops. Created by Terraform (section 8), never by hand.

**Bronze.** Streaming tables fed by Auto Loader from the landing volumes, with schema hints from the contracts, the rescued data column on, and the file path, file modification time, ingest time and drop id on every row. Card numbers, CVVs and addresses, where a source file carries them, never enter bronze columns: the contract maps them to a salted token or drops them at read, and the scan proves it.

**Silver.** Typed, conformed and deduplicated in event time with a watermark stated per desk; expectations in three classes (warn, drop to quarantine, fail the update) with every drop routed to the quarantine table with its rule, never discarded silently; expectations stored as a Unity Catalog table if the workspace offers it, as the 2026 notes describe. Amounts carry their currency; rates say per what.

**CDC.** `dp.create_auto_cdc_flow` keeps cards as SCD type 2 with a sequence column and the out of order and duplicate records from the feed; labels as SCD type 2 with bitemporal tracking if the probe finds it, otherwise with valid from, valid to, known from and known to columns written by the flow and a DECISIONS.md entry. **The reference implementation**: packages/transforms/scd.py writes the same history in plain PySpark from the same feed, and the reconcile asserts the two agree on every key, every version and every interval.

**Gold.** Point in time features per event (section 9), training sets built as of a cut by a builder that reads labels by system time, scores, decisions with reason codes, metric tables by day and cohort. Liquid clustering on the event tables by the key that the feature queries filter on, chosen by a measurement (section 7). A REPLACE WHERE flow, where the workspace offers it, rewrites a stated date range for the backfill job.

**Two engines, one answer.** packages/transforms holds every transformation as a pure function from DataFrames to a DataFrame. pipelines/portable/ wires them with open source SDP decorators only, and runs in CI with `spark-pipelines run` over the fixture slice and, on the build machine, over the full files. pipelines/lakeflow/ wires the same functions with the workspace decorators, expectations and AUTO CDC. results/reconcile.json compares the two at four grains (desk, month, day, user bucket) on row counts, distinct keys, sums of amounts, label counts and feature sums, with a stated float tolerance of one part in a billion and zero tolerance on counts.

**Contracts.** contracts/<table>.yaml: columns, types, units, keys, freshness objective, classification per column, and the owner. A CI check compares every contract with the schema the pipeline declares and with the schema the workspace reports (from the export), and fails on any difference that is not an approved change in DECISIONS.md.

## 7. Jobs, backfills, reliability, performance and cost

**The jobs**, defined in the bundle:

```
daily        land_drop > pipeline_update > features > score > monitor > export
backfill     a date range: land the drops in order > pipeline update with a
             full refresh of the range (REPLACE WHERE where offered) > features
             > score; parameters committed in the request file
train        build training sets as of the cut > fit the floor and the models
             > log to MLflow > register in Unity Catalog > alias challenger
evaluate     score validation and test months with the registered models >
             export scored sets
governance   tags, masks, grants that Terraform cannot express, applied as SQL
             from committed files
integration  the fixture slice end to end in the dev target, plus the
             idempotency, out of order and incident tests
```

Every task is idempotent by construction (it writes by key or replaces a stated range, never appends blindly), has retries with backoff for transient failures and none for data failures, and records a row in ops.run_ledger (job, task, run id, inputs digest, outputs digest, rows in and out, compute seconds, status). Job concurrency stays at or under the Free Edition limit of five tasks, and one pipeline per environment holds both desks because Free Edition runs one active pipeline per pipeline type, so dev and prod updates never overlap; the bundle says both in comments and a test reads the bundle and counts.

**Requests.** requests/<date>-<name>.json names the job, the target and the parameters. The workspace workflow runs every request committed since its last run, writes outputs to a results branch, and opens a pull request or, if the token cannot, leaves the branch for the build machine to fetch. The build machine reviews and promotes the results into results/ with results/provenance.json (request, run id, commit, digests). This is how the backfill, training, evaluation and exports all run.

**The quota as a design input.** The probe measures compute on one landed month of each desk and writes results/workspace_probe.json with the projected compute for the full backfill. The backfill is split into requests that fit inside the fair use of one day with margin. If the workspace shuts compute down, the job fails cleanly, the run ledger says where it stopped, and the next request resumes; the README first paragraph states how much of the backfill has landed until all of it has.

**Idempotency and order, as tests.** In the integration job: land day 10, run, digest gold; land day 10 again, run, assert identical digests. Deliver days 11 to 13 with their late events in order, digest; reset, deliver them shuffled with the late events first, assert identical digests. Run the backfill over a range already processed, assert no change. Deliver labels out of order, assert identical label history.

**SLOs.** Freshness (gold reflects a drop within one daily run of its landing), completeness (silver rows equal source rows minus quarantine, per drop), correctness (zero failed fail class expectations), and parity (online equals offline, section 12). Each objective is a row in ops.slo with its target, its measurement per drop and its error budget for the replay window.

**Performance, measured, not asserted.** Three experiments, each with the query profile metrics the workspace exposes (time, rows, bytes and files read, spill) before and after, committed as results/performance.json and drawn on the platform page: the skewed join (the merchant dimension against transactions, where a few merchants carry a large share of rows; measure, then salt or broadcast, measure again); file sizing (daily drops create small files; measure the files read by a feature query, then optimize, measure again); clustering (liquid clustering keys chosen two ways, measured on the three heaviest gold queries). Serverless has no cluster to size; the notebook says so and measures what serverless lets you change.

**The compute ledger.** Compute seconds per task and per run from the run ledger, and usage from the billing system table if the probe finds it readable; cost per million events by desk and stage; the share of the daily quota each job used. Printed on the platform page with what changed it.

## 8. Governance, infrastructure as code and promotion

**Classification.** Every column carries a tag from its contract (public, internal, sensitive, restricted). Restricted columns (the user id in clear, the IP, the user agent string, any token) are masked by column masks for every reader but the identity the pipeline runs as: the IP reduced to its /16, the user agent to its family, the user id to its salted token. If the workspace has no groups (Free Edition has no account console), the mask functions read a committed entitlements table keyed by `current_user()`, a documented Unity Catalog pattern, and DECISIONS.md says why. A row filter limits a reader role to one desk. The governance page shows a masked and an unmasked read of the same synthetic rows, side by side, with the identity that ran each.

**Lineage and audit.** The lineage system tables, if the probe finds them readable, exported as the lineage graph on the platform page with row counts on every edge; otherwise the lineage is derived from the pipeline graph and the job DAG, labeled as derived.

**The scan.** scripts/scan_for_planted_identifiers.py, ported from ledger with its Luhn check, runs over silver, gold, every export, the decision log, the traces of the API and the site bundle, and fails on a 13 to 19 digit Luhn valid string, a CVV shaped field, a street address pattern, or a raw IPv4 address anywhere past bronze.

**Terraform.** infra/terraform with the databricks provider authenticated by the workspace token in Actions only: catalogs or schemas, volumes, grants, the schema of the entitlements table, the SQL warehouse settings it can manage. State in Neon through the `pg` backend, schema terraform. `terraform fmt`, `validate` and `plan` on every push, with the plan as a workflow artifact; `apply` only from a committed request, and the apply output promoted like any other result. A planted drift (a grant removed by hand in the workspace) is detected by the next plan, with the plan output kept as evidence.

**The bundle.** databricks.yml with dev and prod targets, the dev target in development mode, the pipelines, jobs, models and their permissions. `databricks bundle validate` on every push; deploy to dev on every push to main; deploy to prod only from a committed promotion request after the integration job is green on the same commit.

**CI.** Lint, types, unit tests with local Spark Connect, the portable pipeline dry run and run on the fixture slice, contracts, the scan, the gates, bundle validate, Terraform plan, the web build and Playwright. The workspace workflow is separate and runs only from requests.

## 9. The two desks

**The payments desk (TabFormer).**

```
Features, all as of the event, all from strictly earlier events:
  amount against the history of the card (z score, ratio to median), and in
  dollars; counts and sums by card in the last 1 hour, 24 hours and 7 days;
  time since the previous transaction of the card; merchant novelty for it;
  merchant category novelty; merchant category fraud rate from labels known
  as of the event only; channel (chip, swipe, online) and channel novelty;
  merchant state against the usual states of the card; recent error codes;
  hour of day against the usual hours of the card; days since card issue or
  reissue (from the CDC history)
Channel decides the policy space: online can be stepped up (a one time code);
  chip and swipe can only be approved or declined
Floor: the day one rules of an analyst, written before the data is opened and
  committed: velocity, amount against history, new merchant online at night,
  a reissued card used within a day; graded exactly as the models are
Models: logistic regression on standardized features; LightGBM; class weights
  on training only
Splits over the 36 month window: 24 months train, a 2 month gap, 4 months
  validation, a 2 month gap, the final 4 months test; the gaps cover the bulk
  of the chargeback lag and the tail is what the as of view is for; each
  training set is cut on the last day of its training months and reads labels
  as known on that day; test is graded on labels as eventually reported, with
  the as of view printed beside it
```

**The login desk (RBA).**

```
Features, as of the login: whether the user has been seen from this IP, ASN,
  country, region, city, user agent, browser, OS and device type, with counts;
  time since the last successful login; round trip time against the
  distribution of the user and of the location; failed logins in the last
  hour for the user and for the IP; the attack IP flag, treated as an external
  reputation feed and reported with and without, because it is close to a
  label
Policy: allow, step up (re-authenticate), block
Floor: new device and new country rules, written before the data is opened
Models: the Freeman et al. (2016) statistical model as the RBA papers state
  it, written from the paper with its smoothing and features
  ([NDSS 2016](https://dev.ndss-symposium.org/wp-content/uploads/2017/09/who-are-you-statistical-approach-measuring-user-authenticity.pdf)),
  and LightGBM on the same features
Splits by time: 60 percent train, 20 validation, 20 test, with gaps; the
  attacker models are generated for held out users in the test months only
Truth: the attacker models (known by construction) and the is account
  takeover label (rare; its count is printed beside every rate it feeds)
```

**The published bar.** Before the login models are fit, transcribe the reproduced results from RESULTS.md in the data set repository (the risk score relations in its table, and the median re-authentication rates its figures show, each with the figure cited) into evaluation/rba_published.yaml. The numbers of the reimplementation print beside them on the login page with one sentence on each difference. The repository notes that its own stratification may differ; the comparison is context, not a pass or fail.

## 10. The friction frontier and the evaluation

**Metrics, defined once in packages/evaluation/metrics.py.** Capture rate by count and by dollars (frauds declined or stepped up over all frauds); friction (good events declined or stepped up per 1,000 good events, and declines alone); false positive rate; alert precision over labeled decisions only (the rule from ledger) and over all decisions, both printed; on the login desk, the re-authentication rate of legitimate logins and capture per attacker model and on account takeovers.

**The frontier.** For every model and floor: capture against friction across thresholds, with the policy applied (a step up only where the channel allows it). The chart is the hero of the site.

**Budgets and the operating point.** evaluation/plan.yaml registers the friction budgets per desk (default: 10 step ups and 1 decline per 1,000 good payment events; on the login desk a re-authentication rate chosen after reading the rate the RBA papers report, written into the plan before the test months are scored), the cost parameters for the expected cost view (config/costs.yaml, each a labeled assumption), and the rule: choose the thresholds on validation that maximize dollar capture within the budget. Report on test.

**Intervals.** Cluster bootstrap over users (on the payments desk the synthetic users, whose cards move together), 1,000 resamples, computed from per user sufficient statistics so a resample is a weighted sum, not a pass over the events. Model against floor and model against model differences in capture at the operating point, with intervals and Benjamini-Hochberg across the family.

**The interior test** on the budget (half, as registered, double), the lag setting (the three of the simulator), the bootstrap seed and the training window length (12, 18 and 24 months, all ending at the same cut). The page prints the test capture of the champion under each neighbor and the word degenerate if the champion changes at the first neighbor.

**Calibration.** Expected calibration error and a reliability curve per model on test, because the expected cost view multiplies a probability by a dollar amount.

**Hypotheses, registered.** evaluation/hypotheses.yaml, five questions an analyst would ask, written before the data is opened (for example: online transactions at night carry more fraud per dollar than chip transactions; a first login from a new ASN carries more takeover risk than a first login from a new city), each tested once on validation months with the family corrected, each printed with its verdict whatever it is.

**The secondary reading.** ROC AUC and average precision are printed in the last columns of the scorecard, labeled as threshold free and not the decision.

## 11. Reason codes, model governance and promotion gates

**Reason codes.** reason_codes.yaml: twelve codes per desk in plain words (for example P03, several purchases on this card within an hour; L02, first sign in from this country). Every step up and decline carries the top three codes by SHAP contribution among features that pushed the score toward the decision, mapped through the table. A test asserts every non approve decision has at least one code, codes are stable under a perturbation below the rounding of the feature, and the decisions of the floor carry the code of the rule that fired.

**The model contract (ledger).** A model card per model, a validation report in SR 11-7 order per champion candidate (purpose, data, design, performance on validation and test, stability, sensitivity, limitations, monitoring plan, the effective challenges on record), rendered by the claim gate and refusing to render with a section missing.

**Promotion gates**, each with its deliberate failure test: dollar capture at the registered budget above the floor with the interval of the difference clear of zero; friction within the budget on test; calibration error under a stated bound; score stability between validation and test (PSI under 0.2); reason code coverage 100 percent; parity between the API and the lakehouse (section 12) exact on decisions; API p99 latency under a stated bound. A model that clears every gate takes the champion alias in Unity Catalog by a committed promotion request; a model that does not stays a challenger and the site says which gate it failed.

## 12. Serving, the online store and parity

**The export.** The evaluate job exports the champion of each desk from Unity Catalog in a portable form (LightGBM text model and the preprocessing parameters; the Freeman tables as parquet), with a digest, into results/models/<desk>/.

**The API.** FastAPI in a Vercel container: POST /v1/payments/score and POST /v1/logins/score take one raw event, read and update the online state for its card or user in Neon in one transaction, compute features with the same packages/features code the lakehouse imports, score, apply the policy, write the decision with its reason codes to the decision log before responding, and return the decision id. GET /v1/decisions/{id}, GET /health (version, models and digests, the statement), and a client budget of 60 requests an hour per hashed client address.

**Parity.** The export job writes 1,000 replayed test events per desk with the offline features, scores and decisions, and the online state snapshot at the start of the replay. scripts/check_parity.py replays them through the live API from a clean online state seeded with that snapshot, in order, and asserts every decision equal and every score within 1e-9. It runs in CI against a local container and before the tag against the live URL; results/parity.json records both.

**Persistence tests.** test_decision_is_committed from an independent connection; test_audit_precedes_response; test_state_and_decision_share_a_transaction; the CI out of process check that starts the container, scores a payment, and reads the decision and the updated state back from a separate process.

**Latency.** A load test through the live API with the cold start measured separately, p50 and p99 per endpoint.

## 13. Monitoring and the incident

**The monitor task**, after every scoring run: PSI per feature and on the score against the training window; expectation failure rates per rule; label maturity (the share of the labels of a cohort known by days 30, 60 and 90) and capture and friction per cohort as they mature; volume and freshness. Each check has a threshold in config/monitor.yaml and writes ops.alerts rows; a breach fails the task, and the workspace workflow opens a GitHub issue for it with the alert rows, the run id and the runbook link.

**The incident on replay day 40.** Expected: the duplicate drop deduplicated by natural key with no double count (a test asserts the gold totals for the day); the renamed column landing in rescued data, its rows quarantined by the contract rule, and the completeness objective breached; the six hour shift caught by the ordering check and the freshness check. The monitor fires, the issue opens, and docs/runbooks/ingestion.md is followed: identify from the event log, quarantine counts and the run ledger; contain by pausing the daily job; fix the contract mapping through an approved change; replay the day with the backfill job; verify digests. docs/incidents/<date>-day-40.md is the postmortem: timeline from the event log and run ledger with timestamps, impact in rows and dollars of decisions delayed, root cause, what detected it and how fast, what did not, and three actions. Blameless and specific.

## 14. The repository surface for agents

AGENTS.md as the single source (setup, tests, gates, the files an agent must not edit: the lock files, the plan, the published RBA results, the results promoted from the workspace except through promotion); CLAUDE.md and .github/copilot-instructions.md as one paragraph each pointing at it; three skills in .claude/skills validated with skills-ref: backfill-a-range (how to write a backfill request, what the run ledger must show, how to verify), add-a-feature (the as of rule, the leakage test, the contract change, the reconcile), promote-a-model (the gates, the request, the alias, the card). docs/SPEC.md holds sections 1 to 21 of this prompt as the specification, with a line at the top saying deviations are in DECISIONS.md; the opening paragraph of the prompt is not part of it. docs/adr/ holds one architecture decision record per choice named in section 4, each with the alternatives and why they lost. The playbook page renders AGENTS.md and the skills.

## 15. The site and the visual system

Static export on GitHub Pages, DuckDB-WASM over the committed results, the live API for the replay, and a recorded session so every page works when the API is asleep, labeled as recorded, with the site probing twice.

**Art direction.** A decision slip. Light first: cool thermal paper, black ink, hairline perforations between sections, every decision rendered as a slip with the verdict, the amount, the score and the reason codes; dark is card stock at the switch at night. Dense tables only on the platform and models pages. Nothing decorative.

**Surfaces and ink**, both validated:

```
Light surface   #F4F5F7    dark surface    #12161C    card   #FFFFFF light, #1A2028 dark
Primary ink     #14171C    primary ink     #E8EBF0    rules  #D7DAE0 light, #2B323C dark
Secondary ink   #575E69    secondary ink   #A1A9B5
Control gray    #6B7280    control gray    #8A93A0
```

**Categorical palette, fixed order.** Light: #1F5FAD, #C0306B, #A15C00, #0B7A75, #6B3FC4, #667A12, #075780, #8C5A2E. Dark: #6AA6F0, #F0679E, #E7A33C, #36C2B4, #A88BF5, #AFC04A, #4CB3E6, #D2A271. Measured on 28 October with the series validator (scripts/validate_palette.js from readback, config mode, both modes): zero failures; worst adjacent colour vision deficiency separation 10.0 light and 12.2 dark against a target of 8; worst normal vision separation 16.7 and 20.6 against a floor of 12; every light slot at or above 4.42:1 on the surface and 4.82:1 on white; every dark slot at or above 6.17:1 on the surface and 5.57:1 on the dark card; the first three slots pass the all pairs check (10.0 light, 12.8 dark). Slot one is the good customer (legitimate events, approvals); slot two is the fraudster (fraud, takeovers, attacker models); slot three is friction (step ups, declines of good customers, the intervention). Slots four to eight are the models in fixed order: logistic, LightGBM, Freeman RBA, the sequence challenger if built, a spare; the floor is the control gray with a hatch. Direct labels always. Port the validator and run it in CI against exactly these values; if it rejects a slot after porting, move it by the smallest change that passes and record it in DECISIONS.md.

**Sequential, blue, for magnitude.** Light, light to dark: #A9C8EE, #7AA8E2, #4D86CF, #2D66B0, #184680. Dark, dark to light: #17385F, #1F5390, #3473C0, #69A0E4, #A9CBF3. Validated as ordinal ramps. For drift strips, cohort heatmaps and the compute ledger.

**Diverging, magenta to blue through a neutral midpoint**, #E6E8EC light and #2A3038 dark. Light: #7E1543, #B8336B, #DB7AA0, #F0B6CD, midpoint, #A3C0EA, #7FA8E0, #3F76C2, #1C4A8A. Dark: #F6B3CF, #E879A6, #C9497F, #8E2654, midpoint, #1E4A80, #2F6AB3, #5E95DC, #A6C7F2. Validated. For model against floor differences and for the RBA comparison.

**Status, reserved, always with an icon and a word.** Good #1E7F3F light and #3FB950 dark; warning #8A6D00 and #D9B530; serious #C2410C and #F0883E; critical #B42318 and #F85149. Measured at or above 4.5:1 on both light surfaces and 4.8:1 on both dark ones. Approve, step up and decline are verdicts, not statuses: they wear slots one, three and two with their word.

**Type**, self hosted at build time through next/font from the Fontsource packages:

```
A  Slip (default)   Schibsted Grotesk (display at the heavy weights, interface and
                    body), Chivo Mono (figures, identifiers, SQL, every dense
                    table), Doto (the numerals on a decision slip, nowhere else)
B  Desk             Hanken Grotesk, Sometype Mono, Doto
C  Ledger room      Onest, Reddit Mono, Doto
```

All seven faces were confirmed as Fontsource packages on npm on 28 October. Body 16px, line height 1.5, measure capped at 70 characters, the briefing at 68; every figure right aligned with tabular numerals. If a face is missing at build time, use the next option and record it.

**Chart rules.** The series mark specification unchanged, a table toggle and a show the SQL toggle beside every chart, texture in addition to hue, one message per chart rendered from the manifest as its title. The frontier: friction per 1,000 good events on the x axis, capture by dollars on the y axis, one line per model with its interval band, the floor hatched in control gray, the registered budget as a vertical rule and the chosen operating point as a ring with its numbers beside it. The lineage graph: tables as nodes in medallion columns, edges labeled with rows. The SLO chart: one row per objective, one mark per drop, the incident day marked. The label maturity chart: share known by days since the event, one line per lag setting.

**Pages.**

- / **The frontier.** Both desks' frontiers, the chosen operating points with their test numbers and intervals, the floor, the statement.
- /payments **The payments desk.** The model comparison, the cost view and its sensitivity, label maturity, reason codes by frequency, the hypotheses.
- /logins **The login desk.** The Freeman reimplementation against LightGBM, capture per attacker model, takeovers counted, the published comparison.
- /platform **The lakehouse.** The medallion and lineage with row counts, expectations and quarantine by rule, the SCD2 history of a card and the two clocks of a label, the reconcile, the idempotency digests, the performance experiments, the compute ledger.
- /governance **Unity Catalog.** Classification by column, a masked and unmasked read side by side, grants, the scan result, the Terraform plan of the last push.
- /operations **Running it.** SLOs, drift, alerts, the incident and its postmortem, the runbooks.
- /replay **Live.** Pick a card or a user from the replay set, send its events to the live API in order, and watch the slips print; the parity result beside them.
- /models **The model cards and validation reports.**
- /report **The briefing**, with a print stylesheet.

Skeleton loading states in the shape of the chart, prefers-reduced-motion respected, everything keyboard reachable, contrast checked in both themes, the statement in the footer of every route, mobile first at 390 pixels with the slips full width.

## 16. What makes it read as built by a person

Everything in northstar section 18 applies unchanged. Four additions:

- **What the platform engineer would push back on.** Every page and every section of the briefing ends with the objection and what the build did about it: the data is synthetic and the payments labels carry a simulated delay; a file landing zone with AvailableNow is not a message bus; Free Edition hides the cloud account, so no bucket, role or key owned by the build is shown; one workspace with two catalogs is not two workspaces; takeover labels are rare and their intervals are wide; the RBA comparison is against figures read from a repository.
- **What the fraud lead would push back on.** The cost parameters are assumptions; the attacker models come from the paper and are not attackers; the attack IP flag is close to a label; a 36 month window of synthetic behavior is not a real portfolio.
- **The decisions name the platform.** Every DECISIONS.md entry that chose a workspace path (catalog or schema prefixes, groups or the entitlements table, bitemporal AUTO CDC or the explicit columns, the billing table or the run ledger) cites the documentation page and its date.
- **The dead ends are kept.** The notebooks keep the random split that scored 0.99 and what it did on the time split, the feature that read one label from the future and how the as of test caught it, and the first idempotency test that failed because a task appended.

## 17. Repository layout and build order

```
friction/
+-- README.md  RESULTS.md  ARCHITECTURE.md  DECISIONS.md  CONTRIBUTING.md  LICENSE
+-- AGENTS.md  CLAUDE.md  databricks.yml  vercel.json  render.yaml  Dockerfile.vercel
+-- Dockerfile.render  Makefile  pyproject.toml  uv.lock  .env.example
+-- .github/  workflows/  ci.yml  workspace.yml  pages.yml  release.yml
|             copilot-instructions.md
+-- .claude/skills/  backfill-a-range/  add-a-feature/  promote-a-model/
+-- config/  simulator.yaml  incident.yaml  costs.yaml  monitor.yaml  replay.yaml
+-- contracts/  one yaml per table
+-- data/  sources.lock.json  PROVENANCE.md  fixtures/
+-- requests/  committed workspace requests
+-- infra/terraform/
+-- pipelines/  portable/  lakeflow/
+-- packages/
|   +-- core/  transforms/  features/  simulator/  models/  evaluation/
|   +-- policy/  reasons/  api/  render/  workspace/
+-- evaluation/  plan.yaml  hypotheses.yaml  rba_published.yaml  reason_codes.yaml
+-- results/  manifest.json  provenance.json  workspace_probe.json  reconcile.json
|             performance.json  parity.json  live_check.json  rederive.json
|             models/  scored/ (or a data release, see below)
+-- notebooks/  01_the_random_split  02_the_leak  03_the_rerun
+-- report/  briefing.md  templates/
+-- scripts/  check_no_em_dash.py  check_vocabulary.py  check_published_numbers.py
|             check_statement.py  check_tokens.py  scan_for_planted_identifiers.py
|             validate_palette.js  fetch_sources.py  replay.py  profile_sources.py
|             probe_hosts.py  check_parity.py  load_test.py  check_persistence.py
|             reset_and_rederive.py  build_demo_gif.py
+-- deploy/  probe.ps1  verify.ps1  fetch-tabformer.ps1  push-main.ps1  push-tag.ps1
+-- docs/  SPEC.md  adr/  runbooks/  incidents/  lakehouse.md  governance.md
|          evaluation.md  serving.md
+-- web/
+-- tests/
```

Scored test sets above 50 MB go to a data release (data-v1) as assets with their digests in data/results.lock.json; CI downloads and verifies them before grading.

Build order. Steps 1 to 18 are mandatory and are the product.

```
 1  Scaffold, ported tooling, the em dash, vocabulary, statement and token
    gates, licences; the probe (Vercel, Neon, and a workspace probe run from
    Actions that records what Free Edition allows: catalogs, volumes,
    pipelines, jobs, AUTO CDC modes, masks, groups, system tables, serving)
    chore: scaffold workspace with ported tooling and the probe
 2  Claim gate renderer before any results exist
    ci: published number gate
 3  Sources: fetch with digests in Actions, the first look at full size on the
    build machine, the first ten defects as quarantine rules, contracts, the
    replay splitter, the simulator, the incident config
    feat(data): two sources, contracts, the replay and the simulator
 4  Transforms and the portable pipeline: bronze, silver, the SCD reference,
    gold features; local tests through Spark Connect; the full local run
    feat(pipeline): portable transforms on open source Spark
 5  Terraform with the pg backend, the bundle with dev and prod targets, the
    workspace workflow with requests and the results branch
    feat(infra): terraform, the bundle and requested runs
 6  The Lakeflow pipeline with expectations, AUTO CDC and Auto Loader, deployed
    to dev, run on one landed month per desk; the reconcile against the local
    run; the dev workspace in the README
    feat(lakehouse): lakeflow pipeline deployed and reconciled
 7  Jobs: daily, backfill, governance, integration; the run ledger; the
    idempotency, order and late label tests in the workspace
    feat(jobs): idempotent jobs and their tests
 8  The full backfill of both sources by requests within the quota, nothing
    sampled; the reconcile at full size
    feat(lakehouse): full backfill
 9  Governance: tags, masks, the row filter, grants, the scan, lineage; the
    planted drift caught by Terraform plan
    feat(governance): classification, masks and the scan
10  Features, training sets as of a cut, the floors and the models, MLflow and
    Unity Catalog registration; the plan registered and hashed before any test
    month is scored
    feat(models): two desks and the registered plan
11  Evaluation in the repository: the frontier, budgets, cluster bootstrap,
    Benjamini-Hochberg, interior tests, calibration, hypotheses, the RBA
    comparison; reason codes; cards and validation reports; promotion gates
    and the champion aliases
    feat(evaluation): the friction frontier and the gates
12  Serving: the export, the API in a Vercel container with the online store,
    the decision log, the client budget; deployed and verified from a
    separate client; parity against the live URL
    feat(api): scoring with an online store and parity
13  Persistence tests: independent connection, audit before response, the
    shared transaction, the out of process check
    test(api): decisions observed independently
14  Monitoring and the incident: drift, maturity, alerts, the GitHub issue,
    the runbook, the replay of day 40, the postmortem
    feat(ops): monitoring and the day 40 incident
15  Performance experiments and the compute ledger
    perf(lakehouse): skew, file sizing, clustering and the compute ledger
16  Web: frontier, payments, logins, platform, governance, operations, replay,
    models, report; the test server; Pages deploy; the repository surface for
    agents (AGENTS.md, skills, SPEC, ADRs)
    feat(web): the slip and its pages
17  Full run, RESULTS.md with the section 20 checklist, README with the matrix,
    DECISIONS.md, notebooks 01 to 03; load test with cold start; the live
    check in a real browser with the API awake and asleep and from a separate
    client; demo recording; reset and rederive in a worktree (the local run
    from the lock files, the evaluation from the scored sets, and one landed
    month rerun in the workspace with its digests compared)
    chore: full run, live check and canonical rederive
18  Tag v0.1.0 pushed after the commits
    chore: release
19  A sequence challenger: a small recurrent model over the last 20 events
    of each card, trained on a free Colab or Kaggle GPU from exported features,
    graded on the same test months, promoted only through the same gates
    feat(models): sequence challenger
20  A workspace serving endpoint for the payments champion, if the probe
    found one available, with latency against the Vercel API
    feat(serving): workspace endpoint comparison
21  An AI/BI dashboard and a Genie space over the gold tables, with
    screenshots committed and the definitions exported
    feat(bi): dashboard and genie space
22  friction as a fourth warehouse for readback, through its lock file and
    catalog alone
    feat(data): readback integration
23  Exports a BI team would open (Power BI and Tableau extracts of the gold
    metric tables with their measures)
    feat(bi): exports
24  Dockerfile hardening, release workflow hardening, docs
    ci: containers and documentation
```

If time runs short, cut from 24 backward and never below 18. Inside the first eighteen there is nothing to cut; if one cannot be finished, the README first paragraph names it and the build is not called complete.

## 18. The skills matrix

In the README, one row per skill a Databricks data engineer, fraud data scientist or bank machine learning engineer posting lists, and the file that demonstrates it:

```
Skill                                     Where it lives
Lakehouse design (medallion, contracts)   pipelines/, contracts/, docs/lakehouse.md
Delta and Unity Catalog                   pipelines/lakeflow, infra/terraform,
                                          docs/governance.md
Incremental and streaming ingestion       Auto Loader tables, the replay, the
                                          idempotency tests
CDC and slowly changing dimensions        AUTO CDC flows, packages/transforms/scd.py,
                                          the reconcile
Late arriving data and backfills          the lateness rule, the backfill job,
                                          the order tests
Orchestration and reliability             databricks.yml jobs, ops.run_ledger,
                                          ops.slo, docs/runbooks
Incident response and RCA                 docs/incidents/, the monitor, the issue
Performance tuning                        results/performance.json, notebook 03
Cost management                           the compute ledger, the quota plan
Infrastructure as code                    infra/terraform (pg state), databricks.yml
CI/CD and promotion                       .github/workflows, requests/, the gates
Data governance and privacy               tags, masks, the row filter, the scan
Data quality                              expectations, quarantine, contracts
Testing for data                          tests/, the reconcile, two engines
Fraud feature engineering                 packages/features, the as of test
Model evaluation for fraud                packages/evaluation, the frontier
Decision policy and cost                  packages/policy, config/costs.yaml
Explainability                            packages/reasons, reason_codes.yaml
Model risk documentation                  model cards, validation reports
Identity and account takeover risk        the login desk, the Freeman model
Production serving and online features    packages/api, the online store, parity
Monitoring with delayed labels            the monitor task, maturity curves
Statistics for evaluation                 cluster bootstrap, Benjamini-Hochberg,
                                          calibration, interior tests
Spark and PySpark                         packages/transforms, notebooks
Agent assisted engineering                AGENTS.md, .claude/skills, docs/SPEC.md
Communication                             report/briefing.md, the pushback
                                          paragraphs
```

## 19. Testing

- The three tests for every gate: em dash, vocabulary, statement, token scan, claim gate, palette validator (both surfaces and both cards), the planted identifier scan, every quarantine rule, every promotion gate, every monitor threshold, every interior test.
- **Transforms**: every function on fixtures with chispa equality; the dedupe keeps the right version under a planted duplicate; the watermark drops nothing inside its window and states what it drops outside it.
- **Point in time**: the 500 vector recomputation; a planted future event changes no feature; a planted label known after the cut is absent from the training set.
- **CDC**: the reference SCD2 on a planted out of order and duplicate feed; AUTO CDC against the reference on every key in the workspace integration job; the valid and known intervals of a label on a planted late chargeback.
- **Idempotency and order**: the rerun, the shuffled delivery, the backfill over a processed range, the late labels out of order, each asserting identical digests; a deliberately appending task makes the test fail (the deliberate violation).
- **Two engines**: the reconcile at four grains on the fixture slice in CI and at full size before the tag.
- **Contracts**: every table has one; a planted column rename fails the contract check; an approved change passes.
- **Workspace**: the bundle validates; the job count of concurrent tasks stays at or under five; the limits the probe recorded are read by the deploy step.
- **Terraform**: fmt and validate; the planted drift appears in the plan.
- **Governance**: a masked read returns masked values for a reader identity; the row filter limits rows; the scan catches a planted Luhn valid number, a CVV shaped field and an address, and passes the clean tree.
- **Evaluation**: metrics on fixtures with known answers; the frontier is monotone in threshold; the bootstrap interval contains the point estimate; the bootstrap is reproducible under two PYTHONHASHSEED values; Benjamini-Hochberg on fixtures; the operating point rule picks the right threshold on a planted validation set; the plan hash in the manifest equals the hash of the committed plan.
- **Policy**: a chip transaction is never stepped up; an online one can be; the login policy blocks above its block threshold and steps up between.
- **Reason codes**: coverage, stability, the codes of the floor.
- **API**: parity on the replay set; the client budget refuses past the cap; health reports the model digests; the decision log row before the response; independent connection and out of process checks.
- **Monitoring**: each check fires on a planted breach and not on the clean run; the incident day produces the expected quarantine counts, alert rows and gold totals.
- **Did the step run**: the checkpoint advanced once per drop; the run ledger has one row per task per run; the expectation counts equal the quarantine rows; the parity check compared every event; the scan read every file it lists.
- **Determinism**: every seeded step sorts before it samples; the replay splitter and simulator outputs match their digests; identical manifests under two PYTHONHASHSEED values.
- The test server refuses HEAD and answers a first 503, and the site client handles both; Playwright on every route (the frontier draws every series in its legend, a slip prints on the replay page, the masked and unmasked reads render, the statement on every route); notebooks in CI with nbmake.
- Property tests: capture and friction never decrease as the threshold falls; the policy never steps up a channel that cannot be challenged; the reconcile is symmetric; the online state after a replay equals the offline state at its end.
- Availability probes and named skip markers for Postgres, Spark, the browser and the load test, with CI asserting each dependency.

## 20. Definition of done, printed as a checklist

The build prints this list in its final message and in RESULTS.md, each line marked done or not done, with one sentence of evidence per line (a file, a test name, a URL, a number):

```
 1  Both sources fetched with digests in Actions, profiled at full size, the
    first ten defects as quarantine rules, contracts for every table,
    provenance and licences recorded
 2  The replay splitter, the lateness rule, the simulator (chargeback lag,
    card lifecycle CDC, RBA attacker models) and the incident config,
    deterministic with digests
 3  The portable pipeline on open source Spark 4.2 with local tests through
    Spark Connect and the full local run
 4  Terraform with state in Neon, the bundle with dev and prod targets, the
    workspace workflow with requests, the results branch and reviewed
    promotion; no token anywhere in the repository
 5  The Lakeflow pipeline with Auto Loader, expectations, quarantine and AUTO
    CDC deployed to dev and run; the reconcile against the local run
 6  Jobs with retries and the run ledger; idempotency, order and late label
    tests green in the workspace
 7  Every row of both sources backfilled, or the README first paragraph
    stating how far the backfill got and when it resumes
 8  Governance: tags, masks, the row filter, grants, lineage, the scan clean;
    the planted drift caught by the plan
 9  Point in time features with the leakage test; training sets as of a cut
    with the two clocks test
10  Floors and models on both desks registered in Unity Catalog; the plan
    hashed before any test month was scored
11  The frontier, operating points, intervals, Benjamini-Hochberg, interior
    tests, calibration, hypotheses and the RBA comparison, graded in the
    repository from the exported scored sets
12  Reason codes, model cards, validation reports, promotion gates and the
    champion aliases
13  The API live on Vercel with the online store and the decision log,
    verified from a separate client; parity exact on decisions against the
    live URL
14  Persistence: independent connection, audit before response, shared
    transaction, out of process check
15  Monitoring with delayed label cohorts; the day 40 incident detected,
    handled by the runbook and written up
16  Performance experiments and the compute ledger published
17  The site live on Pages with every route and the statement; the live check
    in a real browser awake and asleep and from a separate client; latency
    with the cold start; RESULTS.md with a specific limitations section;
    README with the matrix; DECISIONS.md with at least twelve dated entries
    and two reversals; three notebooks with a dead end each; AGENTS.md, the
    skills, SPEC and the ADRs
18  Coverage at or above 80 percent, mypy strict and ruff clean, zero em
    dashes, zero banned vocabulary, palette validator green, skills valid,
    forty to sixty commits, the rederive run before the tag with no drift,
    v0.1.0 pushed after the commits, Apache 2.0 for code with the terms of
    both data sets restated, repo described, topics set
```

Stretch items (the sequence challenger, the workspace endpoint, the dashboard and Genie space, the readback warehouse, the BI exports, the hardening) are listed beneath the checklist as stretch, done or not done, and never counted toward it.

## 21. Stretch, and what not to build

Stretch, only if the day allows: steps 19 to 24; a third desk only if it adds something ledger does not already show (ledger has the application fraud model on the Bank Account Fraud data); a Delta Sharing share of the gold metric tables if the probe finds sharing available.

What not to build:

- No random split, no resampling of validation or test, no accuracy as a metric.
- No threshold without its friction number, and no headline that is not a point on the frontier.
- No feature that reads an event at or after the one it describes, and no label read by valid time alone.
- No task that appends without a key or a stated range.
- No token, host credential, card number, CVV, address or raw IP in the repository, a log, a span, an export or the site.
- No workspace call from the build machine.
- No grading inside the workspace; the repository grades.
- No language model anywhere in a decision.
- No notebook as a pipeline; notebooks hold dead ends and explanations only.
- No claim that the data is real, that the system is in production, or that its numbers describe any real bank, card holder or service.
- No mandatory step replaced by a table entry.

Build it in the order in section 17. When you finish, print the file tree, the section 20 checklist with every line marked, the frontier numbers at the registered budget per desk with intervals and the floor beside them, the RBA comparison, the reconcile summary, the idempotency digests, the quarantine counts by rule, the incident timeline, the performance before and after, the compute ledger, the parity result, the latency with the cold start, the live URLs, and anything you could not measure or any dependency that was missing today.

