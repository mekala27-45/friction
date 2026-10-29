# Validation report: LightGBM, logins desk

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

Sections in the order of SR 11-7 guidance on model risk management.

## Purpose

Rank successful sign ins so the desk's policy can
step up or decline the riskiest within a friction budget registered before the test months were scored
(plan 2ba61fcd4da0).

## Data

RBA sign ins, training months 2020-02-03 to 2020-09-21,
with synthetic attackers for training users; test months 2020-12-15 to
2021-03-01 with synthetic attackers for held out users. The unknown account bucket is
excluded.
## Design

Gradient boosted trees on the user and address features; TreeSHAP reason codes.

## Performance on validation and test

Validation dollar capture at the chosen thresholds: 58.3%. Test dollar
capture: 25.9% (25.6% to 26.1%).

| Model | Against | Difference | Low | High | p | Adjusted p | Rejected |
|---|---|---:|---:|---:|---:|---:|---|
| Freeman RBA | the floor | -58.5 pts | -58.7 pts | -58.3 pts | 0.002 | 0.002 | yes |
| LightGBM | the floor | -32.6 pts | -33.0 pts | -32.3 pts | 0.002 | 0.002 | yes |
| LightGBM | Freeman RBA | +25.9 pts | +25.6 pts | +26.1 pts | 0.002 | 0.002 | yes |

## Stability

PSI of the score from validation to test: 0.008.

## Sensitivity

The interior test on the budget: stable.

| Budget | Freeman RBA | LightGBM |
|---|---:|---:|
| double | 2.5% | 35.7% |
| half | 0.0% | 25.9% |
| registered | 0.0% | 25.9% |

## Limitations

Synthetic data; simulated labels; for the login desk, synthetic attackers built from the paper's
definitions. The expected cost view rests on assumptions in config/costs.yaml.

## Monitoring plan

The monitor task after every scoring run: feature and score PSI against the training months, expectation
rates, completeness, the clock, and label maturity at the checkpoints in config/monitor.yaml.

## Effective challenges on record

The promotion gates as the repository decided them:

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

The challenges recorded in DECISIONS.md: class weights reversed to sampling weights to keep the
probabilities calibrated; the unknown account bucket removed from grading; the Freeman score is not a
probability and cannot pass the calibration gate.
