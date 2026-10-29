# Validation report: LightGBM, payments desk

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

Sections in the order of SR 11-7 guidance on model risk management.

## Purpose

Rank card transactions so the desk's policy can
step up or decline the riskiest within a friction budget registered before the test months were scored
(plan 2ba61fcd4da0).

## Data

TabFormer transactions with simulated chargeback labels, training months
2016-11-01 to 2018-11-01, labels as known at the training
cut. Test months 2019-07-01 to 2019-11-01.

## Design

Gradient boosted trees, four hundred rounds, deterministic, sampling weights; TreeSHAP reason codes.

## Performance on validation and test

Validation dollar capture at the chosen thresholds: 92.7%. Test dollar
capture: 90.2% (85.0% to 94.6%).

| Model | Against | Difference | Low | High | p | Adjusted p | Rejected |
|---|---|---:|---:|---:|---:|---:|---|
| logistic regression | the floor | +79.4 pts | +73.7 pts | +84.1 pts | 0.002 | 0.002 | yes |
| LightGBM | the floor | +88.5 pts | +81.6 pts | +93.6 pts | 0.002 | 0.002 | yes |
| LightGBM | logistic regression | +9.0 pts | +3.5 pts | +14.5 pts | 0.002 | 0.002 | yes |

## Stability

PSI of the score from validation to test: 0.002.

## Sensitivity

The interior test on the budget: stable.

| Budget | LightGBM | logistic regression |
|---|---:|---:|
| double | 98.1% | 91.3% |
| half | 80.0% | 62.8% |
| registered | 90.2% | 81.2% |

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

The challenges recorded in DECISIONS.md: class weights reversed to sampling weights to keep the
probabilities calibrated; the unknown account bucket removed from grading; the Freeman score is not a
probability and cannot pass the calibration gate.
