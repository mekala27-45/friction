# Model card: LightGBM, logins desk

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

**What it is.** Gradient boosted trees on the login features, without the attack IP flag.

**What it reads.** The user and address features of packages/features.

**Intended use.** Ranking successful sign ins for the
logins desk's policy at the friction budget registered in evaluation/plan.yaml. Not for any decision
about a real person: the data is synthetic.

**Performance on test** (open source run, thresholds chosen on validation):

| Measure | Value | Interval |
|---|---:|---:|
| Dollar capture | 25.9% | 25.6% to 26.1% |
| Capture by count | 25.9% | 25.6% to 26.1% |
| Step ups of good events | 41.30 per 1,000 | 40.97 per 1,000 to 41.63 per 1,000 |
| Declines of good events | 0.82 per 1,000 | 0.78 per 1,000 to 0.86 per 1,000 |
| Precision over all decisions | 14.1% | 13.9% to 14.3% |

Threshold free: ROC AUC 0.636, average precision 0.096.
Calibration error: 0.026. Score stability from validation to test (PSI): 0.008.
Reason code coverage of flagged events: 100.0%.

**Limitations.** Trained and graded on synthetic data; see RESULTS.md, Limitations.
