# Model card: logistic regression, payments desk

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

**What it is.** Logistic regression on standardized features with sampling weights.

**What it reads.** The twenty four payment features of packages/features.

**Intended use.** Ranking transactions for the
payments desk's policy at the friction budget registered in evaluation/plan.yaml. Not for any decision
about a real person: the data is synthetic.

**Performance on test** (open source run, thresholds chosen on validation):

| Measure | Value | Interval |
|---|---:|---:|
| Dollar capture | 81.2% | 76.5% to 85.0% |
| Capture by count | 71.9% | 67.6% to 75.8% |
| Step ups of good events | 9.45 per 1,000 | 9.06 per 1,000 to 9.85 per 1,000 |
| Declines of good events | 1.21 per 1,000 | 1.04 per 1,000 to 1.38 per 1,000 |
| Precision over all decisions | 8.4% | 6.7% to 10.1% |

Threshold free: ROC AUC 0.999, average precision 0.557.
Calibration error: 0.000. Score stability from validation to test (PSI): 0.000.
Reason code coverage of flagged events: 100.0%.

**In Unity Catalog.** workspace.friction_dev_ml.payments_logistic, version 1, alias
challenger; the registered file's digest begins bf156afcb314.

**Limitations.** Trained and graded on synthetic data; see RESULTS.md, Limitations.
