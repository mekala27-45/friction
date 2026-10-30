# Model card: LightGBM, payments desk

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

**What it is.** Gradient boosted trees, deterministic single thread training, reason codes from TreeSHAP.

**What it reads.** The twenty four payment features of packages/features.

**Intended use.** Ranking transactions for the
payments desk's policy at the friction budget registered in evaluation/plan.yaml. Not for any decision
about a real person: the data is synthetic.

**Performance on test** (open source run, thresholds chosen on validation):

| Measure | Value | Interval |
|---|---:|---:|
| Dollar capture | 90.2% | 85.0% to 94.6% |
| Capture by count | 89.7% | 86.3% to 92.6% |
| Step ups of good events | 8.65 per 1,000 | 8.35 per 1,000 to 8.98 per 1,000 |
| Declines of good events | 1.18 per 1,000 | 0.98 per 1,000 to 1.40 per 1,000 |
| Precision over all decisions | 11.0% | 8.9% to 13.0% |

Threshold free: ROC AUC 1.000, average precision 0.801.
Calibration error: 0.000. Score stability from validation to test (PSI): 0.002.
Reason code coverage of flagged events: 100.0%.

**In Unity Catalog.** workspace.friction_dev_ml.payments_lightgbm, version 1, alias
challenger; the registered file's digest begins 6570b08a40d8.

**Limitations.** Trained and graded on synthetic data; see RESULTS.md, Limitations.
