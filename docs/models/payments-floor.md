# Model card: the floor, payments desk

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

**What it is.** Five rules an analyst writes on the first day, each adding points; the score is their sum (packages/models/src/friction_models/floors.py).

**What it reads.** Velocity in the last hour, amount against the card's median, a first merchant online at night, a recent reissue, a first in person location.

**Intended use.** Ranking transactions for the
payments desk's policy at the friction budget registered in evaluation/plan.yaml. Not for any decision
about a real person: the data is synthetic.

**Performance on test** (open source run, thresholds chosen on validation):

| Measure | Value | Interval |
|---|---:|---:|
| Dollar capture | 1.8% | 0.0% to 4.4% |
| Capture by count | 0.3% | 0.0% to 0.7% |
| Step ups of good events | 9.75 per 1,000 | 9.26 per 1,000 to 10.23 per 1,000 |
| Declines of good events | 0.98 per 1,000 | 0.85 per 1,000 to 1.11 per 1,000 |
| Precision over all decisions | 0.0% | 0.0% to 0.1% |

Threshold free: ROC AUC 0.597, average precision 0.003.
Calibration error: not applicable. Score stability from validation to test (PSI): 0.000.
Reason code coverage of flagged events: 100.0%.

**In Unity Catalog.** workspace.friction_dev_ml.payments_floor, version 1, alias
floor; the registered file's digest begins 7a645f3b33da.

**Limitations.** Trained and graded on synthetic data; see RESULTS.md, Limitations.
