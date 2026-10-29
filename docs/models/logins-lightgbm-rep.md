# Model card: LightGBM with the attack IP flag, logins desk

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

**What it is.** The same model with the attack IP flag as an external reputation feature; printed beside the one without because the flag is close to a label.

**What it reads.** The user and address features and the attack IP flag.

**Intended use.** Ranking successful sign ins for the
logins desk's policy at the friction budget registered in evaluation/plan.yaml. Not for any decision
about a real person: the data is synthetic.

**Performance on test** (open source run, thresholds chosen on validation):

| Measure | Value | Interval |
|---|---:|---:|
| Dollar capture | 27.5% | 27.3% to 27.8% |
| Capture by count | 27.5% | 27.3% to 27.8% |
| Step ups of good events | 60.37 per 1,000 | 59.97 per 1,000 to 60.77 per 1,000 |
| Declines of good events | 0.74 per 1,000 | 0.71 per 1,000 to 0.78 per 1,000 |
| Precision over all decisions | 10.8% | 10.6% to 10.9% |

Threshold free: ROC AUC 0.613, average precision 0.095.
Calibration error: 0.026. Score stability from validation to test (PSI): 0.006.
Reason code coverage of flagged events: 100.0%.

**Limitations.** Trained and graded on synthetic data; see RESULTS.md, Limitations.
