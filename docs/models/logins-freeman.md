# Model card: Freeman RBA, logins desk

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

**What it is.** The Freeman et al. (2016) statistical model as written in the paper, with its smoothing, and the interpolation weights of Wiefling et al. (2022).

**What it reads.** Address, network, country, browser string, browser, operating system and device, against the user's history and the global distribution.

**Intended use.** Ranking successful sign ins for the
logins desk's policy at the friction budget registered in evaluation/plan.yaml. Not for any decision
about a real person: the data is synthetic.

**Performance on test** (open source run, thresholds chosen on validation):

| Measure | Value | Interval |
|---|---:|---:|
| Dollar capture | 0.0% | 0.0% to 0.0% |
| Capture by count | 0.0% | 0.0% to 0.0% |
| Step ups of good events | 0.00 per 1,000 | 0.00 per 1,000 to 0.00 per 1,000 |
| Declines of good events | 0.00 per 1,000 | 0.00 per 1,000 to 0.00 per 1,000 |
| Precision over all decisions | not applicable | not applicable to not applicable |

Threshold free: ROC AUC 0.384, average precision 0.020.
Calibration error: not applicable. Score stability from validation to test (PSI): 0.021.
Reason code coverage of flagged events: 100.0%.

**In Unity Catalog.** workspace.friction_dev_ml.logins_freeman, version 1, alias
challenger; the registered file's digest begins 8fd11ff83138.

**Limitations.** Trained and graded on synthetic data; see RESULTS.md, Limitations.
