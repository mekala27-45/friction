# Model card: the floor, logins desk

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

**What it is.** Three rules: a first country, a first device, browser or operating system, a first network.

**What it reads.** Whether the user has signed in from the country, device type, browser, operating system and network before.

**Intended use.** Ranking successful sign ins for the
logins desk's policy at the friction budget registered in evaluation/plan.yaml. Not for any decision
about a real person: the data is synthetic.

**Performance on test** (open source run, thresholds chosen on validation):

| Measure | Value | Interval |
|---|---:|---:|
| Dollar capture | 58.5% | 58.3% to 58.7% |
| Capture by count | 58.5% | 58.3% to 58.7% |
| Step ups of good events | 57.67 per 1,000 | 57.29 per 1,000 to 58.00 per 1,000 |
| Declines of good events | 0.00 per 1,000 | 0.00 per 1,000 to 0.00 per 1,000 |
| Precision over all decisions | 21.3% | 21.1% to 21.6% |

Threshold free: ROC AUC 0.863, average precision 0.320.
Calibration error: not applicable. Score stability from validation to test (PSI): 0.000.
Reason code coverage of flagged events: 100.0%.

**Limitations.** Trained and graded on synthetic data; see RESULTS.md, Limitations.
