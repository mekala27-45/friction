# friction: the briefing

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

## Where it stands

10 of the eighteen mandatory lines are done. Not done: 5, 6, 10, 12, 13, 15, 17, 18. The
Databricks workspace has run its probe; the deploy, the backfill, governance and the Unity Catalog
registration wait on three GitHub Actions secrets. The graded numbers below come from the
open source run of the same transforms on open source Spark.

## The payments desk

At the registered budget of 10.0 per 1,000 step ups and
1.0 per 1,000 declines of good transactions, the champion,
LightGBM, captured the share of fraud dollars in the scorecard below; the floor of
first day rules is beside it. Every threshold was chosen on validation.

| Model | Capture | Low | High | Step ups | Declines | Precision | ECE | ROC AUC | AP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| the floor | 1.8% | 0.0% | 4.4% | 9.75 per 1,000 | 0.98 per 1,000 | 0.0% | not applicable | 0.597 | 0.003 |
| LightGBM | 90.2% | 85.0% | 94.6% | 8.65 per 1,000 | 1.18 per 1,000 | 11.0% | 0.000 | 1.000 | 0.801 |
| logistic regression | 81.2% | 76.5% | 85.0% | 9.45 per 1,000 | 1.21 per 1,000 | 8.4% | 0.000 | 0.999 | 0.557 |

## The login desk

At 100.0 per 1,000 re-authentications and 1.0 per 1,000 blocks
of legitimate sign ins, the champion is none. No real takeover falls in the test
months, so capture is of the attacker models built from the RBA papers.

| Model | Capture | Low | High | Step ups | Declines | Precision | ECE | ROC AUC | AP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| the floor | 58.5% | 58.3% | 58.7% | 57.67 per 1,000 | 0.00 per 1,000 | 21.3% | not applicable | 0.863 | 0.320 |
| Freeman RBA | 0.0% | 0.0% | 0.0% | 0.00 per 1,000 | 0.00 per 1,000 | not applicable | not applicable | 0.384 | 0.020 |
| LightGBM | 25.9% | 25.6% | 26.1% | 41.30 per 1,000 | 0.82 per 1,000 | 14.1% | 0.026 | 0.636 | 0.096 |
| LightGBM with the attack IP flag | 27.5% | 27.3% | 27.8% | 60.37 per 1,000 | 0.74 per 1,000 | 10.8% | 0.026 | 0.613 | 0.095 |

## What a platform engineer would push back on

The data is synthetic and the payment labels carry a simulated delay. A file landing zone is not a
message bus. Free Edition hides the cloud account. One workspace with prefixed schemas is not two
workspaces. Takeover labels are rare. The RBA comparison is against figures read from a repository.

## What a fraud lead would push back on

The cost parameters are assumptions. The attacker models come from the paper and are not attackers. The
attack IP flag is close to a label. A synthetic window of card behavior is not a real portfolio.
