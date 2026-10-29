# friction: the briefing

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

## Where it stands

3 of the eighteen mandatory lines are done. Not done: 1, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 15, 16, 17, 18. The
Databricks workspace has run its probe; the deploy, the backfill, governance and the Unity Catalog
registration wait on three GitHub Actions secrets. The graded numbers below come from the
local run of the same transforms on open source Spark.


## What a platform engineer would push back on

The data is synthetic and the payment labels carry a simulated delay. A file landing zone is not a
message bus. Free Edition hides the cloud account. One workspace with prefixed schemas is not two
workspaces. Takeover labels are rare. The RBA comparison is against figures read from a repository.

## What a fraud lead would push back on

The cost parameters are assumptions. The attacker models come from the paper and are not attackers. The
attack IP flag is close to a label. A synthetic window of card behavior is not a real portfolio.
