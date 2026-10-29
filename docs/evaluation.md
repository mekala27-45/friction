# Evaluation

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

The plan (evaluation/plan.yaml) was registered and hashed before any test month was scored; the
hypotheses (evaluation/hypotheses.yaml) were added to the lock before any validation month was
scored. scripts/check_registration.py refuses to grade when either differs from its lock.

**Operating point.** On validation: the lowest decline threshold whose good declines stay within the
decline budget, then the lowest step up threshold whose good step ups stay within the step up budget
(online purchases only on the payments desk). Both thresholds go to test unchanged.

**Metrics.** Defined once in packages/evaluation: capture by count and by dollars, friction per 1,000
good events, declines alone, false positive rate, precision over all decisions. ROC AUC and average
precision sit in the last columns, labeled threshold free.

**Intervals.** Cluster bootstrap over the desk's unit (card accounts, users), drawn as a multinomial in
two stages so a million users fit in memory, from per user sufficient statistics.

**Comparisons.** Every model against the floor and against every other model, dollar capture at the
operating point, Benjamini-Hochberg across the family per desk.

**Interior tests.** The budget halved and doubled, the bootstrap seed, the training window and the
label lag; the word degenerate prints when the champion changes at the first neighbour.

**Calibration.** Expected calibration error and a reliability curve per probabilistic model on test.
The floor and the Freeman score are not probabilities, so they cannot pass the calibration gate.
