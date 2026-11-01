# Definitions

A policy, not a glossary: each definition below is a decision of the build, with the reason beside
it. The expressions are the ones the BI extracts and their measures run, over the decisions_daily
extract (results/bi/csv) or over the gold table it is cut from.

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

## Decision

One card transaction decided by the payments champion: approve, step up or decline. Every live event
since the day the thresholds went live is decided once, by the version the champion alias names in
Unity Catalog, at the thresholds the grade chose on validation (jobs/decisions.py).

    count(*)

## Step up and decline

A **step up** asks the customer for a one time code; only an online purchase can be stepped up,
since a chip or swipe purchase is decided at the terminal. A **decline** refuses the purchase. A
decline threshold is never below the step up threshold (friction_policy).

    sum(decisions) filter (where verdict = 'step_up')

## Fraud and good

An event is a **fraud** when its label as eventually reported says so: the version of the label with
no end in labels_payments_history. Every other event is a **good** one, including the events nobody
labelled, because an unlabelled purchase is one the bank never heard a complaint about.

    sum(decisions) - sum(frauds)

## Fraud dollars

The amount of every fraud, with a refund counted as zero dollars, so a reversed charge never offsets
a stolen one. The extract keeps cents, so every sum is exact; dollars are cents over one hundred.

    sum(fraud_cents) / 100

## Capture of fraud dollars

Fraud dollars on a step up or a decline over all fraud dollars: the headline of the payments desk,
because a desk that stops many small frauds and misses the large ones has not protected the bank.

    sum(fraud_cents) filter (where verdict <> 'approve') / sum(fraud_cents)

## Friction

Good customers asked to do something, per thousand good decisions: step ups and declines of goods,
each printed beside capture, because a fraud number without a friction number is half a sentence.

    1000 * sum(decisions - frauds) filter (where verdict = 'step_up') / sum(decisions - frauds)

## Reason code

Up to three codes on every step up and decline, the features that pushed the score toward it the
most, in plain words from evaluation/reason_codes.yaml; the graph features have their own table,
evaluation/reason_codes_v2.yaml.

## Refused row

A source row a contract rule refused, kept in the quarantine with the rule that refused it and
never passed on to silver. A row can be refused by more than one rule and is counted once per rule.

    sum(rows_refused)
