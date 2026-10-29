# The lakehouse

**Landing.** The replay splitter (scripts/replay.py) writes each desk's rows as drops: monthly for
the history, then one per day for the final 92 days. Each drop is a folder of gzip CSV files. The
land job copies drops from the outbox volume to the landing volume in order and once each.

**Bronze.** Auto Loader reads every file as text with a header lookup per file, keeps unexpected
columns in rescued data, and records the drop, the file and its modification time. Nothing is typed or
dropped here.

**Silver.** Typed, repaired where a contract rule says so (an amount written as text, a dash for a
missing region), conformed to tokens for restricted values, deduplicated by natural key in arrival
order. A row that breaks a rule goes to the quarantine with every rule it broke; the Lakeflow
pipeline counts each rule as an expectation. contracts/quarantine_rules.yaml lists the rules and the
defect each came from.

**CDC.** Card lifecycle changes arrive as a feed with deliberate faults (out of order, duplicated).
AUTO CDC keeps card history as SCD type 2 and label history on the system clock; the portable
pipeline builds the same history with packages/transforms/scd.py.

**Gold.** Feature inputs as of each event (the card generation in effect, the merchant category rate
from labels known at the start of the month), then the state machines in packages/features compute
every feature from strictly earlier events. Training sets are cut on a date and read labels as known
on it.

**Contracts.** One YAML file per table under contracts/; scripts/check_contracts.py compares each with
the pipeline's schema and fails on a difference that DECISIONS.md has not approved.
