# Labels on two clocks

Status: accepted, 29 October 2026.

A label row carries when the event happened (valid time) and when the bank learned the label (system time). Training sets read labels as known at their cut.

## Alternatives

- **Latest label only.** What most fraud notebooks do. Lost because it leaks the future: a model trained on labels that arrived after its cut looks better than it could have been on the day.
- **Event time only with a fixed lag.** Simpler, but a chargeback reversed after investigation (friendly fraud) then has no history, and the as of test cannot be written.

## Consequences

The label history is SCD type 2 on the system clock (AUTO CDC in the workspace, the reference implementation locally); training.label_as_of reads the version in effect at an instant; tests/transforms/test_training.py plants a label known one day after the cut and finds it absent.
