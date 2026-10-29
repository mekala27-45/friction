# Two engines run the same transforms

Status: accepted, 29 October 2026.

packages/transforms runs in a Lakeflow pipeline in the workspace and in open source Spark Declarative Pipelines on the build machine and in CI. The two are reconciled at the desk, month, day and user bucket grains.

## Alternatives

- **Workspace only.** Less code. Lost because every change would spend Free Edition quota to find out whether it runs, and CI could not test a transform without a token.
- **pandas or DuckDB for local tests.** Fast. Lost because the semantics differ from Spark's in the places that matter here (null handling in joins, window frames, timestamp parsing), so a passing local test would prove little.

## Consequences

Open source SDP lacks AUTO CDC history and expectations, so the portable pipeline builds history with a reference implementation (packages/transforms/scd.py) and quarantines with explicit rules; the reconcile is what shows the two agree. Tokens differ between engines because the build machine never holds the production salt.
