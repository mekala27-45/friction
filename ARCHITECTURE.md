# Architecture

See the diagram in docs/SPEC.md, section 4, and one decision record per choice in docs/adr.

**Files in a volume, not a message bus.** The build has no broker on a free tier, and most bank
lakehouses ingest batch extracts and CDC exports as files; the failures worth testing (a resent file,
a renamed column, a shifted clock) are file failures (docs/adr/0001).

**Actions is the only machine with the token.** A token on a laptop ends up in a history or a log. The
build machine commits a request; the workspace workflow runs it and writes the result to a branch that
reaches main only through a reviewed promotion (docs/adr/0002).

**The repository grades.** Grading next to the plan lock, in code CI runs, means a threshold rule cannot
change in a notebook. The workspace trains, scores and exports (docs/adr/0003).

**Two engines.** The same transforms run in Lakeflow and in open source Spark Declarative Pipelines, so
CI tests them without a token and the reconcile shows they agree (docs/adr/0004).

**Bitemporal labels.** A label has the day the event happened and the day the bank learned of it.
Training reads labels as known at its cut, so a model never learns from a chargeback that had not
arrived (docs/adr/0005).

**Postgres for the online store.** The decision and the state change it causes commit together. A key
value store would be faster per read and would need an outbox to keep that guarantee (docs/adr/0006).

**One workspace with prefixed schemas.** It separates names and grants, not compute or metastores, and
every page that shows the platform says so (docs/adr/0007).
