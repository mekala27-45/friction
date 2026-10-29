---
name: promote-a-model
description: Promote a model to champion through the gates, a committed request, the Unity Catalog alias and the model card. Use when a challenger has been graded and may replace a desk's champion.
---

# Promote a model

1. Grade: `scripts/grade.py` over the exported scored sets. Read `results/evaluation.json`, desk
   `gates` for the model.
2. Every gate must pass: dollar capture above the floor with the interval of the difference clear of
   zero; friction within the budget times 1.25; expected calibration error under 0.05; PSI between
   validation and test under 0.2; reason code coverage of every step up and decline; parity exact
   against the API (`results/parity.json`); p99 latency under the bound.
3. Commit `requests/<date>-<nn>-promote-<desk>-<model>.json` with `kind: sql` that sets the alias
   `champion` on the model version in Unity Catalog and moves the old champion to `challenger`.
4. Export the bundle (`scripts/export_models.py`), rerun parity locally and against the live URL.
5. Rebuild the manifest; the model card and the validation report render from it and refuse to render
   with a section missing.

A model that fails a gate stays a challenger and the site says which gate it failed.
