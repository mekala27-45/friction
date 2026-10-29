---
name: add-a-feature
description: Add a feature to one desk with its as of test, its contract change and the reconcile. Use when a payments or login feature is added or changed in packages/features.
---

# Add a feature

1. Write it in the state machine (`packages/features/src/friction_features/payments.py` or
   `logins.py`): read the state in `features()`, change it only in `update()`. A feature that needs
   anything from the event being described, other than its own fields, reads the future.
2. Add the name to `FEATURES` in the same position the vector returns it, and a code mapping in
   `evaluation/reason_codes.yaml`.
3. Tests, all three:
   - a naive recomputation in `tests/features/test_state_machines.py`;
   - the 500 vector recomputation and the planted future event in `tests/features/test_point_in_time.py`
     still pass;
   - snapshot and restore give the same next vector.
4. Contracts: the feature tables' schemas change; update `contracts/` and run
   `scripts/check_contracts.py` against the fixture run.
5. Run the fixture pipeline and features, then the reconcile of feature sums between engines.
6. A new feature changes every model: retrain, rescore and regrade. The plan does not change.
