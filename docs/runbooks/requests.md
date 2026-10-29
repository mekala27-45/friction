# Runbook: running something in the workspace

The build machine never calls the workspace. GitHub Actions holds DATABRICKS_HOST and
DATABRICKS_TOKEN and runs committed requests.

1. Write `requests/<date>-<nn>-<what>.json` with an `id` equal to the file name stem, a `kind`
   (probe, secrets, upload_sources, bundle_deploy, bundle_run, sql, export, terraform_plan,
   terraform_apply) and its parameters. packages/workspace/src/friction_workspace/requests.py defines
   each kind and validates the file.
2. Push to main. The workspace workflow runs pending requests (those without `runs/<id>/done.json` on
   the results branch) in file name order and stops at the first failure.
3. Read the result on the results branch: `runs/<id>/done.json` and anything the request exported.
4. Promote what should enter main with `scripts/promote_results.py`, which writes
   `results/provenance.json`.
5. A failed request is retried under a new id that sorts where it should run; the failed one stays
   in the history.
