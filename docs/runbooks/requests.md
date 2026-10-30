# Runbook: running something in the workspace

The build machine never calls the workspace. GitHub Actions holds DATABRICKS_HOST and
DATABRICKS_TOKEN and runs committed requests.

1. Write `requests/<date>-<nn>-<what>.json` with an `id` equal to the file name stem, a `kind`
   (probe, secrets, upload_sources, bundle_deploy, bundle_run, sql, export, terraform_plan,
   terraform_apply, portable, pipeline_refresh, model_alias, api_check) and its parameters.
   packages/workspace/src/friction_workspace/requests.py defines each kind and validates the file.
2. Push to main. The workspace workflow runs pending requests (those without `runs/<id>/done.json` on
   the results branch) in file name order and stops at the first failure.
3. Read the result on the results branch: `runs/<id>/done.json` and anything the request exported.
4. Promote what should enter main with `scripts/promote_results.py`, which writes
   `results/provenance.json`.
5. A failed request is retried under a new id that sorts where it should run; the failed one stays
   in the history.

## The two engine reconcile

A `portable` request copies the named drops from the outbox volume to the runner and runs the open
source pipeline and features over them with the workspace salt, then writes `summary.json`. The
build machine cannot hold the salt, so this is the only place where salted tables from both engines
can be compared by digest. `scripts/reconcile.py` compares that summary with the workspace export of
the same drops and exits non zero on any difference.

## The live API check

An `api_check` request runs on the runner, which holds FRICTION_REPLAY_KEY; the URL is the repository
variable FRICTION_API_URL. It waits seven minutes so the Vercel container scales down, runs
`scripts/live_check.py` against the published site (asleep), runs it again at once (awake, recorded
as the demo), replays the parity sets through the live URL, waits again, and runs the load test,
whose first request is the cold start. Promote `live_check.json`, `parity.json` (its live entry) and
`latency.json`. Rebuild Pages after setting FRICTION_API_URL, because the site reads the URL at build
time.

## The rederive

Three parts, before the tag. `scripts/reset_and_rederive.py` runs the replay from the lock and the
fixture pipeline in a clean worktree on the build machine. A `portable` request with `models: true`
over every drop reruns the model chain on the runner; its `evaluation.json` and served
`models/payments/model.txt` are passed back with `--evaluation` and `--model`. The integration job's
`month` test reruns the first month in a clean workspace environment; its `integration.json` and the
open source summary of the same month go in with `--month-workspace` and `--month-open-source`.
`results/rederive.json` records each part and says whether all of them ran.
