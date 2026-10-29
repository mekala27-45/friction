# The repository grades, the workspace does not

Status: accepted, 29 October 2026.

The workspace trains and scores; it exports scored sets. scripts/grade.py in the repository applies the registered plan to them.

## Alternatives

- **Grade in a workspace job.** One place for everything. Lost because the grading code would then run where the plan lock cannot be checked by CI, and a change to a threshold rule could happen in a notebook without a commit.
- **Grade in MLflow evaluation.** Good for metrics, but the frontier, the operating point rule, the cluster bootstrap and Benjamini-Hochberg are not its defaults, and writing them as custom metrics would hide them.

## Consequences

The plan hash is checked before grading (scripts/check_registration.py); the grade is reproducible from the exported files alone; a reviewer can rerun it without workspace access.
