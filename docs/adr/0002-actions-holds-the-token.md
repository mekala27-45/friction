# GitHub Actions is the only machine with the workspace token

Status: accepted, 29 October 2026.

The workspace host and token live only in GitHub Actions secrets. The build machine never calls the workspace; it commits a request under requests/, and the workspace workflow runs it and writes the result to the results branch.

## Alternatives

- **A personal access token on the build machine.** Faster to iterate. Lost because a token on a laptop ends up in a shell history, a log or a notebook, and the build's rule is that no token appears anywhere a person or a model can read it.
- **OAuth machine to machine credentials.** The production answer. Not available on Free Edition, which has no account console.

## Consequences

Every workspace action is a commit, so the history of what was run is the git log. Results come back on a branch and enter main only through scripts/promote_results.py, with provenance. The cost is latency: a workspace step takes a push and a workflow run.
