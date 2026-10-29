# One workspace with prefixed schemas stands in for two

Status: accepted, 29 October 2026.

dev and prod are schema prefixes (friction_dev_, friction_prod_) in the workspace catalog of one Free Edition workspace.

## Alternatives

- **A catalog per environment.** The probe asked for one and the API refused: the metastore has no storage root, and Default Storage catalogs can only be made in the UI (results/workspace_probe.json, DECISIONS.md).
- **Two workspaces.** What a bank runs. Free Edition gives one per account.

## What it does not prove

Separation of compute, quotas, network and metastores. It separates names and grants, and promotion between the two is still a reviewed request.
