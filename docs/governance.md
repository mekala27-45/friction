# Governance

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

**Classification.** Every column in contracts/ carries a class: internal, sensitive or restricted.
Restricted values (user, card, address and browser string identifiers) never leave bronze in clear:
silver holds salted tokens, with the salt in a workspace secret scope. Sensitive values (country,
region, city, the /16 of an address) are coarsened for readers.

**Masks and the row filter.** Readers see silver through dynamic views in gold whose mask function
returns a restricted token only to a principal entitled in ops.entitlements, and whose row filter
keeps rows past the principal's label horizon (jobs/governance.py). One Free Edition identity shows
both reads: entitled, then with the entitlement withdrawn for one query. infra/terraform holds the
grants. The probe found mask functions allowed on Free Edition (results/workspace_probe.json); the job
has not run in the workspace yet.

**The scan.** scripts/scan_for_planted_identifiers.py reads every committed file past bronze for a Luhn
valid card number, a CVV shaped field, a street address and a raw IPv4 address, and fails on any; its
tests plant each one.

**Drift.** Terraform plan runs on every push once its state backend is reachable, and shows any change
made in the workspace outside Terraform. The planted drift, a grant added by a SQL request, is the
test of that; it waits on the workspace secrets.
