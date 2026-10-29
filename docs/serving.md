# Serving

> friction is a portfolio demonstration built on two published synthetic data sets (the IBM TabFormer card transactions, and the RBA login data set under CC BY 4.0) and a simulator with known truth. No real person, card or account appears in it, its numbers describe synthetic data, and it must not be used to decide anything about anyone. It runs on Databricks Free Edition, which is for non commercial use.

**The API.** FastAPI in a container (Dockerfile.vercel; Render is the fallback, render.yaml). Routes:
POST /v1/payments/score, POST /v1/logins/score, GET /v1/decisions/{id}, GET /health, and two routes for
the parity replay that need the replay key (POST /v1/admin/seed, GET /v1/admin/state/...).

**One transaction per event.** Lock the state rows of the event's card, or user and address; compute
the features with packages/features; score with the exported bundle; apply the policy; write the new
state and the decision; commit; answer with the decision id. tests/api checks the decision is visible
from an independent connection, that a failed log write leaves no response and no state change, and
that state and decision commit together.

**Budget.** Sixty requests an hour per client address, hashed with a key, counted in its own
transaction. The replay key is not budgeted.

**Parity.** scripts/check_parity.py seeds a namespace with the state just before the first exported
event and replays every event in order; decisions must be equal and scores within 1e-9.

**Environment.** DATABASE_URL (Neon), FRICTION_REPLAY_KEY, FRICTION_CLIENT_KEY, FRICTION_CORS_ORIGINS,
and FRICTION_SALT only if raw values are sent. None of them is in the repository.
