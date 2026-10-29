# The online store is Postgres

Status: accepted, 29 October 2026.

The API keeps each card's, user's and address's feature state as a JSON row in Postgres (Neon), and writes the decision in the same transaction.

## Alternatives

- **Redis or another key value store.** Lower latency per read, the usual production answer for online features. Lost because a decision and the state change it causes must commit together or not at all, and doing that across a cache and a log needs a second system and an outbox.
- **A feature serving endpoint in the workspace.** Not on Free Edition.

## What a key value store would change

Reads would drop to about a millisecond and the decision log would move to a separate durable store with an outbox; the parity test would then also have to cover replication lag.
