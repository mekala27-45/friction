"""The online store and the decision log, in Postgres (Neon in production).

One transaction per scored event: lock the state rows of the event's entities, compute, write the
new state and the decision, commit, and only then answer. The decision id goes back to the caller
after the commit, so a decision the caller holds is a decision another connection can read
(tests/api: test_decision_is_committed, test_audit_precedes_response,
test_state_and_decision_share_a_transaction).

State lives under a namespace, so the parity replay can seed and reset its own entities without
touching what site visitors have scored.
"""

from __future__ import annotations

import json
import queue
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

DDL = """
CREATE TABLE IF NOT EXISTS online_state (
  namespace text NOT NULL,
  kind text NOT NULL,
  key text NOT NULL,
  state jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace, kind, key)
);
CREATE TABLE IF NOT EXISTS card_issues (
  namespace text NOT NULL,
  account_key text NOT NULL,
  issued_at bigint[] NOT NULL,
  PRIMARY KEY (namespace, account_key)
);
CREATE TABLE IF NOT EXISTS decision_log (
  id uuid PRIMARY KEY,
  namespace text NOT NULL,
  desk text NOT NULL,
  event_ref bigint NOT NULL,
  entity text NOT NULL,
  score double precision NOT NULL,
  verdict text NOT NULL,
  codes text NOT NULL,
  model text NOT NULL,
  bundle_digest text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS decision_log_event ON decision_log (namespace, desk, event_ref);
CREATE TABLE IF NOT EXISTS client_budget (
  client text NOT NULL,
  window_start timestamptz NOT NULL,
  n integer NOT NULL,
  PRIMARY KEY (client, window_start)
);
"""


class Tx:
    """The operations of one scoring transaction."""

    def __init__(self, conn: psycopg.Connection[Any], namespace: str) -> None:
        self.conn = conn
        self.ns = namespace

    def lock_state(self, kind: str, key: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT state FROM online_state WHERE namespace = %s AND kind = %s AND key = %s FOR UPDATE",
            (self.ns, kind, key),
        ).fetchone()
        return None if row is None else dict(row[0])

    def put_state(self, kind: str, key: str, state: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO online_state (namespace, kind, key, state, updated_at) VALUES (%s, %s, %s, %s, now()) "
            "ON CONFLICT (namespace, kind, key) DO UPDATE SET state = EXCLUDED.state, updated_at = now()",
            (self.ns, kind, key, Jsonb(state)),
        )

    def issues(self, account_key: str) -> list[int]:
        row = self.conn.execute(
            "SELECT issued_at FROM card_issues WHERE namespace = %s AND account_key = %s", (self.ns, account_key)
        ).fetchone()
        return [] if row is None else [int(x) for x in row[0]]

    def log(self, desk: str, event_ref: int, entity: str, score: float, verdict: str, codes: list[str],
            model: str, digest: str) -> str:  # fmt: skip
        decision_id = str(uuid.uuid4())
        self.conn.execute(
            "INSERT INTO decision_log (id, namespace, desk, event_ref, entity, score, verdict, codes, model, "
            "bundle_digest) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (decision_id, self.ns, desk, event_ref, entity, score, verdict, ",".join(codes), model, digest),
        )
        return decision_id


class Store:
    """Connections are kept and reused, a few per process: opening one costs more than scoring."""

    def __init__(self, url: str, size: int = 4) -> None:
        self.url = url
        self.size = size
        self._idle: queue.LifoQueue[psycopg.Connection[Any]] = queue.LifoQueue()

    @contextmanager
    def connect(self) -> Iterator[psycopg.Connection[Any]]:
        try:
            conn = self._idle.get_nowait()
            if conn.closed:
                conn = psycopg.connect(self.url, autocommit=False)
        except queue.Empty:
            conn = psycopg.connect(self.url, autocommit=False)
        clean = False
        try:
            yield conn
            clean = not conn.closed and conn.info.transaction_status == TransactionStatus.IDLE
        finally:
            if clean and self._idle.qsize() < self.size:
                self._idle.put(conn)
            else:
                conn.close()

    def migrate(self) -> None:
        with self.connect() as conn:
            conn.execute(DDL)
            conn.commit()

    @contextmanager
    def transaction(self, namespace: str) -> Iterator[Tx]:
        with self.connect() as conn:
            try:
                yield Tx(conn, namespace)
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def spend(self, client: str, limit: int, now: datetime | None = None) -> tuple[bool, int]:
        """Count one request against the client's hour; its own transaction, kept even when refused."""
        t = (now or datetime.now(UTC)).replace(minute=0, second=0, microsecond=0)
        with self.connect() as conn:
            row = conn.execute(
                "INSERT INTO client_budget (client, window_start, n) VALUES (%s, %s, 1) "
                "ON CONFLICT (client, window_start) DO UPDATE SET n = client_budget.n + 1 RETURNING n",
                (client, t),
            ).fetchone()
            conn.commit()
        n = int(row[0]) if row else 0
        return n <= limit, n

    def decision(self, decision_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT id, namespace, desk, event_ref, entity, score, verdict, codes, model, bundle_digest, "
                "created_at FROM decision_log WHERE id = %s",
                (decision_id,),
            ).fetchone()
        if row is None:
            return None
        keys = ("id", "namespace", "desk", "event_ref", "entity", "score", "verdict", "codes", "model",
                "bundle_digest", "created_at")  # fmt: skip
        out = dict(zip(keys, row, strict=True))
        out["id"] = str(out["id"])
        out["codes"] = [c for c in str(out["codes"]).split(",") if c]
        out["created_at"] = out["created_at"].isoformat()
        return out

    def seed(self, namespace: str, snapshot: dict[str, Any]) -> dict[str, int]:
        """Replace a namespace's state with a snapshot: {kind: {key: state}, "card_issues": {key: [..]}}."""
        counts: dict[str, int] = {}
        with self.connect() as conn:
            conn.execute("DELETE FROM online_state WHERE namespace = %s", (namespace,))
            conn.execute("DELETE FROM card_issues WHERE namespace = %s", (namespace,))
            for kind, entries in snapshot.items():
                if kind == "card_issues":
                    for key, issued in entries.items():
                        conn.execute(
                            "INSERT INTO card_issues (namespace, account_key, issued_at) VALUES (%s, %s, %s)",
                            (namespace, key, [int(x) for x in issued]),
                        )
                else:
                    for key, state in entries.items():
                        conn.execute(
                            "INSERT INTO online_state (namespace, kind, key, state) VALUES (%s, %s, %s, %s)",
                            (namespace, kind, key, Jsonb(state)),
                        )
                counts[kind] = len(entries)
            conn.commit()
        return counts

    def state(self, namespace: str, kind: str, key: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT state FROM online_state WHERE namespace = %s AND kind = %s AND key = %s", (namespace, kind, key)
            ).fetchone()
        return None if row is None else dict(row[0])


def dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True)
