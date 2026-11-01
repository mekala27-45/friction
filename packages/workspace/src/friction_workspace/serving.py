"""The payments champion behind a Databricks model serving endpoint, checked and timed.

Request kind serving. The endpoint serves the Unity Catalog version the alias names, the same file
the Vercel API serves (the promotion request checked its sha256). The check reads the feature
vectors of the parity events from the gold feature table through the SQL warehouse, sends them to
the endpoint, and compares every score with the score the lakehouse gave the event. Then it times
single row queries from the runner, the machine that timed the Vercel API, so the two latencies are
measured from the same place. The endpoint scores a vector it is given; the Vercel API builds the
vector from its online state first, which is more work per call, and the record says so.
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any

TOLERANCE = 1e-9


def percentiles(ms: list[float]) -> dict[str, float]:
    if not ms:
        return {"n": 0}
    s = sorted(ms)

    def at(q: float) -> float:
        return round(s[min(len(s) - 1, max(0, math.ceil(q * len(s)) - 1))], 1)

    return {"n": len(s), "p50_ms": at(0.50), "p95_ms": at(0.95), "p99_ms": at(0.99),
            "mean_ms": round(sum(s) / len(s), 1)}  # fmt: skip


def feature_sql(table: str, key: str, features: list[str], refs: list[int]) -> str:
    """The vectors of the given events, every feature as a double, in key order."""
    if not refs or not all(isinstance(r, int) for r in refs):
        raise ValueError("event references must be integers")
    cols = ", ".join(f"CAST(`{f}` AS DOUBLE) AS `{f}`" for f in features)  # foreign and hour are keywords
    return f"SELECT {key}, {cols} FROM {table} WHERE {key} IN ({', '.join(map(str, sorted(refs)))}) ORDER BY {key}"


def compare(scores: dict[int, float], expected: dict[int, float]) -> dict[str, Any]:
    missing = sorted(set(expected) - set(scores))
    worst = 0.0
    differ = []
    for ref, want in expected.items():
        if ref not in scores:
            continue
        diff = abs(scores[ref] - want) / max(abs(want), 1e-12)
        worst = max(worst, diff)
        if diff > TOLERANCE:
            differ.append(ref)
    scored = len(expected) - len(missing)
    return {"events": len(expected), "scored": scored, "equal": scored - len(differ), "missing": len(missing),
            "differ": len(differ), "max_relative_difference": worst}  # fmt: skip


def _rows(client: Any, warehouse: str, sql: str) -> tuple[list[str], list[list[Any]]]:
    from databricks.sdk.service.sql import StatementState

    resp = client.statement_execution.execute_statement(statement=sql, warehouse_id=warehouse, wait_timeout="50s")
    while resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = client.statement_execution.get_statement(resp.statement_id)
    if not resp.status or resp.status.state != StatementState.SUCCEEDED:
        message = resp.status.error.message if resp.status and resp.status.error else "no status"
        raise RuntimeError(f"statement failed: {message}")
    columns = [c.name for c in resp.manifest.schema.columns]
    rows = list(resp.result.data_array or [])
    chunk = resp.result.next_chunk_index
    while chunk is not None:
        part = client.statement_execution.get_statement_result_chunk_n(resp.statement_id, chunk)
        rows += list(part.data_array or [])
        chunk = part.next_chunk_index
    return columns, rows


def deploy(client: Any, endpoint: str, model: str, version: str, wait_minutes: int) -> dict[str, Any]:
    """Create the endpoint, or point it at this version; wait until it serves it."""
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

    entity = ServedEntityInput(entity_name=model, entity_version=version, workload_size="Small",
                               scale_to_zero_enabled=True)  # fmt: skip
    started = time.monotonic()
    existing = {e.name for e in client.serving_endpoints.list()}
    if endpoint in existing:
        client.serving_endpoints.update_config(name=endpoint, served_entities=[entity])
        action = "updated"
    else:
        config = EndpointCoreConfigInput(name=endpoint, served_entities=[entity])
        client.serving_endpoints.create(name=endpoint, config=config)
        action = "created"
    state = "unknown"
    while time.monotonic() - started < wait_minutes * 60:
        e = client.serving_endpoints.get(endpoint)
        ready = e.state.ready.value if e.state and e.state.ready else "unknown"
        update = e.state.config_update.value if e.state and e.state.config_update else "unknown"
        state = f"{ready}/{update}"
        if (ready == "READY" and update == "NOT_UPDATING") or update in ("UPDATE_FAILED", "UPDATE_CANCELED"):
            break
        time.sleep(20)
    if not state.startswith("READY/NOT_UPDATING"):
        raise RuntimeError(f"endpoint {endpoint} is {state} after {wait_minutes} minutes")
    return {"action": action, "state": state, "minutes_to_ready": round((time.monotonic() - started) / 60, 1)}


def run(client: Any, params: dict[str, Any], root: Path, out: Path) -> dict[str, Any]:
    from friction_features import payments

    model = str(params["model"])
    alias = str(params.get("alias", "champion"))
    endpoint = str(params["endpoint"])
    version = str(client.model_versions.get_by_alias(full_name=model, alias=alias).version)
    record: dict[str, Any] = {"endpoint": endpoint, "model": model, "alias": alias, "version": version}
    record["deploy"] = deploy(client, endpoint, model, version, int(params.get("wait_minutes", 40)))

    parity = json.loads((root / str(params["parity"])).read_text(encoding="utf-8"))
    expected = {int(e["event_ref"]): float(e["score"]) for e in parity["expected"]}
    warehouse = next(iter(client.warehouses.list())).id
    features = list(payments.FEATURES)
    columns, rows = _rows(client, warehouse, feature_sql(str(params["features_table"]), "txn_ref", features,
                                                         sorted(expected)))  # fmt: skip
    vectors = {int(r[0]): {c: float(v) for c, v in zip(columns[1:], r[1:], strict=True)} for r in rows
               if all(v is not None for v in r[1:])}  # fmt: skip
    record["vectors"] = {"asked": len(expected), "found": len(rows), "complete": len(vectors)}
    refs = sorted(vectors)

    def query(batch: list[int]) -> list[float]:
        resp = client.serving_endpoints.query(name=endpoint, dataframe_records=[vectors[r] for r in batch])
        predictions = resp.predictions or []
        return [float(p["score"] if isinstance(p, dict) else p) for p in predictions]

    t = time.monotonic()
    first = query(refs[:1])
    record["first_query_ms"] = round((time.monotonic() - t) * 1000, 1)
    scores: dict[int, float] = {refs[0]: first[0]}
    size = int(params.get("batch", 100))
    for i in range(0, len(refs), size):
        batch = refs[i : i + size]
        scores.update(zip(batch, query(batch), strict=True))
    record["parity"] = compare(scores, expected)

    timed = []
    for ref in refs[: int(params.get("queries", 200))]:
        t = time.monotonic()
        query([ref])
        timed.append((time.monotonic() - t) * 1000)
    record["latency_single_row"] = percentiles(timed)
    vercel = root / "results" / "latency.json"
    if vercel.exists():
        v = json.loads(vercel.read_text(encoding="utf-8"))
        work = "builds the vector from its online state, scores it, codes it and stores the decision"
        record["vercel_payments"] = {**v.get("payments", {}), "measured_at": v.get("at"), "work": work}
    record["work"] = "scores a vector it is given"
    if not params.get("keep", True):
        client.serving_endpoints.delete(endpoint)
        record["deleted"] = True
    (out / "serving.json").write_text(json.dumps(record, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    if record["parity"]["equal"] != record["parity"]["events"]:
        raise RuntimeError(f"serving parity: {record['parity']}")
    return {"endpoint": endpoint, "version": version, "parity_equal": record["parity"]["equal"]}
