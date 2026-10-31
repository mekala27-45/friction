"""What Databricks Free Edition lets this account do today, measured rather than assumed.

Run from GitHub Actions only. Every check is tried and recorded as allowed, refused (with the
error class and the first line of its message) or absent. Anything the probe creates is named
with a probe prefix and removed before it exits. The deploy steps read results/workspace_probe.json
and choose their path from it: catalogs or schema prefixes, groups or the entitlements table,
the billing system table or the run ledger.

No identity leaves the workspace: the probe records that a user identity exists and its kind,
never the user name.
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROBE_PREFIX = "friction_probe"


def _attempt(fn: Callable[[], Any]) -> dict[str, Any]:
    started = time.monotonic()
    try:
        detail = fn()
        out: dict[str, Any] = {"status": "allowed"}
        if detail is not None:
            out["detail"] = detail
    except Exception as exc:  # the class and first line are the finding
        message = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
        out = {"status": "refused", "error": type(exc).__name__, "message": message[:300]}
    out["seconds"] = round(time.monotonic() - started, 2)
    return out


def run(out_path: Path) -> dict[str, Any]:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient()
    findings: dict[str, Any] = {"probed_at": datetime.now(UTC).isoformat(timespec="seconds")}
    suffix = hashlib.sha256(str(time.time()).encode()).hexdigest()[:6]

    def me() -> dict[str, Any]:
        user = w.current_user.me()
        return {
            "identity": "present",
            "kind": "user" if user.user_name and "@" in user.user_name else "service",
        }

    findings["identity"] = _attempt(me)
    findings["catalogs_visible"] = _attempt(lambda: sorted(c.name for c in w.catalogs.list() if c.name))

    probe_catalog = f"{PROBE_PREFIX}_{suffix}"

    def create_catalog() -> str:
        w.catalogs.create(name=probe_catalog, comment="friction probe, removed at exit")
        return probe_catalog

    findings["catalog_create"] = _attempt(create_catalog)
    catalog_ok = findings["catalog_create"]["status"] == "allowed"
    base_catalog = probe_catalog if catalog_ok else "workspace"
    schema = f"{PROBE_PREFIX}_{suffix}"
    findings["schema_create"] = _attempt(lambda: w.schemas.create(name=schema, catalog_name=base_catalog).full_name)

    from databricks.sdk.service.catalog import VolumeType

    findings["volume_create"] = _attempt(
        lambda: (
            w.volumes.create(
                catalog_name=base_catalog, schema_name=schema, name="landing", volume_type=VolumeType.MANAGED
            ).full_name
        )
    )

    def warehouses() -> list[dict[str, Any]]:
        return [
            {
                "id": wh.id,
                "size": wh.cluster_size,
                "serverless": wh.enable_serverless_compute,
                "state": str(wh.state),
            }
            for wh in w.warehouses.list()
        ]

    findings["warehouses"] = _attempt(warehouses)
    warehouse_id = None
    if findings["warehouses"]["status"] == "allowed" and findings["warehouses"].get("detail"):
        warehouse_id = findings["warehouses"]["detail"][0]["id"]

    def sql(statement: str) -> Callable[[], Any]:
        def inner() -> Any:
            if warehouse_id is None:
                raise RuntimeError("no SQL warehouse")
            resp = w.statement_execution.execute_statement(
                statement=statement, warehouse_id=warehouse_id, wait_timeout="50s"
            )
            state = resp.status.state if resp.status else None
            if state != StatementState.SUCCEEDED:
                err = resp.status.error.message if resp.status and resp.status.error else str(state)
                raise RuntimeError(err)
            rows = resp.result.data_array if resp.result and resp.result.data_array else []
            return {"rows": len(rows)}

        return inner

    fq = f"{base_catalog}.{schema}"
    findings["sql_version"] = _attempt(sql("SELECT current_version()"))
    findings["create_table"] = _attempt(sql(f"CREATE TABLE {fq}.probe_t (ip STRING, n INT)"))
    findings["create_mask_function"] = _attempt(
        sql(
            f"CREATE FUNCTION {fq}.mask_ip(ip STRING) RETURN "
            "CASE WHEN is_account_group_member('none') THEN ip ELSE 'masked' END"
        )
    )
    findings["set_column_mask"] = _attempt(sql(f"ALTER TABLE {fq}.probe_t ALTER COLUMN ip SET MASK {fq}.mask_ip"))
    findings["set_tag"] = _attempt(
        sql(f"ALTER TABLE {fq}.probe_t ALTER COLUMN ip SET TAGS ('classification' = 'restricted')")
    )
    findings["row_filter_function"] = _attempt(sql(f"CREATE FUNCTION {fq}.rf(n INT) RETURN n > 0"))
    findings["set_row_filter"] = _attempt(sql(f"ALTER TABLE {fq}.probe_t SET ROW FILTER {fq}.rf ON (n)"))
    findings["liquid_clustering"] = _attempt(sql(f"CREATE TABLE {fq}.probe_c (k STRING) CLUSTER BY (k)"))
    for table in (
        "system.billing.usage",
        "system.billing.list_prices",
        "system.access.table_lineage",
        "system.access.column_lineage",
        "system.access.audit",
        "system.lakeflow.jobs",
        "system.lakeflow.pipelines",
        "system.information_schema.tables",
    ):
        findings[f"read:{table}"] = _attempt(sql(f"SELECT 1 FROM {table} LIMIT 1"))

    findings["groups_list"] = _attempt(lambda: len(list(w.groups.list())))
    findings["service_principals_list"] = _attempt(lambda: len(list(w.service_principals.list())))
    findings["jobs_list"] = _attempt(lambda: len(list(w.jobs.list(limit=5))))
    findings["pipelines_list"] = _attempt(lambda: len(list(w.pipelines.list_pipelines(max_results=5))))
    findings["serving_endpoints_list"] = _attempt(lambda: len(list(w.serving_endpoints.list())))
    findings["apps_list"] = _attempt(lambda: len(list(w.apps.list())))
    findings["registered_models_list"] = _attempt(lambda: len(list(w.registered_models.list(max_results=5))))
    findings["experiments_list"] = _attempt(lambda: len(list(w.experiments.list_experiments(max_results=5))))

    # Clean up everything the probe made.
    cleanup: list[str] = []
    for statement in (f"DROP SCHEMA IF EXISTS {fq} CASCADE",):
        result = _attempt(sql(statement))
        cleanup.append(f"{statement}: {result['status']}")
    if catalog_ok:
        result = _attempt(lambda: w.catalogs.delete(name=probe_catalog, force=True))
        cleanup.append(f"drop catalog {probe_catalog}: {result['status']}")
    findings["cleanup"] = cleanup

    allowed = {k: v.get("status") == "allowed" for k, v in findings.items() if isinstance(v, dict) and "status" in v}
    findings["summary"] = {
        "catalog_per_environment": allowed.get("catalog_create", False),
        "masks": allowed.get("set_column_mask", False),
        "row_filter": allowed.get("set_row_filter", False),
        "tags": allowed.get("set_tag", False),
        "groups": allowed.get("groups_list", False),
        "billing_system_table": allowed.get("read:system.billing.usage", False),
        "lineage_system_table": allowed.get("read:system.access.table_lineage", False),
        "serving": allowed.get("serving_endpoints_list", False),
        "sql_warehouse": warehouse_id is not None,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(findings, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return findings


if __name__ == "__main__":
    try:
        result = run(Path(sys.argv[1]) if len(sys.argv) > 1 else Path("workspace_probe.json"))
        print(json.dumps(result["summary"], indent=1))
    except Exception:
        traceback.print_exc()
        sys.exit(1)


def run_v2(out_path: Path, wait_minutes: int = 30) -> dict[str, Any]:
    """The second probe, for the work after v0.1.0: model serving from Unity Catalog, AI/BI
    dashboards, Genie spaces, Delta Sharing, feature tables with a timeseries key, online tables and
    Lakebase. Anything created is named with the probe prefix and removed before exit."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient()
    suffix = hashlib.sha256(str(time.time()).encode()).hexdigest()[:6]
    findings: dict[str, Any] = {"probed_at": datetime.now(UTC).isoformat(timespec="seconds")}
    warehouse_id = next((wh.id for wh in w.warehouses.list()), None)

    def sql(statement: str) -> Callable[[], Any]:
        def inner() -> Any:
            if warehouse_id is None:
                raise RuntimeError("no SQL warehouse")
            resp = w.statement_execution.execute_statement(
                statement=statement, warehouse_id=warehouse_id, wait_timeout="50s"
            )
            if not resp.status or resp.status.state != StatementState.SUCCEEDED:
                err = resp.status.error.message if resp.status and resp.status.error else "no status"
                raise RuntimeError(err)
            return None

        return inner

    schema = f"workspace.{PROBE_PREFIX}_{suffix}"
    findings["schema_create"] = _attempt(sql(f"CREATE SCHEMA {schema}"))
    findings["feature_table_timeseries_key"] = _attempt(
        sql(
            f"CREATE TABLE {schema}.ft (k STRING NOT NULL, ts TIMESTAMP NOT NULL, v DOUBLE, "
            "CONSTRAINT ft_pk PRIMARY KEY (k, ts TIMESERIES))"
        )
    )
    findings["change_data_feed"] = _attempt(
        sql(f"ALTER TABLE {schema}.ft SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    )

    dashboard_id: list[str] = []

    def lakeview() -> Any:
        from databricks.sdk.service.dashboards import Dashboard

        body = {"pages": [{"name": "p", "displayName": "probe", "layout": []}]}
        d = w.lakeview.create(Dashboard(display_name=f"{PROBE_PREFIX}_{suffix}", serialized_dashboard=json.dumps(body),
                                        warehouse_id=warehouse_id))  # fmt: skip
        dashboard_id.append(str(d.dashboard_id))
        return {"created": True}

    findings["lakeview_create"] = _attempt(lakeview)

    def publish() -> Any:
        if not dashboard_id:
            raise RuntimeError("no dashboard to publish")
        w.lakeview.publish(dashboard_id[0], warehouse_id=warehouse_id, embed_credentials=True)
        return None

    findings["lakeview_publish"] = _attempt(publish)
    findings["genie_list_spaces"] = _attempt(lambda: len(w.genie.list_spaces().spaces or []))
    space_id: list[str] = []

    def genie() -> Any:
        space = {
            "version": 1,
            "data_sources": {"tables": [{"identifier": "workspace.friction_dev_gold.mcc_fraud_rates"}]},
        }
        s = w.genie.create_space(warehouse_id=str(warehouse_id), serialized_space=json.dumps(space),
                                 title=f"{PROBE_PREFIX}_{suffix}")  # fmt: skip
        space_id.append(str(s.space_id))
        return {"created": True}

    findings["genie_create_space"] = _attempt(genie)
    findings["shares_list"] = _attempt(lambda: len(list(w.shares.list_shares())))
    findings["database_instances_list"] = _attempt(lambda: len(list(w.database.list_database_instances())))
    findings["online_table_get"] = _attempt(lambda: w.online_tables.get(f"{schema}.ft_online") and None)

    endpoint = f"friction-probe-{suffix}"

    def serving() -> Any:
        started = time.monotonic()
        entity = ServedEntityInput(entity_name="workspace.friction_dev_ml.payments_lightgbm", entity_version="1",
                                   workload_size="Small", scale_to_zero_enabled=True)  # fmt: skip
        w.serving_endpoints.create(
            name=endpoint, config=EndpointCoreConfigInput(name=endpoint, served_entities=[entity])
        )
        state = "unknown"
        while time.monotonic() - started < wait_minutes * 60:
            e = w.serving_endpoints.get(endpoint)
            ready = e.state.ready.value if e.state and e.state.ready else "unknown"
            update = e.state.config_update.value if e.state and e.state.config_update else "unknown"
            state = f"{ready}/{update}"
            if ready == "READY" or update in ("UPDATE_FAILED", "UPDATE_CANCELED"):
                break
            time.sleep(30)
        return {"state": state, "minutes_to_state": round((time.monotonic() - started) / 60, 1)}

    findings["serving_create_from_uc"] = _attempt(serving)
    if findings["serving_create_from_uc"].get("detail", {}).get("state", "").startswith("READY"):

        def query() -> Any:
            from friction_features import payments

            row = dict.fromkeys(payments.FEATURES, 0.0)
            started = time.monotonic()
            r = w.serving_endpoints.query(name=endpoint, dataframe_records=[row])
            return {"predictions": len(r.predictions or []), "seconds": round(time.monotonic() - started, 2)}

        findings["serving_query"] = _attempt(query)

    cleanup = [
        f"endpoint: {_attempt(lambda: w.serving_endpoints.delete(endpoint))['status']}",
        f"schema: {_attempt(sql(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))['status']}",
    ]
    if dashboard_id:
        cleanup.append(f"dashboard: {_attempt(lambda: w.lakeview.trash(dashboard_id[0]))['status']}")
    if space_id:
        cleanup.append(f"genie space: {_attempt(lambda: w.genie.trash_space(space_id[0]))['status']}")
    findings["cleanup"] = cleanup
    allowed = {k: v.get("status") == "allowed" for k, v in findings.items() if isinstance(v, dict) and "status" in v}
    serving_state = findings["serving_create_from_uc"].get("detail", {}).get("state", "")
    findings["summary"] = {
        "serving_from_uc": serving_state.startswith("READY") and allowed.get("serving_query", False),
        "lakeview": allowed.get("lakeview_create", False),
        "lakeview_publish": allowed.get("lakeview_publish", False),
        "genie": allowed.get("genie_create_space", False),
        "sharing": allowed.get("shares_list", False),
        "feature_tables": allowed.get("feature_table_timeseries_key", False),
        "lakebase": allowed.get("database_instances_list", False),
        "online_tables_api": findings["online_table_get"].get("error") in (None, "NotFound", "ResourceDoesNotExist"),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(findings, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return findings
