"""The governance job: tags from the contracts, the entitlements table, masks, the row filter, the
masked and unmasked read, and the scan of what readers can see.

Restricted columns are tokens already; readers see them only through dynamic views whose mask
functions consult ops.entitlements, so one Free Edition identity can show both reads: entitled, then
with its entitlement withdrawn for the length of one query. The pipeline's own tables keep their
contract tags. Writes the evidence to the exports volume for the repository.
"""

from __future__ import annotations

import json
from pathlib import Path

import _boot

args = _boot.parse([("--export", {"default": "governance"})])
spark = _boot.session()
ledger = _boot.Ledger(spark, args, "governance")
import yaml  # noqa: E402

ops = f"{args.catalog}.{args.prefix}_ops"
gold = f"{args.catalog}.{args.prefix}_gold"
silver = f"{args.catalog}.{args.prefix}_silver"
evidence: dict[str, object] = {"tags": [], "views": []}

spark.sql(f"CREATE TABLE IF NOT EXISTS {ops}.entitlements (principal STRING, restricted BOOLEAN, horizon_days INT)")
me = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
spark.sql(
    f"MERGE INTO {ops}.entitlements t USING (SELECT '{me}' AS principal) s ON t.principal = s.principal "
    "WHEN NOT MATCHED THEN INSERT (principal, restricted, horizon_days) VALUES (s.principal, true, 0)"
)
spark.sql(
    f"CREATE OR REPLACE FUNCTION {ops}.mask_restricted(v STRING) RETURNS STRING RETURN "
    f"CASE WHEN EXISTS (SELECT 1 FROM {ops}.entitlements e WHERE e.principal = current_user() AND e.restricted) "
    "THEN v ELSE NULL END"
)
spark.sql(
    f"CREATE OR REPLACE FUNCTION {ops}.past_horizon(ts TIMESTAMP) RETURNS BOOLEAN RETURN "
    f"ts <= current_timestamp() - make_interval(0, 0, 0, coalesce((SELECT max(horizon_days) FROM {ops}.entitlements "
    "e WHERE e.principal = current_user()), 90))"
)

root = Path(args.root)
for path in sorted((root / "contracts").glob("*.yaml")):
    c = yaml.safe_load(path.read_text(encoding="utf-8"))
    layer = c.get("layer")
    if layer not in {"bronze", "silver", "cdc", "gold"} or "columns" not in c:
        continue
    table = f"{args.catalog}.{args.prefix}_{layer}.{c['table']}"
    if not spark.catalog.tableExists(table):
        continue
    for col in c["columns"]:
        cls = col.get("classification", "internal")
        try:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{col['name']}` SET TAGS ('classification' = '{cls}')")
            evidence["tags"].append([c["table"], col["name"], cls])  # type: ignore[union-attr]
        except Exception as exc:  # a tag the table type refuses is recorded, not hidden
            evidence["tags"].append([c["table"], col["name"], f"refused: {type(exc).__name__}"])  # type: ignore[union-attr]

for name, key_cols in (
    ("logins_silver", ("user_key", "ip_key", "ua_key")),
    ("payments_silver", ("account_key", "user_key")),
):
    cols = [
        r["col_name"]
        for r in spark.sql(f"DESCRIBE TABLE {silver}.{name}").collect()
        if r["col_name"] and not r["col_name"].startswith("#")
    ]
    exprs = [f"{ops}.mask_restricted({c}) AS {c}" if c in key_cols else c for c in dict.fromkeys(cols)]
    view = f"{gold}.v_{name}"
    spark.sql(
        f"CREATE OR REPLACE VIEW {view} AS SELECT {', '.join(exprs)} "
        f"FROM {silver}.{name} WHERE {ops}.past_horizon(event_ts)"
    )
    evidence["views"].append(view)  # type: ignore[union-attr]

view = f"{gold}.v_logins_silver"
unmasked = spark.sql(f"SELECT count(*) AS n, count(ip_key) AS visible FROM {view}").collect()[0].asDict()
spark.sql(f"UPDATE {ops}.entitlements SET restricted = false WHERE principal = current_user()")
try:
    masked = spark.sql(f"SELECT count(*) AS n, count(ip_key) AS visible FROM {view}").collect()[0].asDict()
finally:
    spark.sql(f"UPDATE {ops}.entitlements SET restricted = true WHERE principal = current_user()")
evidence["read"] = {"entitled": unmasked, "withdrawn": masked}
evidence["grants"] = [r.asDict() for r in spark.sql(f"SHOW GRANTS ON SCHEMA {gold}").collect()]

out = f"{_boot.volume(args, 'exports')}/{args.export}/governance.json"
spark.createDataFrame([(json.dumps(evidence, default=str),)], "doc string").coalesce(1).write.mode("overwrite").text(
    out
)
ledger.write(
    inputs={"contracts": len(evidence["tags"])}, outputs=evidence["read"], rows_in=0, rows_out=0, status="succeeded"
)  # type: ignore[arg-type]
print(json.dumps(evidence["read"]))
