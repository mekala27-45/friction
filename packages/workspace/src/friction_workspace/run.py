"""Run the pending workspace requests. Called by .github/workflows/workspace.yml, never elsewhere.

    python -m friction_workspace.run --requests requests --out results-branch/runs

Each request writes into runs/<request id>/: what it produced, a log, and done.json with the run
id, the commit it ran from, timings and the digest of every file it wrote. A failed request still
writes done.json with status failed and the error, so the next push does not run it again by
accident; a new request with a new id asks for a retry.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from friction_core.hashing import sha256_file

from friction_workspace.neon import neon_endpoint
from friction_workspace.requests import Request, pending

ROOT = Path(__file__).resolve().parents[4]
# The feeds that follow each desk's calendar, as jobs/land.py lands them.
FEEDS = {"payments": ["payments", "labels_payments", "cdc_cards"], "logins": ["logins", "labels_logins"]}


SECRET_ENV = ("DATABRICKS_TOKEN", "FRICTION_SALT", "TF_STATE_CONN", "PG_CONN_STR", "DATABASE_URL",
              "FRICTION_REPLAY_KEY")  # fmt: skip


def redact(text: str, env: dict[str, str]) -> str:
    """Every secret value the runner holds, and the Neon form of the state string, out of a log.

    The logs go to the results branch, which is public. A Spark error prints its plan, and the salt
    is a literal in the plan, so redaction happens here and not only in GitHub's console masking.
    """
    values = {v for k in SECRET_ENV if len(v := env.get(k, "")) >= 8}
    values |= {neon_endpoint(v) for v in set(values) if "://" in v}
    for value in sorted(values, key=len, reverse=True):
        text = text.replace(value, "<secret>")
    return text


def _sh(cmd: list[str], log: Path, *, cwd: Path = ROOT, env: dict[str, str] | None = None) -> str:
    full = {**os.environ, **(env or {})}
    with log.open("a", encoding="utf-8") as handle:
        handle.write(redact(f"$ {shlex.join(cmd)}\n", full))
        proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=full)
        handle.write(redact(proc.stdout[-20000:], full))
        handle.write(redact(proc.stderr[-20000:], full))
        handle.write(f"[exit {proc.returncode}]\n")
    if proc.returncode != 0:
        raise RuntimeError(f"{cmd[0]} {cmd[1] if len(cmd) > 1 else ''} exited {proc.returncode}")
    return proc.stdout


def _probe_vars() -> dict[str, str]:
    probe = ROOT / "results" / "workspace_probe.json"
    if not probe.exists():
        return {}
    summary = json.loads(probe.read_text(encoding="utf-8")).get("summary", {})
    return {"catalog_mode": "catalog" if summary.get("catalog_per_environment") else "prefix"}


def _bundle_vars(request: Request) -> list[str]:
    values = {**_probe_vars(), **{k: str(v) for k, v in request.params.get("vars", {}).items()}}
    out: list[str] = []
    for key, value in sorted(values.items()):
        out.extend(["--var", f"{key}={value}"])
    return out


def handle(request: Request, out: Path, log: Path) -> dict[str, Any]:
    if request.kind == "probe":
        from friction_workspace.probe import run as probe_run

        findings = probe_run(out / "workspace_probe.json")
        return {"summary": findings["summary"]}
    if request.kind == "secrets":
        from databricks.sdk import WorkspaceClient

        salt = os.environ.get("FRICTION_SALT", "")
        if len(salt) < 32:
            raise ValueError("the FRICTION_SALT Actions secret is missing or shorter than 32 characters")
        w = WorkspaceClient()
        scopes = {s.name for s in w.secrets.list_scopes()}
        if "friction" not in scopes:
            w.secrets.create_scope(scope="friction")
        w.secrets.put_secret(scope="friction", key="salt", string_value=salt)
        return {"scope": "friction", "keys": [k.key for k in w.secrets.list_secrets(scope="friction")]}
    if request.kind == "upload_sources":
        # Regenerate the drops from the locked sources and refuse to upload unless every digest
        # equals the one committed in data/replay.lock.json: Actions lands exactly what was locked.
        _sh([sys.executable, "scripts/fetch_sources.py"], log)
        _sh([sys.executable, "scripts/stage_sources.py", "--drop-csv"], log)
        _sh([sys.executable, "scripts/replay.py", "--all"], log)
        # Kept before the check, so a refused upload leaves the regenerated lock to compare.
        for name in ("data/replay.lock.json", "results/fetch_status.json"):
            target = out / Path(name).name
            target.write_bytes((ROOT / name).read_bytes())
        _sh(["git", "diff", "--exit-code", "--stat", "data/replay.lock.json"], log)
        volume = str(request.params["outbox_volume"])
        _sh(["databricks", "fs", "cp", "-r", "--overwrite", "data/landing", f"dbfs:{volume}"], log)
        return {"uploaded_to": volume}
    if request.kind == "bundle_deploy":
        _sh(["databricks", "bundle", "validate", "-t", request.target, *_bundle_vars(request)], log)
        _sh(
            [
                "databricks",
                "bundle",
                "deploy",
                "-t",
                request.target,
                "--auto-approve",
                *_bundle_vars(request),
            ],
            log,
        )
        summary = _sh(
            ["databricks", "bundle", "summary", "-t", request.target, "-o", "json", *_bundle_vars(request)],
            log,
        )
        (out / "bundle_summary.json").write_text(summary, encoding="utf-8")
        return {"deployed": request.target}
    if request.kind == "bundle_run":
        if not request.job:
            raise ValueError("a bundle_run request names a job")
        cmd = ["databricks", "bundle", "run", "-t", request.target, request.job, *_bundle_vars(request)]
        params = request.params.get("job_params", {})
        if params:
            cmd += ["--params", ",".join(f"{k}={v}" for k, v in sorted(params.items()))]
        started = time.monotonic()
        _sh(cmd, log)
        exported = request.params.get("export_from")
        if exported:
            _sh(["databricks", "fs", "cp", "-r", "--overwrite", f"dbfs:{exported}", str(out / "export")], log)
        return {"job": request.job, "wall_seconds": round(time.monotonic() - started, 1)}
    if request.kind == "portable":
        # The open source engine over the same drops, on this runner, with the workspace salt: the
        # reconcile then compares digests of salted tables exactly, which the build machine cannot.
        outbox = str(request.params["outbox_volume"])
        landing = ROOT / "data" / "work" / f"landing_{request.id}"
        lake = ROOT / "data" / "lake" / request.id
        for desk, (start, through) in sorted(request.params["drops"].items()):
            for feed in FEEDS[desk]:
                (landing / feed / "drops").mkdir(parents=True, exist_ok=True)
                listing = _sh(["databricks", "fs", "ls", f"dbfs:{outbox}/{feed}/drops"], log)
                for drop in sorted(n.strip().rstrip("/") for n in listing.split()):
                    if drop[:4].isdigit() and int(start) <= int(drop[:4]) <= int(through):
                        _sh(["databricks", "fs", "cp", "-r", f"dbfs:{outbox}/{feed}/drops/{drop}",
                             str(landing / feed / "drops" / drop)], log)  # fmt: skip
        started = time.monotonic()
        _sh([sys.executable, "scripts/run_portable.py", "--landing", str(landing), "--out", str(lake)], log)
        _sh([sys.executable, "scripts/run_features.py", "--lake", str(lake)], log)
        _sh([sys.executable, "scripts/summarize_lake.py", "--lake", str(lake), "--engine", "local",
             "--out", str(out / "summary.json")], log)  # fmt: skip
        return {"drops": request.params["drops"], "wall_seconds": round(time.monotonic() - started, 1)}
    if request.kind == "export":
        source = str(request.params["from"])
        _sh(["databricks", "fs", "cp", "-r", "--overwrite", f"dbfs:{source}", str(out / "export")], log)
        return {"exported": source}
    if request.kind == "sql":
        from friction_workspace.sql import run_file

        return run_file(ROOT / str(request.params["file"]), out, request.params.get("vars", {}))
    if request.kind in {"terraform_plan", "terraform_apply"}:
        tf = ROOT / "infra" / "terraform"
        env = {"TF_IN_AUTOMATION": "1"}
        conn = os.environ.get("TF_STATE_CONN") or os.environ.get("PG_CONN_STR")
        if conn:
            env["PG_CONN_STR"] = neon_endpoint(conn)
        _sh(["terraform", "init", "-input=false"], log, cwd=tf, env=env)
        varfile = ["-var-file", str(request.params.get("var_file", "env/dev.tfvars"))]
        _sh(
            ["terraform", "plan", "-input=false", "-no-color", "-out", "plan.bin", *varfile],
            log,
            cwd=tf,
            env=env,
        )
        plan_text = _sh(["terraform", "show", "-no-color", "plan.bin"], log, cwd=tf, env=env)
        (out / "terraform_plan.txt").write_text(plan_text, encoding="utf-8")
        if request.kind == "terraform_apply":
            applied = _sh(["terraform", "apply", "-input=false", "-no-color", "plan.bin"], log, cwd=tf, env=env)
            (out / "terraform_apply.txt").write_text(applied, encoding="utf-8")
        return {"terraform": request.kind}
    raise ValueError(f"unknown request kind {request.kind}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=Path, default=ROOT / "requests")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--only", help="run one request id even if it has a record")
    args = parser.parse_args()
    todo = pending(args.requests, args.out)
    if args.only:
        todo = [r for r in todo if r.id == args.only]
    if not todo:
        print("no pending requests")
        return 0
    failures = 0
    for request in todo:
        out = args.out / request.id
        out.mkdir(parents=True, exist_ok=True)
        log = out / "run.log"
        started = datetime.now(UTC)
        record: dict[str, Any] = {
            "request": request.model_dump(),
            "github_run_id": os.environ.get("GITHUB_RUN_ID"),
            "commit": os.environ.get("GITHUB_SHA"),
            "started_at": started.isoformat(timespec="seconds"),
        }
        try:
            record["result"] = handle(request, out, log)
            record["status"] = "succeeded"
        except Exception as exc:
            record["status"] = "failed"
            record["error"] = redact(f"{type(exc).__name__}: {str(exc)[:500]}", dict(os.environ))
            failures += 1
        record["finished_at"] = datetime.now(UTC).isoformat(timespec="seconds")
        record["files"] = {
            str(p.relative_to(out)): sha256_file(p)
            for p in sorted(out.rglob("*"))
            if p.is_file() and p.name != "done.json"
        }
        (out / "done.json").write_text(json.dumps(record, indent=1, default=str) + "\n", encoding="utf-8")
        print(f"{request.id}: {record['status']}")
        if record["status"] == "failed" and request.params.get("stop_on_failure", True):
            break
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
