"""The request runner, with the shell replaced: what each kind runs, what it records, where it stops."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from friction_workspace import run


class FakeShell:
    def __init__(self, fail_on: str | None = None) -> None:
        self.calls: list[tuple[list[str], dict[str, str]]] = []
        self.fail_on = fail_on

    def __call__(self, cmd: list[str], log: Path, *, cwd: Path = run.ROOT, env: dict[str, str] | None = None) -> str:
        self.calls.append((cmd, env or {}))
        line = " ".join(cmd)
        if self.fail_on and self.fail_on in line:
            raise RuntimeError(f"{cmd[0]} exited 1 holding {'s' * 40}")
        if cmd[:3] == ["databricks", "bundle", "summary"]:
            return '{"resources": {}}'
        if cmd[:3] == ["databricks", "fs", "ls"]:
            return "0001_1991-01/\n0002_1991-02/\n"
        return ""


def _request(folder: Path, rid: str, kind: str, **extra: Any) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{rid}.json").write_text(json.dumps({"id": rid, "kind": kind, "note": "test", **extra}))


def _main(monkeypatch: pytest.MonkeyPatch, requests: Path, out: Path, *extra: str) -> int:
    monkeypatch.setattr("sys.argv", ["run", "--requests", str(requests), "--out", str(out), *extra])
    return run.main()


def test_deploy_run_and_export_are_recorded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    shell = FakeShell()
    monkeypatch.setattr(run, "_sh", shell)
    req, out = tmp_path / "requests", tmp_path / "runs"
    _request(req, "2026-01-01-01-deploy", "bundle_deploy")
    params = {"job_params": {"desk": "logins", "through": "2"}, "export_from": "/Volumes/x/exports/t"}
    _request(req, "2026-01-01-02-run", "bundle_run", job="backfill", params=params)
    _request(req, "2026-01-01-03-export", "export", params={"from": "/Volumes/x/exports/t"})
    assert _main(monkeypatch, req, out) == 0
    done = json.loads((out / "2026-01-01-02-run" / "done.json").read_text())
    assert done["status"] == "succeeded" and done["result"]["job"] == "backfill"
    assert (out / "2026-01-01-01-deploy" / "bundle_summary.json").read_text() == '{"resources": {}}'
    lines = [" ".join(c) for c, _ in shell.calls]
    assert any("--params desk=logins,through=2" in line for line in lines)
    assert sum("fs cp -r --overwrite dbfs:/Volumes/x/exports/t" in line for line in lines) == 2
    assert _main(monkeypatch, req, out) == 0  # every request has a record now; nothing runs twice
    assert len(shell.calls) == len(lines)


def test_terraform_gets_the_neon_endpoint(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    shell = FakeShell()
    monkeypatch.setattr(run, "_sh", shell)
    monkeypatch.setenv("TF_STATE_CONN", "postgresql://u:p@ep-a-1.us-east-2.aws.neon.tech/db?sslmode=require")
    req, out = tmp_path / "requests", tmp_path / "runs"
    _request(req, "2026-01-01-01-tf", "terraform_apply", params={"var_file": "env/dev.tfvars"})
    assert _main(monkeypatch, req, out) == 0
    envs = [env for cmd, env in shell.calls if cmd[0] == "terraform"]
    assert len(envs) == 4 and all(e["PG_CONN_STR"].endswith("options=endpoint%3Dep-a-1") for e in envs)
    assert (out / "2026-01-01-01-tf" / "terraform_apply.txt").exists()


def test_portable_copies_only_the_named_drops(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    shell = FakeShell()
    monkeypatch.setattr(run, "_sh", shell)
    req, out = tmp_path / "requests", tmp_path / "runs"
    _request(req, "2026-01-01-01-portable", "portable",
             params={"outbox_volume": "/Volumes/x/outbox", "drops": {"payments": [1, 1]}})  # fmt: skip
    assert _main(monkeypatch, req, out) == 0
    copies = [c for c, _ in shell.calls if c[:3] == ["databricks", "fs", "cp"]]
    assert len(copies) == len(run.FEEDS["payments"])
    assert all("/0001_1991-01" in c[4] for c in copies)
    scripts = [c[1] for c, _ in shell.calls if c[0].endswith("python") or "python" in c[0]]
    assert scripts == ["scripts/run_portable.py", "scripts/run_features.py", "scripts/summarize_lake.py"]


def test_a_failure_is_recorded_redacted_and_stops_the_queue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run, "_sh", FakeShell(fail_on="bundle validate"))
    monkeypatch.setenv("FRICTION_SALT", "s" * 40)
    req, out = tmp_path / "requests", tmp_path / "runs"
    _request(req, "2026-01-01-01-deploy", "bundle_deploy")
    _request(req, "2026-01-01-02-export", "export", params={"from": "/Volumes/x"})
    assert _main(monkeypatch, req, out) == 1
    done = json.loads((out / "2026-01-01-01-deploy" / "done.json").read_text())
    assert done["status"] == "failed" and "<secret>" in done["error"] and "s" * 40 not in done["error"]
    assert not (out / "2026-01-01-02-export").exists()


def test_only_reruns_one_request(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    shell = FakeShell()
    monkeypatch.setattr(run, "_sh", shell)
    req, out = tmp_path / "requests", tmp_path / "runs"
    _request(req, "2026-01-01-01-export", "export", params={"from": "/Volumes/a"})
    _request(req, "2026-01-01-02-export", "export", params={"from": "/Volumes/b"})
    assert _main(monkeypatch, req, out, "--only", "2026-01-01-02-export") == 0
    assert len(shell.calls) == 1 and "dbfs:/Volumes/b" in shell.calls[0][0]
    assert _main(monkeypatch, tmp_path / "empty", out) == 0


def test_the_real_shell_redacts_what_it_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FRICTION_REPLAY_KEY", "k" * 32)
    log = tmp_path / "run.log"
    assert run._sh(["python3", "-c", "import os; print(os.environ['FRICTION_REPLAY_KEY'])"], log) == "k" * 32 + "\n"
    text = log.read_text()
    assert "k" * 32 not in text and "<secret>" in text and "[exit 0]" in text
    with pytest.raises(RuntimeError):
        run._sh(["python3", "-c", "raise SystemExit(3)"], log)


def test_portable_copies_a_whole_feed_in_one_call(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    shell = FakeShell()
    monkeypatch.setattr(run, "_sh", shell)
    req, out = tmp_path / "requests", tmp_path / "runs"
    _request(req, "2026-01-01-01-portable", "portable",
             params={"outbox_volume": "/Volumes/x/outbox", "drops": {"logins": "all"}})  # fmt: skip
    assert _main(monkeypatch, req, out) == 0
    copies = [c for c, _ in shell.calls if c[:3] == ["databricks", "fs", "cp"]]
    assert [c[4] for c in copies] == [f"dbfs:/Volumes/x/outbox/{f}/drops" for f in ("logins", "labels_logins")]
    assert not [c for c, _ in shell.calls if c[:3] == ["databricks", "fs", "ls"]]


def test_portable_with_models_grades_and_exports(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    shell = FakeShell()
    monkeypatch.setattr(run, "_sh", shell)
    req, out = tmp_path / "requests", tmp_path / "runs"
    _request(req, "2026-01-01-01-portable", "portable",
             params={"outbox_volume": "/Volumes/x/outbox", "drops": {"logins": "all"}, "models": True})  # fmt: skip
    assert _main(monkeypatch, req, out) == 0
    scripts = [c[1] for c, _ in shell.calls if "python" in c[0]]
    assert scripts[-3:] == ["scripts/run_models.py", "scripts/grade.py", "scripts/export_models.py"]
    grade = next(c for c, _ in shell.calls if c[1:2] == ["scripts/grade.py"])
    assert grade[grade.index("--out") + 1].endswith("2026-01-01-01-portable/evaluation.json")


def test_a_failed_job_still_exports_its_evidence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    shell = FakeShell(fail_on="bundle run")
    monkeypatch.setattr(run, "_sh", shell)
    req, out = tmp_path / "requests", tmp_path / "runs"
    params = {"export_from": "/Volumes/x/exports/monitor", "stop_on_failure": False}
    _request(req, "2026-01-01-01-monitor", "bundle_run", job="monitor", params=params)
    _request(req, "2026-01-01-02-export", "export", params={"from": "/Volumes/y"})
    assert _main(monkeypatch, req, out) == 1
    assert json.loads((out / "2026-01-01-01-monitor" / "done.json").read_text())["status"] == "failed"
    lines = [" ".join(c) for c, _ in shell.calls]
    assert any("dbfs:/Volumes/x/exports/monitor" in line for line in lines)
    assert json.loads((out / "2026-01-01-02-export" / "done.json").read_text())["status"] == "succeeded"
