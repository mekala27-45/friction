"""The bundle stays inside Free Edition's limits: at most five tasks can run at once in any job."""

from __future__ import annotations

import yaml

from tests.conftest import ROOT


def _width(tasks: list[dict]) -> int:  # type: ignore[type-arg]
    depth: dict[str, int] = {}

    def d(key: str) -> int:
        if key not in depth:
            task = next(t for t in tasks if t["task_key"] == key)
            deps = [x["task_key"] for x in task.get("depends_on", [])]
            depth[key] = 1 + max((d(k) for k in deps), default=0)
        return depth[key]

    levels: dict[int, int] = {}
    for t in tasks:
        levels[d(t["task_key"])] = levels.get(d(t["task_key"]), 0) + 1
    return max(levels.values())


def test_no_job_runs_more_than_five_tasks_at_once() -> None:
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    for name, job in bundle["resources"]["jobs"].items():
        assert _width(job["tasks"]) <= 5, name
        assert job.get("max_concurrent_runs") == 1, name


def test_every_job_python_file_exists() -> None:
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    for job in bundle["resources"]["jobs"].values():
        for t in job["tasks"]:
            if "spark_python_task" in t:
                assert (ROOT / t["spark_python_task"]["python_file"]).exists()


def test_a_task_that_ships_a_package_has_its_third_party_imports() -> None:
    """ship() imports every module of a package, so the task's environment needs every module's
    imports, not only the ones the task calls (friction_core.log imports structlog)."""
    import ast

    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    needs = {"friction_core": {"structlog"}}
    for name, job in bundle["resources"]["jobs"].items():
        envs = {e["environment_key"]: e["spec"].get("dependencies", []) for e in job.get("environments", [])}
        for t in job["tasks"]:
            if "spark_python_task" not in t:
                continue
            tree = ast.parse((ROOT / t["spark_python_task"]["python_file"]).read_text(encoding="utf-8"))
            shipped = {
                a.id
                for n in ast.walk(tree)
                if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "ship"
                for a in n.args
                if isinstance(a, ast.Name)
            }
            listed = {d.split("=")[0].split(">")[0].split("<")[0].strip() for d in envs[t["environment_key"]]}
            for package in shipped & set(needs):
                assert needs[package] <= listed, f"{name}.{t['task_key']} ships {package} without {needs[package]}"
