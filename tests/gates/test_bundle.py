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
