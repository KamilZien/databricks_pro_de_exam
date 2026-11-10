"""
Subpoint 1.10 Drill: Serverless compute configuration in the REAL bundle file.

These unit tests read `resources/jobs.yml` (the file that is actually deployed) and
check the serverless settings. No Databricks connection is needed, so they run in CI.

BUSINESS REQUIREMENTS for job `finops_e2e_workflow` (read these, then fix the tests):
  R1. The job runs on a schedule overnight. Nobody waits for it. Cost must be minimised.
  R2. Task `audit_cleanup` writes an audit record. Running it twice would duplicate that
      record, so serverless must NOT silently retry it.
  R3. Task `process_success_report` only reads data and prints a report. It is safe to
      repeat, so serverless auto-optimization (automatic retries) should stay ENABLED.

The two tests below contain 2 INTENTIONAL BUGS in their expectations. The YAML is correct.
Fix the TESTS so they express R1-R3, then run:
    .venv\\Scripts\\python.exe -m pytest tests/test_subpoint_1_10_serverless_config_sabotage.py -v
"""

from pathlib import Path

import pytest
import yaml

JOBS_YML = Path(__file__).resolve().parent.parent.parent / "resources" / "jobs.yml"


def _load_job() -> dict:
    with JOBS_YML.open(encoding="utf-8") as f:
        spec = yaml.safe_load(f)
    return spec["resources"]["jobs"]["finops_e2e_workflow"]


def _task(job: dict, task_key: str) -> dict:
    return next(t for t in job["tasks"] if t["task_key"] == task_key)


@pytest.mark.unit
def test_job_performance_mode_matches_requirement_r1():
    job = _load_job()

    # R1: overnight batch, nobody waits, minimise cost -> exactly STANDARD.
    # (Valid values are STANDARD and PERFORMANCE_OPTIMIZED; accepting both would not enforce R1.)
    assert job["performance_target"] == "STANDARD"


@pytest.mark.unit
def test_retry_policy_matches_requirements_r2_and_r3():
    job = _load_job()
    audit = _task(job, "audit_cleanup")
    report = _task(job, "process_success_report")

    # `disable_auto_optimization: true` turns serverless auto-retries OFF for that task.
    # R2: audit_cleanup is not idempotent -> auto-retries must be disabled.
    assert audit.get("disable_auto_optimization", False) is True
    # R3: process_success_report is idempotent -> auto-retries stay enabled.
    assert report.get("disable_auto_optimization", False) is False
