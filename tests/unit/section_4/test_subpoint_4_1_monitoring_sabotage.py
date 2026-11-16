"""
Sabotage Drill — Section 4, Subpoint 4.1: Lakeflow Jobs Monitoring, Soft SLA Warnings & Query Profile
Run with:
    .venv\\Scripts\\python.exe -m pytest tests/unit/section_4/test_subpoint_4_1_monitoring_sabotage.py -v
"""

import pytest

from src.monitoring.job_monitoring import (
    build_job_monitoring_config,
    diagnose_query_profile,
    simulate_job_run_outcome,
    validate_skew_mitigation_config,
)


@pytest.mark.unit
def test_job_soft_sla_warning_vs_hard_timeout_sabotage():
    """
    Scenario:
    A nightly FinOps Lakeflow Job normally finishes in 10 minutes.
    Operations requires an alert if any run exceeds 15 minutes (900s), WITHOUT killing the active job
    (hard kill should only occur at 1 hour / 3600s).

    Today a data spike makes the run take 1,100 seconds (~18 minutes).
    Expected: `result_state == "SUCCEEDED"` AND `warning_sent is True`.
    """
    # --- FIXED BUG 1: Use soft SLA warning (`use_soft_sla_warning=True`) ---
    # Places 900s into `health.rules` (RUN_DURATION_SECONDS) + `on_duration_warning_threshold_exceeded`,
    # while keeping hard `timeout_seconds=3600`!
    job_cfg = build_job_monitoring_config(
        sla_warning_seconds=900,
        hard_timeout_seconds=3600,
        notify_email="oncall-finops@corp.com",
        use_soft_sla_warning=True,
    )

    outcome = simulate_job_run_outcome(job_cfg, actual_duration_seconds=1100)

    assert outcome["result_state"] == "SUCCEEDED", (
        f"Job was killed with {outcome['result_state']} ({outcome['termination_code']}) at 1100s! "
        "Must use `health.rules` (RUN_DURATION_SECONDS) + `on_duration_warning_threshold_exceeded` for non-fatal SLAs."
    )
    assert outcome["warning_sent"] is True


@pytest.mark.unit
def test_query_profile_straggler_task_diagnosis_sabotage():
    """
    Scenario:
    In the Databricks Query Profile / Spark UI for a join stage:
      - `median_task_seconds`: 3.0s
      - `max_task_seconds`: 1,800.0s (one straggler task took 30 minutes while 199 finished in 3 seconds!)
      - `spill_disk_gb`: 0.0
      - `files_read`: 120, `bytes_read_mb`: 2400.0 (20 MB per file — healthy file size)
    The engineer diagnosed DATA_SKEW and attempted to mitigate it via Spark AQE configuration.
    """
    profile_metrics = {
        "median_task_seconds": 3.0,
        "max_task_seconds": 1800.0,
        "spill_disk_gb": 0.0,
        "files_read": 120,
        "bytes_read_mb": 2400.0,
    }
    diagnosis = diagnose_query_profile(profile_metrics)
    assert diagnosis["bottleneck"] == "DATA_SKEW"

    # --- FIXED BUG 2: Enable AQE Skew Join (`spark.sql.adaptive.skewJoin.enabled = "true"`) ---
    spark_conf = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
    }
    assert validate_skew_mitigation_config(spark_conf) is True
