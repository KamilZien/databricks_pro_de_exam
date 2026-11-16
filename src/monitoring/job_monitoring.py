"""
Section 4, Subpoint 4.1: Monitoring Lakeflow Jobs, Spark Execution & System Tables
Official Docs:
  - https://docs.databricks.com/en/admin/system-tables/jobs.html
  - https://docs.databricks.com/en/jobs/notifications.html
  - https://docs.databricks.com/en/sql/user/queries/query-profile.html

Provides:
  1. Lakeflow Job SLA configuration & runtime simulator distinguishing:
       - Soft Duration Warning (`health.rules` `RUN_DURATION_SECONDS` + `on_duration_warning_threshold_exceeded`):
         fires a warning notification when runtime exceeds `sla_seconds` WITHOUT killing the run.
       - Hard Timeout (`timeout_seconds`):
         terminates the active run with `result_state = "TIMEDOUT"` and triggers `on_failure`.
  2. Query Profile / Spark UI bottleneck classifier distinguishing Data Skew, Memory/Disk Spill, and Small Files.
"""

from typing import Any


def build_job_monitoring_config(
    sla_warning_seconds: int,
    hard_timeout_seconds: int | None,
    notify_email: str,
    use_soft_sla_warning: bool = True,
) -> dict[str, Any]:
    """
    Build a Declarative Automation Bundle (DAB) job monitoring block (`resources/jobs.yml`).
    When `use_soft_sla_warning=True`, places `sla_warning_seconds` inside `health.rules` +
    `email_notifications.on_duration_warning_threshold_exceeded` so slow runs alert without aborting.
    """
    config: dict[str, Any] = {
        "email_notifications": {"on_failure": [notify_email]},
    }
    if use_soft_sla_warning:
        config["health"] = {
            "rules": [
                {
                    "metric": "RUN_DURATION_SECONDS",
                    "op": "GREATER_THAN",
                    "value": sla_warning_seconds,
                }
            ]
        }
        config["email_notifications"]["on_duration_warning_threshold_exceeded"] = [notify_email]
        if hard_timeout_seconds is not None:
            config["timeout_seconds"] = hard_timeout_seconds
    else:
        # Anti-pattern: putting the SLA warning threshold directly into hard `timeout_seconds`!
        config["timeout_seconds"] = sla_warning_seconds
    return config


def simulate_job_run_outcome(job_config: dict[str, Any], actual_duration_seconds: int) -> dict[str, Any]:
    """
    Simulate how Lakeflow Jobs handles a run of `actual_duration_seconds` against `job_config`.
    """
    hard_timeout = job_config.get("timeout_seconds")
    if hard_timeout is not None and actual_duration_seconds > hard_timeout:
        return {
            "result_state": "TIMEDOUT",
            "termination_code": "RUN_TIMED_OUT",
            "warning_sent": False,
            "failure_sent": True,
        }

    warning_sent = False
    for rule in job_config.get("health", {}).get("rules", []):
        if (
            rule.get("metric") == "RUN_DURATION_SECONDS"
            and actual_duration_seconds > rule.get("value", 0)
            and job_config.get("email_notifications", {}).get("on_duration_warning_threshold_exceeded")
        ):
            warning_sent = True

    return {
        "result_state": "SUCCEEDED",
        "termination_code": "SUCCESS",
        "warning_sent": warning_sent,
        "failure_sent": False,
    }


def diagnose_query_profile(metrics: dict[str, float]) -> dict[str, str]:
    """
    Classify Spark UI / Query Profile bottlenecks using standard diagnostic ratios:
      - Small File Problem: `files_read >= 10_000` and average file size `< 1 MB`.
      - Memory/Disk Spill: `spill_disk_gb > 0` or `spill_memory_gb > 0`.
      - Data Skew: `max_task_seconds / max(median_task_seconds, 0.1) >= 10.0`.
    """
    files_read = metrics.get("files_read", 0)
    bytes_read_mb = metrics.get("bytes_read_mb", 0.0)
    avg_file_mb = (bytes_read_mb / files_read) if files_read > 0 else 999.0

    max_task = metrics.get("max_task_seconds", 0.0)
    median_task = max(metrics.get("median_task_seconds", 0.1), 0.1)
    skew_ratio = max_task / median_task

    spill_disk = metrics.get("spill_disk_gb", 0.0)

    if spill_disk > 0:
        return {
            "bottleneck": "DISK_SPILL",
            "recommended_fix": "Increase shuffle partitions / enable AQE coalescing or resize warehouse",
        }
    if skew_ratio >= 10.0:
        return {
            "bottleneck": "DATA_SKEW",
            "recommended_fix": (
                "Enable AQE Skew Join (spark.sql.adaptive.skewJoin.enabled) or filter/salt skewed NULL keys"
            ),
        }
    if files_read >= 10_000 and avg_file_mb < 1.0:
        return {
            "bottleneck": "SMALL_FILES",
            "recommended_fix": "Enable Predictive Optimization or run OPTIMIZE with Liquid Clustering (CLUSTER BY)",
        }
    return {"bottleneck": "HEALTHY", "recommended_fix": "No action needed"}


def validate_skew_mitigation_config(spark_conf: dict[str, str], use_broadcast_small_table: bool = False) -> bool:
    """
    Validate that AQE Skew Join mitigation is properly configured when a Query Profile detects DATA_SKEW:
      - Requires `spark.sql.adaptive.enabled == "true"` AND `spark.sql.adaptive.skewJoin.enabled == "true"`
        (unless explicitly mitigated via `use_broadcast_small_table=True` when the lookup side fits broadcast limits).
    """
    aqe_enabled = spark_conf.get("spark.sql.adaptive.enabled", "false").lower() == "true"
    skew_join_enabled = spark_conf.get("spark.sql.adaptive.skewJoin.enabled", "false").lower() == "true"

    if use_broadcast_small_table:
        return True
    if not (aqe_enabled and skew_join_enabled):
        raise ValueError(
            "[DATA_SKEW_UNMITIGATED] AQE Skew Join is disabled! Set both "
            "`spark.sql.adaptive.enabled = 'true'` and `spark.sql.adaptive.skewJoin.enabled = 'true'` "
            "(or broadcast the small lookup table) to eliminate straggler tasks."
        )
    return True
