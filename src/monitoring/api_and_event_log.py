"""
Section 4 Gap-Fill (Official Subpoints 4.2, 4.3, 4.5):
REST API / CLI / SDK Monitoring, Jobs API Multi-Task Outputs, and Pipeline Event Logs.
Official Docs:
  - https://docs.databricks.com/api/workspace/jobs
  - https://docs.databricks.com/en/ldp/monitor-event-logs.html
"""

from typing import Any


def fetch_failed_task_error_trace(
    run_payload: dict[str, Any],
    outputs_by_run_id: dict[int, dict[str, str]],
    target_run_id: int | None = None,
) -> str:
    """
    Emulates `GET /api/2.1/jobs/runs/get-output?run_id=<task_run_id>` (or `w.jobs.get_run_output(run_id=...)`).

    CRITICAL EXAM RULE:
    For a multi-task Lakeflow Job, calling `runs/get-output` with the parent job `run_id` FAILS!
    You MUST inspect `run_payload["tasks"]`, locate the failed task's `run_id`, and pass THAT `task_run_id`
    to `runs/get-output`.
    """
    parent_run_id = run_payload["run_id"]
    tasks = run_payload.get("tasks", [])

    if target_run_id is None:
        failed_tasks = [t for t in tasks if t.get("state", {}).get("result_state") == "FAILED"]
        if not failed_tasks:
            raise ValueError(f"No FAILED tasks found in run {parent_run_id}.")
        target_run_id = failed_tasks[0]["run_id"]

    if len(tasks) > 1 and target_run_id == parent_run_id:
        raise ValueError(
            f"[INVALID_PARAMETER_VALUE] Run {parent_run_id} has {len(tasks)} tasks. "
            "Cannot call `runs/get-output` with the parent job `run_id`; "
            "pass the specific failed task's `run_id` from `tasks[].run_id`."
        )

    if target_run_id not in outputs_by_run_id:
        raise KeyError(f"No output found for task_run_id={target_run_id}.")

    return outputs_by_run_id[target_run_id]["error_trace"]


def extract_pipeline_audit_events(
    event_log_rows: list[dict[str, Any]],
    event_type_filter: str = "user_action",
) -> list[dict[str, Any]]:
    """
    Emulates querying `event_log('<pipeline-id>')` for specific `event_type`s
    (`user_action`, `update_progress`, `flow_progress`, `autoscale`).
    """
    valid_types = {"user_action", "update_progress", "flow_progress", "autoscale", "cluster_resources"}
    if event_type_filter not in valid_types:
        return []

    return [row for row in event_log_rows if row.get("event_type") == event_type_filter]
